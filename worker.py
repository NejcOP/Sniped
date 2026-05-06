"""
Sniped – Async Job Queue Worker
==================================
Run this process alongside the FastAPI server:

    python worker.py

It polls the `system_tasks` table in Postgres, atomically claims queued
rows with `FOR UPDATE SKIP LOCKED`, processes up to MAX_CONCURRENT tasks
simultaneously using asyncio.Semaphore, and lets the shared backend task
executors write status + results back to the same table.

Environment keys read:
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
    WORKER_CONCURRENCY  (default 3, max 6)
  WORKER_POLL_INTERVAL  (seconds between polls, default 2)
  WORKER_ID  (optional label, auto-generated if not set)
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import socket
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import bindparam, text

# ---------------------------------------------------------------------------
# Bootstrap – ensure repo root is on sys.path so backend.* imports work
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.app import (
    DEFAULT_CONFIG_PATH,
    app as backend_app,
    finish_task_record,
    get_task_executor,
)
from backend.scraper.db import dispose_cached_engines, get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MAX_CONCURRENT: int = max(1, min(6, int(os.environ.get("WORKER_CONCURRENCY", "3"))))
POLL_INTERVAL: float = float(os.environ.get("WORKER_POLL_INTERVAL", "2"))
WORKER_ID: str = os.environ.get("WORKER_ID", f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}")
BACKOFF_BASE: float = 1.5        # exponential backoff base (seconds)
BACKOFF_MAX: float = 60.0        # max sleep after repeating errors
SUPPORTED_TASK_TYPES: tuple[str, ...] = ("scrape", "enrich", "mailer")
WORKER_STARTED_AT = datetime.now(timezone.utc)

# ---------------------------------------------------------------------------
# Postgres task/runtime helpers
# ---------------------------------------------------------------------------

def _pg_enabled() -> bool:
    database_url = str(os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        return False
    try:
        get_engine()
    except Exception:
        return False
    return True


def _log_worker_credential_status() -> None:
    has_database_url = bool(str(os.environ.get("DATABASE_URL") or "").strip())
    has_supabase_url = bool(str(os.environ.get("SUPABASE_URL") or "").strip())
    has_supabase_service_role = bool(str(os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip())

    log.info(
        "Worker credential check | DATABASE_URL=%s SUPABASE_URL=%s SUPABASE_SERVICE_ROLE_KEY=%s",
        "set" if has_database_url else "missing",
        "set" if has_supabase_url else "missing",
        "set" if has_supabase_service_role else "missing",
    )

    if not has_database_url:
        log.error("DATABASE_URL is missing; worker cannot claim or update tasks.")
    if has_supabase_url and not has_supabase_service_role:
        log.warning("SUPABASE_SERVICE_ROLE_KEY is missing; Supabase writes may fail under RLS policies.")


def _runtime_upsert(key: str, value: str) -> None:
    if not _pg_enabled():
        return
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO system_runtime (key, value, updated_at)
                VALUES (:key, :value, :updated_at)
                ON CONFLICT(key)
                DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """
            ),
            {
                "key": key,
                "value": value,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )


def _runtime_increment(key: str, delta: int = 1) -> None:
    if not _pg_enabled():
        return []
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO system_runtime (key, value, updated_at)
                VALUES (:key, :delta_text, :updated_at)
                ON CONFLICT(key)
                DO UPDATE SET
                    value = (
                        CASE
                            WHEN system_runtime.value ~ '^-?[0-9]+$' THEN (system_runtime.value::bigint + :delta)::text
                            ELSE :delta_text
                        END
                    ),
                    updated_at = excluded.updated_at
                """
            ),
            {
                "key": key,
                "delta": int(delta),
                "delta_text": str(int(delta)),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )


def _update_worker_heartbeat() -> None:
    now = datetime.now(timezone.utc)
    uptime_seconds = int((now - WORKER_STARTED_AT).total_seconds())
    _runtime_upsert(f"worker:{WORKER_ID}:started_at", WORKER_STARTED_AT.isoformat())
    _runtime_upsert(f"worker:{WORKER_ID}:last_heartbeat_at", now.isoformat())
    _runtime_upsert(f"worker:{WORKER_ID}:uptime_seconds", str(uptime_seconds))


def _record_task_outcome(task_id: int, task_type: str) -> None:
    if not _pg_enabled():
        return
    with get_engine().begin() as conn:
        row = conn.execute(
            text("SELECT status FROM system_tasks WHERE id = :task_id LIMIT 1"),
            {"task_id": int(task_id)},
        ).fetchone()
    status = str(row[0] if row else "").strip().lower()
    now_iso = datetime.now(timezone.utc).isoformat()
    _runtime_increment("tasks_processed_total", 1)
    _runtime_upsert(f"worker:{WORKER_ID}:last_task_finished_at", now_iso)
    _runtime_upsert(f"worker:{WORKER_ID}:last_task_type", task_type)
    if status == "completed":
        _runtime_increment("tasks_success_total", 1)
        _runtime_upsert(f"worker:{WORKER_ID}:last_task_success_at", now_iso)
    elif status in {"failed", "stopped"}:
        _runtime_increment("tasks_failed_total", 1)
        _runtime_upsert(f"worker:{WORKER_ID}:last_task_failure_at", now_iso)


def _claim_next_tasks_postgres(batch: int = MAX_CONCURRENT) -> list[dict[str, Any]]:
    if not _pg_enabled():
        return []

    now_iso = datetime.now(timezone.utc).isoformat()
    claim_statement = text(
        """
        WITH next_tasks AS (
            SELECT id
            FROM system_tasks
            WHERE status = 'queued'
              AND task_type IN :task_types
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT :batch
        )
        UPDATE system_tasks AS task
        SET
            status = 'running',
            started_at = COALESCE(task.started_at, :now_iso),
            worker_id = :worker_id,
            updated_at = :now_iso
        FROM next_tasks
        WHERE task.id = next_tasks.id
        RETURNING task.id, task.user_id, task.task_type, task.request_payload
        """
    ).bindparams(bindparam("task_types", expanding=True))

    with get_engine().begin() as conn:
        rows = conn.execute(
            claim_statement,
            {
                "task_types": list(SUPPORTED_TASK_TYPES),
                "batch": max(1, int(batch)),
                "now_iso": now_iso,
                "worker_id": WORKER_ID,
            },
        ).mappings().all()
    if rows:
        _runtime_upsert(f"worker:{WORKER_ID}:last_claimed_task_at", now_iso)
    return [dict(row) for row in rows]


def _deserialize_payload(raw_payload: Any) -> dict[str, Any]:
    if isinstance(raw_payload, dict):
        return dict(raw_payload)
    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


# ---------------------------------------------------------------------------
# Task execution
# ---------------------------------------------------------------------------
async def process_task(task: dict[str, Any], semaphore: asyncio.Semaphore) -> None:
    task_id = int(task.get("id") or 0)
    task_type = str(task.get("task_type") or "").strip().lower()
    payload_data = _deserialize_payload(task.get("request_payload"))
    payload_data["task_id"] = task_id
    payload_data["task_type"] = task_type
    payload_data["user_id"] = str(task.get("user_id") or payload_data.get("user_id") or "legacy")
    payload_data.setdefault("config_path", str(DEFAULT_CONFIG_PATH))

    log.info("Processing system task %s type=%s", task_id, task_type)
    _runtime_upsert(f"worker:{WORKER_ID}:last_task_started_at", datetime.now(timezone.utc).isoformat())

    async with semaphore:
        try:
            executor = get_task_executor(task_type)
        except Exception:
            finish_task_record(None, task_id, status="failed", error=f"Unsupported task type: {task_type}")
            _record_task_outcome(task_id, task_type)
            return

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, executor, backend_app, payload_data)
        except Exception:
            err_msg = traceback.format_exc()
            finish_task_record(None, task_id, status="failed", error=err_msg)
            log.error("System task %s crashed in worker", task_id, exc_info=True)
        finally:
            _record_task_outcome(task_id, task_type)
            gc.collect()


async def poll_loop() -> None:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    in_flight_tasks: set[asyncio.Task[Any]] = set()
    consecutive_empty = 0
    connection_failures = 0
    _log_worker_credential_status()
    log.info(
        "Worker %s started | concurrency=%d | poll_interval=%.1fs",
        WORKER_ID,
        MAX_CONCURRENT,
        POLL_INTERVAL,
    )
    _runtime_upsert(f"worker:{WORKER_ID}:status", "online")

    while True:
        try:
            if not _pg_enabled():
                connection_failures += 1
                retry_sleep = min(BACKOFF_BASE ** min(connection_failures, 8), BACKOFF_MAX)
                log.warning(
                    "Worker %s is waiting for a healthy DATABASE_URL connection (retry in %.1fs).",
                    WORKER_ID,
                    retry_sleep,
                )
                await asyncio.sleep(retry_sleep)
                continue

            connection_failures = 0

            _update_worker_heartbeat()
            in_flight_tasks = {task for task in in_flight_tasks if not task.done()}
            free_slots = max(0, MAX_CONCURRENT - len(in_flight_tasks))
            tasks = _claim_next_tasks_postgres(batch=free_slots) if free_slots > 0 else []

            if tasks:
                consecutive_empty = 0
                _runtime_upsert(f"worker:{WORKER_ID}:status", "busy")
                for task in tasks:
                    background_task = asyncio.create_task(process_task(task, semaphore))
                    in_flight_tasks.add(background_task)
            else:
                consecutive_empty += 1
                _runtime_upsert(f"worker:{WORKER_ID}:status", "idle")

            # Exponential backoff when queue is consistently empty (up to BACKOFF_MAX)
            sleep_time = min(POLL_INTERVAL * (BACKOFF_BASE ** min(consecutive_empty, 8)), BACKOFF_MAX) if consecutive_empty > 3 else POLL_INTERVAL
            gc.collect()
            await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            _runtime_upsert(f"worker:{WORKER_ID}:status", "stopped")
            log.info("Worker %s shutting down.", WORKER_ID)
            if in_flight_tasks:
                await asyncio.gather(*in_flight_tasks, return_exceptions=True)
            dispose_cached_engines()
            break
        except Exception as exc:
            connection_failures += 1
            retry_sleep = min(BACKOFF_BASE ** min(connection_failures, 8), BACKOFF_MAX)
            log.error("Poll loop error: %s", exc, exc_info=True)
            _runtime_upsert(f"worker:{WORKER_ID}:status", "error")
            gc.collect()
            await asyncio.sleep(retry_sleep)

    dispose_cached_engines()


if __name__ == "__main__":
    asyncio.run(poll_loop())
