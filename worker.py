"""
Sniped – Async Job Queue Worker
==================================
Run this process alongside the FastAPI server:

    python worker.py

It polls the `jobs` table (Supabase first, SQLite fallback) for pending
jobs, processes up to MAX_CONCURRENT tasks simultaneously using
asyncio.Semaphore, and writes back status + results when finished.

Environment / config.json keys read:
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY  (or config.json supabase.*)
  WORKER_CONCURRENCY  (default 5, max 10)
  WORKER_POLL_INTERVAL  (seconds between polls, default 2)
  WORKER_ID  (optional label, auto-generated if not set)
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import random
import socket
import sqlite3
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Bootstrap – ensure repo root is on sys.path so backend.* imports work
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.app import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    get_supabase_client,
    is_supabase_primary_enabled,
    load_supabase_settings,
    GoogleMapsScraper,
    LeadEnricher,
)
from backend.scraper.db import batch_upsert_leads, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MAX_CONCURRENT: int = max(1, min(10, int(os.environ.get("WORKER_CONCURRENCY", "5"))))
POLL_INTERVAL: float = float(os.environ.get("WORKER_POLL_INTERVAL", "2"))
WORKER_ID: str = os.environ.get("WORKER_ID", f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}")
BACKOFF_BASE: float = 1.5        # exponential backoff base (seconds)
BACKOFF_MAX: float = 60.0        # max sleep after repeating errors

# ---------------------------------------------------------------------------
# Supabase / SQLite helpers
# ---------------------------------------------------------------------------

def _use_supabase() -> bool:
    return is_supabase_primary_enabled(DEFAULT_CONFIG_PATH)


def _get_client():
    return get_supabase_client(DEFAULT_CONFIG_PATH)


def _ensure_sqlite_jobs_table() -> None:
    with sqlite3.connect(DEFAULT_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'default',
                type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                payload TEXT NOT NULL DEFAULT '{}',
                result TEXT,
                error TEXT,
                worker_id TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                started_at TEXT,
                completed_at TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, created_at ASC)"
        )
        conn.commit()


def _claim_next_jobs_supabase(batch: int = MAX_CONCURRENT) -> list[dict]:
    """
    Atomically claim up to `batch` pending jobs via Supabase RPC / update trick.
    Uses UPDATE … RETURNING pattern via the supabase-py client.
    """
    client = _get_client()
    if client is None:
        return []

    # Grab a batch of pending job IDs
    pending = (
        client.table("jobs")
        .select("id")
        .eq("status", "pending")
        .order("created_at", desc=False)
        .limit(batch)
        .execute()
        .data or []
    )
    if not pending:
        return []

    ids = [row["id"] for row in pending]

    # Atomically flip them to 'processing' (only rows still 'pending' will match)
    now_iso = datetime.now(timezone.utc).isoformat()
    updated = (
        client.table("jobs")
        .update({"status": "processing", "worker_id": WORKER_ID, "started_at": now_iso})
        .in_("id", ids)
        .eq("status", "pending")          # guard: only claim still-pending rows
        .execute()
        .data or []
    )
    if not updated:
        return []

    # Fetch full payload for claimed jobs
    claimed_ids = [row["id"] for row in updated]
    full = (
        client.table("jobs")
        .select("id,user_id,type,payload")
        .in_("id", claimed_ids)
        .execute()
        .data or []
    )
    return full


def _claim_next_jobs_sqlite(batch: int = MAX_CONCURRENT) -> list[dict]:
    _ensure_sqlite_jobs_table()
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DEFAULT_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, user_id, type, payload FROM jobs WHERE status='pending' ORDER BY created_at ASC LIMIT ?",
            (batch,),
        ).fetchall()
        if not rows:
            return []
        ids = [r["id"] for r in rows]
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            f"UPDATE jobs SET status='processing', worker_id=?, started_at=? WHERE id IN ({placeholders}) AND status='pending'",
            [WORKER_ID, now_iso, *ids],
        )
        conn.commit()
    return [dict(r) for r in rows]


def _mark_job_supabase(job_id: Any, status: str, result: Any = None, error: str | None = None) -> None:
    client = _get_client()
    if client is None:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    update: dict = {"status": status, "updated_at": now_iso}
    if status in {"completed", "failed"}:
        update["completed_at"] = now_iso
    if result is not None:
        update["result"] = result if isinstance(result, dict) else {"value": result}
    if error is not None:
        update["error"] = error[:2000]
    client.table("jobs").update(update).eq("id", job_id).execute()


def _mark_job_sqlite(job_id: Any, status: str, result: Any = None, error: str | None = None) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    result_json = json.dumps(result) if result is not None else None
    with sqlite3.connect(DEFAULT_DB_PATH) as conn:
        conn.execute(
            "UPDATE jobs SET status=?, result=?, error=?, completed_at=? WHERE id=?",
            (status, result_json, error[:2000] if error else None, now_iso if status in {"completed", "failed"} else None, job_id),
        )
        conn.commit()


def mark_job(job_id: Any, status: str, result: Any = None, error: str | None = None) -> None:
    if _use_supabase():
        _mark_job_supabase(job_id, status, result, error)
    else:
        _mark_job_sqlite(job_id, status, result, error)


# ---------------------------------------------------------------------------
# Job executors
# ---------------------------------------------------------------------------

async def execute_scrape(payload: dict, job_id=None) -> dict:
    """Run Google Maps scraper and upsert leads."""
    loop = asyncio.get_running_loop()

    def _run() -> dict:
        keyword = str(payload.get("keyword") or "")
        results = int(payload.get("results") or 25)
        country = str(payload.get("country") or "US")
        headless = bool(payload.get("headless", True))

        # Live progress: write partial result to jobs table on every lead found
        def _on_progress(current_found: int, total_to_find: int, scanned_count: int, _lead) -> None:
            if job_id is not None:
                mark_job(job_id, "processing", result={
                    "current_found": current_found,
                    "total_to_find": total_to_find or results,
                    "scanned_count": scanned_count,
                    "keyword": keyword,
                })

        # Resolve user data dir (same logic as the main API)
        from pathlib import Path as _Path
        user_data_dir = _Path(f"{ROOT_DIR}/profiles/maps_profile_{country.lower()}")

        init_db(str(DEFAULT_DB_PATH))
        with GoogleMapsScraper(
            headless=headless,
            country=country,
            user_data_dir=str(user_data_dir),
        ) as scraper:
            leads = scraper.scrape(
                keyword=keyword,
                max_results=results,
                progress_callback=_on_progress,
            )

        inserted = batch_upsert_leads(leads, db_path=str(DEFAULT_DB_PATH))
        return {
            "keyword": keyword,
            "scraped": len(leads),
            "inserted": inserted,
            "current_found": len(leads),
            "total_to_find": results,
            "country": country,
        }

    # Run blocking scraper in a thread pool so we don't block the event loop
    return await loop.run_in_executor(None, _run)


async def execute_enrich(payload: dict, job_id=None) -> dict:
    loop = asyncio.get_running_loop()
    limit = int(payload.get("limit") or 50)
    headless = bool(payload.get("headless", True))

    def _progress(processed: int, total: int, with_email: int, _name) -> None:
        if job_id is not None:
            mark_job(job_id, "processing", result={
                "processed": processed,
                "total": total,
                "with_email": with_email,
            })

    def _run() -> dict:
        enricher = LeadEnricher(
            db_path=str(DEFAULT_DB_PATH),
            headless=headless,
            config_path=str(DEFAULT_CONFIG_PATH),
        )
        enriched, with_email = enricher.run(limit=limit, progress_callback=_progress)
        return {"enriched": enriched, "with_email": with_email, "processed": enriched, "total": limit}

    return await loop.run_in_executor(None, _run)


async def execute_mailer(payload: dict, job_id=None) -> dict:
    """Mailer is handled by the main process via /api/mailer/send; worker logs a skip."""
    log.info("Mailer job %s delegated to main API process", payload)
    return {"note": "Mailer jobs are executed via the main FastAPI process (POST /api/mailer/send). Job logged."}


JOB_EXECUTORS = {
    "scrape": execute_scrape,
    "enrich": execute_enrich,
    "mailer": execute_mailer,
}

# ---------------------------------------------------------------------------
# Core worker loop
# ---------------------------------------------------------------------------

async def process_job(job: dict, semaphore: asyncio.Semaphore) -> None:
    job_id = job.get("id")
    job_type = str(job.get("type") or "")
    raw_payload = job.get("payload") or {}
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except Exception:
            raw_payload = {}

    log.info("Processing job %s type=%s", job_id, job_type)

    async with semaphore:
        executor = JOB_EXECUTORS.get(job_type)
        if executor is None:
            mark_job(job_id, "failed", error=f"Unknown job type: {job_type}")
            log.warning("Job %s: unknown type '%s'", job_id, job_type)
            return

        try:
            result = await executor(raw_payload, job_id=job_id)
            mark_job(job_id, "completed", result=result)
            log.info("Job %s completed: %s", job_id, result)
        except Exception as exc:
            err_msg = traceback.format_exc()
            mark_job(job_id, "failed", error=err_msg)
            log.error("Job %s failed: %s", job_id, exc)


async def poll_loop() -> None:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    consecutive_empty = 0
    log.info(
        "Worker %s started | concurrency=%d | poll_interval=%.1fs | Supabase=%s",
        WORKER_ID, MAX_CONCURRENT, POLL_INTERVAL, _use_supabase(),
    )

    while True:
        try:
            claim_fn = _claim_next_jobs_supabase if _use_supabase() else _claim_next_jobs_sqlite
            # Only claim as many slots as are currently free
            free_slots = MAX_CONCURRENT - (MAX_CONCURRENT - semaphore._value)  # noqa: SLF001
            jobs = claim_fn(batch=max(1, free_slots)) if semaphore._value > 0 else []  # noqa: SLF001

            if jobs:
                consecutive_empty = 0
                for job in jobs:
                    asyncio.create_task(process_job(job, semaphore))
            else:
                consecutive_empty += 1

            # Exponential backoff when queue is consistently empty (up to BACKOFF_MAX)
            sleep_time = min(POLL_INTERVAL * (BACKOFF_BASE ** min(consecutive_empty, 8)), BACKOFF_MAX) if consecutive_empty > 3 else POLL_INTERVAL
            await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            log.info("Worker %s shutting down.", WORKER_ID)
            break
        except Exception as exc:
            log.error("Poll loop error: %s", exc, exc_info=True)
            await asyncio.sleep(POLL_INTERVAL * 2)


if __name__ == "__main__":
    asyncio.run(poll_loop())
