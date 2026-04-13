import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def test_jobs_and_revenue_endpoints_require_auth_and_scope_to_current_user() -> None:
    temp_dir = Path(tempfile.gettempdir()) / f"sniped_jobs_auth_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "jobs_revenue_test.db"

    try:
        app_module.ensure_system_tables(db_path)
        app_module._ensure_jobs_table_sqlite(db_path)

        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO users (email, password_hash, salt, niche, token, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("user-a@example.com", "hash-a", "salt-a", "SEO", "token-a", _iso_now()),
                    ("user-b@example.com", "hash-b", "salt-b", "Ads", "token-b", _iso_now()),
                ],
            )
            conn.executemany(
                "INSERT INTO jobs (user_id, type, status, payload) VALUES (?, ?, ?, ?)",
                [
                    ("1", "scrape", "pending", "{}"),
                    ("2", "enrich", "done", "{}"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO revenue_log (user_id, amount, service_type, lead_name, lead_id, is_recurring, date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("1", 1000.0, "website", "Lead A", 1, 0, _iso_now()),
                    ("2", 2500.0, "ads", "Lead B", 2, 1, _iso_now()),
                ],
            )
            conn.commit()

        app = app_module.create_app()

        with (
            patch.object(app_module, "DEFAULT_DB_PATH", db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
        ):
            with TestClient(app) as client:
                unauth_jobs = client.get("/api/jobs")
                assert unauth_jobs.status_code == 401

                auth_headers = {"Authorization": "Bearer token-a"}

                jobs_response = client.get("/api/jobs", headers=auth_headers)
                assert jobs_response.status_code == 200, jobs_response.text
                job_items = jobs_response.json()["items"]
                assert len(job_items) == 1
                assert str(job_items[0]["user_id"]) == "1"

                job_id = str(job_items[0]["id"])
                own_job_response = client.get(f"/api/jobs/{job_id}", headers=auth_headers)
                assert own_job_response.status_code == 200, own_job_response.text

                foreign_job_response = client.get("/api/jobs/2", headers=auth_headers)
                assert foreign_job_response.status_code == 404, foreign_job_response.text

                create_job_response = client.post(
                    "/api/jobs",
                    headers=auth_headers,
                    json={"type": "mailer", "user_id": "2", "payload": {"ignored": True}},
                )
                assert create_job_response.status_code == 200, create_job_response.text

                with sqlite3.connect(db_path) as conn:
                    created_job = conn.execute(
                        "SELECT user_id, type FROM jobs WHERE id = ?",
                        (create_job_response.json()["job_id"],),
                    ).fetchone()
                assert created_job == ("1", "mailer")

                revenue_response = client.get("/api/revenue", headers=auth_headers)
                assert revenue_response.status_code == 200, revenue_response.text
                revenue_items = revenue_response.json()["items"]
                assert len(revenue_items) == 1
                assert float(revenue_items[0]["amount"]) == 1000.0

                add_revenue_response = client.post(
                    "/api/revenue",
                    headers=auth_headers,
                    json={
                        "amount": 300.0,
                        "service_type": "retainer",
                        "lead_name": "Scoped Lead",
                        "lead_id": 3,
                        "is_recurring": True,
                    },
                )
                assert add_revenue_response.status_code == 200, add_revenue_response.text

                stats_response = client.get("/api/stats", headers=auth_headers)
                assert stats_response.status_code == 200, stats_response.text
                stats_payload = stats_response.json()
                assert float(stats_payload["setup_revenue"]) == 1300.0
                assert float(stats_payload["monthly_recurring_revenue"]) == 300.0
    finally:
        try:
            if db_path.exists():
                db_path.unlink()
        except Exception:
            pass
        try:
            if temp_dir.exists():
                temp_dir.rmdir()
        except Exception:
            pass