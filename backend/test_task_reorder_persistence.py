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


def test_task_reorder_persists_and_scopes_to_current_user() -> None:
    temp_dir = Path(tempfile.gettempdir()) / f"sniped_task_reorder_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "task_reorder_test.db"

    try:
        app_module.ensure_system_tables(db_path)

        with sqlite3.connect(db_path) as conn:
            now_iso = _iso_now()
            conn.executemany(
                """
                INSERT INTO users (email, password_hash, salt, niche, token, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("user-a@example.com", "hash-a", "salt-a", "SEO", "token-a", now_iso),
                    ("user-b@example.com", "hash-b", "salt-b", "Ads", "token-b", now_iso),
                ],
            )
            conn.executemany(
                """
                INSERT INTO delivery_tasks (
                    user_id, lead_id, worker_id, business_name, task_type, status, notes, due_at, done_at, position, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("1", 101, None, "Alpha", "Website Setup", "todo", None, now_iso, None, 1, now_iso, now_iso),
                    ("1", 102, None, "Beta", "Website Setup", "todo", None, now_iso, None, 2, now_iso, now_iso),
                    ("2", 201, None, "Gamma", "Website Setup", "todo", None, now_iso, None, 9, now_iso, now_iso),
                ],
            )
            conn.commit()

            user_one_ids = [
                int(row[0])
                for row in conn.execute(
                    "SELECT id FROM delivery_tasks WHERE user_id = ? ORDER BY position ASC",
                    ("1",),
                ).fetchall()
            ]
            user_two_id = int(
                conn.execute("SELECT id FROM delivery_tasks WHERE user_id = ?", ("2",)).fetchone()[0]
            )

        app = app_module.create_app()

        with (
            patch.object(app_module, "DEFAULT_DB_PATH", db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
        ):
            with TestClient(app) as client:
                unauth = client.post("/api/tasks/reorder", json={"task_ids": user_one_ids[::-1]})
                assert unauth.status_code == 401

                auth_headers = {"Authorization": "Bearer token-a"}
                reorder = client.post(
                    "/api/tasks/reorder",
                    headers=auth_headers,
                    json={"task_ids": user_one_ids[::-1]},
                )
                assert reorder.status_code == 200, reorder.text
                assert int(reorder.json().get("updated") or 0) == 2

                with sqlite3.connect(db_path) as conn:
                    reordered_ids = [
                        int(row[0])
                        for row in conn.execute(
                            "SELECT id FROM delivery_tasks WHERE user_id = ? ORDER BY position ASC",
                            ("1",),
                        ).fetchall()
                    ]
                    foreign_position = int(
                        conn.execute("SELECT position FROM delivery_tasks WHERE id = ?", (user_two_id,)).fetchone()[0]
                    )

                assert reordered_ids == user_one_ids[::-1]
                assert foreign_position == 9

                forbidden_mix = client.post(
                    "/api/tasks/reorder",
                    headers=auth_headers,
                    json={"task_ids": [user_one_ids[0], user_two_id]},
                )
                assert forbidden_mix.status_code == 404
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
