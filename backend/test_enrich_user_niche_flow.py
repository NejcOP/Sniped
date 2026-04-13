import json
import sqlite3
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import backend.app as app_module


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def test_enrich_queues_user_niche_without_token() -> None:
    temp_dir = Path(tempfile.gettempdir()) / f"sniped_enrich_test_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "enrich_test.db"
    try:
        app_module.ensure_system_tables(db_path)

        session_token = "test-session-token"
        expected_niche = "SEO & Content"

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO users (email, password_hash, salt, niche, token, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "tester@example.com",
                    "dummy_hash",
                    "dummy_salt",
                    expected_niche,
                    session_token,
                    _iso_now(),
                ),
            )
            conn.commit()

        app = app_module.create_app()

        with (
            patch.object(app_module, "launch_detached_task", lambda *_args, **_kwargs: None),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/api/enrich",
                    json={
                        "token": session_token,
                        "db_path": str(db_path),
                        "limit": 1,
                        "headless": True,
                    },
                )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body.get("status") == "started"
        task_id = int(body["task_id"])

        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT request_payload FROM system_tasks WHERE id = ?",
                (task_id,),
            ).fetchone()

        assert row is not None
        payload = json.loads(row[0] or "{}")
        assert payload.get("user_niche") == expected_niche
        assert "token" not in payload
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


if __name__ == "__main__":
    test_enrich_queues_user_niche_without_token()
    print("[PASS] Enrich task stores user_niche and strips session token.")
