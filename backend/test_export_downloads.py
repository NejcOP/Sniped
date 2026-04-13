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


def test_export_leads_downloads_are_filtered_and_user_scoped() -> None:
    temp_dir = Path(tempfile.gettempdir()) / f"sniped_export_downloads_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "export_downloads.db"

    try:
        app_module.ensure_system_tables(db_path)

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
                """
                INSERT INTO leads (
                    user_id,
                    business_name,
                    website_url,
                    phone_number,
                    rating,
                    review_count,
                    address,
                    search_keyword,
                    email,
                    ai_score,
                    status,
                    enrichment_status,
                    main_shortcoming
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "1",
                        "Alpha Qualified",
                        "https://alpha.example.com",
                        "+38640111222",
                        4.8,
                        19,
                        "Alpha City",
                        "roofer",
                        "owner@alpha.example.com",
                        8.8,
                        "enriched",
                        "completed",
                        "Slow mobile site",
                    ),
                    (
                        "1",
                        "Alpha Low Score",
                        "https://alpha-low.example.com",
                        "+38640111333",
                        4.1,
                        7,
                        "Alpha City",
                        "roofer",
                        "owner@alpha-low.example.com",
                        6.2,
                        "enriched",
                        "completed",
                        "Weak local SEO",
                    ),
                    (
                        "1",
                        "Alpha Missing Email",
                        "https://alpha-no-email.example.com",
                        "+38640111444",
                        4.9,
                        22,
                        "Alpha City",
                        "roofer",
                        None,
                        9.3,
                        "enriched",
                        "completed",
                        "No booking funnel",
                    ),
                    (
                        "1",
                        "Alpha Invalid Email",
                        "https://alpha-invalid.example.com",
                        "+38640111555",
                        4.7,
                        14,
                        "Alpha City",
                        "roofer",
                        "bad@alpha-invalid.example.com",
                        9.1,
                        "invalid_email",
                        "completed",
                        "Broken contact flow",
                    ),
                    (
                        "2",
                        "Beta Foreign Lead",
                        "https://beta.example.com",
                        "+38640111666",
                        5.0,
                        30,
                        "Beta City",
                        "roofer",
                        "owner@beta.example.com",
                        9.6,
                        "enriched",
                        "completed",
                        "Foreign user row",
                    ),
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
                headers = {"Authorization": "Bearer token-a"}

                target_response = client.get("/api/export-leads?kind=target", headers=headers)
                assert target_response.status_code == 200, target_response.text
                assert "text/csv" in (target_response.headers.get("content-type") or "")
                assert "target_leads.csv" in (target_response.headers.get("content-disposition") or "")

                target_csv = target_response.text
                assert "Alpha Qualified" in target_csv
                assert "Alpha Missing Email" in target_csv
                assert "Alpha Low Score" not in target_csv
                assert "Beta Foreign Lead" not in target_csv

                ai_response = client.get("/api/export-leads?kind=ai_mailer", headers=headers)
                assert ai_response.status_code == 200, ai_response.text
                assert "text/csv" in (ai_response.headers.get("content-type") or "")
                assert "ai_mailer_ready.csv" in (ai_response.headers.get("content-disposition") or "")

                ai_csv = ai_response.text
                assert "Alpha Qualified" in ai_csv
                assert "Alpha Low Score" in ai_csv
                assert "Alpha Missing Email" not in ai_csv
                assert "Alpha Invalid Email" not in ai_csv
                assert "Beta Foreign Lead" not in ai_csv
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
