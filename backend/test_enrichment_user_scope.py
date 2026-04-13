import sqlite3
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import backend.app as app_module
from backend.services.enrichment_service import LeadEnricher


def test_enrichment_and_queue_are_user_scoped() -> None:
    temp_dir = Path(tempfile.gettempdir()) / f"sniped_user_scope_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "scope_test.db"

    try:
        app_module.ensure_system_tables(db_path)

        with sqlite3.connect(db_path) as conn:
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
                    enrichment_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "user-a",
                        "A Scraped Lead",
                        "https://a.example.com",
                        None,
                        4.2,
                        10,
                        "Alpha City",
                        "plumber",
                        None,
                        None,
                        "scraped",
                        "pending",
                    ),
                    (
                        "user-b",
                        "B Scraped Lead",
                        "https://b.example.com",
                        None,
                        4.4,
                        12,
                        "Beta City",
                        "plumber",
                        None,
                        None,
                        "scraped",
                        "pending",
                    ),
                    (
                        "user-a",
                        "A Enriched Lead",
                        "https://a-mail.example.com",
                        None,
                        4.8,
                        20,
                        "Alpha City",
                        "roofer",
                        "owner@a-mail.example.com",
                        9.0,
                        "enriched",
                        "completed",
                    ),
                    (
                        "user-b",
                        "B Enriched Lead",
                        "https://b-mail.example.com",
                        None,
                        4.7,
                        18,
                        "Beta City",
                        "roofer",
                        "owner@b-mail.example.com",
                        9.0,
                        "enriched",
                        "completed",
                    ),
                ],
            )
            conn.commit()

        enricher = LeadEnricher(
            db_path=str(db_path),
            headless=True,
            config_path=str(app_module.DEFAULT_CONFIG_PATH),
            user_id="user-a",
        )

        pending_rows = enricher._fetch_leads_for_enrichment()
        pending_names = [str(row["business_name"]) for row in pending_rows]
        assert pending_names == ["A Scraped Lead"]

        with patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False):
            queued = app_module.queue_high_score_enriched_leads(db_path, user_id="user-a")
        assert queued == 1

        with sqlite3.connect(db_path) as conn:
            status_rows = conn.execute(
                "SELECT user_id, business_name, status FROM leads WHERE ai_score IS NOT NULL ORDER BY business_name ASC"
            ).fetchall()

        assert status_rows == [
            ("user-a", "A Enriched Lead", "queued_mail"),
            ("user-b", "B Enriched Lead", "enriched"),
        ]
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