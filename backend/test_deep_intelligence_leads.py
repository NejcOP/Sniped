import json
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module


class DeepIntelligenceLeadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.gettempdir()) / f"sniped_deep_intel_{uuid.uuid4().hex}"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.temp_dir / "deep_intel.db"
        app_module.ensure_users_table(self.db_path)
        app_module.ensure_system_tables(self.db_path)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO users (email, password_hash, salt, niche, token, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    "intel@example.com",
                    "hash",
                    "salt",
                    "SEO & Content",
                    "token-deep-intel",
                ),
            )
            self.user_id = str(
                conn.execute("SELECT id FROM users WHERE token = ? LIMIT 1", ("token-deep-intel",)).fetchone()[0]
            )

            conn.executemany(
                """
                INSERT INTO leads (
                    user_id, business_name, email, website_url, address, search_keyword,
                    status, enrichment_status, ai_score, ai_description, enrichment_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        self.user_id,
                        "Atlas Commerce",
                        "owner@atlas.example.com",
                        "https://atlas.example.com",
                        "Austin, TX, US",
                        "shopify agency austin",
                        "enriched",
                        "completed",
                        8.8,
                        "Strong growth signals with clear demand capture upside.",
                        json.dumps(
                            {
                                "employee_count": 42,
                                "ai_sentiment_score": 91,
                                "strengths": ["Strong trust badges", "Clear CTA", "Fast mobile nav"],
                                "weak_points": ["Thin SEO pages", "No pricing clarity", "Weak schema markup"],
                                "competitor_snapshot": ["Outerbox", "Fuel Made", "Local Shopify pros"],
                                "tech_stack": ["Shopify", "Klaviyo"],
                                "intent_signals": ["Shopify detected", "Recently updated site"],
                            }
                        ),
                    ),
                    (
                        self.user_id,
                        "Tiny Studio",
                        "",
                        "https://tiny.example.com",
                        "Austin, TX, US",
                        "web design austin",
                        "enriched",
                        "completed",
                        6.2,
                        "Needs better visibility and offer clarity.",
                        json.dumps(
                            {
                                "employee_count": 4,
                                "ai_sentiment_score": 38,
                                "strengths": ["Nice branding"],
                                "weak_points": ["Weak CTA"],
                                "competitor_snapshot": ["Local design rivals"],
                                "tech_stack": ["Wix"],
                                "intent_signals": ["Wix detected"],
                            }
                        ),
                    ),
                ],
            )
            conn.commit()

    def tearDown(self) -> None:
        try:
            if self.db_path.exists():
                self.db_path.unlink()
        except Exception:
            pass
        try:
            if self.temp_dir.exists():
                self.temp_dir.rmdir()
        except Exception:
            pass

    def test_ensure_system_tables_migrates_legacy_leads_schema(self) -> None:
        legacy_db_path = self.temp_dir / "legacy_schema.db"
        with sqlite3.connect(legacy_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    business_name TEXT NOT NULL,
                    website_url TEXT,
                    phone_number TEXT,
                    rating REAL,
                    review_count INTEGER,
                    address TEXT NOT NULL DEFAULT '',
                    search_keyword TEXT,
                    scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(business_name, address)
                )
                """
            )
            conn.commit()

        app_module.ensure_system_tables(legacy_db_path)

        with sqlite3.connect(legacy_db_path) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}

        self.assertIn("user_id", columns)
        self.assertIn("created_at", columns)

    def test_leads_endpoint_supports_pagination_and_search(self) -> None:
        app = app_module.create_app()
        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            with TestClient(app) as client:
                response = client.get(
                    "/api/leads?limit=1&page=2&search=example.com",
                    headers={"Authorization": "Bearer token-deep-intel"},
                )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload.get("page"), 2)
        self.assertEqual(payload.get("page_size"), 1)
        self.assertEqual(payload.get("total"), 2)
        self.assertFalse(payload.get("has_more"))
        self.assertEqual(len(payload.get("items", [])), 1)
        self.assertEqual(payload["items"][0]["business_name"], "Atlas Commerce")

    def test_leads_endpoint_returns_deep_intelligence_fields(self) -> None:
        app = app_module.create_app()
        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            with TestClient(app) as client:
                response = client.get(
                    "/api/leads?limit=20",
                    headers={"Authorization": "Bearer token-deep-intel"},
                )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload.get("count"), 2)

        items = {item["business_name"]: item for item in payload.get("items", [])}
        atlas = items["Atlas Commerce"]
        tiny = items["Tiny Studio"]

        self.assertIn("company_audit", atlas)
        self.assertEqual(len(atlas["company_audit"].get("strengths") or []), 3)
        self.assertEqual(len(atlas["company_audit"].get("weaknesses") or []), 3)
        self.assertGreaterEqual(len(atlas.get("competitor_snapshot") or []), 2)
        self.assertIn("Shopify detected", atlas.get("intent_signals") or [])
        self.assertIn("Shopify", atlas.get("tech_stack") or [])
        self.assertGreater(float(atlas.get("best_lead_score") or 0), float(tiny.get("best_lead_score") or 0))


if __name__ == "__main__":
    unittest.main()
