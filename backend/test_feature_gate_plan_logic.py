import json
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module


class FeatureGatePlanLogicTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.gettempdir()) / f"sniped_feature_gate_{uuid.uuid4().hex}"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.temp_dir / "feature_gate.db"
        app_module.ensure_users_table(self.db_path)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO users (
                    email, password_hash, salt, niche, token,
                    credits_balance, monthly_quota, monthly_limit, credits_limit,
                    topup_credits_balance, subscription_active, subscription_status, plan_key, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    "free@example.com",
                    "hash",
                    "salt",
                    "SEO & Content",
                    "token-free",
                    50,
                    50,
                    50,
                    50,
                    0,
                    0,
                    "inactive",
                    "free",
                ),
            )
            self.user_id = str(
                conn.execute("SELECT id FROM users WHERE token = ? LIMIT 1", ("token-free",)).fetchone()[0]
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

    def test_free_plan_is_blocked_from_enrich_but_can_launch_mailer(self) -> None:
        app = app_module.create_app()
        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "enqueue_task", lambda *_args, **_kwargs: {"status": "started", "task_type": "mailer"}),
        ):
            with TestClient(app) as client:
                headers = {"Authorization": "Bearer token-free"}

                enrich_response = client.post(
                    "/api/enrich",
                    headers=headers,
                    json={"limit": 1, "headless": True, "skip_export": True},
                )
                self.assertEqual(enrich_response.status_code, 403, enrich_response.text)
                self.assertEqual(
                    enrich_response.json().get("detail"),
                    "This feature is available on Growth and above.",
                )

                mailer_response = client.post(
                    "/api/mailer/send",
                    headers=headers,
                    json={"limit": 1, "delay_min": 1, "delay_max": 1},
                )
                self.assertEqual(mailer_response.status_code, 200, mailer_response.text)
                self.assertEqual(mailer_response.json().get("status"), "started")

    def test_plan_access_context_maps_models_and_priority(self) -> None:
        self.assertEqual(app_module.normalize_plan_key("basic"), "hustler")

        hustler_access = app_module.get_plan_feature_access("hustler")
        self.assertEqual(hustler_access["ai_model"], "gpt-4o-mini")
        self.assertTrue(hustler_access["ai_lead_scoring"])
        self.assertFalse(hustler_access["bulk_export"])

        growth_access = app_module.get_plan_feature_access("growth")
        self.assertEqual(growth_access["ai_model"], "gpt-4o")
        self.assertTrue(growth_access["deep_analysis"])
        self.assertTrue(growth_access["bulk_export"])
        self.assertTrue(growth_access["drip_campaigns"])
        self.assertFalse(growth_access.get("advanced_reporting"))

        scale_access = app_module.get_plan_feature_access("business")
        self.assertEqual(scale_access["plan_key"], "scale")
        self.assertTrue(scale_access["webhooks"])
        self.assertTrue(scale_access.get("advanced_reporting"))
        self.assertTrue(scale_access.get("client_success_dashboard"))

        empire_access = app_module.get_plan_feature_access("empire")
        self.assertEqual(empire_access["plan_type"], "Empire")
        self.assertTrue(empire_access["queue_priority"])
        self.assertTrue(empire_access["webhooks"])
        self.assertTrue(empire_access["ai_lead_scoring"])
        self.assertTrue(empire_access.get("advanced_reporting"))
        self.assertTrue(empire_access.get("client_success_dashboard"))

    def test_zero_credit_guard_returns_upgrade_message(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE users SET credits_balance = 0 WHERE token = ?", ("token-free",))
            conn.commit()

        with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
            with self.assertRaises(app_module.HTTPException) as ctx:
                app_module.has_enough_credits(self.user_id, required_credits=1, db_path=self.db_path)

        self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(str(ctx.exception.detail), "Out of credits. Please upgrade.")

    def test_execute_mailer_task_charges_one_credit_per_sent_email(self) -> None:
        app = app_module.create_app()
        app.state.mailer_stop_event = app_module.Event()
        app_module.ensure_system_tables(self.db_path)
        task_id = app_module.create_task_record(
            self.db_path,
            self.user_id,
            "mailer",
            "queued",
            {"limit": 5, "delay_min": 1, "delay_max": 1},
            source="test",
        )

        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "maybe_sync_supabase", lambda *_args, **_kwargs: None),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
            patch("backend.app.AIMailer") as mock_mailer_cls,
        ):
            mock_mailer = mock_mailer_cls.return_value
            mock_mailer.open_tracking_base_url = ""
            mock_mailer.send.return_value = (3, 0, 0)
            mock_mailer.last_send_summary = {
                "requested_limit": 5,
                "effective_limit": 3,
                "daily_cap": 25,
                "sent_today": 0,
                "remaining_today": 25,
                "candidate_count": 3,
            }

            app_module.execute_mailer_task(app, {
                "task_id": task_id,
                "user_id": self.user_id,
                "limit": 5,
                "delay_min": 1,
                "delay_max": 1,
            })

            snapshot = app_module._load_user_credit_snapshot(self.user_id, db_path=self.db_path)
            self.assertEqual(int(snapshot.get("credits_balance") or 0), 47)

    def test_profile_can_persist_quickstart_completion_and_average_deal_value(self) -> None:
        app = app_module.create_app()
        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            with TestClient(app) as client:
                initial = client.post("/api/auth/profile", json={"token": "token-free"})
                self.assertEqual(initial.status_code, 200, initial.text)
                self.assertFalse(bool(initial.json().get("quickstart_completed")))
                self.assertEqual(float(initial.json().get("average_deal_value") or 0), 1000.0)

                update = client.put(
                    "/api/auth/profile",
                    json={"token": "token-free", "quickstart_completed": True, "average_deal_value": 2500},
                )
                self.assertEqual(update.status_code, 200, update.text)
                self.assertTrue(bool(update.json().get("quickstart_completed")))
                self.assertEqual(float(update.json().get("average_deal_value") or 0), 2500.0)

                refreshed = client.post("/api/auth/profile", json={"token": "token-free"})
                self.assertEqual(refreshed.status_code, 200, refreshed.text)
                self.assertTrue(bool(refreshed.json().get("quickstart_completed")))
                self.assertEqual(float(refreshed.json().get("average_deal_value") or 0), 2500.0)

    def test_unsubscribe_endpoint_blacklists_email(self) -> None:
        app = app_module.create_app()
        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            with TestClient(app) as client:
                response = client.get("/api/unsubscribe/unsubscribed%40example.com")
                self.assertEqual(response.status_code, 200, response.text)

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT kind, value FROM lead_blacklist WHERE kind = 'email' AND value = ? LIMIT 1",
                ("unsubscribed@example.com",),
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], "email")
        self.assertEqual(row[1], "unsubscribed@example.com")

    def test_blacklist_entry_can_be_removed_and_lead_reactivated(self) -> None:
        app = app_module.create_app()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO leads (
                    business_name, email, address, website_url, status, search_keyword, user_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Reactivated Lead Co",
                    "reactivation-test@example.invalid",
                    "123 Main St",
                    "https://reactivate.example.org",
                    "blacklisted",
                    "roofer dallas",
                    self.user_id,
                ),
            )
            conn.execute(
                "INSERT INTO lead_blacklist (kind, value, reason, created_at) VALUES ('email', ?, ?, CURRENT_TIMESTAMP)",
                ("reactivation-test@example.invalid", "Manual dashboard block"),
            )
            conn.commit()

        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            with TestClient(app) as client:
                response = client.delete(
                    "/api/blacklist?kind=email&value=reactivation-test%40example.invalid",
                    headers={"Authorization": "Bearer token-free"},
                )
                self.assertEqual(response.status_code, 200, response.text)

        with sqlite3.connect(self.db_path) as conn:
            blacklist_row = conn.execute(
                "SELECT 1 FROM lead_blacklist WHERE kind = 'email' AND value = ? LIMIT 1",
                ("reactivation-test@example.invalid",),
            ).fetchone()
            lead_row = conn.execute(
                "SELECT status FROM leads WHERE email = ? LIMIT 1",
                ("reactivation-test@example.invalid",),
            ).fetchone()

        self.assertIsNone(blacklist_row)
        self.assertIsNotNone(lead_row)
        self.assertEqual(str(lead_row[0]), "Pending")

    def test_reserved_domain_lead_is_skipped_as_test_lead(self) -> None:
        config_path = self.temp_dir / "mailer_config.json"
        config_path.write_text(
            json.dumps(
                {
                    "openai": {"api_key": "test-key"},
                    "smtp_accounts": [
                        {
                            "email": "sender@example.com",
                            "password": "secret",
                            "host": "smtp.example.com",
                            "port": 587,
                            "from_name": "Sniped",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO leads (
                    business_name, email, website_url, status, ai_score, search_keyword, user_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Reserved QA Lead",
                    "reactivation-test@example.invalid",
                    "https://reactivate.example.org",
                    "enriched",
                    9.5,
                    "roofers dallas",
                    self.user_id,
                ),
            )
            conn.commit()

        with patch("backend.services.ai_mailer_service.AIMailer._send_via_account", lambda *_args, **_kwargs: None):
            mailer = app_module.AIMailer(db_path=str(self.db_path), config_path=str(config_path))
            sent, skipped, failed = mailer.send(limit=1, delay_min=0, delay_max=0)

        self.assertEqual((sent, skipped, failed), (0, 1, 0))
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT status FROM leads WHERE email = ?", ("reactivation-test@example.invalid",)).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), "Skipped (Test Lead)")

    def test_blacklisted_lead_is_skipped_as_unsubscribed(self) -> None:
        config_path = self.temp_dir / "mailer_config.json"
        config_path.write_text(
            json.dumps(
                {
                    "openai": {"api_key": "test-key"},
                    "smtp_accounts": [
                        {
                            "email": "sender@example.com",
                            "password": "secret",
                            "host": "smtp.example.com",
                            "port": 587,
                            "from_name": "Sniped",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO leads (
                    business_name, email, website_url, status, ai_score, search_keyword, user_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Blocked Lead Co",
                    "blocked@example.com",
                    "https://blocked.example.com",
                    "enriched",
                    9.1,
                    "dentist marketing",
                    self.user_id,
                ),
            )
            conn.execute(
                "INSERT INTO lead_blacklist (kind, value, reason, created_at) VALUES ('email', ?, ?, CURRENT_TIMESTAMP)",
                ("blocked@example.com", "Unsubscribe link"),
            )
            conn.commit()

        with patch("backend.services.ai_mailer_service.AIMailer._send_via_account", lambda *_args, **_kwargs: None):
            mailer = app_module.AIMailer(db_path=str(self.db_path), config_path=str(config_path))
            sent, skipped, failed = mailer.send(limit=1, delay_min=0, delay_max=0)

        self.assertEqual((sent, skipped, failed), (0, 1, 0))
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT status FROM leads WHERE email = ?", ("blocked@example.com",)).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), "Skipped (Unsubscribed)")

    def test_empire_plan_has_100000_monthly_credits(self) -> None:
        self.assertEqual(int(app_module.PLAN_MONTHLY_QUOTAS["empire"]), 100000)
        self.assertEqual(int(app_module.STRIPE_SUBSCRIPTION_PLANS["empire"]["credits"]), 100000)


if __name__ == "__main__":
    unittest.main()
