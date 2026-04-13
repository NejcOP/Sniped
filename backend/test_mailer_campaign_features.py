import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module


class MailerCampaignFeatureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.gettempdir()) / f"sniped_mailer_campaigns_{uuid.uuid4().hex}"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.temp_dir / "mailer_campaigns.db"
        app_module.ensure_users_table(self.db_path)
        app_module.ensure_system_tables(self.db_path)

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
                    "campaign-user@example.com",
                    "hash",
                    "salt",
                    "SEO & Content",
                    "token-campaign",
                    500,
                    500,
                    500,
                    500,
                    0,
                    1,
                    "active",
                    "growth",
                ),
            )
            self.user_id = str(
                conn.execute("SELECT id FROM users WHERE token = ? LIMIT 1", ("token-campaign",)).fetchone()[0]
            )
            conn.executemany(
                """
                INSERT INTO leads (
                    user_id, business_name, email, website_url, address, search_keyword,
                    status, sent_at, open_count, first_opened_at, last_opened_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    (
                        self.user_id,
                        "Alpha Roofing",
                        "alpha@example.com",
                        "https://alpha.example.com",
                        "Dallas, TX, US",
                        "roofers dallas",
                        "emailed",
                        1,
                    ),
                    (
                        self.user_id,
                        "Beta Dental",
                        "beta@example.com",
                        "https://beta.example.com",
                        "Austin, TX, US",
                        "dentists austin",
                        "emailed",
                        0,
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

    def test_campaign_sequence_template_library_and_stats_endpoints(self) -> None:
        app = app_module.create_app()
        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            with TestClient(app) as client:
                headers = {"Authorization": "Bearer token-campaign"}

                sequence_response = client.post(
                    "/api/mailer/sequences",
                    headers=headers,
                    json={
                        "name": "Main 3-Step Sequence",
                        "step1_subject": "Quick idea for {BusinessName}",
                        "step1_body": "Hi {BusinessName},\n\nEmail 1 body",
                        "step2_delay_days": 3,
                        "step2_subject": "Following up on {BusinessName}",
                        "step2_body": "Hi again,\n\nEmail 2 body",
                        "step3_delay_days": 7,
                        "step3_subject": "Last note for {BusinessName}",
                        "step3_body": "Final follow-up",
                        "ab_subject_a": "A subject",
                        "ab_subject_b": "B subject",
                        "active": True,
                    },
                )
                self.assertEqual(sequence_response.status_code, 200, sequence_response.text)
                self.assertEqual(sequence_response.json().get("item", {}).get("name"), "Main 3-Step Sequence")

                template_response = client.post(
                    "/api/mailer/templates",
                    headers=headers,
                    json={
                        "name": "Winning Roofing Prompt",
                        "category": "roofing",
                        "prompt_text": "Use a direct local-offer opener.",
                        "subject_template": "Question about {BusinessName}",
                        "body_template": "Hi {BusinessName}, we can help improve local conversions.",
                    },
                )
                self.assertEqual(template_response.status_code, 200, template_response.text)
                self.assertEqual(template_response.json().get("item", {}).get("name"), "Winning Roofing Prompt")

                reply_event = client.post(
                    "/api/mailer/events",
                    headers=headers,
                    json={"lead_id": 1, "event_type": "reply"},
                )
                self.assertEqual(reply_event.status_code, 200, reply_event.text)

                bounce_event = client.post(
                    "/api/mailer/events",
                    headers=headers,
                    json={"lead_id": 2, "event_type": "bounce", "reason": "Mailbox unavailable"},
                )
                self.assertEqual(bounce_event.status_code, 200, bounce_event.text)

                stats_response = client.get("/api/mailer/campaign-stats", headers=headers)
                self.assertEqual(stats_response.status_code, 200, stats_response.text)
                payload = stats_response.json()
                self.assertEqual(payload.get("sent"), 2)
                self.assertEqual(payload.get("opened"), 1)
                self.assertEqual(payload.get("replied"), 1)
                self.assertEqual(payload.get("bounced"), 1)
                self.assertGreaterEqual(float(payload.get("open_rate") or 0), 50.0)
                self.assertEqual(len(payload.get("sequences") or []), 1)
                self.assertEqual(len(payload.get("saved_templates") or []), 1)


if __name__ == "__main__":
    unittest.main()
