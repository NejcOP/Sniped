import json
import sqlite3
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module


class ReportingExportClientDashboardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.gettempdir()) / f"sniped_reporting_{uuid.uuid4().hex}"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.temp_dir / "reporting.db"
        self.config_path = self.temp_dir / "config.json"
        self.config_path.write_text(
            json.dumps(
                {
                    "hubspot_webhook_url": "https://example.invalid/hubspot",
                    "google_sheets_webhook_url": "https://example.invalid/sheets",
                    "smtp_accounts": [],
                }
            ),
            encoding="utf-8",
        )

        app_module.ensure_users_table(self.db_path)
        app_module.ensure_system_tables(self.db_path)

        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO users (
                    email, password_hash, salt, niche, token,
                    credits_balance, monthly_quota, monthly_limit, credits_limit,
                    topup_credits_balance, subscription_active, subscription_status, plan_key, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "owner@agency.example",
                    "hash",
                    "salt",
                    "SEO & Content",
                    "token-business",
                    500,
                    500,
                    500,
                    500,
                    0,
                    1,
                    "active",
                    "scale",
                    now,
                ),
            )
            user_id = str(conn.execute("SELECT id FROM users WHERE token = ?", ("token-business",)).fetchone()[0])
            conn.executemany(
                """
                INSERT INTO leads (
                    user_id, business_name, email, website_url, address, search_keyword,
                    status, enrichment_status, ai_score, ai_description,
                    scraped_at, sent_at, reply_detected_at, paid_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        user_id,
                        "Atlas Roofing",
                        "owner@atlas.example",
                        "https://atlas.example",
                        "Austin, TX",
                        "roofers austin",
                        "Pending",
                        "completed",
                        8.9,
                        "Fresh lead ready for outreach.",
                        now,
                        None,
                        None,
                        None,
                    ),
                    (
                        user_id,
                        "Beacon HVAC",
                        "info@beacon.example",
                        "https://beacon.example",
                        "Austin, TX",
                        "hvac austin",
                        "Emailed",
                        "completed",
                        7.9,
                        "Contacted this month.",
                        now,
                        now,
                        None,
                        None,
                    ),
                    (
                        user_id,
                        "Crown Dental",
                        "hello@crown.example",
                        "https://crown.example",
                        "Austin, TX",
                        "dentist austin",
                        "Paid",
                        "completed",
                        9.4,
                        "Won and converted.",
                        now,
                        now,
                        now,
                        now,
                    ),
                ],
            )
            conn.commit()

    def tearDown(self) -> None:
        for path in [self.db_path, self.config_path]:
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass
        try:
            if self.temp_dir.exists():
                self.temp_dir.rmdir()
        except Exception:
            pass

    def test_business_plan_can_use_reporting_exports_and_client_dashboard(self) -> None:
        app = app_module.create_app()
        captured: dict[str, object] = {}

        def fake_webhook(url: str, payload: dict) -> dict:
            captured["url"] = url
            captured["payload"] = payload
            return {"ok": True, "status": 200}

        def fake_report_send(*_args, **_kwargs) -> None:
            return None

        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "DEFAULT_CONFIG_PATH", self.config_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "deliver_export_webhook", fake_webhook, create=True),
            patch.object(app_module, "get_primary_smtp_account", lambda *_args, **_kwargs: {"email": "opnjc06@gmail.com", "host": "smtp.gmail.com", "password": "secret", "port": 587}),
            patch.object(app_module, "send_weekly_report_email", fake_report_send),
            patch.object(app_module, "send_monthly_report_email", fake_report_send),
        ):
            with TestClient(app) as client:
                headers = {"Authorization": "Bearer token-business"}

                folder_response = client.post(
                    "/api/client-folders",
                    headers=headers,
                    json={"name": "Atlas PPC", "color": "cyan", "notes": "Main agency folder"},
                )
                self.assertEqual(folder_response.status_code, 200, folder_response.text)
                folder_id = int(folder_response.json()["id"])

                assign_response = client.patch(
                    "/api/leads/1/client-folder",
                    headers=headers,
                    json={"client_folder_id": folder_id},
                )
                self.assertEqual(assign_response.status_code, 200, assign_response.text)

                segment_response = client.post(
                    "/api/saved-segments",
                    headers=headers,
                    json={
                        "name": "Hot Shopify Leads",
                        "filters": {
                            "advancedLeadFilters": {"techStacks": ["Shopify"], "highScoreOnly": True},
                            "leadSortMode": "best",
                        },
                    },
                )
                self.assertEqual(segment_response.status_code, 200, segment_response.text)
                self.assertEqual(segment_response.json().get("name"), "Hot Shopify Leads")

                saved_segments_response = client.get("/api/saved-segments", headers=headers)
                self.assertEqual(saved_segments_response.status_code, 200, saved_segments_response.text)
                saved_segments = saved_segments_response.json().get("items", [])
                self.assertTrue(any(item.get("name") == "Hot Shopify Leads" for item in saved_segments))

                leads_response = client.get("/api/leads?limit=20", headers=headers)
                self.assertEqual(leads_response.status_code, 200, leads_response.text)
                items = leads_response.json().get("items", [])
                by_name = {item["business_name"]: item for item in items}
                self.assertEqual(by_name["Atlas Roofing"].get("pipeline_stage"), "Scraped")
                self.assertEqual(by_name["Beacon HVAC"].get("pipeline_stage"), "Contacted")
                self.assertEqual(by_name["Crown Dental"].get("pipeline_stage"), "Won (Paid)")

                weekly_response = client.get("/api/reporting/weekly-summary", headers=headers)
                self.assertEqual(weekly_response.status_code, 200, weekly_response.text)
                weekly = weekly_response.json()
                self.assertEqual(weekly.get("found_this_week"), 3)
                self.assertEqual(weekly.get("contacted_this_week"), 2)
                self.assertEqual(weekly.get("won_this_week"), 1)

                report_response = client.get("/api/reporting/monthly-summary", headers=headers)
                self.assertEqual(report_response.status_code, 200, report_response.text)
                report = report_response.json()
                self.assertEqual(report.get("found_this_month"), 3)
                self.assertEqual(report.get("contacted_this_month"), 2)
                self.assertIn("pipeline", report)
                self.assertEqual(report["pipeline"].get("won_paid"), 1)

                weekly_email_response = client.post(
                    "/api/reporting/weekly-summary/email",
                    headers=headers,
                    json={"recipient": "wrong@example.com"},
                )
                self.assertEqual(weekly_email_response.status_code, 200, weekly_email_response.text)
                self.assertEqual(weekly_email_response.json().get("recipient"), "owner@agency.example")

                pdf_response = client.get("/api/reporting/monthly-summary.pdf", headers=headers)
                self.assertEqual(pdf_response.status_code, 200, pdf_response.text)
                self.assertIn("application/pdf", pdf_response.headers.get("content-type") or "")

                monthly_email_response = client.post(
                    "/api/reporting/monthly-summary/email",
                    headers=headers,
                    json={"recipient": "wrong@example.com"},
                )
                self.assertEqual(monthly_email_response.status_code, 200, monthly_email_response.text)
                self.assertEqual(monthly_email_response.json().get("recipient"), "owner@agency.example")

                dashboard_response = client.get("/api/client-dashboard", headers=headers)
                self.assertEqual(dashboard_response.status_code, 200, dashboard_response.text)
                dashboard = dashboard_response.json()
                self.assertGreaterEqual(dashboard.get("folder_count") or 0, 1)
                self.assertTrue(any(folder.get("name") == "Atlas PPC" for folder in dashboard.get("folders", [])))

                export_response = client.post(
                    "/api/export/webhook",
                    headers=headers,
                    json={"target": "hubspot", "kind": "target"},
                )
                self.assertEqual(export_response.status_code, 200, export_response.text)
                payload = export_response.json()
                self.assertEqual(payload.get("target"), "hubspot")
                self.assertGreaterEqual(int(payload.get("exported") or 0), 1)
                self.assertEqual(captured.get("url"), "https://example.invalid/hubspot")
                self.assertIn("items", captured.get("payload") or {})


    def test_weekly_and_monthly_report_emails_use_signup_email(self) -> None:
        self.config_path.write_text(
            json.dumps(
                {
                    "smtp_accounts": [
                        {
                            "email": "sender@example.com",
                            "password": "secret",
                            "host": "smtp.example.com",
                            "port": 587,
                            "from_name": "Sniped",
                        }
                    ],
                    "auto_weekly_report_email": True,
                    "auto_monthly_report_email": True,
                }
            ),
            encoding="utf-8",
        )

        sent_reports: list[dict[str, object]] = []

        def fake_send(account: dict, recipient: str, summary: dict[str, object], pdf_bytes: bytes | None = None) -> None:
            sent_reports.append(
                {
                    "recipient": recipient,
                    "label": summary.get("period_label") or summary.get("month_label"),
                    "has_pdf": bool(pdf_bytes),
                }
            )

        app = app_module.create_app()
        with (
            patch.object(app_module, "DEFAULT_DB_PATH", self.db_path),
            patch.object(app_module, "DEFAULT_CONFIG_PATH", self.config_path),
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "is_supabase_primary_enabled", lambda *_args, **_kwargs: False),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "send_weekly_report_email", fake_send, create=True),
            patch.object(app_module, "send_monthly_report_email", fake_send),
        ):
            app_module.run_weekly_report_digest(app)
            app_module.run_monthly_report_digest(app)

        self.assertEqual(len(sent_reports), 2)
        self.assertTrue(all(item.get("recipient") == "owner@agency.example" for item in sent_reports))
        self.assertTrue(any(not bool(item.get("has_pdf")) for item in sent_reports))
        self.assertTrue(any(bool(item.get("has_pdf")) for item in sent_reports))


if __name__ == "__main__":
    unittest.main()
