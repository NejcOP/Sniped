import unittest
from unittest.mock import patch

import backend.app as app_module


class DashboardStatsSupabaseResilienceTests(unittest.TestCase):
    def test_dashboard_stats_ignores_missing_revenue_log_table(self) -> None:
        def fake_select_rows(_client, table_name, **_kwargs):
            if table_name == "leads":
                return [
                    {
                        "id": 1,
                        "status": "paid",
                        "sent_at": "2026-04-28T09:00:00+00:00",
                        "last_contacted_at": "2026-04-28T09:00:00+00:00",
                        "reply_detected_at": "2026-04-28T10:00:00+00:00",
                        "paid_at": "2026-04-28T11:00:00+00:00",
                        "pipeline_stage": "Won (Paid)",
                        "scraped_at": "2026-04-27T09:00:00+00:00",
                        "status_updated_at": "2026-04-28T11:00:00+00:00",
                        "open_count": 2,
                        "client_tier": "standard",
                        "is_ads_client": 0,
                        "is_website_client": 1,
                        "client_folder_id": None,
                    }
                ]
            if table_name == "revenue_log":
                raise Exception("relation 'public.revenue_log' does not exist")
            if table_name == "ClientFolders":
                return []
            raise AssertionError(f"Unexpected table lookup: {table_name}")

        with (
            patch.object(app_module, "get_supabase_client", return_value=object()),
            patch.object(app_module, "supabase_select_rows", side_effect=fake_select_rows),
            patch.object(app_module, "get_queued_mail_count_supabase", return_value=0),
        ):
            stats = app_module.get_dashboard_stats_supabase(app_module.DEFAULT_CONFIG_PATH, user_id="user-1")

        self.assertEqual(stats["total_leads"], 1)
        self.assertEqual(stats["paid_count"], 1)
        self.assertEqual(stats["client_folder_count"], 0)
        self.assertGreaterEqual(float(stats["setup_revenue"]), 0.0)

    def test_queued_mail_count_falls_back_when_next_mail_at_is_missing(self) -> None:
        calls = []

        def fake_select_rows(_client, _table_name, **kwargs):
            calls.append(kwargs.get("columns"))
            if kwargs.get("columns") == "status,email,next_mail_at":
                raise Exception("Could not find the 'next_mail_at' column")
            return [
                {"status": "queued_mail", "email": "lead@example.com"},
                {"status": "queued_mail", "email": ""},
                {"status": "pending", "email": "other@example.com"},
            ]

        with (
            patch.object(app_module, "get_supabase_client", return_value=object()),
            patch.object(app_module, "supabase_select_rows", side_effect=fake_select_rows),
        ):
            queued_count = app_module.get_queued_mail_count_supabase(app_module.DEFAULT_CONFIG_PATH, user_id="user-1")

        self.assertEqual(queued_count, 1)
        self.assertEqual(calls[:2], ["status,email,next_mail_at", "status,email"])


if __name__ == "__main__":
    unittest.main()