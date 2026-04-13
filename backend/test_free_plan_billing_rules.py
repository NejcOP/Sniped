import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module


class _FakeSupabaseResponse:
    def __init__(self, data):
        self.data = data


class _FakeSupabaseUsersTable:
    def __init__(self, row):
        self._row = dict(row)

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        return _FakeSupabaseResponse([dict(self._row)])


class _FakeSupabaseClient:
    def __init__(self, row):
        self._row = dict(row)

    def table(self, name):
        assert name == "users"
        return _FakeSupabaseUsersTable(self._row)


class FreePlanBillingRulesTests(unittest.TestCase):
    def test_free_plan_monthly_quota_is_50(self) -> None:
        self.assertEqual(app_module.PLAN_MONTHLY_QUOTAS.get("free"), 50)

    def test_free_plan_credit_deduction_persists_after_user_table_check(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_free_plan_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "free_plan_rules.db"

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            email, password_hash, salt, niche, account_type, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            topup_credits_balance, subscription_active, plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "free@example.com",
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            "entrepreneur",
                            "token-free",
                            50,
                            50,
                            50,
                            50,
                            0,
                            0,
                            "free",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                billing = app_module.deduct_credits_on_success("1", credits_to_deduct=1, db_path=db_path)
                self.assertEqual(billing["credits_balance"], 49)

                app_module.ensure_users_table(db_path)
                snapshot = app_module._load_user_credit_snapshot("1", db_path=db_path)
                self.assertEqual(snapshot["credits_balance"], 49)
                self.assertEqual(snapshot["credits_limit"], 50)
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

    def test_free_plan_niche_recommendation_does_not_consume_credits(self) -> None:
        runtime_store: dict[str, str] = {}
        counters = {"paid_ai": 0, "niche_generation": 0}
        fake_client = _FakeSupabaseClient(
            {
                "id": "user-1",
                "email": "user@example.com",
                "token": "token-free",
                "niche": "Web Design & Dev",
                "display_name": "Tester",
                "contact_name": "",
                "account_type": "entrepreneur",
                "credits_balance": 50,
                "topup_credits_balance": 0,
                "monthly_quota": 50,
                "monthly_limit": 50,
                "credits_limit": 50,
                "subscription_start_date": "2026-04-01T00:00:00+00:00",
                "subscription_active": False,
                "subscription_status": "",
                "subscription_cancel_at": "",
                "subscription_cancel_at_period_end": False,
                "plan_key": "free",
                "stripe_customer_id": "",
                "updated_at": "",
            }
        )

        def _fake_get_runtime_value(_db_path, key):
            return runtime_store.get(str(key))

        def _fake_set_runtime_value(_db_path, key, value):
            runtime_store[str(key)] = str(value)

        def _fake_paid_ai(*_args, **_kwargs):
            counters["paid_ai"] += 1
            return ({"keyword": "Paid path should not run"}, {"credits_charged": 1, "credits_balance": 49, "credits_limit": 50})

        def _fake_get_niche(*_args, **_kwargs):
            counters["niche_generation"] += 1
            return {
                "keyword": "Web Design & Dev",
                "location": "Remote",
                "expected_reply_rate": 12.5,
                "estimated_opportunity": "$15k+",
            }

        app_module._NICHE_REC_CACHE.clear()

        with (
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: True),
            patch.object(app_module, "ensure_supabase_users_table", lambda *_args, **_kwargs: True),
            patch.object(app_module, "get_supabase_client", lambda *_args, **_kwargs: fake_client),
            patch.object(app_module, "get_runtime_value", _fake_get_runtime_value),
            patch.object(app_module, "set_runtime_value", _fake_set_runtime_value),
            patch.object(app_module, "run_ai_with_credit_policy", _fake_paid_ai),
            patch.object(app_module, "get_niche_recommendation", _fake_get_niche),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            app = app_module.create_app()
            with TestClient(app) as client:
                response = client.get("/api/recommend-niche", headers={"Authorization": "Bearer token-free"})
                response_again = client.get("/api/recommend-niche", headers={"Authorization": "Bearer token-free"})

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response_again.status_code, 200, response_again.text)
        payload = response.json()
        self.assertEqual(payload["credits_charged"], 0)
        self.assertEqual(payload["credits_balance"], 50)
        self.assertFalse(bool(payload.get("cached")))
        self.assertTrue(bool(response_again.json().get("cached")))
        self.assertEqual(counters["paid_ai"], 0)
        self.assertEqual(counters["niche_generation"], 1)
        self.assertTrue(any("niche_recommendation" in key for key in runtime_store.keys()))

    def test_free_plan_niche_recommendation_refreshes_after_seven_days(self) -> None:
        runtime_store: dict[str, str] = {}
        counters = {"niche_generation": 0}
        fake_client = _FakeSupabaseClient(
            {
                "id": "user-1",
                "email": "user@example.com",
                "token": "token-free",
                "niche": "Web Design & Dev",
                "display_name": "Tester",
                "contact_name": "",
                "account_type": "entrepreneur",
                "credits_balance": 50,
                "topup_credits_balance": 0,
                "monthly_quota": 50,
                "monthly_limit": 50,
                "credits_limit": 50,
                "subscription_start_date": "2026-04-01T00:00:00+00:00",
                "subscription_active": False,
                "subscription_status": "",
                "subscription_cancel_at": "",
                "subscription_cancel_at_period_end": False,
                "plan_key": "free",
                "stripe_customer_id": "",
                "updated_at": "",
            }
        )

        stale_key = "niche_recommendation:free:user-1"
        runtime_store[stale_key] = app_module.json.dumps(
            {
                "result": {
                    "keyword": "Old niche",
                    "location": "Old City",
                    "expected_reply_rate": 4.2,
                },
                "generated_at": "2026-03-01T00:00:00+00:00",
            }
        )

        def _fake_get_runtime_value(_db_path, key):
            return runtime_store.get(str(key))

        def _fake_set_runtime_value(_db_path, key, value):
            runtime_store[str(key)] = str(value)

        def _fake_get_niche(*_args, **_kwargs):
            counters["niche_generation"] += 1
            return {
                "keyword": "Fresh free-plan niche",
                "location": "Barcelona, ES",
                "expected_reply_rate": 10.5,
                "estimated_opportunity": "$42k+",
                "recommendations": [
                    {
                        "keyword": "Fresh free-plan niche",
                        "location": "Barcelona, ES",
                        "country_code": "ES",
                        "reason": "Fresh weekly recommendation.",
                        "expected_reply_rate": 10.5,
                    }
                ],
                "top_pick": {
                    "keyword": "Fresh free-plan niche",
                    "location": "Barcelona, ES",
                    "country_code": "ES",
                    "reason": "Fresh weekly recommendation.",
                    "expected_reply_rate": 10.5,
                },
                "generated_at": app_module.utc_now_iso(),
                "performance_snapshot": [],
            }

        app_module._NICHE_REC_CACHE.clear()

        with (
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: True),
            patch.object(app_module, "ensure_supabase_users_table", lambda *_args, **_kwargs: True),
            patch.object(app_module, "get_supabase_client", lambda *_args, **_kwargs: fake_client),
            patch.object(app_module, "get_runtime_value", _fake_get_runtime_value),
            patch.object(app_module, "set_runtime_value", _fake_set_runtime_value),
            patch.object(app_module, "get_niche_recommendation", _fake_get_niche),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            app = app_module.create_app()
            with TestClient(app) as client:
                response = client.get("/api/recommend-niche", headers={"Authorization": "Bearer token-free"})

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["keyword"], "Fresh free-plan niche")
        self.assertEqual(payload["credits_charged"], 0)
        self.assertEqual(counters["niche_generation"], 1)

    def test_paid_plan_niche_recommendation_is_free_and_refreshes_on_demand(self) -> None:
        runtime_store: dict[str, str] = {}
        counters = {"paid_ai": 0, "niche_generation": 0}
        fake_client = _FakeSupabaseClient(
            {
                "id": "user-2",
                "email": "paid@example.com",
                "token": "token-paid",
                "niche": "Lead Gen",
                "display_name": "Paid Tester",
                "contact_name": "",
                "account_type": "entrepreneur",
                "credits_balance": 7000,
                "topup_credits_balance": 0,
                "monthly_quota": 7000,
                "monthly_limit": 7000,
                "credits_limit": 7000,
                "subscription_start_date": "2026-04-01T00:00:00+00:00",
                "subscription_active": True,
                "subscription_status": "active",
                "subscription_cancel_at": "",
                "subscription_cancel_at_period_end": False,
                "plan_key": "growth",
                "stripe_customer_id": "cus_test",
                "updated_at": "",
            }
        )

        def _fake_get_runtime_value(_db_path, key):
            return runtime_store.get(str(key))

        def _fake_set_runtime_value(_db_path, key, value):
            runtime_store[str(key)] = str(value)

        def _fake_paid_ai(*_args, **_kwargs):
            counters["paid_ai"] += 1
            return ({"keyword": "Paid path should not run"}, {"credits_charged": 1, "credits_balance": 6999, "credits_limit": 7000})

        def _fake_get_niche(*_args, **_kwargs):
            counters["niche_generation"] += 1
            if counters["niche_generation"] == 1:
                keyword = "Solar Panel Installation in Barcelona, ES"
                location = "Barcelona, ES"
            else:
                keyword = "Commercial Cleaning in Austin, TX"
                location = "Austin, TX"
            return {
                "keyword": keyword,
                "location": location,
                "expected_reply_rate": 9.1,
                "estimated_opportunity": "$75k+",
                "recommendations": [
                    {
                        "keyword": keyword,
                        "location": location,
                        "country_code": "ES" if "Barcelona" in location else "US",
                        "reason": "Fresh paid-plan refresh.",
                        "expected_reply_rate": 9.1,
                    }
                ],
                "top_pick": {
                    "keyword": keyword,
                    "location": location,
                    "country_code": "ES" if "Barcelona" in location else "US",
                    "reason": "Fresh paid-plan refresh.",
                    "expected_reply_rate": 9.1,
                },
                "generated_at": app_module.utc_now_iso(),
                "performance_snapshot": [],
            }

        app_module._NICHE_REC_CACHE.clear()

        with (
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: True),
            patch.object(app_module, "ensure_supabase_users_table", lambda *_args, **_kwargs: True),
            patch.object(app_module, "get_supabase_client", lambda *_args, **_kwargs: fake_client),
            patch.object(app_module, "get_runtime_value", _fake_get_runtime_value),
            patch.object(app_module, "set_runtime_value", _fake_set_runtime_value),
            patch.object(app_module, "run_ai_with_credit_policy", _fake_paid_ai),
            patch.object(app_module, "get_niche_recommendation", _fake_get_niche),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            app = app_module.create_app()
            with TestClient(app) as client:
                response = client.get("/api/recommend-niche", headers={"Authorization": "Bearer token-paid"})
                refreshed = client.get("/api/recommend-niche?refresh=1", headers={"Authorization": "Bearer token-paid"})

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(refreshed.status_code, 200, refreshed.text)
        first_payload = response.json()
        refreshed_payload = refreshed.json()
        self.assertEqual(first_payload["credits_charged"], 0)
        self.assertEqual(refreshed_payload["credits_charged"], 0)
        self.assertEqual(counters["paid_ai"], 0)
        self.assertEqual(counters["niche_generation"], 2)
        self.assertNotEqual(first_payload["keyword"], refreshed_payload["keyword"])

    def test_paid_plan_niche_recommendation_respects_selected_country_and_hourly_refresh_window(self) -> None:
        runtime_store: dict[str, str] = {}
        fake_client = _FakeSupabaseClient(
            {
                "id": "user-3",
                "email": "geo@example.com",
                "token": "token-geo",
                "niche": "Lead Gen",
                "display_name": "Geo Tester",
                "contact_name": "",
                "account_type": "entrepreneur",
                "credits_balance": 7000,
                "topup_credits_balance": 0,
                "monthly_quota": 7000,
                "monthly_limit": 7000,
                "credits_limit": 7000,
                "subscription_start_date": "2026-04-01T00:00:00+00:00",
                "subscription_active": True,
                "subscription_status": "active",
                "subscription_cancel_at": "",
                "subscription_cancel_at_period_end": False,
                "plan_key": "growth",
                "stripe_customer_id": "cus_geo",
                "updated_at": "",
            }
        )

        calls: list[str] = []

        def _fake_get_runtime_value(_db_path, key):
            return runtime_store.get(str(key))

        def _fake_set_runtime_value(_db_path, key, value):
            runtime_store[str(key)] = str(value)

        def _fake_get_niche(*_args, **kwargs):
            country_code = str(kwargs.get("country_code") or "US").upper()
            calls.append(country_code)
            if country_code == "DE":
                return {
                    "keyword": "Solar in Berlin, DE",
                    "location": "Berlin, DE",
                    "country_code": "DE",
                    "expected_reply_rate": 8.4,
                    "recommendations": [{"keyword": "Solar in Berlin, DE", "location": "Berlin, DE", "country_code": "DE", "reason": "German market fit.", "expected_reply_rate": 8.4}],
                    "top_pick": {"keyword": "Solar in Berlin, DE", "location": "Berlin, DE", "country_code": "DE", "reason": "German market fit.", "expected_reply_rate": 8.4},
                    "generated_at": app_module.utc_now_iso(),
                    "performance_snapshot": [],
                }
            return {
                "keyword": "Roofers in Miami, US",
                "location": "Miami, US",
                "country_code": "US",
                "expected_reply_rate": 7.9,
                "recommendations": [{"keyword": "Roofers in Miami, US", "location": "Miami, US", "country_code": "US", "reason": "US market fit.", "expected_reply_rate": 7.9}],
                "top_pick": {"keyword": "Roofers in Miami, US", "location": "Miami, US", "country_code": "US", "reason": "US market fit.", "expected_reply_rate": 7.9},
                "generated_at": app_module.utc_now_iso(),
                "performance_snapshot": [],
            }

        app_module._NICHE_REC_CACHE.clear()

        with (
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: True),
            patch.object(app_module, "ensure_supabase_users_table", lambda *_args, **_kwargs: True),
            patch.object(app_module, "get_supabase_client", lambda *_args, **_kwargs: fake_client),
            patch.object(app_module, "get_runtime_value", _fake_get_runtime_value),
            patch.object(app_module, "set_runtime_value", _fake_set_runtime_value),
            patch.object(app_module, "get_niche_recommendation", _fake_get_niche),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
        ):
            app = app_module.create_app()
            with TestClient(app) as client:
                response_de = client.get("/api/recommend-niche?country=DE", headers={"Authorization": "Bearer token-geo"})
                response_us = client.get("/api/recommend-niche?country=US", headers={"Authorization": "Bearer token-geo"})

        self.assertEqual(response_de.status_code, 200, response_de.text)
        self.assertEqual(response_us.status_code, 200, response_us.text)
        payload_de = response_de.json()
        payload_us = response_us.json()
        self.assertEqual(payload_de["selected_country_code"], "DE")
        self.assertEqual(payload_us["selected_country_code"], "US")
        self.assertEqual(payload_de["refresh_window_hours"], 1.0)
        self.assertIn("Solar in Berlin, DE", payload_de["keyword"])
        self.assertIn("Roofers in Miami, US", payload_us["keyword"])
        self.assertEqual(calls, ["DE", "US"])


if __name__ == "__main__":
    unittest.main()
