import hashlib
import hmac
import io
import json
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs

import pgdb
from fastapi.testclient import TestClient

import backend.app as app_module


class _FakeUrlopenResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class StripeSubscriptionCheckoutTests(unittest.TestCase):
    def _make_email(self, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:8]}@example.com"

    def _make_token(self, prefix: str) -> str:
        return f"token-{prefix}-{uuid.uuid4().hex[:8]}"

    def test_create_subscription_session_uses_expected_price_id(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_checkout_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_checkout.db"
        captured: dict[str, str] = {}
        user_email = self._make_email("growth")
        user_id = int(uuid.uuid4().hex[:6], 16)

        def _fake_urlopen(req, timeout=0):
            captured["url"] = req.full_url
            captured["body"] = req.data.decode("utf-8")
            captured["auth"] = req.headers.get("Authorization", "")
            return _FakeUrlopenResponse({"url": "https://checkout.stripe.test/session_123"})

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                token = self._make_token("growth")
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            50,
                            50,
                            50,
                            50,
                            "free",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch("urllib.request.urlopen", _fake_urlopen),
                    patch.dict(app_module.os.environ, {"STRIPE_SECRET_KEY": "sk_test_123"}, clear=False),
                ):
                    app = app_module.create_app()
                    with TestClient(app, base_url="http://localhost:8000") as client:
                        response = client.post(
                            "/api/stripe/create-subscription-session",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"plan_id": "growth"},
                        )

                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(payload["plan_id"], "growth")
                self.assertEqual(payload["credits"], 7000)
                self.assertEqual(payload["url"], "https://checkout.stripe.test/session_123")

                encoded = parse_qs(captured["body"])
                self.assertEqual(encoded.get("mode"), ["subscription"])
                self.assertEqual(encoded.get("line_items[0][price]"), ["price_1TV8fzIHcumhGMC4MDVaUcBx"])
                self.assertEqual(encoded.get("metadata[user_id]"), [str(user_id)])
                self.assertEqual(encoded.get("metadata[plan_key]"), ["growth"])
                self.assertEqual(encoded.get("metadata[monthly_limit]"), ["7000"])
                self.assertEqual(encoded.get("subscription_data[metadata][plan_key]"), ["growth"])
                self.assertEqual(encoded.get("subscription_data[metadata][monthly_limit]"), ["7000"])
                self.assertIn("payment=success", encoded.get("success_url", [""])[0])
                self.assertIn("payment=cancelled", encoded.get("cancel_url", [""])[0])
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

    def test_create_subscription_session_surfaces_stripe_http_error_details(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_checkout_error_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_checkout_error.db"
        user_email = self._make_email("growth-error")
        token = self._make_token("growth-error")
        user_id = int(uuid.uuid4().hex[:6], 16)

        def _fake_urlopen(_req, timeout=0):
            raise HTTPError(
                url="https://api.stripe.com/v1/checkout/sessions",
                code=400,
                msg="Bad Request",
                hdrs=None,
                fp=io.BytesIO(
                    json.dumps({
                        "error": {
                            "type": "invalid_request_error",
                            "message": "No such price: 'price_invalid_123'",
                            "param": "line_items[0][price]",
                        }
                    }).encode("utf-8")
                ),
            )

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            50,
                            50,
                            50,
                            50,
                            "free",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch("urllib.request.urlopen", _fake_urlopen),
                    patch.dict(app_module.os.environ, {"STRIPE_SECRET_KEY": "sk_test_123"}, clear=False),
                ):
                    app = app_module.create_app()
                    with TestClient(app, base_url="http://localhost:8000") as client:
                        response = client.post(
                            "/api/stripe/create-subscription-session",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"plan_id": "growth"},
                        )

                self.assertEqual(response.status_code, 502, response.text)
                self.assertIn("Could not create Stripe subscription checkout session.", response.text)
                self.assertIn("No such price: 'price_invalid_123'", response.text)
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

    def test_create_topup_session_uses_expected_price_id(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_topup_checkout_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_topup_checkout.db"
        captured: dict[str, str] = {}
        user_email = self._make_email("topup")
        token = self._make_token("topup")
        user_id = int(uuid.uuid4().hex[:6], 16)

        def _fake_urlopen(req, timeout=0):
            captured["url"] = req.full_url
            captured["body"] = req.data.decode("utf-8")
            captured["auth"] = req.headers.get("Authorization", "")
            return _FakeUrlopenResponse({"url": "https://checkout.stripe.test/topup_123"})

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            50,
                            50,
                            50,
                            50,
                            "free",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch("urllib.request.urlopen", _fake_urlopen),
                    patch.dict(app_module.os.environ, {"STRIPE_SECRET_KEY": "sk_test_123"}, clear=False),
                ):
                    app = app_module.create_app()
                    with TestClient(app, base_url="http://localhost:8000") as client:
                        response = client.post(
                            "/api/stripe/create-topup-session",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"package_id": "credits_1000"},
                        )

                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(payload["package_id"], "credits_1000")
                self.assertEqual(payload["credits"], 1000)
                self.assertEqual(payload["url"], "https://checkout.stripe.test/topup_123")

                encoded = parse_qs(captured["body"])
                self.assertEqual(encoded.get("mode"), ["payment"])
                self.assertEqual(encoded.get("line_items[0][price]"), ["price_1TV8i8IHcumhGMC4mW4LYWvN"])
                self.assertEqual(encoded.get("metadata[user_id]"), [str(user_id)])
                self.assertEqual(encoded.get("metadata[package_id]"), ["credits_1000"])
                self.assertEqual(encoded.get("metadata[credits_added]"), ["1000"])
                success_url = encoded.get("success_url", [""])[0]
                self.assertIn("payment=success", success_url)
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

    def test_create_subscription_session_reads_secret_from_config_json(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_config_checkout_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_config_checkout.db"
        config_path = temp_dir / "config.json"
        config_path.write_text(json.dumps({"stripe": {"secret_key": "sk_test_from_config"}}), encoding="utf-8")
        captured: dict[str, str] = {}
        user_email = self._make_email("config")
        token = self._make_token("config")
        user_id = int(uuid.uuid4().hex[:6], 16)

        def _fake_urlopen(req, timeout=0):
            captured["auth"] = req.headers.get("Authorization", "")
            return _FakeUrlopenResponse({"url": "https://checkout.stripe.test/session_cfg"})

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            50,
                            50,
                            50,
                            50,
                            "free",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "DEFAULT_CONFIG_PATH", config_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch("urllib.request.urlopen", _fake_urlopen),
                    patch.dict(app_module.os.environ, {"STRIPE_SECRET_KEY": "", "SNIPED_STRIPE_SECRET_KEY": ""}, clear=False),
                ):
                    app = app_module.create_app()
                    with TestClient(app) as client:
                        response = client.post(
                            "/api/stripe/create-subscription-session",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"plan_id": "growth"},
                        )

                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(captured["auth"], "Bearer sk_test_from_config")
        finally:
            try:
                if db_path.exists():
                    db_path.unlink()
            except Exception:
                pass
            try:
                if config_path.exists():
                    config_path.unlink()
            except Exception:
                pass
            try:
                if temp_dir.exists():
                    temp_dir.rmdir()
            except Exception:
                pass

    def test_auth_profile_recovers_paid_plan_from_stripe_when_local_state_is_stale(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_profile_sync_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_profile_sync.db"
        config_path = temp_dir / "config.json"
        config_path.write_text(json.dumps({"stripe": {"secret_key": "sk_test_from_config"}}), encoding="utf-8")
        user_email = self._make_email("recover")
        token = self._make_token("recover")
        user_id = int(uuid.uuid4().hex[:6], 16)

        def _fake_urlopen(req, timeout=0):
            if "https://api.stripe.com/v1/customers" in req.full_url:
                return _FakeUrlopenResponse({"data": [{"id": "cus_growth_123", "email": user_email}]})
            if "https://api.stripe.com/v1/subscriptions" in req.full_url:
                return _FakeUrlopenResponse({
                    "data": [
                        {
                            "id": "sub_growth_123",
                            "status": "active",
                            "cancel_at_period_end": False,
                            "cancel_at": None,
                            "current_period_end": int(time.time()) + 86400 * 20,
                            "items": {"data": [{"price": {"id": "price_1TV8fzIHcumhGMC4MDVaUcBx"}}]},
                        }
                    ]
                })
            raise AssertionError(f"Unexpected Stripe URL: {req.full_url}")

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            stripe_customer_id, subscription_active, plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            0,
                            50,
                            50,
                            50,
                            "cus_growth_123",
                            False,
                            "free",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "DEFAULT_CONFIG_PATH", config_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch("urllib.request.urlopen", _fake_urlopen),
                    patch.dict(app_module.os.environ, {"STRIPE_SECRET_KEY": "", "SNIPED_STRIPE_SECRET_KEY": ""}, clear=False),
                ):
                    app = app_module.create_app()
                    with TestClient(app) as client:
                        response = client.post("/api/auth/profile", json={"token": token})

                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(payload["plan_key"], "growth")
                self.assertTrue(payload["isSubscribed"])
                self.assertEqual(payload["currentPlanName"], "The Growth")
                self.assertEqual(int(payload["monthly_quota"]), 7000)
                self.assertEqual(int(payload["credits_balance"]), 7000)
        finally:
            try:
                if db_path.exists():
                    db_path.unlink()
            except Exception:
                pass
            try:
                if config_path.exists():
                    config_path.unlink()
            except Exception:
                pass
            try:
                if temp_dir.exists():
                    temp_dir.rmdir()
            except Exception:
                pass

    def test_auth_profile_recovers_missing_topup_credits_when_webhook_was_missed(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_topup_profile_sync_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_topup_profile_sync.db"
        config_path = temp_dir / "config.json"
        config_path.write_text(json.dumps({"stripe": {"secret_key": "sk_test_from_config"}}), encoding="utf-8")
        user_email = self._make_email("recover-topup")
        token = self._make_token("recover-topup")
        user_id = int(uuid.uuid4().hex[:6], 16)
        payment_intent_id = f"pi_topup_{uuid.uuid4().hex[:10]}"

        def _fake_urlopen(req, timeout=0):
            if "https://api.stripe.com/v1/payment_intents" in req.full_url:
                return _FakeUrlopenResponse({
                    "data": [
                        {
                            "id": payment_intent_id,
                            "status": "succeeded",
                            "created": int(time.time()),
                            "metadata": {
                                "payment_kind": "topup",
                                "credits_added": "1000",
                                "email": user_email,
                                "user_id": str(user_id),
                                "stripe_price_id": "price_1TV8i8IHcumhGMC4mW4LYWvN",
                            },
                        }
                    ]
                })
            raise AssertionError(f"Unexpected Stripe URL: {req.full_url}")

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            topup_credits_balance, stripe_customer_id, subscription_active, plan_key, updated_at, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            6999,
                            7000,
                            7000,
                            7000,
                            0,
                            "cus_growth_123",
                            True,
                            "growth",
                            "2026-04-07T17:09:23.361010+00:00",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "DEFAULT_CONFIG_PATH", config_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch("urllib.request.urlopen", _fake_urlopen),
                    patch.dict(app_module.os.environ, {"STRIPE_SECRET_KEY": "", "SNIPED_STRIPE_SECRET_KEY": ""}, clear=False),
                ):
                    app = app_module.create_app()
                    with TestClient(app) as client:
                        response = client.post("/api/auth/profile", json={"token": token})

                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(int(payload["credits_balance"]), 7999)
                self.assertEqual(int(payload["topup_credits_balance"]), 1000)
                self.assertEqual(payload["plan_key"], "growth")
        finally:
            try:
                if db_path.exists():
                    db_path.unlink()
            except Exception:
                pass
            try:
                if config_path.exists():
                    config_path.unlink()
            except Exception:
                pass
            try:
                if temp_dir.exists():
                    temp_dir.rmdir()
            except Exception:
                pass

    def test_webhook_topup_adds_credits_without_changing_subscription_tier(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_topup_webhook_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_topup_webhook.db"
        user_email = self._make_email("growth-topup")
        token = self._make_token("growth-topup")
        user_id = int(uuid.uuid4().hex[:6], 16)

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            topup_credits_balance, stripe_customer_id, subscription_active, plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            7000,
                            7000,
                            7000,
                            7000,
                            0,
                            "cus_growth_topup_123",
                            True,
                            "growth",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                event = {
                    "type": "checkout.session.completed",
                    "data": {
                        "object": {
                            "mode": "payment",
                            "customer": "cus_growth_topup_123",
                            "metadata": {
                                "user_id": str(user_id),
                                "email": user_email,
                                "package_id": "credits_25000",
                                "credits_added": "25000",
                            },
                        }
                    },
                }

                payload = json.dumps(event)
                webhook_secret = "whsec_test_signature"
                timestamp = str(int(time.time()))
                signature = hmac.new(
                    webhook_secret.encode("utf-8"),
                    f"{timestamp}.{payload}".encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "get_stripe_webhook_secret", lambda *_args, **_kwargs: webhook_secret),
                ):
                    app = app_module.create_app()
                    with TestClient(app) as client:
                        response = client.post(
                            "/api/stripe/webhook",
                            content=payload,
                            headers={
                                "Content-Type": "application/json",
                                "Stripe-Signature": f"t={timestamp},v1={signature}",
                            },
                        )

                self.assertEqual(response.status_code, 200, response.text)
                with pgdb.connect(db_path) as conn:
                    conn.row_factory = pgdb.Row
                    row = conn.execute(
                        "SELECT credits_balance, topup_credits_balance, monthly_quota, monthly_limit, credits_limit, subscription_active, plan_key FROM users WHERE id = ?",
                        (user_id,),
                    ).fetchone()

                self.assertIsNotNone(row)
                self.assertEqual(int(row["credits_balance"]), 32000)
                self.assertEqual(int(row["topup_credits_balance"]), 25000)
                self.assertEqual(int(row["monthly_quota"]), 7000)
                self.assertEqual(int(row["monthly_limit"]), 7000)
                self.assertEqual(int(row["credits_limit"]), 7000)
                self.assertTrue(bool(row["subscription_active"]))
                self.assertEqual(str(row["plan_key"]), "growth")
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

    def test_webhook_applies_monthly_credits_for_subscription_price(self) -> None:
        temp_dir = Path(tempfile.gettempdir()) / f"sniped_stripe_webhook_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / "stripe_webhook.db"
        user_email = self._make_email("scale")
        token = self._make_token("scale")
        user_id = int(uuid.uuid4().hex[:6], 16)

        try:
            with patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: False):
                app_module.ensure_users_table(db_path)
                with pgdb.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO users (
                            id, email, password_hash, salt, niche, token,
                            credits_balance, monthly_quota, monthly_limit, credits_limit,
                            stripe_customer_id, subscription_active, plan_key, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            user_email,
                            "hash",
                            "salt",
                            "Web Design & Dev",
                            token,
                            12,
                            50,
                            50,
                            50,
                            "cus_scale_123",
                            False,
                            "free",
                            app_module.utc_now_iso(),
                        ),
                    )
                    conn.commit()

                event = {
                    "type": "invoice.payment_succeeded",
                    "data": {
                        "object": {
                            "customer": "cus_scale_123",
                            "billing_reason": "subscription_create",
                            "status": "paid",
                            "metadata": {
                                "user_id": str(user_id),
                                "monthly_limit": "20000",
                            },
                            "lines": {
                                "data": [
                                    {
                                        "price": {
                                            "id": "price_1TV8gOIHcumhGMC4WZZqHo78"
                                        }
                                    }
                                ]
                            },
                        }
                    },
                }

                payload = json.dumps(event)
                webhook_secret = "whsec_test_signature"
                timestamp = str(int(time.time()))
                signature = hmac.new(
                    webhook_secret.encode("utf-8"),
                    f"{timestamp}.{payload}".encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()

                with (
                    patch.object(app_module, "DEFAULT_DB_PATH", db_path),
                    patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
                    patch.object(app_module, "get_stripe_webhook_secret", lambda *_args, **_kwargs: webhook_secret),
                ):
                    app = app_module.create_app()
                    with TestClient(app) as client:
                        response = client.post(
                            "/api/stripe/webhook",
                            content=payload,
                            headers={
                                "Content-Type": "application/json",
                                "Stripe-Signature": f"t={timestamp},v1={signature}",
                            },
                        )

                self.assertEqual(response.status_code, 200, response.text)
                with pgdb.connect(db_path) as conn:
                    conn.row_factory = pgdb.Row
                    row = conn.execute(
                        "SELECT credits_balance, monthly_quota, monthly_limit, credits_limit, subscription_active, plan_key FROM users WHERE id = ?",
                        (user_id,),
                    ).fetchone()

                self.assertIsNotNone(row)
                self.assertEqual(int(row["credits_balance"]), 20000)
                self.assertEqual(int(row["monthly_quota"]), 20000)
                self.assertEqual(int(row["monthly_limit"]), 20000)
                self.assertEqual(int(row["credits_limit"]), 20000)
                self.assertTrue(bool(row["subscription_active"]))
                self.assertEqual(str(row["plan_key"]), "scale")
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
    unittest.main()
