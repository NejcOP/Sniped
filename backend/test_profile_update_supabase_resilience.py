import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeSelectQuery:
    def __init__(self, row):
        self._row = row

    def eq(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        return _FakeResponse([self._row])


class _FakeUpdateQuery:
    def __init__(self, client, payload):
        self._client = client
        self._payload = dict(payload)

    def eq(self, *_args, **_kwargs):
        return self

    def execute(self):
        if "updated_at" in self._payload and not self._client.seen_missing_updated_at:
            self._client.seen_missing_updated_at = True
            raise Exception(
                "{'message': \"Could not find the 'updated_at' column of 'users' in the schema cache\", 'code': 'PGRST204'}"
            )
        self._client.last_update_payload = dict(self._payload)
        return _FakeResponse([self._client.user_row])


class _FakeUsersTable:
    def __init__(self, client):
        self._client = client

    def select(self, *_args, **_kwargs):
        return _FakeSelectQuery(self._client.user_row)

    def update(self, payload):
        return _FakeUpdateQuery(self._client, payload)


class _FakeSupabaseClient:
    def __init__(self):
        self.user_row = {
            "id": "user-1",
            "email": "user@example.com",
            "niche": "SEO & Content",
            "display_name": "Old Name",
            "contact_name": "Old Contact",
            "account_type": "entrepreneur",
            "average_deal_value": 1000,
            "password_hash": "stored-hash",
            "salt": "stored-salt",
        }
        self.seen_missing_updated_at = False
        self.last_update_payload = None

    def table(self, name):
        assert name == "users"
        return _FakeUsersTable(self)


class ProfileUpdateSupabaseResilienceTests(unittest.TestCase):
    def test_auth_profile_update_retries_without_missing_updated_at_column(self) -> None:
        fake_client = _FakeSupabaseClient()
        app = app_module.create_app()

        with TestClient(app) as client:
            with (
                patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: True),
                patch.object(app_module, "ensure_supabase_users_table", lambda *_args, **_kwargs: True),
                patch.object(app_module, "get_supabase_client", lambda *_args, **_kwargs: fake_client),
            ):
                response = client.put(
                    "/api/auth/profile",
                    json={
                        "token": "valid-token",
                        "display_name": "New Name",
                        "contact_name": "New Contact",
                        "account_type": "agency",
                        "niche": "Web Design & Dev",
                        "average_deal_value": 3200,
                    },
                )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["display_name"], "New Name")
        self.assertEqual(payload["contact_name"], "New Contact")
        self.assertEqual(payload["account_type"], "agency")
        self.assertEqual(payload["niche"], "Web Design & Dev")
        self.assertEqual(float(payload["average_deal_value"]), 3200.0)
        self.assertTrue(fake_client.seen_missing_updated_at)
        self.assertIsNotNone(fake_client.last_update_payload)
        self.assertEqual(float(fake_client.last_update_payload.get("average_deal_value") or 0), 3200.0)
        self.assertNotIn("updated_at", fake_client.last_update_payload)
