import unittest
from unittest.mock import patch
import os

from fastapi.testclient import TestClient

import backend.app as app_module


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeUsersTable:
    def __init__(self, store):
        self.store = store
        self._reset()

    def _reset(self):
        self._operation = None
        self._payload = None
        self._filters = {}
        self._limit = None
        return self

    def select(self, _fields):
        self._operation = "select"
        return self

    def insert(self, payload):
        self._operation = "insert"
        self._payload = dict(payload)
        return self

    def update(self, payload):
        self._operation = "update"
        self._payload = dict(payload)
        return self

    def eq(self, field, value):
        self._filters[field] = value
        return self

    def limit(self, value):
        self._limit = int(value)
        return self

    def execute(self):
        try:
            if self._operation == "select":
                rows = [row for row in self.store if all(row.get(key) == value for key, value in self._filters.items())]
                if self._limit is not None:
                    rows = rows[: self._limit]
                return _FakeResponse(rows)

            if self._operation == "insert":
                self.store.append(dict(self._payload or {}))
                return _FakeResponse([dict(self._payload or {})])

            if self._operation == "update":
                updated = []
                for row in self.store:
                    if all(row.get(key) == value for key, value in self._filters.items()):
                        row.update(self._payload or {})
                        updated.append(dict(row))
                return _FakeResponse(updated)

            raise AssertionError(f"Unsupported fake operation: {self._operation}")
        finally:
            self._reset()


class _FakeSupabaseClient:
    def __init__(self):
        self.users = []

    def table(self, name):
        if name != "users":
            raise AssertionError(f"Unexpected table requested: {name}")
        return _FakeUsersTable(self.users)


class AuthRegistrationTests(unittest.TestCase):
    def test_check_email_reports_duplicate_and_register_blocks_reuse(self) -> None:
        fake_supabase = _FakeSupabaseClient()

        with (
            patch.object(app_module, "is_supabase_auth_enabled", lambda *_args, **_kwargs: True),
            patch.object(app_module, "ensure_supabase_users_table", lambda *_args, **_kwargs: True),
            patch.object(app_module, "get_supabase_client", lambda *_args, **_kwargs: fake_supabase),
            patch.object(app_module, "load_supabase_settings", lambda *_args, **_kwargs: {"enabled": True}),
            patch.object(app_module, "start_scheduler", lambda *_args, **_kwargs: None),
            patch.object(app_module, "stop_scheduler", lambda *_args, **_kwargs: None),
            patch.dict(os.environ, {"SUPABASE_URL": "https://example.supabase.co", "OPENAI_API_KEY": "test-key", "SUPABASE_KEY": "test-service-key"}, clear=False),
        ):
            app = app_module.create_app()
            with TestClient(app) as client:
                available_response = client.get(
                    "/api/auth/check-email",
                    params={"email": "dupe@example.com"},
                )
                self.assertEqual(available_response.status_code, 200, available_response.text)
                self.assertTrue(available_response.json()["available"])

                register_response = client.post(
                    "/api/auth/register",
                    json={
                        "email": "dupe@example.com",
                        "password": "password123",
                        "niche": "B2B Service Provider",
                    },
                )
                self.assertEqual(register_response.status_code, 200, register_response.text)

                duplicate_check_response = client.get(
                    "/api/auth/check-email",
                    params={"email": "dupe@example.com"},
                )
                self.assertEqual(duplicate_check_response.status_code, 200, duplicate_check_response.text)
                duplicate_check_payload = duplicate_check_response.json()
                self.assertFalse(duplicate_check_payload["available"])
                self.assertEqual(
                    duplicate_check_payload["detail"],
                    "An account with this email already exists.",
                )

                duplicate_register_response = client.post(
                    "/api/auth/register",
                    json={
                        "email": "dupe@example.com",
                        "password": "password123",
                        "niche": "B2B Service Provider",
                    },
                )
                self.assertEqual(duplicate_register_response.status_code, 409, duplicate_register_response.text)
                self.assertEqual(
                    duplicate_register_response.json()["detail"],
                    "An account with this email already exists.",
                )

        self.assertEqual(len(fake_supabase.users), 1)


if __name__ == "__main__":
    unittest.main()