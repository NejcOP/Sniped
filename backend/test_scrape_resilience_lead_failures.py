import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import backend.app as app_module
import backend.scraper.db as db_module
from backend.scraper.models import Lead


class _FakeSession:
    def __init__(self) -> None:
        self.bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        self.rollback_count = 0
        self.commit_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        payloads = list(statement.get("payloads") or [])
        if len(payloads) > 1:
            raise RuntimeError("simulated bulk upsert failure")
        payload = payloads[0] if payloads else {}
        if str(payload.get("business_name") or "").strip().lower() == "bad co":
            raise RuntimeError("simulated invalid lead payload")
        return None

    def merge(self, _record):
        return None

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


class _FakePgConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, _params=None):
        return SimpleNamespace(rowcount=0)

    def commit(self):
        return None


class _FakeMapsScraper:
    def __init__(self, *args, **kwargs):
        del args, kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def scrape(self, keyword, max_results, progress_callback=None, max_runtime_seconds=300, stall_timeout_seconds=45):
        del max_runtime_seconds, stall_timeout_seconds
        leads = [
            Lead(
                business_name="Good Co 1",
                website_url="https://good-1.example",
                phone_number="+1-555-1001",
                rating=4.2,
                review_count=12,
                address="Miami, FL",
                search_keyword=keyword,
            ),
            Lead(
                business_name="Bad Co",
                website_url="https://bad.example",
                phone_number="+1-555-1002",
                rating=3.8,
                review_count=4,
                address="Miami, FL",
                search_keyword=keyword,
            ),
            Lead(
                business_name="Good Co 2",
                website_url="https://good-2.example",
                phone_number="+1-555-1003",
                rating=4.7,
                review_count=27,
                address="Miami, FL",
                search_keyword=keyword,
            ),
        ][: int(max_results)]

        if progress_callback is not None:
            scanned = 0
            found = 0
            for lead in leads:
                scanned += 1
                progress_callback(found, int(max_results), scanned, None)
                if lead.business_name != "Bad Co":
                    found += 1
                    progress_callback(found, int(max_results), scanned, lead)
        return leads


class ScrapeResilienceLeadFailureTests(unittest.TestCase):
    def test_batch_upsert_skips_bad_payload_and_continues(self) -> None:
        session = _FakeSession()

        def fake_session_factory():
            return session

        def fake_statement(payloads, _dialect_name):
            return {"payloads": payloads}

        leads = [
            Lead(
                business_name="Good Co 1",
                website_url="https://good-1.example",
                phone_number="+1-555-1001",
                rating=4.1,
                review_count=10,
                address="Miami, FL",
                search_keyword="roofers in miami",
            ),
            Lead(
                business_name="Bad Co",
                website_url="https://bad.example",
                phone_number="+1-555-1002",
                rating=3.2,
                review_count=2,
                address="Miami, FL",
                search_keyword="roofers in miami",
            ),
            Lead(
                business_name="Good Co 2",
                website_url="https://good-2.example",
                phone_number="+1-555-1003",
                rating=4.8,
                review_count=32,
                address="Miami, FL",
                search_keyword="roofers in miami",
            ),
        ]

        with (
            patch.object(db_module, "init_db"),
            patch.object(db_module, "get_session_factory", side_effect=lambda _db_path=None: fake_session_factory),
            patch.object(db_module, "_build_lead_upsert_statement", side_effect=fake_statement),
        ):
            inserted = db_module.batch_upsert_leads(leads, db_path="ignored", user_id="user-1")

        self.assertEqual(inserted, 2)
        self.assertGreaterEqual(session.rollback_count, 1)
        self.assertGreaterEqual(session.commit_count, 2)

    def test_execute_scrape_task_completes_with_partial_insert_and_processing_progress(self) -> None:
        progress_updates: list[dict] = []
        finish_calls: list[dict] = []

        def capture_progress(_db_path, _task_id, payload):
            progress_updates.append(copy.deepcopy(payload))

        def capture_finish(_db_path, _task_id, status, result_payload=None, error=None):
            finish_calls.append(
                {
                    "status": status,
                    "result_payload": copy.deepcopy(result_payload or {}),
                    "error": error,
                }
            )

        payload = {
            "task_id": 501,
            "user_id": "user-1",
            "keyword": "roofers in miami",
            "results": 3,
            "country": "US",
            "headless": True,
            "db_path": "ignored.db",
        }

        with (
            patch.object(app_module, "ensure_scrape_tables"),
            patch.object(app_module.pgdb, "connect", side_effect=lambda *_args, **_kwargs: _FakePgConn()),
            patch.object(app_module, "mark_task_running"),
            patch.object(app_module, "update_task_progress", side_effect=capture_progress),
            patch.object(app_module, "GoogleMapsScraper", _FakeMapsScraper),
            patch.object(app_module, "batch_upsert_leads", return_value=2),
            patch.object(app_module, "is_supabase_primary_enabled", return_value=False),
            patch.object(app_module, "sync_blacklisted_leads", return_value=0),
            patch.object(app_module, "maybe_sync_supabase"),
            patch.object(app_module, "_invalidate_leads_cache"),
            patch.object(app_module, "deduct_credits_on_success", return_value={"credits_charged": 2, "credits_balance": 998, "credits_limit": 1000}),
            patch.object(app_module, "finish_task_record", side_effect=capture_finish),
        ):
            app_module.execute_scrape_task(app_module.app, payload)

        self.assertTrue(progress_updates, "Expected scrape progress updates to be emitted")
        self.assertTrue(
            any(str(update.get("phase") or "").strip().lower() == "processing" for update in progress_updates),
            "Expected at least one processing phase progress update",
        )
        self.assertTrue(
            any(str(update.get("status_message") or "").startswith("Scraped ") for update in progress_updates),
            "Expected Scraped x/y style status messages",
        )

        self.assertTrue(finish_calls, "Expected finish_task_record to be called")
        completed = [call for call in finish_calls if call.get("status") == "completed"]
        self.assertTrue(completed, "Expected scrape task to complete despite partial lead failure")
        final_payload = completed[-1]["result_payload"]
        self.assertEqual(int(final_payload.get("scraped") or 0), 3)
        self.assertEqual(int(final_payload.get("inserted") or 0), 2)


if __name__ == "__main__":
    unittest.main()
