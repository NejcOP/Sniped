import unittest
from threading import Lock
from types import SimpleNamespace
from unittest.mock import patch

import backend.app as app_module
import worker as worker_module


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.executed_sql = []

    def execute(self, statement, params=None):
        self.executed_sql.append(str(statement))
        return _FakeResult(self.rows)


class _FakeBegin:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return _FakeBegin(self.connection)


class WorkerSystemTasksPostgresTests(unittest.TestCase):
    def test_claim_next_tasks_uses_for_update_skip_locked(self) -> None:
        fake_connection = _FakeConnection(
            [
                {
                    "id": 12,
                    "user_id": "user-1",
                    "task_type": "scrape",
                    "request_payload": "{}",
                }
            ]
        )
        fake_engine = _FakeEngine(fake_connection)

        with patch.object(worker_module, "_pg_enabled", return_value=True), patch.object(worker_module, "get_engine", return_value=fake_engine), patch.object(worker_module, "_runtime_upsert"):
            rows = worker_module._claim_next_tasks_postgres(batch=2)

        self.assertEqual(len(rows), 1)
        sql = "\n".join(fake_connection.executed_sql).upper()
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("UPDATE SYSTEM_TASKS", sql)

    def test_enqueue_task_queues_only_in_primary_mode(self) -> None:
        fake_app = SimpleNamespace(state=SimpleNamespace(task_lock=Lock()))

        with patch.object(app_module, "reconcile_orphaned_active_tasks"), patch.object(app_module, "task_is_active", return_value=False), patch.object(app_module, "create_task_record", return_value=77), patch.object(app_module, "launch_detached_task") as launch_mock, patch.object(app_module, "_is_postgres_task_store_enabled", return_value=True):
            response = app_module.enqueue_task(
                fake_app,
                background_tasks=None,
                db_path=app_module.DEFAULT_DB_PATH,
                user_id="user-1",
                task_type="scrape",
                request_payload={"keyword": "roofers"},
            )

        launch_mock.assert_not_called()
        self.assertEqual(response["task_id"], 77)
        self.assertEqual(response["job_status"], "queued")
        self.assertEqual(response["execution_mode"], "worker")


if __name__ == "__main__":
    unittest.main()