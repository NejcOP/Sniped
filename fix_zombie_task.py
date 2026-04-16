import datetime
import sys

from sqlalchemy import text

sys.path.insert(0, "backend")

from scraper.db import get_engine

def main() -> int:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE system_tasks
                SET status = :status,
                    error = :error,
                    finished_at = :finished_at
                WHERE task_type = :task_type AND status = :running_status
                """
            ),
            {
                "status": "failed",
                "error": "Backend restarted - task orphaned",
                "finished_at": datetime.datetime.utcnow().isoformat(),
                "task_type": "mailer",
                "running_status": "running",
            },
        )
        row = conn.execute(text("SELECT id, status, error FROM system_tasks WHERE task_type = :task_type ORDER BY id DESC LIMIT 1"), {"task_type": "mailer"}).mappings().first()
        print("Postgres mailer task:", dict(row) if row else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
