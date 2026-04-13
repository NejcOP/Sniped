import sys, sqlite3, datetime
sys.path.insert(0, "backend")
from app import DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH, get_supabase_client

db = str(DEFAULT_DB_PATH)
with sqlite3.connect(db) as conn:
    conn.execute(
        "UPDATE system_tasks SET status=?, error=?, finished_at=? WHERE task_type=? AND status=?",
        ("failed", "Backend restarted - task orphaned", datetime.datetime.utcnow().isoformat(), "mailer", "running")
    )
    conn.commit()
    row = conn.execute("SELECT id, status, error FROM system_tasks WHERE task_type=? ORDER BY id DESC LIMIT 1", ("mailer",)).fetchone()
    print("SQLite mailer task:", row)

sb = get_supabase_client(DEFAULT_CONFIG_PATH)
if sb:
    sb.table("system_tasks").update({
        "status": "failed",
        "error": "Backend restarted - task orphaned"
    }).eq("task_type", "mailer").eq("status", "running").execute()
    print("Supabase updated OK")
else:
    print("No Supabase client")
