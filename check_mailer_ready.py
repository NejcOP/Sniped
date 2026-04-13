import sys, sqlite3, json
sys.path.insert(0, "backend")
from app import DEFAULT_DB_PATH, DEFAULT_CONFIG_PATH

with open(DEFAULT_CONFIG_PATH) as f:
    cfg = json.load(f)

accounts = cfg.get("smtp_accounts", [])
for acc in accounts:
    print("Account:", acc.get("email"), "| Server:", acc.get("server"), acc.get("port"), "| Has password:", bool(acc.get("password")))

with sqlite3.connect(str(DEFAULT_DB_PATH)) as conn:
    row = conn.execute("SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status,''))='queued_mail' AND email IS NOT NULL AND email != ''").fetchone()
    print("Queued leads with email:", row[0])

    lead = conn.execute("SELECT id, business_name, email, ai_score FROM leads WHERE LOWER(COALESCE(status,''))='queued_mail' AND email IS NOT NULL AND email != '' LIMIT 1").fetchone()
    print("First lead to mail:", lead)
