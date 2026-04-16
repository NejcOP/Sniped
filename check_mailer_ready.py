import sys

from sqlalchemy import text

sys.path.insert(0, "backend")

from app import DEFAULT_DB_PATH, load_user_smtp_accounts
from scraper.db import get_engine

def main() -> int:
    user_id = sys.argv[1] if len(sys.argv) > 1 else None
    accounts = load_user_smtp_accounts(user_id=user_id, db_path=DEFAULT_DB_PATH)
    for acc in accounts:
        print("Account:", acc.get("email"), "| Server:", acc.get("server"), acc.get("port"), "| Has password:", bool(acc.get("password")))

    engine = get_engine(str(DEFAULT_DB_PATH))
    with engine.begin() as conn:
        row = conn.execute(text("SELECT COUNT(*) AS total FROM leads WHERE LOWER(COALESCE(status,''))='queued_mail' AND email IS NOT NULL AND email != ''")).mappings().first()
        print("Queued leads with email:", int((row or {}).get("total") or 0))

        lead = conn.execute(text("SELECT id, business_name, email, ai_score FROM leads WHERE LOWER(COALESCE(status,''))='queued_mail' AND email IS NOT NULL AND email != '' ORDER BY id ASC LIMIT 1")).mappings().first()
        print("First lead to mail:", dict(lead) if lead else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
