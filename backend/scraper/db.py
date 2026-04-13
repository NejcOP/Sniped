import sqlite3
from typing import List, Optional, Sequence

from .models import Lead


def init_db(db_path: str = "leads.db") -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'legacy',
                business_name TEXT NOT NULL,
                website_url TEXT,
                phone_number TEXT,
                rating REAL,
                review_count INTEGER,
                address TEXT NOT NULL DEFAULT '',
                search_keyword TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(business_name, address)
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
        if "user_id" not in columns:
            conn.execute("ALTER TABLE leads ADD COLUMN user_id TEXT NOT NULL DEFAULT 'legacy'")
        if "created_at" not in columns:
            conn.execute("ALTER TABLE leads ADD COLUMN created_at TEXT")
        conn.execute(
            """
            UPDATE leads
            SET user_id = 'legacy'
            WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = ''
            """
        )
        conn.execute(
            """
            UPDATE leads
            SET created_at = COALESCE(NULLIF(scraped_at, ''), CURRENT_TIMESTAMP)
            WHERE created_at IS NULL OR TRIM(COALESCE(created_at, '')) = ''
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_user_created_at ON leads(user_id, created_at DESC, id DESC)")
        conn.commit()


def upsert_lead(lead: Lead, db_path: str = "leads.db", user_id: str = "legacy") -> bool:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO leads (
                user_id,
                business_name,
                website_url,
                phone_number,
                rating,
                review_count,
                address,
                search_keyword
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                lead.business_name,
                lead.website_url,
                lead.phone_number,
                lead.rating,
                lead.review_count,
                lead.address,
                lead.search_keyword,
            ),
        )
        conn.commit()
        return cursor.rowcount > 0


def batch_upsert_leads(leads: Sequence[Lead], db_path: str = "leads.db", user_id: str = "legacy") -> int:
    """Insert multiple leads in a single transaction. Returns the number of newly inserted rows."""
    if not leads:
        return 0
    params = [
        (
            user_id,
            lead.business_name,
            lead.website_url,
            lead.phone_number,
            lead.rating,
            lead.review_count,
            lead.address,
            lead.search_keyword,
        )
        for lead in leads
    ]
    with sqlite3.connect(db_path) as conn:
        cursor = conn.executemany(
            """
            INSERT OR IGNORE INTO leads (
                user_id,
                business_name,
                website_url,
                phone_number,
                rating,
                review_count,
                address,
                search_keyword
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        conn.commit()
    return cursor.rowcount


def fetch_target_leads(db_path: str = "leads.db", min_score: float = 7.0, user_id: Optional[str] = None) -> List[sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        query = """
            SELECT
                business_name,
                website_url,
                phone_number,
                email,
                rating,
                review_count,
                address,
                search_keyword,
                scraped_at,
                COALESCE(main_shortcoming, '') AS main_shortcoming,
                COALESCE(ai_score, 0) AS ai_score,
                COALESCE(status, '') AS status,
                COALESCE(enriched_at, '') AS enriched_at
            FROM leads
            WHERE COALESCE(ai_score, 0) >= ?
        """
        params: list[object] = [min_score]

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        query += " ORDER BY COALESCE(ai_score, 0) DESC, review_count DESC, business_name ASC"
        rows = conn.execute(query, params).fetchall()

    return rows
