import sqlite3
from typing import List

from .models import Lead


def init_db(db_path: str = "leads.db") -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_name TEXT NOT NULL,
                website_url TEXT,
                phone_number TEXT,
                rating REAL,
                review_count INTEGER,
                address TEXT NOT NULL,
                search_keyword TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(business_name, address)
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
        if "created_at" not in columns:
            conn.execute("ALTER TABLE leads ADD COLUMN created_at TEXT")
            conn.execute(
                """
                UPDATE leads
                SET created_at = COALESCE(NULLIF(scraped_at, ''), CURRENT_TIMESTAMP)
                WHERE created_at IS NULL OR TRIM(COALESCE(created_at, '')) = ''
                """
            )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at DESC, id DESC)")
        conn.commit()


def upsert_lead(lead: Lead, db_path: str = "leads.db") -> bool:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO leads (
                business_name,
                website_url,
                phone_number,
                rating,
                review_count,
                address,
                search_keyword
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
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


def fetch_target_leads(db_path: str = "leads.db", min_rating: float = 3.5) -> List[sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                business_name,
                website_url,
                phone_number,
                rating,
                review_count,
                address,
                search_keyword,
                scraped_at
            FROM leads
            WHERE
                website_url IS NULL
                OR website_url = ''
                OR LOWER(website_url) = 'none'
                OR rating IS NULL
                OR rating < ?
            ORDER BY rating ASC, review_count ASC
            """,
            (min_rating,),
        ).fetchall()

    return rows
