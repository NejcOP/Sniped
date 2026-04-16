#!/usr/bin/env python3
"""Check current Postgres/Supabase table contents."""

from sqlalchemy import text

from backend.scraper.db import get_engine


def main() -> int:
    engine = get_engine()
    with engine.begin() as conn:
        tables = [
            row["table_name"]
            for row in conn.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name ASC
                    """
                )
            ).mappings()
        ]

        print("TABELE V SUPABASE/POSTGRES:")
        print("=" * 60)
        for table_name in tables:
            total = conn.execute(text(f'SELECT COUNT(*) AS total FROM "{table_name}"')).scalar_one()
            print(f"  - {table_name}: {total} zapisov")

    print("\n" + "=" * 60)
    print("Aktivni storage je Supabase/Postgres.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
