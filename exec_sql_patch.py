#!/usr/bin/env python3
"""Direct SQL executor for Supabase patches."""

import sys
from pathlib import Path

sys.path.insert(0, "backend")

from app import DEFAULT_CONFIG_PATH, get_supabase_client


def main() -> int:
    client = get_supabase_client(DEFAULT_CONFIG_PATH)
    if client is None:
        print("ERROR: Supabase env/config missing!")
        return 1

    sql_patch = Path("supabase_quickstart_completed_patch.sql").read_text(encoding="utf-8")
    print("Executing SQL patch...")
    try:
        for statement in sql_patch.strip().split(";"):
            statement = statement.strip()
            if not statement or statement.startswith("--"):
                continue
            print(f"Executing: {statement[:60]}...")
            client.postgrest.rpc("exec_sql", {"sql": statement}).execute()
        print("SQL patch executed successfully.")
        return 0
    except Exception as exc:
        print(f"Error executing patch: {exc}")
        print("\nPlease execute this SQL manually in Supabase SQL Editor:")
        print("=" * 60)
        print(sql_patch)
        print("=" * 60)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
