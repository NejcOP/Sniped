#!/usr/bin/env python3
"""Execute missing columns patch for quickstart_completed."""

import sys
from pathlib import Path

sys.path.insert(0, "backend")

from app import DEFAULT_CONFIG_PATH, get_supabase_client


def main() -> int:
    client = get_supabase_client(DEFAULT_CONFIG_PATH)
    if client is None:
        print("ERROR: Supabase env/config missing or unreachable!")
        return 1

    sql_patch = Path("supabase_quickstart_completed_patch.sql").read_text(encoding="utf-8")

    print("Executing quickstart_completed column patch...")
    try:
        client.postgrest.rpc("exec_sql", {"sql": sql_patch}).execute()
        print("Patch executed successfully.")
        return 0
    except Exception:
        print("Trying direct verification...")
        try:
            client.table("users").select("id,quickstart_completed").limit(1).execute()
            print("Column appears to exist now.")
            return 0
        except Exception as exc:
            print(f"Column check failed: {exc}")
            print("\nManual patch required. Run this in Supabase SQL Editor:")
            print("=" * 60)
            print(sql_patch)
            print("=" * 60)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
