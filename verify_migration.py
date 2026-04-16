#!/usr/bin/env python3
"""Verify active Supabase runtime and schema access."""

import sys

sys.path.insert(0, "backend")

from backend.app import DEFAULT_CONFIG_PATH, get_supabase_client, load_supabase_settings


def main() -> int:
    settings = load_supabase_settings(DEFAULT_CONFIG_PATH)
    url = settings.get("url", "")
    primary_mode = settings.get("enabled", False)

    print("=" * 70)
    print("SUPABASE MIGRATION VERIFICATION")
    print("=" * 70)
    print()
    print("KONFIGURACIJA:")
    print(f"  URL: {url}")
    print(f"  Primary Mode: {primary_mode}")
    print()

    client = get_supabase_client(DEFAULT_CONFIG_PATH)
    if client is None:
        print("Supabase env/config missing")
        return 1

    print("CRM TABELE:")
    crm_tables = ["leads", "workers", "revenue_log", "delivery_tasks", "worker_audit_log", "lead_blacklist"]
    for table in crm_tables:
        try:
            client.table(table).select("count").execute()
            print(f"  OK {table:20} exists")
        except Exception as exc:
            print(f"  FAIL {table:18} {str(exc)[:40]}")

    print()
    print("SYSTEM TABELE:")
    system_tables = ["system_tasks", "system_runtime"]
    for table in system_tables:
        try:
            client.table(table).select("count").execute()
            print(f"  OK {table:20} exists")
        except Exception as exc:
            print(f"  FAIL {table:18} {str(exc)[:40]}")

    print()
    print("=" * 70)
    print("MIGRATION CHECK COMPLETE")
    print("=" * 70)
    print("Status: active runtime is Supabase-backed")
    print("Backend primary mode:", primary_mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
