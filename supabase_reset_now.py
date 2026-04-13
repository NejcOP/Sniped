#!/usr/bin/env python3
"""Immediate Supabase data reset (keeps schema)."""

from __future__ import annotations

import json
from pathlib import Path

from supabase import create_client

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"


def load_client():
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    sb = cfg.get("supabase", {}) if isinstance(cfg, dict) else {}
    url = str(sb.get("url", "") or "").strip()
    key = str(sb.get("service_role_key", "") or "").strip()
    if not url or not key:
        raise RuntimeError("Missing supabase.url or supabase.service_role_key in config.json")
    return create_client(url, key)


def count_rows(client, table: str) -> int:
    response = client.table(table).select("*", count="exact", head=True).execute()
    return int(response.count or 0)


def wipe_table_by_id(client, table: str) -> None:
    client.table(table).delete().neq("id", 0).execute()


def wipe_table_by_key(client, table: str, key_name: str) -> None:
    client.table(table).delete().neq(key_name, "__never_match__").execute()


def main() -> int:
    client = load_client()

    id_tables = [
        "worker_audit_log",
        "delivery_tasks",
        "system_tasks",
        "revenue_log",
        "workers",
        "lead_blacklist",
        "leads",
    ]

    key_tables = [
        ("system_runtime", "key"),
        ("mailer_meta", "key"),
    ]

    print("[supabase_reset_now] Deleting rows...")

    for table in id_tables:
        try:
            wipe_table_by_id(client, table)
            print(f"- cleared: {table}")
        except Exception as exc:
            print(f"- skipped: {table} ({exc})")

    for table, key_name in key_tables:
        try:
            wipe_table_by_key(client, table, key_name)
            print(f"- cleared: {table}")
        except Exception as exc:
            print(f"- skipped: {table} ({exc})")

    print("\n[supabase_reset_now] Verification counts:")
    for table in id_tables + [t for t, _ in key_tables]:
        try:
            c = count_rows(client, table)
            print(f"- {table}: {c}")
        except Exception as exc:
            print(f"- {table}: verify failed ({exc})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
