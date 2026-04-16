#!/usr/bin/env python3
"""Inspect Supabase schema readiness and print the chosen SQL patch if manual execution is still needed."""

import sys
from pathlib import Path

sys.path.insert(0, "backend")

from backend.app import DEFAULT_CONFIG_PATH, get_supabase_client

PATCH_FILE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("supabase_missing_columns_patch.sql")


def main() -> int:
    client = get_supabase_client(DEFAULT_CONFIG_PATH)
    if client is None:
        print("ERROR: Supabase env/config missing!")
        return 1
    if not PATCH_FILE.exists():
        print(f"ERROR: Patch file not found: {PATCH_FILE}")
        return 1

    sql_patch = PATCH_FILE.read_text(encoding="utf-8")
    print(f"Patch file: {PATCH_FILE}")
    print("Checking mailer campaign schema readiness...\n")

    checks = [
        (
            "leads campaign columns",
            lambda: client.table("leads").select(
                "id,campaign_sequence_id,campaign_step,ab_variant,last_subject_line,reply_detected_at,bounced_at,bounce_reason"
            ).limit(1).execute(),
        ),
        ("CampaignSequences table", lambda: client.table("CampaignSequences").select("id").limit(1).execute()),
        ("SavedTemplates table", lambda: client.table("SavedTemplates").select("id").limit(1).execute()),
        ("CampaignEvents table", lambda: client.table("CampaignEvents").select("id").limit(1).execute()),
    ]

    missing_items = []
    for label, probe in checks:
        try:
            probe()
            print(f"OK {label} already available")
        except Exception as exc:
            missing_items.append((label, str(exc)))
            print(f"MISSING {label}: {exc}")

    print()
    if missing_items:
        print("Run this patch in Supabase SQL Editor:")
        print("=" * 60)
        print(sql_patch)
        print("=" * 60)
        return 1

    print("Supabase already has the required mailer campaign schema.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
