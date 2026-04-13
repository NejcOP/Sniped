#!/usr/bin/env python3
"""Inspect Supabase schema readiness and print the chosen SQL patch if manual execution is still needed."""
import json
import sys
from pathlib import Path

from supabase import create_client

PATCH_FILE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('supabase_missing_columns_patch.sql')

config = json.loads(Path('config.json').read_text(encoding='utf-8'))
sb_config = config.get('supabase', {})
url = sb_config.get('url', '')
key = sb_config.get('service_role_key', '')

if not url or not key:
    print('ERROR: Supabase config missing!')
    raise SystemExit(1)

if not PATCH_FILE.exists():
    print(f'ERROR: Patch file not found: {PATCH_FILE}')
    raise SystemExit(1)

sql_patch = PATCH_FILE.read_text(encoding='utf-8')
client = create_client(url, key)

print(f'Patch file: {PATCH_FILE}')
print('Checking mailer campaign schema readiness...')
print()

checks = [
    (
        'leads campaign columns',
        lambda: client.table('leads').select(
            'id,campaign_sequence_id,campaign_step,ab_variant,last_subject_line,reply_detected_at,bounced_at,bounce_reason'
        ).limit(1).execute(),
    ),
    ('CampaignSequences table', lambda: client.table('CampaignSequences').select('id').limit(1).execute()),
    ('SavedTemplates table', lambda: client.table('SavedTemplates').select('id').limit(1).execute()),
    ('CampaignEvents table', lambda: client.table('CampaignEvents').select('id').limit(1).execute()),
]

missing_items = []
for label, probe in checks:
    try:
        probe()
        print(f'✓ {label} already available')
    except Exception as exc:
        message = str(exc)
        missing_items.append((label, message))
        print(f'✗ {label} missing or out of sync: {message}')

print()
if missing_items:
    print('Run this patch in Supabase SQL Editor:')
    print('1. Open: https://supabase.com/dashboard/project/dwxunoinmgdqftvnaziz/sql/new')
    print(f'2. Copy the full contents of: {PATCH_FILE}')
    print("3. Paste it into SQL Editor and click 'Run'")
    print()
    print('SQL patch contents:')
    print('=' * 60)
    print(sql_patch)
    print('=' * 60)
else:
    print('Supabase already has the required mailer campaign schema. ✓')
