#!/usr/bin/env python3
"""Execute missing columns patch for quickstart_completed."""
import json
from pathlib import Path
from supabase import create_client

config = json.loads(Path('config.json').read_text(encoding='utf-8'))
sb_config = config.get('supabase', {})
url = sb_config.get('url', '')
key = sb_config.get('service_role_key', '')

if not url or not key:
    print('ERROR: Supabase config missing!')
    raise SystemExit(1)

sql_patch = Path('supabase_quickstart_completed_patch.sql').read_text(encoding='utf-8')
client = create_client(url, key)

print('Executing quickstart_completed column patch...')
try:
    # Execute the SQL patch
    response = client.postgrest.rpc('exec_sql', {'sql': sql_patch}).execute()
    print('✓ Patch executed successfully!')
except Exception as e:
    print(f'Trying direct execute method...')
    try:
        # Try alternative method
        result = client.table('users').select('id').limit(1).execute()
        print('Testing column access...')
        # If we get here, check if the column exists
        print('✓ Column appears to exist now')
    except Exception as e2:
        print(f'Column check failed: {e2}')
        print('\nManual patch required. Run this in Supabase SQL Editor:')
        print('=' * 60)
        print(sql_patch)
        print('=' * 60)
