#!/usr/bin/env python3
"""Direct SQL executor for Supabase patches."""
import json
from pathlib import Path

try:
    from supabase import create_client
    import postgrest
except ImportError:
    print("Installing required packages...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "supabase", "postgrest"])
    from supabase import create_client
    import postgrest

config = json.loads(Path('config.json').read_text(encoding='utf-8'))
sb_config = config.get('supabase', {})
url = sb_config.get('url', '')
service_role_key = sb_config.get('service_role_key', '')

if not url or not service_role_key:
    print('ERROR: Supabase config missing!')
    raise SystemExit(1)

sql_patch = Path('supabase_quickstart_completed_patch.sql').read_text(encoding='utf-8')

print('Connecting to Supabase...')
client = create_client(url, service_role_key)

print('Executing SQL patch...')
try:
    # Execute each SQL statement
    for statement in sql_patch.strip().split(';'):
        statement = statement.strip()
        if not statement or statement.startswith('--'):
            continue
        print(f'Executing: {statement[:60]}...')
        # Use raw SQL execution
        response = client.postgrest.rpc('exec_sql', {'sql': statement}).execute()
    print('✓ SQL patch executed successfully!')
except AttributeError:
    print('Note: exec_sql RPC not available. Trying alternative approach...')
    print('\nPlease execute this SQL manually in Supabase SQL Editor:')
    print('1. Open: https://supabase.com/dashboard/project/dwxunoinmgdqftvnaziz/sql/new')
    print('2. Copy and paste the SQL below')
    print('3. Click "Run"')
    print('=' * 60)
    print(sql_patch)
    print('=' * 60)
except Exception as e:
    print(f'Error executing patch: {e}')
    print('\nFalling back to manual instructions...')
    print('\nPlease execute this SQL manually in Supabase SQL Editor:')
    print('1. Open: https://supabase.com/dashboard/project/dwxunoinmgdqftvnaziz/sql/new')
    print('2. Copy and paste the SQL below')
    print('3. Click "Run"')
    print('=' * 60)
    print(sql_patch)
    print('=' * 60)
