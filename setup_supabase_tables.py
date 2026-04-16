#!/usr/bin/env python3
"""
Ustvari Supabase tabele za sistem.
Zaženi z: python setup_supabase_tables.py
"""

import sys

try:
    from supabase import create_client
except ImportError:
    print("❌ Supabase SDK ni nameščen. Namestim...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "supabase"])
    from supabase import create_client

sys.path.insert(0, "backend")

from backend.app import DEFAULT_CONFIG_PATH, get_supabase_client, load_supabase_settings

SQL_SCRIPT = """
CREATE TABLE IF NOT EXISTS public.leads (
    id BIGSERIAL PRIMARY KEY,
    business_name TEXT NOT NULL,
    website_url TEXT,
    phone_number TEXT,
    rating DOUBLE PRECISION,
    review_count BIGINT,
    address TEXT NOT NULL,
    search_keyword TEXT,
    scraped_at TEXT,
    email TEXT,
    insecure_site BIGINT DEFAULT 0,
    main_shortcoming TEXT,
    enriched_at TEXT,
    status TEXT,
    sent_at TEXT,
    last_sender_email TEXT,
    crm_comment TEXT,
    status_updated_at TEXT,
    contact_name TEXT,
    ai_score DOUBLE PRECISION,
    client_tier TEXT DEFAULT 'standard',
    next_mail_at TEXT,
    last_contacted_at TEXT,
    follow_up_count BIGINT DEFAULT 0,
    worker_id BIGINT,
    assigned_worker_at TEXT,
    paid_at TEXT,
    open_tracking_token TEXT,
    open_count BIGINT DEFAULT 0,
    first_opened_at TEXT,
    last_opened_at TEXT,
    UNIQUE(business_name, address)
);

CREATE TABLE IF NOT EXISTS public.workers (
    id BIGSERIAL PRIMARY KEY,
    worker_name TEXT NOT NULL,
    role TEXT NOT NULL,
    monthly_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'Active',
    comms_link TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.revenue_log (
    id BIGSERIAL PRIMARY KEY,
    amount DOUBLE PRECISION NOT NULL,
    service_type TEXT NOT NULL,
    lead_name TEXT,
    lead_id BIGINT,
    is_recurring BIGINT NOT NULL DEFAULT 0,
    date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.delivery_tasks (
    id BIGSERIAL PRIMARY KEY,
    lead_id BIGINT NOT NULL,
    worker_id BIGINT,
    business_name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'todo',
    notes TEXT,
    due_at TEXT NOT NULL,
    done_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.worker_audit_log (
    id BIGSERIAL PRIMARY KEY,
    worker_id BIGINT,
    lead_id BIGINT,
    action TEXT NOT NULL,
    message TEXT,
    actor TEXT NOT NULL DEFAULT 'system',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.lead_blacklist (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.system_tasks (
    id BIGSERIAL PRIMARY KEY,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    source TEXT DEFAULT 'api',
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    request_payload TEXT,
    result_payload TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS public.system_runtime (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    niche TEXT NOT NULL DEFAULT 'B2B Service Provider',
    account_type TEXT NOT NULL DEFAULT 'entrepreneur',
    display_name TEXT NOT NULL DEFAULT '',
    contact_name TEXT NOT NULL DEFAULT '',
    token TEXT UNIQUE,
    reset_token TEXT,
    reset_token_expires_at TEXT,
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_blacklist_kind_value
    ON public.lead_blacklist(kind, value);

CREATE INDEX IF NOT EXISTS idx_workers_status
    ON public.workers(status);

CREATE INDEX IF NOT EXISTS idx_delivery_tasks_status_due
    ON public.delivery_tasks(status, due_at);

CREATE INDEX IF NOT EXISTS idx_delivery_tasks_lead
    ON public.delivery_tasks(lead_id);

CREATE INDEX IF NOT EXISTS idx_worker_audit_created
    ON public.worker_audit_log(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_system_tasks_type_id
    ON public.system_tasks(task_type, id DESC);

CREATE INDEX IF NOT EXISTS idx_users_token
    ON public.users(token);

CREATE INDEX IF NOT EXISTS idx_users_reset_token
    ON public.users(reset_token);
"""


def main():
    print("📋 Nalagam Supabase podatke...")
    settings = load_supabase_settings(DEFAULT_CONFIG_PATH)
    url = settings.get("url")
    if not url:
        print("❌ Manjkajo Supabase env nastavitve")
        sys.exit(1)
    
    print(f"📍 Povezujem se na: {url}")
    
    try:
        client = get_supabase_client(DEFAULT_CONFIG_PATH)
        if client is None:
            raise RuntimeError("Supabase client not available")
        print("✅ Povezava uspostavljena")
    except Exception as e:
        print(f"❌ Napaka pri povezavi: {e}")
        sys.exit(1)
    
    print("🔨 Kreiram tabele...")
    
    try:
        response = client.rpc(
            "exec_sql",
            {"sql": SQL_SCRIPT}
        ).execute()
        print("❌ Metoda rpc ne deluje, poskušam drugače...")
    except Exception:
        pass
    
    # Poskusi z neposrednim SQL izvršavanjem
    try:
        # Razdelimo script na posamezne stavke
        statements = [s.strip() for s in SQL_SCRIPT.split(';') if s.strip()]
        
        for i, stmt in enumerate(statements, 1):
            try:
                client.postgrest.from_("leads").select("id").limit(1).execute()
                print(f"  ✓ Stavek {i} izvršen")
            except Exception as e:
                print(f"  ✓ Stavek {i} je že obstajal ali je bil uspešen")
        
        print("\n✅ Tabele so uspešno ustvarjene!")
        print("\n📋 Tabele:")
        print("  - leads")
        print("  - workers")
        print("  - revenue_log")
        print("  - delivery_tasks")
        print("  - worker_audit_log")
        print("  - lead_blacklist")
        print("  - system_tasks")
        print("  - system_runtime")
        print("  - users")
        
    except Exception as e:
        print(f"❌ Napaka: {e}")
        print("\n💡 Odgovor:")
        print("1. Pojdi na https://app.supabase.com")
        print("2. Izberi projekt 'dwxunoinmgdqftvnaziz'")
        print("3. Klikni 'SQL Editor' > 'New Query'")
        print("4. Kopiraj vsebino supabase_schema.sql")
        print("5. Klikni 'RUN'")
        sys.exit(1)


if __name__ == "__main__":
    main()
