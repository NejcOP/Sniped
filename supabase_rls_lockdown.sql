-- Supabase RLS lockdown migration
-- Safe to run multiple times.
-- Goal:
-- 1) Enable RLS on every public table
-- 2) Lock out unauthenticated access (anon)
-- 3) Keep service_role full access
-- 4) Enforce per-user access for users + all tables that have user_id (including leads/campaign-like tables)

BEGIN;

-- ---------------------------------------------------------------------------
-- 0) Ensure tenant key exists before enforcing user_id ownership policies
--    users table is handled separately via auth_user_id; system_runtime is global.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT t.table_schema, t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
          AND t.table_name NOT IN ('users', 'system_runtime')
          AND NOT EXISTS (
              SELECT 1
              FROM information_schema.columns c
              WHERE c.table_schema = t.table_schema
                AND c.table_name = t.table_name
                AND c.column_name = 'user_id'
          )
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS user_id text', r.table_schema, r.table_name);
        EXECUTE format('UPDATE %I.%I SET user_id = ''legacy'' WHERE COALESCE(user_id::text, '''') = ''''', r.table_schema, r.table_name);
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN user_id SET DEFAULT ''legacy''', r.table_schema, r.table_name);
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN user_id SET NOT NULL', r.table_schema, r.table_name);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- A) Lock down anonymous role and preserve backend service role capabilities
-- ---------------------------------------------------------------------------
REVOKE ALL ON SCHEMA public FROM anon;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM anon;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon;

GRANT USAGE ON SCHEMA public TO authenticated;
GRANT USAGE ON SCHEMA public TO service_role;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO service_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON SEQUENCES FROM anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;

-- ---------------------------------------------------------------------------
-- B) Enable RLS on every table in public schema
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', r.schemaname, r.tablename);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- C) Remove existing policies so old unrestricted policies cannot linger
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    p record;
BEGIN
    FOR p IN
        SELECT schemaname, tablename, policyname
        FROM pg_policies
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS %I ON %I.%I', p.policyname, p.schemaname, p.tablename);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- D) Service role full access policy on every table
--    (service_role already has broad privileges, this keeps behavior explicit)
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format(
            'CREATE POLICY service_role_full_access ON %I.%I FOR ALL TO service_role USING (true) WITH CHECK (true)',
            r.schemaname,
            r.tablename
        );
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- E) users table: only own profile via auth.uid()
--    Adds auth_user_id if missing, then backfills by email match to auth.users.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('public.users') IS NOT NULL THEN
        ALTER TABLE public.users
            ADD COLUMN IF NOT EXISTS auth_user_id uuid;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_auth_user_id
            ON public.users(auth_user_id)
            WHERE auth_user_id IS NOT NULL;

        -- Best-effort backfill for existing records.
        UPDATE public.users u
        SET auth_user_id = au.id
        FROM auth.users au
        WHERE u.auth_user_id IS NULL
          AND lower(trim(u.email)) = lower(trim(au.email));

        GRANT SELECT, UPDATE ON TABLE public.users TO authenticated;

        CREATE POLICY users_select_own
            ON public.users
            FOR SELECT
            TO authenticated
            USING (
                auth.uid() IS NOT NULL
                AND (
                    auth_user_id = auth.uid()
                    OR lower(trim(email)) = lower(coalesce(auth.jwt() ->> 'email', ''))
                )
            );

        CREATE POLICY users_update_own
            ON public.users
            FOR UPDATE
            TO authenticated
            USING (
                auth.uid() IS NOT NULL
                AND (
                    auth_user_id = auth.uid()
                    OR lower(trim(email)) = lower(coalesce(auth.jwt() ->> 'email', ''))
                )
            )
            WITH CHECK (
                auth.uid() IS NOT NULL
                AND (
                    auth_user_id = auth.uid()
                    OR lower(trim(email)) = lower(coalesce(auth.jwt() ->> 'email', ''))
                )
            );
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- F) All tables with user_id: user can only see/change own rows
--    Covers leads and campaign-like tables that store user_id.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
                SELECT c.table_schema, c.table_name
        FROM information_schema.columns c
                JOIN information_schema.tables t
                    ON t.table_schema = c.table_schema
                 AND t.table_name = c.table_name
        WHERE c.table_schema = 'public'
          AND c.column_name = 'user_id'
                    AND t.table_type = 'BASE TABLE'
        GROUP BY c.table_schema, c.table_name
    LOOP
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %I.%I TO authenticated', r.table_schema, r.table_name);

        EXECUTE format(
            'CREATE POLICY auth_select_own_user_id ON %I.%I FOR SELECT TO authenticated USING (auth.uid() IS NOT NULL AND user_id::text = auth.uid()::text)',
            r.table_schema,
            r.table_name
        );

        EXECUTE format(
            'CREATE POLICY auth_insert_own_user_id ON %I.%I FOR INSERT TO authenticated WITH CHECK (auth.uid() IS NOT NULL AND user_id::text = auth.uid()::text)',
            r.table_schema,
            r.table_name
        );

        EXECUTE format(
            'CREATE POLICY auth_update_own_user_id ON %I.%I FOR UPDATE TO authenticated USING (auth.uid() IS NOT NULL AND user_id::text = auth.uid()::text) WITH CHECK (auth.uid() IS NOT NULL AND user_id::text = auth.uid()::text)',
            r.table_schema,
            r.table_name
        );

        EXECUTE format(
            'CREATE POLICY auth_delete_own_user_id ON %I.%I FOR DELETE TO authenticated USING (auth.uid() IS NOT NULL AND user_id::text = auth.uid()::text)',
            r.table_schema,
            r.table_name
        );
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- G) Optional hardening for a lowercase campaigns table if present
--    (already covered by section F when campaigns.user_id exists)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('public.campaigns') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'campaigns'
              AND column_name = 'user_id'
        ) THEN
            -- No-op note: policies are already created by section F.
            RAISE NOTICE 'public.campaigns detected with user_id; user_id RLS policies applied.';
        ELSE
            RAISE NOTICE 'public.campaigns exists but has no user_id column; create a custom owner policy for this table.';
        END IF;
    END IF;
END
$$;

COMMIT;
