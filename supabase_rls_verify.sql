-- Supabase RLS verification script
-- Run after supabase_rls_lockdown.sql

-- 1) RLS status for all public tables
SELECT
  n.nspname AS schema_name,
  c.relname AS table_name,
  c.relrowsecurity AS rls_enabled,
  c.relforcerowsecurity AS rls_forced
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p')
ORDER BY c.relname;

-- 2) Policies on public tables
SELECT
  schemaname,
  tablename,
  policyname,
  roles,
  cmd,
  permissive,
  qual,
  with_check
FROM pg_policies
WHERE schemaname = 'public'
ORDER BY tablename, policyname;

-- 3) Ensure no table grants remain for anon
SELECT
  grantee,
  table_schema,
  table_name,
  privilege_type
FROM information_schema.role_table_grants
WHERE table_schema = 'public'
  AND grantee = 'anon'
ORDER BY table_name, privilege_type;

-- 4) Inspect authenticated/service_role grants on tables
SELECT
  grantee,
  table_schema,
  table_name,
  string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privileges
FROM information_schema.role_table_grants
WHERE table_schema = 'public'
  AND grantee IN ('authenticated', 'service_role')
GROUP BY grantee, table_schema, table_name
ORDER BY grantee, table_name;

-- 5) Inspect schema privileges for anon/authenticated/service_role
SELECT
  r.grantee,
  'public' AS schema_name,
  concat_ws(
    ', ',
    CASE WHEN has_schema_privilege(r.grantee, 'public', 'USAGE') THEN 'USAGE' END,
    CASE WHEN has_schema_privilege(r.grantee, 'public', 'CREATE') THEN 'CREATE' END
  ) AS privileges
FROM (
  VALUES ('anon'), ('authenticated'), ('service_role')
) AS r(grantee)
ORDER BY r.grantee;

-- 6) Tables with user_id but missing expected ownership policies
WITH user_tables AS (
  SELECT c.table_schema, c.table_name
  FROM information_schema.columns c
  JOIN information_schema.tables t
    ON t.table_schema = c.table_schema
   AND t.table_name = c.table_name
  WHERE c.table_schema = 'public'
    AND c.column_name = 'user_id'
    AND t.table_type = 'BASE TABLE'
  GROUP BY c.table_schema, c.table_name
),
policy_counts AS (
  SELECT
    schemaname AS table_schema,
    tablename AS table_name,
    COUNT(*) FILTER (WHERE policyname = 'auth_select_own_user_id') AS has_select_policy,
    COUNT(*) FILTER (WHERE policyname = 'auth_insert_own_user_id') AS has_insert_policy,
    COUNT(*) FILTER (WHERE policyname = 'auth_update_own_user_id') AS has_update_policy,
    COUNT(*) FILTER (WHERE policyname = 'auth_delete_own_user_id') AS has_delete_policy
  FROM pg_policies
  WHERE schemaname = 'public'
  GROUP BY schemaname, tablename
)
SELECT
  u.table_schema,
  u.table_name,
  COALESCE(p.has_select_policy, 0) AS select_policy,
  COALESCE(p.has_insert_policy, 0) AS insert_policy,
  COALESCE(p.has_update_policy, 0) AS update_policy,
  COALESCE(p.has_delete_policy, 0) AS delete_policy
FROM user_tables u
LEFT JOIN policy_counts p
  ON p.table_schema = u.table_schema
 AND p.table_name = u.table_name
WHERE COALESCE(p.has_select_policy, 0) = 0
   OR COALESCE(p.has_insert_policy, 0) = 0
   OR COALESCE(p.has_update_policy, 0) = 0
   OR COALESCE(p.has_delete_policy, 0) = 0
ORDER BY u.table_name;

-- 7) users table own-profile policies check
SELECT
  schemaname,
  tablename,
  policyname,
  cmd,
  roles,
  qual,
  with_check
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename = 'users'
ORDER BY policyname;

-- 8) Tables still missing user_id (excluding special/global tables)
SELECT
  t.table_schema,
  t.table_name
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
ORDER BY t.table_name;

-- 9) Explicit leads visibility checks (quick diagnostics)
SELECT
  n.nspname AS schema_name,
  c.relname AS table_name,
  c.relrowsecurity AS rls_enabled,
  c.relforcerowsecurity AS rls_forced
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname = 'leads'
  AND c.relkind IN ('r', 'p');

SELECT
  schemaname,
  tablename,
  policyname,
  roles,
  cmd,
  qual,
  with_check
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename = 'leads'
ORDER BY policyname;
