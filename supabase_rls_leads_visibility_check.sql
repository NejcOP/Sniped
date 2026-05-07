-- Supabase leads visibility + RLS diagnostics
-- Purpose:
-- 1) Prove whether leads exist physically in DB for a user_id
-- 2) Verify policies on public.leads
-- 3) Simulate authenticated JWT context and test visibility through RLS
--
-- Usage:
-- - Replace the UUID below with the affected user's auth uid
-- - Run sections top-to-bottom in Supabase SQL Editor

-- ============================================================================
-- 0) Set target user UUID (replace this)
-- ============================================================================
WITH params AS (
  SELECT '00000000-0000-0000-0000-000000000000'::text AS target_uid
)
SELECT target_uid AS configured_target_uid FROM params;

-- ============================================================================
-- 1) Check physical rows for that user_id (service context)
--    If this returns > 0 but authenticated test returns 0, issue is RLS/policy.
-- ============================================================================
WITH params AS (
  SELECT '00000000-0000-0000-0000-000000000000'::text AS target_uid
)
SELECT
  COUNT(*) AS leads_for_target_user
FROM public.leads l
JOIN params p ON TRUE
WHERE l.user_id::text = p.target_uid;

WITH params AS (
  SELECT '00000000-0000-0000-0000-000000000000'::text AS target_uid
)
SELECT
  l.id,
  l.user_id,
  l.business_name,
  l.status,
  l.ai_score,
  l.created_at
FROM public.leads l
JOIN params p ON TRUE
WHERE l.user_id::text = p.target_uid
ORDER BY l.created_at DESC NULLS LAST
LIMIT 30;

-- ============================================================================
-- 2) Validate RLS is enabled and inspect lead policies
-- ============================================================================
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
  permissive,
  qual,
  with_check
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename = 'leads'
ORDER BY policyname;

-- ============================================================================
-- 3) Check user_id data quality (bad ownership values can hide rows)
-- ============================================================================
SELECT COUNT(*) AS leads_null_or_blank_user_id
FROM public.leads
WHERE user_id IS NULL OR btrim(user_id::text) = '';

SELECT
  COUNT(*) AS leads_with_non_uuid_user_id
FROM public.leads
WHERE user_id IS NOT NULL
  AND btrim(user_id::text) <> ''
  AND user_id::text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

-- ============================================================================
-- 4) Simulate authenticated client with JWT sub = target user
--    This is the critical visibility test for RLS behavior.
-- ============================================================================
BEGIN;

SET LOCAL ROLE authenticated;
SELECT set_config('request.jwt.claim.role', 'authenticated', true);
SELECT set_config('request.jwt.claim.sub', '00000000-0000-0000-0000-000000000000', true);

SELECT
  auth.role() AS auth_role,
  auth.uid()::text AS auth_uid;

SELECT COUNT(*) AS visible_leads_via_rls
FROM public.leads
WHERE user_id::text = auth.uid()::text;

SELECT
  id,
  user_id,
  business_name,
  status,
  ai_score,
  created_at
FROM public.leads
WHERE user_id::text = auth.uid()::text
ORDER BY created_at DESC NULLS LAST
LIMIT 30;

ROLLBACK;

-- ============================================================================
-- 5) Optional: show whether current lead filters could hide raw leads
--    (for quick comparison with app-side quick_filter=qualified behavior)
-- ============================================================================
WITH params AS (
  SELECT '00000000-0000-0000-0000-000000000000'::text AS target_uid
)
SELECT
  COUNT(*) FILTER (
    WHERE COALESCE(ai_score, 0) >= 7
       OR LOWER(COALESCE(status, '')) IN (
         'queued_mail','emailed','interested','replied','meeting set','zoom scheduled','closed','paid',
         'qualified_not_interested','qualified not interested'
       )
  ) AS qualified_like_count,
  COUNT(*) AS total_count
FROM public.leads l
JOIN params p ON TRUE
WHERE l.user_id::text = p.target_uid;

-- Interpretation:
-- - If section (1) > 0 and section (4) = 0: RLS visibility mismatch.
-- - If both (1) and (4) > 0 but app still shows empty table: frontend filter/state issue.
-- - If section (5) qualified_like_count = 0 while total_count > 0: "Qualified" filter can hide all new raw leads.
