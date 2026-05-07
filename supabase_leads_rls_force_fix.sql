-- FORCE FIX for leads visibility (requested emergency version)
-- WARNING: `users_view_own_leads FOR ALL USING (true)` allows every authenticated
-- user to access all leads. Use only for emergency diagnostics.

BEGIN;

-- 1) Emergency diagnostic toggle: disable RLS temporarily
-- Uncomment only if you need to prove RLS is the blocker right now.
-- ALTER TABLE public.leads DISABLE ROW LEVEL SECURITY;

-- 2) Keep RLS enabled for permanent policy mode
ALTER TABLE public.leads ENABLE ROW LEVEL SECURITY;

-- 3) service_role full access (scraper/backend)
DROP POLICY IF EXISTS service_role_full_access ON public.leads;
CREATE POLICY service_role_full_access
ON public.leads
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

-- 4) Requested emergency authenticated policy (open visibility)
DROP POLICY IF EXISTS users_view_own_leads ON public.leads;
CREATE POLICY users_view_own_leads
ON public.leads
FOR ALL
TO authenticated
USING (true)
WITH CHECK (true);

COMMIT;

-- Safer replacement (recommended after confirming root cause):
-- DROP POLICY IF EXISTS users_view_own_leads ON public.leads;
-- CREATE POLICY users_view_own_leads
-- ON public.leads
-- FOR SELECT
-- TO authenticated
-- USING (auth.uid() IS NOT NULL AND user_id::text = auth.uid()::text);
