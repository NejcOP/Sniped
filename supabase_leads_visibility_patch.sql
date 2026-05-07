-- Ensure leads are visible to authenticated users under RLS.
-- Safe to run multiple times.

BEGIN;

-- 1) Enable RLS on leads.
ALTER TABLE public.leads ENABLE ROW LEVEL SECURITY;

-- 2) Ensure expected privileges exist.
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.leads TO authenticated;
GRANT ALL PRIVILEGES ON TABLE public.leads TO service_role;

-- 3) Recreate policies idempotently.
DROP POLICY IF EXISTS service_role_full_access ON public.leads;
DROP POLICY IF EXISTS users_view_own_leads ON public.leads;

-- service_role (backend/scraper) full access
CREATE POLICY service_role_full_access
ON public.leads
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

-- authenticated users can view only their own rows (plus historical null owner rows)
CREATE POLICY users_view_own_leads
ON public.leads
FOR SELECT
TO authenticated
USING (auth.uid() IS NOT NULL AND (user_id IS NULL OR user_id::text = auth.uid()::text));

COMMIT;
