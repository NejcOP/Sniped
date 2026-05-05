-- Cleanup orphaned leads and invalid ownership after user cleanup.
-- Safe to run multiple times.

BEGIN;

-- 1) Remove leads without a valid user_id value.
DELETE FROM public.leads
WHERE user_id IS NULL OR BTRIM(CAST(user_id AS TEXT)) = '';

-- 2) Remove leads whose user_id no longer exists in either public.users or auth.users.
--    Supports mixed historical ID formats by casting to text.
DELETE FROM public.leads l
WHERE NOT EXISTS (
    SELECT 1
    FROM public.users pu
    WHERE CAST(pu.id AS TEXT) = CAST(l.user_id AS TEXT)
)
AND NOT EXISTS (
    SELECT 1
    FROM auth.users au
    WHERE CAST(au.id AS TEXT) = CAST(l.user_id AS TEXT)
);

COMMIT;

-- Optional verification queries:
-- SELECT COUNT(*) AS leads_without_user_id FROM public.leads WHERE user_id IS NULL OR BTRIM(CAST(user_id AS TEXT)) = '';
-- SELECT COUNT(*) AS orphaned_leads
-- FROM public.leads l
-- WHERE NOT EXISTS (SELECT 1 FROM public.users pu WHERE CAST(pu.id AS TEXT) = CAST(l.user_id AS TEXT))
--   AND NOT EXISTS (SELECT 1 FROM auth.users au WHERE CAST(au.id AS TEXT) = CAST(l.user_id AS TEXT));
