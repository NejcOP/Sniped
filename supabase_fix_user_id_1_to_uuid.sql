-- One-time migration: move leads from hardcoded user_id='1' to your real auth UUID.
-- Usage:
-- 1) Replace YOUR_AUTH_UUID_HERE with your actual logged-in Supabase Auth UUID.
-- 2) Run this script in Supabase SQL Editor.

BEGIN;

-- Preview rows that will be updated.
SELECT id, user_id, business_name, created_at
FROM public.leads
WHERE user_id = '1'
ORDER BY id DESC
LIMIT 200;

-- Update legacy hardcoded user rows to the real auth UUID.
UPDATE public.leads
SET user_id = 'YOUR_AUTH_UUID_HERE'
WHERE user_id = '1';

-- Optional: if you also want to migrate legacy placeholder rows, uncomment below.
-- UPDATE public.leads
-- SET user_id = 'YOUR_AUTH_UUID_HERE'
-- WHERE user_id = 'legacy';

-- Verification summary.
SELECT
  COUNT(*) FILTER (WHERE user_id = '1') AS remaining_user_1,
  COUNT(*) FILTER (WHERE user_id = 'YOUR_AUTH_UUID_HERE') AS moved_to_target_uuid,
  COUNT(*) AS total_rows
FROM public.leads;

COMMIT;
