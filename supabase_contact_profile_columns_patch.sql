-- Ensure leads table supports full digital profile fields.
-- Safe to run multiple times.

BEGIN;

ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS website_url text;
ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS email text;
ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS facebook text;
ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS instagram text;
ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS linkedin text;

-- Backfill compatibility columns from existing *_url fields when available.
UPDATE public.leads
SET
  facebook = COALESCE(NULLIF(facebook, ''), NULLIF(facebook_url, '')),
  instagram = COALESCE(NULLIF(instagram, ''), NULLIF(instagram_url, '')),
  linkedin = COALESCE(NULLIF(linkedin, ''), NULLIF(linkedin_url, ''))
WHERE
  COALESCE(facebook, '') = ''
  OR COALESCE(instagram, '') = ''
  OR COALESCE(linkedin, '') = '';

-- Optional quality checks.
SELECT
  COUNT(*) FILTER (WHERE COALESCE(website_url, '') <> '') AS with_website,
  COUNT(*) FILTER (WHERE COALESCE(email, '') <> '') AS with_email,
  COUNT(*) FILTER (WHERE COALESCE(facebook, '') <> '' OR COALESCE(instagram, '') <> '' OR COALESCE(linkedin, '') <> '') AS with_any_social,
  COUNT(*) AS total_rows
FROM public.leads;

COMMIT;
