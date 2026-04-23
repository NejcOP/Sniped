-- Master type-alignment patch for leads persistence
-- Safe to run multiple times.

BEGIN;

ALTER TABLE IF EXISTS public.leads
  ADD COLUMN IF NOT EXISTS google_claimed bigint DEFAULT 0,
  ADD COLUMN IF NOT EXISTS has_pixel bigint DEFAULT 0,
  ADD COLUMN IF NOT EXISTS insecure_site bigint DEFAULT 0,
  ADD COLUMN IF NOT EXISTS is_ads_client bigint DEFAULT 0,
  ADD COLUMN IF NOT EXISTS is_website_client bigint DEFAULT 0,
  ADD COLUMN IF NOT EXISTS follow_up_count bigint DEFAULT 0,
  ADD COLUMN IF NOT EXISTS open_count bigint DEFAULT 0,
  ADD COLUMN IF NOT EXISTS campaign_step bigint DEFAULT 1,
  ADD COLUMN IF NOT EXISTS qualification_score double precision;

DO $$
DECLARE
  col_type text;
BEGIN
  SELECT data_type INTO col_type
  FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = 'leads' AND column_name = 'google_claimed';
  IF col_type = 'boolean' THEN
    EXECUTE 'ALTER TABLE public.leads ALTER COLUMN google_claimed TYPE bigint USING (CASE WHEN google_claimed THEN 1 ELSE 0 END)';
  END IF;

  SELECT data_type INTO col_type
  FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = 'leads' AND column_name = 'has_pixel';
  IF col_type = 'boolean' THEN
    EXECUTE 'ALTER TABLE public.leads ALTER COLUMN has_pixel TYPE bigint USING (CASE WHEN has_pixel THEN 1 ELSE 0 END)';
  END IF;

  SELECT data_type INTO col_type
  FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = 'leads' AND column_name = 'is_ads_client';
  IF col_type = 'boolean' THEN
    EXECUTE 'ALTER TABLE public.leads ALTER COLUMN is_ads_client TYPE bigint USING (CASE WHEN is_ads_client THEN 1 ELSE 0 END)';
  END IF;

  SELECT data_type INTO col_type
  FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = 'leads' AND column_name = 'is_website_client';
  IF col_type = 'boolean' THEN
    EXECUTE 'ALTER TABLE public.leads ALTER COLUMN is_website_client TYPE bigint USING (CASE WHEN is_website_client THEN 1 ELSE 0 END)';
  END IF;
END $$;

UPDATE public.leads
SET
  google_claimed = COALESCE(google_claimed, 0),
  has_pixel = COALESCE(has_pixel, 0),
  insecure_site = COALESCE(insecure_site, 0),
  is_ads_client = COALESCE(is_ads_client, 0),
  is_website_client = COALESCE(is_website_client, 0),
  follow_up_count = COALESCE(follow_up_count, 0),
  open_count = COALESCE(open_count, 0),
  campaign_step = COALESCE(campaign_step, 1)
WHERE
  google_claimed IS NULL
  OR has_pixel IS NULL
  OR insecure_site IS NULL
  OR is_ads_client IS NULL
  OR is_website_client IS NULL
  OR follow_up_count IS NULL
  OR open_count IS NULL
  OR campaign_step IS NULL;

ALTER TABLE public.leads
  ALTER COLUMN google_claimed SET DEFAULT 0,
  ALTER COLUMN google_claimed SET NOT NULL,
  ALTER COLUMN has_pixel SET DEFAULT 0,
  ALTER COLUMN has_pixel SET NOT NULL,
  ALTER COLUMN insecure_site SET DEFAULT 0,
  ALTER COLUMN insecure_site SET NOT NULL,
  ALTER COLUMN is_ads_client SET DEFAULT 0,
  ALTER COLUMN is_ads_client SET NOT NULL,
  ALTER COLUMN is_website_client SET DEFAULT 0,
  ALTER COLUMN is_website_client SET NOT NULL,
  ALTER COLUMN follow_up_count SET DEFAULT 0,
  ALTER COLUMN follow_up_count SET NOT NULL,
  ALTER COLUMN open_count SET DEFAULT 0,
  ALTER COLUMN open_count SET NOT NULL,
  ALTER COLUMN campaign_step SET DEFAULT 1,
  ALTER COLUMN campaign_step SET NOT NULL;

COMMIT;
