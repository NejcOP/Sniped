-- Tenant scope for lead_blacklist
-- Run this in Supabase SQL editor after deploying backend changes.

ALTER TABLE IF EXISTS public.lead_blacklist
    ADD COLUMN IF NOT EXISTS user_id TEXT;

UPDATE public.lead_blacklist
SET user_id = 'legacy'
WHERE COALESCE(user_id, '') = '';

ALTER TABLE IF EXISTS public.lead_blacklist
    ALTER COLUMN user_id SET DEFAULT 'legacy';

ALTER TABLE IF EXISTS public.lead_blacklist
    ALTER COLUMN user_id SET NOT NULL;

-- Remove duplicate rows inside each tenant key before adding unique index.
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY user_id, lower(trim(kind)), lower(trim(value))
               ORDER BY id DESC
           ) AS rn
    FROM public.lead_blacklist
)
DELETE FROM public.lead_blacklist lb
USING ranked r
WHERE lb.id = r.id
  AND r.rn > 1;

DROP INDEX IF EXISTS public.idx_lead_blacklist_kind_value;

CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_blacklist_user_kind_value
    ON public.lead_blacklist(user_id, kind, value);

CREATE INDEX IF NOT EXISTS idx_lead_blacklist_user_id
    ON public.lead_blacklist(user_id);
