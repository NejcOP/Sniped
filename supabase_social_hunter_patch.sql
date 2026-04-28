-- Add twitter_url and youtube_url columns to the leads table.
-- Safe to run multiple times (uses IF NOT EXISTS).

BEGIN;

ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS twitter_url text;
ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS youtube_url text;

COMMIT;
