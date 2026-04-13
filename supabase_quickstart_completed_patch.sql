-- Add missing columns to users table for schema sync
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS quickstart_completed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS average_deal_value DOUBLE PRECISION NOT NULL DEFAULT 1000;

-- Update any NULL values to safe defaults
UPDATE public.users
SET quickstart_completed = COALESCE(quickstart_completed, FALSE)
WHERE quickstart_completed IS NULL;

UPDATE public.users
SET average_deal_value = COALESCE(NULLIF(average_deal_value, 0), 1000)
WHERE average_deal_value IS NULL OR average_deal_value <= 0;
