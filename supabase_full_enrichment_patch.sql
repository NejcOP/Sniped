-- Full-data enrichment columns for lead scraping pipeline.
-- Safe to run multiple times.

ALTER TABLE IF EXISTS leads ADD COLUMN IF NOT EXISTS ig_link TEXT;
ALTER TABLE IF EXISTS leads ADD COLUMN IF NOT EXISTS fb_link TEXT;
ALTER TABLE IF EXISTS leads ADD COLUMN IF NOT EXISTS tiktok_url TEXT;
ALTER TABLE IF EXISTS leads ADD COLUMN IF NOT EXISTS has_pixel BOOLEAN DEFAULT FALSE;
ALTER TABLE IF EXISTS leads ADD COLUMN IF NOT EXISTS tech_stack TEXT;
ALTER TABLE IF EXISTS leads ADD COLUMN IF NOT EXISTS qualification_score DOUBLE PRECISION;

-- Ensure every lead is user-scoped.
ALTER TABLE IF EXISTS leads ADD COLUMN IF NOT EXISTS user_id TEXT;
UPDATE leads
SET user_id = 'legacy'
WHERE user_id IS NULL OR TRIM(COALESCE(user_id, '')) = '';

-- Normalize nulls used by the unique key columns.
UPDATE leads
SET business_name = COALESCE(NULLIF(TRIM(COALESCE(business_name, '')), ''), 'Unknown Business')
WHERE business_name IS NULL OR TRIM(COALESCE(business_name, '')) = '';

UPDATE leads
SET address = COALESCE(NULLIF(TRIM(COALESCE(address, '')), ''), 'Unknown Address')
WHERE address IS NULL OR TRIM(COALESCE(address, '')) = '';

-- Remove duplicates before adding user-scoped unique index.
WITH ranked AS (
	SELECT
		id,
		ROW_NUMBER() OVER (
			PARTITION BY user_id, business_name, address
			ORDER BY id ASC
		) AS rn
	FROM leads
)
DELETE FROM leads
WHERE id IN (
	SELECT id
	FROM ranked
	WHERE rn > 1
);

-- Drop legacy global unique key if it exists.
ALTER TABLE IF EXISTS leads DROP CONSTRAINT IF EXISTS leads_business_name_address_key;
ALTER TABLE IF EXISTS leads DROP CONSTRAINT IF EXISTS uq_leads_business_name_address;
DROP INDEX IF EXISTS uq_leads_business_name_address;
DROP INDEX IF EXISTS leads_business_name_address_key;

-- Enforce per-user uniqueness.
CREATE UNIQUE INDEX IF NOT EXISTS uq_leads_user_business_address
ON leads (user_id, business_name, address);

-- Helpful query indexes for user-scoped dashboard/listing queries.
CREATE INDEX IF NOT EXISTS idx_leads_user_id ON leads (user_id);
CREATE INDEX IF NOT EXISTS idx_leads_user_created_at ON leads (user_id, created_at DESC, id DESC);
