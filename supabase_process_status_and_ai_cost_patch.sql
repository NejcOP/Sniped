-- Adds lead process tracking + AI cost tracking columns.
-- Safe to run multiple times.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'lead_process_status'
          AND n.nspname = 'public'
    ) THEN
        CREATE TYPE public.lead_process_status AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED');
    END IF;
END
$$;

ALTER TABLE public.leads
    ADD COLUMN IF NOT EXISTS process_status public.lead_process_status,
    ADD COLUMN IF NOT EXISTS retry_count bigint DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error text,
    ADD COLUMN IF NOT EXISTS tokens_used bigint DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_usd double precision DEFAULT 0.0;

UPDATE public.leads
SET process_status = CASE
    WHEN lower(coalesce(enrichment_status, '')) = 'completed' THEN 'COMPLETED'::public.lead_process_status
    WHEN lower(coalesce(enrichment_status, '')) = 'processing' THEN 'PROCESSING'::public.lead_process_status
    WHEN lower(coalesce(enrichment_status, '')) IN ('failed', 'failed_no_url') THEN 'FAILED'::public.lead_process_status
    ELSE 'PENDING'::public.lead_process_status
END
WHERE process_status IS NULL;

ALTER TABLE public.leads
    ALTER COLUMN process_status SET DEFAULT 'PENDING'::public.lead_process_status,
    ALTER COLUMN process_status SET NOT NULL,
    ALTER COLUMN retry_count SET DEFAULT 0,
    ALTER COLUMN tokens_used SET DEFAULT 0,
    ALTER COLUMN cost_usd SET DEFAULT 0.0;

CREATE INDEX IF NOT EXISTS idx_leads_user_process_status
    ON public.leads(user_id, process_status, id);

ALTER TABLE public.worker_audit_log
    ADD COLUMN IF NOT EXISTS tokens_used bigint,
    ADD COLUMN IF NOT EXISTS cost_usd double precision;

COMMIT;
