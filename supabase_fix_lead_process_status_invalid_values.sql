-- Normalizes legacy/invalid lead process statuses to enum-safe values.
-- Safe to run multiple times.

BEGIN;

DO $$
DECLARE
    process_status_udt text;
BEGIN
    SELECT c.udt_name
    INTO process_status_udt
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
      AND c.table_name = 'leads'
      AND c.column_name = 'process_status'
    LIMIT 1;

    IF process_status_udt IS NULL THEN
        RETURN;
    END IF;

    IF process_status_udt = 'lead_process_status' THEN
        -- Enum column: only NULL can be invalid/missing.
        EXECUTE $enum_sql$
            UPDATE public.leads
            SET process_status = CASE
                WHEN lower(coalesce(enrichment_status, '')) = 'completed' THEN 'COMPLETED'::public.lead_process_status
                WHEN lower(coalesce(enrichment_status, '')) = 'processing' THEN 'PROCESSING'::public.lead_process_status
                WHEN lower(coalesce(enrichment_status, '')) IN ('failed', 'failed_no_url') THEN 'FAILED'::public.lead_process_status
                ELSE 'PENDING'::public.lead_process_status
            END
            WHERE process_status IS NULL
        $enum_sql$;
    ELSE
        -- Text/varchar column: clean NULL, empty, mixed-case, and unknown values.
        EXECUTE $text_sql$
            UPDATE public.leads
            SET process_status = CASE
                WHEN lower(coalesce(enrichment_status, '')) = 'completed' THEN 'COMPLETED'
                WHEN lower(coalesce(enrichment_status, '')) = 'processing' THEN 'PROCESSING'
                WHEN lower(coalesce(enrichment_status, '')) IN ('failed', 'failed_no_url') THEN 'FAILED'
                ELSE 'PENDING'
            END
            WHERE process_status IS NULL
               OR btrim(coalesce(process_status::text, '')) = ''
               OR upper(process_status::text) NOT IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')
               OR process_status::text <> upper(process_status::text)
        $text_sql$;
    END IF;
END
$$;

COMMIT;
