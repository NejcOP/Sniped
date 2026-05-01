-- Supabase performance indexes for ultra-fast lead listing by user.
-- Safe to run multiple times.

CREATE INDEX IF NOT EXISTS idx_leads_user_id
ON public.leads (user_id);

CREATE INDEX IF NOT EXISTS idx_leads_user_created_at
ON public.leads (user_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_leads_user_scraped_at
ON public.leads (user_id, scraped_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_leads_user_status
ON public.leads (user_id, status);

CREATE INDEX IF NOT EXISTS idx_leads_status
ON public.leads (status);

DO $$
BEGIN
	IF EXISTS (
		SELECT 1
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'leads'
			AND column_name = 'is_qualified'
	) THEN
		EXECUTE 'CREATE INDEX IF NOT EXISTS idx_leads_is_qualified ON public.leads (is_qualified)';
		EXECUTE 'CREATE INDEX IF NOT EXISTS idx_leads_user_is_qualified ON public.leads (user_id, is_qualified)';
	END IF;
END
$$;

-- Enrichment pipeline filter (used in dashboard + enrichment service)
CREATE INDEX IF NOT EXISTS idx_leads_enrichment_status
ON public.leads (enrichment_status);

CREATE INDEX IF NOT EXISTS idx_leads_user_enrichment_status
ON public.leads (user_id, enrichment_status);

-- Pipeline stage filter (CRM board + drip dispatch)
CREATE INDEX IF NOT EXISTS idx_leads_pipeline_stage
ON public.leads (pipeline_stage);

CREATE INDEX IF NOT EXISTS idx_leads_user_pipeline_stage
ON public.leads (user_id, pipeline_stage);

-- Drip mail scheduling: partial index on leads that need follow-up
CREATE INDEX IF NOT EXISTS idx_leads_user_next_mail_at_pending
ON public.leads (user_id, next_mail_at)
WHERE next_mail_at IS NOT NULL;

-- Enrichment queue: partial index on pending leads
CREATE INDEX IF NOT EXISTS idx_leads_user_enrichment_pending
ON public.leads (user_id, created_at)
WHERE enrichment_status = 'pending';
