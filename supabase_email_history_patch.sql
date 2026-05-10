-- Email Tracking + History schema patch
-- Safe to run multiple times.

CREATE TABLE IF NOT EXISTS public.communications (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    lead_id BIGINT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('outbound', 'inbound')),
    subject TEXT,
    body_html TEXT,
    body_text TEXT,
    status TEXT NOT NULL DEFAULT 'sent' CHECK (status IN ('sent', 'opened', 'replied', 'received', 'failed')),
    tracking_id TEXT,
    campaign_event_id BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_communications_user_id ON public.communications(user_id);
CREATE INDEX IF NOT EXISTS idx_communications_lead_id ON public.communications(lead_id);
CREATE INDEX IF NOT EXISTS idx_communications_timestamp ON public.communications(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_communications_tracking_id ON public.communications(tracking_id);
CREATE INDEX IF NOT EXISTS idx_communications_user_lead_ts ON public.communications(user_id, lead_id, timestamp DESC);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'leads'
    ) THEN
        ALTER TABLE public.communications
            DROP CONSTRAINT IF EXISTS communications_lead_id_fkey;

        ALTER TABLE public.communications
            ADD CONSTRAINT communications_lead_id_fkey
            FOREIGN KEY (lead_id)
            REFERENCES public.leads(id)
            ON DELETE CASCADE;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION public.set_communications_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_communications_updated_at ON public.communications;
CREATE TRIGGER trg_communications_updated_at
BEFORE UPDATE ON public.communications
FOR EACH ROW
EXECUTE FUNCTION public.set_communications_updated_at();

ALTER TABLE public.communications ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS communications_select_own ON public.communications;
CREATE POLICY communications_select_own
ON public.communications
FOR SELECT
USING (COALESCE(auth.uid()::text, '') = COALESCE(user_id, ''));

DROP POLICY IF EXISTS communications_insert_own ON public.communications;
CREATE POLICY communications_insert_own
ON public.communications
FOR INSERT
WITH CHECK (COALESCE(auth.uid()::text, '') = COALESCE(user_id, ''));

DROP POLICY IF EXISTS communications_update_own ON public.communications;
CREATE POLICY communications_update_own
ON public.communications
FOR UPDATE
USING (COALESCE(auth.uid()::text, '') = COALESCE(user_id, ''))
WITH CHECK (COALESCE(auth.uid()::text, '') = COALESCE(user_id, ''));

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication_tables
        WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'communications'
    ) THEN
        ALTER PUBLICATION supabase_realtime ADD TABLE public.communications;
    END IF;
END $$;
