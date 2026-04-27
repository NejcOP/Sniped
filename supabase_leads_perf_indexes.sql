-- Supabase performance indexes for ultra-fast lead listing by user.
-- Safe to run multiple times.

CREATE INDEX IF NOT EXISTS idx_leads_user_id
ON public.leads (user_id);

CREATE INDEX IF NOT EXISTS idx_leads_user_created_at
ON public.leads (user_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_leads_user_scraped_at
ON public.leads (user_id, scraped_at DESC, id DESC);
