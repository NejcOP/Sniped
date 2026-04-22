ALTER TABLE public.leads
ADD COLUMN IF NOT EXISTS google_claimed boolean,
ADD COLUMN IF NOT EXISTS linkedin_url text,
ADD COLUMN IF NOT EXISTS instagram_url text,
ADD COLUMN IF NOT EXISTS facebook_url text,
ADD COLUMN IF NOT EXISTS qualification_score double precision;

CREATE INDEX IF NOT EXISTS idx_leads_qualification_score ON public.leads (qualification_score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_linkedin_url ON public.leads (linkedin_url);
CREATE INDEX IF NOT EXISTS idx_leads_instagram_url ON public.leads (instagram_url);
CREATE INDEX IF NOT EXISTS idx_leads_facebook_url ON public.leads (facebook_url);