ALTER TABLE public.leads
ADD COLUMN IF NOT EXISTS maps_url text;
