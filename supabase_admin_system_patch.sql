-- Admin system bootstrap patch
-- Run this once in Supabase SQL Editor.

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS last_login_at TEXT,
  ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS blocked_at TEXT,
  ADD COLUMN IF NOT EXISTS blocked_reason TEXT;

UPDATE public.users
SET is_admin = TRUE
WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io';

UPDATE public.users
SET is_blocked = FALSE
WHERE is_blocked IS NULL;

CREATE INDEX IF NOT EXISTS idx_users_is_admin ON public.users(is_admin);
CREATE INDEX IF NOT EXISTS idx_users_last_login_at ON public.users(last_login_at);
CREATE INDEX IF NOT EXISTS idx_users_is_blocked ON public.users(is_blocked);
