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

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'plan_key'
  ) THEN
    EXECUTE $sql$
      UPDATE public.users
      SET plan_key = 'empire'
      WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io'
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'subscription_active'
  ) THEN
    EXECUTE $sql$
      UPDATE public.users
      SET subscription_active = TRUE
      WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io'
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'subscription_status'
  ) THEN
    EXECUTE $sql$
      UPDATE public.users
      SET subscription_status = 'active'
      WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io'
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'credits_balance'
  ) THEN
    EXECUTE $sql$
      UPDATE public.users
      SET credits_balance = 1000000000
      WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io'
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'credits_limit'
  ) THEN
    EXECUTE $sql$
      UPDATE public.users
      SET credits_limit = 100000
      WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io'
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'monthly_limit'
  ) THEN
    EXECUTE $sql$
      UPDATE public.users
      SET monthly_limit = 100000
      WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io'
    $sql$;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'monthly_quota'
  ) THEN
    EXECUTE $sql$
      UPDATE public.users
      SET monthly_quota = 100000
      WHERE LOWER(COALESCE(email, '')) = 'info@sniped.io'
    $sql$;
  END IF;
END $$;

UPDATE public.users
SET is_blocked = FALSE
WHERE is_blocked IS NULL;

CREATE INDEX IF NOT EXISTS idx_users_is_admin ON public.users(is_admin);
CREATE INDEX IF NOT EXISTS idx_users_last_login_at ON public.users(last_login_at);
CREATE INDEX IF NOT EXISTS idx_users_is_blocked ON public.users(is_blocked);
