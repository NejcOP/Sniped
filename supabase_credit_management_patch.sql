-- Credit management patch
-- Adds users.credits alias and credit_logs ledger table.
-- Safe to run multiple times.

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS credits bigint default 0;

UPDATE public.users
SET credits = COALESCE(credits_balance, credits, 0)
WHERE credits IS NULL OR credits != COALESCE(credits_balance, credits, 0);

CREATE TABLE IF NOT EXISTS public.credit_logs (
  id bigserial primary key,
  user_id text not null,
  amount integer not null,
  action_type text not null,
  metadata jsonb,
  created_at text not null default now()::text
);

CREATE INDEX IF NOT EXISTS idx_credit_logs_user_created
  ON public.credit_logs (user_id, created_at desc);

CREATE INDEX IF NOT EXISTS idx_credit_logs_action_created
  ON public.credit_logs (action_type, created_at desc);
