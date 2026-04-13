-- Run this in Supabase SQL Editor if auth/users table is missing.

CREATE TABLE IF NOT EXISTS public.users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    niche TEXT NOT NULL DEFAULT 'B2B Service Provider',
    account_type TEXT NOT NULL DEFAULT 'entrepreneur',
    display_name TEXT NOT NULL DEFAULT '',
    contact_name TEXT NOT NULL DEFAULT '',
    token TEXT UNIQUE,
    reset_token TEXT,
    reset_token_expires_at TEXT,
    created_at TEXT NOT NULL
);

ALTER TABLE public.users ADD COLUMN IF NOT EXISTS niche TEXT NOT NULL DEFAULT 'B2B Service Provider';
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'entrepreneur';
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '';
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS contact_name TEXT NOT NULL DEFAULT '';
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS token TEXT;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS reset_token TEXT;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS reset_token_expires_at TEXT;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS created_at TEXT NOT NULL DEFAULT NOW()::text;

CREATE INDEX IF NOT EXISTS idx_users_token
    ON public.users(token);

CREATE INDEX IF NOT EXISTS idx_users_reset_token
    ON public.users(reset_token);

CREATE INDEX IF NOT EXISTS idx_users_niche
    ON public.users(niche);
