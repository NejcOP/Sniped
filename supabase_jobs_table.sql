-- ============================================================
-- Sniped – Job Queue Table
-- Run in Supabase SQL Editor (once).
-- ============================================================

CREATE TABLE IF NOT EXISTS jobs (
    id            BIGSERIAL PRIMARY KEY,
    user_id       TEXT       NOT NULL DEFAULT 'default',
    type          TEXT       NOT NULL,                         -- 'scrape' | 'enrich' | 'mailer'
    status        TEXT       NOT NULL DEFAULT 'pending',       -- pending | processing | completed | failed
    payload       JSONB      NOT NULL DEFAULT '{}',
    result        JSONB,
    error         TEXT,
    worker_id     TEXT,                                        -- which worker instance picked it up
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fast poll by workers (pending jobs ordered by created_at)
CREATE INDEX IF NOT EXISTS idx_jobs_status_created
    ON jobs (status, created_at ASC);

-- Per-user job lookup from frontend
CREATE INDEX IF NOT EXISTS idx_jobs_user_id_status
    ON jobs (user_id, status);

-- Auto-update updated_at on every row change
CREATE OR REPLACE FUNCTION jobs_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION jobs_set_updated_at();

-- Lightweight status-only view for frontend polling
CREATE OR REPLACE VIEW jobs_status AS
SELECT id, user_id, type, status, error, created_at, started_at, completed_at, updated_at
FROM jobs;
