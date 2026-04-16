-- ============================================================
-- Sniped – Performance Indexes
-- Run once against your Supabase project.
-- Paste into the Supabase SQL Editor and execute.
-- ============================================================

-- Main filter column – status is used in almost every dashboard query
CREATE INDEX IF NOT EXISTS idx_leads_status
    ON leads (status);

-- Scrape pipeline lookup
CREATE INDEX IF NOT EXISTS idx_leads_search_keyword
    ON leads (search_keyword);

-- Mail / enrichment pipeline ordering
CREATE INDEX IF NOT EXISTS idx_leads_ai_score
    ON leads (ai_score);

-- CRM drip / follow-up scheduling
CREATE INDEX IF NOT EXISTS idx_leads_sent_at
    ON leads (sent_at);

CREATE INDEX IF NOT EXISTS idx_leads_next_mail_at
    ON leads (next_mail_at);

-- Worker assignment board
CREATE INDEX IF NOT EXISTS idx_leads_worker_id
    ON leads (worker_id);

-- Revenue/paid tracking
CREATE INDEX IF NOT EXISTS idx_leads_paid_at
    ON leads (paid_at);

-- Blacklist fast lookup
CREATE INDEX IF NOT EXISTS idx_lead_blacklist_kind_value
    ON lead_blacklist (kind, value);

-- Open-tracking pixel deduplication
CREATE INDEX IF NOT EXISTS idx_leads_open_tracking_token
    ON leads (open_tracking_token);

-- Composite: common dashboard filter combination (status + ai_score)
CREATE INDEX IF NOT EXISTS idx_leads_status_score
    ON leads (status, ai_score);

-- ── User niche lookup (for AI strategy & personalization) ──
CREATE INDEX IF NOT EXISTS idx_users_niche
    ON users (niche);
