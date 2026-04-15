# LeadGen Full-Stack (FastAPI + React)

Project reorganized into a modern full-stack system while preserving existing scraping and anti-bot intelligence.

## New structure

- backend/ -> FastAPI API + scraper logic + enrichment + AI mailer services
- frontend/ -> Vite + React dashboard
- archive/ -> previous standalone scripts moved here (not deleted)
- leads.db -> existing SQLite database remains in project root

## What was moved

The previous standalone logic was moved into backend services:

- Google Maps scraping logic available through backend API (uses backend/scraper)
- enrichment flow available through API endpoints
- AI mailer flow available through API endpoints

Previous standalone scripts are preserved in archive/:

- archive/main.py
- archive/enrichment.py
- archive/ai_mailer.py
- archive/streamlit_app.py

## Run the full stack with one command

From project root:

1. Activate your Python environment.
2. Install Python dependencies:

   pip install -r requirements.txt
   playwright install chromium

3. Install npm dependencies:

   npm install

4. Start backend + frontend together:

   npm run dev

Services:

- Backend API: http://localhost:8000
- Frontend App: http://localhost:5173

## API endpoints

- GET /api/health
- GET /api/leads?limit=250
- POST /api/scrape
- POST /api/export-targets
- POST /api/enrich
- POST /api/export-ai
- POST /api/mailer/send
- GET /api/supabase-health
- POST /api/supabase/sync-all
- POST /api/supabase/migrate-primary

## Optional: Supabase Sync

The app still works with SQLite as the primary local database. Supabase can be enabled as a mirror for cloud access and backup.

1. Create these tables in Supabase with matching names and compatible columns:
   - leads
   - workers
   - revenue_log
   - delivery_tasks
   - worker_audit_log
   - lead_blacklist
   - system_tasks (for task history in primary mode)
   - system_runtime (for scheduler runtime state in primary mode)
2. Configure Supabase keys in config.json:
   - supabase.url
   - supabase.publishable_key (or service_role_key)
   - supabase.service_role_key (recommended for server-side sync)
3. Optionally override via environment variables:
   - SUPABASE_URL
   - SUPABASE_PUBLISHABLE_KEY
   - SUPABASE_SERVICE_ROLE_KEY
4. Verify connectivity:
   - GET /api/supabase-health
5. Run a full sync if needed:
   - POST /api/supabase/sync-all

6. Enable Supabase as primary datastore (for CRM/revenue/workers/delivery APIs):
   - POST /api/supabase/migrate-primary
   - This performs a sync and sets `supabase.primary_mode = true` in config.json.

Write operations in the API now trigger automatic sync attempts when Supabase is configured.

When `supabase.primary_mode` is true, key dashboard APIs read/write directly against Supabase tables.
Task tracking and scheduler runtime keys also use Supabase when `system_tasks` and `system_runtime` tables are present.

## Stripe Production Webhook

If the backend is deployed on Railway, configure Stripe to call the hosted webhook endpoint instead of a local CLI tunnel.

Production webhook URL:

- `https://sniped-production.up.railway.app/api/stripe/webhook`

Railway environment variables:

- `STRIPE_SECRET_KEY` = your live Stripe secret key
- `STRIPE_WEBHOOK_SECRET` = the webhook signing secret from Stripe Dashboard (`whsec_...`)

Recommended Stripe events for this app:

- `checkout.session.completed`
- `invoice.payment_succeeded`
- `invoice.paid`
- `invoice.payment_failed`
- `customer.subscription.updated`
- `customer.subscription.deleted`

Stripe Dashboard setup:

1. Open Stripe Dashboard -> Developers -> Webhooks.
2. Click Add endpoint.
3. Enter `https://sniped-production.up.railway.app/api/stripe/webhook`.
4. Select the events listed above.
5. Save the endpoint and copy the Signing secret.
6. Add that value to Railway as `STRIPE_WEBHOOK_SECRET`.
7. Redeploy Railway after saving the env var.

Notes:

- The webhook route verifies the `Stripe-Signature` header using `STRIPE_WEBHOOK_SECRET`.
- If `STRIPE_WEBHOOK_SECRET` is missing, the endpoint still accepts payloads, but production should always use signature verification.
- Checkout and billing flows also require the matching live `STRIPE_SECRET_KEY` and live price IDs.

## Notes

- Existing data in leads.db is reused by default.
- Existing profile folders and anti-bot behavior are preserved via backend scraper modules.
- If you need old CLI behavior, you can still run files from archive/ manually.
