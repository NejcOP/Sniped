# LeadGen Full-Stack (FastAPI + React)

Project runs as a Supabase-backed full-stack system.

## New structure

- backend/ -> FastAPI API + scraper logic + enrichment + AI mailer services
- frontend/ -> Vite + React dashboard
- Supabase -> single source of truth for auth, billing, leads, tasks, and app state

## What was moved

The previous standalone logic was moved into backend services:

- Google Maps scraping logic available through backend API (uses backend/scraper)
- enrichment flow available through API endpoints
- AI mailer flow available through API endpoints

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
- GET /api/auth/me
- GET /api/leads?limit=250
- POST /api/scrape
- POST /api/export-targets
- POST /api/enrich
- POST /api/export-ai
- POST /api/mailer/send
- GET /api/leads/{id}/report
- POST /api/leads/{id}/report/share
- GET /public/report/{token}
- POST /api/create-checkout-session
- GET /api/supabase-health
- POST /api/supabase/sync-all
- POST /api/supabase/migrate-primary

## Supabase-Only Backend

The backend expects Supabase as the only supported datastore for both local development and production.

1. Create these tables in Supabase with matching names and compatible columns:
   - users
   - leads
   - workers
   - revenue_log
   - delivery_tasks
   - worker_audit_log
   - lead_blacklist
   - system_tasks (for task history in primary mode)
   - system_runtime (for scheduler runtime state in primary mode)

2. Configure environment variables:
   - SUPABASE_URL
   - SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY
   - DATABASE_URL (recommended for external tools and schema migrations)
   - SUPABASE_PUBLISHABLE_KEY (optional when service-role key is present)

3. Verify connectivity:
   - GET /api/supabase-health

The backend now starts in Supabase-only mode. If Supabase credentials are missing, startup fails instead of falling back to a local `.db` file.

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

- Existing profile folders and anti-bot behavior are preserved via backend scraper modules.
- If you need old CLI behavior, you can still run files from archive/ manually.

## SMTP fallback and reply webhook

You can run outreach without per-user SMTP setup by configuring a single system sender account.

Required environment variables:

- SNIPED_SYSTEM_SMTP_HOST
- SNIPED_SYSTEM_SMTP_PORT (usually 587)
- SNIPED_SYSTEM_SMTP_EMAIL
- SNIPED_SYSTEM_SMTP_PASSWORD

Optional environment variables:

- SNIPED_SYSTEM_SMTP_FROM_NAME
- SNIPED_SYSTEM_SMTP_USE_TLS (default true)
- SNIPED_SYSTEM_SMTP_USE_SSL (default false)
- SNIPED_SYSTEM_SMTP_SIGNATURE
- SNIPED_SYSTEM_SMTP_SEND_LIMIT (default 50)

Behavior:

- Free plan users can send through system SMTP up to the system limit (default 50).
- Custom SMTP testing/saving is paid-plan only.
- Paid users can connect their own SMTP account and send from their own mailbox.

Incoming reply webhook:

- Endpoint: POST /api/webhooks/incoming-email
- Secret header: x-sniped-webhook-secret
- Configure secret with SNIPED_INBOUND_WEBHOOK_SECRET

Minimal JSON payload example:

{
   "event_type": "reply",
   "thread_token": "OPEN_TRACKING_OR_THREAD_TOKEN",
   "email": "lead@example.com",
   "from_email": "lead@example.com",
   "subject_line": "Re: quick question"
}

When a reply is recorded, the matched lead is moved to pipeline stage Replied.
