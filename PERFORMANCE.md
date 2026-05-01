# Sniped.io — Production Performance Results

> Last updated: 2026-05-01  
> Infrastructure: Railway (single replica) · Supabase shared pooler (200-connection limit)  
> Benchmark tool: [Locust 2.43.4](https://locust.io)  
> Target: `https://sniped-production.up.railway.app`

---

## Summary

| Concurrent Users | Total Requests | Failures | Throughput | Avg Latency | p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|
| **500** | 6,229 | **0 (0.00%)** | 80.4 req/s | 4,950 ms | 4,300 ms | 13,000 ms | 16,000 ms |
| **1,000** | 5,515 | **0 (0.00%)** | 61.4 req/s | 13,067 ms | 11,000 ms | 31,000 ms | 34,000 ms |

Zero failures at both load levels. **The application does not crash at 1,000 concurrent users.**

---

## 500 Concurrent Users — Detail

Run time: 90 s · Spawn rate: 80 users/s

| Endpoint | Requests | Failures | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|
| GET /api/config | 521 | 0 | 4,626 | 4,000 | 12,000 | 14,000 |
| GET /api/health | 1,125 | 0 | 3,541 | 2,000 | 10,000 | 11,000 |
| GET /api/jobs | 518 | 0 | 5,851 | 5,200 | 15,000 | 17,000 |
| GET /api/leads | 2,545 | 0 | 5,267 | 4,700 | 13,000 | 17,000 |
| GET /api/scrape | 482 | 0 | 5,506 | 4,800 | 13,000 | 16,000 |
| GET /api/workers | 1,038 | 0 | 5,155 | 4,300 | 13,000 | 17,000 |
| **Aggregated** | **6,229** | **0** | **4,950** | **4,300** | **13,000** | **16,000** |

---

## 1,000 Concurrent Users — Detail (post-optimisation)

Run time: 90 s · Spawn rate: 80 users/s  
Optimisations applied: `APP_THREADPOOL_WORKERS=4`, wider pool caps, 8 new DB indexes.

| Endpoint | Requests | Failures | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|
| GET /api/config | 458 | 0 | 12,907 | 10,000 | 32,000 | 35,000 |
| GET /api/health | 941 | 0 | 11,106 | 9,000 | 29,000 | 32,000 |
| GET /api/jobs | 431 | 0 | 13,322 | 11,000 | 32,000 | 34,000 |
| GET /api/leads | 2,315 | 0 | 13,277 | 11,000 | 32,000 | 34,000 |
| GET /api/scrape | 471 | 0 | 14,644 | 12,000 | 33,000 | 35,000 |
| GET /api/workers | 899 | 0 | 13,715 | 11,000 | 32,000 | 35,000 |
| **Aggregated** | **5,515** | **0** | **13,067** | **11,000** | **31,000** | **34,000** |

### Improvement vs. baseline (1,000 users, before optimisation)

| Metric | Before | After | Change |
|---|---|---|---|
| Total requests served | 2,052 | **5,515** | **+169%** |
| Failures | 0 | **0** | unchanged |
| p95 latency | 50,000 ms | **31,000 ms** | **−38%** |
| p99 latency | 62,000 ms | **34,000 ms** | **−45%** |
| Max latency | 75,118 ms | **36,154 ms** | **−52%** |

---

## Optimisations Applied

### Infrastructure (Railway env vars)
| Variable | Value | Purpose |
|---|---|---|
| `APP_THREADPOOL_WORKERS` | `4` | Double FastAPI thread pool workers (was 2) |
| `SUPABASE_POOLER_POOL_SIZE_CAP` | `10` | Per-replica pool (was 3) |
| `SUPABASE_POOLER_MAX_OVERFLOW_CAP` | `20` | Per-replica overflow (was 3) |
| `DB_POOL_TIMEOUT` | `10` | Fail fast on queue wait (was 15 s) |
| `RUN_STARTUP_JOBS` | `0` | Prevent startup tasks hammering DB under load |

**Pool math:** `(10 pool + 20 overflow) × 4 replicas = 120 connections`, safely within the 200-connection Supabase pooler limit.

### Database indexes added
```sql
-- enrichment pipeline
idx_leads_enrichment_status         ON leads (enrichment_status)
idx_leads_user_enrichment_status    ON leads (user_id, enrichment_status)

-- CRM pipeline board
idx_leads_pipeline_stage            ON leads (pipeline_stage)
idx_leads_user_pipeline_stage       ON leads (user_id, pipeline_stage)

-- drip mail scheduling (partial — only rows with pending mail)
idx_leads_user_next_mail_at_pending ON leads (user_id, next_mail_at)
  WHERE next_mail_at IS NOT NULL

-- enrichment queue (partial — only pending rows)
idx_leads_user_enrichment_pending   ON leads (user_id, created_at)
  WHERE enrichment_status = 'pending'
```

### Resilience hardening (code)
- **503 graceful degradation** on `/api/leads` and `/api/scrape` when pool is exhausted
- **Pool saturation monitoring** — every EMAXCONN error calls `record_pool_saturation_event()`;
  after 5 events in 60 s it emits a `CRITICAL [POOL_SATURATION]` log line for Railway alerting
- **Engine cooldown** — prevents reconnect storms after consecutive failures
- **Background job gating** (`RUN_STARTUP_JOBS=False`) — prevents scheduler threads from
  competing for connections during saturation events
- **`pool_use_lifo=True`** — keeps fewer idle connections warm

---

## Architecture

```
Browser / AppSumo users
        │
        ▼
  Vercel (frontend)
        │  HTTPS
        ▼
  Railway (FastAPI, 1 replica)
  ├── APP_THREADPOOL_WORKERS = 4
  ├── pool_size = 10 / max_overflow = 20
  └── uvicorn --workers 1
        │  port 6543 (transaction-mode pooler)
        ▼
  Supabase shared pooler  (200 conn limit)
        │
        ▼
  Supabase Postgres (eu-west-1)
```

---

## Known Bottlenecks & Next Steps

| Priority | Bottleneck | Recommended Action |
|---|---|---|
| High | Supabase shared pooler (200 conn global cap) | Upgrade to Supabase Pro (dedicated pooler) |
| Medium | p95 latency at 1,000 users (~31 s) | Add Railway horizontal scaling (2–4 replicas) |
| Low | Single uvicorn worker | Already mitigated by `APP_THREADPOOL_WORKERS=4`; uvicorn multi-worker would require stateless sessions |

---

*Benchmarks run from a local machine (Locust headless) against the live production URL.
Results reflect real network latency between the test client and Railway EU-WEST.*
