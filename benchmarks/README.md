# Locust Benchmark Suite

## Purpose

Repeatable concurrency benchmark for Sniped.io endpoints with realistic authenticated traffic.

## Files

- `benchmarks/locustfile.py`: load profile
- `benchmarks/run_locust_ramp.ps1`: staged ramp runner
- `benchmarks/rr_proxy.py`: local round-robin reverse proxy for horizontal simulation
- `benchmarks/results/`: CSV outputs

## Quick start

1. Create `.env` from `.env.example` and fill secrets.

2. Start backend in multi-worker mode:

```powershell
.\scripts\start_backend_multiworker.ps1
```

3. Save auth token in a file (avoid passing secrets on CLI):

```powershell
Set-Content -Path benchmarks/results/loadtest_token.txt -Value "<token>"
```

4. Run ramp test:

```powershell
.\benchmarks\run_locust_ramp.ps1
```

## Horizontal scaling simulation (optional)

Start 2 app instances with 2 workers each behind local round-robin proxy:

```powershell
.\scripts\start_horizontal_sim.ps1 -InstanceCount 2 -WorkersPerInstance 2 -ProxyPort 8000
```

Stop the simulation:

```powershell
.\scripts\stop_horizontal_sim.ps1
```

## Optional knobs

- `LOCUST_LEADS_LIMIT` (default `50`)
- `LOCUST_SEARCH_TERMS` (default `roof,dental,law,clinic`)
- `LOCUST_ENABLE_SCRAPE=1` to include `POST /api/scrape`
- `run_locust_ramp.ps1 -AuthTokenFile <path>` to read token from a specific file

## Notes

- Without a token in `AuthToken` / `AuthTokenFile` / `LOCUST_AUTH_TOKEN`, authenticated endpoints return `401` and results are not representative.
- For pool tuning, focus on p95, failure rate, and RPS at each stage.
