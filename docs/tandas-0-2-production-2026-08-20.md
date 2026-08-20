# Tandas 0-2 production closure

## Tanda 0: recovery baseline

- Git tag: `v1-pre-batch` at commit `00a9769`.
- Off-host and Droplet backup: `20260820T171446Z`.
- Contents: Supabase custom dump, restore catalog, application tree, uploads,
  environment, Nginx, systemd unit, users and operational table counts.
- Integrity: SHA-256 verified on both copies; PostgreSQL 17 parsed all 577 archive
  entries.
- Frontend bootstrap: the session request has a 10-second timeout, retry action and
  a real service-unavailable screen instead of an infinite validation state.

## Tanda 1: API and worker

- `dashboard-api.service` serves authentication, published reads and job enqueueing.
- `dashboard-worker.service` owns Howen, historical cuts, rebuilds, maintenance and
  publication.
- Jobs are durable in Supabase with priority, leases, heartbeats, attempts and
  PostgreSQL `FOR UPDATE SKIP LOCKED` claiming.
- Historical batch ingestion remains the rebuild engine and never runs in FastAPI.

## Tanda 2: cadence and availability

- One newest queued harvest per company; obsolete queued cuts are marked
  `superseded` and remain auditable.
- A newer cut absorbs the complete unpublished gap plus configured overlap.
- Successful devices are retained across retries.
- Provider throttling schedules a retry and releases the worker for another company.
- Manual refreshes are coalesced with the active cut whenever possible.
- `/api/healthz` is DB-free. `/api/readyz` uses a short PostgreSQL statement timeout.
- API and worker use systemd readiness notifications, watchdogs, restart throttling,
  memory limits and the existing 2 GB swap file.

## Production acceptance commands

```bash
curl -fsS http://127.0.0.1:8000/api/healthz
curl -fsS http://127.0.0.1:8000/api/readyz
systemctl is-active dashboard-api.service dashboard-worker.service nginx
systemctl show dashboard-api.service dashboard-worker.service \
  -p ActiveState -p SubState -p WatchdogUSec -p MemoryCurrent -p MemoryPeak
```

The current Droplet has 2 GB RAM. The software safeguards are complete, but the
recommended production capacity remains 4 GB RAM before onboarding more fleets.
That resize is an infrastructure control-panel action and does not alter application
data or code.
