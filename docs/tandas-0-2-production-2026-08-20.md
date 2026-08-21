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
- A newer operational cut supersedes older queued manual refreshes. The worker also
  rejects stale refreshes at execution time, and publication metadata is monotonic,
  so an old request can never move the visible cut backwards.
- `/api/healthz` is DB-free. `/api/readyz` uses a short PostgreSQL statement timeout.
- API and worker use systemd readiness notifications, watchdogs, restart throttling,
  memory limits and the existing 2 GB swap file.

## Production verification (2026-08-20/21 UTC)

- Automated tests: `36 passed` after the ingestion and publication query changes.
- Live API and worker remained active with zero restarts during real Howen cuts.
- `/api/healthz`: HTTP 200 in approximately 2 ms.
- `/api/readyz`: HTTP 200 in approximately 52-57 ms, including the bounded
  Supabase readiness query.
- Authenticated reads while a cut was running:
  - `/api/auth/me`: 200 in 0.27 s.
  - `/api/dashboard?company=ismocol`: 200 in 0.22 s.
  - `/api/feed?company=ismocol`: 200 in 0.23 s.
  - `/api/admin/overview`: 200 in 2.90 s.
- Worker memory stayed below 110 MB during a real 45-device ISMOCOL cut, with no
  swap usage. Before the fix, publication and identity propagation reached roughly
  700 MB plus swap.
- Snapshot publication no longer loads the global `mileage_readings` table. The
  production database had 409,990 readings; publication now aggregates the
  company/device/day values in PostgreSQL and materializes roughly 177 daily rows
  for ISMOCOL.
- Snapshot benchmark: approximately 13.4 s and 120 MB peak RSS, with no swap. Alarm
  metrics matched the previous implementation exactly. The small provisional km
  movement observed between runs came from newer validated device status, not from
  a change in historical aggregation.
- A real ISMOCOL cut completed 45/45 devices and published successfully while the
  API remained available. Subsequent due cuts were kept as durable jobs and claimed
  by priority.

The monthly diagnostic audit remains a known expensive interactive query. It is not
executed by the worker and does not block health, authentication or snapshot reads,
but its reduction through daily aggregates belongs to the later performance tanda.

## Production acceptance commands

```bash
curl -fsS http://127.0.0.1:8000/api/healthz
curl -fsS http://127.0.0.1:8000/api/readyz
systemctl is-active dashboard-api.service dashboard-worker.service nginx
systemctl show dashboard-api.service dashboard-worker.service \
  -p ActiveState -p SubState -p WatchdogUSec -p MemoryCurrent -p MemoryPeak
```

The current Droplet has 2 GB RAM. The Tanda 2 software safeguards are complete:
separate processes, swap, cgroup limits, watchdogs, bounded readiness and durable
jobs. The recommended production capacity remains 4 GB RAM before onboarding more
fleets. That resize is an infrastructure control-panel action and does not alter
application data or code.

## Recovery and rollback

- Baseline tag: `v1-pre-batch`.
- API/worker baseline tag: `v1-api-worker-20260820.1`.
- Tandas 0-2 code tag: `v1-tanda-0-2`.
- Certified backup/restore and monotonic-cut marker:
  `v1-tanda-0-2-certified.1`.
- Restore source: the off-host and Droplet copies of `20260820T171446Z`.
- Immediate application rollback: deploy the desired tag and restart
  `dashboard-api.service` and `dashboard-worker.service`.
- Data rollback: restore the Supabase custom-format dump and the uploads/config
  archive from the same timestamp so code and data stay aligned.

The custom-format dump integrity and its 577-entry restore catalog were verified.
The dump was also restored into an isolated PostgreSQL 17 container without touching
production. The portable restore excluded only the Supabase-managed
`supabase_vault` extension and its `vault.secrets` data, which are unavailable in the
stock PostgreSQL image. Application table counts matched the baseline exactly except
for two audit rows and three anomaly rows written between the live baseline query and
the dump snapshot. This proves the application backup can be restored and explains
the small concurrent-capture delta.
