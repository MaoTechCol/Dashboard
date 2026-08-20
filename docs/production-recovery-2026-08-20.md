# Production recovery baseline - 2026-08-20

## Incident

The public portal stopped at session validation because the single FastAPI process
was unresponsive. The process had reached approximately 1.8 GB on a 2 GB Droplet
without swap and had already been killed three times by the kernel OOM killer.
Nginx remained healthy, but `/api/auth/me`, `/api/healthz`, and `/api/readyz` timed
out while a historical rebuild shared the API process.

## Recovery point

- Remote backup: `/root/dashboard-backups/20260820T171446Z`
- Off-host backup: `/Users/andrescarvajal/Documents/Maotech 2/Production Backups/Dashboard/20260820T171446Z`
- Supabase dump: `data/supabase.dump`, PostgreSQL custom format
- Restore catalog: `data/supabase.restore.list`
- Deployment archive: `data/deployment-tree.tar.gz`
- Upload and runtime storage: `data/storage.tar.gz`
- Baseline counts: `data/baseline-counts.json`
- File integrity: `SHA256SUMS`

The backup includes the backend environment and must remain private. Never commit
the backup directory or its contents.

## Baseline counts

| Layer | Total |
| --- | ---: |
| Devices | 116 |
| Howen raw alarms | 66,615 |
| Analytic DMS alarms | 12,881 |
| Alarm audit rows | 185,623 |
| Ingestion anomalies | 61,804 |
| Daily mileage snapshots | 1,360 |
| Manual reviews | 531 |
| Published company snapshots | 3 |
| Harvest runs | 921 |

## Restore procedure

1. Stop `dashboard-api.service` and `dashboard-worker.service`.
2. Restore the deployment archive under `/opt/dashboard`.
3. Restore the protected `.env`, systemd unit, Nginx configuration, and storage archive.
4. Restore Supabase into an empty recovery database with PostgreSQL 17:

   ```bash
   pg_restore --clean --if-exists --no-owner --no-acl \
     --dbname "$RECOVERY_DATABASE_URL" data/supabase.dump
   ```

5. Validate counts against `data/baseline-counts.json` before pointing the API at the recovered database.
6. Start the API and worker, then run `/api/healthz`, `/api/readyz`, session, dashboard, job queue, and report smoke tests.

## Worker separation

Historical rebuilds, 15-minute cuts, Howen, km maintenance, purges and snapshot
publication run in `dashboard-worker.service`. FastAPI only authenticates, reads
published state and enqueues durable jobs. The temporary Nginx rebuild guard can
be removed after both services and the queue heartbeat pass the smoke test.
