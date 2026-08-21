#!/usr/bin/env bash
set -euo pipefail

umask 077

APP_DIR="${APP_DIR:-/opt/dashboard}"
BACKUP_ROOT="${BACKUP_ROOT:-/root/dashboard-backups}"
STAMP="${BACKUP_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
DESTINATION="${BACKUP_DESTINATION:-$BACKUP_ROOT/$STAMP}"

mkdir -p "$DESTINATION/data" "$DESTINATION/config"

DATABASE_URL="${DATABASE_URL:-$(
  cd "$APP_DIR/back"
  uv run python -c 'from app.core.settings import get_settings; print(get_settings().database_url)'
)}"

pg_dump --format=custom --no-owner --no-acl \
  --file "$DESTINATION/data/supabase.dump" \
  "$DATABASE_URL"
pg_restore --list "$DESTINATION/data/supabase.dump" > "$DESTINATION/data/supabase.restore.list"

tar -C "$APP_DIR" \
  --exclude='.git' \
  --exclude='back/.venv' \
  --exclude='front/node_modules' \
  --exclude='front/dist' \
  -czf "$DESTINATION/data/deployment-tree.tar.gz" .

if [[ -d "$APP_DIR/back/storage" ]]; then
  tar -C "$APP_DIR/back" -czf "$DESTINATION/data/storage.tar.gz" storage
fi

for source in \
  "$APP_DIR/back/.env" \
  /etc/systemd/system/dashboard-api.service \
  /etc/systemd/system/dashboard-worker.service \
  /etc/nginx/sites-available/dashboard; do
  if [[ -f "$source" ]]; then
    cp --preserve=mode,timestamps "$source" "$DESTINATION/config/$(basename "$source")"
  fi
done

(
  cd "$APP_DIR/back"
  uv run python - "$DESTINATION/data/baseline-counts.json" <<'PY'
import json
import sys

from sqlalchemy import func, select

from app.bootstrap import build_context
from app.models import (
    AlarmEvent,
    AlarmHarvestRun,
    BackgroundJob,
    DailyMileageSnapshot,
    DeviceRecord,
    HowenAlarmRaw,
    IngestionAnomaly,
    ManagedCompany,
    PublishedDashboardSnapshot,
    ReconciliationReview,
)

models = {
    "devices": DeviceRecord,
    "howen_alarm_raw": HowenAlarmRaw,
    "alarm_events": AlarmEvent,
    "ingestion_anomalies": IngestionAnomaly,
    "daily_mileage_snapshots": DailyMileageSnapshot,
    "manual_reviews": ReconciliationReview,
    "published_snapshots": PublishedDashboardSnapshot,
    "harvest_runs": AlarmHarvestRun,
    "background_jobs": BackgroundJob,
    "managed_companies": ManagedCompany,
}
context = build_context(seed_users=False)
with context.session_factory() as session:
    counts = {name: int(session.scalar(select(func.count()).select_from(model)) or 0) for name, model in models.items()}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(counts, handle, indent=2, sort_keys=True)
PY
)

if git -C "$APP_DIR" rev-parse HEAD > "$DESTINATION/data/git-commit.txt" 2>/dev/null; then
  git -C "$APP_DIR" describe --tags --always --dirty > "$DESTINATION/data/git-version.txt"
elif [[ -s "$APP_DIR/DEPLOYED_RELEASE" ]]; then
  cp "$APP_DIR/DEPLOYED_RELEASE" "$DESTINATION/data/git-commit.txt"
  cp "$APP_DIR/DEPLOYED_RELEASE" "$DESTINATION/data/git-version.txt"
else
  printf '%s\n' "unknown" > "$DESTINATION/data/git-commit.txt"
  printf '%s\n' "unknown" > "$DESTINATION/data/git-version.txt"
fi
systemctl is-active dashboard-api.service dashboard-worker.service nginx \
  > "$DESTINATION/data/service-state.txt" || true

(
  cd "$DESTINATION"
  find data config -type f -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
)

printf '%s\n' "$DESTINATION"
