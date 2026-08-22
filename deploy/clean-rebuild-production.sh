#!/usr/bin/env bash
set -euo pipefail

if [[ "${CONFIRM_CLEAN_REBUILD:-}" != "YES" ]]; then
  echo "Operacion cancelada. Define CONFIRM_CLEAN_REBUILD=YES." >&2
  exit 2
fi

APP_DIR="${APP_DIR:-/opt/dashboard}"
BACKUP_PATH="${BACKUP_PATH:?Define BACKUP_PATH con un respaldo ya verificado}"
REBUILD_DAYS="${REBUILD_DAYS:-30}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
MANIFEST_PATH="$APP_DIR/back/storage/clean_rebuild_manifests/clean-rebuild-$STAMP.json"

bash "$APP_DIR/deploy/verify-production-backup.sh" "$BACKUP_PATH"

systemctl stop dashboard-worker.service
if systemctl is-active --quiet dashboard-worker.service; then
  echo "No se pudo detener dashboard-worker.service" >&2
  exit 1
fi

cd "$APP_DIR/back"
if ! uv run --no-sync python -m scripts.clean_rebuild_operational_data \
  --execute \
  --days "$REBUILD_DAYS" \
  --backup-path "$BACKUP_PATH" \
  --manifest-path "$MANIFEST_PATH"; then
  echo "La limpieza fallo. El worker queda detenido y mantenimiento permanece activo." >&2
  exit 1
fi

systemctl start dashboard-worker.service
systemctl is-active --quiet dashboard-api.service
systemctl is-active --quiet dashboard-worker.service
systemctl is-active --quiet nginx

echo "Reconstruccion limpia encolada. Manifiesto: $MANIFEST_PATH"
