#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Uso: $0 /ruta/al/respaldo" >&2
  exit 2
fi

BACKUP_DIR="$1"

(
  cd "$BACKUP_DIR"
  sha256sum --check SHA256SUMS
)

pg_restore --list "$BACKUP_DIR/data/supabase.dump" > /dev/null
tar -tzf "$BACKUP_DIR/data/deployment-tree.tar.gz" > /dev/null
if [[ -f "$BACKUP_DIR/data/storage.tar.gz" ]]; then
  tar -tzf "$BACKUP_DIR/data/storage.tar.gz" > /dev/null
fi

if [[ -n "${RECOVERY_DATABASE_URL:-}" ]]; then
  pg_restore --clean --if-exists --no-owner --no-acl \
    --dbname "$RECOVERY_DATABASE_URL" \
    "$BACKUP_DIR/data/supabase.dump"
fi

echo "Respaldo verificado: $BACKUP_DIR"
