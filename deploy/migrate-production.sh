#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/dashboard}"

cd "$APP_DIR/back"

# Schema changes are applied once, before API and worker restart. Neither
# service mutates the PostgreSQL schema during startup.
PROCESS_ROLE=worker uv run --no-sync alembic upgrade head
PROCESS_ROLE=worker uv run --no-sync alembic current --check-heads

