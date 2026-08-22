# DMS Dashboard Backend

Backend local en FastAPI para el dashboard DMS basado en Howen VSS.

## Batch historico de activacion

Las reconstrucciones iniciales de empresas usan escritura masiva sin cambiar los cortes operativos de 15 minutos.

- `HISTORICAL_BATCH_MODE=activation_only` habilita el batch solo para activaciones.
- `HISTORICAL_BATCH_MODE=off` vuelve inmediatamente al pipeline individual anterior.
- `HISTORICAL_BATCH_MODE=all_historical` queda reservado para una ampliacion posterior.
- `HISTORICAL_BATCH_SIZE=500` controla el tamano de cada transaccion.
- `HISTORICAL_REBUILD_CHUNK_DAYS=1` permite progreso y reanudacion por dia.
- `HOWEN_EVIDENCE_MAX_RANGE_DAYS=1` evita que Howen trunque silenciosamente rangos amplios de clips.
- `HOWEN_EVIDENCE_RETENTION_DAYS=20` delimita el tramo reciente que Alarm Clips conserva de forma confiable.
- `HOWEN_HISTORICAL_PREFIX_FALLBACK=true` recupera el prefijo anterior con el API historico oficial por dispositivo.
- `HOWEN_ALARM_SOURCE=evidence_bulk` usa la misma consulta agrupada de Alarm Clips para los cortes de 15 minutos.
- `HOWEN_ALARM_SOURCE=official_device` restaura inmediatamente la consulta historica por vehiculo.

La activacion reconstruye tambien el kilometraje con el reporte diario oficial de Howen. Los huecos no se convierten en cero: quedan como revisiones manuales. La empresa solo se publica al superar `MILEAGE_REBUILD_MIN_COVERAGE_PCT`, salvo aprobacion degradada explicita del administrador. La desactivacion exige doble confirmacion y genera un backup JSONL junto con un archivo de los PDF cargados.

El benchmark reproducible no toca datos productivos. En PostgreSQL usa un esquema temporal que elimina al terminar:

```bash
uv run python scripts/benchmark_alarm_batch.py --rows 6000 --batch-size 500
uv run python scripts/benchmark_alarm_batch.py --configured-postgres --rows 6000 --batch-size 500
```

## Manejo con uv

Instalacion y sincronizacion:

```bash
cd back
cp .env.example .env
uv sync
```

Arranque local sin activar `.venv` manualmente, en dos terminales:

```bash
cd back
PROCESS_ROLE=api uv run uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

```bash
cd back
PROCESS_ROLE=worker uv run python -m app.worker
```

Si prefieres modo estable sin `--reload`:

```bash
PROCESS_ROLE=api uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Variables del login Howen:

- `INGEST_MODE=live`
- `HOWEN_USERNAME`
- `HOWEN_PASSWORD` o `HOWEN_PASSWORD_MD5`
- `HOWEN_TOKEN` y `HOWEN_PID` si quieres bootstrap de una sesion ya validada

Variables locales del portal:

- `JWT_SECRET`
- `SESSION_COOKIE_NAME`
- `SESSION_COOKIE_SECURE`
- `SESSION_TTL_MINUTES`
- `SEED_ADMIN_USERNAME`
- `SEED_ADMIN_PASSWORD`
- `SEED_CLIENT_PASSWORD`
- `ANOMALY_FUTURE_TOLERANCE_MINUTES`
- `LIVE_RETENTION_DAYS`
- `ANOMALY_RETENTION_DAYS`
- `CATCHUP_OVERLAP_MINUTES`
- `CATCHUP_BOOTSTRAP_HOURS`
- `CATCHUP_STALE_AFTER_MINUTES`
- `CATCHUP_MAX_WINDOW_MINUTES`
- `CATCHUP_DEVICE_BATCH_SIZE`
- `CATCHUP_CHECK_INTERVAL_MINUTES`
- `CATCHUP_RATE_LIMIT_BASE_SECONDS`
- `CATCHUP_RATE_LIMIT_MAX_SECONDS`
- `CATCHUP_ERROR_RETRY_SECONDS`
- `PUBLIC_DASHBOARD_URL`
- `PUBLIC_API_URL`

Si las credenciales Howen quedan vacias, la API iniciara pero la ingesta permanecera reconectando hasta que completes `HOWEN_USERNAME` y `HOWEN_PASSWORD` o `HOWEN_PASSWORD_MD5`.

## Postgres / Supabase

El backend soporta `PostgreSQL` por `psycopg`.

Ejemplo de produccion:

```env
DATABASE_URL=postgresql+psycopg://postgres:***@db.<project-ref>.supabase.co:5432/postgres?sslmode=require
SESSION_COOKIE_SECURE=true
```

Aplicar el esquema versionado antes de iniciar los servicios:

```bash
uv run alembic upgrade head
uv run alembic current --check-heads
```

El arranque de PostgreSQL no ejecuta `create_all`, reparaciones ni backfills. `ManagedCompany` es la fuente durable de empresas y el JSON de `storage/companies.json` se usa solo como seed inicial.

Para almacenar los informes en un bucket privado de Supabase:

```env
REPORT_STORAGE_BACKEND=supabase
SUPABASE_URL=https://<project-ref>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_REPORTS_BUCKET=dms-reports
```

Los PDF locales existentes se migran una sola vez con:

```bash
uv run python scripts/migrate_reports_to_object_storage.py
```

## Credenciales semilla

- Admin local: `admin / Admin123!`
- Cliente local: `ismocol / Cliente123!`

## Endpoints principales

- `GET /api/healthz`
- `GET /api/readyz`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `GET /api/dashboard?company=ismocol`
- `GET /api/feed?company=ismocol`
- `GET /api/reports`
- `GET /api/admin/ingestion/status`
- `GET /api/admin/overview?company=ismocol`
- `GET /api/admin/audit?company=ismocol&from=2026-07-30T00:00:00Z&to=2026-08-06T23:59:59Z`
- `GET /api/admin/vehicles?company=ismocol`
- `GET /api/admin/anomalies`
- `POST /api/admin/backfill`
- `POST /api/admin/reports`
