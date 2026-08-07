# DMS Dashboard Backend

Backend local en FastAPI para el dashboard DMS basado en Howen VSS.

## Manejo con uv

Instalacion y sincronizacion:

```bash
cd back
cp .env.example .env
uv sync
```

Arranque local sin activar `.venv` manualmente:

```bash
cd back
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

Si prefieres modo estable sin `--reload`:

```bash
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Variables del login Howen:

- `INGEST_MODE=live`
- `HOWEN_USERNAME`
- `HOWEN_PASSWORD` o `HOWEN_PASSWORD_MD5`
- `HOWEN_TOKEN` y `HOWEN_PID` si quieres bootstrap de una sesion ya validada

Variables locales del portal:

- `JWT_SECRET`
- `SESSION_COOKIE_NAME`
- `SESSION_TTL_MINUTES`
- `SEED_ADMIN_USERNAME`
- `SEED_ADMIN_PASSWORD`
- `SEED_CLIENT_PASSWORD`
- `ANOMALY_FUTURE_TOLERANCE_MINUTES`
- `LIVE_RETENTION_DAYS`
- `ANOMALY_RETENTION_DAYS`

Si las credenciales Howen quedan vacias, la API iniciara pero la ingesta permanecera reconectando hasta que completes `HOWEN_USERNAME` y `HOWEN_PASSWORD` o `HOWEN_PASSWORD_MD5`.

## Credenciales semilla

- Admin local: `admin / Admin123!`
- Cliente local: `ismocol / Cliente123!`

## Endpoints principales

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
