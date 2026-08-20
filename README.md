# Dashboard DMS Local

Implementacion local del dashboard DMS en dos capas:

- `back/`: API en FastAPI con autenticacion local, ingestion `live`, websocket Howen -> backend, snapshot REST, diagnostico admin, auditoria y carga de reportes mensuales.
- `front/`: portal React + Vite con login, dashboard por empresa y polling autenticado hacia la API.

## Lo que ya queda listo

- Login local real en `POST /api/auth/login` con roles `admin` y `client`, cookie `HttpOnly` y empresas visibles por sesion.
- Contrato Howen preparado para `POST /user/apiLogin.action`, catalogo de vehiculos y websocket `80000 / 80001 / 80009`.
- Agregaciones del dashboard segun las reglas del documento: 24h exactas, semana, 30 dias, comparativos por vehiculo, patrones, notas de calidad y reportes mensuales.
- Panel admin local para estado de ingesta, overview operativo, anomalias, backfill manual y auditoria.

## Arranque local

### Backend API

```bash
cd back
cp .env.example .env
uv sync
PROCESS_ROLE=api uv run uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

### Worker

En una segunda terminal:

```bash
cd back
PROCESS_ROLE=worker uv run python -m app.worker
```

### Frontend

```bash
cd front
npm install
npm run dev -- --host 127.0.0.1 --port 5173 --strictPort
```

El front queda apuntando por defecto a `http://127.0.0.1:8000/api`.

No necesitas activar `.venv` manualmente si usas `uv run`.

## Credenciales semilla locales

- `admin / Admin123!`
- `ismocol / Cliente123!`

## Modo live

Para conectarlo al servidor Howen real, cambia el `.env`:

```env
INGEST_MODE=live
HOWEN_HTTP_BASE=http://172.86.110.17:9966/vss
HOWEN_WS_URL=ws://172.86.110.17:36300/ws
HOWEN_WSS_URL=wss://172.86.110.17:36301/ws
HOWEN_USERNAME=...
HOWEN_PASSWORD=...
HOWEN_PASSWORD_MD5=...
HOWEN_TOKEN=...
HOWEN_PID=...
```

Notas:

- Si pones `HOWEN_TOKEN` y `HOWEN_PID`, el backend los usa solo como bootstrap opcional y luego reautentica con credenciales durables si la sesion cae.
- El flujo websocket implementado es el que se valido en Postman: `80000`, luego `80001` con payload vacio y heartbeat `80009` cada 60 segundos.
- El navegador no depende de websocket para redibujar graficas. El dashboard principal cambia en el corte de 15 minutos o por refresh manual.
- El backend ya trae `uv.lock`, asi que `uv sync` recrea el entorno con las versiones bloqueadas.
- El `subtype_map` del cliente en `back/app/data/companies.json` queda preparado para completar los codigos `tp` reales que aun no venian completos en la documentacion.
- Si faltan `HOWEN_USERNAME` y `HOWEN_PASSWORD` o `HOWEN_PASSWORD_MD5`, la API puede arrancar pero la ingesta quedara reconectando hasta que completes esas credenciales reales.
- Para Supabase en produccion, usa `DATABASE_URL=postgresql+psycopg://...?...sslmode=require`.
- Para cookies de sesion sobre HTTPS, activa `SESSION_COOKIE_SECURE=true`.

## Estructura util

- `back/app/main.py`
- `back/app/services/howen.py`
- `back/app/services/dashboard.py`
- `front/src/App.tsx`
- `front/src/hooks/useDashboardStream.ts`

## Redeploy limpio

- Variables de produccion backend: [back/.env.production.example](/Users/andrescarvajal/Documents/Maotech%202/Dashboard/back/.env.production.example)
- Variables de produccion frontend: [front/.env.production.example](/Users/andrescarvajal/Documents/Maotech%202/Dashboard/front/.env.production.example)
- Guia Ubuntu: [docs/redeploy-ubuntu.md](/Users/andrescarvajal/Documents/Maotech%202/Dashboard/docs/redeploy-ubuntu.md)
- Servicios `systemd`: `deploy/dashboard-api.service.example` y `deploy/dashboard-worker.service.example`.
- La API usa `PROCESS_ROLE=api`: autentica, consulta snapshots y encola trabajos; no abre Howen ni ejecuta reconstrucciones.
- El worker usa `PROCESS_ROLE=worker`: mantiene status/km, consume la cola persistida, ejecuta cortes, reconstrucciones, purgas y publica snapshots.
- En local abre dos terminales dentro de `back`: `PROCESS_ROLE=api uv run uvicorn app.main:app --host 127.0.0.1 --port 8000` y `PROCESS_ROLE=worker uv run python -m app.worker`.
- Reverse proxy `nginx`: [deploy/nginx.dashboard.monitoreocotaba.conf.example](/Users/andrescarvajal/Documents/Maotech%202/Dashboard/deploy/nginx.dashboard.monitoreocotaba.conf.example)
- Smoke test: [scripts/smoke_test.py](/Users/andrescarvajal/Documents/Maotech%202/Dashboard/scripts/smoke_test.py)
