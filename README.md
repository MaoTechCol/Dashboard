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

### Backend

```bash
cd back
cp .env.example .env
uv sync
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
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
- El navegador no depende de websocket para redibujar graficas. El dashboard principal refresca snapshot cada 15 minutos y hace polling liviano del feed para estado y diagnostico.
- El backend ya trae `uv.lock`, asi que `uv sync` recrea el entorno con las versiones bloqueadas.
- El `subtype_map` del cliente en `back/app/data/companies.json` queda preparado para completar los codigos `tp` reales que aun no venian completos en la documentacion.
- Si faltan `HOWEN_USERNAME` y `HOWEN_PASSWORD` o `HOWEN_PASSWORD_MD5`, la API puede arrancar pero la ingesta quedara reconectando hasta que completes esas credenciales reales.

## Estructura util

- `back/app/main.py`
- `back/app/services/howen.py`
- `back/app/services/dashboard.py`
- `front/src/App.tsx`
- `front/src/hooks/useDashboardStream.ts`
