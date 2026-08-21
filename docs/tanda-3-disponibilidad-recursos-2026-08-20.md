# Tanda 3: disponibilidad y recursos

Fecha de cierre: 20 de agosto de 2026.

## Objetivo

Mantener la API interactiva disponible cuando el worker procesa reconstrucciones, cortes historicos y tareas de mantenimiento. El Droplet objetivo tiene 2 vCPU, 4 GB de RAM, 80 GB de disco y 2 GB de swap.

## Controles implementados

- `dashboard-api.service` y `dashboard-worker.service` conservan watchdogs independientes.
- La API cancela consultas PostgreSQL despues de 12 segundos y responde `504` con `Retry-After` en lugar de dejar conexiones colgadas.
- El worker dispone de hasta 300 segundos por consulta batch y un timeout de pool separado.
- Cada proceso vigila su RSS y registra transiciones `warning`, `critical` y `recovered` en el journal de systemd.
- Los limites para el Droplet de 4 GB son 850 MB para API y 2.2 GB para worker, dejando margen al sistema operativo, Nginx y PostgreSQL remoto.
- Las operaciones largas se encolan y responden `202` con `job_id`. El refresh del dashboard usa `POST /api/dashboard/refresh` y luego consulta el snapshot publicado.
- Nginx limita las esperas de API a 30 segundos; ninguna reconstruccion depende de mantener abierto un request HTTP.

## Variables operativas

```env
DATABASE_CONNECT_TIMEOUT_SECONDS=5
DATABASE_LOCK_TIMEOUT_MS=5000
API_DATABASE_STATEMENT_TIMEOUT_MS=12000
WORKER_DATABASE_STATEMENT_TIMEOUT_MS=300000
API_DATABASE_POOL_TIMEOUT_SECONDS=5
WORKER_DATABASE_POOL_TIMEOUT_SECONDS=30
MEMORY_MONITOR_INTERVAL_SECONDS=15
API_MEMORY_WARNING_MB=450
API_MEMORY_CRITICAL_MB=750
WORKER_MEMORY_WARNING_MB=1100
WORKER_MEMORY_CRITICAL_MB=2000
```

## Verificacion

```bash
systemctl show dashboard-api.service dashboard-worker.service \
  -p ActiveState -p NRestarts -p MemoryCurrent -p MemoryPeak -p WatchdogTimestampMonotonic
journalctl -u dashboard-api.service -u dashboard-worker.service \
  --since "30 minutes ago" | grep -E "memory_|query_cancelled|watchdog"
curl -fsS http://127.0.0.1:8000/api/healthz
curl -fsS http://127.0.0.1:8000/api/readyz
```

Un aviso de memoria queda registrado localmente en systemd. La integracion con un canal externo de alertas corresponde a la tanda de monitoreo final.
