# Certificacion de produccion: Tanda 3

Fecha: 20 de agosto de 2026.

## Release

- Host: DigitalOcean `167.99.232.40`
- Recursos: 2 vCPU, 4 GB RAM, 80 GB de disco y 2 GB de swap
- Commit funcional: `b55d3f6`
- Servicios: `dashboard-api.service`, `dashboard-worker.service` y `nginx`

## Evidencia

- Backend: 44 pruebas aprobadas.
- Frontend: TypeScript, build Vite y oxlint aprobados; las advertencias de hooks ya existentes no bloquean el build.
- Dependencias de runtime del frontend: 0 vulnerabilidades reportadas por `npm audit --omit=dev`.
- `healthz`: HTTP 200 en 0.07-0.09 segundos durante un refresh pesado.
- `readyz`: HTTP 200 con DB, registro, uploads y credenciales disponibles.
- Lecturas autenticadas:
  - `auth/me`: 0.28 segundos.
  - snapshot ISMOCOL: 0.31 segundos.
  - feed ISMOCOL: 0.20 segundos.
  - overview global: 1.81 segundos.
- `POST /api/dashboard/refresh?company=ismocol`: HTTP 202 con `job_id`; el job termino en `succeeded` sin error.
- PostgreSQL API: una consulta `pg_sleep(20)` fue cancelada a los 12.01 segundos.
- PostgreSQL worker: timeout de batch verificado en 5 minutos.
- Memoria: el monitor detecto 503 MiB, publico warning y luego recuperacion a 431 MiB.
- API y worker: `NRestarts=0`, watchdog activo y limites de cgroup aplicados.
- Swap: 2 GB activos y 0 B usados despues de limpiar paginas residuales del Droplet anterior.
- Marcador de release y respaldo previo conservados en el VPS.

## Resultado

La Tanda 3 queda aceptada. Una operacion larga ya no depende de mantener un request HTTP abierto; se encola, responde 202 con identificador durable y se ejecuta en el worker. La API permanece disponible y PostgreSQL cancela consultas interactivas que superen su presupuesto.

Las alertas hacia un canal externo siguen reservadas para la tanda final de monitoreo. En esta tanda los avisos quedan integrados con journal y `STATUS` de systemd.
