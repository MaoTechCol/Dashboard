# Certificacion de Tandas 0-5

## Arquitectura de cortes multiempresa

Los cortes oficiales de 15 minutos usan consumidores concurrentes por empresa,
pero todas las solicitudes historicas de una misma cuenta Howen atraviesan un unico
canal serializado y regulado. Esto evita dos problemas opuestos:

- una empresa grande ya no bloquea completamente a las siguientes;
- Howen nunca recibe una rafaga paralela que provoque rate limit.

El espaciado base es 2,5 segundos. Si Howen limita solicitudes, el cliente aumenta
automaticamente el espaciado hasta 8 segundos y lo reduce gradualmente despues de
20 respuestas exitosas. Las reconstrucciones y tareas de mantenimiento no toman
trabajo mientras existan cortes oficiales en cola o ejecucion.

La publicacion usa una barrera por cohorte: cada empresa procesa su propio corte,
pero ninguna publica antes de que todas las empresas operativas tengan listo el
mismo `cut_at`. El ultimo job listo materializa y publica los snapshots del grupo
con el mismo `publishedCutAt`. Un corte parcial nunca reemplaza el anterior.

## Tanda 0: punto de retorno

- El respaldo `20260820T171446Z` fue validado por SHA-256 y restaurado previamente
  en PostgreSQL 17 aislado.
- `deploy/backup-production.sh` genera un respaldo actual reproducible con dump
  custom de Supabase, catalogo de restore, arbol desplegado, storage, configuracion,
  conteos operativos, commit y hashes.
- `deploy/verify-production-backup.sh` valida integridad y, si se define
  `RECOVERY_DATABASE_URL`, ejecuta una restauracion real aislada.
- La copia fuera del Droplet debe conservar permisos privados y nunca entrar a Git.

## Tandas 1-3: ejecucion y disponibilidad

- API y worker estan separados por systemd.
- Supabase conserva la cola durable, leases, heartbeats, reintentos y reclamo con
  `FOR UPDATE SKIP LOCKED`.
- Hay cuatro lanes reservados para cortes y uno para mantenimiento.
- Los cortes tienen prioridad absoluta; una reconstruccion cede entre dispositivos.
- El Droplet tiene 4 GB RAM y 2 GB swap, watchdog, limites de memoria y endpoints
  `healthz`/`readyz` separados.

## Tandas 4-5: reglas y presentacion N2

- Las reglas N2 estan cubiertas por pruebas automatizadas.
- Las categorias operativas y los textos del cliente estan normalizados al espanol.
- Patrones usa `Accion sugerida` y recomienda revision/descarga de evidencia por
  placa sin servir videos desde este portal.

## Validacion obligatoria de despliegue

1. Ejecutar la suite backend y el build frontend.
2. Desplegar API y worker con cuatro lanes de harvest.
3. Observar un corte real completo.
4. Confirmar que todas las empresas publican el mismo `publishedCutAt`.
5. Confirmar que la cola activa vuelve a cero antes del siguiente cuarto.
6. Confirmar que no hubo rate limit; si aparece, verificar que el pacing adaptativo
   aumenta y el job queda reintentable, no fallido de forma terminal.
7. Generar, verificar y copiar fuera del host el respaldo final.

La certificacion se considera cerrada solo despues de registrar los tiempos y
resultados reales de los pasos anteriores.
