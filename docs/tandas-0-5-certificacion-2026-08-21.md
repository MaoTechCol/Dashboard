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

## Evidencia de produccion

La version `ca053ec` se desplego antes del corte UTC de las `04:00` del 21 de
agosto de 2026. El ajuste posterior `f778e4b` solo corrige la portabilidad del
respaldo en despliegues sin directorio `.git`.

### Corte multiempresa 04:00 UTC

- ISMOCOL fue reclamado a las `04:00:56`, gayco a las `04:00:57` y HM-HOLDING
  a las `04:00:58`. Los tres jobs comenzaron en carriles distintos.
- Howen limito dos intentos iniciales. Ambos quedaron como reintentos durables
  con espera de 20 segundos; no hubo fallos terminales ni trabajo duplicado.
- gayco termino `1/1` dispositivos a las `04:01:20`.
- HM-HOLDING termino `39/39` dispositivos a las `04:09:11`.
- ISMOCOL termino `45/45` dispositivos a las `04:09:34`.
- Los tres snapshots se publicaron entre `04:09:34` y `04:09:41`, todos con
  `publishedCutAt = 2026-08-21T04:00:00Z` y `cutStatus = succeeded`.
- La cola activa y la cola de harvest quedaron en cero. No hubo jobs fallidos
  desde el inicio del corte.
- Duracion desde el primer claim hasta la publicacion final: `8 min 45 s`.
  Margen real hasta el siguiente cuarto: `5 min 19 s`.

Durante el corte, `healthz` respondio en `0,096 s` y `readyz` en `0,058 s`.
La API uso aproximadamente 68 MB, el worker 73 MB, quedaron 3,3 GB de RAM
disponible y no se uso swap.

### Capacidad y limite del proveedor

La cohorte certificada contiene 85 dispositivos. La capacidad medida permite
proyectar una empresa adicional de aproximadamente 35-40 dispositivos dentro
del cuarto, aunque con menor margen. No se deben abrir mas solicitudes HTTP en
paralelo contra la misma cuenta: el manual de Howen exige una consulta por
vehiculo y advierte que este endpoint consume recursos y puede causar errores
si se consulta a alta frecuencia. El crecimiento por encima de esa capacidad
requiere otra cuenta/canal Howen o una ampliacion de arquitectura del proveedor.

### Respaldo final de Tanda 0

- Respaldo en VPS: `/root/dashboard-backups/20260821T041300Z`.
- Copia fuera del host:
  `/Users/andrescarvajal/Documents/Maotech 2/Production Backups/Dashboard/20260821T041300Z`.
- Tamano: 65 MB.
- Catalogo del dump: 590 entradas.
- Todos los SHA-256 pasaron tanto en el VPS como en la copia local.
- Version registrada: `f778e4b`.
- Servicios registrados: API, worker y Nginx activos.
- El dump aislado anterior `20260820T171446Z` ya habia sido restaurado y
  validado en PostgreSQL 17. El respaldo final conserva el mismo esquema y
  agrega la verificacion reproducible mediante scripts.

Con esta evidencia, las Tandas 0-5 quedan certificadas para la capacidad actual:
rescate y retorno, API/worker separados, cortes dentro del cuarto, recursos y
watchdog, reglas N2 y presentacion N2.
