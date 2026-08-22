# Cierre de tandas 10 a 12

## Tanda 10: agregados y rendimiento

- `company_window_aggregates` conserva las ventanas 24 h, 7 d y 30 d por empresa y version de snapshot.
- API y worker usan pools y timeouts independientes.
- Los indices de diagnostico y certificacion se gestionan con Alembic.
- El frontend deduplica solicitudes GET concurrentes.
- El detalle tecnico permanece bajo demanda.

Mediciones de produccion del 21 de agosto de 2026:

- Administracion fria: 1.509 ms.
- Diagnostico 24 h: 144 ms frio y 94 ms cacheado.
- Diagnostico 7 d: 137 ms frio y 90 ms cacheado.
- Diagnostico 30 d: 132 ms frio y 97 ms cacheado.

## Tanda 11: certificacion

Los Excel entregados son benchmarks externos de una ventana determinada. No son
fuente de ingesta y ninguna fila del archivo se copia a las tablas operativas.

Cada corte oficial guarda por empresa y dispositivo:

- DMS unicos recibidos del proveedor.
- DMS aceptados en `howen_alarm_raw`.
- DMS persistidos en `alarm_events`.
- DMS rechazados por temporalidad.
- Diferencia inexplicada.

La aceptacion por corte exige diferencia inexplicada igual a cero. Los benchmarks
Excel se conservan como `data_certification_runs`, con archivo, rango y resultado,
para comparar ventanas equivalentes sin alterar produccion.

## Tanda 12: modelo durable

- Alembic es el unico mecanismo de cambios de esquema PostgreSQL.
- El arranque no ejecuta backfills, normalizaciones ni reparaciones historicas.
- `managed_companies` es la fuente autoritativa; JSON se usa solo como seed inicial.
- Los reportes soportan almacenamiento local o Supabase Storage mediante el mismo contrato.

La migracion de PDFs a Supabase requiere `SUPABASE_SERVICE_ROLE_KEY`. Una clave
publishable o anon no tiene privilegios suficientes para crear el bucket privado,
subir objetos y reemplazarlos de forma segura.
