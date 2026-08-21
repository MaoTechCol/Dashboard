# Tanda 7: auditoria compacta y retencion

## Politica aplicada

- `howen_alarm_raw` conserva el payload completo del proveedor y es la fuente de
  trazabilidad por `provider_event_key`.
- `alarm_events` conserva solamente columnas normalizadas para analitica y reglas.
- `alarm_event_audit` se limita a mapeos dudosos, resoluciones temporales y eventos
  rechazados; referencia el evento canonico y no duplica el payload.
- Los exitos normales se contabilizan en `alarm_harvest_run`,
  `alarm_harvest_device` y las metricas batch de reconstruccion.
- Las anomalias y decisiones humanas conservan su detalle funcional en sus tablas
  dedicadas.

## Retencion

- Raw Howen, alarmas analiticas y lecturas operativas: 40 dias.
- Anomalias y auditoria excepcional: 90 dias.
- Decisiones aprobadas o descartadas: 90 dias desde su decision.
- Decisiones pendientes: se conservan hasta que una persona decida.

La purga se ejecuta en `dashboard-worker.service`, como maximo una vez por hora.
La API no realiza compactaciones ni purgas dentro de solicitudes interactivas.

## Validacion

- Un DMS normal nuevo escribe raw y analitica, sin dos filas adicionales de auditoria.
- Un evento no DMS normal escribe solo raw.
- Un evento anomalo mantiene referencia y razon de auditoria.
- Repetir un batch no duplica raw, analitica, auditoria ni anomalias.
- La suite automatizada verifica compactacion, ventanas 40/90 dias y preservacion de
  decisiones pendientes.

Esto reduce las escrituras normales por DMS de cuatro a dos y por no DMS de tres a
una, cumpliendo una reduccion minima del 50% sin perder trazabilidad accionable.
