# Certificacion de datos reales

La certificacion compara exportes de Howen con las capas autoritativas de Supabase sin modificar datos operativos.

## Alarmas

El export debe incluir al menos `Device ID`, `Alarm Type`, `Fleet` y `Begin Time`.

```bash
cd back
PROCESS_ROLE=worker uv run python scripts/certify_real_data.py \
  --company ismocol \
  --fleet-name "ISMOCOL UTIJP" \
  --alarm-file /ruta/Alarm_20260817203612.xlsx \
  --output /tmp/certificacion-ismocol-alarmas.json
```

La aceptacion exige `unexplained_alarm_count = 0`. El resultado separa las filas fisicas duplicadas por Howen de los eventos unicos, para no convertir duplicados del proveedor en faltantes locales.

## Kilometraje

El export debe incluir `Device ID`, `Fleet Name`, `Total` y una columna por dia. Los ceros explicitos se conservan; una celda vacia se trata como falta de evidencia y nunca como `0 km`.

```bash
cd back
PROCESS_ROLE=worker uv run python scripts/certify_real_data.py \
  --company ismocol \
  --fleet-name "ISMOCOL UTIJP" \
  --mileage-file /ruta/mileage_Statistic_20260817203731.xlsx \
  --output /tmp/certificacion-ismocol-km.json
```

La tolerancia de aceptacion es inferior al `1%` y el resultado incluye diferencia y cobertura por dispositivo.

La referencia externa `48.085,52 km` solo se puede certificar contra un export del mismo rango exacto. El archivo `mileage_Statistic_20260817203731.xlsx` cubre `19/07/2026 -> 17/08/2026`; no debe compararse directamente con una referencia de otra ventana.

## Evidencia durable

Cada ejecucion queda registrada en `data_certification_runs`, incluyendo rango, archivos, conteos, diferencias y resultado completo. El JSON de salida se conserva como evidencia transportable para auditoria.

