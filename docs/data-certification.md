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

La aceptacion exige simultaneamente `raw_unexplained_alarm_count = 0` y
`analytic_unexplained_alarm_count = 0`. `unexplained_alarm_count` suma las
diferencias de ambas capas y nunca puede ocultar una capa analitica incompleta.
El resultado separa las filas fisicas duplicadas por Howen de los eventos
unicos y muestra `raw_source_counts`, para distinguir clips autoritativos de
filas historicas heredadas.

Tanto los cortes oficiales como las reconstrucciones historicas usan
`record/findEvidences.action`, la misma fuente de Alarm Clips con video. El API
historico por dispositivo queda disponible solo como fallback configurable y
no esta habilitado en produccion.

## Kilometraje

Se aceptan tres formatos de Howen:

- resumen diario con `Device ID`, `Fleet Name`, `Total` y una columna por dia;
- resumen mensual con una columna `AAAA-MM`, cuyo cierre se deriva de la fecha del archivo exportado;
- registro de odometro con `Device No.`, `Start mileage` y `End mileage`.

Los valores con sufijo `km` se interpretan como numeros, los ceros explicitos se conservan y una celda vacia se trata como falta de evidencia, nunca como `0 km`. En el registro de odometro prevalece `End mileage - Start mileage`; los resultados negativos se reportan como anomalías y no deben aprobarse automaticamente.

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

Si la empresa fue desactivada y ya no existe en el registro durable, la herramienta entrega `status=blocked` y `reason=company_not_registered`. No restaura ni recrea empresas de forma implicita.
