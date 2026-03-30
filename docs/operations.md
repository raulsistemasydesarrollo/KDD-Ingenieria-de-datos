# Operaciones del Proyecto KDD

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Guia de operaciones y troubleshooting`
- Version: `v1.0-entrega`
- Fecha: `30/03/2026`

## Indice

1. Arranque y parada
2. Healthcheck en Airflow
3. Arranque del stack desde Airflow
4. Flujo NiFi
5. Validaciones
6. Politica de almacenamiento
7. Matriz operativa end-to-end
8. Limpieza de estado streaming
9. Limpieza de historico failed en Airflow
10. Zona horaria
11. Vistas Hive en hora Madrid
12. Bitacora operativa
13. Operacion del dashboard (estado final)
14. Troubleshooting dashboard

Este documento resume los comandos recomendados para operar el entorno y validar resultados.

## Arranque y parada

Arrancar todo el stack:

```bash
./scripts/start_kdd.sh
```

El script de arranque incluye automaticamente:

1. bootstrap de NiFi,
2. `ensure_hive_streaming_compat.sh` (recreacion idempotente de tablas/vistas streaming Hive),
3. sanity check Hive streaming (tablas + vistas Madrid + `COUNT(*)` de ambas vistas),
4. repoblado de `transport_analytics.weather_observations_streaming` desde Cassandra si `v_weather_observations_madrid` queda en `0` filas.

Parar todo el stack:

```bash
./scripts/stop_kdd.sh
```

## Healthcheck en Airflow

Se incorporan dos DAGs de comprobacion:

- `kdd_daily_healthcheck`
- `kdd_hourly_healthcheck`

Estos DAGs validan:

1. Contenedores core activos (`kafka`, `hadoop`, `hive-metastore`, `nifi`, `spark-client`).
2. Topics Kafka necesarios (`transport.filtered`, `transport.weather.filtered`).
3. Paths base de HDFS.
4. Tablas Hive base de `transport_analytics`.
5. Vistas en hora Madrid:
   - `transport_analytics.v_weather_observations_madrid`
   - `transport_analytics.v_delay_metrics_streaming_madrid`
6. Consulta smoke de ambas vistas (con degradacion segura):
   - si las tablas streaming fuente no estan disponibles temporalmente, el smoke no rompe el DAG y valida metastore/batch.
7. Auto-recuperacion meteo:
   - tarea `repopulate_hive_weather_if_empty`,
   - si `weather_rows=0` en `v_weather_observations_madrid`, ejecuta `./scripts/repopulate_hive_weather_from_cassandra.sh` antes del smoke query de meteo.

Alertas:

- Cada DAG incluye `on_failure_callback` (alerta en logs de Airflow).
- Si defines `AIRFLOW_ALERT_EMAIL`, se habilita envio de email en fallo.

## Arranque del stack desde Airflow

DAG de bootstrap:

- `kdd_bootstrap_stack`

Este DAG arranca servicios no-Airflow del stack (`kafka`, `nifi`, `gps-generator`, `hadoop`, `raw-hdfs-loader`, `postgres-metastore`, `hive-metastore`, `cassandra`, `spark-client`) y muestra estado final.

Script recomendado (arranca Airflow y dispara el DAG):

```bash
./scripts/start_airflow_then_stack.sh
```

## Flujo NiFi

Bootstrap automatico del flujo de ingesta:

```bash
./scripts/bootstrap_nifi_flow.sh
```

Estructura visual creada por defecto:

- Process Group: `kdd_ingestion_auto_v8`
- Subgrupos:
  - `gps_ingestion`
  - `weather_ingestion`

Limpieza de grupos legacy (para dejar solo la version actual):

```bash
./scripts/cleanup_nifi_legacy_pgs.py
```

## Validaciones

Validacion E2E principal (batch + streaming):

```bash
./scripts/validate_hive_pipeline.sh
```

Validacion especifica del parseo meteo (soporta numeros y numeros como string):

```bash
./scripts/validate_weather_parsing_fix.sh
```

## Politica de almacenamiento

El proyecto sigue el criterio recomendado:

- Capa `raw`: conservar entrada original en `JSON/JSONL` para trazabilidad y reprocesado.
- Capa `curated`: persistir salida transformada en `Parquet` para analitica eficiente.

Rutas principales:

- Raw en HDFS:
  - `/data/raw/gps_events.jsonl`
  - `/data/raw/nifi/...`
- Curated en HDFS:
  - `/data/curated/enriched_events`
  - `/data/curated/delay_metrics_streaming`
  - `/data/curated/weather_observations_streaming`

Comprobacion de rutas y formatos:

```bash
sg docker -c "docker compose exec -T hadoop hdfs dfs -ls -R /data/raw | head -n 50"
sg docker -c "docker compose exec -T hadoop hdfs dfs -ls -R /data/curated | head -n 100"
```

Comprobacion SQL en Hive:

```bash
sg docker -c "docker compose exec -T spark-client spark-sql -e \"SHOW TABLES IN transport_analytics;\""
sg docker -c "docker compose exec -T spark-client spark-sql -e \"DESCRIBE FORMATTED transport_analytics.enriched_events;\""
```

## Consultas desde terminal (Hive y Cassandra)

### Hive (via spark-sql en `spark-client`)

Listar tablas del esquema:

```bash
docker compose exec -T spark-client spark-sql -e "SHOW TABLES IN transport_analytics;"
```

Ver estructura de una tabla:

```bash
docker compose exec -T spark-client spark-sql -e "DESCRIBE transport_analytics.delay_metrics_streaming;"
```

Consultar ultimos registros (primero valida que haya filas):

```bash
docker compose exec -T spark-client spark-sql -e "SELECT COUNT(*) AS weather_rows FROM transport_analytics.v_weather_observations_madrid;"
docker compose exec -T spark-client spark-sql -e "SELECT weather_event_id, weather_timestamp_utc, weather_timestamp_madrid, warehouse_id, temperature_c, precipitation_mm, wind_kmh FROM transport_analytics.v_weather_observations_madrid WHERE weather_timestamp_utc IS NOT NULL ORDER BY weather_timestamp_utc DESC LIMIT 20;"
```

Si `weather_rows = 0`, la vista existe pero no hay carga streaming meteo en Hive en ese momento.
Como consulta operativa inmediata usa Cassandra:

```bash
docker compose exec -T cassandra cqlsh -e "SELECT bucket, weather_timestamp, weather_event_id, warehouse_id, temperature_c, precipitation_mm, wind_kmh FROM transport.weather_observations_recent WHERE bucket='all' LIMIT 20;"
```

Para repoblar Hive meteo desde Cassandra:

```bash
./scripts/repopulate_hive_weather_from_cassandra.sh
```

Comprobar si existe la tabla streaming fisica (puede no existir y seguir siendo correcto en este proyecto):

```bash
docker compose exec -T spark-client spark-sql -e "SHOW TABLES IN transport_analytics LIKE 'weather_observations_streaming';"
```

Reparar/asegurar objetos streaming de Hive (tablas + vistas Madrid) tras limpiezas:

```bash
./scripts/ensure_hive_streaming_compat.sh
```

Sanity check manual (equivalente al que ejecuta `start_kdd.sh`):

```bash
docker compose exec -T spark-client spark-sql -e "
SHOW TABLES IN transport_analytics LIKE 'delay_metrics_streaming';
SHOW TABLES IN transport_analytics LIKE 'weather_observations_streaming';
SHOW TABLES IN transport_analytics LIKE 'v_delay_metrics_streaming_madrid';
SHOW TABLES IN transport_analytics LIKE 'v_weather_observations_madrid';
SELECT COUNT(*) AS delay_rows FROM transport_analytics.v_delay_metrics_streaming_madrid;
SELECT COUNT(*) AS weather_rows FROM transport_analytics.v_weather_observations_madrid;
"
```

### Cassandra (via `cqlsh` en contenedor `cassandra`)

Listar keyspaces:

```bash
docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES;"
```

Listar tablas del keyspace `transport`:

```bash
docker compose exec -T cassandra cqlsh -e "DESCRIBE TABLES IN transport;"
```

Ver definicion de tabla:

```bash
docker compose exec -T cassandra cqlsh -e "DESCRIBE TABLE transport.vehicle_latest_state;"
```

Consultar estado de flota:

```bash
docker compose exec -T cassandra cqlsh -e "SELECT vehicle_id, warehouse_id, route_id, last_event_timestamp, delay_minutes, speed_kmh FROM transport.vehicle_latest_state LIMIT 20;"
```

Consultar clima reciente:

```bash
docker compose exec -T cassandra cqlsh -e "SELECT weather_timestamp, warehouse_id, temperature_c, precipitation_mm, wind_kmh FROM transport.weather_observations_recent WHERE bucket='all' LIMIT 20;"
```

## Matriz operativa end-to-end

Tabla de referencia rapida para trazar cada flujo desde origen hasta consumo analitico:

| Flujo | Fuente | Kafka | HDFS | Hive |
|---|---|---|---|---|
| GPS raw | `gps-generator` -> NiFi (`nifi/input/*.jsonl`) | `transport.raw` | `/data/raw/nifi/gps/...` | No aplica (capa raw) |
| GPS filtrado/normalizado | NiFi | `transport.filtered` | `/data/curated/delay_metrics_streaming` (fallback parquet streaming) | `transport_analytics.delay_metrics_streaming` |
| Weather raw | API Open-Meteo -> NiFi | `transport.weather.raw` | `/data/raw/nifi/weather/...` | No aplica (capa raw) |
| Weather filtrado/normalizado | NiFi | `transport.weather.filtered` | `/data/curated/weather_observations_streaming` (fallback parquet streaming) | `transport_analytics.v_weather_observations_madrid` (vista operativa) |
| Batch historico GPS | HDFS seed/raw (`/data/raw/gps_events.jsonl`) | No aplica | `/data/curated/enriched_events` | `transport_analytics.enriched_events`, `transport_analytics.delay_metrics_batch`, `transport_analytics.route_graph_metrics` |
| Grafo shortest path | `data/graph/*` (batch Spark) | No aplica | No aplica (tabla Hive) | `transport_analytics.route_shortest_paths` |
| ML riesgo retraso | `enriched_events` (batch Spark ML) | No aplica | `hdfs://hadoop:9000/models/delay_risk_rf` (modelo) | `transport_analytics.ml_delay_risk_scores` |

Checklist de comprobacion rapida:

```bash
sg docker -c "docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | sort"
sg docker -c "docker compose exec -T hadoop hdfs dfs -ls -R /data/raw/nifi | tail -n 40"
sg docker -c "docker compose exec -T hadoop hdfs dfs -ls -R /data/curated | tail -n 60"
sg docker -c "docker compose exec -T spark-client spark-sql -e \"SHOW TABLES IN transport_analytics;\""
```

## Limpieza de estado streaming

Elimina tablas de streaming en Hive y checkpoints en HDFS para arrancar limpio:

```bash
./scripts/reset_streaming_state.sh
```

## Limpieza de historico failed en Airflow

Para limpiar runs fallidos antiguos (sin tocar runs `success`):

```bash
./scripts/cleanup_airflow_failed_runs.sh
```

Aplicar borrado:

```bash
./scripts/cleanup_airflow_failed_runs.sh --apply
```

Estado esperado tras limpieza:

- Sin `failed` para:
  - `kdd_daily_healthcheck`
  - `kdd_hourly_healthcheck`

Comprobacion SQL directa en metastore de Airflow:

```bash
docker exec airflow-postgres psql -U airflow -d airflow -c "
SELECT dag_id, count(*) AS failed_runs
FROM dag_run
WHERE state='failed'
  AND dag_id IN ('kdd_daily_healthcheck','kdd_hourly_healthcheck')
GROUP BY dag_id;"
```

## Zona horaria

- Spark usa por defecto `SPARK_SQL_TIMEZONE=Europe/Madrid`.
- Los eventos entran habitualmente con marca temporal UTC (`...Z`), por lo que puede ser util consultar tanto UTC como Madrid.

## Vistas Hive en hora Madrid

Vistas disponibles:

- `transport_analytics.v_weather_observations_madrid`
- `transport_analytics.v_delay_metrics_streaming_madrid`

Si una vista falla con `TABLE_OR_VIEW_NOT_FOUND`, ejecutar:

```bash
./scripts/ensure_hive_streaming_compat.sh
```

Consulta ejemplo meteo:

```sql
SELECT weather_event_id, weather_timestamp_utc, weather_timestamp_madrid
FROM transport_analytics.v_weather_observations_madrid
ORDER BY weather_timestamp_utc DESC
LIMIT 10;
```

Consulta ejemplo GPS agregado:

```sql
SELECT warehouse_id, window_start_utc, window_start_madrid, window_end_utc, window_end_madrid
FROM transport_analytics.v_delay_metrics_streaming_madrid
ORDER BY window_start_utc DESC
LIMIT 10;
```

## Bitacora operativa (actualizada 30/03/2026)

Cambios aplicados en esta fecha:

1. Healthchecks Airflow estabilizados frente a ausencia temporal de tablas streaming fuente.
2. Historial rojo de healthchecks limpiado en Airflow.
3. NiFi reorganizado por dominios (`gps_ingestion` y `weather_ingestion`) en `kdd_ingestion_auto_v8`.
4. Process Group legacy eliminado para evitar ruido visual en la UI de NiFi.

## Operacion del dashboard (estado final)

### Desacople de vistas

El dashboard opera con dos contextos de filtro independientes:

1. `Operacion en Tiempo Real` (izquierda):
   - filtros: `Origen RT`, `Destino RT`.
2. `Analisis de Red Logistica` (derecha):
   - filtros: `Origen`, `Destino`, `Perfil`.

Un cambio de filtro en un bloque no debe alterar el otro.

### Reglas de filtro en Tiempo Real

- `TODOS -> TODOS`: muestra toda la flota.
- `TODOS -> X`: muestra vehiculos cuyo destino reportado es `X`.
- `X -> TODOS`: muestra vehiculos cuyo origen reportado es `X`.
- `X -> Y`: restringe por corredor y coherencia espacial.

### Fuente de ruta por vehiculo

Para evitar inconsistencias de inferencia:

1. Se usa plan de trayecto por vehiculo desde `nifi/input/.vehicle_path_state.json`.
2. Ese plan se expone por API en cada item como:
   - `planned_origin`
   - `planned_destination`
3. El frontend prioriza estos campos para `Ruta reportada`, `Siguiente nodo` y linea de proyeccion.

## Troubleshooting dashboard

### Caso: veo ruta incoherente (`X -> X`) o destino inesperado

1. Forzar recarga de navegador:

```bash
# navegador
Ctrl+F5
```

2. Comprobar payload de vehiculos:

```bash
docker compose exec -T dashboard python -c "import json,urllib.request;d=json.load(urllib.request.urlopen('http://127.0.0.1:8501/api/vehicles/latest?limit=10'));print([{k:i.get(k) for k in ('vehicle_id','warehouse_id','planned_origin','planned_destination')} for i in d.get('items',[])])"
```

3. Verificar estado del plan del generador:

```bash
python3 - <<'PY'
import json
with open('nifi/input/.vehicle_path_state.json','r',encoding='utf-8') as f:
    d=json.load(f)
print(d.get('V1'))
PY
```

Si `planned_origin/planned_destination` no aparecen en API, reiniciar dashboard:

```bash
docker compose restart dashboard
```

### Caso: ETA incoherente (ejemplo: >300 km y ETA en pocos minutos)

Comprobar primero distancia y velocidad mostradas en el panel de vehiculo.
Si son coherentes entre si pero la ETA no lo es:

1. Forzar recarga fuerte del frontend (`Ctrl+F5`) y pulsar `Actualizar` en el dashboard.
2. Verificar que el frontend ya incluye el ajuste de resincronizacion de ETA:

```bash
rg -n "forceResync|divergence|destinationChanged" dashboard/static/app.js
```

3. Reiniciar el servicio dashboard para limpiar estado en memoria:

```bash
docker compose restart dashboard
```

Nota tecnica:
- El calculo ETA ahora fuerza resincronizacion inmediata cuando hay cambio de destino estimado o desviacion grande entre ETA cacheado y ETA fisico (`distance/speed + delay`), evitando ETAs irreales persistentes.
