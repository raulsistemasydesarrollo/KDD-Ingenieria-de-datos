# TODO de Cumplimiento del Enunciado (Ciclo KDD)

Documento de seguimiento para comprobar el cumplimiento de `enunciado.md` y ejecutar los faltantes.

Estado:

- `[x]` Hecho
- `[~]` Parcial / mejorable
- `[ ]` Pendiente

## Fase I: Ingesta y Seleccion (NiFi + Kafka)

- `[x]` Consumo desde API publica (Open-Meteo) en NiFi.
- `[x]` Consumo de logs GPS simulados en NiFi (`gps-generator` -> `nifi/input`).
- `[x]` Archivado GPS en NiFi sin sobrescrituras por split (nombre unico por evento).
- `[x]` Publicacion en Kafka separando crudo/filtrado:
  - `transport.raw`
  - `transport.filtered`
  - `transport.weather.raw`
  - `transport.weather.filtered`
- `[x]` Copia raw para auditoria en HDFS (`/data/raw/nifi/...`) via `raw-hdfs-loader`.
- `[x]` Back-pressure en conexiones NiFi (definido en bootstrap).
- `[x]` Manejo base de errores en NiFi (`failure` enruta a `raw-archive/failures/gps` y `raw-archive/failures/weather`).

## Fase II: Preprocesamiento y Transformacion (Spark)

- `[x]` Limpieza con Spark SQL (normalizacion, nulos, duplicados).
- `[x]` Enriquecimiento batch con datos maestros.
- `[x]` Enriquecimiento streaming con datos maestros en Hive (`master_warehouses`, `master_vehicles`).
- `[x]` Persistencia de eventos enriquecidos en tiempo real en Hive (`transport_analytics.enriched_events_streaming`).
- `[x]` Analisis de grafos con GraphFrames (PageRank + connected components).
- `[x]` Camino mas corto/hops entre almacenes criticos (`transport_analytics.route_shortest_paths`).

## Fase III: Mineria y Accion (Streaming + ML)

- `[x]` Ventanas de 15 minutos en Structured Streaming.
- `[x]` Carga multicapa:
  - Hive (historico / reporting).
  - Cassandra (ultimo estado por vehiculo).
- `[x]` Simulacion GPS realista sobre grafo logistico (rutas calculadas + reasignacion dinamica).
- `[x]` Componente ML minimo implementado (modelo de riesgo de retraso con Spark ML y scoring en Hive).
- `[x]` Operativa de reentreno IA expuesta en dashboard (`POST /api/ml/retrain`, `GET /api/ml/retrain/status`) con persistencia de estado en Cassandra.

## Fase IV: Orquestacion (Airflow)

- `[x]` DAG mensual de mantenimiento:
  - Recalculo batch/grafo.
  - Limpieza temporal en HDFS.
- `[x]` DAG diario de healthcheck.
- `[x]` DAG de bootstrap de stack desde Airflow.
- `[x]` Alertas basicas en DAGs (callback de fallo + email opcional via `AIRFLOW_ALERT_EMAIL`).

## Requisitos globales del enunciado

- `[x]` Java 21 en `spark-app`.
- `[x]` Docker / Docker Compose para despliegue.
- `[x]` Hadoop/HDFS en pseudodistribuido de nodo unico.
- `[x]` Stack Apache en versiones exigidas o superiores.
- `[x]` Separacion raw JSON / procesado Parquet documentada y operativa.
- `[x]` Dashboard con optimizacion de rutas multiobjetivo (perfiles ampliados, pesos tiempo/riesgo/eco, modo temporal y exclusion de nodos).

## Plan de cierre (priorizado)

1. `[x]` Ejecutar validacion E2E completa post-cambios (`validate_hive_pipeline.sh`) y guardar evidencias.
2. `[ ]` (Opcional) Endurecer NiFi con cola Kafka DLQ adicional ademas de fichero local de fallos.
3. `[ ]` (Opcional) Mejorar modelo ML (feature store, validacion temporal, metricas historicas).
4. `[ ]` (Opcional) Integrar notificacion externa real (Slack/Teams/Webhook) en Airflow.

## Evidencias y comandos de verificacion

- Validacion principal:
  - `./scripts/validate_hive_pipeline.sh`
- Validacion parseo meteo:
  - `./scripts/validate_weather_parsing_fix.sh`
- Healthcheck diario en Airflow:
  - DAG `kdd_daily_healthcheck`
- Orquestacion mensual:
  - DAG `logistics_kdd_monthly_maintenance`
