# Guia de Anotacion de Codigo

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Mapa de comentarios tecnicos en codigo`
- Version: `v1.2`
- Fecha: `02/04/2026`
- Repositorio GitHub: `https://github.com/raulsistemasydesarrollo/KDD-Ingenieria-de-datos`

## Objetivo

Este documento traza donde se han añadido comentarios detallados en el codigo para facilitar:

1. Comprension funcional por bloques.
2. Onboarding tecnico rapido.
3. Defensa de arquitectura y operaciones en revision academica.

## 1. Backend Dashboard

Archivo:

- [dashboard/server.py](../dashboard/server.py)

Bloques comentados:

1. Cabecera general del servicio y endpoints.
2. Parseo de fechas y utilidades de ficheros.
3. Motor de rutas (`edge_weight`, `dijkstra`).
4. Estrategia de fuentes (Cassandra primario + fallback NiFi).
5. Enriquecimiento de vehiculos con `planned_origin/planned_destination` y ruta completa (`planned_route_nodes`, `planned_route_label`).
6. Router API (`/api/overview`, `/api/vehicles/latest`, `/api/network/best-route`, etc.).
7. Operativa de reentreno IA (`/api/ml/retrain`, `/api/ml/retrain/status`) y cache/recomendacion.
8. Persistencia de estado de reentreno en Cassandra (`model_retrain_state`).

## 2. Frontend Dashboard

Archivo:

- [dashboard/static/app.js](../dashboard/static/app.js)

Bloques comentados:

1. Cabecera funcional y desacople de vistas.
2. Estado global de UI (`state`) y formateadores.
3. Utilidades geograficas (bearing, distancia, suavizado).
4. Filtros RT (`selectedRealtimeRouteFilter`, `resolveVisibleVehicles`).
5. Motor de render de marcadores y trazas.
6. Recalculo de ruta para vista logistica.
7. Registro de eventos (`bindEvents`) y secuencia de arranque (`bootstrap`).
8. Panel de reentreno IA (`renderRetrainStatePanel`, `triggerRetrain`, `refreshRetrainStatus`).
9. Optimizacion de ruta multiobjetivo (pesos `time/risk/eco`, `temporalMode`, `avoid_nodes`).

## 3. Spark (Batch + Streaming + ML)

Archivo:

- [spark-app/src/main/java/com/proyectobigdata/LogisticsAnalyticsJob.java](../spark-app/src/main/java/com/proyectobigdata/LogisticsAnalyticsJob.java)

Bloques comentados:

1. Javadoc de clase principal y modos de ejecucion.
2. `main` (inicializacion Spark/Hive/Cassandra).
3. `runBatchAnalysis` (pipeline historico).
4. `runStreamingAnalysis` (pipeline continuo GPS/Clima).
5. `computeGraphMetrics` y `computeShortestPathMetrics`.
6. `trainAndScoreDelayRiskModel`.
7. Persistencia robusta (`appendToHiveTable` con fallback Parquet).
8. Escritura en Cassandra (`saveLatestVehicleState`, `saveWeatherObservationsToCassandra`).

## 4. Scripts Operativos Comentados

Directorios:

- `/scripts`
- `/docker/hadoop/scripts`
- `/dags`

Archivos clave anotados:

1. Bootstrap y limpieza NiFi:
   - [bootstrap_nifi_flow.py](../scripts/bootstrap_nifi_flow.py)
   - [bootstrap_nifi_flow.sh](../scripts/bootstrap_nifi_flow.sh)
   - [cleanup_nifi_legacy_pgs.py](../scripts/cleanup_nifi_legacy_pgs.py)
2. Arranque/parada y reset:
   - [start_kdd.sh](../scripts/start_kdd.sh)
   - [stop_kdd.sh](../scripts/stop_kdd.sh)
   - [reset_demo_data.sh](../scripts/reset_demo_data.sh)
   - [reset_streaming_state.sh](../scripts/reset_streaming_state.sh)
3. Validaciones:
   - [validate_hive_pipeline.sh](../scripts/validate_hive_pipeline.sh)
   - [validate_weather_parsing_fix.sh](../scripts/validate_weather_parsing_fix.sh)
4. Hadoop raw loaders:
   - [entrypoint.sh](../docker/hadoop/scripts/entrypoint.sh)
   - [load-nifi-raw-to-hdfs.sh](../docker/hadoop/scripts/load-nifi-raw-to-hdfs.sh)
   - [load-seed-data.sh](../docker/hadoop/scripts/load-seed-data.sh)
5. Airflow DAGs:
   - [kdd_bootstrap_stack_dag.py](../dags/kdd_bootstrap_stack_dag.py)
   - [kdd_hourly_healthcheck_dag.py](../dags/kdd_hourly_healthcheck_dag.py)
   - [kdd_daily_healthcheck_dag.py](../dags/kdd_daily_healthcheck_dag.py)
   - [logistics_kdd_dag.py](../dags/logistics_kdd_dag.py)

## 5. Criterio de anotacion aplicado

Se siguio un criterio uniforme:

1. Comentarios de cabecera: proposito, flujo y responsabilidades.
2. Comentarios por bloque critico: entradas/salidas, decision funcional y fallback.
3. Sin alterar logica de ejecucion ni contratos de API.

## 6. Estado

- Anotacion aplicada sobre programas principales del proyecto.
- Sintaxis validada en Python (AST) y Bash (`bash -n`) tras cambios.
