# Proyecto Big Data KDD - Java 21 + Docker

Implementacion de una plataforma Big Data orientada a logistica y transporte siguiendo el ciclo KDD del enunciado, usando Java 21 para la aplicacion Spark y una arquitectura desplegada con contenedores Docker.

Repositorio GitHub oficial del proyecto:

- `https://github.com/raulsistemasydesarrollo/KDD-Ingenieria-de-datos`

## Arquitectura

- `NiFi 2.7.0`: ingesta desde API/logs simulados y publicacion en Kafka.
- `Kafka 3.9.1 (KRaft)`: bus de eventos para crudo y filtrado.
- `gps-generator`: generador continuo de logs GPS para alimentar NiFi (`nifi/input`).
- `raw-hdfs-loader`: sincronizacion continua de raw archivado por NiFi hacia HDFS (`/data/raw/nifi`).
- `Hadoop 3.4.2 pseudodistribuido`: HDFS + YARN en un unico nodo.
- `Hive`: almacenamiento SQL historico y catalogo.
- `Cassandra 5.0`: ultimo estado por vehiculo para consultas de baja latencia.
- `Spark 3.5.x + GraphFrames`: limpieza, enriquecimiento, streaming, analisis de grafos y modelo ML de riesgo de retraso.
- `Airflow 2.10.x`: orquestacion del mantenimiento mensual.
- `Dashboard Web`: visualizacion en tiempo real de vehiculos (mapa), trazas de movimiento y analitica de rutas con grafo + impacto meteorologico.

## Estructura

- `docker-compose.yml`: stack completo.
- `docker/`: Dockerfiles y configuracion de infraestructura.
- `spark-app/`: proyecto Maven en Java 21 para Spark.
- `dags/`: DAGs de Airflow.
- `data/`: datasets semilla para demo.
- `nifi/`: material para la ingesta y notas del flujo.
- `docs/`: documentacion tecnica y pasos operativos.
- `dashboard/`: servidor web ligero + frontend del dashboard logistico.

Indice maestro de entrega:

- [docs/ENTREGA.md](./docs/ENTREGA.md)
- [docs/architecture.md](./docs/architecture.md) (incluye diagrama [docs/architecture-diagram.png](./docs/architecture-diagram.png))
- [CHANGELOG.md](./CHANGELOG.md) (historial consolidado de cambios)

## Arranque rapido

1. Construir y levantar la plataforma:

   ```bash
   docker compose up --build -d
   ```

2. Compilar la aplicacion Spark:

   ```bash
   docker compose exec spark-client mvn -f /opt/spark-app/pom.xml -DskipTests package
   ```

3. Cargar datasets de demo en HDFS:

   ```bash
   docker compose exec hadoop bash /opt/hadoop/bootstrap/load-seed-data.sh
   ```

4. Ejecutar el job batch/grafo:

   ```bash
   docker compose exec spark-client bash /opt/spark-app/run-batch.sh
   ```

   Opcional (sincronizar snapshots de insights Cassandra -> Hive):

   ```bash
   sg docker -c "docker compose exec -T spark-client /opt/spark-app/run-insights-sync.sh"
   ```

5. Ejecutar el job de streaming:

   ```bash
   docker compose exec spark-client bash /opt/spark-app/run-streaming.sh
   ```

   Opcional (forzar reproceso desde el inicio de los topics):

   ```bash
   docker compose exec -e STREAMING_STARTING_OFFSETS=earliest spark-client bash /opt/spark-app/run-streaming.sh
   ```

   Opcional (zona horaria SQL de Spark; por defecto `Europe/Madrid`):

   ```bash
   docker compose exec -e SPARK_SQL_TIMEZONE=Europe/Madrid spark-client bash /opt/spark-app/run-streaming.sh
   ```

6. Configurar y arrancar flujo NiFi automaticamente (sin UI manual):

   ```bash
   ./scripts/bootstrap_nifi_flow.sh
   ```

   El bootstrap usa por defecto `NIFI_PG_NAME=kdd_ingestion_auto_v9` y crea dos subgrupos:
   - `gps_ingestion`
   - `weather_ingestion`

7. Abrir dashboard logistico (mapa + grafos + meteo):

   ```bash
   docker compose up -d dashboard
   ```

   URL:

   - `http://localhost:8501`

   Comportamiento actual de filtros:
   - Vista Tiempo Real (izquierda): usa `Origen RT` / `Destino RT` y afecta solo esa vista.
   - Vista Analisis Logistico (derecha): usa `Origen` / `Destino` y afecta solo mapa/logistica/tablas de esa columna.
   - Ambas vistas estan desacopladas intencionalmente.

   Capacidades actuales de routing/analitica en dashboard:
   - Perfiles: `balanced`, `fastest`, `resilient`, `eco`, `low_risk`, `reliable`.
   - Multiobjetivo: pesos `tiempo/riesgo/eco` (normalizados).
   - Patron horario: `auto`, `peak`, `offpeak`, `night`.
   - Exclusiones de nodo: `Evitar nodos`.
   - Candidatas de ruta: hasta 4 alternativas con `delta_vs_best`.
   - Reentreno IA:
     - trigger `POST /api/ml/retrain`
     - estado/recomendacion `GET /api/ml/retrain/status`

## Scripts operativos

- Arranque completo del stack:

  ```bash
  ./scripts/start_kdd.sh
  ```

  Incluye automaticamente:
  - bootstrap de flujo NiFi,
  - `scripts/ensure_hive_streaming_compat.sh`,
  - sanity check Hive (`SHOW TABLES` + `COUNT(*)` sobre vistas Madrid),
  - repoblado automatico de meteo en Hive desde Cassandra si `weather_rows=0`.

- Parada completa del stack:

  ```bash
  ./scripts/stop_kdd.sh
  ```

- Limpieza de estado streaming (tablas Hive + checkpoints HDFS):

  ```bash
  ./scripts/reset_streaming_state.sh
  ```

- Reset de demo (limpieza de eventos locales + reset streaming + recrear generadores):

  ```bash
  ./scripts/reset_demo_data.sh
  ```

  Hard reset (incluye limpieza de `/data/raw/nifi` en HDFS):

  ```bash
  ./scripts/reset_demo_data.sh --hard
  ```

## Validacion automatica Hive

Para validar en un solo comando la escritura batch + streaming en Hive:

```bash
chmod +x scripts/validate_hive_pipeline.sh
./scripts/validate_hive_pipeline.sh
```

Documentacion detallada:

- `docs/hive-validation.md`

Para limpiar tablas/checkpoints de streaming (quitar estado de pruebas):

```bash
chmod +x scripts/reset_streaming_state.sh
./scripts/reset_streaming_state.sh
```

Validacion especifica del fix de parseo meteo (numeros y numeros-string):

```bash
./scripts/validate_weather_parsing_fix.sh
```

Garantizar compatibilidad Hive de objetos streaming (tablas/vistas Madrid):

```bash
./scripts/ensure_hive_streaming_compat.sh
```

Repoblar Hive meteo desde Cassandra (cuando `v_weather_observations_madrid` esta vacia):

```bash
./scripts/repopulate_hive_weather_from_cassandra.sh
```

## Formato de datos (raw vs procesado)

Se aplica la separacion recomendada:

- `Raw`: eventos sin transformar en `JSON/JSONL`.
- `Procesado`: datasets limpios y agregados en `Parquet` (consultables via Hive).

Referencias del proyecto:

- Raw GPS base Spark batch: `hdfs://hadoop:9000/data/raw/gps_events.jsonl`
- Raw NiFi archivado en host: `nifi/raw-archive/`
- Raw NiFi sincronizado a HDFS: `/data/raw/nifi`
- Curated batch en Parquet: `hdfs://hadoop:9000/data/curated/enriched_events`
- Curated streaming fallback Parquet:
  - `hdfs://hadoop:9000/data/curated/delay_metrics_streaming`
  - `hdfs://hadoop:9000/data/curated/weather_observations_streaming`
  - `hdfs://hadoop:9000/data/curated/enriched_events_streaming`

Comprobacion rapida:

```bash
sg docker -c "docker compose exec -T hadoop hdfs dfs -ls -R /data/raw | head -n 40"
sg docker -c "docker compose exec -T hadoop hdfs dfs -ls -R /data/curated | head -n 80"
```

### Matriz operativa (resumen)

| Flujo | Kafka | HDFS | Hive |
|---|---|---|---|
| GPS raw | `transport.raw` | `/data/raw/nifi/gps/...` | No aplica |
| GPS filtrado | `transport.filtered` | `/data/curated/delay_metrics_streaming` | `transport_analytics.delay_metrics_streaming` |
| GPS enriquecido streaming | `transport.filtered` | `/data/curated/enriched_events_streaming` | `transport_analytics.enriched_events_streaming` |
| Weather raw | `transport.weather.raw` | `/data/raw/nifi/weather/...` | No aplica |
| Weather filtrado | `transport.weather.filtered` | `/data/curated/weather_observations_streaming` | `transport_analytics.v_weather_observations_madrid` (vista operativa) |
| Batch historico GPS | No aplica | `/data/curated/enriched_events` | `transport_analytics.enriched_events`, `transport_analytics.delay_metrics_batch`, `transport_analytics.route_graph_metrics` |

Detalle operativo completo:

- `docs/operations.md` (seccion `Matriz operativa end-to-end`)

## Vistas Hive en hora Madrid

Se han creado vistas para consultar timestamps en `Europe/Madrid` sin conversion manual:

- `transport_analytics.v_weather_observations_madrid`
- `transport_analytics.v_delay_metrics_streaming_madrid`

Ejemplos:

```sql
SELECT weather_event_id, weather_timestamp_utc, weather_timestamp_madrid
FROM transport_analytics.v_weather_observations_madrid
ORDER BY weather_timestamp_utc DESC
LIMIT 10;
```

```sql
SELECT warehouse_id, window_start_utc, window_start_madrid, window_end_utc, window_end_madrid
FROM transport_analytics.v_delay_metrics_streaming_madrid
ORDER BY window_start_utc DESC
LIMIT 10;
```

Nota de compatibilidad:

- Si el estado streaming fue limpiado, el proyecto recrea objetos base con `scripts/ensure_hive_streaming_compat.sh`.
- Esto evita errores `TABLE_OR_VIEW_NOT_FOUND` en las vistas Madrid aunque no haya datos todavia.

## Servicios



- NiFi: `http://localhost:8443/nifi`
- Kafka broker interno: `kafka:9092`
- Hadoop/YARN: `http://localhost:9870`, `http://localhost:8088`
- HiveServer2: `localhost:10000`
- Cassandra CQL: `localhost:9042`
- Airflow: `http://localhost:8080`
- Dashboard logistico: `http://localhost:8501`

Usuario NiFi:

- usuario: `admin`
- password: `adminadminadmin`

Usuario por defecto Airflow:

- usuario: `airflow`
- password: `airflow`

DAGs relevantes:

- `logistics_kdd_monthly_maintenance` (mantenimiento mensual)
- `kdd_daily_healthcheck` (checklist diario automatizado)
- `kdd_hourly_healthcheck` (checklist horario automatizado)
- `kdd_bootstrap_stack` (arranque del stack no-Airflow desde Airflow)

Auto-recuperacion meteo en DAGs de healthcheck:

- `kdd_daily_healthcheck` y `kdd_hourly_healthcheck` incluyen la tarea `repopulate_hive_weather_if_empty`.
- Si `SELECT COUNT(*) FROM transport_analytics.v_weather_observations_madrid` devuelve `0`, ejecutan `./scripts/repopulate_hive_weather_from_cassandra.sh` antes del smoke query meteo.
- Si hay filas, no repueblan.

Alertas Airflow:

- Todos los DAGs incorporan callback de fallo.
- Para email en fallo: exporta `AIRFLOW_ALERT_EMAIL` en el entorno de Airflow.

Arranque "Airflow primero y despues stack desde DAG":

```bash
./scripts/start_airflow_then_stack.sh
```

Limpieza de runs `failed` historicos de healthchecks (modo simulacion + apply):

```bash
./scripts/cleanup_airflow_failed_runs.sh
./scripts/cleanup_airflow_failed_runs.sh --apply
```

Limpieza de Process Groups legacy de NiFi (manteniendo `kdd_ingestion_auto_v9`):

```bash
./scripts/cleanup_nifi_legacy_pgs.py
```

## Cambios recientes (02/04/2026)

- Entrega documental v1.2 sincronizada en todos los documentos principales de `docs/`:
  - portada, version y fecha de entrega alineadas a `02/04/2026`.
- Captura del dashboard actualizada y referenciada en documentacion:
  - `docs/dashboard.png`,
  - `docs/dashboard.md`,
  - `docs/memoria-tecnica-sistema.md`.
- Nuevo documento de release notes para la iteracion actual:
  - `docs/release-notes-2026-04-02.md`.
- Estado funcional del dashboard alineado con codigo actual:
  - perfiles adicionales `eco`, `low_risk`, `reliable`,
  - patron horario y pesos `tiempo/riesgo/eco`,
  - panel `Evitar nodos`,
  - boton `Reentrenar IA` + recomendacion de deriva,
  - bloque de modelos IA en cabecera con dos paneles:
    - panel izquierdo: `EN USO` + candidato elegido (A/B/C) y comparativa RMSE,
    - panel derecho: descripcion de candidatos en columna unica,
  - rutas de Tiempo Real muestran ruta completa (`origen -> intermedios -> final`) cuando el generador publica `planned_route_nodes`,
  - tabla de vehiculos y panel de historial muestran `ETA nodos restantes` y `ETA destino final`.
- ML de riesgo de retraso reforzado con validacion comparativa en cada batch:
  - candidatos `baseline_rf`, `tuned_baseline_rf`, `enhanced_rf`,
  - `enhanced_rf` incorpora features de clima y congestion alineadas por ventana temporal de 15 minutos y almacen,
  - seleccion automatica del mejor por RMSE para evitar regresiones,
  - benchmark actual (dataset semilla 20k): `baseline_rmse=6.1329`, `tuned_baseline_rmse=6.1343`, `enhanced_rmse=6.0206` -> seleccionado `enhanced_rf`.
- Modelo en HDFS validado tras reentreno:
  - ruta `hdfs://hadoop:9000/models/delay_risk_rf`,
  - tamano aproximado `1.2 MB` (Spark ML pipeline serializado en Parquet/Snappy).
- Builders de PDF de entrega ajustados a la iteracion activa:
  - `scripts/build_delivery_pdf.py` apunta a release notes actual,
  - `scripts/build_delivery_pdf_professional.sh` genera nombre de salida con fecha dinamica.

## Cambios recientes (31/03/2026)

- Dashboard:
  - Etiquetas de nodos en mapa de Tiempo Real normalizadas a codigo de 3 letras (igual que el mapa logistico).
  - Etiquetas de nodos de Tiempo Real con transparencia para no tapar vehiculos.
  - Selectores `Origen/Destino` (RT y Red Logistica) ordenados alfabeticamente.
  - Todas las tablas del dashboard ordenables por columna (click en cabeceras).
  - Tabla bajo mapa logistico enfocada en origen/destino seleccionados (o global en `TODOS`).
  - Nuevos insights live de red (cuellos de botella, nodos criticos e historico).
  - Resolucion de congestion `UNKNOWN` a `low/medium/high` cuando no hay muestras live.
- Backend dashboard / Spark:
  - Nuevos campos live por arista (`effective_avg_delay_minutes`, `live_avg_delay_minutes`, `live_sample_count`, `congestion_level`) y resumen `live_edge_summary`.
  - Persistencia de snapshots de insights en Cassandra (`transport.network_insights_snapshots`).
  - Endpoint historico de insights: `/api/network/insights/history`.
  - Nuevo modo Spark `insights-sync` + launcher `spark-app/run-insights-sync.sh` para consolidar en Hive:
    - `transport_analytics.network_insights_snapshots_hive`
    - `transport_analytics.network_insights_hourly_trends`
- Red y simulacion:
  - Red actual ampliada a 15 nodos y flota de 15 vehiculos (14 activos + 1 en mantenimiento).
  - Script de regeneracion de aristas por cercania geografica:
    - `python3 scripts/rebuild_graph_edges_by_proximity.py --k 4`
  - Reglas de negocio aplicadas en la red:
    - prohibidas directas: `BCN-MUR`, `ACO-BIO`, `VAL-ALM`
    - obligatorias: `BCN-VAL`, `VAL-MUR`, `ACO-GIJ`, `GIJ-BIO`, `MAD-SEV`, `MAD-MAL`
    - corredor sur sin atajos: `CAC-SEV-MAL-ALM-MUR`
- Operativa:
  - Si aparece `permission denied` al lanzar `run-insights-sync.sh`, ejecutar:
    - `chmod +x spark-app/run-insights-sync.sh`
    - reconstruir/recrear `spark-client` para refrescar permisos en imagen/contenedor.

## Cambios recientes (30/03/2026)

- Airflow:
  - Corregidas `smoke_query_weather` y `smoke_query_delay` en healthchecks diario/horario para evitar caida cuando faltan tablas streaming fuente.
  - Runs historicos `failed` limpiados para `kdd_daily_healthcheck` y `kdd_hourly_healthcheck`.
  - Auto-recuperacion de meteo Hive en healthchecks (`repopulate_hive_weather_if_empty`) cuando `v_weather_observations_madrid` queda en `0` filas.
- NiFi:
  - Nuevo bootstrap visual por subflujos: `gps_ingestion` y `weather_ingestion`.
  - Proceso de limpieza legacy disponible en `scripts/cleanup_nifi_legacy_pgs.py`.
- Arranque:
  - `scripts/start_kdd.sh` y `scripts/bootstrap_nifi_flow.sh` apuntan por defecto a `kdd_ingestion_auto_v9`.
  - `scripts/start_kdd.sh` repuebla meteo en Hive desde Cassandra si la vista Madrid esta vacia.
- Spark + Dashboard:
  - El dashboard usa Cassandra como fuente primaria de clima (`transport.weather_observations_recent`) con fallback a `nifi/raw-archive/weather`.
  - `LogisticsAnalyticsJob` en modo streaming persiste observaciones meteo recientes en Cassandra para mejorar precision de ruta y panel de clima.
  - `LogisticsAnalyticsJob` en modo streaming persiste tambien `transport_analytics.enriched_events_streaming` en Hive (ademas de `delay_metrics_streaming` y `weather_observations_streaming`).
  - `spark-app/run-streaming.sh` y `spark-app/run-batch.sh` recompilan el JAR automaticamente si hay cambios en `src/` o `pom.xml`.
  - Ajuste de ETA en tiempo real: resincronizacion inmediata cuando hay divergencia grande entre ETA cacheado y ETA fisico (distancia/velocidad) o cambio de destino estimado.
- GPS/NiFi:
  - El generador GPS usa rutas realistas derivadas del grafo logistico y ya no etiquetas aleatorias fijas.
  - Flota leida desde `data/master/vehicles.csv` (IDs actuales `TRUCK-*`; filtra por `status` activo).
  - En NiFi se evita sobrescritura de raw GPS: cada split se archiva con nombre unico (`${filename}_${fragment.index}_${UUID()}.jsonl`).

## Notas

- La arquitectura Hadoop es de nodo unico en modo pseudodistribuido.
- El proyecto prioriza reproducibilidad local y separa claramente batch, streaming y orquestacion.
- Se contempla ingesta meteo (API publica) hacia `transport.weather.raw` y `transport.weather.filtered`, con persistencia en Hive.
- Hay documentacion operativa en `docs/architecture.md`, `docs/nifi-flow.md`, `docs/nifi-weather-template.md` y `docs/hive-validation.md`.
- Guia operativa consolidada: `docs/operations.md`.
- Guia de dashboard y leyenda visual: `docs/dashboard.md`.
- Endpoint debug de fuentes del dashboard: `GET /api/debug/sources`.
- Plan TODO de cumplimiento del enunciado por fases KDD: `docs/kdd-todo.md`.
- Bitacora de cambios de la iteracion 02/04/2026: `docs/release-notes-2026-04-02.md`.
- Memoria tecnica detallada del sistema (entrega): `docs/memoria-tecnica-sistema.md`.
- Resumen ejecutivo (1-2 paginas) para portada/anexo inicial: `docs/resumen-ejecutivo-memoria.md`.
