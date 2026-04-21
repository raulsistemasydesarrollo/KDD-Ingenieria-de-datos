# Manual de Usuario - Plataforma KDD Logistica

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Manual de usuario funcional y operativo`
- Version: `v1.1`
- Fecha: `04/04/2026`
- Repositorio GitHub: `https://github.com/raulsistemasydesarrollo/KDD-Ingenieria-de-datos`

## Indice

1. Objetivo y alcance
2. Requisitos previos
3. Arranque y parada del stack
4. Uso completo del dashboard
5. Uso de DAGs en Airflow
6. Comprobaciones de salud y estado
7. Consultas de datos (Hive y Cassandra)
8. Troubleshooting guiado
9. Comandos de referencia rapida

## 1. Objetivo y alcance

Este manual explica, de forma practica, como usar toda la aplicacion:

1. Arrancar y parar la plataforma completa.
2. Operar el dashboard en sus dos vistas (Tiempo Real y Red Logistica).
3. Entender y ejecutar los DAGs de Airflow.
4. Verificar salud de servicios y datos.
5. Resolver incidencias operativas comunes.

## 2. Requisitos previos

Entorno recomendado:

1. Docker y Docker Compose activos.
2. Usuario con permisos para ejecutar Docker (grupo `docker`).
3. Proyecto clonado en local.
4. Puertos disponibles:
   - `8080` (Airflow)
   - `8443` (NiFi)
   - `8501` (Dashboard)
   - `9870` (HDFS UI)

Comprobar Docker:

```bash
docker ps
```

## 3. Arranque y parada del stack

### 3.1 Arranque recomendado (todo en uno)

```bash
./scripts/start_kdd.sh
```

Que hace este script:

1. Levanta el stack con `docker compose up -d --build`.
2. Espera disponibilidad de NiFi.
3. Ejecuta bootstrap del flujo NiFi (`kdd_ingestion_auto_v9`).
4. Asegura compatibilidad de objetos Hive streaming.
5. Ejecuta sanity check de tablas/vistas en Hive.
6. Si la vista meteo en Hive esta vacia, repuebla desde Cassandra.
7. Espera health del dashboard y muestra estado final.

### 3.2 Arranque alternativo (Airflow primero)

```bash
./scripts/start_airflow_then_stack.sh
```

Este flujo:

1. Levanta solo Airflow.
2. Espera `http://localhost:8080/health`.
3. Despausa y dispara el DAG `kdd_bootstrap_stack`.
4. Deja trazabilidad del arranque en Airflow.

### 3.3 Parada completa

```bash
./scripts/stop_kdd.sh
```

### 3.4 Reinicio rapido por servicio

```bash
docker compose restart dashboard
docker compose restart nifi
docker compose restart spark-client
```

### 3.5 Hard reset de demo

Soft reset (limpia estado streaming y eventos locales):

```bash
./scripts/reset_demo_data.sh
```

Hard reset (incluye limpieza raw historico en HDFS):

```bash
./scripts/reset_demo_data.sh --hard
```

## 4. Uso completo del dashboard

URL:

- `http://localhost:8501`

Health endpoint:

```bash
curl -fsS http://localhost:8501/health
```

### 4.1 Estructura funcional del dashboard

El dashboard esta dividido en dos contextos independientes:

1. `Operacion en Tiempo Real` (columna izquierda).
2. `Analisis de Red Logistica` (columna derecha).

Regla clave:

- Los filtros de una vista no afectan a la otra.

Cabecera operativa actual:

1. Primera fila: fuentes de datos + `Spark` + `YARN nodo` + acceso `DAG limpieza`.
2. Segunda fila: `HDFS disco`, estado de `Limpieza DAG`, ultima limpieza local y enlaces de plataforma.
3. El enlace `DAG limpieza` usa el mismo color de estado que `HDFS disco` (`ok/warn/bad`).

### 4.2 Vista de Tiempo Real (izquierda)

Elementos principales:

1. Mapa de vehiculos en vivo.
2. Trazas recientes (lineas naranjas).
3. Proyeccion de ruta restante del vehiculo seleccionado (linea amarilla discontinua).
4. Tabla de flota con ETA por nodos restantes y ETA destino final.
5. Panel de detalle/historial de vehiculo.

Filtros de esta vista:

1. `Origen RT`
2. `Destino RT`

Casos tipicos de filtro:

1. `TODOS -> TODOS`: vista global de flota.
2. `TODOS -> X`: vehiculos con destino `X`.
3. `X -> TODOS`: vehiculos con origen `X`.
4. `X -> Y`: corredor concreto.

### 4.3 Vista de Red Logistica (derecha)

Elementos principales:

1. Grafo de almacenes y conexiones.
2. Ruta calculada resaltada en rojo.
3. Tabla de ruta con distancia, tiempo base, penalizacion meteo y ETA final.
4. Bloque `Insights de red` con rankings de cuellos de botella y nodos criticos.

Filtros de esta vista:

1. `Origen`
2. `Destino`
3. `Perfil`
4. `Patron horario`
5. `Peso tiempo`, `Peso riesgo`, `Peso eco`
6. `Evitar nodos`

Perfiles de optimizacion disponibles:

1. `balanced`
2. `fastest`
3. `resilient`
4. `eco`
5. `low_risk`
6. `reliable`

### 4.4 Flujo recomendado de uso del dashboard

1. Verifica que el banner/metricas cargan y que no hay error de fuente.
2. En Tiempo Real, selecciona un vehiculo y valida ruta reportada + ETA.
3. En Red Logistica, define origen/destino y pulsa calcular mejor ruta.
4. Ajusta perfil, patron horario y pesos para comparar alternativas.
5. Usa `Evitar nodos` para simular cortes/incidencias.
6. Revisa bloque de `Insights` para priorizacion operativa.

### 4.5 Diagnostico de fuentes del dashboard

Endpoint:

```bash
curl -s http://localhost:8501/api/debug/sources
```

Este endpoint informa, entre otros:

1. Fuente activa de vehiculos (`cassandra` o `nifi_files`).
2. Fuente activa de clima (`cassandra` o `nifi_raw_archive`).
3. Estado de conexion Cassandra.
4. Conteo de nodos/aristas cargados.

### 4.6 Reentreno IA desde dashboard

Trigger manual:

```bash
curl -s -X POST http://localhost:8501/api/ml/retrain \
  -H 'Content-Type: application/json' \
  -d '{"trigger":"manual_dashboard"}'
```

Estado y recomendacion:

```bash
curl -s http://localhost:8501/api/ml/retrain/status
```

Comportamiento:

1. Si ya hay reentreno en curso (dashboard o Airflow), responde `409`.
2. Si arranca correctamente, responde `202`.
3. La cabecera del dashboard muestra estado, recomendacion y modelo en uso.
4. Sobre el boton `Reentrenar IA` se muestran:
   - `Ultimo reentreno`
   - `Siguiente programado`
   en horario `Europe/Madrid`.
5. Si aparece motivo de cobertura live, incluye:
   - cobertura real de aristas,
   - cobertura esperada por vehiculos activos.

### 4.7 Limpieza automatica por uso de disco

Comportamiento operativo:

1. Umbral por defecto: `88%` (`DISK_CLEANUP_USAGE_THRESHOLD`).
2. Si `HDFS disco >= umbral`, el dashboard intenta disparar limpieza automaticamente.
3. El backend evita ejecuciones duplicadas con lock y cooldown.

Trigger manual por API:

```bash
curl -s -X POST http://localhost:8501/api/platform/cleanup/trigger \
  -H 'Content-Type: application/json' \
  -d '{"trigger":"manual_dashboard"}'
```

## 5. Uso de DAGs en Airflow

URL:

- `http://localhost:8080`

Credenciales por defecto:

1. Usuario: `airflow`
2. Password: `airflow`

### 5.1 DAG `kdd_bootstrap_stack`

Tipo:

- Arranque bajo demanda (`schedule=None`).

Para que sirve:

1. Arranca servicios no-Airflow del stack.
2. Ejecuta `ensure_hive_streaming_compat.sh`.
3. Publica estado final con `docker compose ps`.

Tareas:

1. `start_non_airflow_stack`
2. `ensure_hive_streaming_compat`
3. `show_stack_status`

### 5.2 DAG `kdd_hourly_healthcheck`

Tipo:

- Ejecucion horaria (`@hourly`).

Para que sirve:

1. Detectar incidencias de infraestructura/datos con baja latencia.

Chequeos:

1. Contenedores core.
2. Topics Kafka de streaming.
3. Paths HDFS (`/data/raw`, `/data/raw/nifi`, `/data/curated`).
4. Tablas Hive base.
5. Vistas Madrid.
6. Smoke query de clima y delay con degradacion segura.
7. Auto-repoblado de meteo en Hive si `weather_rows=0`.

### 5.3 DAG `kdd_daily_healthcheck`

Tipo:

- Ejecucion diaria (`@daily`).

Para que sirve:

1. Evidencia diaria de salud operativa para seguimiento y entrega.

Chequeos:

- Equivalentes al DAG horario, con foco de control diario.

### 5.4 DAG `logistics_kdd_monthly_maintenance`

Tipo:

- Ejecucion mensual (`@monthly`).

Para que sirve:

1. Reentreno/recalculo batch de Spark (`run-batch.sh`).
2. Limpieza de checkpoints temporales en HDFS.
3. Evitar solapamiento de runs del propio DAG (`max_active_runs=1`).

Tareas:

1. `retrain_graph_model`
2. `cleanup_temp_hdfs`

### 5.5 Como ejecutar DAGs manualmente

Desde UI de Airflow:

1. Abre `DAGs`.
2. Activa el DAG (toggle) si esta pausado.
3. Pulsa `Trigger DAG`.
4. Entra al `Graph` o `Grid` para revisar tareas.

Desde CLI (contenedor Airflow):

```bash
docker compose exec -T airflow-webserver /home/airflow/.local/bin/airflow dags list
docker compose exec -T airflow-webserver /home/airflow/.local/bin/airflow dags trigger kdd_bootstrap_stack
docker compose exec -T airflow-webserver /home/airflow/.local/bin/airflow dags list-runs -d kdd_bootstrap_stack -o table
```

## 6. Comprobaciones de salud y estado

### 6.1 Estado de servicios

```bash
docker compose ps
```

### 6.2 Estado de topics Kafka

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | sort
```

### 6.3 Estado de HDFS

```bash
docker compose exec -T hadoop hdfs dfs -ls -R /data/raw/nifi | tail -n 40
docker compose exec -T hadoop hdfs dfs -ls -R /data/curated | tail -n 60
```

### 6.4 Estado de objetos Hive

```bash
docker compose exec -T spark-client spark-sql -e "SHOW TABLES IN transport_analytics;"
docker compose exec -T spark-client spark-sql -e "SELECT COUNT(*) AS delay_rows FROM transport_analytics.v_delay_metrics_streaming_madrid;"
docker compose exec -T spark-client spark-sql -e "SELECT COUNT(*) AS weather_rows FROM transport_analytics.v_weather_observations_madrid;"
```

## 7. Consultas de datos (Hive y Cassandra)

### 7.1 Consultas Hive utiles

```bash
docker compose exec -T spark-client spark-sql -e "DESCRIBE transport_analytics.enriched_events_streaming;"
docker compose exec -T spark-client spark-sql -e "SELECT warehouse_id, window_start_utc, window_start_madrid, avg_delay_minutes FROM transport_analytics.v_delay_metrics_streaming_madrid ORDER BY window_start_utc DESC LIMIT 20;"
docker compose exec -T spark-client spark-sql -e "SELECT weather_event_id, weather_timestamp_utc, weather_timestamp_madrid, warehouse_id, temperature_c, precipitation_mm, wind_kmh FROM transport_analytics.v_weather_observations_madrid ORDER BY weather_timestamp_utc DESC LIMIT 20;"
```

### 7.2 Consultas Cassandra utiles

```bash
docker compose exec -T cassandra cqlsh -e "DESCRIBE TABLES IN transport;"
docker compose exec -T cassandra cqlsh -e "SELECT vehicle_id, warehouse_id, route_id, last_event_timestamp, delay_minutes, speed_kmh FROM transport.vehicle_latest_state LIMIT 20;"
docker compose exec -T cassandra cqlsh -e "SELECT bucket, weather_timestamp, weather_event_id, warehouse_id, temperature_c, precipitation_mm, wind_kmh FROM transport.weather_observations_recent WHERE bucket='all' LIMIT 20;"
docker compose exec -T cassandra cqlsh -e "SELECT bucket, entity_type, profile, min_congestion, snapshot_time, rank, entity_id, impact_score, criticality_score FROM transport.network_insights_snapshots LIMIT 20;"
```

## 8. Troubleshooting guiado

### 8.1 Dashboard no responde

1. Verifica health:

```bash
curl -fsS http://localhost:8501/health
```

2. Reinicia servicio:

```bash
docker compose restart dashboard
```

### 8.2 Vista meteo vacia en Hive (`weather_rows=0`)

1. Asegura compatibilidad de objetos:

```bash
./scripts/ensure_hive_streaming_compat.sh
```

2. Repuebla desde Cassandra:

```bash
./scripts/repopulate_hive_weather_from_cassandra.sh
```

### 8.3 Estado streaming inconsistente

1. Limpia estado streaming:

```bash
./scripts/reset_streaming_state.sh
```

2. Arranca de nuevo el stack:

```bash
./scripts/start_kdd.sh
```

### 8.4 Flujo NiFi desactualizado o ausente

```bash
./scripts/bootstrap_nifi_flow.sh
./scripts/cleanup_nifi_legacy_pgs.py
```

### 8.5 Runs `failed` antiguos en Airflow

```bash
./scripts/cleanup_airflow_failed_runs.sh
./scripts/cleanup_airflow_failed_runs.sh --apply
```

### 8.6 ETA/ruta incoherente en frontend

1. Fuerza recarga de navegador (`Ctrl+F5`).
2. Verifica payload de vehiculos:

```bash
curl -s http://localhost:8501/api/vehicles/latest?limit=5
```

3. Reinicia dashboard si persiste:

```bash
docker compose restart dashboard
```

### 8.7 HDFS UI muestra "Couldn't preview the file"

Opcion recomendada (sin tocar HDFS interno):

```bash
./scripts/toggle_hdfs_browser_preview_hosts.sh status
sudo ./scripts/toggle_hdfs_browser_preview_hosts.sh enable
```

Implicaciones de la opcion recomendada:

1. Solo afecta al host local (`/etc/hosts`).
2. Mantiene estabilidad de Spark/Airflow contra HDFS.
3. Permite que el navegador resuelva `hadoop` al seguir el redirect de NameNode.

Cambio temporal alternativo (solo para prueba puntual, no recomendado):

1. Cambiar `docker/hadoop/conf/hdfs-site.xml` para anunciar DataNode como `localhost`.
2. Reconstruir/recrear `hadoop`.

Implicaciones del cambio temporal alternativo:

1. El preview web puede funcionar mejor desde host.
2. Spark/Airflow en contenedores pueden fallar escribiendo en HDFS (`Connection refused`, `minReplication nodes`).
3. Revertir el cambio al terminar la prueba.

## 9. Comandos de referencia rapida

Arranque/parada:

```bash
./scripts/start_kdd.sh
./scripts/stop_kdd.sh
```

Healthchecks:

```bash
docker compose ps
curl -fsS http://localhost:8501/health
curl -fsS http://localhost:8080/health
```

Validaciones:

```bash
./scripts/validate_hive_pipeline.sh
./scripts/validate_weather_parsing_fix.sh
```

Recuperacion:

```bash
./scripts/ensure_hive_streaming_compat.sh
./scripts/repopulate_hive_weather_from_cassandra.sh
./scripts/reset_streaming_state.sh
./scripts/reset_demo_data.sh --hard
```

Airflow manual:

```bash
docker compose exec -T airflow-webserver /home/airflow/.local/bin/airflow dags trigger kdd_bootstrap_stack
docker compose exec -T airflow-webserver /home/airflow/.local/bin/airflow dags list-runs -d kdd_hourly_healthcheck -o table
```
