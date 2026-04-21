# Changelog

Todos los cambios relevantes de este proyecto se documentan en este archivo.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).

## [Unreleased]

### Added

- Nuevo manual de usuario integral:
  - `docs/manual-usuario.md`
  - incluye arranque/parada del stack, uso completo del dashboard, uso de DAGs de Airflow, checks de salud, consultas BBDD y troubleshooting.
- Persistencia streaming en Hive de eventos enriquecidos: `transport_analytics.enriched_events_streaming`.
- Nuevo fallback Parquet de streaming: `/data/curated/enriched_events_streaming`.
- Persistencia de snapshots de insights en Cassandra: `transport.network_insights_snapshots`.
- Nuevo endpoint de historico de insights: `/api/network/insights/history`.
- Nuevo launcher Spark: `spark-app/run-insights-sync.sh`.
- Nuevas tablas Hive de consolidacion de insights:
  - `transport_analytics.network_insights_snapshots_hive`
  - `transport_analytics.network_insights_hourly_trends`
- Script `scripts/rebuild_graph_edges_by_proximity.py` para regenerar aristas por cercania geografica con reglas de negocio.
- Nuevo endpoint de limpieza operativa en dashboard:
  - `POST /api/platform/cleanup/trigger`
  - ejecuta limpieza segura asincrona con lock y cooldown.

### Changed

- Documentacion de entrega sincronizada a `v1.3` (`03/04/2026`) en README y documentos principales de `docs/`.
- Nuevo release notes de iteracion actual: `docs/release-notes-2026-04-03.md`.
- `docs/operations.md` ampliado con catalogo operativo de scripts:
  - checks de salud,
  - reinicio completo/parcial de servicios,
  - consultas BBDD (Hive/Cassandra),
  - troubleshooting y recuperacion.
- `README.md` ampliado con bloque de operacion diaria (healthchecks, reinicios, consultas y recuperacion).
- Enlaces al nuevo manual de usuario añadidos en documentacion principal:
  - `README.md`
  - `docs/ENTREGA.md`
  - `docs/operations.md`
  - `docs/dashboard.md`
- Builders PDF actualizados para consumir release notes actual:
  - `scripts/build_delivery_pdf.py`
  - `scripts/build_delivery_pdf_professional.sh`
- Generador GPS actualizado a rutas realistas sobre grafo logistico y flota actual leida desde `data/master/vehicles.csv`.
- Bootstrap NiFi actualizado a `kdd_ingestion_auto_v9`.
- Flujo GPS NiFi actualizado con renombrado unico por split (`${filename}_${fragment.index}_${UUID()}.jsonl`) para evitar sobrescrituras en `raw-archive/gps`.
- Parseo de timestamps ISO UTC robusto en Spark streaming para GPS y clima.
- Documentacion tecnica y operativa actualizada para reflejar el estado real de `v9` y las nuevas salidas streaming.
- Dashboard actualizado:
  - tablas ordenables por columna,
  - selectores de origen/destino ordenados alfabeticamente,
  - etiquetas de nodos RT en codigo de 3 letras y semitransparentes.
- Red logistico-geografica consolidada:
  - 15 nodos (`ACO, ALM, BCN, BIO, CAC, GIJ, LIS, MAD, MAL, MUR, OPO, SEV, VAL, VLL, ZAR`),
  - 15 vehiculos (`14` activos y `1` en mantenimiento),
  - restricciones de conectividad por corredor y atajos prohibidos.
- Actualizacion integral de documentacion de entrega a `v1.2` (`02/04/2026`) en los documentos principales de `docs/`.
- Captura de dashboard renovada en `docs/dashboard.png` y leyenda sincronizada en documentacion funcional/tecnica.
- `scripts/build_delivery_pdf.py` actualizado para incluir `docs/release-notes-2026-04-02.md`.
- `scripts/build_delivery_pdf_professional.sh` preparado para salida PDF con fecha dinamica.
- Dashboard de rutas ampliado con nuevos perfiles `low_risk` y `reliable` (ademas de `balanced`, `fastest`, `resilient`, `eco`).
- Dashboard actualizado con optimizacion multiobjetivo (`objective_time`, `objective_risk`, `objective_eco`), `temporal_mode` y exclusion de nodos (`avoid_nodes`).
- Nuevos endpoints ML operativos en dashboard:
  - `POST /api/ml/retrain`
  - `GET /api/ml/retrain/status`
- Persistencia del estado/recomendacion de reentreno en Cassandra (`transport.model_retrain_state`).
- Dashboard actualizado (21/04/2026):
  - cabecera de sensores reorganizada (Spark/YARN en primera fila junto a fuentes),
  - enlace `DAG limpieza` movido a primera fila y sincronizado en color con `HDFS disco`,
  - auto-trigger de limpieza cuando `HDFS disco >= DISK_CLEANUP_USAGE_THRESHOLD` (default `88`),
  - tarjetas KPI compactadas (`Vehiculos / eventos` en tarjeta unificada),
  - motivos de reentreno ampliados con "cobertura esperada por vehiculos activos".
- Documentacion funcional/operativa/memoria actualizada a estado real del sistema (dashboard, manual, operaciones y API).

### Fixed

- Correccion de sobrescritura de ficheros GPS en `nifi/raw-archive/gps` al archivar flowfiles de `SplitText`.
- Correccion de `UNKNOWN` en congestion de insights cuando no hay muestras live (normalizacion a `low/medium/high`).
- Correccion en computo de insights por perfil (`KeyError: profile_minutes_sum`).

### Removed

- Sin cambios todavia.

## [1.0.0] - 2026-03-30

### Added

- Script `scripts/repopulate_hive_weather_from_cassandra.sh` para repoblar `transport_analytics.weather_observations_streaming` desde Cassandra (`transport.weather_observations_recent`).
- Tarea `repopulate_hive_weather_if_empty` en los DAGs:
  - `dags/kdd_hourly_healthcheck_dag.py`
  - `dags/kdd_daily_healthcheck_dag.py`

### Changed

- `scripts/start_kdd.sh` incluye repoblado automatico de meteo en Hive cuando `v_weather_observations_madrid` devuelve `weather_rows=0`.
- `docs/operations.md` actualizado con flujo de diagnostico y repoblado manual.
- `README.md` actualizado con el flujo de auto-recuperacion meteo.

### Fixed

- ETA incoherente en dashboard por cache/suavizado:
  - resincronizacion forzada al cambiar destino estimado,
  - resincronizacion forzada ante divergencia grande entre ETA cacheado y ETA fisico (`distancia/velocidad + delay`).
  - archivo afectado: `dashboard/static/app.js`.

### Removed

- Sin cambios.

### Documentation

- Normalizacion de enlaces en `docs/CODE-ANNOTATION-GUIDE.md` a rutas relativas.
- Actualizacion de:
  - `docs/dashboard.md` (logica ETA y estabilidad),
  - `docs/release-notes-2026-04-02.md`,
  - `docs/operations.md` (troubleshooting ETA y Hive meteo),
  - `README.md` (operacion consolidada).
