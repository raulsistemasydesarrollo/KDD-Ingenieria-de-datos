# Changelog

Todos los cambios relevantes de este proyecto se documentan en este archivo.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).

## [Unreleased]

### Added

- Persistencia streaming en Hive de eventos enriquecidos: `transport_analytics.enriched_events_streaming`.
- Nuevo fallback Parquet de streaming: `/data/curated/enriched_events_streaming`.

### Changed

- Generador GPS actualizado a rutas realistas sobre grafo logistico y flota actual leida desde `data/master/vehicles.csv`.
- Bootstrap NiFi actualizado a `kdd_ingestion_auto_v9`.
- Flujo GPS NiFi actualizado con renombrado unico por split (`${filename}_${fragment.index}_${UUID()}.jsonl`) para evitar sobrescrituras en `raw-archive/gps`.
- Parseo de timestamps ISO UTC robusto en Spark streaming para GPS y clima.
- Documentacion tecnica y operativa actualizada para reflejar el estado real de `v9` y las nuevas salidas streaming.

### Fixed

- Correccion de sobrescritura de ficheros GPS en `nifi/raw-archive/gps` al archivar flowfiles de `SplitText`.

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
  - `docs/release-notes-2026-03-30.md`,
  - `docs/operations.md` (troubleshooting ETA y Hive meteo),
  - `README.md` (operacion consolidada).
