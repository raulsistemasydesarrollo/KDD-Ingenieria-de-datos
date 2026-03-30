#!/usr/bin/env bash
set -euo pipefail

# Garantiza objetos Hive de compatibilidad para streaming.
# Objetivo:
# - Evitar que las vistas Madrid queden "colgadas" cuando las tablas
#   streaming fueron limpiadas o no se materializaron todavia.
# - Mantener consultas SQL estables para operacion/documentacion.
#
# Nota:
# - No fuerza carga de datos; solo asegura esquema minimo y vistas.
# - Es idempotente: puede ejecutarse multiples veces sin efectos secundarios.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "Asegurando objetos Hive de compatibilidad (streaming + vistas Madrid)..."

docker compose exec -T spark-client spark-sql -e "
CREATE DATABASE IF NOT EXISTS transport_analytics;

CREATE TABLE IF NOT EXISTS transport_analytics.delay_metrics_streaming (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  warehouse_id STRING,
  avg_delay_minutes DOUBLE,
  avg_speed_kmh DOUBLE,
  last_event_timestamp TIMESTAMP,
  event_count BIGINT
) USING PARQUET;

CREATE TABLE IF NOT EXISTS transport_analytics.weather_observations_streaming (
  weather_event_id STRING,
  warehouse_id STRING,
  temperature_c DOUBLE,
  precipitation_mm DOUBLE,
  wind_kmh DOUBLE,
  weather_code STRING,
  source STRING,
  weather_timestamp TIMESTAMP
) USING PARQUET;

CREATE OR REPLACE VIEW transport_analytics.v_delay_metrics_streaming_madrid AS
SELECT
  warehouse_id,
  avg_delay_minutes,
  avg_speed_kmh,
  event_count,
  window_start AS window_start_utc,
  window_end AS window_end_utc,
  from_utc_timestamp(window_start, 'Europe/Madrid') AS window_start_madrid,
  from_utc_timestamp(window_end, 'Europe/Madrid') AS window_end_madrid
FROM transport_analytics.delay_metrics_streaming;

CREATE OR REPLACE VIEW transport_analytics.v_weather_observations_madrid AS
SELECT
  weather_event_id,
  warehouse_id,
  temperature_c,
  precipitation_mm,
  wind_kmh,
  weather_code,
  source,
  weather_timestamp AS weather_timestamp_utc,
  from_utc_timestamp(weather_timestamp, 'Europe/Madrid') AS weather_timestamp_madrid
FROM transport_analytics.weather_observations_streaming;
"

echo "Objetos Hive de compatibilidad listos."
