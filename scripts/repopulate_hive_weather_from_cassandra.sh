#!/usr/bin/env bash
set -euo pipefail

# Repuebla la tabla Hive de meteo usando Cassandra como fuente de respaldo.
# Flujo:
# 1) Garantiza tablas/vistas Hive de compatibilidad.
# 2) Exporta filas de Cassandra (transport.weather_observations_recent) a CSV.
# 3) Carga CSV en Spark SQL y hace INSERT OVERWRITE en weather_observations_streaming.
# 4) Verifica recuento final de la vista operativa Madrid.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

STAMP="$(date +%Y%m%d_%H%M%S)"
HOST_TMP_CSV="/tmp/weather_observations_recent_${STAMP}.csv"
SPARK_TMP_CSV="/tmp/weather_observations_recent_${STAMP}.csv"
SPARK_TMP_CSV_URI="file://${SPARK_TMP_CSV}"

cleanup() {
  rm -f "${HOST_TMP_CSV}" || true
  docker compose exec -T spark-client bash -lc "rm -f '${SPARK_TMP_CSV}'" >/dev/null 2>&1 || true
  if [ -n "${CASSANDRA_CID:-}" ]; then
    docker exec "${CASSANDRA_CID}" bash -lc "rm -f '/tmp/weather_observations_recent.csv'" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Asegurando esquema Hive de compatibilidad..."
./scripts/ensure_hive_streaming_compat.sh

echo "Localizando contenedor Cassandra..."
CASSANDRA_CID="$(docker compose ps -q cassandra)"
if [ -z "${CASSANDRA_CID}" ]; then
  echo "[ERROR] No se encontro contenedor de Cassandra en docker compose."
  exit 1
fi

echo "Exportando snapshots meteo desde Cassandra a CSV..."
docker exec "${CASSANDRA_CID}" cqlsh -e "COPY transport.weather_observations_recent (bucket, weather_timestamp, weather_event_id, warehouse_id, temperature_c, precipitation_mm, wind_kmh, weather_code, source) TO '/tmp/weather_observations_recent.csv' WITH HEADER=TRUE;"

docker cp "${CASSANDRA_CID}:/tmp/weather_observations_recent.csv" "${HOST_TMP_CSV}"

if [ ! -s "${HOST_TMP_CSV}" ]; then
  echo "[ERROR] Export CSV vacio; no hay datos para repoblar."
  exit 1
fi

echo "Copiando CSV al contenedor spark-client..."
SPARK_CID="$(docker compose ps -q spark-client)"
if [ -z "${SPARK_CID}" ]; then
  echo "[ERROR] No se encontro contenedor spark-client en docker compose."
  exit 1
fi
docker cp "${HOST_TMP_CSV}" "${SPARK_CID}:${SPARK_TMP_CSV}"

echo "Repoblando Hive transport_analytics.weather_observations_streaming desde Cassandra..."
docker compose exec -T spark-client spark-sql -e "
CREATE DATABASE IF NOT EXISTS transport_analytics;

CREATE OR REPLACE TEMP VIEW cass_weather_raw
USING csv
OPTIONS (
  path '${SPARK_TMP_CSV_URI}',
  header 'true',
  inferSchema 'false'
);

INSERT OVERWRITE TABLE transport_analytics.weather_observations_streaming
SELECT
  weather_event_id,
  COALESCE(NULLIF(TRIM(warehouse_id), ''), 'UNK') AS warehouse_id,
  CAST(temperature_c AS DOUBLE) AS temperature_c,
  CAST(precipitation_mm AS DOUBLE) AS precipitation_mm,
  CAST(wind_kmh AS DOUBLE) AS wind_kmh,
  COALESCE(NULLIF(TRIM(weather_code), ''), 'unknown') AS weather_code,
  COALESCE(NULLIF(TRIM(source), ''), 'cassandra') AS source,
  TO_TIMESTAMP(weather_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSSXX') AS weather_timestamp
FROM (
  SELECT
    weather_event_id,
    warehouse_id,
    temperature_c,
    precipitation_mm,
    wind_kmh,
    weather_code,
    source,
    weather_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY weather_event_id
      ORDER BY TO_TIMESTAMP(weather_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSSXX') DESC
    ) AS rn
  FROM cass_weather_raw
  WHERE LOWER(TRIM(bucket)) = 'all'
) t
WHERE rn = 1
  AND weather_event_id IS NOT NULL
  AND weather_event_id <> ''
  AND TO_TIMESTAMP(weather_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSSXX') IS NOT NULL;

SELECT COUNT(*) AS weather_rows_hive
FROM transport_analytics.v_weather_observations_madrid;
"

echo "Repoblado Hive meteo completado."
