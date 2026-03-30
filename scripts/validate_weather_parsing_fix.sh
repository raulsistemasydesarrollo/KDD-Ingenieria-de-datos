#!/usr/bin/env bash
set -euo pipefail

# Prueba especifica del parser de clima en streaming.
# Objetivo:
# - Verificar que Spark acepta tanto valores numericos como string numerico
#   en temperature/precipitation/wind.
# - Confirmar persistencia correcta en Hive para ambos formatos.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

run() {
  echo
  echo "==> $*"
  eval "$@"
}

echo "Validacion fix parsing meteo (Kafka string/number -> Hive double)"

run "sg docker -c \"docker compose exec -T spark-client mvn -f /opt/spark-app/pom.xml -DskipTests package\""
run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"DROP TABLE IF EXISTS transport_analytics.weather_observations_streaming;\\\"\""
run "sg docker -c \"docker compose exec -T hadoop bash -lc 'hdfs dfs -rm -r -f /tmp/checkpoints/weather_observations /data/curated/weather_observations_streaming || true'\""

echo
echo "==> Lanzando streaming (timeout 240s) ..."
sg docker -c "docker compose exec -T spark-client bash -lc 'STREAMING_STARTING_OFFSETS=latest timeout 240 bash /opt/spark-app/run-streaming.sh'" &
STREAM_PID=$!

echo "==> Esperando 100s para asegurar arranque de Spark streaming..."
sleep 100

run "sg docker -c \"docker compose exec -T kafka bash -lc 'printf \\\"{\\\\\\\"weather_event_id\\\\\\\":\\\\\\\"fix_evt_num\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"WH1\\\\\\\",\\\\\\\"temperature_c\\\\\\\":21.7,\\\\\\\"precipitation_mm\\\\\\\":0.3,\\\\\\\"wind_kmh\\\\\\\":14.2,\\\\\\\"weather_code\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"source\\\\\\\":\\\\\\\"open-meteo\\\\\\\",\\\\\\\"observation_time\\\\\\\":\\\\\\\"2099-01-01T00:00:01Z\\\\\\\"}\\\\n{\\\\\\\"weather_event_id\\\\\\\":\\\\\\\"fix_evt_str\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"WH1\\\\\\\",\\\\\\\"temperature_c\\\\\\\":\\\\\\\"22.9\\\\\\\",\\\\\\\"precipitation_mm\\\\\\\":\\\\\\\"0.1\\\\\\\",\\\\\\\"wind_kmh\\\\\\\":\\\\\\\"11.8\\\\\\\",\\\\\\\"weather_code\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"source\\\\\\\":\\\\\\\"open-meteo\\\\\\\",\\\\\\\"observation_time\\\\\\\":\\\\\\\"2099-01-01T00:00:02Z\\\\\\\"}\\\\n\\\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic transport.weather.filtered >/dev/null'\""

echo
echo "==> Esperando fin de streaming..."
wait "${STREAM_PID}" || true

run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"SELECT COUNT(*) AS weather_rows FROM transport_analytics.weather_observations_streaming;\\\"\""
run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"SELECT weather_event_id, temperature_c, precipitation_mm, wind_kmh, weather_timestamp FROM transport_analytics.weather_observations_streaming WHERE weather_event_id IN ('fix_evt_num','fix_evt_str') ORDER BY weather_timestamp;\\\"\""

echo
echo "Validacion completada."
