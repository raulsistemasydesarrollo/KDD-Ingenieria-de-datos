#!/usr/bin/env bash
set -euo pipefail

# Limpieza tecnica del estado de streaming.
# - Elimina tablas streaming en Hive.
# - Elimina checkpoints y fallback curated en HDFS.
# - Se usa antes de validaciones reproducibles o cuando hay estado corrupto.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

run() {
  echo
  echo "==> $*"
  eval "$@"
}

echo "Limpieza de estado streaming (Hive + HDFS checkpoints)"

run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"DROP TABLE IF EXISTS transport_analytics.delay_metrics_streaming; DROP TABLE IF EXISTS transport_analytics.weather_observations_streaming;\\\"\""
run "sg docker -c \"docker compose exec -T hadoop bash -lc 'hdfs dfs -rm -r -f /tmp/checkpoints/delay_metrics /tmp/checkpoints/latest_vehicle_state /tmp/checkpoints/weather_observations /data/curated/delay_metrics_streaming /data/curated/weather_observations_streaming || true'\""
run "sg docker -c \"./scripts/ensure_hive_streaming_compat.sh\""

echo
echo "Estado streaming limpiado."
