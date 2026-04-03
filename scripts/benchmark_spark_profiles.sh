#!/usr/bin/env bash
set -euo pipefail

# Benchmark rapido y reproducible de perfiles Spark en el contenedor spark-client.
# Mide tiempo wall-clock de run-insights-sync.sh para comparar configuraciones.
#
# Uso:
#   ./scripts/benchmark_spark_profiles.sh
#
# Salida:
#   logs en /tmp/insights_profile_*.log y resumen por stdout.

CONTAINER="${SPARK_CLIENT_CONTAINER:-spark-client}"
APP_DIR="${SPARK_APP_DIR:-/opt/spark-app}"

run_profile() {
  local label="$1"
  local exec_mem="$2"
  local exec_cores="$3"
  local max_offsets="$4"
  local log_file="/tmp/insights_profile_${label}.log"

  docker exec "${CONTAINER}" bash -lc "
    cd '${APP_DIR}' &&
    export SPARK_EXECUTOR_MEMORY='${exec_mem}' &&
    export SPARK_EXECUTOR_CORES='${exec_cores}' &&
    export SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS='1' &&
    export SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS='2' &&
    export SPARK_SQL_SHUFFLE_PARTITIONS='4' &&
    export STREAMING_MAX_OFFSETS_PER_TRIGGER='${max_offsets}' &&
    TIMEFORMAT='${label} elapsed_s=%3R'
    { time ./run-insights-sync.sh; } > '${log_file}' 2>&1
    grep 'elapsed_s=' '${log_file}' | tail -n 1
  "
}

echo "== Benchmark Spark profiles =="
echo "Container: ${CONTAINER}"
echo

run_profile "baseline" "2g" "2" "8000"
run_profile "recommended" "1536m" "2" "6000"

echo
echo "Logs:"
echo "  /tmp/insights_profile_baseline.log"
echo "  /tmp/insights_profile_recommended.log"
