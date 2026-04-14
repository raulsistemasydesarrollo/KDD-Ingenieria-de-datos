#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "11 - Artefacto de modelo IA en HDFS"
ensure_service hadoop || exit 0

section "Contenido de /models en HDFS"
run_service hadoop hdfs dfs -ls -R /models || true

section "Modelo seleccionado"
if ensure_service spark-client >/dev/null 2>&1; then
  run_service spark-client spark-sql -e "SELECT vehicle_id, predicted_delay_minutes, risk_level, last_event_timestamp FROM transport_analytics.ml_delay_risk_scores ORDER BY last_event_timestamp DESC LIMIT 10;" || true
fi
