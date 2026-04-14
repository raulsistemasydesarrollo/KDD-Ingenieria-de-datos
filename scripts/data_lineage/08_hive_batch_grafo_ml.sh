#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "08 - Capa analitica batch en Hive (enriquecido + grafo + ML)"
ensure_service spark-client || exit 0

run_sql() {
  local q="$1"
  run_service spark-client spark-sql -e "$q"
}

section "Tablas disponibles en transport_analytics"
run_sql "SHOW TABLES IN transport_analytics;"

section "Ejemplo enriched_events"
run_sql "SELECT event_id, vehicle_id, warehouse_id, route_id, delay_minutes, speed_kmh, event_timestamp FROM transport_analytics.enriched_events LIMIT 5;"

section "Ejemplo route_graph_metrics"
run_sql "SELECT id, community_id, pagerank FROM transport_analytics.route_graph_metrics ORDER BY pagerank DESC LIMIT 5;"

section "Ejemplo route_shortest_paths"
run_sql "SELECT source_warehouse_id, target_warehouse_id, hop_distance FROM transport_analytics.route_shortest_paths LIMIT 5;"

section "Ejemplo ml_delay_risk_scores"
run_sql "SELECT vehicle_id, warehouse_id, predicted_delay_minutes, risk_level, last_event_timestamp FROM transport_analytics.ml_delay_risk_scores LIMIT 5;"
