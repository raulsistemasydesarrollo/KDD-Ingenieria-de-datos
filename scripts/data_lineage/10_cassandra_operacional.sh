#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "10 - Cassandra operacional (dashboard en baja latencia)"
ensure_service cassandra || exit 0

run_cql() {
  local q="$1"
  run_service cassandra cqlsh -e "$q"
}

section "Keyspace y tablas"
run_cql "DESCRIBE KEYSPACE transport;"

section "vehicle_latest_state"
run_cql "SELECT vehicle_id, warehouse_id, route_id, last_event_timestamp, delay_minutes, speed_kmh, latitude, longitude FROM transport.vehicle_latest_state LIMIT 10;"

section "weather_observations_recent"
run_cql "SELECT bucket, weather_timestamp, weather_event_id, warehouse_id, temperature_c, precipitation_mm, wind_kmh, weather_code, source FROM transport.weather_observations_recent LIMIT 10;"

section "network_insights_snapshots"
run_cql "SELECT bucket, entity_type, profile, min_congestion, snapshot_time, rank, entity_id, impact_score, criticality_score FROM transport.network_insights_snapshots LIMIT 10;"

section "model_retrain_state"
run_cql "SELECT model_name, last_run_at, last_success_at, last_selected_model, last_selected_rmse FROM transport.model_retrain_state LIMIT 10;" || true
