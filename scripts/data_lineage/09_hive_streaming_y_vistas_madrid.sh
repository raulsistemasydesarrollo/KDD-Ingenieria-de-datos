#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "09 - Streaming en Hive + vistas en hora Madrid"
ensure_service spark-client || exit 0

run_sql() {
  local q="$1"
  run_service spark-client spark-sql -e "$q"
}

section "Delay streaming"
run_sql "SELECT window_start, window_end, warehouse_id, avg_delay_minutes, avg_speed_kmh, event_count FROM transport_analytics.delay_metrics_streaming ORDER BY window_start DESC LIMIT 5;"

section "Weather streaming"
run_sql "SELECT weather_event_id, warehouse_id, temperature_c, precipitation_mm, wind_kmh, weather_timestamp FROM transport_analytics.weather_observations_streaming ORDER BY weather_timestamp DESC LIMIT 5;"

section "Vista Madrid - delay"
run_sql "SELECT warehouse_id, avg_delay_minutes, window_start_madrid, window_end_madrid FROM transport_analytics.v_delay_metrics_streaming_madrid ORDER BY window_start_madrid DESC LIMIT 5;"

section "Vista Madrid - weather"
run_sql "SELECT warehouse_id, temperature_c, precipitation_mm, wind_kmh, weather_timestamp_madrid FROM transport_analytics.v_weather_observations_madrid ORDER BY weather_timestamp_madrid DESC LIMIT 5;"
