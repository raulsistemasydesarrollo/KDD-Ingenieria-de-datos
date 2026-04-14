#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "12 - Consumo final por dashboard (mapas, estadisticas, rutas, IA)"
ensure_service dashboard || exit 0

show_endpoint() {
  local path="$1"
  section "GET ${path}"
  curl -fsS "http://localhost:8501${path}" | head -c 1200
  echo
}

show_endpoint "/api/overview"
show_endpoint "/api/vehicles/latest?limit=5"
show_endpoint "/api/weather/latest?limit=5"
show_endpoint "/api/network/graph"
show_endpoint "/api/network/best-route?source=MAD&target=BCN&profile=balanced"
show_endpoint "/api/ml/retrain/status"
show_endpoint "/api/debug/sources"
