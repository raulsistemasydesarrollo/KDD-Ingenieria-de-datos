#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "02 - Ingesta GPS inicial en NiFi input (JSONL)"
echo "Ubicacion: nifi/input/gps_*.jsonl"

latest_file="$(ls -1t nifi/input/gps_*.jsonl 2>/dev/null | head -n 1 || true)"
if [ -z "${latest_file}" ]; then
  echo "No hay ficheros gps_*.jsonl en nifi/input"
  exit 0
fi

echo "Ultimo fichero: ${latest_file}"
section "Primeras lineas del JSONL"
sed -n '1,5p' "${latest_file}"

section "Estado de trayectoria por vehiculo"
sed -n '1,40p' nifi/input/.vehicle_path_state.json
