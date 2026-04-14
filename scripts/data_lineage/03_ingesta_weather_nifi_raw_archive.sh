#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "03 - Ingesta meteorologica raw (Open-Meteo via NiFi)"
echo "Ubicacion: nifi/raw-archive/weather/*"

latest_file="$(find nifi/raw-archive/weather -maxdepth 1 -type f | sort | tail -n 1 || true)"
if [ -z "${latest_file}" ]; then
  echo "No hay snapshots meteorologicos en nifi/raw-archive/weather"
  exit 0
fi

echo "Snapshot seleccionado: ${latest_file}"
section "Contenido JSON raw"
sed -n '1,40p' "${latest_file}"
