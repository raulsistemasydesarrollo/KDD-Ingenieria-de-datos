#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "04 - Salida GPS raw archivada por NiFi (JSON por linea)"
echo "Ubicacion: nifi/raw-archive/gps/*.jsonl"

echo "Numero de eventos raw archivados:"
find nifi/raw-archive/gps -maxdepth 1 -type f | wc -l

sample="$(find nifi/raw-archive/gps -maxdepth 1 -type f | sort | tail -n 1 || true)"
if [ -z "${sample}" ]; then
  echo "No hay eventos en nifi/raw-archive/gps"
  exit 0
fi

echo "Evento ejemplo: ${sample}"
sed -n '1,20p' "${sample}"

echo
echo "Nota: NiFi publica a transport.filtered cuando delay_minutes >= 5."
