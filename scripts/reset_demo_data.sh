#!/usr/bin/env bash
set -euo pipefail

# Reset integral de datos de demo.
# Fases:
# 1) Limpia entradas locales de NiFi y artefactos raw temporales.
# 2) Resetea estado streaming (tablas + checkpoints).
# 3) Recrea servicios generadores para arrancar con estado limpio.
# 4) Opcional --hard: limpia tambien /data/raw/nifi en HDFS.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

HARD_RESET="false"
if [[ "${1:-}" == "--hard" ]]; then
  HARD_RESET="true"
fi

echo "Reset de demo: limpieza local de eventos + reset de estado streaming"

echo
echo "[1/4] Limpiando archivos locales de ingesta NiFi..."
find nifi/input -maxdepth 1 -type f -name 'gps_*.jsonl' -delete || true
find nifi/raw-archive/weather -maxdepth 1 -type f ! -name '.gitkeep' -delete || true
find nifi/raw-archive/failures/gps -maxdepth 1 -type f -name 'gps_*.jsonl' -delete || true
rm -rf nifi/raw-archive/weather/.processed/* 2>/dev/null || true

echo
echo "[2/4] Limpiando estado streaming (Hive + checkpoints HDFS curated)..."
./scripts/reset_streaming_state.sh

echo
echo "[3/4] Recreando generador y loader de raw..."
sg docker -c "docker compose up -d --force-recreate gps-generator raw-hdfs-loader"

if [[ "${HARD_RESET}" == "true" ]]; then
  echo
  echo "[4/4] Hard reset: limpiando raw HDFS historico y cache de dashboard..."
  sg docker -c "docker compose exec -T hadoop bash -lc 'hdfs dfs -rm -r -f /data/raw/nifi || true; hdfs dfs -mkdir -p /data/raw/nifi || true'"
else
  echo
  echo "[4/4] Hard reset omitido. Usa --hard para limpiar tambien /data/raw/nifi en HDFS."
fi

echo
echo "Reset de demo completado."
