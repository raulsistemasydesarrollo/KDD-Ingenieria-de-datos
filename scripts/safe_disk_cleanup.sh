#!/usr/bin/env bash
set -euo pipefail

# Limpieza segura de disco (sin tocar datasets historicos de entrenamiento).
# Alcance intencionalmente limitado a artefactos locales de NiFi y temporales.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

GPS_RETENTION_DAYS="${GPS_ARCHIVE_RETENTION_DAYS:-2}"
WEATHER_RETENTION_DAYS="${WEATHER_ARCHIVE_RETENTION_DAYS:-7}"
FAIL_RETENTION_DAYS="${FAILURES_RETENTION_DAYS:-7}"
DISK_CLEANUP_USAGE_THRESHOLD="${DISK_CLEANUP_USAGE_THRESHOLD:-88}"
STATE_DIR="${ROOT_DIR}/data/state"
STATE_FILE="${STATE_DIR}/disk_cleanup_state.json"

mkdir -p "${STATE_DIR}"

usage_before_pct="$(df -P / | awk 'NR==2 {gsub("%","",$5); print $5+0}')"
deleted_gps=0
deleted_weather=0
deleted_failures=0
deleted_input=0
cleaned_weather_processed=0
did_cleanup=0

echo "[INFO] safe_disk_cleanup start: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "[INFO] Retenciones -> gps:${GPS_RETENTION_DAYS}d weather:${WEATHER_RETENTION_DAYS}d failures:${FAIL_RETENTION_DAYS}d"
echo "[INFO] Umbral de limpieza por uso de disco: ${DISK_CLEANUP_USAGE_THRESHOLD}%"

echo "[INFO] Uso disco antes:" 
df -h / | tail -n 1

if [ "${usage_before_pct}" -ge "${DISK_CLEANUP_USAGE_THRESHOLD}" ]; then
  did_cleanup=1
  echo "[INFO] Uso de disco ${usage_before_pct}% >= ${DISK_CLEANUP_USAGE_THRESHOLD}%; aplicando limpieza segura."

  # 1) Limpieza de raw-archive local de NiFi (no es fuente batch historica principal).
  deleted_gps="$(find nifi/raw-archive/gps -maxdepth 1 -type f ! -name '.gitkeep' -mtime +"${GPS_RETENTION_DAYS}" -print | wc -l || true)"
  deleted_weather="$(find nifi/raw-archive/weather -maxdepth 1 -type f ! -name '.gitkeep' -mtime +"${WEATHER_RETENTION_DAYS}" -print | wc -l || true)"
  deleted_failures="$(find nifi/raw-archive/failures -type f -mtime +"${FAIL_RETENTION_DAYS}" -print | wc -l || true)"
  find nifi/raw-archive/gps -maxdepth 1 -type f ! -name '.gitkeep' -mtime +"${GPS_RETENTION_DAYS}" -delete || true
  find nifi/raw-archive/weather -maxdepth 1 -type f ! -name '.gitkeep' -mtime +"${WEATHER_RETENTION_DAYS}" -delete || true
  find nifi/raw-archive/failures -type f -mtime +"${FAIL_RETENTION_DAYS}" -delete || true

  # 2) Higiene de input local: deja margen para reintentos recientes.
  deleted_input="$(find nifi/input -maxdepth 1 -type f -name 'gps_*.jsonl' -mtime +1 -print | wc -l || true)"
  find nifi/input -maxdepth 1 -type f -name 'gps_*.jsonl' -mtime +1 -delete || true

  # 3) Checkpoints locales de weather (temporales internos).
  if [ -d nifi/raw-archive/weather/.processed ]; then
    cleaned_weather_processed="$(find nifi/raw-archive/weather/.processed -mindepth 1 -maxdepth 1 -print | wc -l || true)"
  fi
  rm -rf nifi/raw-archive/weather/.processed/* 2>/dev/null || true

  # 4) Mantener .gitkeep por seguridad del repo.
  mkdir -p nifi/raw-archive/gps nifi/raw-archive/weather
  : > nifi/raw-archive/gps/.gitkeep
  : > nifi/raw-archive/weather/.gitkeep
else
  echo "[INFO] Uso de disco ${usage_before_pct}% < ${DISK_CLEANUP_USAGE_THRESHOLD}%; no se aplica limpieza (modo solo vigilancia)."
fi

usage_after_pct="$(df -P / | awk 'NR==2 {gsub("%","",$5); print $5+0}')"

echo "[INFO] Uso disco despues:"
df -h / | tail -n 1

cat > "${STATE_FILE}" <<EOF
{
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "threshold_percent": ${DISK_CLEANUP_USAGE_THRESHOLD},
  "usage_before_percent": ${usage_before_pct},
  "usage_after_percent": ${usage_after_pct},
  "cleanup_executed": ${did_cleanup},
  "deleted_counts": {
    "gps_archive_files": ${deleted_gps},
    "weather_archive_files": ${deleted_weather},
    "failures_files": ${deleted_failures},
    "input_files": ${deleted_input},
    "weather_processed_entries": ${cleaned_weather_processed}
  }
}
EOF

echo "[INFO] Estado de limpieza guardado en ${STATE_FILE}"
echo "[INFO] safe_disk_cleanup end"
