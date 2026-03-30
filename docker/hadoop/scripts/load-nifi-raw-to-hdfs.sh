#!/usr/bin/env bash
set -euo pipefail

# Sincronizador continuo de capa raw NiFi -> HDFS.
# - Lee ficheros raw GPS/Weather montados desde el contenedor NiFi.
# - Los copia a /data/raw/nifi/{gps|weather} con sello temporal.
# - Mueve cada fichero procesado a .processed para evitar reprocesado.
# - Opera en bucle cada 30 segundos.

HDFS_BASE="/data/raw/nifi"
RAW_BASE="/opt/nifi-raw"

mkdir -p "${RAW_BASE}/gps" "${RAW_BASE}/weather" "${RAW_BASE}/gps/.processed" "${RAW_BASE}/weather/.processed"

echo "[raw-hdfs-loader] Esperando HDFS..."
for _ in $(seq 1 60); do
  if hdfs dfs -test -d / >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

hdfs dfs -mkdir -p "${HDFS_BASE}/gps" "${HDFS_BASE}/weather" || true

sync_kind() {
  local kind="$1"
  local src_dir="${RAW_BASE}/${kind}"
  local done_dir="${src_dir}/.processed"
  shopt -s nullglob
  for file in "${src_dir}"/*; do
    [ -f "${file}" ] || continue
    local base
    base="$(basename "${file}")"
    local stamp
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    local target="${HDFS_BASE}/${kind}/${stamp}_${base}"
    if hdfs dfs -put -f "${file}" "${target}"; then
      mv "${file}" "${done_dir}/${stamp}_${base}" || true
      echo "[raw-hdfs-loader] Subido ${file} -> ${target}"
    fi
  done
  shopt -u nullglob
}

echo "[raw-hdfs-loader] Iniciado. Sincronizando cada 30s..."
while true; do
  sync_kind "gps"
  sync_kind "weather"
  sleep 30
done
