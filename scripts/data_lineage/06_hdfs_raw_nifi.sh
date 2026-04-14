#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "06 - Raw en HDFS sincronizado desde NiFi"
ensure_service hadoop || exit 0

section "Listado HDFS /data/raw/nifi"
run_service hadoop hdfs dfs -ls -R /data/raw/nifi || true

show_hdfs_file() {
  local path="$1"
  local label="$2"
  section "Muestra ${label}"
  local file
  file="$(run_service hadoop bash -lc "hdfs dfs -ls ${path} 2>/dev/null | awk '{print \\\$8}' | tail -n 1")"
  if [ -z "${file}" ]; then
    echo "Sin ficheros en ${path}"
    return
  fi
  echo "Fichero: ${file}"
  run_service hadoop bash -lc "hdfs dfs -cat '${file}' | head -n 3"
}

show_hdfs_file /data/raw/nifi/gps "raw GPS en HDFS"
show_hdfs_file /data/raw/nifi/weather "raw weather en HDFS"
