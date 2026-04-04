#!/usr/bin/env bash
set -euo pipefail

# Ajuste opcional para que el navegador del host resuelva "hadoop" cuando
# NameNode redirige a WebHDFS/DataNode (http://hadoop:9864/...).
# No cambia configuracion de HDFS ni impacta Spark.
#
# Uso:
#   ./scripts/toggle_hdfs_browser_preview_hosts.sh status
#   ./scripts/toggle_hdfs_browser_preview_hosts.sh enable
#   ./scripts/toggle_hdfs_browser_preview_hosts.sh disable

HOSTS_FILE="/etc/hosts"
ENTRY_IP="127.0.0.1"
ENTRY_HOST="hadoop"
ENTRY_LINE="${ENTRY_IP} ${ENTRY_HOST}"

usage() {
  echo "Uso: $0 {status|enable|disable}"
}

has_entry() {
  grep -E "^[[:space:]]*${ENTRY_IP}[[:space:]]+${ENTRY_HOST}([[:space:]]|\$)" "${HOSTS_FILE}" >/dev/null 2>&1
}

status_cmd() {
  if has_entry; then
    echo "OK: ${ENTRY_HOST} ya resuelve a ${ENTRY_IP} en ${HOSTS_FILE}."
  else
    echo "MISSING: falta entrada '${ENTRY_LINE}' en ${HOSTS_FILE}."
  fi
}

enable_cmd() {
  if has_entry; then
    echo "Sin cambios: la entrada ya existe."
    exit 0
  fi
  if [ "${EUID}" -ne 0 ]; then
    echo "Se requieren permisos root para editar ${HOSTS_FILE}."
    echo "Ejecuta: sudo $0 enable"
    exit 1
  fi
  printf "\n# proyectoBigData: HDFS browser preview\n%s\n" "${ENTRY_LINE}" >> "${HOSTS_FILE}"
  echo "Añadida entrada: ${ENTRY_LINE}"
}

disable_cmd() {
  if [ "${EUID}" -ne 0 ]; then
    echo "Se requieren permisos root para editar ${HOSTS_FILE}."
    echo "Ejecuta: sudo $0 disable"
    exit 1
  fi
  tmp="$(mktemp)"
  awk '
    BEGIN {skip=0}
    /# proyectoBigData: HDFS browser preview/ {skip=1; next}
    skip==1 && $1=="127.0.0.1" && $2=="hadoop" {skip=0; next}
    {print}
  ' "${HOSTS_FILE}" > "${tmp}"
  cat "${tmp}" > "${HOSTS_FILE}"
  rm -f "${tmp}"
  echo "Eliminada entrada opcional de preview HDFS (si existia)."
}

cmd="${1:-}"
case "${cmd}" in
  status) status_cmd ;;
  enable) enable_cmd ;;
  disable) disable_cmd ;;
  *) usage; exit 1 ;;
esac

