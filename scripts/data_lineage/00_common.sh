#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

section() {
  printf '\n===== %s =====\n' "$1"
}

service_running() {
  local service="$1"
  local cid
  cid="$(docker compose ps -q "${service}" 2>/dev/null || true)"
  [ -n "${cid}" ] && [ "$(docker inspect -f '{{.State.Running}}' "${cid}" 2>/dev/null || true)" = "true" ]
}

ensure_service() {
  local service="$1"
  if ! service_running "${service}"; then
    echo "[WARN] El servicio '${service}' no esta corriendo."
    echo "       Levanta el stack con: docker compose up -d"
    return 1
  fi
  return 0
}

run_service() {
  local service="$1"
  shift
  docker compose exec -T "${service}" "$@"
}
