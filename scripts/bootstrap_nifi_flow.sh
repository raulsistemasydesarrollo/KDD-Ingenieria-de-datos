#!/usr/bin/env bash
set -euo pipefail

# Wrapper de bootstrap NiFi.
# - Normaliza variables de entorno (URL, credenciales, nombre de PG).
# - Ejecuta el bootstrap Python que crea/actualiza el flujo por API.
# - Punto de entrada recomendado para reconstruir flujo sin acciones manuales en UI.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export NIFI_URL="${NIFI_URL:-https://localhost:8443/nifi-api}"
export NIFI_USER="${NIFI_USER:-admin}"
export NIFI_PASS="${NIFI_PASS:-adminadminadmin}"
export NIFI_PG_NAME="${NIFI_PG_NAME:-kdd_ingestion_auto_v9}"

python3 scripts/bootstrap_nifi_flow.py
