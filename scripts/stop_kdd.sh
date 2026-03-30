#!/usr/bin/env bash
set -euo pipefail

# Parada controlada del entorno KDD.
# - Ejecuta docker compose down sobre el stack completo.
# - Mantiene la forma canonica de apagar servicios para evitar residuos de estado.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "Parando stack completo del proyecto KDD..."
sg docker -c "docker compose down"

echo
echo "Stack KDD parado."
