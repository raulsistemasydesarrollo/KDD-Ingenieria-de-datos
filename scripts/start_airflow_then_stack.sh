#!/usr/bin/env bash
set -euo pipefail

# Arranque "Airflow primero".
# Flujo:
# 1) Levanta solo servicios de Airflow.
# 2) Espera health endpoint del webserver.
# 3) Despausa y dispara DAG kdd_bootstrap_stack para arrancar resto del stack.
# Objetivo: demostrar orquestacion desde Airflow como punto de control.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

DAG_ID="${DAG_ID:-kdd_bootstrap_stack}"

echo "Arrancando solo Airflow (postgres/init/webserver/scheduler)..."
sg docker -c "docker compose up -d --build airflow-postgres airflow-init airflow-webserver airflow-scheduler"

echo
echo "Esperando a que Airflow webserver responda en http://localhost:8080/health ..."
for _ in $(seq 1 60); do
  if curl -fsS "http://localhost:8080/health" >/dev/null 2>&1; then
    echo "Airflow webserver disponible."
    break
  fi
  sleep 2
done

if ! curl -fsS "http://localhost:8080/health" >/dev/null 2>&1; then
  echo "ERROR: Airflow webserver no responde en el tiempo esperado." >&2
  exit 1
fi

echo
echo "Despausando y lanzando DAG '${DAG_ID}' para arrancar el resto del stack..."
sg docker -c "docker compose exec -T airflow-webserver airflow dags unpause ${DAG_ID} >/dev/null || true"
sg docker -c "docker compose exec -T airflow-webserver airflow dags trigger ${DAG_ID}"

echo
echo "Ultimos DAG runs de ${DAG_ID}:"
sg docker -c "docker compose exec -T airflow-webserver airflow dags list-runs -d ${DAG_ID} -o table | head -n 20"

echo
echo "Listo: Airflow esta arrancado y el DAG de bootstrap fue disparado."
