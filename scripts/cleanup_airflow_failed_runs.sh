#!/usr/bin/env bash
set -euo pipefail

# Limpia ejecuciones failed historicas de DAGs concretos.
# - Por defecto actua sobre healthchecks diario y horario.
# - Soporta modo dry-run (sin --apply) y modo aplicacion real.
# - Util para dejar el panel de Airflow en estado limpio antes de demo/entrega.

DAGS="kdd_daily_healthcheck,kdd_hourly_healthcheck"
APPLY=0

usage() {
  cat <<'EOF'
Uso:
  ./scripts/cleanup_airflow_failed_runs.sh [--dags dag1,dag2,...] [--apply]

Opciones:
  --dags   Lista CSV de DAG IDs a limpiar (por defecto: kdd_daily_healthcheck,kdd_hourly_healthcheck)
  --apply  Aplica borrado de runs en estado failed. Sin esta opcion solo muestra (dry-run).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dags)
      DAGS="${2:-}"
      shift 2
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Parametro no reconocido: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$DAGS" ]]; then
  echo "ERROR: --dags no puede estar vacio" >&2
  exit 1
fi

if [[ ! "$DAGS" =~ ^[a-zA-Z0-9_,-]+$ ]]; then
  echo "ERROR: --dags contiene caracteres no permitidos" >&2
  exit 1
fi

echo "DAGs objetivo: $DAGS"
echo "Mostrando runs failed actuales..."
docker exec airflow-postgres psql -U airflow -d airflow -v ON_ERROR_STOP=1 -c "\
SELECT dag_id, run_id, state, execution_date \
FROM dag_run \
WHERE state='failed' \
  AND dag_id = ANY(string_to_array('${DAGS}', ',')) \
ORDER BY execution_date DESC;"

if [[ "$APPLY" -ne 1 ]]; then
  echo
  echo "Modo dry-run. Para borrar definitivamente, ejecuta:"
  echo "  ./scripts/cleanup_airflow_failed_runs.sh --dags ${DAGS} --apply"
  exit 0
fi

echo
echo "Borrando runs failed de los DAGs objetivo..."
docker exec airflow-postgres psql -U airflow -d airflow -v ON_ERROR_STOP=1 -c "\
DELETE FROM dag_run \
WHERE state='failed' \
  AND dag_id = ANY(string_to_array('${DAGS}', ','));"

echo
echo "Runs failed restantes:"
docker exec airflow-postgres psql -U airflow -d airflow -v ON_ERROR_STOP=1 -c "\
SELECT dag_id, count(*) AS failed_runs \
FROM dag_run \
WHERE state='failed' \
  AND dag_id = ANY(string_to_array('${DAGS}', ',')) \
GROUP BY dag_id \
ORDER BY dag_id;"

echo "Limpieza completada."
