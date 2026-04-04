#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="/opt/project/data/state"
STATE_FILE="${STATE_DIR}/retrain_runtime_state.json"

mkdir -p "${STATE_DIR}"

now_iso() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

write_state() {
  local status="$1"
  local message="$2"
  local started_at="$3"
  local finished_at="$4"
  local exit_code="$5"
  local trigger="$6"
  local source="$7"
  local run_id="$8"

  mkdir -p "${STATE_DIR}"
  python3 - "$STATE_FILE" "$status" "$message" "$started_at" "$finished_at" "$exit_code" "$trigger" "$source" "$run_id" <<'PY'
import json
import sys

state_file, status, message, started_at, finished_at, exit_code, trigger, source, run_id = sys.argv[1:]
payload = {
    "status": status,
    "message": message,
    "started_at": started_at or None,
    "finished_at": finished_at or None,
    "exit_code": int(exit_code) if exit_code not in {"", "null"} else None,
    "trigger": trigger or None,
    "source": source or "airflow_dag",
    "run_id": run_id or None,
    "updated_at": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
}
with open(state_file, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, ensure_ascii=True)
PY
}

STARTED_AT="$(now_iso)"
RUN_ID="${AIRFLOW_CTX_DAG_RUN_ID:-manual}"
TRIGGER="airflow:${RUN_ID}"

write_state "running" "Reentrenamiento mensual en ejecucion desde Airflow." "${STARTED_AT}" "" "" "${TRIGGER}" "airflow_dag" "${RUN_ID}"

set +e
docker exec spark-client bash /opt/spark-app/run-batch.sh
RC=$?
set -e

FINISHED_AT="$(now_iso)"
if [ "${RC}" -eq 0 ]; then
  write_state "done" "Reentrenamiento mensual completado por Airflow." "${STARTED_AT}" "${FINISHED_AT}" "${RC}" "${TRIGGER}" "airflow_dag" "${RUN_ID}"
else
  write_state "error" "Reentrenamiento mensual fallo en Airflow (exit_code=${RC})." "${STARTED_AT}" "${FINISHED_AT}" "${RC}" "${TRIGGER}" "airflow_dag" "${RUN_ID}"
fi

exit "${RC}"
