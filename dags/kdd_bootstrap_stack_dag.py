from __future__ import annotations

"""
DAG: kdd_bootstrap_stack

Funcion:
- Arranca los servicios no-Airflow del stack KDD desde el propio scheduler.
- Sirve como "arranque orquestado" para demos y recuperacion operativa.

Flujo:
1) Levanta servicios core con docker compose.
2) Asegura objetos Hive de compatibilidad para vistas streaming.
3) Publica estado final del stack para trazabilidad en logs de Airflow.

Notas:
- Reintenta una vez en caso de error transitorio.
- Soporta alerta por email si AIRFLOW_ALERT_EMAIL esta definido.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


NON_AIRFLOW_SERVICES = (
    "kafka nifi gps-generator "
    "hadoop raw-hdfs-loader "
    "postgres-metastore hive-metastore "
    "cassandra spark-client dashboard"
)


def notify_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    print(f"[ALERT] DAG failure detected - dag_id={dag_id} task_id={task_id} run_id={run_id}")


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

alert_email = os.environ.get("AIRFLOW_ALERT_EMAIL")
if alert_email:
    default_args["email"] = [alert_email]
    default_args["email_on_failure"] = True


with DAG(
    dag_id="kdd_bootstrap_stack",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    on_failure_callback=notify_failure,
    tags=["big-data", "kdd", "bootstrap", "airflow"],
) as dag:
    start_non_airflow_stack = BashOperator(
        task_id="start_non_airflow_stack",
        bash_command=(
            "cd /opt/project && "
            "docker compose up -d --build " + NON_AIRFLOW_SERVICES
        ),
    )

    ensure_hive_streaming_compat = BashOperator(
        task_id="ensure_hive_streaming_compat",
        bash_command=(
            "cd /opt/project && "
            "if [ -x ./scripts/ensure_hive_streaming_compat.sh ]; then "
            "./scripts/ensure_hive_streaming_compat.sh; "
            "else chmod +x ./scripts/ensure_hive_streaming_compat.sh && "
            "./scripts/ensure_hive_streaming_compat.sh; fi"
        ),
    )

    show_stack_status = BashOperator(
        task_id="show_stack_status",
        bash_command="cd /opt/project && docker compose ps",
    )

    start_non_airflow_stack >> ensure_hive_streaming_compat >> show_stack_status
