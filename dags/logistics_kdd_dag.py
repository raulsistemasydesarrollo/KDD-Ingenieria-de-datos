from __future__ import annotations

"""
DAG: logistics_kdd_monthly_maintenance

Proposito:
- Ejecutar mantenimiento mensual del pipeline analitico.

Tareas:
1) Recalcular batch/grafos/ML mediante run-batch.sh.
2) Limpiar checkpoints temporales para mantener higiene operativa.

Contexto:
- DAG de mantenimiento periodico, no orientado a tiempo real.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


def notify_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    print(f"[ALERT] DAG failure detected - dag_id={dag_id} task_id={task_id} run_id={run_id}")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

alert_email = os.environ.get("AIRFLOW_ALERT_EMAIL")
if alert_email:
    default_args["email"] = [alert_email]
    default_args["email_on_failure"] = True


with DAG(
    dag_id="logistics_kdd_monthly_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    default_args=default_args,
    on_failure_callback=notify_failure,
    tags=["big-data", "spark", "hadoop", "kdd"],
) as dag:
    retrain_graph_model = BashOperator(
        task_id="retrain_graph_model",
        # Nota: el espacio final evita que Airflow/Jinja intente resolver todo el comando
        # como una plantilla de archivo .sh (TemplateNotFound).
        bash_command="docker exec spark-client bash /opt/spark-app/run-batch.sh ",
    )

    cleanup_temp_hdfs = BashOperator(
        task_id="cleanup_temp_hdfs",
        bash_command="docker exec hadoop hdfs dfs -rm -r -f /tmp/checkpoints || true",
    )

    retrain_graph_model >> cleanup_temp_hdfs
