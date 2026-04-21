from __future__ import annotations

"""
DAG: kdd_daily_disk_cleanup

Proposito:
- Liberar espacio de forma preventiva sin afectar historicos de entrenamiento.

Garantias de seguridad:
- Verifica que los datasets base de entrenamiento existan en HDFS.
- Limpia solo artefactos locales de NiFi (raw-archive/input/failures) y temporales.
- No toca /data/raw/gps_events.jsonl, /data/curated ni tablas Hive.

Frecuencia:
- Se ejecuta cada 4 horas. El script aplica limpieza efectiva solo cuando se supera
  el umbral de uso de disco configurado.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="kdd_daily_disk_cleanup",
    start_date=datetime(2026, 1, 1),
    schedule="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["big-data", "ops", "cleanup", "disk"],
) as dag:
    verify_training_datasets = BashOperator(
        task_id="verify_training_datasets",
        bash_command=(
            "set -euo pipefail; "
            "docker exec hadoop bash -lc '"
            "hdfs dfs -test -f /data/raw/gps_events.jsonl && "
            "hdfs dfs -test -d /data/curated && "
            "hdfs dfsadmin -safemode get | grep -q \"Safe mode is OFF\"'"
        ),
    )

    safe_disk_cleanup = BashOperator(
        task_id="safe_disk_cleanup",
        # Espacio final: evita que Airflow/Jinja trate el comando como plantilla de archivo.
        bash_command="bash /opt/project/scripts/safe_disk_cleanup.sh ",
    )

    cleanup_hdfs_tmp_checkpoints = BashOperator(
        task_id="cleanup_hdfs_tmp_checkpoints",
        bash_command="docker exec hadoop hdfs dfs -rm -r -f /tmp/checkpoints || true",
    )

    verify_training_datasets >> safe_disk_cleanup >> cleanup_hdfs_tmp_checkpoints
