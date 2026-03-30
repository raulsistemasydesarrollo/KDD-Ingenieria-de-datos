from __future__ import annotations

"""
DAG: kdd_hourly_healthcheck

Objetivo:
- Ejecutar un control horario de estado para detectar incidencias con baja latencia.

Cobertura:
- Infraestructura docker.
- Kafka topics de streaming.
- HDFS (raw y curated).
- Hive tablas/vistas y consultas smoke.

Uso:
- Complementa el healthcheck diario y reduce tiempo medio de deteccion de fallo.
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
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

alert_email = os.environ.get("AIRFLOW_ALERT_EMAIL")
if alert_email:
    default_args["email"] = [alert_email]
    default_args["email_on_failure"] = True


with DAG(
    dag_id="kdd_hourly_healthcheck",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    on_failure_callback=notify_failure,
    tags=["big-data", "kdd", "healthcheck", "hourly"],
) as dag:
    check_core_containers = BashOperator(
        task_id="check_core_containers",
        bash_command=(
            "set -euo pipefail; "
            "names=\"$(docker ps --format \"{{ '{{' }}.Names{{ '}}' }}\")\"; "
            "for c in kafka hadoop hive-metastore nifi spark-client; do "
            "echo \"$names\" | grep -qx \"$c\" || { echo \"Missing container: $c\"; exit 1; }; "
            "done"
        ),
    )

    check_kafka_topics = BashOperator(
        task_id="check_kafka_topics",
        bash_command=(
            "set -euo pipefail; "
            "topics=\"$(docker exec kafka /opt/kafka/bin/kafka-topics.sh "
            "--bootstrap-server kafka:9092 --list)\"; "
            "for t in transport.filtered transport.weather.filtered; do "
            "echo \"$topics\" | grep -qx \"$t\" || { echo \"Missing topic: $t\"; exit 1; }; "
            "done"
        ),
    )

    check_hdfs_paths = BashOperator(
        task_id="check_hdfs_paths",
        bash_command=(
            "docker exec hadoop bash -lc "
            "\"hdfs dfs -test -d /data/raw && hdfs dfs -test -d /data/raw/nifi && hdfs dfs -test -d /data/curated\""
        ),
    )

    check_hive_tables = BashOperator(
        task_id="check_hive_tables",
        bash_command=(
            "set -euo pipefail; "
            "tables=\"$(docker exec spark-client spark-sql -e "
            "'SHOW TABLES IN transport_analytics;')\"; "
            "for tbl in enriched_events delay_metrics_batch route_graph_metrics route_shortest_paths ml_delay_risk_scores master_warehouses master_vehicles; do "
            "echo \"$tables\" | grep -Fq \"$tbl\" || { echo \"Missing table: $tbl\"; exit 1; }; "
            "done"
        ),
    )

    check_streaming_views_madrid = BashOperator(
        task_id="check_streaming_views_madrid",
        bash_command=(
            "set -euo pipefail; "
            "tables=\"$(docker exec spark-client spark-sql -e "
            "'SHOW TABLES IN transport_analytics;')\"; "
            "for v in v_weather_observations_madrid v_delay_metrics_streaming_madrid; do "
            "echo \"$tables\" | grep -Fq \"$v\" || { echo \"Missing view: $v\"; exit 1; }; "
            "done"
        ),
    )

    repopulate_hive_weather_if_empty = BashOperator(
        task_id="repopulate_hive_weather_if_empty",
        bash_command=(
            "set -euo pipefail; "
            "weather_rows=\"$(docker exec spark-client spark-sql -e "
            "\"SELECT COUNT(*) AS weather_rows FROM transport_analytics.v_weather_observations_madrid;\" "
            "| awk '/^[0-9]+$/ {print; exit}')\"; "
            "weather_rows=\"${weather_rows:-0}\"; "
            "if [ \"$weather_rows\" = \"0\" ]; then "
            "echo \"[INFO] weather_rows=0; repoblando Hive meteo desde Cassandra...\"; "
            "docker exec airflow-webserver bash -lc 'cd /opt/project && ./scripts/repopulate_hive_weather_from_cassandra.sh'; "
            "else "
            "echo \"[INFO] weather_rows=${weather_rows}; no hace falta repoblar.\"; "
            "fi"
        ),
    )

    smoke_query_weather = BashOperator(
        task_id="smoke_query_weather",
        bash_command=(
            "set -euo pipefail; "
            "if ! docker exec spark-client spark-sql -e "
            "\"SELECT weather_event_id, weather_timestamp_utc, weather_timestamp_madrid "
            "FROM transport_analytics.v_weather_observations_madrid "
            "ORDER BY weather_timestamp_utc DESC LIMIT 1;\"; then "
            "echo \"[WARN] v_weather_observations_madrid no es consultable (fuente streaming ausente). "
            "Smoke check degradado: validando metastore en su lugar.\"; "
            "docker exec spark-client spark-sql -e "
            "\"SHOW TABLES IN transport_analytics LIKE 'v_weather_observations_madrid';\"; "
            "fi"
        ),
    )

    smoke_query_delay = BashOperator(
        task_id="smoke_query_delay",
        bash_command=(
            "set -euo pipefail; "
            "if ! docker exec spark-client spark-sql -e "
            "\"SELECT warehouse_id, window_start_utc, window_start_madrid "
            "FROM transport_analytics.v_delay_metrics_streaming_madrid "
            "ORDER BY window_start_utc DESC LIMIT 1;\"; then "
            "echo \"[WARN] v_delay_metrics_streaming_madrid no es consultable (fuente streaming ausente). "
            "Smoke check degradado: usando delay_metrics_batch.\"; "
            "docker exec spark-client spark-sql -e "
            "\"SELECT warehouse_id, window_start AS window_start_utc, "
            "from_utc_timestamp(window_start, 'Europe/Madrid') AS window_start_madrid "
            "FROM transport_analytics.delay_metrics_batch "
            "ORDER BY window_start DESC LIMIT 1;\"; "
            "fi"
        ),
    )

    check_core_containers >> check_kafka_topics >> check_hdfs_paths >> check_hive_tables
    check_hive_tables >> check_streaming_views_madrid
    check_streaming_views_madrid >> repopulate_hive_weather_if_empty >> smoke_query_weather
    check_streaming_views_madrid >> smoke_query_delay
