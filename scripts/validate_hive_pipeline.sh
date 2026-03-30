#!/usr/bin/env bash
set -euo pipefail

# Valida escritura a Hive en batch y streaming.
# Requisitos:
# - Ejecutar desde la raiz del proyecto.
# - Usuario con permisos Docker (grupo docker).
# Flujo de validacion:
# - Compilar Spark app, ejecutar batch y comprobar tablas base.
# - Levantar streaming, inyectar eventos de prueba en Kafka.
# - Confirmar recepcion en tablas Hive de streaming.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

run() {
  echo
  echo "==> $*"
  eval "$@"
}

echo "Validacion E2E Hive (batch + streaming)"
echo "Proyecto: ${ROOT_DIR}"

EVENT_TS_A="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
EVENT_TS_B="$(date -u -d "+1 minute" +"%Y-%m-%dT%H:%M:%SZ")"
echo "Timestamp prueba A (UTC): ${EVENT_TS_A}"
echo "Timestamp prueba B (UTC): ${EVENT_TS_B}"

run "sg docker -c \"docker compose up -d --build\""
run "sg docker -c \"docker compose exec -T hadoop bash /opt/hadoop/bootstrap/load-seed-data.sh\""
run "sg docker -c \"docker compose exec -T spark-client mvn -f /opt/spark-app/pom.xml -DskipTests package\""
run "sg docker -c \"docker compose exec -T spark-client bash /opt/spark-app/run-batch.sh\""

run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"SHOW TABLES IN transport_analytics;\\\"\""
run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"SELECT COUNT(*) AS enriched_count FROM transport_analytics.enriched_events; SELECT COUNT(*) AS delay_batch_count FROM transport_analytics.delay_metrics_batch; SELECT COUNT(*) AS graph_rank_count FROM transport_analytics.route_graph_metrics; SELECT COUNT(*) AS shortest_path_count FROM transport_analytics.route_shortest_paths; SELECT COUNT(*) AS ml_scores_count FROM transport_analytics.ml_delay_risk_scores; SELECT COUNT(*) AS master_warehouses_count FROM transport_analytics.master_warehouses; SELECT COUNT(*) AS master_vehicles_count FROM transport_analytics.master_vehicles;\\\"\""

run "sg docker -c \"docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic transport.filtered --partitions 3 --replication-factor 1\""
run "sg docker -c \"docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic transport.weather.filtered --partitions 3 --replication-factor 1\""
run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"DROP TABLE IF EXISTS transport_analytics.delay_metrics_streaming; DROP TABLE IF EXISTS transport_analytics.weather_observations_streaming;\\\"\""
run "sg docker -c \"docker compose exec -T hadoop bash -lc 'hdfs dfs -rm -r -f /tmp/checkpoints/delay_metrics /tmp/checkpoints/latest_vehicle_state /tmp/checkpoints/weather_observations /data/curated/delay_metrics_streaming /data/curated/weather_observations_streaming || true'\""

echo
echo "==> Lanzando streaming (timeout 240s) en segundo plano..."
sg docker -c "docker compose exec -T spark-client bash -lc 'STREAMING_STARTING_OFFSETS=earliest timeout 240 bash /opt/spark-app/run-streaming.sh'" &
STREAM_PID=$!

echo "==> Esperando 100s para asegurar que el streaming queda activo..."
sleep 100

run "sg docker -c \"docker compose exec -T kafka bash -lc 'printf \\\"{\\\\\\\"event_id\\\\\\\":\\\\\\\"stream_evt_a1\\\\\\\",\\\\\\\"vehicle_id\\\\\\\":\\\\\\\"TRUCK-001\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"MAD\\\\\\\",\\\\\\\"route_id\\\\\\\":\\\\\\\"R1\\\\\\\",\\\\\\\"event_type\\\\\\\":\\\\\\\"position\\\\\\\",\\\\\\\"latitude\\\\\\\":40.4168,\\\\\\\"longitude\\\\\\\":-3.7038,\\\\\\\"delay_minutes\\\\\\\":1,\\\\\\\"speed_kmh\\\\\\\":55.0,\\\\\\\"event_time\\\\\\\":\\\\\\\"${EVENT_TS_A}\\\\\\\"}\\\\n{\\\\\\\"event_id\\\\\\\":\\\\\\\"stream_evt_a2\\\\\\\",\\\\\\\"vehicle_id\\\\\\\":\\\\\\\"TRUCK-001\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"MAD\\\\\\\",\\\\\\\"route_id\\\\\\\":\\\\\\\"R1\\\\\\\",\\\\\\\"event_type\\\\\\\":\\\\\\\"position\\\\\\\",\\\\\\\"latitude\\\\\\\":40.4168,\\\\\\\"longitude\\\\\\\":-3.7038,\\\\\\\"delay_minutes\\\\\\\":2,\\\\\\\"speed_kmh\\\\\\\":55.0,\\\\\\\"event_time\\\\\\\":\\\\\\\"${EVENT_TS_A}\\\\\\\"}\\\\n{\\\\\\\"event_id\\\\\\\":\\\\\\\"stream_evt_a3\\\\\\\",\\\\\\\"vehicle_id\\\\\\\":\\\\\\\"TRUCK-001\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"MAD\\\\\\\",\\\\\\\"route_id\\\\\\\":\\\\\\\"R1\\\\\\\",\\\\\\\"event_type\\\\\\\":\\\\\\\"position\\\\\\\",\\\\\\\"latitude\\\\\\\":40.4168,\\\\\\\"longitude\\\\\\\":-3.7038,\\\\\\\"delay_minutes\\\\\\\":3,\\\\\\\"speed_kmh\\\\\\\":55.0,\\\\\\\"event_time\\\\\\\":\\\\\\\"${EVENT_TS_A}\\\\\\\"}\\\\n{\\\\\\\"event_id\\\\\\\":\\\\\\\"stream_evt_a4\\\\\\\",\\\\\\\"vehicle_id\\\\\\\":\\\\\\\"TRUCK-001\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"MAD\\\\\\\",\\\\\\\"route_id\\\\\\\":\\\\\\\"R1\\\\\\\",\\\\\\\"event_type\\\\\\\":\\\\\\\"position\\\\\\\",\\\\\\\"latitude\\\\\\\":40.4168,\\\\\\\"longitude\\\\\\\":-3.7038,\\\\\\\"delay_minutes\\\\\\\":4,\\\\\\\"speed_kmh\\\\\\\":55.0,\\\\\\\"event_time\\\\\\\":\\\\\\\"${EVENT_TS_A}\\\\\\\"}\\\\n{\\\\\\\"event_id\\\\\\\":\\\\\\\"stream_evt_a5\\\\\\\",\\\\\\\"vehicle_id\\\\\\\":\\\\\\\"TRUCK-001\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"MAD\\\\\\\",\\\\\\\"route_id\\\\\\\":\\\\\\\"R1\\\\\\\",\\\\\\\"event_type\\\\\\\":\\\\\\\"position\\\\\\\",\\\\\\\"latitude\\\\\\\":40.4168,\\\\\\\"longitude\\\\\\\":-3.7038,\\\\\\\"delay_minutes\\\\\\\":5,\\\\\\\"speed_kmh\\\\\\\":55.0,\\\\\\\"event_time\\\\\\\":\\\\\\\"${EVENT_TS_A}\\\\\\\"}\\\\n\\\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic transport.filtered >/dev/null'\""
run "sg docker -c \"docker compose exec -T kafka bash -lc 'printf \\\"{\\\\\\\"weather_event_id\\\\\\\":\\\\\\\"w_evt_1\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"MAD\\\\\\\",\\\\\\\"temperature_c\\\\\\\":18.4,\\\\\\\"precipitation_mm\\\\\\\":0.2,\\\\\\\"wind_kmh\\\\\\\":9.1,\\\\\\\"weather_code\\\\\\\":\\\\\\\"partly_cloudy\\\\\\\",\\\\\\\"source\\\\\\\":\\\\\\\"open-meteo\\\\\\\",\\\\\\\"observation_time\\\\\\\":\\\\\\\"${EVENT_TS_A}\\\\\\\"}\\\\n{\\\\\\\"weather_event_id\\\\\\\":\\\\\\\"w_evt_2\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"BCN\\\\\\\",\\\\\\\"temperature_c\\\\\\\":17.1,\\\\\\\"precipitation_mm\\\\\\\":0.0,\\\\\\\"wind_kmh\\\\\\\":12.4,\\\\\\\"weather_code\\\\\\\":\\\\\\\"clear\\\\\\\",\\\\\\\"source\\\\\\\":\\\\\\\"open-meteo\\\\\\\",\\\\\\\"observation_time\\\\\\\":\\\\\\\"${EVENT_TS_A}\\\\\\\"}\\\\n\\\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic transport.weather.filtered >/dev/null'\""
sleep 25
run "sg docker -c \"docker compose exec -T kafka bash -lc 'printf \\\"{\\\\\\\"event_id\\\\\\\":\\\\\\\"stream_evt_b1\\\\\\\",\\\\\\\"vehicle_id\\\\\\\":\\\\\\\"TRUCK-002\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"BCN\\\\\\\",\\\\\\\"route_id\\\\\\\":\\\\\\\"R2\\\\\\\",\\\\\\\"event_type\\\\\\\":\\\\\\\"position\\\\\\\",\\\\\\\"latitude\\\\\\\":41.3874,\\\\\\\"longitude\\\\\\\":2.1686,\\\\\\\"delay_minutes\\\\\\\":8,\\\\\\\"speed_kmh\\\\\\\":42.0,\\\\\\\"event_time\\\\\\\":\\\\\\\"${EVENT_TS_B}\\\\\\\"}\\\\n\\\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic transport.filtered >/dev/null'\""
run "sg docker -c \"docker compose exec -T kafka bash -lc 'printf \\\"{\\\\\\\"weather_event_id\\\\\\\":\\\\\\\"w_evt_3\\\\\\\",\\\\\\\"warehouse_id\\\\\\\":\\\\\\\"MAD\\\\\\\",\\\\\\\"temperature_c\\\\\\\":18.9,\\\\\\\"precipitation_mm\\\\\\\":0.0,\\\\\\\"wind_kmh\\\\\\\":11.3,\\\\\\\"weather_code\\\\\\\":\\\\\\\"clear\\\\\\\",\\\\\\\"source\\\\\\\":\\\\\\\"open-meteo\\\\\\\",\\\\\\\"observation_time\\\\\\\":\\\\\\\"${EVENT_TS_B}\\\\\\\"}\\\\n\\\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic transport.weather.filtered >/dev/null'\""

echo
echo "==> Esperando a que termine streaming..."
wait "${STREAM_PID}" || true

run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"SHOW TABLES IN transport_analytics;\\\"\""
run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"SELECT COUNT(*) AS streaming_count FROM transport_analytics.delay_metrics_streaming;\\\"\""
run "sg docker -c \"docker compose exec -T spark-client spark-sql -e \\\"SELECT COUNT(*) AS weather_streaming_count FROM transport_analytics.weather_observations_streaming;\\\"\""

echo
echo "Validacion finalizada."
echo "Si streaming_count > 0 y weather_streaming_count > 0, la escritura streaming (GPS + meteo) en Hive esta OK."
