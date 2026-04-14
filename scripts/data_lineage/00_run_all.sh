#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

for script in \
  01_origen_maestros_locales.sh \
  02_ingesta_gps_nifi_input.sh \
  03_ingesta_weather_nifi_raw_archive.sh \
  04_nifi_raw_archive_gps.sh \
  05_kafka_topics_raw_y_filtrados.sh \
  06_hdfs_raw_nifi.sh \
  07_hdfs_seed_batch_input.sh \
  08_hive_batch_grafo_ml.sh \
  09_hive_streaming_y_vistas_madrid.sh \
  10_cassandra_operacional.sh \
  11_modelo_ia_y_artifacto_hdfs.sh \
  12_dashboard_fuentes_y_consumo.sh; do
  echo
  echo "############################################################"
  echo "Ejecutando ${script}"
  echo "############################################################"
  "${BASE_DIR}/${script}" || true
  echo
  sleep 1
done
