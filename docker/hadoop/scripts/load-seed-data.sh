#!/usr/bin/env bash
set -euo pipefail

# Carga datasets semilla desde el volumen del proyecto hacia HDFS.
# Estructura cargada:
# - /data/raw
# - /data/master
# - /data/graph
# Se usa para inicializar entorno de pruebas/demo antes de ejecutar Spark batch.

HDFS_BIN="${HADOOP_HOME:-/opt/hadoop}/bin/hdfs"

"${HDFS_BIN}" dfs -mkdir -p /data/raw /data/master /data/graph
"${HDFS_BIN}" dfs -put -f /opt/project-data/raw/* /data/raw/
"${HDFS_BIN}" dfs -put -f /opt/project-data/master/* /data/master/
"${HDFS_BIN}" dfs -put -f /opt/project-data/graph/* /data/graph/

echo "Datasets semilla cargados en HDFS:"
"${HDFS_BIN}" dfs -ls -R /data
