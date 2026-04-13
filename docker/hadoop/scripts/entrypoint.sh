#!/usr/bin/env bash
set -euo pipefail

# Entrypoint del contenedor Hadoop pseudo-distribuido.
# Responsabilidades:
# - Formatear NameNode en primer arranque.
# - Arrancar NameNode/DataNode + YARN + HistoryServer.
# - Inicializar directorios HDFS requeridos por Hive y el proyecto.
# - Mantener proceso vivo para uso continuo del contenedor.

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/jre}"
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}"
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"

mkdir -p /data/hdfs/namenode /data/hdfs/datanode /data/hdfs/tmp
chmod -R 775 /data/hdfs || true

if [ ! -f /data/hdfs/namenode/current/VERSION ]; then
  "${HADOOP_HOME}/bin/hdfs" namenode -format -force -nonInteractive
fi

"${HADOOP_HOME}/bin/hdfs" --daemon start namenode
"${HADOOP_HOME}/bin/hdfs" --daemon start datanode
"${HADOOP_HOME}/bin/yarn" --daemon start resourcemanager
"${HADOOP_HOME}/bin/yarn" --daemon start nodemanager
"${HADOOP_HOME}/bin/mapred" --daemon start historyserver

until curl -fsS http://localhost:9870 >/dev/null 2>&1; do
  sleep 2
done

"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /tmp /user/hive/warehouse /data/raw /data/filtered /data/master /data/graph /data/curated || true
"${HADOOP_HOME}/bin/hdfs" dfs -chmod -R 777 /tmp /user/hive/warehouse /data || true

tail -f /dev/null
