#!/usr/bin/env bash
set -euo pipefail

# Launcher del modo batch de Spark.
# Flujo:
# 1) Compila jar si codigo/pom cambiaron.
# 2) Garantiza jars auxiliares requeridos en runtime.
# 3) Ejecuta spark-submit en YARN con dependencias Kafka/GraphFrames/Cassandra.
# 4) Lanza com.proyectobigdata.LogisticsAnalyticsJob en modo "batch".

cd /opt/spark-app

JAR_PATH="target/spark-app-1.0.0.jar"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-4}"
SPARK_AUTO_BROADCAST_THRESHOLD="${SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD:-20971520}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-1536m}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-2}"
SPARK_MIN_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS:-1}"
SPARK_MAX_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS:-2}"

if [ ! -f "${JAR_PATH}" ] || [ -n "$(find src pom.xml -type f -newer "${JAR_PATH}" 2>/dev/null | head -n 1)" ]; then
  mvn -DskipTests package
fi

mkdir -p /root/.m2/repository/org/slf4j/slf4j-api/2.0.7
if [ ! -f /root/.m2/repository/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar ]; then
  curl -fsSL "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar" \
    -o /root/.m2/repository/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar
fi

mkdir -p /root/.m2/repository/io/dropwizard/metrics/metrics-core/4.1.18
if [ ! -f /root/.m2/repository/io/dropwizard/metrics/metrics-core/4.1.18/metrics-core-4.1.18.jar ]; then
  curl -fsSL "https://repo1.maven.org/maven2/io/dropwizard/metrics/metrics-core/4.1.18/metrics-core-4.1.18.jar" \
    -o /root/.m2/repository/io/dropwizard/metrics/metrics-core/4.1.18/metrics-core-4.1.18.jar
fi

/opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --class com.proyectobigdata.LogisticsAnalyticsJob \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY}" \
  --executor-cores "${SPARK_EXECUTOR_CORES}" \
  --conf spark.dynamicAllocation.enabled=true \
  --conf "spark.dynamicAllocation.minExecutors=${SPARK_MIN_EXECUTORS}" \
  --conf "spark.dynamicAllocation.maxExecutors=${SPARK_MAX_EXECUTORS}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  --conf "spark.sql.autoBroadcastJoinThreshold=${SPARK_AUTO_BROADCAST_THRESHOLD}" \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.rdd.compress=true \
  --conf spark.shuffle.compress=true \
  --conf spark.shuffle.spill.compress=true \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.graphframes:graphframes-spark3_2.12:0.9.0-spark3.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  "${JAR_PATH}" \
  batch
