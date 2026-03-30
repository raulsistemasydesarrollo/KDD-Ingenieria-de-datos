#!/usr/bin/env bash
set -euo pipefail

# Launcher del modo streaming de Spark.
# Flujo:
# 1) Compila jar si hay cambios.
# 2) Asegura librerias auxiliares necesarias.
# 3) Ejecuta spark-submit en YARN con conectores de Kafka y Cassandra.
# 4) Lanza com.proyectobigdata.LogisticsAnalyticsJob en modo "streaming".

cd /opt/spark-app

JAR_PATH="target/spark-app-1.0.0.jar"

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
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.graphframes:graphframes-spark3_2.12:0.9.0-spark3.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  "${JAR_PATH}" \
  streaming
