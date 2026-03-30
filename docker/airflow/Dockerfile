FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl docker.io && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/hadoop/etc/hadoop
USER airflow
