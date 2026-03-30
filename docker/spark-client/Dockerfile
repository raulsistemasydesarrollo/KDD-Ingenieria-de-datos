FROM maven:3.9.9-eclipse-temurin-21

ARG SPARK_VERSION=3.5.5
ARG HADOOP_MAJOR=3

RUN apt-get update && apt-get install -y --no-install-recommends curl python3 tini && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz" \
    | tar -xz -C /opt && \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}" /opt/spark

RUN mkdir -p /opt/hadoop/etc/hadoop

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

WORKDIR /opt/spark-app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sleep", "infinity"]
