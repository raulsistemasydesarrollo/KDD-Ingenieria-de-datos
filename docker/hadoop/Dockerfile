FROM eclipse-temurin:21-jdk

ARG HADOOP_VERSION=3.4.2

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    netcat-openbsd \
    python3 \
    tini \
    xmlstarlet \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar -xz -C /opt && \
    ln -s "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop

ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

COPY docker/hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY docker/hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
COPY docker/hadoop/conf/mapred-site.xml /opt/hadoop/etc/hadoop/mapred-site.xml
COPY docker/hadoop/conf/yarn-site.xml /opt/hadoop/etc/hadoop/yarn-site.xml
COPY docker/hadoop/conf/workers /opt/hadoop/etc/hadoop/workers
COPY docker/hadoop/scripts/entrypoint.sh /entrypoint.sh
COPY docker/hadoop/scripts/load-seed-data.sh /opt/hadoop/bootstrap/load-seed-data.sh

RUN chmod +x /entrypoint.sh /opt/hadoop/bootstrap/load-seed-data.sh && \
    mkdir -p /data/hdfs/namenode /data/hdfs/datanode /data/hdfs/tmp

EXPOSE 9870 9864 8088 19888 9000

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
