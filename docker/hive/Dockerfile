FROM apache/hive:3.1.3

USER root

RUN if command -v microdnf >/dev/null 2>&1; then \
      microdnf install -y findutils curl && microdnf clean all; \
    elif command -v apt-get >/dev/null 2>&1; then \
      apt-get update && apt-get install -y --no-install-recommends findutils curl && rm -rf /var/lib/apt/lists/*; \
    else \
      echo "No package manager available to install findutils" >&2; \
      exit 1; \
    fi
RUN curl -fsSL "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar" -o /opt/hive/lib/postgresql-42.7.4.jar

COPY docker/hive/conf/hive-site.xml /opt/hive/conf/hive-site.xml

USER hive
