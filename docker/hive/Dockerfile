FROM apache/hive:3.1.3

USER root

# La imagen oficial de Hive ya incluye driver JDBC PostgreSQL,
# asi evitamos descargar paquetes/JARs en build (entornos restringidos).

COPY docker/hive/conf/hive-site.xml /opt/hive/conf/hive-site.xml

USER hive
