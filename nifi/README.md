# NiFi

Este directorio contiene los ficheros de entrada de demostracion para que el flujo de NiFi publique eventos en Kafka y deje copia raw en HDFS.

En este proyecto se alimenta automaticamente con:

- `gps-generator` (contenedor Docker) que escribe JSONL continuos en `nifi/input`.
- Flujo NiFi recomendado en `docs/nifi-flow.md` para:
  - GPS -> `transport.raw` / `transport.filtered`
  - API meteo (InvokeHTTP) -> `transport.weather.raw` / `transport.weather.filtered`

Estado actual recomendado del bootstrap:

- Process Group: `kdd_ingestion_auto_v9`
- Subgrupos: `gps_ingestion`, `weather_ingestion`
- Script: `./scripts/bootstrap_nifi_flow.sh`
- Archivado GPS: nombre unico por split para evitar sobrescritura en `raw-archive/gps`.

Documentacion relacionada:

- `docs/nifi-flow.md`
- `docs/nifi-weather-template.md`
- `docs/operations.md`

Credenciales por defecto del contenedor:

- usuario: `admin`
- password: `adminadminadmin`
