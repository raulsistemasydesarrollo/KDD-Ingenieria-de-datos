# Plantilla NiFi - Weather API a Kafka (estado actual)

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Plantilla de flujo meteo en NiFi`
- Version: `v1.3`
- Fecha: `03/04/2026`
- Repositorio GitHub: `https://github.com/raulsistemasydesarrollo/KDD-Ingenieria-de-datos`

## Objetivo

Implementar en NiFi la ingesta meteorologica desde API publica y publicar en:

- `transport.weather.raw`
- `transport.weather.filtered`

Con archivado raw local en:

- `/opt/nifi/nifi-current/raw-archive/weather`

La subida a HDFS se realiza fuera de NiFi mediante `raw-hdfs-loader`.

## Opcion recomendada (automatizada)

Usar bootstrap en lugar de montarlo manualmente:

```bash
./scripts/bootstrap_nifi_flow.sh
```

Por defecto crea el PG `kdd_ingestion_auto_v9` con subgrupo `weather_ingestion`.

## Flujo logico de procesadores

1. `GenerateFlowFile` (`tick_weather`)
2. `InvokeHTTP` (`invoke_weather_api`)
3. `EvaluateJsonPath` (`extract_weather_fields`)
4. `UpdateAttribute` (`update_weather_attrs`)
5. `AttributesToJSON` (`attrs_to_weather_json`)
6. `PublishKafka` (`publish_weather_raw`)
7. `PublishKafka` (`publish_weather_filtered`)
8. `PutFile` (`archive_weather_raw_local`)
9. `PutFile` (`weather_failure_sink`)

## Configuracion clave

### `InvokeHTTP`

- Method: `GET`
- URL:
  - `https://api.open-meteo.com/v1/forecast?latitude=40.4168&longitude=-3.7038&current=temperature_2m,precipitation,wind_speed_10m,weather_code`
- Always Output Response: `true`

### `EvaluateJsonPath`

- `temperature_c` -> `$.current.temperature_2m`
- `precipitation_mm` -> `$.current.precipitation`
- `wind_kmh` -> `$.current.wind_speed_10m`
- `weather_code` -> `$.current.weather_code`

### `UpdateAttribute`

- `weather_event_id = ${UUID()}`
- `warehouse_id = WH1` (o el identificador de referencia que uses)
- `source = open-meteo`
- `observation_time` en UTC

### `PublishKafka`

- Connection: `Kafka3ConnectionService`
- Raw topic: `transport.weather.raw`
- Filtered topic: `transport.weather.filtered`

### `PutFile`

- Raw archive: `/opt/nifi/nifi-current/raw-archive/weather`
- Failure sink: `/opt/nifi/nifi-current/raw-archive/failures/weather`

## Relaciones recomendadas

- Camino principal:
  - `tick_weather -> invoke_weather_api -> extract_weather_fields -> update_weather_attrs -> attrs_to_weather_json -> publish_weather_raw -> publish_weather_filtered -> archive_weather_raw_local`
- Errores:
  - `failure|retry|no retry|unmatched` a `weather_failure_sink`

## Nota operativa

El servicio `raw-hdfs-loader` sincroniza los raw archivados por NiFi a HDFS:

- origen local: `nifi/raw-archive/weather`
- destino HDFS: `/data/raw/nifi/weather`
