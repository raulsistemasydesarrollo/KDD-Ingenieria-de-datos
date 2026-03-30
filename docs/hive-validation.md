# Validacion de Escritura a Hive

Este documento describe como ejecutar una validacion completa (batch + streaming) para confirmar que Spark escribe en Hive correctamente, incluyendo eventos GPS y meteorologia.

## Script de validacion

Ruta:

- `scripts/validate_hive_pipeline.sh`

El script hace lo siguiente:

1. Levanta el stack Docker.
2. Carga datos semilla en HDFS.
3. Compila el `spark-app` (Java 21).
4. Ejecuta el job batch para crear/llenar tablas Hive.
5. Verifica tablas y conteos batch.
   - Incluye tablas maestras Hive usadas para enriquecimiento:
     - `transport_analytics.master_warehouses`
     - `transport_analytics.master_vehicles`
6. Crea topic Kafka `transport.filtered` (si no existe).
7. Crea topic Kafka `transport.weather.filtered` (si no existe).
8. Limpia tablas/checkpoints de streaming para evitar estado residual entre ejecuciones.
9. Ejecuta streaming durante 240s (`STREAMING_STARTING_OFFSETS=earliest`).
10. Inyecta eventos GPS y meteorologicos de prueba en Kafka en dos tandas con timestamps actuales.
11. Verifica la salida streaming GPS en `transport_analytics.delay_metrics_streaming` y la salida meteo operativa en `transport_analytics.v_weather_observations_madrid`.

## Uso

Desde la raiz del proyecto:

```bash
./scripts/validate_hive_pipeline.sh
```

Arranque/parada recomendados del entorno:

```bash
./scripts/start_kdd.sh
./scripts/stop_kdd.sh
```

## Criterio de exito

La validacion es correcta cuando:

- En Hive existen las tablas:
  - `transport_analytics.enriched_events`
  - `transport_analytics.delay_metrics_batch`
  - `transport_analytics.route_graph_metrics`
  - `transport_analytics.route_shortest_paths`
  - `transport_analytics.ml_delay_risk_scores`
  - `transport_analytics.master_warehouses`
  - `transport_analytics.master_vehicles`
  - `transport_analytics.delay_metrics_streaming`
- El resultado de:
  - `SELECT COUNT(*) FROM transport_analytics.delay_metrics_streaming;`
  devuelve un valor mayor que `0`.
- El resultado de:
  - `SELECT COUNT(*) FROM transport_analytics.v_weather_observations_madrid;`
  devuelve un valor mayor que `0`.

Nota:

- En algunos entornos de validacion puede existir tambien `transport_analytics.weather_observations_streaming`, pero en la operacion actual de entrega la referencia de consulta recomendada es la vista `v_weather_observations_madrid`.

## Nota operativa

El script usa `sg docker -c` para ejecutar comandos Docker con el grupo `docker`.

## Zona horaria

- El job Spark usa por defecto `SPARK_SQL_TIMEZONE=Europe/Madrid`.
- Puedes sobreescribirlo si necesitas otro huso horario exportando la variable en `docker compose exec`.

## Consultas en hora Madrid

Si quieres evitar conversiones manuales de zona horaria, usa:

- `transport_analytics.v_weather_observations_madrid`
- `transport_analytics.v_delay_metrics_streaming_madrid`

## Compatibilidad de objetos streaming

Si tras una limpieza aparecen errores `TABLE_OR_VIEW_NOT_FOUND` al consultar vistas Madrid, ejecutar:

```bash
./scripts/ensure_hive_streaming_compat.sh
```

Este script recrea (si faltan) las tablas streaming base y recompone las vistas Madrid para evitar roturas operativas.
