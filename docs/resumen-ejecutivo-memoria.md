# Resumen Ejecutivo - Sistema KDD Logistico

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Resumen ejecutivo para memoria`
- Version: `v1.0-entrega`
- Fecha: `30/03/2026`

## Indice

1. Que se ha construido
2. Arquitectura y flujo de valor
3. Resultados funcionales alcanzados
4. Robustez y operaciones
5. Estado tecnico final (entrega)
6. Entregables documentales
7. Conclusiones

Este resumen sintetiza la solucion implementada para la entrega del proyecto, con foco en arquitectura, valor funcional, robustez operativa y estado final.

## 1. Que se ha construido

Se ha desarrollado una plataforma Big Data end-to-end para analisis logistico que integra:

1. Ingesta de eventos GPS y meteorologia en tiempo casi real.
2. Procesamiento batch y streaming sobre Spark.
3. Analitica de red logistica con grafos y rutas optimizadas.
4. Scoring de riesgo de retraso mediante ML.
5. Persistencia en HDFS/Hive y estado operativo en Cassandra.
6. Orquestacion y monitorizacion con Airflow.
7. Dashboard web operativo para explotacion funcional.

El sistema esta empaquetado en Docker Compose, con scripts de arranque, validacion y limpieza para reproducibilidad local.

## 2. Arquitectura y flujo de valor

### Ingesta

- NiFi organiza la ingesta en dos subflujos claramente separados:
  - `gps_ingestion`
  - `weather_ingestion`
- Publica en Kafka cuatro topics principales:
  - `transport.raw`
  - `transport.filtered`
  - `transport.weather.raw`
  - `transport.weather.filtered`
- Conserva raw local y un proceso dedicado (`raw-hdfs-loader`) lo sincroniza a HDFS para trazabilidad y reprocesado.

### Procesamiento

- Spark batch construye tablas historicas y analiticas:
  - enriquecimiento de eventos,
  - agregados de delay/velocidad,
  - metricas de grafo (`connected components`, `pagerank`),
  - shortest paths,
  - scoring ML de riesgo.
- Spark streaming procesa GPS y clima filtrado desde Kafka, con watermarks y deduplicacion, y persiste resultados en Hive.
- Spark streaming persiste ademas eventos enriquecidos en tiempo real en `transport_analytics.enriched_events_streaming`.

### Almacenamiento y consulta

- Hive centraliza el catalogo analitico (batch y streaming).
- Cassandra mantiene el ultimo estado de cada vehiculo para consulta rapida.
- HDFS mantiene capa raw y curated.

### Explotacion

- Dashboard con:
  - mapa operativo de flota en tiempo real,
  - analisis de red logistica y mejor ruta por perfil,
  - impacto meteorologico en ETA y penalizacion de ruta.
- El dashboard usa Cassandra como fuente principal de flota y fallback a archivos NiFi si Cassandra no esta disponible.
- El dashboard usa plan de trayecto por vehiculo (`planned_origin`, `planned_destination`) para reforzar coherencia de rutas visualizadas.
- Los filtros de Tiempo Real y de Red Logistica estan desacoplados para evitar interferencias entre vistas.

## 3. Resultados funcionales alcanzados

1. Visualizacion de vehiculos con trazas y foco por seleccion.
2. Calculo de ETA por vehiculo con suavizado para evitar fluctuaciones bruscas.
3. Calculo de mejor ruta por perfiles (`balanced`, `fastest`, `resilient`).
4. Incorporacion real del factor meteorologico en estimaciones de tiempo.
5. Representacion tabular de rutas y metricas (distancia, tiempos, penalizacion clima, factor aplicado).
6. Modo oscuro por defecto y cambio claro/oscuro en frontend.
7. Filtros RT parciales y completos (`TODOS->X`, `X->TODOS`, `X->Y`) con limpieza de seleccion fuera de filtro.

## 4. Robustez y operaciones

Se incorporaron mejoras de estabilidad relevantes para entrega:

1. Healthchecks Airflow (diario y horario) con degradacion segura en consultas smoke, evitando falsos fallos cuando las tablas fuente streaming no estan disponibles temporalmente.
2. Limpieza controlada de historico fallido en Airflow para dejar estado operativo en verde.
3. Reorganizacion visual de NiFi por dominios (GPS/Clima), simplificando soporte y revisiones.
4. Script de limpieza de Process Groups legacy de NiFi para mantener entorno limpio.
5. Fallback Parquet en Spark streaming si falla escritura a Hive, evitando parada del pipeline.

## 5. Estado tecnico final (entrega)

- NiFi:
  - Process Group activo: `kdd_ingestion_auto_v9`
  - Subgrupos: `gps_ingestion`, `weather_ingestion`
- Airflow:
  - DAGs de healthcheck estabilizados y sin failed pendientes en la validacion operativa.
- Dashboard:
  - Endpoint de diagnostico de fuentes: `/api/debug/sources`
  - Fuente vehiculos: Cassandra (primaria), `nifi/input` (fallback).

## 6. Entregables documentales

Se han dejado documentos para operacion y defensa:

1. Memoria tecnica completa:
   - `docs/memoria-tecnica-sistema.md`
2. Operaciones y checklist:
   - `docs/operations.md`
3. Dashboard y leyenda funcional:
   - `docs/dashboard.md`
4. Release notes de iteracion:
   - `docs/release-notes-2026-03-30.md`

## 7. Conclusiones

La solucion cumple los objetivos del proyecto KDD en un entorno reproducible y demostrable:

- separa correctamente ingesta, transformacion, analitica y visualizacion,
- combina batch + streaming con gobernanza de datos,
- incorpora analitica avanzada (grafos y ML),
- ofrece mecanismos de observabilidad, resiliencia y mantenimiento.

Como resultado, se dispone de un sistema funcional para analisis logistico en tiempo real y soporte de decisiones operativas.
