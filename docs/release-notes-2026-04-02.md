# Release Notes - 02/04/2026

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Release notes iteracion 02/04/2026`
- Version: `v1.2-entrega`
- Fecha: `02/04/2026`

## Indice

1. Resumen ejecutivo
2. Dashboard web
3. Datos de red y simulacion
4. Airflow
5. NiFi
6. Scripts nuevos/actualizados
7. Verificaciones recomendadas
8. Impacto operativo

Este documento consolida los cambios funcionales y operativos aplicados hasta la iteracion del 2 de abril de 2026.

## Addendum - 02/04/2026

Cambios adicionales aplicados en esta iteracion de cierre documental:

### Documentacion unificada

- Actualizacion de portada, version y fecha en toda la documentacion principal:
  - `docs/ENTREGA.md`
  - `docs/memoria-tecnica-sistema.md`
  - `docs/resumen-ejecutivo-memoria.md`
  - `docs/dashboard.md`
  - `docs/operations.md`
  - `docs/architecture.md`
  - `docs/nifi-flow.md`
  - `docs/nifi-weather-template.md`
  - `docs/CODE-ANNOTATION-GUIDE.md`
- Actualizacion de referencias internas a release notes de la iteracion vigente (`docs/release-notes-2026-04-02.md`).
- Sincronizacion de la portada para PDF profesional (`docs/cover-profesional.md`) con la fecha de entrega actual.

### Dashboard

- Sustitucion de la captura de referencia por la imagen actualizada:
  - `docs/dashboard.png` (actualizada el 02/04/2026).
- Leyenda de captura actualizada en:
  - `docs/dashboard.md`
  - `docs/memoria-tecnica-sistema.md`
- Documentacion de funcionalidades nuevas ya activas en codigo:
  - perfiles de ruta adicionales `eco`, `low_risk`, `reliable`,
  - patron horario (`auto`, `peak`, `offpeak`, `night`),
  - pesos multiobjetivo `time/risk/eco`,
  - exclusion de nodos (`avoid_nodes`),
  - rutas candidatas con ranking y deltas.
- Operativa de reentreno IA incorporada en dashboard:
  - boton `Reentrenar IA`,
  - endpoint `POST /api/ml/retrain`,
  - endpoint `GET /api/ml/retrain/status`,
  - recomendacion con score de deriva e histeresis/cooldown.

### Entregables PDF

- Actualizacion del bundle PDF unificado para consumir release notes de la iteracion actual:
  - `scripts/build_delivery_pdf.py`
- Actualizacion del builder profesional para generar salida fechada dinamicamente:
  - `scripts/build_delivery_pdf_professional.sh`
- Regeneracion de PDFs de entrega con fecha `2026-04-02`.

## Addendum - 31/03/2026

Cambios adicionales aplicados tras la iteracion base:

### Dashboard

- Etiquetas de nodos en Tiempo Real normalizadas a codigos de 3 letras (mismo criterio que Red Logistica).
- Etiquetas de nodo en Tiempo Real con transparencia para no ocultar vehiculos.
- Selectores de origen/destino (RT y red) ordenados alfabeticamente.
- Todas las tablas del dashboard ahora son ordenables por columnas.
- Tabla de datos bajo el mapa logistico ajustada para reflejar origen/destino seleccionados.
- Leyendas refinadas en mapas de Tiempo Real y Red Logistica.

### Insights de red

- Nuevo bloque `Insights de red (live)` con:
  - ranking de cuellos de botella,
  - ranking de nodos criticos,
  - historico de snapshots.
- Persistencia de snapshots en Cassandra (`transport.network_insights_snapshots`).
- Endpoint historico: `/api/network/insights/history`.
- Normalizacion de congestion `UNKNOWN` a niveles operativos (`low/medium/high`) cuando faltan muestras live.

### Spark / Hive

- Nuevo launcher `spark-app/run-insights-sync.sh` para ejecutar modo `insights-sync`.
- Consolidacion Cassandra -> Hive:
  - `transport_analytics.network_insights_snapshots_hive`
  - `transport_analytics.network_insights_hourly_trends`

### Red y simulacion

- Red actual consolidada en 15 nodos:
  - `ACO, ALM, BCN, BIO, CAC, GIJ, LIS, MAD, MAL, MUR, OPO, SEV, VAL, VLL, ZAR`
- Flota actual consolidada en 15 vehiculos (`14` activos + `1` mantenimiento).
- Regeneracion de aristas por proximidad con `scripts/rebuild_graph_edges_by_proximity.py`.
- Reglas de negocio en la topologia:
  - prohibidas: `BCN-MUR`, `ACO-BIO`, `VAL-ALM`,
  - obligatorias: `BCN-VAL`, `VAL-MUR`, `ACO-GIJ`, `GIJ-BIO`, `MAD-SEV`, `MAD-MAL`,
  - corredor sur sin atajos: `CAC-SEV-MAL-ALM-MUR`.

## Resumen ejecutivo

Se estabilizo la plataforma end-to-end para demo operativa en logistica:

- Dashboard mas legible y accionable (mapa tiempo real + analisis de red).
- Healthchecks de Airflow robustos y sin historico rojo pendiente.
- NiFi reestructurado visualmente por dominios (`GPS` y `Clima`).
- NiFi actualizado a `kdd_ingestion_auto_v9` con archivado GPS sin sobrescrituras.
- Scripts de arranque/limpieza alineados para reproducibilidad.
- Spark streaming ampliado con tabla de eventos enriquecidos en tiempo real (`transport_analytics.enriched_events_streaming`).
- Generador GPS actualizado a rutas realistas sobre grafo logistico y flota actual (`TRUCK-*`).
- Operativa de reentreno IA disponible desde dashboard (trigger + estado + recomendacion).

## Dashboard web

Cambios relevantes incorporados:

- Modo oscuro por defecto y soporte de cambio claro/oscuro.
- Mapa en tiempo real simplificado para reducir ruido visual.
- Seleccion de vehiculo con foco de ruta y panel de detalles.
- Ajustes de trazas para reducir "teletransportaciones" percibidas.
- Mejora de marcadores de vehiculo y orientacion en sentido de marcha.
- Inclusiones de ETA en panel de vehiculo.
- Correccion de ETA incoherente por caché/suavizado:
  - resincronizacion inmediata cuando cambia el destino estimado,
  - resincronizacion inmediata ante divergencia grande entre ETA cacheado y ETA fisico (`distancia/velocidad + delay`).
- Tabla de rutas en analisis logistico con tiempos en formato horas/minutos.
- Etiquetas de ciudad y mejoras de legibilidad en el mapa de red.
- Sustitucion de paneles/graficos redundantes por tablas de datos de rutas.
- Calculo de clima y factor meteorologico enlazado a ruta seleccionada.
- Desacople de filtros entre vistas:
  - filtros propios para Tiempo Real (`Origen RT`, `Destino RT`),
  - filtros propios para Red Logistica (`Origen`, `Destino`, `Perfil`).
- Modo `TODOS` soportado en ambos contextos de filtro.
- Regla de no permitir origen=destino en ruta concreta de Red Logistica.
- Actualizacion inmediata de la vista de Tiempo Real al cambiar sus filtros.
- Correcciones de coherencia de ruta por vehiculo:
  - eliminacion de casos `X -> X` no deseados,
  - eliminacion de proyecciones a nodos no esperados (ej. Lisboa cuando la ruta reportada indica otro destino).
- Enriquecimiento backend de `/api/vehicles/latest` y `/api/vehicles/history` con:
  - `planned_origin`,
  - `planned_destination`,
  tomados de `nifi/input/.vehicle_path_state.json`.
- Perfiles de optimizacion ampliados:
  - `balanced`, `fastest`, `resilient`, `eco`, `low_risk`, `reliable`.
- Nuevo control de patron horario en routing:
  - `auto`, `peak`, `offpeak`, `night`.
- Nuevo control de exclusion de nodos (`avoid_nodes`) en calculo de ruta.
- Optimizacion multiobjetivo configurable desde UI:
  - `objective_time`, `objective_risk`, `objective_eco`.
- Nuevo bloque de operacion ML:
  - `POST /api/ml/retrain`,
  - `GET /api/ml/retrain/status`,
  - panel de recomendacion de reentreno por score de deriva.

## Datos de red y simulacion

- Actualizacion de nodos/ciudades de la red.
- Inclusiones y ajustes de almacenes para mantener consistencia entre:
  - mapa en tiempo real,
  - mapa de red logistica,
  - calculo de rutas.

## Airflow

### DAGs impactados

- `kdd_hourly_healthcheck`
- `kdd_daily_healthcheck`

### Problema

Las tareas smoke contra vistas Madrid podian fallar cuando no existian temporalmente las tablas streaming fuente:

- `transport_analytics.weather_observations_streaming`
- `transport_analytics.delay_metrics_streaming`

Error observado: `TABLE_OR_VIEW_NOT_FOUND`.

### Solucion aplicada

Se ajustaron `smoke_query_weather` y `smoke_query_delay` para:

1. intentar consulta normal sobre vistas Madrid,
2. degradar a validacion segura (metastore/batch) si la fuente streaming no esta disponible.
3. mantener como referencia operativa meteo la vista `transport_analytics.v_weather_observations_madrid` (en lugar de depender de la tabla `weather_observations_streaming`).

Se incorpora ademas auto-recuperacion meteo en ambos DAGs (`kdd_hourly_healthcheck` y `kdd_daily_healthcheck`):

4. nueva tarea `repopulate_hive_weather_if_empty`,
5. si `weather_rows=0` en `v_weather_observations_madrid`, ejecuta `./scripts/repopulate_hive_weather_from_cassandra.sh`,
6. despues ejecuta el smoke query meteo para validar que la vista vuelve a ser consultable.

Resultado:

- ejecuciones manuales validadas en `success`,
- limpieza de runs `failed` historicos de healthchecks,
- sin rojos pendientes para esos DAGs.
- se incorpora `scripts/ensure_hive_streaming_compat.sh` para recrear tablas/vistas streaming de Hive tras limpiezas y evitar `TABLE_OR_VIEW_NOT_FOUND`.

## NiFi

### Bootstrap y visualizacion

Se mejoro el bootstrap para crear una estructura separada por dominio:

- PG principal: `kdd_ingestion_auto_v9`
- Subgrupos:
  - `gps_ingestion`
  - `weather_ingestion`

Objetivo:

- simplificar navegacion en UI,
- evitar canvas unico sobrecargado,
- facilitar diagnostico por flujo.

### Limpieza de legacy

Se elimino el Process Group legacy `kdd_ingestion_auto` tras:

- stop de componentes,
- vaciado de colas,
- deshabilitado de controller services,
- borrado final por API.

## Scripts nuevos/actualizados

### Nuevos

- `scripts/cleanup_nifi_legacy_pgs.py`
  - Limpia PGs legacy por prefijo.
  - Mantiene un PG objetivo (por defecto `kdd_ingestion_auto_v9`).
  - Gestiona colas, controller services y borrado con reintentos.
- `scripts/repopulate_hive_weather_from_cassandra.sh`
  - Exporta snapshots meteo desde Cassandra (`transport.weather_observations_recent`).
  - Repuebla `transport_analytics.weather_observations_streaming` en Hive.
  - Verifica recuento final de `transport_analytics.v_weather_observations_madrid`.

### Actualizados

- `scripts/bootstrap_nifi_flow.py`
  - crea subgrupos `gps_ingestion` y `weather_ingestion`,
  - incorpora renombrado unico por split para archivado GPS (`${filename}_${fragment.index}_${UUID()}.jsonl`),
  - mantiene wiring y controller service Kafka.
 
- `scripts/gps_generator.py`
  - genera rutas realistas desde el grafo de ciudades (ya no etiquetas aleatorias fijas),
  - carga flota desde `data/master/vehicles.csv`,
  - permite reasignacion dinamica de ruta al cerrar tramos.

- `spark-app/src/main/java/com/proyectobigdata/LogisticsAnalyticsJob.java`
  - parseo robusto de timestamps ISO UTC para GPS y clima,
  - nueva salida Hive `transport_analytics.enriched_events_streaming` en modo streaming.

- `scripts/bootstrap_nifi_flow.sh`
  - `NIFI_PG_NAME` por defecto: `kdd_ingestion_auto_v9`.

- `scripts/start_kdd.sh`
  - exporta `NIFI_PG_NAME` por defecto a `kdd_ingestion_auto_v9` antes de bootstrap.
  - si `v_weather_observations_madrid` queda vacia, dispara repoblado desde Cassandra.

## Verificaciones recomendadas

### Airflow

```bash
docker exec airflow-postgres psql -U airflow -d airflow -c "
SELECT dag_id, count(*) AS failed_runs
FROM dag_run
WHERE state='failed'
  AND dag_id IN ('kdd_daily_healthcheck','kdd_hourly_healthcheck')
GROUP BY dag_id;"
```

### NiFi

Comprobar en UI/API que en root solo queda el PG activo esperado y sus subgrupos:

- `kdd_ingestion_auto_v9`
- `gps_ingestion`
- `weather_ingestion`

## Impacto operativo

- Menos incidencias visuales y de UX en demo de dashboard.
- Menos falsos negativos en healthchecks por dependencia de estado streaming.
- Arranque mas predecible del flujo NiFi.
- Menor deuda operativa al eliminar estructura legacy.
