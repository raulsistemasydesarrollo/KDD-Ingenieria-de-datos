# Dashboard Logistico

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Especificacion funcional del dashboard`
- Version: `v1.1`
- Fecha: `31/03/2026`

## Indice

1. Objetivo
2. Fuentes de datos del dashboard
3. Que representa cada elemento visual
4. Filtros de Tiempo Real (solo vista izquierda)
5. Filtros de Analisis Logistico (solo vista derecha)
6. Reglas de interpretacion de ruta por vehiculo
7. ETA en panel de vehiculo
8. Datos y nodos actuales
9. Reinicio limpio de demo
10. Nota de uso

## Objetivo

El dashboard muestra dos vistas operativas **desacopladas** (independientes), mas el bloque de metricas globales:

- Operacion en tiempo real de vehiculos (mapa y paneles de flota).
- Analisis de red logistica (mapa de red y tablas de rutas/impacto).

## Captura actual

![Dashboard logistica (21 vehiculos activos)](./dashboard.png)

## Fuentes de datos del dashboard

- Vehiculos en tiempo real: fuente primaria `Cassandra` (`transport.vehicle_latest_state`).
- Fallback de vehiculos: ficheros `nifi/input/gps_*.jsonl` si Cassandra no esta disponible.
- Plan de ruta por vehiculo (origen/destino planificados): `nifi/input/.vehicle_path_state.json`.
- Clima reciente: fuente primaria `Cassandra` (`transport.weather_observations_recent`), cargada por Spark Streaming.
- Fallback de clima: `nifi/raw-archive/weather` (snapshots Open-Meteo) si Cassandra no esta disponible.
- Red logistica: `data/graph/vertices.csv`, `data/graph/edges.csv`, `data/master/warehouses.csv`.

Endpoint de diagnostico de fuentes:

```bash
curl -s http://localhost:8501/api/debug/sources
```

Devuelve:

- fuente activa para vehiculos (`cassandra` o `nifi_files`),
- estado de conexion a Cassandra y numero de filas obtenidas,
- ultimo fichero GPS detectado en fallback,
- fuente activa de clima (`cassandra` o `nifi_raw_archive`) y estado Cassandra de clima,
- estado/ficheros recientes de clima en fallback,
- conteo de vertices/aristas del grafo.

## Que representa cada elemento visual

### Mapa en tiempo real

- Marcadores de vehiculo: posicion actual por `vehicle_id`.
- Lineas naranjas: traza reciente del movimiento del vehiculo (historial corto), **no** son carreteras fijas.
- Linea discontinua amarilla: proyeccion del tramo del vehiculo seleccionado hacia su siguiente nodo estimado.
- Marcadores de almacen:
  - naranja: criticidad `high`
  - azul: criticidad `medium`
  - etiqueta semitransparente para no ocultar vehiculos.
- Etiquetas de nodo mostradas en codigo de 3 letras (ej. `MAD`, `BCN`, `GIJ`), alineadas con la vista de red.

### Filtros de Tiempo Real (solo vista izquierda)

- `Origen RT` y `Destino RT` filtran **solo** la vista de Operacion en Tiempo Real.
- Listado de `Origen RT` / `Destino RT` ordenado alfabeticamente.
- `TODOS -> TODOS`: sin filtro (vista global de flota).
- `TODOS -> X`: muestra vehiculos cuya ruta reportada termina en `X`.
- `X -> TODOS`: muestra vehiculos cuya ruta reportada comienza en `X`.
- `X -> Y`: restringe al corredor X-Y con criterio geografico y de ruta.
- Si el vehiculo seleccionado deja de cumplir el filtro, se limpia automaticamente la seleccion.

### Mapa de red logistica

- Nodos: almacenes con coordenadas reales (`data/master/warehouses.csv`).
- Aristas: conexiones del grafo (`data/graph/edges.csv`).
- Arista roja: tramo perteneciente a la ruta calculada para el origen/destino seleccionados.
- Color de arista base: severidad meteorologica agregada (low/medium/high).
- Selectores `Origen` / `Destino` ordenados alfabeticamente.
- Delay de arista en tabla/mapa: valor **efectivo** (`effective_avg_delay_minutes`) que mezcla:
  - delay estatico del grafo (`avg_delay_minutes`),
  - telemetria viva de flota (`planned_origin/planned_destination`, delay y velocidad recientes).
- Cuando hay telemetria live en un tramo, se muestra etiqueta `live` en la tabla de rutas.

### Tabla de ruta (debajo del mapa logistico)

Muestra:

- Perfil de optimizacion.
- Camino calculado.
- Distancia total.
- Tiempo base sin meteo.
- Penalizacion meteo (en minutos).
- Delay esperado.
- Tiempo estimado final.
- Factor meteorologico aplicado.
- Numero de aristas de la ruta con telemetria live usada en el calculo.
- Nota: el contador `Aristas live` refleja solo tramos de la **ruta seleccionada** con muestras live (`live_sample_count > 0`), no el total del grafo.
- Todas las tablas del dashboard son ordenables por columna (asc/desc) con click en cabecera.

### Insights de red (live)

Bloque adicional en la vista de red con dos tablas:

- `Cuellos de botella`: ranking de tramos por `impact_score` (delay efectivo, congestion, distancia y muestras live).
- `Nodos criticos`: ranking de almacenes por `criticality_score` (grado, delay incidente medio, ratio de congestion alta y cobertura live).

Filtros disponibles:

- `Perfil insights`: recalcula score de impacto con los costes del perfil (`balanced`, `fastest`, `resilient`).
- `Congestion minima`: filtra el ranking para mostrar solo tramos/nodos afectados por el nivel indicado (`all`, `low`, `medium`, `high`).
- `Historico insights`: tabla de snapshots recientes persistidos en Cassandra por perfil/congestion.
- Si un tramo llega sin muestras live (`live_sample_count=0`) y congestion `UNKNOWN`, se normaliza a `low/medium/high` con heuristica de delay/distancia para mantener consistencia visual.

Endpoint historico:

```bash
curl -s "http://localhost:8501/api/network/insights/history?insights_profile=balanced&insights_min_congestion=all&snapshots=12"
```

## Consolidacion en Hive (reporting)

Se incorpora modo Spark `insights-sync` para consolidar snapshots de Cassandra en Hive:

```bash
sg docker -c "docker compose exec -T spark-client /opt/spark-app/run-insights-sync.sh"
```

Tablas resultado:

- `transport_analytics.network_insights_snapshots_hive` (detalle de snapshots).
- `transport_analytics.network_insights_hourly_trends` (top tramo y top nodo por hora/perfil/congestion).

## Grafo por cercania geografica (k vecinos)

Para regenerar `data/graph/edges.csv` con una red "casi completa" por cercania:

```bash
python3 scripts/rebuild_graph_edges_by_proximity.py --k 4
```

Alternativa con variable de entorno:

```bash
GRAPH_K_NEIGHBORS=5 python3 scripts/rebuild_graph_edges_by_proximity.py
```

Reglas de negocio que aplica el script al regenerar aristas:

- Conexiones directas prohibidas: `BCN-MUR`, `ACO-BIO`, `VAL-ALM`.
- Conexiones obligatorias: `BCN-VAL`, `VAL-MUR`, `ACO-GIJ`, `GIJ-BIO`, `MAD-SEV`, `MAD-MAL`.
- Corredor sur sin atajos directos: `CAC-SEV-MAL-ALM-MUR`.

### Filtros de Analisis Logistico (solo vista derecha)

- `Origen` y `Destino` afectan **solo** al bloque de Analisis de Red Logistica.
- `TODOS` en alguno de los selectores pone el bloque en modo vista global (sin ruta concreta).
- No se permite ruta concreta con mismo origen y destino.
- `Calcular mejor ruta` aplica el perfil seleccionado (`balanced`, `fastest`, `resilient`) solo al bloque logistico.

## Reglas de interpretacion de ruta por vehiculo

El dashboard prioriza para cada vehiculo:

1. `planned_origin/planned_destination` (estado del generador en `.vehicle_path_state.json`).
2. Si no existe plan, inferencia por posicion y heading.

Esto evita incoherencias visuales (ejemplo: rutas tipo `X -> X` o proyecciones a nodos no esperados).

## ETA en panel de vehiculo

La ETA (`Hora estimada llegada`) se calcula en frontend por vehiculo como:

1. distancia al siguiente nodo estimado,
2. velocidad actual (`speed_kmh`),
3. buffer parcial de delay (`delay_minutes`).

Para estabilidad visual existe suavizado temporal, pero con resincronizacion forzada en estos casos:

- cambio de destino estimado (`destinationId`),
- divergencia grande entre ETA cacheado y ETA fisico (`distance/speed + delay`).

Con esto se evita que persistan ETAs irreales tras saltos de posicion, cambios de ruta o refrescos incompletos.

## Datos y nodos actuales

Red actual (15 nodos): `ACO, ALM, BCN, BIO, CAC, GIJ, LIS, MAD, MAL, MUR, OPO, SEV, VAL, VLL, ZAR`.

Flota actual (15 vehiculos):

- Activos: `14`
- Mantenimiento: `1` (`TRUCK-004`)

Archivos fuente:

- `data/master/warehouses.csv`
- `data/graph/vertices.csv`
- `data/graph/edges.csv`
- `scripts/gps_generator.py`

## Reinicio limpio de demo

Para empezar de cero y evitar sesgo por historico antiguo:

```bash
./scripts/reset_demo_data.sh
```

Incluye:

- limpieza de eventos locales en `nifi/input` y raw weather/failures,
- reset de estado streaming (Hive + checkpoints curated),
- recreacion de `gps-generator` y `raw-hdfs-loader`.

Modo agresivo (tambien limpia raw HDFS `/data/raw/nifi`):

```bash
./scripts/reset_demo_data.sh --hard
```

## Nota de uso

Tras cambios de frontend, hacer recarga fuerte (`Ctrl+F5`) en `http://localhost:8501`.
