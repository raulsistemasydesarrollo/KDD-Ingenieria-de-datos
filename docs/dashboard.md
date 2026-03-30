# Dashboard Logistico

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Especificacion funcional del dashboard`
- Version: `v1.0-entrega`
- Fecha: `30/03/2026`

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

### Filtros de Tiempo Real (solo vista izquierda)

- `Origen RT` y `Destino RT` filtran **solo** la vista de Operacion en Tiempo Real.
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

Red actual: `MAD, BCN, VAL, SEV, LIS, OPO, ZAR, BIO, ACO, ALM, VLL`.

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
