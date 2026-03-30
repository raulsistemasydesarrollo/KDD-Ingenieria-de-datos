# Paquete Final de Entrega

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Indice maestro de entrega`
- Version: `v1.0-entrega`
- Fecha: `30/03/2026`

## 1. Documentos principales

1. Memoria tecnica completa:
   - [docs/memoria-tecnica-sistema.md](./memoria-tecnica-sistema.md)
2. Resumen ejecutivo (1-2 paginas):
   - [docs/resumen-ejecutivo-memoria.md](./resumen-ejecutivo-memoria.md)
3. Guia funcional del dashboard:
   - [docs/dashboard.md](./dashboard.md)
4. Guia de operaciones y troubleshooting:
   - [docs/operations.md](./operations.md)
5. Release notes de la iteracion:
   - [docs/release-notes-2026-03-30.md](./release-notes-2026-03-30.md)
6. Guia general del repositorio:
   - [README.md](../README.md)
7. Arquitectura tecnica + diagrama:
   - [docs/architecture.md](./architecture.md)
   - [docs/architecture-diagram.svg](./architecture-diagram.svg)
8. Guia de anotacion de codigo:
   - [docs/CODE-ANNOTATION-GUIDE.md](./CODE-ANNOTATION-GUIDE.md)
9. Changelog consolidado:
   - [CHANGELOG.md](../CHANGELOG.md)

## 2. Alcance funcional entregado

1. Plataforma KDD end-to-end con NiFi, Kafka, Spark, Hive, Cassandra, Airflow y dashboard web.
2. Ingesta separada por dominios (`gps_ingestion` y `weather_ingestion`) en NiFi.
3. Dashboard con dos vistas desacopladas:
   - Tiempo Real con filtros propios (`Origen RT`, `Destino RT`).
   - Red Logistica con filtros propios (`Origen`, `Destino`, `Perfil`).
4. Rutas y proyecciones de vehiculo coherentes con plan de trayecto (`planned_origin`, `planned_destination`).
5. Estado operativo con fuentes primarias en Cassandra y fallback controlado.

## 3. Checklist de revision previa a entrega

### 3.1 Infraestructura

- [ ] `docker compose ps` sin servicios core caidos.
- [ ] NiFi accesible en `https://localhost:8443/nifi`.
- [ ] Airflow accesible en `http://localhost:8080`.
- [ ] Dashboard accesible en `http://localhost:8501`.

### 3.2 Datos y pipeline

- [ ] Topic Kafka GPS/Weather creados y activos.
- [ ] Raw en HDFS presente (`/data/raw/nifi/...`).
- [ ] Tablas Hive principales visibles en `transport_analytics`.
- [ ] Cassandra con estado de flota y observaciones meteo recientes.

### 3.3 Dashboard

- [ ] Modo oscuro por defecto y toggle claro/oscuro operativo.
- [ ] Filtros de Tiempo Real afectan solo vista izquierda.
- [ ] Filtros de Red Logistica afectan solo vista derecha.
- [ ] `TODOS -> TODOS` funciona en ambos contextos.
- [ ] No aparecen rutas espurias tipo `X -> X` en vehiculos.
- [ ] Panel de vehiculo muestra ruta, rumbo, siguiente nodo y ETA consistentes.

### 3.4 Documentacion

- [ ] Memoria tecnica y resumen ejecutivo actualizados.
- [ ] Dashboard/Operations/Release notes alineados con implementacion final.
- [ ] README consistente con comportamiento actual del sistema.

## 4. Comandos de validacion rapida

```bash
./scripts/start_kdd.sh
docker compose ps
curl -s http://localhost:8501/health
curl -s http://localhost:8501/api/debug/sources
./scripts/validate_hive_pipeline.sh
```

Nota:

- `start_kdd.sh` ya ejecuta validacion de compatibilidad Hive streaming y sanity check de vistas Madrid.

## 5. Nota final

Si se realizan cambios de frontend antes de defensa/demo, usar recarga fuerte del navegador:

```text
Ctrl+F5
```
