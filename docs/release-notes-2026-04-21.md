# Release Notes - 21/04/2026

## Resumen

Actualizacion de dashboard, backend operativo y documentacion para reflejar:

1. Nueva cabecera de sensores de plataforma.
2. Limpieza automatica por umbral de uso de disco.
3. Mejora de explicabilidad en cobertura live de aristas.
4. Ajustes de layout/UX para reducir ancho horizontal.

## Cambios funcionales

### Dashboard (UI)

- Tarjetas KPI compactadas en una sola fila horizontal.
- Tarjeta `Vehiculos activos` + `Eventos cargados` unificada como `Vehiculos / eventos`.
- `Spark` y `YARN nodo` movidos a primera fila de chips junto a fuentes.
- Enlace `DAG limpieza` movido a primera fila.
- Enlace `DAG limpieza` sincroniza color con estado `HDFS disco` (`ok`, `warn`, `bad`).
- Ajuste de texto en recomendacion IA para evitar saltos de linea innecesarios.

### Backend dashboard

- Nuevo endpoint:
  - `POST /api/platform/cleanup/trigger`
- Ejecucion asincrona de `scripts/safe_disk_cleanup.sh`.
- Protecciones operativas:
  - lock de concurrencia,
  - cooldown configurable (`DISK_CLEANUP_TRIGGER_COOLDOWN_SECONDS`),
  - invalidacion de cache de `platform_status` tras limpieza.

### Politica de limpieza por disco

- Auto-trigger desde frontend cuando:
  - `HDFS disco >= DISK_CLEANUP_USAGE_THRESHOLD` (default `88`).
- Antirebote en frontend para evitar triggers repetidos en el mismo ciclo de sobre-umbral.

### Reentreno IA (explicabilidad)

- Motivos de cobertura live ampliados con:
  - cobertura real de aristas con muestra live,
  - cobertura esperada por vehiculos activos.
- Nueva metrica en payload de recomendacion:
  - `metrics.live_coverage_expected_ratio`.

## Archivos principales impactados

- `dashboard/static/app.js`
- `dashboard/static/styles.css`
- `dashboard/server.py`
- `docs/dashboard.md`
- `docs/manual-usuario.md`
- `docs/operations.md`
- `docs/memoria-tecnica-sistema.md`
- `docs/CODE-ANNOTATION-GUIDE.md`
- `README.md`
- `CHANGELOG.md`

## Variables de entorno relevantes

- `DISK_CLEANUP_USAGE_THRESHOLD` (default `88`)
- `DISK_CLEANUP_TIMEOUT_SECONDS`
- `DISK_CLEANUP_TRIGGER_COOLDOWN_SECONDS`

