# Release Notes - 04/04/2026

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Release notes iteracion 04/04/2026`
- Version: `v1.4-entrega`
- Fecha: `04/04/2026`
- Repositorio GitHub: `https://github.com/raulsistemasydesarrollo/KDD-Ingenieria-de-datos`

## Resumen ejecutivo

Esta iteracion consolida el estado operativo final del stack con foco en:

1. Orquestacion mensual robusta en Airflow sin solapamientos.
2. Operativa de reentreno IA sincronizada entre DAG y dashboard.
3. Documentacion integral actualizada a fecha `04/04/2026`.
4. Estrategia segura para preview de ficheros en HDFS desde navegador.

## Cambios funcionales

### Airflow + DAG mensual

- `logistics_kdd_monthly_maintenance` endurecido con `max_active_runs=1`.
- Reentreno mensual encapsulado en `scripts/airflow_retrain_with_status.sh`.
- Marcador runtime compartido para exponer estado de reentreno durante ejecucion.

### Dashboard (reentreno IA)

- Bloque anti-colision de triggers:
  - si hay reentreno activo (incluido origen Airflow), `POST /api/ml/retrain` devuelve `409`.
- Cabecera IA ampliada:
  - `Ultimo reentreno`
  - `Siguiente programado`
  - mostrado en horario `Europe/Madrid`.
- `GET /api/ml/retrain/status` ampliado con `schedule_info`.

### HDFS UI (preview de fichero)

- Se adopta como ruta recomendada un ajuste de host local:
  - script `scripts/toggle_hdfs_browser_preview_hosts.sh`
  - objetivo: resolver `hadoop` en navegador para redirects WebHDFS.
- Se documenta explicitamente la alternativa temporal en `hdfs-site.xml` y su riesgo operativo.

## Cambios de documentacion

- Portadas/versiones/fechas alineadas a `04/04/2026` en docs principales.
- Capturas operativas actualizadas:
  - `docs/airflow.png`
  - `docs/dashboard.png`
- Documentos sincronizados:
  - `README.md`
  - `docs/ENTREGA.md`
  - `docs/memoria-tecnica-sistema.md`
  - `docs/resumen-ejecutivo-memoria.md`
  - `docs/dashboard.md`
  - `docs/operations.md`
  - `docs/manual-usuario.md`
  - `docs/architecture.md`
  - `docs/nifi-flow.md`
  - `docs/nifi-weather-template.md`
  - `docs/CODE-ANNOTATION-GUIDE.md`

## Implicaciones operativas (HDFS preview)

Camino recomendado:

```bash
./scripts/toggle_hdfs_browser_preview_hosts.sh status
sudo ./scripts/toggle_hdfs_browser_preview_hosts.sh enable
```

Reversion:

```bash
sudo ./scripts/toggle_hdfs_browser_preview_hosts.sh disable
```

Riesgo de la alternativa temporal sobre `hdfs-site.xml`:

- Aunque puede facilitar preview web, puede romper escrituras HDFS desde Spark/Airflow en contenedores:
  - `Connection refused`
  - `could only be written to 0 of the 1 minReplication nodes`

## Referencias rapidas

- Guia operativa: `docs/operations.md`
- Manual de usuario: `docs/manual-usuario.md`
- Dashboard: `docs/dashboard.md`
- Indice de entrega: `docs/ENTREGA.md`
