#!/usr/bin/env bash
set -euo pipefail

# Script principal de arranque del proyecto.
# Responsabilidades:
# - Levantar contenedores base.
# - Esperar disponibilidad de servicios criticos.
# - Ejecutar bootstrap de NiFi.
# - Mostrar estado final y dejar entorno listo para demo.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

wait_for_http() {
  local label="$1"
  local url="$2"
  local expected_codes="${3:-200}"
  local max_tries="${4:-60}"
  local sleep_seconds="${5:-3}"
  local code
  local i

  echo "Esperando ${label} en ${url} ..."
  for i in $(seq 1 "${max_tries}"); do
    code="$(curl -k -s -o /dev/null -w '%{http_code}' "${url}" || true)"
    for ok in ${expected_codes}; do
      if [ "${code}" = "${ok}" ]; then
        echo "${label} disponible (HTTP ${code}) tras $(( i * sleep_seconds ))s."
        return 0
      fi
    done
    echo "[${label}] intento ${i}/${max_tries} | ${url} | HTTP ${code:-000} | elapsed $(( i * sleep_seconds ))s"
    sleep "${sleep_seconds}"
  done
  echo "Timeout esperando ${label} (${url})."
  return 1
}

echo "Arrancando stack completo del proyecto KDD..."
sg docker -c "docker compose up -d --build"

echo
wait_for_http "NiFi UI" "https://localhost:8443/nifi/" "200" 60 3 || true

wait_for_http "API de NiFi" "https://localhost:8443/nifi-api/access/config" "200 401" 80 3 || true

echo
echo "Bootstrap del flujo de NiFi..."
export NIFI_PG_NAME="${NIFI_PG_NAME:-kdd_ingestion_auto_v9}"
BOOTSTRAP_OK=0
for i in $(seq 1 8); do
  if ./scripts/bootstrap_nifi_flow.sh >/tmp/bootstrap_nifi_flow.log 2>&1; then
    BOOTSTRAP_OK=1
    echo "Flujo de NiFi desplegado correctamente."
    break
  fi
  echo "Intento ${i}/8 fallido, reintentando en 15s..."
  tail -n 2 /tmp/bootstrap_nifi_flow.log || true
  sleep 15
done
if [ "${BOOTSTRAP_OK}" -ne 1 ]; then
  echo "No se pudo bootstrappear NiFi automaticamente. Ultimas lineas:"
  tail -n 20 /tmp/bootstrap_nifi_flow.log || true
fi

echo
echo "Asegurando compatibilidad de objetos Hive para vistas streaming..."
if ! sg docker -c "./scripts/ensure_hive_streaming_compat.sh" >/tmp/ensure_hive_streaming_compat.log 2>&1; then
  echo "[WARN] No se pudo asegurar compatibilidad Hive automaticamente."
  tail -n 20 /tmp/ensure_hive_streaming_compat.log || true
else
  echo "Compatibilidad Hive asegurada."
fi

echo
echo "Sanity check Hive streaming (tablas + vistas Madrid)..."
if ! sg docker -c "docker compose exec -T spark-client spark-sql -e \"SHOW TABLES IN transport_analytics LIKE 'delay_metrics_streaming'; SHOW TABLES IN transport_analytics LIKE 'weather_observations_streaming'; SHOW TABLES IN transport_analytics LIKE 'v_delay_metrics_streaming_madrid'; SHOW TABLES IN transport_analytics LIKE 'v_weather_observations_madrid'; SELECT COUNT(*) AS delay_rows FROM transport_analytics.v_delay_metrics_streaming_madrid; SELECT COUNT(*) AS weather_rows FROM transport_analytics.v_weather_observations_madrid;\"" >/tmp/start_kdd_hive_sanity.log 2>&1; then
  echo "[WARN] Fallo en sanity check Hive streaming."
  tail -n 30 /tmp/start_kdd_hive_sanity.log || true
else
  echo "Sanity check Hive streaming OK."
fi

echo
echo "Comprobando si Hive meteo requiere repoblado..."
WEATHER_ROWS="$(sg docker -c "docker compose exec -T spark-client spark-sql -e \"SELECT COUNT(*) AS weather_rows FROM transport_analytics.v_weather_observations_madrid;\"" 2>/tmp/start_kdd_weather_count.err | awk '/^[0-9]+$/ {print; exit}' || true)"
if [ "${WEATHER_ROWS:-0}" = "0" ]; then
  echo "Vista meteo vacia (weather_rows=0). Ejecutando repoblado desde Cassandra..."
  if ! sg docker -c "./scripts/repopulate_hive_weather_from_cassandra.sh" >/tmp/repopulate_hive_weather.log 2>&1; then
    echo "[WARN] No se pudo repoblar Hive meteo automaticamente."
    tail -n 30 /tmp/repopulate_hive_weather.log || true
  else
    echo "Repoblado Hive meteo completado."
  fi
else
  echo "Hive meteo ya tiene datos (weather_rows=${WEATHER_ROWS})."
fi

echo
echo "Esperando dashboard en http://localhost:8501/health ..."
for _ in $(seq 1 40); do
  if curl -fsS "http://localhost:8501/health" >/dev/null 2>&1; then
    echo "Dashboard disponible."
    break
  fi
  sleep 2
done

echo
echo "Estado de servicios:"
sg docker -c "docker compose ps"

echo
echo "URLs:"
echo "- Dashboard: http://localhost:8501"
echo "- NiFi: http://localhost:8443/nifi"
echo "- Airflow: http://localhost:8080"
echo "- Hadoop: http://localhost:9870"
echo
echo "Stack KDD iniciado."
