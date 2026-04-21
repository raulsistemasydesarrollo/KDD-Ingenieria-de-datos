#!/usr/bin/env python3
"""
Backend HTTP del dashboard logistico.

Resumen funcional:
1) Sirve frontend estatico (HTML/CSS/JS).
2) Expone API REST para flota, clima y red logistica.
3) Prioriza Cassandra como fuente de baja latencia con fallback a ficheros NiFi.
4) Calcula mejor ruta en backend (Dijkstra) con penalizacion meteorologica.
5) Enriquece respuesta de vehiculos con plan de trayecto del generador
   (planned_origin/planned_destination) para coherencia visual del mapa.

Endpoints principales:
- /api/overview
- /api/vehicles/latest
- /api/vehicles/history
- /api/weather/latest
- /api/network/graph
- /api/network/best-route
- /api/debug/sources
"""

import csv
import heapq
import json
import math
import os
import re
import subprocess
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

try:
    from cassandra.cluster import Cluster
except Exception:  # pragma: no cover - fallback en runtime sin dependencia instalada
    Cluster = None

HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
PORT = int(os.getenv("DASHBOARD_PORT", "8501"))

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
STATIC_DIR = PROJECT_ROOT / "dashboard" / "static"
GPS_DIR = PROJECT_ROOT / "nifi" / "input"
GPS_PATH_STATE_FILE = GPS_DIR / ".vehicle_path_state.json"
WEATHER_DIR = PROJECT_ROOT / "nifi" / "raw-archive" / "weather"
GRAPH_EDGES_PATH = PROJECT_ROOT / "data" / "graph" / "edges.csv"
GRAPH_VERTICES_PATH = PROJECT_ROOT / "data" / "graph" / "vertices.csv"
WAREHOUSES_PATH = PROJECT_ROOT / "data" / "master" / "warehouses.csv"
MASTER_VEHICLES_PATH = PROJECT_ROOT / "data" / "master" / "vehicles.csv"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "transport")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "vehicle_latest_state")
CASSANDRA_WEATHER_TABLE = os.getenv("CASSANDRA_WEATHER_TABLE", "weather_observations_recent")
CASSANDRA_INSIGHTS_TABLE = os.getenv("CASSANDRA_INSIGHTS_TABLE", "network_insights_snapshots")
VEHICLE_FRESHNESS_SECONDS = int(os.getenv("VEHICLE_FRESHNESS_SECONDS", "900"))
LIVE_EDGE_BLEND_MAX = float(os.getenv("LIVE_EDGE_BLEND_MAX", "0.65"))
LIVE_EDGE_MIN_SAMPLES = int(os.getenv("LIVE_EDGE_MIN_SAMPLES", "1"))
INSIGHTS_PERSIST_INTERVAL_SECONDS = int(os.getenv("INSIGHTS_PERSIST_INTERVAL_SECONDS", "60"))
_INSIGHTS_LAST_PERSIST = {}
ROUTING_TZ = os.getenv("ROUTING_TZ", "Europe/Madrid")
RETRAIN_COMMAND = os.getenv("RETRAIN_COMMAND", "docker exec spark-client /opt/spark-app/run-batch.sh")
RETRAIN_TIMEOUT_SECONDS = int(os.getenv("RETRAIN_TIMEOUT_SECONDS", "3600"))
RETRAIN_MODEL_PATH = os.getenv("RETRAIN_MODEL_PATH", "hdfs://hadoop:9000/models/delay_risk_rf")
RETRAIN_RECOMMEND_THRESHOLD = int(os.getenv("RETRAIN_RECOMMEND_THRESHOLD", "55"))
RETRAIN_RECOMMEND_ON_THRESHOLD = int(os.getenv("RETRAIN_RECOMMEND_ON_THRESHOLD", "70"))
RETRAIN_RECOMMEND_OFF_THRESHOLD = int(os.getenv("RETRAIN_RECOMMEND_OFF_THRESHOLD", "55"))
RETRAIN_COOLDOWN_HOURS = int(os.getenv("RETRAIN_COOLDOWN_HOURS", "48"))
RETRAIN_STATUS_POLL_CACHE_SECONDS = int(os.getenv("RETRAIN_STATUS_POLL_CACHE_SECONDS", "30"))
RETRAIN_STATE_TABLE = os.getenv("CASSANDRA_RETRAIN_STATE_TABLE", "model_retrain_state")
RETRAIN_MODEL_NAME = os.getenv("RETRAIN_MODEL_NAME", "delay_risk_rf")
RETRAIN_RUNTIME_STATE_FILE = PROJECT_ROOT / "data" / "state" / "retrain_runtime_state.json"
RETRAIN_EXTERNAL_STATE_MAX_AGE_SECONDS = int(os.getenv("RETRAIN_EXTERNAL_STATE_MAX_AGE_SECONDS", "21600"))
DISK_CLEANUP_STATE_FILE = PROJECT_ROOT / "data" / "state" / "disk_cleanup_state.json"
DISK_CLEANUP_SCRIPT = PROJECT_ROOT / "scripts" / "safe_disk_cleanup.sh"
DISK_CLEANUP_TIMEOUT_SECONDS = int(os.getenv("DISK_CLEANUP_TIMEOUT_SECONDS", "900"))
DISK_CLEANUP_TRIGGER_COOLDOWN_SECONDS = int(os.getenv("DISK_CLEANUP_TRIGGER_COOLDOWN_SECONDS", "900"))
OPS_STATUS_CACHE_SECONDS = int(os.getenv("OPS_STATUS_CACHE_SECONDS", "20"))
_RETRAIN_LOCK = threading.Lock()
_RETRAIN_STATE = {
    "status": "idle",  # idle | running | done | error
    "started_at": None,
    "finished_at": None,
    "duration_seconds": None,
    "exit_code": None,
    "trigger": None,
    "message": "Sin ejecuciones de reentrenamiento en esta sesion.",
    "output_tail": [],
    "model_selection": None,
}
_RETRAIN_ADVICE_CACHE = {"computed_at": None, "payload": None}
STATIC_DATA_CACHE_TTL_SECONDS = int(os.getenv("STATIC_DATA_CACHE_TTL_SECONDS", "20"))
_STATIC_DATA_CACHE = {}
_STATIC_DATA_CACHE_LOCK = threading.Lock()
_OPS_STATUS_CACHE_LOCK = threading.Lock()
_OPS_STATUS_CACHE = {"loaded_at": 0.0, "value": None}
_DISK_CLEANUP_TRIGGER_LOCK = threading.Lock()
_DISK_CLEANUP_TRIGGER_STATE = {
    "status": "idle",  # idle | running | done | error
    "trigger": None,
    "message": "Sin ejecuciones de limpieza iniciadas desde dashboard.",
    "started_at": None,
    "finished_at": None,
    "duration_seconds": None,
    "exit_code": None,
    "last_started_ts": 0.0,
}

MODEL_CANDIDATES = [
    {"name": "baseline_rf", "description": "Base: variables operativas esenciales (velocidad/tipo/almacen)."},
    {"name": "tuned_baseline_rf", "description": "Baseline con hiperparametros ajustados para mejorar generalizacion."},
    {"name": "enhanced_rf", "description": "Enriquecido con clima y congestion alineados temporalmente (15 min)."},
]


def _path_mtime_ns(path: Path):
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return -1


def _get_cached_static_data(cache_key: str, paths, loader):
    now = time.time()
    mtimes = tuple(_path_mtime_ns(path) for path in paths)
    with _STATIC_DATA_CACHE_LOCK:
        cached = _STATIC_DATA_CACHE.get(cache_key)
        if (
            cached
            and cached.get("mtimes") == mtimes
            and (now - float(cached.get("loaded_at") or 0.0)) <= STATIC_DATA_CACHE_TTL_SECONDS
        ):
            return cached.get("value")

    value = loader()
    with _STATIC_DATA_CACHE_LOCK:
        _STATIC_DATA_CACHE[cache_key] = {
            "mtimes": mtimes,
            "loaded_at": now,
            "value": value,
        }
    return value


def run_command(command, timeout_seconds=8):
    try:
        proc = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=max(2, int(timeout_seconds)),
            check=False,
        )
        return {
            "ok": proc.returncode == 0,
            "code": int(proc.returncode),
            "stdout": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip(),
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": -1,
            "stdout": "",
            "stderr": str(exc),
        }


def fetch_json_url(url: str, timeout_seconds: int = 5):
    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=max(2, int(timeout_seconds))) as response:
            raw = response.read().decode("utf-8", errors="replace")
        return json.loads(raw)
    except Exception:
        return None


def _status_level_from_disk_usage(usage_percent: int):
    if usage_percent >= 90:
        return "critical"
    if usage_percent >= 85:
        return "warn"
    return "ok"


def _parse_yarn_apps_table(raw_text: str):
    apps = []
    if not raw_text:
        return apps
    for line in raw_text.splitlines():
        line = line.strip()
        if not line.startswith("application_"):
            continue
        parts = re.split(r"\s+", line)
        if len(parts) < 8:
            continue
        tracking_url = parts[-1] if len(parts) >= 9 else None
        apps.append(
            {
                "application_id": parts[0],
                "application_name": parts[1],
                "application_type": parts[2],
                "user": parts[3],
                "queue": parts[4],
                "state": parts[5],
                "final_state": parts[6],
                "progress": parts[7],
                "tracking_url": None if tracking_url == "N/A" else tracking_url,
            }
        )
    return apps


def _parse_first_yarn_node_state(raw_text: str):
    if not raw_text:
        return None
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith("Total Nodes:") or line.startswith("Node-Id"):
            continue
        parts = re.split(r"\s+", line)
        if len(parts) < 2:
            continue
        return {
            "node_id": parts[0],
            "state": parts[1],
            "healthy": str(parts[1]).upper() == "RUNNING",
        }
    return None


def load_last_disk_cleanup_state():
    if not DISK_CLEANUP_STATE_FILE.exists():
        return None
    try:
        raw = json.loads(DISK_CLEANUP_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return {
        "updated_at": raw.get("updated_at"),
        "threshold_percent": int(raw.get("threshold_percent") or 88),
        "usage_before_percent": int(raw.get("usage_before_percent") or 0),
        "usage_after_percent": int(raw.get("usage_after_percent") or 0),
        "cleanup_executed": bool(raw.get("cleanup_executed")),
        "deleted_counts": raw.get("deleted_counts") if isinstance(raw.get("deleted_counts"), dict) else {},
    }


def load_last_cleanup_dag_run():
    cmd = [
        "docker",
        "exec",
        "airflow-postgres",
        "psql",
        "-U",
        "airflow",
        "-d",
        "airflow",
        "-At",
        "-F",
        "|",
        "-c",
        (
            "select run_id, state, start_date, end_date "
            "from dag_run "
            "where dag_id='kdd_daily_disk_cleanup' "
            "order by start_date desc limit 1;"
        ),
    ]
    out = run_command(cmd, timeout_seconds=7)
    if not out["ok"] or not out["stdout"]:
        return None
    first_line = out["stdout"].splitlines()[0].strip()
    parts = first_line.split("|")
    if len(parts) < 4:
        return None
    return {
        "run_id": parts[0] or None,
        "state": parts[1] or None,
        "start_date": parts[2] or None,
        "end_date": parts[3] or None,
    }


def _build_platform_status_uncached():
    rm_apps = fetch_json_url(
        "http://hadoop:8088/ws/v1/cluster/apps?states=RUNNING,ACCEPTED&applicationTypes=SPARK",
        timeout_seconds=6,
    )
    rm_nodes = fetch_json_url("http://hadoop:8088/ws/v1/cluster/nodes", timeout_seconds=6)
    hdfs_disk_out = run_command(
        ["docker", "exec", "hadoop", "bash", "-lc", "df -P /data/hdfs | tail -n 1"],
        timeout_seconds=6,
    )

    apps_raw = (((rm_apps or {}).get("apps") or {}).get("app") or [])
    apps = []
    for app in apps_raw:
        if not isinstance(app, dict):
            continue
        apps.append(
            {
                "application_id": str(app.get("id") or ""),
                "application_name": str(app.get("name") or ""),
                "application_type": str(app.get("applicationType") or ""),
                "user": str(app.get("user") or ""),
                "queue": str(app.get("queue") or ""),
                "state": str(app.get("state") or ""),
                "final_state": str(app.get("finalStatus") or ""),
                "progress": str(app.get("progress") or ""),
                "tracking_url": str(app.get("trackingUrl") or "") or None,
            }
        )

    spark_streaming = next((a for a in apps if str(a.get("application_name")) == "LogisticsKddJob"), None)
    if spark_streaming is None and apps:
        spark_streaming = apps[0]

    nodes_raw = (((rm_nodes or {}).get("nodes") or {}).get("node") or [])
    node_status = None
    for node in nodes_raw:
        if not isinstance(node, dict):
            continue
        state_raw = str(node.get("state") or "UNKNOWN").upper()
        node_status = {
            "node_id": str(node.get("id") or "unknown"),
            "state": state_raw,
            "healthy": state_raw == "RUNNING",
        }
        break
    if node_status is None:
        node_status = {"node_id": "unknown", "state": "UNKNOWN", "healthy": False}

    disk_use_percent = None
    disk_avail = None
    disk_size = None
    if hdfs_disk_out.get("ok") and hdfs_disk_out.get("stdout"):
        parts = re.split(r"\s+", hdfs_disk_out["stdout"])
        if len(parts) >= 6:
            try:
                disk_size = parts[1]
                disk_avail = parts[3]
                disk_use_percent = int(str(parts[4]).replace("%", ""))
            except ValueError:
                disk_use_percent = None

    cleanup_dag_run = load_last_cleanup_dag_run()
    local_cleanup_state = load_last_disk_cleanup_state()

    spark_state = str((spark_streaming or {}).get("state") or "NOT_FOUND").upper()
    spark_running = spark_state == "RUNNING"
    tracking_url = (spark_streaming or {}).get("tracking_url")
    app_id = (spark_streaming or {}).get("application_id")
    if app_id:
        tracking_url = f"http://localhost:8088/proxy/{app_id}/"

    return {
        "captured_at": to_iso(now_utc()),
        "spark_streaming": {
            "application_id": app_id,
            "state": spark_state,
            "running": spark_running,
            "tracking_url": tracking_url,
        },
        "yarn_node": node_status,
        "hdfs_disk": {
            "use_percent": disk_use_percent,
            "available": disk_avail,
            "size": disk_size,
            "status": _status_level_from_disk_usage(disk_use_percent) if isinstance(disk_use_percent, int) else "unknown",
        },
        "cleanup_policy": {
            "dag_id": "kdd_daily_disk_cleanup",
            "schedule": "0 */4 * * *",
            "schedule_human": "Cada 4 horas",
            "disk_threshold_percent": int(os.getenv("DISK_CLEANUP_USAGE_THRESHOLD", "88")),
            "safe_scope": [
                "nifi/raw-archive/gps",
                "nifi/raw-archive/weather",
                "nifi/raw-archive/failures",
                "nifi/input (eventos antiguos)",
                "/tmp/checkpoints en HDFS",
            ],
            "protected_data": [
                "/data/raw/gps_events.jsonl",
                "/data/curated",
                "tablas Hive",
                "historicos para entrenamiento",
            ],
            "last_dag_run": cleanup_dag_run,
            "last_local_cleanup": local_cleanup_state,
        },
        "links": {
            "yarn_running_apps": "http://localhost:8088/cluster/apps/RUNNING",
            "spark_history": "http://localhost:18080",
            "airflow_cleanup_dag": "http://localhost:8080/dags/kdd_daily_disk_cleanup/grid",
        },
    }


def get_platform_status_cached():
    now_ts = time.time()
    with _OPS_STATUS_CACHE_LOCK:
        cached = _OPS_STATUS_CACHE.get("value")
        loaded_at = float(_OPS_STATUS_CACHE.get("loaded_at") or 0.0)
        if cached and (now_ts - loaded_at) <= max(5, OPS_STATUS_CACHE_SECONDS):
            return cached

    value = _build_platform_status_uncached()
    with _OPS_STATUS_CACHE_LOCK:
        _OPS_STATUS_CACHE["value"] = value
        _OPS_STATUS_CACHE["loaded_at"] = now_ts
    return value


def invalidate_retrain_advice_cache():
    _RETRAIN_ADVICE_CACHE["computed_at"] = None
    _RETRAIN_ADVICE_CACHE["payload"] = None


def parse_iso_utc(value: str):
    # Parser defensivo: admite formato ISO y sufijo Z.
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def to_iso(dt: datetime):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def now_utc():
    return datetime.now(timezone.utc)


def load_external_retrain_runtime_state():
    if not RETRAIN_RUNTIME_STATE_FILE.exists():
        return None
    try:
        raw = json.loads(RETRAIN_RUNTIME_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    status = str(raw.get("status") or "").strip().lower()
    if status not in {"idle", "running", "done", "error"}:
        return None
    updated_at = parse_iso_utc(str(raw.get("updated_at") or ""))
    if updated_at is not None:
        age_seconds = (now_utc() - updated_at).total_seconds()
        if age_seconds > max(60, RETRAIN_EXTERNAL_STATE_MAX_AGE_SECONDS):
            return None
    return {
        "status": status,
        "message": str(raw.get("message") or ""),
        "started_at": raw.get("started_at"),
        "finished_at": raw.get("finished_at"),
        "exit_code": raw.get("exit_code"),
        "trigger": raw.get("trigger"),
        "source": str(raw.get("source") or "airflow_dag"),
        "run_id": raw.get("run_id"),
        "updated_at": raw.get("updated_at"),
    }


def merge_retrain_state(runtime_state):
    state = dict(runtime_state or {})
    external = load_external_retrain_runtime_state()
    if not external:
        return state
    runtime_status = str(state.get("status") or "idle").lower()
    external_status = str(external.get("status") or "idle").lower()
    if runtime_status == "running":
        return state
    if external_status == "running":
        merged = dict(state)
        merged["status"] = "running"
        merged["started_at"] = external.get("started_at") or merged.get("started_at")
        merged["finished_at"] = None
        merged["duration_seconds"] = None
        merged["exit_code"] = None
        merged["trigger"] = external.get("trigger") or merged.get("trigger")
        merged["message"] = external.get("message") or "Reentrenamiento en ejecucion desde Airflow."
        merged["source"] = external.get("source")
        merged["run_id"] = external.get("run_id")
        return merged
    return state


def _next_monthly_run_utc(reference_utc: datetime | None = None):
    ref = reference_utc or now_utc()
    year = ref.year
    month = ref.month + 1
    if month == 13:
        month = 1
        year += 1
    return datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)


def build_retrain_schedule_info(persisted_state=None):
    persisted = persisted_state if isinstance(persisted_state, dict) else {}
    last_success = persisted.get("last_success_at")
    last_run = persisted.get("last_run_at")
    return {
        "schedule": "@monthly",
        "timezone": ROUTING_TZ,
        "last_retrain_at": last_success or last_run,
        "last_success_at": last_success,
        "last_run_at": last_run,
        "next_scheduled_at": to_iso(_next_monthly_run_utc()),
    }


def _clamp(value: float, low: float, high: float):
    return max(low, min(high, value))


def parse_retrain_model_selection(output_lines):
    if not output_lines:
        return None
    pattern = re.compile(
        r"baseline_rmse=([0-9]+(?:\.[0-9]+)?)\s*\|\s*"
        r"tuned_baseline_rmse=([0-9]+(?:\.[0-9]+)?)\s*\|\s*"
        r"enhanced_rmse=([0-9]+(?:\.[0-9]+)?)\s*\|\s*"
        r"selected=([a-zA-Z0-9_]+)\s*\|\s*"
        r"selected_rmse=([0-9]+(?:\.[0-9]+)?)"
    )
    for line in reversed(output_lines):
        match = pattern.search(str(line))
        if not match:
            continue
        baseline_rmse, tuned_rmse, enhanced_rmse, selected_name, selected_rmse = match.groups()
        rmses = {
            "baseline_rf": float(baseline_rmse),
            "tuned_baseline_rf": float(tuned_rmse),
            "enhanced_rf": float(enhanced_rmse),
        }
        return {
            "criterion": "Se elige automaticamente el menor RMSE en test.",
            "selected_name": str(selected_name),
            "selected_rmse": float(selected_rmse),
            "rmses": rmses,
            "reason": f"{selected_name} obtuvo el RMSE mas bajo ({float(selected_rmse):.4f}).",
        }
    return None


def build_retrain_model_info(runtime_state, persisted_state=None):
    selected = runtime_state.get("model_selection") if isinstance(runtime_state, dict) else None
    if not selected and isinstance(persisted_state, dict):
        persisted_selected = persisted_state.get("last_selected_model")
        if persisted_selected:
            selected = {
                "criterion": "Se elige automaticamente el menor RMSE en test.",
                "selected_name": persisted_selected,
                "selected_rmse": persisted_state.get("last_selected_rmse"),
                "rmses": {
                    "baseline_rf": persisted_state.get("last_baseline_rmse"),
                    "tuned_baseline_rf": persisted_state.get("last_tuned_baseline_rmse"),
                    "enhanced_rf": persisted_state.get("last_enhanced_rmse"),
                },
                "reason": f"{persisted_selected} fue el ultimo candidato ganador registrado.",
            }
    return {
        "artifact_name": RETRAIN_MODEL_NAME,
        "artifact_path": RETRAIN_MODEL_PATH,
        "criterion": "Menor RMSE en test entre baseline_rf, tuned_baseline_rf y enhanced_rf.",
        "candidates": MODEL_CANDIDATES,
        "selected": selected,
    }


def _parse_float(value, default: float):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _routing_now_local(temporal_mode: str):
    mode = str(temporal_mode or "auto").strip().lower()
    try:
        tzinfo = ZoneInfo(ROUTING_TZ)
    except Exception:
        tzinfo = timezone.utc
    now_local = datetime.now(tzinfo)
    if mode == "peak":
        # Hora punta simulada para comparativa reproducible.
        return now_local.replace(hour=8, minute=30, second=0, microsecond=0)
    if mode == "night":
        return now_local.replace(hour=1, minute=30, second=0, microsecond=0)
    if mode == "offpeak":
        return now_local.replace(hour=11, minute=30, second=0, microsecond=0)
    return now_local


def normalize_objective_weights(time_weight: float, risk_weight: float, eco_weight: float):
    raw = {
        "time": _clamp(_parse_float(time_weight, 1.0), 0.0, 10.0),
        "risk": _clamp(_parse_float(risk_weight, 1.0), 0.0, 10.0),
        "eco": _clamp(_parse_float(eco_weight, 1.0), 0.0, 10.0),
    }
    total = raw["time"] + raw["risk"] + raw["eco"]
    if total <= 0:
        return {"time": 1.0 / 3.0, "risk": 1.0 / 3.0, "eco": 1.0 / 3.0}
    return {k: (v / total) for k, v in raw.items()}


def temporal_factor_for_edge(edge, temporal_mode: str = "auto"):
    now_local = _routing_now_local(temporal_mode)
    weekday = now_local.weekday() < 5
    hour = now_local.hour
    distance = float(edge.get("distance_km") or 0.0)
    congestion = str(edge.get("congestion_level") or "unknown").strip().lower()

    # Penalizacion por hora punta en dias laborables, sobre todo en tramos congestionados.
    if weekday and ((7 <= hour <= 10) or (17 <= hour <= 20)):
        base = 1.13
        if congestion == "high":
            base += 0.08
        elif congestion == "medium":
            base += 0.05
        return min(1.32, base + min(0.06, distance / 5000.0))

    # Horario nocturno: menor friccion en tramo urbano/interurbano.
    if hour >= 22 or hour <= 5:
        base = 0.94
        if congestion == "high":
            base = 1.0
        return max(0.88, base - min(0.03, distance / 9000.0))

    # Horario valle diurno.
    return 1.0


def edge_uncertainty_index(delay: float, congestion_weight: float, live_samples: int):
    telemetry_component = 1.0 / (1.0 + min(8.0, max(0, live_samples)) * 0.7)
    delay_component = min(1.3, max(0.0, delay - 4.0) / 14.0)
    congestion_component = (congestion_weight - 1.0) * 0.95
    return telemetry_component + delay_component + congestion_component


def _route_explanation(route, objective_weights):
    weather_pen = float(route.get("weather_penalty_minutes") or 0.0)
    risk_minutes = float(route.get("risk_score_minutes") or 0.0)
    eco_minutes = float(route.get("eco_score_minutes") or 0.0)
    uncertainty = float(route.get("uncertainty_score") or 0.0)
    time_weight = float((objective_weights or {}).get("time") or 0.0)
    risk_weight = float((objective_weights or {}).get("risk") or 0.0)
    eco_weight = float((objective_weights or {}).get("eco") or 0.0)

    factors = [
        (weather_pen * max(0.4, time_weight + risk_weight), f"Impacto meteorologico: +{round(weather_pen, 1)} min"),
        (
            risk_minutes * max(0.5, risk_weight),
            f"Riesgo operativo acumulado: {round(risk_minutes, 1)} pts",
        ),
        (
            eco_minutes * max(0.5, eco_weight),
            f"Coste eco (distancia + stop&go): {round(eco_minutes, 1)} pts",
        ),
        (
            uncertainty * max(0.5, risk_weight),
            f"Incertidumbre de red: {round(uncertainty, 2)}",
        ),
    ]
    factors.sort(key=lambda x: x[0], reverse=True)
    return [text for _, text in factors[:3]]


def _run_retrain_worker(trigger: str):
    started = now_utc()
    with _RETRAIN_LOCK:
        _RETRAIN_STATE["status"] = "running"
        _RETRAIN_STATE["started_at"] = to_iso(started)
        _RETRAIN_STATE["finished_at"] = None
        _RETRAIN_STATE["duration_seconds"] = None
        _RETRAIN_STATE["exit_code"] = None
        _RETRAIN_STATE["trigger"] = trigger
        _RETRAIN_STATE["message"] = "Reentrenamiento en ejecucion..."
        _RETRAIN_STATE["output_tail"] = []
        _RETRAIN_STATE["model_selection"] = None

    output_tail = []
    try:
        proc = subprocess.Popen(
            RETRAIN_COMMAND,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + max(30, RETRAIN_TIMEOUT_SECONDS)
        while True:
            line = proc.stdout.readline() if proc.stdout else ""
            if line:
                output_tail.append(line.rstrip())
                if len(output_tail) > 120:
                    output_tail = output_tail[-120:]
            if proc.poll() is not None:
                break
            if time.time() > deadline:
                proc.kill()
                raise TimeoutError(f"Timeout de reentrenamiento ({RETRAIN_TIMEOUT_SECONDS}s)")
        exit_code = int(proc.returncode or 0)
        status = "done" if exit_code == 0 else "error"
        msg = "Reentrenamiento completado correctamente." if exit_code == 0 else f"Reentrenamiento fallo con exit_code={exit_code}."
    except Exception as exc:
        status = "error"
        exit_code = 124 if isinstance(exc, TimeoutError) else 1
        msg = f"Error ejecutando reentrenamiento: {exc}"

    finished = now_utc()
    with _RETRAIN_LOCK:
        model_selection = parse_retrain_model_selection(output_tail)
        _RETRAIN_STATE["status"] = status
        _RETRAIN_STATE["finished_at"] = to_iso(finished)
        _RETRAIN_STATE["duration_seconds"] = round((finished - started).total_seconds(), 2)
        _RETRAIN_STATE["exit_code"] = exit_code
        _RETRAIN_STATE["message"] = msg
        _RETRAIN_STATE["output_tail"] = output_tail[-40:]
        _RETRAIN_STATE["model_selection"] = model_selection
    persist_retrain_runtime_state(
        status=status,
        trigger=trigger,
        started_at=started,
        finished_at=finished,
        duration_seconds=round((finished - started).total_seconds(), 2),
        exit_code=exit_code,
        message=msg,
        model_selection=model_selection,
    )


def start_retrain_if_idle(trigger: str = "manual"):
    with _RETRAIN_LOCK:
        external = load_external_retrain_runtime_state()
        if external and str(external.get("status") or "").lower() == "running":
            merged = merge_retrain_state(dict(_RETRAIN_STATE))
            return False, merged
        if _RETRAIN_STATE.get("status") == "running":
            return False, dict(_RETRAIN_STATE)
        _RETRAIN_STATE["status"] = "running"
        _RETRAIN_STATE["started_at"] = to_iso(now_utc())
        _RETRAIN_STATE["finished_at"] = None
        _RETRAIN_STATE["duration_seconds"] = None
        _RETRAIN_STATE["exit_code"] = None
        _RETRAIN_STATE["trigger"] = trigger
        _RETRAIN_STATE["message"] = "Reentrenamiento en cola..."
        _RETRAIN_STATE["output_tail"] = []
        _RETRAIN_STATE["model_selection"] = None
        invalidate_retrain_advice_cache()
        worker = threading.Thread(target=_run_retrain_worker, args=(trigger,), daemon=True)
        worker.start()
        return True, dict(_RETRAIN_STATE)


def _run_disk_cleanup_worker(trigger: str):
    started = now_utc()
    with _DISK_CLEANUP_TRIGGER_LOCK:
        _DISK_CLEANUP_TRIGGER_STATE["status"] = "running"
        _DISK_CLEANUP_TRIGGER_STATE["trigger"] = trigger
        _DISK_CLEANUP_TRIGGER_STATE["message"] = "Limpieza de disco en ejecucion..."
        _DISK_CLEANUP_TRIGGER_STATE["started_at"] = to_iso(started)
        _DISK_CLEANUP_TRIGGER_STATE["finished_at"] = None
        _DISK_CLEANUP_TRIGGER_STATE["duration_seconds"] = None
        _DISK_CLEANUP_TRIGGER_STATE["exit_code"] = None

    if not DISK_CLEANUP_SCRIPT.exists():
        status = "error"
        exit_code = 127
        msg = f"Script no encontrado: {DISK_CLEANUP_SCRIPT}"
    else:
        out = run_command(["bash", str(DISK_CLEANUP_SCRIPT)], timeout_seconds=max(30, DISK_CLEANUP_TIMEOUT_SECONDS))
        status = "done" if out.get("ok") else "error"
        exit_code = int(out.get("code") or 1)
        stderr = str(out.get("stderr") or "").strip()
        msg = "Limpieza de disco completada." if status == "done" else f"Limpieza de disco fallo (code={exit_code}): {stderr or 'sin detalle'}"

    finished = now_utc()
    with _DISK_CLEANUP_TRIGGER_LOCK:
        _DISK_CLEANUP_TRIGGER_STATE["status"] = status
        _DISK_CLEANUP_TRIGGER_STATE["message"] = msg
        _DISK_CLEANUP_TRIGGER_STATE["finished_at"] = to_iso(finished)
        _DISK_CLEANUP_TRIGGER_STATE["duration_seconds"] = round((finished - started).total_seconds(), 2)
        _DISK_CLEANUP_TRIGGER_STATE["exit_code"] = exit_code

    with _OPS_STATUS_CACHE_LOCK:
        _OPS_STATUS_CACHE["loaded_at"] = 0.0
        _OPS_STATUS_CACHE["value"] = None


def start_disk_cleanup_if_allowed(trigger: str = "manual_dashboard"):
    now_ts = time.time()
    with _DISK_CLEANUP_TRIGGER_LOCK:
        if _DISK_CLEANUP_TRIGGER_STATE.get("status") == "running":
            return False, "running", dict(_DISK_CLEANUP_TRIGGER_STATE)
        last_started_ts = float(_DISK_CLEANUP_TRIGGER_STATE.get("last_started_ts") or 0.0)
        cooldown = max(30, int(DISK_CLEANUP_TRIGGER_COOLDOWN_SECONDS))
        if (now_ts - last_started_ts) < cooldown:
            return False, "cooldown", dict(_DISK_CLEANUP_TRIGGER_STATE)

        _DISK_CLEANUP_TRIGGER_STATE["status"] = "running"
        _DISK_CLEANUP_TRIGGER_STATE["trigger"] = trigger
        _DISK_CLEANUP_TRIGGER_STATE["message"] = "Limpieza de disco en cola..."
        _DISK_CLEANUP_TRIGGER_STATE["started_at"] = to_iso(now_utc())
        _DISK_CLEANUP_TRIGGER_STATE["finished_at"] = None
        _DISK_CLEANUP_TRIGGER_STATE["duration_seconds"] = None
        _DISK_CLEANUP_TRIGGER_STATE["exit_code"] = None
        _DISK_CLEANUP_TRIGGER_STATE["last_started_ts"] = now_ts
        worker = threading.Thread(target=_run_disk_cleanup_worker, args=(trigger,), daemon=True)
        worker.start()
        return True, "started", dict(_DISK_CLEANUP_TRIGGER_STATE)


def _retrain_default_persisted_state():
    return {
        "model_name": RETRAIN_MODEL_NAME,
        "last_success_at": None,
        "last_run_at": None,
        "last_status": None,
        "last_recommendation": None,
        "last_score": None,
        "last_selected_model": None,
        "last_selected_rmse": None,
        "last_baseline_rmse": None,
        "last_tuned_baseline_rmse": None,
        "last_enhanced_rmse": None,
        "updated_at": None,
        "source": "none",
    }


def ensure_retrain_state_table(session):
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} (
            model_name text PRIMARY KEY,
            last_success_at timestamp,
            last_run_at timestamp,
            last_status text,
            last_recommendation boolean,
            last_score int,
            last_selected_model text,
            last_selected_rmse double,
            last_baseline_rmse double,
            last_tuned_baseline_rmse double,
            last_enhanced_rmse double,
            updated_at timestamp
        )
        """
    )
    # Compatibilidad con tablas antiguas ya creadas sin columnas de seleccion.
    alter_stmts = [
        f"ALTER TABLE {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} ADD last_selected_model text",
        f"ALTER TABLE {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} ADD last_selected_rmse double",
        f"ALTER TABLE {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} ADD last_baseline_rmse double",
        f"ALTER TABLE {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} ADD last_tuned_baseline_rmse double",
        f"ALTER TABLE {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} ADD last_enhanced_rmse double",
    ]
    for stmt in alter_stmts:
        try:
            session.execute(stmt)
        except Exception:
            pass


def load_persisted_retrain_state():
    if Cluster is None:
        state = _retrain_default_persisted_state()
        state["source"] = "driver_missing"
        return state

    cluster = None
    session = None
    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        ensure_retrain_state_table(session)
        row = session.execute(
            f"SELECT model_name, last_success_at, last_run_at, last_status, "
            f"last_recommendation, last_score, "
            f"last_selected_model, last_selected_rmse, "
            f"last_baseline_rmse, last_tuned_baseline_rmse, last_enhanced_rmse, "
            f"updated_at "
            f"FROM {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} WHERE model_name=%s",
            [RETRAIN_MODEL_NAME],
        ).one()
        if row is None:
            state = _retrain_default_persisted_state()
            state["source"] = "cassandra_empty"
            return state
        return {
            "model_name": str(getattr(row, "model_name", RETRAIN_MODEL_NAME) or RETRAIN_MODEL_NAME),
            "last_success_at": _to_event_time(getattr(row, "last_success_at", None)),
            "last_run_at": _to_event_time(getattr(row, "last_run_at", None)),
            "last_status": getattr(row, "last_status", None),
            "last_recommendation": getattr(row, "last_recommendation", None),
            "last_score": int(getattr(row, "last_score", 0) or 0) if getattr(row, "last_score", None) is not None else None,
            "last_selected_model": getattr(row, "last_selected_model", None),
            "last_selected_rmse": float(getattr(row, "last_selected_rmse", 0.0) or 0.0)
            if getattr(row, "last_selected_rmse", None) is not None
            else None,
            "last_baseline_rmse": float(getattr(row, "last_baseline_rmse", 0.0) or 0.0)
            if getattr(row, "last_baseline_rmse", None) is not None
            else None,
            "last_tuned_baseline_rmse": float(getattr(row, "last_tuned_baseline_rmse", 0.0) or 0.0)
            if getattr(row, "last_tuned_baseline_rmse", None) is not None
            else None,
            "last_enhanced_rmse": float(getattr(row, "last_enhanced_rmse", 0.0) or 0.0)
            if getattr(row, "last_enhanced_rmse", None) is not None
            else None,
            "updated_at": _to_event_time(getattr(row, "updated_at", None)),
            "source": "cassandra",
        }
    except Exception:
        state = _retrain_default_persisted_state()
        state["source"] = "cassandra_error"
        return state
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def persist_retrain_runtime_state(status, trigger, started_at, finished_at, duration_seconds, exit_code, message, model_selection=None):
    if Cluster is None:
        return
    cluster = None
    session = None
    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        ensure_retrain_state_table(session)
        previous = session.execute(
            f"SELECT last_success_at, last_recommendation, last_score, "
            f"last_selected_model, last_selected_rmse, "
            f"last_baseline_rmse, last_tuned_baseline_rmse, last_enhanced_rmse "
            f"FROM {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} "
            f"WHERE model_name=%s",
            [RETRAIN_MODEL_NAME],
        ).one()
        prev_success = getattr(previous, "last_success_at", None) if previous else None
        prev_recommendation = getattr(previous, "last_recommendation", None) if previous else None
        prev_score = getattr(previous, "last_score", None) if previous else None
        prev_selected_model = getattr(previous, "last_selected_model", None) if previous else None
        prev_selected_rmse = getattr(previous, "last_selected_rmse", None) if previous else None
        prev_baseline_rmse = getattr(previous, "last_baseline_rmse", None) if previous else None
        prev_tuned_rmse = getattr(previous, "last_tuned_baseline_rmse", None) if previous else None
        prev_enhanced_rmse = getattr(previous, "last_enhanced_rmse", None) if previous else None
        selected_model = model_selection.get("selected_name") if isinstance(model_selection, dict) else None
        rmses = model_selection.get("rmses") if isinstance(model_selection, dict) else {}
        baseline_rmse = rmses.get("baseline_rf") if isinstance(rmses, dict) else None
        tuned_rmse = rmses.get("tuned_baseline_rf") if isinstance(rmses, dict) else None
        enhanced_rmse = rmses.get("enhanced_rf") if isinstance(rmses, dict) else None
        selected_rmse = model_selection.get("selected_rmse") if isinstance(model_selection, dict) else None
        new_success = finished_at if status == "done" else prev_success
        session.execute(
            f"INSERT INTO {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} "
            f"(model_name, last_success_at, last_run_at, last_status, last_recommendation, last_score, "
            f"last_selected_model, last_selected_rmse, last_baseline_rmse, last_tuned_baseline_rmse, last_enhanced_rmse, "
            f"updated_at) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                RETRAIN_MODEL_NAME,
                new_success,
                finished_at,
                str(status),
                prev_recommendation,
                int(prev_score) if prev_score is not None else None,
                selected_model or prev_selected_model,
                float(selected_rmse) if selected_rmse is not None else prev_selected_rmse,
                float(baseline_rmse) if baseline_rmse is not None else prev_baseline_rmse,
                float(tuned_rmse) if tuned_rmse is not None else prev_tuned_rmse,
                float(enhanced_rmse) if enhanced_rmse is not None else prev_enhanced_rmse,
                now_utc(),
            ],
        )
        invalidate_retrain_advice_cache()
    except Exception:
        return
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def persist_retrain_advice_state(recommended: bool, score: int):
    if Cluster is None:
        return
    persisted = load_persisted_retrain_state()
    last_success = parse_iso_utc(persisted.get("last_success_at")) if persisted.get("last_success_at") else None
    last_run = parse_iso_utc(persisted.get("last_run_at")) if persisted.get("last_run_at") else None
    cluster = None
    session = None
    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        ensure_retrain_state_table(session)
        session.execute(
            f"INSERT INTO {CASSANDRA_KEYSPACE}.{RETRAIN_STATE_TABLE} "
            f"(model_name, last_success_at, last_run_at, last_status, last_recommendation, last_score, "
            f"last_selected_model, last_selected_rmse, last_baseline_rmse, last_tuned_baseline_rmse, last_enhanced_rmse, "
            f"updated_at) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                RETRAIN_MODEL_NAME,
                last_success,
                last_run,
                persisted.get("last_status"),
                bool(recommended),
                int(score),
                persisted.get("last_selected_model"),
                persisted.get("last_selected_rmse"),
                persisted.get("last_baseline_rmse"),
                persisted.get("last_tuned_baseline_rmse"),
                persisted.get("last_enhanced_rmse"),
                now_utc(),
            ],
        )
        invalidate_retrain_advice_cache()
    except Exception:
        return
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def _compute_retrain_advice_payload():
    events = load_gps_events(max_files=60, max_events=3000)
    weather_rows, _weather_source, _weather_meta = load_weather_latest(limit=12)
    warehouses = read_warehouses()
    vertices, edges = load_route_graph()
    warehouse_aliases = build_warehouse_aliases(vertices)
    latest_rows, _source, _meta = load_vehicle_latest_preferred(events, warehouse_aliases, limit=200)
    vehicle_plans = load_vehicle_path_plans(warehouse_aliases)
    attach_vehicle_plans(latest_rows, vehicle_plans)
    overview = build_overview_from_latest_vehicle_state(latest_rows, weather_rows)
    _edges_with_live, live_edge_summary = apply_live_edge_telemetry(edges, latest_rows)

    score = 0
    reasons = []
    avg_delay = float(overview.get("avg_delay_minutes") or 0.0)
    weather_factor = float(overview.get("weather_factor") or 0.0)
    latest_event = parse_iso_utc(overview.get("latest_event_time"))
    age_minutes = None
    if latest_event:
        age_minutes = (now_utc() - latest_event).total_seconds() / 60.0
    vehicles_considered = int(live_edge_summary.get("vehicles_considered") or 0)
    edges_with_live_samples = int(live_edge_summary.get("edges_with_live_samples") or 0)
    total_edges = max(1, len(edges))
    live_coverage_ratio = 0.0 if not edges else edges_with_live_samples / total_edges
    expected_edges_by_active_vehicles = min(total_edges, max(0, vehicles_considered))
    expected_live_coverage_ratio = expected_edges_by_active_vehicles / total_edges

    persisted = load_persisted_retrain_state()
    last_success = parse_iso_utc(persisted.get("last_success_at")) if persisted.get("last_success_at") else None
    previous_recommendation = persisted.get("last_recommendation")

    if last_success is None:
        score += 16
        reasons.append("No hay constancia persistida de reentrenamiento exitoso.")
    else:
        hours_since_success = (now_utc() - last_success).total_seconds() / 3600.0
        if hours_since_success >= 24 * 7:
            score += 26
            reasons.append(f"Ultimo reentrenamiento exitoso hace {round(hours_since_success / 24.0, 1)} dias.")
        elif hours_since_success >= 24 * 3:
            score += 12
            reasons.append(f"Han pasado {round(hours_since_success / 24.0, 1)} dias desde el ultimo reentrenamiento exitoso.")

    if avg_delay >= 14:
        score += 24
        reasons.append(f"Delay medio elevado ({round(avg_delay, 1)} min).")
    elif avg_delay >= 9:
        score += 12
        reasons.append(f"Delay medio moderado-alto ({round(avg_delay, 1)} min).")

    if weather_factor >= 1.2:
        score += 8
        reasons.append(f"Condiciones meteo exigentes (factor {round(weather_factor, 2)}).")

    if live_coverage_ratio < 0.32:
        score += 15
        reasons.append(
            f"Baja cobertura live en aristas ({edges_with_live_samples}/{total_edges}, "
            f"{round(live_coverage_ratio * 100, 1)}%; porcentaje de aristas del grafo con muestra live reciente; "
            f"cobertura esperada por vehiculos activos: "
            f"{expected_edges_by_active_vehicles}/{total_edges}, {round(expected_live_coverage_ratio * 100, 1)}%)."
        )
    elif live_coverage_ratio < 0.5:
        score += 7
        reasons.append(
            f"Cobertura live mejorable ({edges_with_live_samples}/{total_edges}, "
            f"{round(live_coverage_ratio * 100, 1)}%; porcentaje de aristas del grafo con muestra live reciente; "
            f"cobertura esperada por vehiculos activos: "
            f"{expected_edges_by_active_vehicles}/{total_edges}, {round(expected_live_coverage_ratio * 100, 1)}%)."
        )

    if age_minutes is not None and age_minutes > 180:
        score += 9
        reasons.append(f"Ultimo evento con {round(age_minutes)} min de antiguedad.")

    score = int(_clamp(score, 0, 100))
    threshold_on = max(RETRAIN_RECOMMEND_THRESHOLD, RETRAIN_RECOMMEND_ON_THRESHOLD)
    threshold_off = min(RETRAIN_RECOMMEND_THRESHOLD, RETRAIN_RECOMMEND_OFF_THRESHOLD)

    recommended = bool(score >= threshold_on)
    if previous_recommendation is not None:
        if bool(previous_recommendation):
            recommended = bool(score >= threshold_off)
        else:
            recommended = bool(score >= threshold_on)

    cooldown_remaining_hours = 0.0
    if last_success is not None and RETRAIN_COOLDOWN_HOURS > 0:
        hours_since_success = (now_utc() - last_success).total_seconds() / 3600.0
        if hours_since_success < RETRAIN_COOLDOWN_HOURS:
            cooldown_remaining_hours = max(0.0, RETRAIN_COOLDOWN_HOURS - hours_since_success)
            recommended = False
            reasons.insert(
                0,
                f"En enfriamiento tras reentreno exitoso: faltan {round(cooldown_remaining_hours, 1)} h para nueva recomendacion.",
            )

    persist_retrain_advice_state(recommended=recommended, score=score)
    if not reasons:
        reasons.append("Metricas estables: no se observan sintomas fuertes de deriva.")
    return {
        "recommended": recommended,
        "score": score,
        "threshold": RETRAIN_RECOMMEND_THRESHOLD,
        "threshold_on": threshold_on,
        "threshold_off": threshold_off,
        "reasons": reasons[:5],
        "cooldown_hours": RETRAIN_COOLDOWN_HOURS,
        "cooldown_remaining_hours": round(cooldown_remaining_hours, 2),
        "persisted_state": {
            "last_success_at": persisted.get("last_success_at"),
            "last_status": persisted.get("last_status"),
            "last_recommendation": persisted.get("last_recommendation"),
            "source": persisted.get("source"),
        },
        "metrics": {
            "avg_delay_minutes": round(avg_delay, 2),
            "weather_factor": round(weather_factor, 3),
            "live_coverage_ratio": round(live_coverage_ratio, 3),
            "live_coverage_expected_ratio": round(expected_live_coverage_ratio, 3),
            "vehicles_considered": vehicles_considered,
            "edges_with_live_samples": edges_with_live_samples,
            "total_edges": len(edges),
            "latest_event_age_minutes": round(age_minutes, 1) if age_minutes is not None else None,
        },
    }


def get_retrain_advice_cached():
    now = now_utc()
    cached_at = _RETRAIN_ADVICE_CACHE.get("computed_at")
    cached_payload = _RETRAIN_ADVICE_CACHE.get("payload")
    if cached_at and cached_payload:
        age = (now - cached_at).total_seconds()
        if age <= RETRAIN_STATUS_POLL_CACHE_SECONDS:
            return cached_payload
    payload = _compute_retrain_advice_payload()
    _RETRAIN_ADVICE_CACHE["computed_at"] = now
    _RETRAIN_ADVICE_CACHE["payload"] = payload
    return payload

def list_latest_files(path: Path, pattern: str, limit: int):
    # Devuelve los ficheros mas recientes por fecha de modificacion.
    if not path.exists():
        return []
    # Evita errores de carrera cuando otro proceso borra ficheros mientras se listan.
    candidates = []
    for file_path in path.glob(pattern):
        try:
            mtime = file_path.stat().st_mtime
        except OSError:
            continue
        candidates.append((mtime, file_path))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return [file_path for _, file_path in candidates[:limit]]


def load_gps_events(max_files: int = 120, max_events: int = 7000):
    # Carga eventos recientes desde jsonl producidos por gps-generator.
    # Se ordena por timestamp ascendente para calculos consistentes posteriores.
    events = []
    for file_path in list_latest_files(GPS_DIR, "gps_*.jsonl", max_files):
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = parse_iso_utc(payload.get("event_time"))
                    if ts is None:
                        continue
                    payload["event_timestamp"] = ts
                    events.append(payload)
                    if len(events) >= max_events:
                        break
        except OSError:
            continue
        if len(events) >= max_events:
            break

    events.sort(key=lambda e: e["event_timestamp"])
    return events


def load_weather_snapshots(limit: int = 30):
    # Fallback local de clima: snapshots archivados por NiFi.
    # Se deduplica por timestamp + variables para evitar mostrar repetidos.
    snapshots = []
    if not WEATHER_DIR.exists():
        return snapshots

    candidate_dirs = [WEATHER_DIR, WEATHER_DIR / ".processed"]
    files = []
    for directory in candidate_dirs:
        if not directory.exists() or not directory.is_dir():
            continue
        files.extend(
            [
                p
                for p in directory.iterdir()
                if p.is_file() and not p.name.startswith(".") and p.name != ".gitkeep"
            ]
        )
    files = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)[: max(limit * 6, limit)]

    for file_path in files:
        try:
            raw = file_path.read_text(encoding="utf-8").strip()
            if not raw:
                continue
            payload = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            continue

        current = payload.get("current") or {}
        raw_weather_code = current.get("weather_code")
        observed = parse_iso_utc(current.get("time"))
        snapshots.append(
            {
                "source": payload.get("timezone") or "open-meteo",
                "observed_at": to_iso(observed) if observed else None,
                "temperature_c": float(current.get("temperature_2m") or 0.0),
                "precipitation_mm": float(current.get("precipitation") or 0.0),
                "wind_kmh": float(current.get("wind_speed_10m") or 0.0),
                "weather_code": str(raw_weather_code) if raw_weather_code is not None else "unknown",
            }
        )

    snapshots.sort(key=lambda w: w.get("observed_at") or "", reverse=True)
    deduped = []
    seen = set()
    for row in snapshots:
        key = (
            row.get("observed_at"),
            round(float(row.get("temperature_c") or 0.0), 1),
            round(float(row.get("precipitation_mm") or 0.0), 2),
            round(float(row.get("wind_kmh") or 0.0), 1),
            str(row.get("weather_code") or "unknown"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
        if len(deduped) >= limit:
            break
    return deduped


def load_weather_from_cassandra_with_meta(limit: int = 30):
    meta = {
        "enabled": Cluster is not None,
        "host": CASSANDRA_HOST,
        "port": CASSANDRA_PORT,
        "keyspace": CASSANDRA_KEYSPACE,
        "table": CASSANDRA_WEATHER_TABLE,
        "status": "unknown",
        "error": None,
        "row_count": 0,
    }
    if Cluster is None:
        meta["status"] = "driver_missing"
        meta["error"] = "cassandra-driver no disponible"
        return [], meta

    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        stmt = (
            f"SELECT weather_timestamp, weather_event_id, warehouse_id, temperature_c, "
            f"precipitation_mm, wind_kmh, weather_code, source "
            f"FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_WEATHER_TABLE} "
            f"WHERE bucket='all' LIMIT %s"
        )
        rows = session.execute(stmt, [max(limit * 3, limit)])
        items = []
        for row in rows:
            observed_at = _to_event_time(getattr(row, "weather_timestamp", None))
            if not observed_at:
                continue
            items.append(
                {
                    "source": str(getattr(row, "source", None) or "cassandra"),
                    "observed_at": observed_at,
                    "temperature_c": float(getattr(row, "temperature_c", 0.0) or 0.0),
                    "precipitation_mm": float(getattr(row, "precipitation_mm", 0.0) or 0.0),
                    "wind_kmh": float(getattr(row, "wind_kmh", 0.0) or 0.0),
                    "weather_code": str(getattr(row, "weather_code", None) or "unknown"),
                    "warehouse_id": getattr(row, "warehouse_id", None),
                    "weather_event_id": getattr(row, "weather_event_id", None),
                }
            )
        items.sort(key=lambda r: r.get("observed_at") or "", reverse=True)
        selected = items[:limit]
        meta["status"] = "ok"
        meta["row_count"] = len(selected)
        return selected, meta
    except Exception as exc:
        meta["status"] = "error"
        meta["error"] = str(exc)
        return [], meta
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def load_weather_latest(limit: int = 30):
    # Estrategia de fuente:
    # 1) Cassandra (primaria).
    # 2) Si fallback NiFi es mas reciente, sincroniza y vuelve a intentar Cassandra.
    # 3) Si no hay Cassandra, devuelve fallback raw-archive.
    rows, meta = load_weather_from_cassandra_with_meta(limit=limit)
    fallback = load_weather_snapshots(limit=limit)

    def _latest_weather_dt(items):
        parsed = [parse_iso_utc(i.get("observed_at")) for i in items if i.get("observed_at")]
        parsed = [p for p in parsed if p is not None]
        return max(parsed) if parsed else None

    cass_latest = _latest_weather_dt(rows)
    fallback_latest = _latest_weather_dt(fallback)

    if fallback and (not rows or (fallback_latest and cass_latest and fallback_latest > cass_latest + timedelta(minutes=2))):
        sync_weather_to_cassandra(fallback)
        refreshed_rows, refreshed_meta = load_weather_from_cassandra_with_meta(limit=limit)
        if refreshed_rows:
            return refreshed_rows, "cassandra", refreshed_meta
        return fallback, "nifi_raw_archive", refreshed_meta

    if rows:
        return rows, "cassandra", meta
    return fallback, "nifi_raw_archive", meta


def compute_weather_factor(weather_rows):
    if not weather_rows:
        return 0.0
    avg_rain = sum(w["precipitation_mm"] for w in weather_rows) / len(weather_rows)
    avg_wind = sum(w["wind_kmh"] for w in weather_rows) / len(weather_rows)
    rain_component = min(avg_rain / 0.4, 2.0)
    wind_component = min(avg_wind / 18.0, 2.0)
    severe_code_component = 0.0
    if any(str(w.get("weather_code", "0")) not in {"0", "1"} for w in weather_rows):
        severe_code_component = 0.25
    return min(0.10 + (0.70 * rain_component) + (0.45 * wind_component) + severe_code_component, 2.5)


def weather_impact_level(weather_factor: float):
    if weather_factor < 0.45:
        return "low"
    if weather_factor < 0.95:
        return "medium"
    return "high"


def load_route_graph():
    def _load():
        vertices = []
        edges = []

        if GRAPH_VERTICES_PATH.exists():
            with GRAPH_VERTICES_PATH.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    vertices.append(
                        {
                            "id": row.get("id"),
                            "name": row.get("name"),
                            "type": row.get("type"),
                            "criticality": row.get("criticality", "unknown"),
                        }
                    )

        if GRAPH_EDGES_PATH.exists():
            with GRAPH_EDGES_PATH.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    try:
                        distance_km = float(row.get("distance_km") or 0)
                        avg_delay = float(row.get("avg_delay_minutes") or 0)
                    except ValueError:
                        continue

                    edges.append(
                        {
                            "src": row.get("src"),
                            "dst": row.get("dst"),
                            "distance_km": distance_km,
                            "avg_delay_minutes": avg_delay,
                        }
                    )

        return vertices, edges

    return _get_cached_static_data(
        "route_graph",
        [GRAPH_VERTICES_PATH, GRAPH_EDGES_PATH],
        _load,
    )


def build_warehouse_aliases(vertices):
    aliases = {}
    ordered_ids = [v["id"] for v in vertices if v.get("id")]
    for idx, canonical in enumerate(ordered_ids, start=1):
        aliases[f"WH{idx}"] = canonical
    return aliases


def normalize_warehouse_id(raw_warehouse_id, aliases):
    if raw_warehouse_id is None:
        return None
    return aliases.get(raw_warehouse_id, raw_warehouse_id)


def load_allowed_vehicle_ids():
    # IDs de flota validos para UI (activos + mantenimiento) para evitar legacy.
    ids, _status_map = _load_vehicle_master_cache()
    return set(ids)


def load_vehicle_status_map():
    _ids, status_map = _load_vehicle_master_cache()
    return dict(status_map)


def _load_vehicle_master_cache():
    def _load():
        ids = set()
        status_map = {}
        if not MASTER_VEHICLES_PATH.exists():
            return ids, status_map
        try:
            with MASTER_VEHICLES_PATH.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    vid = str(row.get("vehicle_id") or "").strip()
                    status = str(row.get("status") or "").strip().lower() or "unknown"
                    if not vid:
                        continue
                    ids.add(vid)
                    status_map[vid] = status
        except OSError:
            return set(), {}
        return ids, status_map

    return _get_cached_static_data("vehicle_master", [MASTER_VEHICLES_PATH], _load)


def filter_rows_by_vehicle_ids(rows, allowed_ids):
    if not rows or not allowed_ids:
        return rows
    return [row for row in rows if str(row.get("vehicle_id") or "") in allowed_ids]


def edge_weight(edge, profile: str, weather_factor: float, objective_weights=None, temporal_mode: str = "auto"):
    # Calcula coste de arista para routing segun perfil y clima.
    # Retorna metricas de coste (operativas + optimizacion).
    objective_weights = objective_weights or {"time": 1.0 / 3.0, "risk": 1.0 / 3.0, "eco": 1.0 / 3.0}
    profile = str(profile or "balanced").strip().lower()
    if profile not in {"balanced", "fastest", "resilient", "eco", "low_risk", "reliable"}:
        profile = "balanced"
    distance = edge["distance_km"]
    delay = float(edge.get("effective_avg_delay_minutes", edge.get("avg_delay_minutes", 0.0)))
    live_samples = int(edge.get("live_sample_count") or 0)
    congestion = str(edge.get("congestion_level") or "unknown").strip().lower()
    congestion_weight = 1.1
    if congestion == "medium":
        congestion_weight = 1.25
    elif congestion == "high":
        congestion_weight = 1.6
    elif congestion == "low":
        congestion_weight = 1.0

    temporal_factor = temporal_factor_for_edge(edge, temporal_mode=temporal_mode)
    uncertainty = edge_uncertainty_index(delay, congestion_weight, live_samples)
    weather_multiplier = max(0.5, temporal_factor)

    if profile == "reliable":
        # Minimiza variabilidad de ETA: fuerte castigo a incertidumbre y climatologia.
        base = (distance / 71.0) * 60.0 + (delay * 1.35)
        weather_penalty = (delay * weather_factor * 2.8 * weather_multiplier) + ((congestion_weight - 1.0) * 16.0)
        risk_component = (uncertainty * 14.0) + (max(0.0, delay - 6.0) ** 1.15) * 1.2
        eco_component = (distance * 0.085) + (delay * 0.32) + ((congestion_weight - 1.0) * 4.2)
        operational_total = base + weather_penalty + (uncertainty * 2.4)
        routing_weight = (
            operational_total * float(objective_weights.get("time", 0.0))
            + risk_component * float(objective_weights.get("risk", 0.0))
            + eco_component * float(objective_weights.get("eco", 0.0))
        )
        return {
            "routing_weight": routing_weight,
            "base_minutes": base,
            "weather_penalty_minutes": weather_penalty,
            "total_minutes": operational_total,
            "uncertainty_index": uncertainty,
            "risk_component_minutes": risk_component,
            "eco_component_minutes": eco_component,
            "temporal_factor": temporal_factor,
        }

    if profile == "fastest":
        # Prioriza tiempo puro y tolera peor climatologia/congestion.
        base = (distance / 100.0) * 60.0 + (delay * 0.55)
        weather_penalty = (delay * weather_factor * 0.8 * weather_multiplier) + ((congestion_weight - 1.0) * 4.0)
    elif profile == "resilient":
        # Penaliza fuerte la climatologia y la congestion para evitar tramos "fragiles".
        base = (distance / 62.0) * 60.0 + (delay * 1.65)
        weather_penalty = (
            (delay * weather_factor * 2.6 * weather_multiplier)
            + ((congestion_weight - 1.0) * 14.0)
            + (max(0.0, delay - 8.0) ** 1.2) * 0.9
        )
    elif profile == "eco":
        # Favorece tramos eficientes (menos km, menos stop&go, menos delay).
        base = (distance / 74.0) * 60.0 + (delay * 1.2) + (distance * 0.018)
        weather_penalty = (
            (delay * weather_factor * 1.2 * weather_multiplier)
            + ((congestion_weight - 1.0) * 10.0)
            + (max(0.0, delay - 6.0) ** 1.12) * 0.55
        )
    elif profile == "low_risk":
        # Busca ETA estable: evita clima adverso, congestion y tramos con poca telemetria live.
        telemetry_risk = 1.9 if live_samples <= 0 else 1.0 / (1.0 + min(6.0, live_samples) * 0.65)
        base = (distance / 69.0) * 60.0 + (delay * 1.45)
        weather_penalty = (
            (delay * weather_factor * 3.05 * weather_multiplier)
            + ((congestion_weight - 1.0) * 17.0)
            + (max(0.0, delay - 7.0) ** 1.18) * 1.05
            + telemetry_risk * 3.0
        )
    else:
        # Balanceado: compromiso entre velocidad y robustez.
        base = (distance / 76.0) * 60.0 + delay
        weather_penalty = (delay * weather_factor * 1.6 * weather_multiplier) + ((congestion_weight - 1.0) * 7.0)

    risk_component = (uncertainty * 11.5) + (max(0.0, delay - 7.0) ** 1.08) + ((congestion_weight - 1.0) * 6.0)
    eco_component = (distance * 0.075) + (delay * 0.42) + ((congestion_weight - 1.0) * 5.2)
    operational_total = base + weather_penalty + (uncertainty * 1.8)
    routing_weight = (
        operational_total * float(objective_weights.get("time", 0.0))
        + risk_component * float(objective_weights.get("risk", 0.0))
        + eco_component * float(objective_weights.get("eco", 0.0))
    )
    return {
        "routing_weight": routing_weight,
        "base_minutes": base,
        "weather_penalty_minutes": weather_penalty,
        "total_minutes": operational_total,
        "uncertainty_index": uncertainty,
        "risk_component_minutes": risk_component,
        "eco_component_minutes": eco_component,
        "temporal_factor": temporal_factor,
    }


def route_weather_factor(base_weather_factor: float, path_edges):
    if not path_edges:
        return base_weather_factor
    avg_delay = sum(float(e.get("avg_delay_minutes", 0.0)) for e in path_edges) / len(path_edges)
    avg_distance = sum(float(e.get("distance_km", 0.0)) for e in path_edges) / len(path_edges)
    delay_component = min(avg_delay / 20.0, 1.0)
    distance_component = min(avg_distance / 450.0, 1.0)
    multiplier = 1.0 + (0.55 * delay_component) + (0.25 * distance_component)
    return min(base_weather_factor * multiplier, 3.2)


def build_weighted_graph(edges, profile: str, weather_factor: float, blocked_nodes=None, objective_weights=None, temporal_mode: str = "auto"):
    blocked = {str(node) for node in (blocked_nodes or []) if node}
    graph = defaultdict(list)
    for edge in edges:
        src = str(edge.get("src") or "")
        dst = str(edge.get("dst") or "")
        if not src or not dst:
            continue
        if src in blocked or dst in blocked:
            continue
        costs = edge_weight(
            edge,
            profile,
            weather_factor,
            objective_weights=objective_weights,
            temporal_mode=temporal_mode,
        )
        edge_with_costs = {
            **edge,
            "base_minutes": round(costs["base_minutes"], 2),
            "weather_penalty_minutes": round(costs["weather_penalty_minutes"], 2),
            "total_minutes": round(costs["total_minutes"], 2),
            "uncertainty_index": round(costs["uncertainty_index"], 3),
            "risk_component_minutes": round(costs["risk_component_minutes"], 2),
            "eco_component_minutes": round(costs["eco_component_minutes"], 2),
            "temporal_factor": round(costs["temporal_factor"], 3),
            "routing_weight_minutes": round(costs["routing_weight"], 2),
        }
        graph[src].append((dst, float(costs["routing_weight"]), edge_with_costs))
        reverse_edge = {
            "src": dst,
            "dst": src,
            "distance_km": edge.get("distance_km"),
            "avg_delay_minutes": edge.get("avg_delay_minutes"),
            "effective_avg_delay_minutes": edge.get("effective_avg_delay_minutes"),
            "live_avg_delay_minutes": edge.get("live_avg_delay_minutes"),
            "live_avg_speed_kmh": edge.get("live_avg_speed_kmh"),
            "live_sample_count": edge.get("live_sample_count"),
            "congestion_level": edge.get("congestion_level"),
            "base_minutes": round(costs["base_minutes"], 2),
            "weather_penalty_minutes": round(costs["weather_penalty_minutes"], 2),
            "total_minutes": round(costs["total_minutes"], 2),
            "uncertainty_index": round(costs["uncertainty_index"], 3),
            "risk_component_minutes": round(costs["risk_component_minutes"], 2),
            "eco_component_minutes": round(costs["eco_component_minutes"], 2),
            "temporal_factor": round(costs["temporal_factor"], 3),
            "routing_weight_minutes": round(costs["routing_weight"], 2),
        }
        graph[dst].append((src, float(costs["routing_weight"]), reverse_edge))
    return graph


def build_route_payload(
    profile,
    path_nodes,
    path_edges,
    weather_factor,
    avoided_nodes=None,
    rank=1,
    objective_weights=None,
    temporal_mode: str = "auto",
):
    total_distance = sum(float(e.get("distance_km") or 0.0) for e in path_edges)
    total_delay = sum(float(e.get("avg_delay_minutes") or 0.0) for e in path_edges)
    total_base = round(sum(float(e.get("base_minutes") or 0.0) for e in path_edges), 2)
    total_weather_penalty = round(sum(float(e.get("weather_penalty_minutes") or 0.0) for e in path_edges), 2)
    total_risk_component = round(sum(float(e.get("risk_component_minutes") or 0.0) for e in path_edges), 2)
    total_eco_component = round(sum(float(e.get("eco_component_minutes") or 0.0) for e in path_edges), 2)
    total_routing_weight = round(sum(float(e.get("routing_weight_minutes") or 0.0) for e in path_edges), 2)
    avg_uncertainty = (
        round(sum(float(e.get("uncertainty_index") or 0.0) for e in path_edges) / len(path_edges), 3) if path_edges else 0.0
    )
    effective_weather_factor = route_weather_factor(weather_factor, path_edges)
    if weather_factor > 0:
        scale = effective_weather_factor / weather_factor
        total_weather_penalty = round(total_weather_penalty * scale, 2)
    eta_minutes = round(total_base + total_weather_penalty, 2)
    reliability = round(_clamp(math.exp(-0.06 * max(0.0, total_risk_component)), 0.05, 0.99), 4)
    payload = {
        "rank": int(rank),
        "profile": profile,
        "path": path_nodes,
        "edges": path_edges,
        "avoided_nodes": sorted({str(n) for n in (avoided_nodes or []) if n}),
        "total_distance_km": round(total_distance, 2),
        "expected_delay_minutes": round(total_delay, 2),
        "base_travel_minutes": total_base,
        "weather_penalty_minutes": total_weather_penalty,
        "estimated_travel_minutes": eta_minutes,
        "weather_factor": round(weather_factor, 3),
        "weather_impact_level": weather_impact_level(weather_factor),
        "route_weather_factor": round(effective_weather_factor, 3),
        "route_weather_impact_level": weather_impact_level(effective_weather_factor),
        "objective_weights": {
            "time": round(float((objective_weights or {}).get("time", 1.0 / 3.0)), 4),
            "risk": round(float((objective_weights or {}).get("risk", 1.0 / 3.0)), 4),
            "eco": round(float((objective_weights or {}).get("eco", 1.0 / 3.0)), 4),
        },
        "temporal_mode": str(temporal_mode or "auto"),
        "routing_weight_minutes": total_routing_weight,
        "risk_score_minutes": total_risk_component,
        "eco_score_minutes": total_eco_component,
        "uncertainty_score": avg_uncertainty,
        "on_time_probability": reliability,
    }
    payload["explain"] = _route_explanation(payload, payload.get("objective_weights"))
    return payload


def find_candidate_routes(
    vertices,
    edges,
    source,
    target,
    profile,
    weather_factor,
    avoid_nodes=None,
    k=3,
    objective_weights=None,
    temporal_mode: str = "auto",
):
    blocked_nodes = {str(node) for node in (avoid_nodes or []) if node}
    blocked_nodes.discard(source)
    blocked_nodes.discard(target)
    vertex_ids = {str(v.get("id") or "") for v in vertices if v.get("id")}
    if source not in vertex_ids or target not in vertex_ids:
        return []
    weighted_graph = build_weighted_graph(
        edges,
        profile,
        weather_factor,
        blocked_nodes=blocked_nodes,
        objective_weights=objective_weights,
        temporal_mode=temporal_mode,
    )
    if source not in weighted_graph or target not in vertex_ids:
        return []

    max_candidates = max(1, min(int(k or 3), 6))
    max_expansions = 30000
    expansions = 0

    # Heap de busqueda de caminos simples (sin ciclos), ordenado por coste acumulado.
    heap = [(0.0, [source], [])]
    candidates = []
    seen_paths = set()

    while heap and len(candidates) < max_candidates and expansions < max_expansions:
        cost, path_nodes, path_edges = heapq.heappop(heap)
        expansions += 1
        current = path_nodes[-1]
        if current == target:
            signature = tuple(path_nodes)
            if signature in seen_paths:
                continue
            seen_paths.add(signature)
            candidates.append(
                build_route_payload(
                    profile,
                    path_nodes,
                    path_edges,
                    weather_factor,
                    blocked_nodes,
                    rank=len(candidates) + 1,
                    objective_weights=objective_weights,
                    temporal_mode=temporal_mode,
                )
            )
            continue

        for neighbor, edge_cost, edge_data in weighted_graph.get(current, []):
            if neighbor in path_nodes:
                continue
            next_nodes = path_nodes + [neighbor]
            next_edges = path_edges + [edge_data]
            heapq.heappush(heap, (cost + float(edge_cost), next_nodes, next_edges))

    return candidates


def dijkstra(vertices, edges, source, target, profile, weather_factor, avoid_nodes=None, objective_weights=None, temporal_mode: str = "auto"):
    # Motor de ruta minima: construye camino y metricas agregadas del trayecto.
    blocked_nodes = {str(node) for node in (avoid_nodes or []) if node}
    blocked_nodes.discard(source)
    blocked_nodes.discard(target)
    graph = build_weighted_graph(
        edges,
        profile,
        weather_factor,
        blocked_nodes=blocked_nodes,
        objective_weights=objective_weights,
        temporal_mode=temporal_mode,
    )

    dist = {node["id"]: math.inf for node in vertices if node["id"] not in blocked_nodes}
    prev = {}
    prev_edge = {}

    if source not in dist or target not in dist:
        return None

    dist[source] = 0.0
    pending = set(dist.keys())

    while pending:
        current = min(pending, key=lambda node: dist[node])
        pending.remove(current)

        if dist[current] == math.inf or current == target:
            break

        for neighbor, weight, edge in graph.get(current, []):
            alt = dist[current] + weight
            if alt < dist.get(neighbor, math.inf):
                dist[neighbor] = alt
                prev[neighbor] = current
                prev_edge[neighbor] = edge

    if dist.get(target, math.inf) == math.inf:
        return None

    path_nodes = [target]
    path_edges = []
    cursor = target
    while cursor in prev:
        path_edges.append(prev_edge[cursor])
        cursor = prev[cursor]
        path_nodes.append(cursor)

    path_nodes.reverse()
    path_edges.reverse()

    return build_route_payload(
        profile,
        path_nodes,
        path_edges,
        weather_factor,
        blocked_nodes,
        rank=1,
        objective_weights=objective_weights,
        temporal_mode=temporal_mode,
    )


def _edge_key(src: str, dst: str):
    if src <= dst:
        return src, dst
    return dst, src


def _congestion_level(avg_delay: float, avg_speed: float, sample_count: int):
    if sample_count <= 0:
        return "unknown"
    if avg_delay >= 14.0 or avg_speed <= 38.0:
        return "high"
    if avg_delay >= 8.0 or avg_speed <= 54.0:
        return "medium"
    return "low"


def apply_live_edge_telemetry(edges, latest_rows):
    # Mezcla delay estatico por tramo con telemetria reciente de flota.
    # Solo se consideran vehiculos con planned_origin/planned_destination.
    if not edges:
        return edges, {"vehicles_considered": 0, "live_samples": 0, "edges_with_live_samples": 0}

    stats = {}
    vehicles_considered = 0
    total_samples = 0

    for row in latest_rows or []:
        origin = row.get("planned_origin")
        destination = row.get("planned_destination")
        if not origin or not destination or origin == destination:
            continue
        vehicles_considered += 1
        key = _edge_key(str(origin), str(destination))
        bucket = stats.setdefault(
            key,
            {
                "sample_count": 0,
                "delay_sum": 0.0,
                "speed_sum": 0.0,
            },
        )
        bucket["sample_count"] += 1
        bucket["delay_sum"] += float(row.get("delay_minutes") or 0.0)
        bucket["speed_sum"] += float(row.get("speed_kmh") or 0.0)
        total_samples += 1

    enriched = []
    edges_with_live_samples = 0
    for edge in edges:
        src = str(edge.get("src") or "")
        dst = str(edge.get("dst") or "")
        key = _edge_key(src, dst)
        edge_stats = stats.get(key)
        static_delay = float(edge.get("avg_delay_minutes") or 0.0)

        if edge_stats and edge_stats["sample_count"] >= LIVE_EDGE_MIN_SAMPLES:
            sample_count = int(edge_stats["sample_count"])
            live_avg_delay = edge_stats["delay_sum"] / sample_count
            live_avg_speed = edge_stats["speed_sum"] / sample_count if sample_count > 0 else 0.0
            blend = min(LIVE_EDGE_BLEND_MAX, LIVE_EDGE_BLEND_MAX * min(sample_count / 3.0, 1.0))
            effective_delay = (static_delay * (1.0 - blend)) + (live_avg_delay * blend)
            congestion = _congestion_level(live_avg_delay, live_avg_speed, sample_count)
            edges_with_live_samples += 1
            enriched.append(
                {
                    **edge,
                    "effective_avg_delay_minutes": round(effective_delay, 2),
                    "live_avg_delay_minutes": round(live_avg_delay, 2),
                    "live_avg_speed_kmh": round(live_avg_speed, 2),
                    "live_sample_count": sample_count,
                    "congestion_level": congestion,
                }
            )
        else:
            enriched.append(
                {
                    **edge,
                    "effective_avg_delay_minutes": round(static_delay, 2),
                    "live_avg_delay_minutes": None,
                    "live_avg_speed_kmh": None,
                    "live_sample_count": 0,
                    "congestion_level": "unknown",
                }
            )

    summary = {
        "vehicles_considered": vehicles_considered,
        "live_samples": total_samples,
        "edges_with_live_samples": edges_with_live_samples,
    }
    return enriched, summary


def _criticality_weight(value: str):
    normalized = str(value or "").strip().lower()
    if normalized == "high":
        return 1.35
    if normalized == "medium":
        return 1.1
    return 1.0


def _congestion_rank(level: str):
    value = str(level or "unknown").lower()
    if value == "high":
        return 3
    if value == "medium":
        return 2
    if value == "low":
        return 1
    return 0


def _resolved_congestion_level(raw_level: str, effective_delay: float, total_minutes: float):
    normalized = str(raw_level or "unknown").strip().lower()
    if normalized in {"low", "medium", "high"}:
        return normalized
    # Fallback cuando no hay muestra live: estimacion por severidad operativa del tramo.
    if effective_delay >= 12.0 or total_minutes >= 300.0:
        return "high"
    if effective_delay >= 7.0 or total_minutes >= 180.0:
        return "medium"
    return "low"


def compute_network_insights(vertices, edges, profile="balanced", min_congestion="all", weather_factor=0.0):
    # Extrae rankings accionables para dashboard operacional.
    # 1) Cuellos de botella por tramo.
    # 2) Nodos criticos por centralidad operativa simple.
    if not vertices or not edges:
        return {"top_bottlenecks": [], "top_critical_nodes": []}

    vertex_index = {str(v.get("id") or ""): v for v in vertices}
    node_stats = {
        node_id: {
            "degree": 0,
            "incident_delay_sum": 0.0,
            "incident_distance_sum": 0.0,
            "high_congestion_edges": 0,
            "live_edges": 0,
            "total_edges": 0,
            "profile_minutes_sum": 0.0,
        }
        for node_id in vertex_index.keys()
    }

    bottlenecks = []
    min_rank = 0 if str(min_congestion or "all").lower() in {"all", "any"} else _congestion_rank(min_congestion)

    for edge in edges:
        src = str(edge.get("src") or "")
        dst = str(edge.get("dst") or "")
        if not src or not dst:
            continue
        distance = float(edge.get("distance_km") or 0.0)
        effective_delay = float(edge.get("effective_avg_delay_minutes", edge.get("avg_delay_minutes") or 0.0))
        live_samples = int(edge.get("live_sample_count") or 0)
        raw_congestion = str(edge.get("congestion_level") or "unknown")
        costs = edge_weight(edge, profile, weather_factor)
        total_minutes = float(costs.get("total_minutes") or 0.0)
        base_minutes = float(costs.get("base_minutes") or 0.0)
        weather_penalty = float(costs.get("weather_penalty_minutes") or 0.0)
        congestion = _resolved_congestion_level(raw_congestion, effective_delay, total_minutes)
        if _congestion_rank(congestion) < min_rank:
            continue
        congestion_multiplier = 1.0
        if congestion == "high":
            congestion_multiplier = 1.45
        elif congestion == "medium":
            congestion_multiplier = 1.2
        elif congestion == "low":
            congestion_multiplier = 1.05
        live_multiplier = 1.0 + min(0.35, live_samples * 0.07)
        distance_multiplier = 1.0 + min(0.65, distance / 750.0)
        profile_multiplier = 1.0
        if profile == "fastest":
            profile_multiplier = 1.08
        elif profile == "resilient":
            profile_multiplier = 0.94
        elif profile == "eco":
            profile_multiplier = 0.97
        elif profile == "low_risk":
            profile_multiplier = 0.9
        elif profile == "reliable":
            profile_multiplier = 0.88
        impact_score = total_minutes * congestion_multiplier * live_multiplier * distance_multiplier * profile_multiplier

        bottlenecks.append(
            {
                "src": src,
                "dst": dst,
                "distance_km": round(distance, 2),
                "effective_avg_delay_minutes": round(effective_delay, 2),
                "base_minutes": round(base_minutes, 2),
                "weather_penalty_minutes": round(weather_penalty, 2),
                "total_minutes": round(total_minutes, 2),
                "live_sample_count": live_samples,
                "congestion_level": congestion,
                "impact_score": round(impact_score, 3),
            }
        )

        for node_id in (src, dst):
            if node_id not in node_stats:
                node_stats[node_id] = {
                    "degree": 0,
                    "incident_delay_sum": 0.0,
                    "incident_distance_sum": 0.0,
                    "high_congestion_edges": 0,
                    "live_edges": 0,
                    "total_edges": 0,
                    "profile_minutes_sum": 0.0,
                }
            node_stats[node_id]["degree"] += 1
            node_stats[node_id]["incident_delay_sum"] += effective_delay
            node_stats[node_id]["incident_distance_sum"] += distance
            node_stats[node_id]["total_edges"] += 1
            node_stats[node_id]["profile_minutes_sum"] += float(total_minutes)
            if live_samples > 0:
                node_stats[node_id]["live_edges"] += 1
            if congestion == "high":
                node_stats[node_id]["high_congestion_edges"] += 1

    bottlenecks.sort(key=lambda e: (e["impact_score"], e["effective_avg_delay_minutes"]), reverse=True)

    critical_nodes = []
    for node_id, stats in node_stats.items():
        total_edges = max(1, int(stats["total_edges"]))
        avg_incident_delay = stats["incident_delay_sum"] / total_edges
        avg_incident_distance = stats["incident_distance_sum"] / total_edges
        avg_profile_minutes = stats["profile_minutes_sum"] / total_edges
        high_ratio = stats["high_congestion_edges"] / total_edges
        live_ratio = stats["live_edges"] / total_edges
        node_meta = vertex_index.get(node_id, {})
        node_criticality = str(node_meta.get("criticality") or "unknown")
        criticality_score = (
            (stats["degree"] * 1.3)
            + (avg_incident_delay * 0.9)
            + (avg_profile_minutes * 0.18)
            + (avg_incident_distance / 160.0)
            + (high_ratio * 5.0)
            + (live_ratio * 1.8)
        ) * _criticality_weight(node_criticality)
        critical_nodes.append(
            {
                "id": node_id,
                "name": node_meta.get("name") or node_meta.get("warehouse_name") or node_id,
                "criticality": node_criticality,
                "degree": int(stats["degree"]),
                "avg_incident_delay_minutes": round(avg_incident_delay, 2),
                "avg_profile_minutes": round(avg_profile_minutes, 2),
                "high_congestion_ratio": round(high_ratio, 3),
                "live_coverage_ratio": round(live_ratio, 3),
                "criticality_score": round(criticality_score, 3),
            }
        )

    critical_nodes.sort(
        key=lambda n: (n["criticality_score"], n["avg_incident_delay_minutes"], n["degree"]),
        reverse=True,
    )
    return {
        "top_bottlenecks": bottlenecks[:8],
        "top_critical_nodes": critical_nodes[:8],
        "filters": {
            "profile": profile,
            "min_congestion": min_congestion,
        },
    }


def build_vehicle_latest(events, warehouse_aliases, limit=200):
    # Reduce stream historico al ultimo estado por vehiculo.
    latest = {}
    for event in events:
        vid = event.get("vehicle_id")
        if not vid:
            continue
        current = latest.get(vid)
        if current is None or event["event_timestamp"] > current["event_timestamp"]:
            latest[vid] = event

    rows = []
    for event in latest.values():
        rows.append(
            {
                "vehicle_id": event.get("vehicle_id"),
                "warehouse_id": normalize_warehouse_id(event.get("warehouse_id"), warehouse_aliases),
                "route_id": event.get("route_id"),
                "delay_minutes": int(event.get("delay_minutes") or 0),
                "speed_kmh": float(event.get("speed_kmh") or 0.0),
                "latitude": float(event.get("latitude") or 0.0),
                "longitude": float(event.get("longitude") or 0.0),
                "event_time": to_iso(event["event_timestamp"]),
            }
        )

    rows.sort(key=lambda r: r["event_time"], reverse=True)
    return rows[:limit]


def build_vehicle_history(events, warehouse_aliases, vehicle_id=None, points=120):
    rows = []
    for event in events:
        if vehicle_id and event.get("vehicle_id") != vehicle_id:
            continue
        rows.append(
            {
                "vehicle_id": event.get("vehicle_id"),
                "warehouse_id": normalize_warehouse_id(event.get("warehouse_id"), warehouse_aliases),
                "route_id": event.get("route_id"),
                "delay_minutes": int(event.get("delay_minutes") or 0),
                "speed_kmh": float(event.get("speed_kmh") or 0.0),
                "latitude": float(event.get("latitude") or 0.0),
                "longitude": float(event.get("longitude") or 0.0),
                "event_time": to_iso(event["event_timestamp"]),
            }
        )

    rows.sort(key=lambda r: r["event_time"])
    return rows[-points:]


def load_vehicle_path_plans(warehouse_aliases):
    # Lee estado persistido del generador para conocer origen/destino planificados
    # por vehiculo, usado para coherencia de rutas en frontend.
    if not GPS_PATH_STATE_FILE.exists():
        return {}
    try:
        raw = json.loads(GPS_PATH_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}

    plans = {}
    for vehicle_id, item in raw.items():
        if not isinstance(item, dict):
            continue
        origin = normalize_warehouse_id(item.get("origin"), warehouse_aliases)
        destination = normalize_warehouse_id(item.get("destination"), warehouse_aliases)
        route_origin = normalize_warehouse_id(item.get("planned_route_origin"), warehouse_aliases)
        route_destination = normalize_warehouse_id(item.get("planned_route_destination"), warehouse_aliases)
        raw_route_nodes = item.get("planned_route_nodes")
        route_nodes = []
        if isinstance(raw_route_nodes, list):
            for node in raw_route_nodes:
                norm = normalize_warehouse_id(node, warehouse_aliases)
                if norm:
                    route_nodes.append(str(norm))
        route_label = item.get("planned_route_label")
        if not vehicle_id or not origin or not destination:
            continue
        if not route_nodes and route_origin and route_destination:
            route_nodes = [str(route_origin), str(route_destination)]
        if not route_origin and route_nodes:
            route_origin = route_nodes[0]
        if not route_destination and route_nodes:
            route_destination = route_nodes[-1]
        if not isinstance(route_label, str) or not route_label.strip():
            route_label = " -> ".join(route_nodes) if route_nodes else f"{origin} -> {destination}"
        plans[str(vehicle_id)] = {
            # Compatibilidad: tramo actual (nodo inmediato).
            "planned_origin": str(origin),
            "planned_destination": str(destination),
            # Ruta completa orientada (origen, intermedios, final).
            "planned_route_origin": str(route_origin or origin),
            "planned_route_destination": str(route_destination or destination),
            "planned_route_nodes": route_nodes,
            "planned_route_label": str(route_label),
        }
    return plans


def attach_vehicle_plans(rows, plans):
    # Enriquecimiento final de payload de vehiculos para frontend.
    if not rows:
        return rows
    for row in rows:
        vid = str(row.get("vehicle_id") or "")
        plan = plans.get(vid)
        if not plan:
            continue
        row["planned_origin"] = plan.get("planned_origin")
        row["planned_destination"] = plan.get("planned_destination")
        row["planned_route_origin"] = plan.get("planned_route_origin")
        row["planned_route_destination"] = plan.get("planned_route_destination")
        row["planned_route_nodes"] = plan.get("planned_route_nodes")
        row["planned_route_label"] = plan.get("planned_route_label")
    return rows


def build_overview(events, weather_rows):
    # KPIs globales calculados sobre stream historico disponible.
    weather_factor = compute_weather_factor(weather_rows)
    if not events:
        return {
            "vehicles_active": 0,
            "events_loaded": 0,
            "avg_delay_minutes": 0.0,
            "avg_speed_kmh": 0.0,
            "latest_event_time": None,
            "weather_factor": round(weather_factor, 3),
            "weather_impact_level": weather_impact_level(weather_factor),
        }

    unique_vehicles = {e.get("vehicle_id") for e in events if e.get("vehicle_id")}
    avg_delay = sum(float(e.get("delay_minutes") or 0) for e in events) / len(events)
    avg_speed = sum(float(e.get("speed_kmh") or 0) for e in events) / len(events)

    return {
        "vehicles_active": len(unique_vehicles),
        "events_loaded": len(events),
        "avg_delay_minutes": round(avg_delay, 2),
        "avg_speed_kmh": round(avg_speed, 2),
        "latest_event_time": to_iso(events[-1]["event_timestamp"]),
        "weather_factor": round(weather_factor, 3),
        "weather_impact_level": weather_impact_level(weather_factor),
    }


def build_overview_from_latest_vehicle_state(latest_rows, weather_rows):
    # Variante de KPIs usando snapshot latest-state (1 fila por vehiculo).
    weather_factor = compute_weather_factor(weather_rows)
    if not latest_rows:
        return {
            "vehicles_active": 0,
            "events_loaded": 0,
            "avg_delay_minutes": 0.0,
            "avg_speed_kmh": 0.0,
            "latest_event_time": None,
            "weather_factor": round(weather_factor, 3),
            "weather_impact_level": weather_impact_level(weather_factor),
        }

    rows_with_event = [v for v in latest_rows if v.get("event_time")]
    active_rows = [v for v in rows_with_event if str(v.get("vehicle_status") or "").lower() != "maintenance"]
    metric_rows = active_rows or rows_with_event
    if not metric_rows:
        metric_rows = latest_rows

    avg_delay = sum(float(v.get("delay_minutes") or 0.0) for v in metric_rows) / len(metric_rows)
    avg_speed = sum(float(v.get("speed_kmh") or 0.0) for v in metric_rows) / len(metric_rows)
    latest_event_time = max((v.get("event_time") for v in rows_with_event if v.get("event_time")), default=None)

    return {
        "vehicles_active": len(active_rows) if active_rows else len(rows_with_event),
        "events_loaded": len(rows_with_event),
        "avg_delay_minutes": round(avg_delay, 2),
        "avg_speed_kmh": round(avg_speed, 2),
        "latest_event_time": latest_event_time,
        "weather_factor": round(weather_factor, 3),
        "weather_impact_level": weather_impact_level(weather_factor),
    }


def _to_event_time(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return to_iso(value)
    parsed = parse_iso_utc(str(value))
    if parsed is None:
        return None
    return to_iso(parsed)


def load_vehicle_latest_from_cassandra(limit: int = 200):
    rows, _meta = load_vehicle_latest_from_cassandra_with_meta(limit=limit)
    return rows


def sync_vehicle_latest_to_cassandra(rows):
    # Persistencia de snapshot vehicular en Cassandra.
    meta = {
        "enabled": Cluster is not None,
        "status": "unknown",
        "error": None,
        "written": 0,
    }
    if Cluster is None:
        meta["status"] = "driver_missing"
        meta["error"] = "cassandra-driver no disponible"
        return meta

    if not rows:
        meta["status"] = "noop"
        return meta

    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        statement = (
            f"INSERT INTO {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} "
            f"(vehicle_id, warehouse_id, route_id, last_event_timestamp, delay_minutes, speed_kmh, latitude, longitude) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        written = 0
        for row in rows:
            event_dt = parse_iso_utc(row.get("event_time"))
            if event_dt is None:
                continue
            session.execute(
                statement,
                (
                    row.get("vehicle_id"),
                    row.get("warehouse_id"),
                    row.get("route_id"),
                    event_dt,
                    int(row.get("delay_minutes") or 0),
                    float(row.get("speed_kmh") or 0.0),
                    float(row.get("latitude") or 0.0),
                    float(row.get("longitude") or 0.0),
                ),
            )
            written += 1
        meta["status"] = "ok"
        meta["written"] = written
        return meta
    except Exception as exc:
        meta["status"] = "error"
        meta["error"] = str(exc)
        return meta
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def load_vehicle_latest_preferred(events, warehouse_aliases, limit: int = 200):
    # Estrategia de lectura de flota:
    # - Cassandra si hay datos frescos.
    # - Fallback nifi/input si Cassandra va atras.
    # - Sincronizacion de fallback hacia Cassandra para converger fuente principal.
    allowed_ids = load_allowed_vehicle_ids()
    status_map = load_vehicle_status_map()
    cassandra_rows, cassandra_meta = load_vehicle_latest_from_cassandra_with_meta(limit=limit)
    fallback_rows = build_vehicle_latest(events, warehouse_aliases, limit=limit)
    cassandra_rows = filter_rows_by_vehicle_ids(cassandra_rows, allowed_ids)
    fallback_rows = filter_rows_by_vehicle_ids(fallback_rows, allowed_ids)
    for row in cassandra_rows:
        row["vehicle_status"] = status_map.get(str(row.get("vehicle_id") or ""), "unknown")
    for row in fallback_rows:
        row["vehicle_status"] = status_map.get(str(row.get("vehicle_id") or ""), "unknown")

    # Asegura que los vehiculos en mantenimiento tambien aparezcan en tabla RT
    # aunque no emitan eventos recientes (estado informativo no operativo).
    present_ids = {str(r.get("vehicle_id") or "") for r in cassandra_rows + fallback_rows}
    maintenance_missing = sorted(
        vid for vid, status in status_map.items() if status == "maintenance" and vid not in present_ids
    )
    maintenance_rows = [
        {
            "vehicle_id": vid,
            "warehouse_id": "-",
            "route_id": "-",
            "delay_minutes": 0,
            "speed_kmh": 0.0,
            "latitude": None,
            "longitude": None,
            "event_time": None,
            "vehicle_status": "maintenance",
        }
        for vid in maintenance_missing
    ]

    def _latest_vehicle_dt(items):
        parsed = [parse_iso_utc(i.get("event_time")) for i in items if i.get("event_time")]
        parsed = [p for p in parsed if p is not None]
        return max(parsed) if parsed else None

    cass_latest = _latest_vehicle_dt(cassandra_rows)
    fallback_latest = _latest_vehicle_dt(fallback_rows)

    if cassandra_rows and not (fallback_latest and cass_latest and fallback_latest > cass_latest + timedelta(seconds=20)):
        for row in cassandra_rows:
            row["warehouse_id"] = normalize_warehouse_id(row.get("warehouse_id"), warehouse_aliases)
        return cassandra_rows + maintenance_rows, "cassandra", cassandra_meta

    if fallback_rows:
        sync_vehicle_latest_to_cassandra(fallback_rows)
        reloaded_rows, reloaded_meta = load_vehicle_latest_from_cassandra_with_meta(limit=limit)
        if reloaded_rows:
            for row in reloaded_rows:
                row["warehouse_id"] = normalize_warehouse_id(row.get("warehouse_id"), warehouse_aliases)
            for row in reloaded_rows:
                row["vehicle_status"] = status_map.get(str(row.get("vehicle_id") or ""), "unknown")
            return reloaded_rows + maintenance_rows, "cassandra", reloaded_meta

    return fallback_rows + maintenance_rows, "nifi_files", cassandra_meta


def _build_weather_event_id(row, idx):
    observed = str(row.get("observed_at") or "unknown").replace("-", "").replace(":", "").replace(".", "")
    source = str(row.get("source") or "unknown").replace(" ", "_")
    return f"snap_{observed}_{source}_{idx}"


def sync_weather_to_cassandra(rows):
    # Persistencia de clima reciente en Cassandra para consumo del dashboard.
    if Cluster is None or not rows:
        return
    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        stmt = (
            f"INSERT INTO {CASSANDRA_KEYSPACE}.{CASSANDRA_WEATHER_TABLE} "
            f"(bucket, weather_timestamp, weather_event_id, warehouse_id, temperature_c, precipitation_mm, wind_kmh, weather_code, source) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        for idx, row in enumerate(rows):
            observed_dt = parse_iso_utc(row.get("observed_at"))
            if observed_dt is None:
                continue
            session.execute(
                stmt,
                (
                    "all",
                    observed_dt,
                    _build_weather_event_id(row, idx),
                    str(row.get("warehouse_id") or "UNK"),
                    float(row.get("temperature_c") or 0.0),
                    float(row.get("precipitation_mm") or 0.0),
                    float(row.get("wind_kmh") or 0.0),
                    str(row.get("weather_code") or "unknown"),
                    str(row.get("source") or "snapshot"),
                ),
            )
    except Exception:
        pass
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def _insights_partition_bucket(now_utc: datetime):
    return now_utc.strftime("%Y%m%d")


def _should_persist_insights(profile: str, min_congestion: str, now_utc: datetime):
    key = f"{profile}:{min_congestion}"
    last = _INSIGHTS_LAST_PERSIST.get(key)
    if last is None:
        _INSIGHTS_LAST_PERSIST[key] = now_utc
        return True
    if (now_utc - last).total_seconds() >= INSIGHTS_PERSIST_INTERVAL_SECONDS:
        _INSIGHTS_LAST_PERSIST[key] = now_utc
        return True
    return False


def persist_network_insights_snapshot(network_insights):
    # Persistencia best-effort del ranking live para analisis historico.
    meta = {"status": "noop", "written": 0, "error": None}
    if Cluster is None:
        meta["status"] = "driver_missing"
        return meta
    if not network_insights:
        return meta

    filters = network_insights.get("filters") or {}
    profile = str(filters.get("profile") or "balanced")
    min_congestion = str(filters.get("min_congestion") or "all")
    now_utc = datetime.now(timezone.utc)
    if not _should_persist_insights(profile, min_congestion, now_utc):
        meta["status"] = "throttled"
        return meta

    bottlenecks = network_insights.get("top_bottlenecks") or []
    critical_nodes = network_insights.get("top_critical_nodes") or []
    if not bottlenecks and not critical_nodes:
        return meta

    bucket = _insights_partition_bucket(now_utc)
    cluster = None
    session = None
    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_INSIGHTS_TABLE} (
                bucket text,
                entity_type text,
                profile text,
                min_congestion text,
                snapshot_time timestamp,
                rank int,
                entity_id text,
                impact_score double,
                criticality_score double,
                effective_avg_delay_minutes double,
                total_minutes double,
                congestion_level text,
                live_sample_count int,
                PRIMARY KEY ((bucket, entity_type, profile, min_congestion), snapshot_time, rank, entity_id)
            ) WITH CLUSTERING ORDER BY (snapshot_time DESC, rank ASC, entity_id ASC)
            """
        )
        stmt = (
            f"INSERT INTO {CASSANDRA_KEYSPACE}.{CASSANDRA_INSIGHTS_TABLE} "
            f"(bucket, entity_type, profile, min_congestion, snapshot_time, rank, entity_id, impact_score, "
            f"criticality_score, effective_avg_delay_minutes, total_minutes, congestion_level, live_sample_count) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        written = 0
        for idx, edge in enumerate(bottlenecks, start=1):
            entity_id = f"{edge.get('src')}->{edge.get('dst')}"
            session.execute(
                stmt,
                (
                    bucket,
                    "edge",
                    profile,
                    min_congestion,
                    now_utc,
                    idx,
                    entity_id,
                    float(edge.get("impact_score") or 0.0),
                    None,
                    float(edge.get("effective_avg_delay_minutes") or 0.0),
                    float(edge.get("total_minutes") or 0.0),
                    str(edge.get("congestion_level") or "unknown"),
                    int(edge.get("live_sample_count") or 0),
                ),
            )
            written += 1
        for idx, node in enumerate(critical_nodes, start=1):
            entity_id = str(node.get("id") or f"node_{idx}")
            session.execute(
                stmt,
                (
                    bucket,
                    "node",
                    profile,
                    min_congestion,
                    now_utc,
                    idx,
                    entity_id,
                    None,
                    float(node.get("criticality_score") or 0.0),
                    float(node.get("avg_incident_delay_minutes") or 0.0),
                    float(node.get("avg_profile_minutes") or 0.0),
                    str(node.get("criticality") or "unknown"),
                    0,
                ),
            )
            written += 1
        meta["status"] = "ok"
        meta["written"] = written
        return meta
    except Exception as exc:
        meta["status"] = "error"
        meta["error"] = str(exc)
        return meta
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def load_network_insights_history(profile="balanced", min_congestion="all", snapshots=10):
    meta = {"status": "unknown", "error": None}
    if Cluster is None:
        return {"items": [], "meta": {"status": "driver_missing", "error": "cassandra-driver no disponible"}}
    snapshots = max(1, min(int(snapshots or 10), 48))
    today = datetime.now(timezone.utc)
    buckets = [
        _insights_partition_bucket(today),
        _insights_partition_bucket(today - timedelta(days=1)),
    ]
    cluster = None
    session = None
    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        query = (
            f"SELECT snapshot_time, entity_type, rank, entity_id, impact_score, criticality_score, "
            f"effective_avg_delay_minutes, total_minutes, congestion_level, live_sample_count "
            f"FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_INSIGHTS_TABLE} "
            f"WHERE bucket=%s AND entity_type=%s AND profile=%s AND min_congestion=%s LIMIT %s"
        )
        rows = []
        fetch_limit = max(80, snapshots * 12)
        for bucket in buckets:
            for entity_type in ("edge", "node"):
                part = session.execute(query, [bucket, entity_type, profile, min_congestion, fetch_limit])
                rows.extend(list(part))

        grouped = {}
        for row in rows:
            ts = getattr(row, "snapshot_time", None)
            if ts is None:
                continue
            ts_key = to_iso(ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc))
            entity_type = str(getattr(row, "entity_type", "") or "")
            rank = int(getattr(row, "rank", 0) or 0)
            if rank != 1:
                continue
            item = grouped.setdefault(
                ts_key,
                {
                    "snapshot_time": ts_key,
                    "top_bottleneck": None,
                    "top_node": None,
                },
            )
            if entity_type == "edge":
                item["top_bottleneck"] = {
                    "entity_id": getattr(row, "entity_id", None),
                    "impact_score": float(getattr(row, "impact_score", 0.0) or 0.0),
                    "effective_avg_delay_minutes": float(getattr(row, "effective_avg_delay_minutes", 0.0) or 0.0),
                    "total_minutes": float(getattr(row, "total_minutes", 0.0) or 0.0),
                    "congestion_level": getattr(row, "congestion_level", None),
                    "live_sample_count": int(getattr(row, "live_sample_count", 0) or 0),
                }
            if entity_type == "node":
                item["top_node"] = {
                    "entity_id": getattr(row, "entity_id", None),
                    "criticality_score": float(getattr(row, "criticality_score", 0.0) or 0.0),
                    "avg_incident_delay_minutes": float(getattr(row, "effective_avg_delay_minutes", 0.0) or 0.0),
                    "avg_profile_minutes": float(getattr(row, "total_minutes", 0.0) or 0.0),
                }
        items = sorted(grouped.values(), key=lambda x: x["snapshot_time"], reverse=True)[:snapshots]
        meta["status"] = "ok"
        return {"items": items, "meta": meta}
    except Exception as exc:
        meta["status"] = "error"
        meta["error"] = str(exc)
        return {"items": [], "meta": meta}
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def load_vehicle_latest_from_cassandra_with_meta(limit: int = 200):
    meta = {
        "enabled": Cluster is not None,
        "host": CASSANDRA_HOST,
        "port": CASSANDRA_PORT,
        "keyspace": CASSANDRA_KEYSPACE,
        "table": CASSANDRA_TABLE,
        "status": "unknown",
        "error": None,
        "row_count": 0,
        "fresh_row_count": 0,
        "stale_row_count": 0,
        "freshest_event_time": None,
        "max_age_seconds": VEHICLE_FRESHNESS_SECONDS,
    }
    if Cluster is None:
        meta["status"] = "driver_missing"
        meta["error"] = "cassandra-driver no disponible"
        return [], meta

    try:
        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        query = (
            f"SELECT vehicle_id, warehouse_id, route_id, last_event_timestamp, "
            f"delay_minutes, speed_kmh, latitude, longitude "
            f"FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}"
        )
        rows = session.execute(query)
        items = []
        for row in rows:
            event_time = _to_event_time(getattr(row, "last_event_timestamp", None))
            if not event_time:
                continue
            items.append(
                {
                    "vehicle_id": getattr(row, "vehicle_id", None),
                    "warehouse_id": getattr(row, "warehouse_id", None),
                    "route_id": getattr(row, "route_id", None),
                    "delay_minutes": int(getattr(row, "delay_minutes", 0) or 0),
                    "speed_kmh": float(getattr(row, "speed_kmh", 0.0) or 0.0),
                    "latitude": float(getattr(row, "latitude", 0.0) or 0.0),
                    "longitude": float(getattr(row, "longitude", 0.0) or 0.0),
                    "event_time": event_time,
                }
            )
        items.sort(key=lambda r: r.get("event_time") or "", reverse=True)
        now = datetime.now(timezone.utc)
        fresh_items = []
        stale_count = 0
        freshest_dt = None
        for item in items:
            dt = parse_iso_utc(item.get("event_time"))
            if dt is None:
                stale_count += 1
                continue
            if freshest_dt is None:
                freshest_dt = dt
            age_seconds = (now - dt).total_seconds()
            if age_seconds <= VEHICLE_FRESHNESS_SECONDS:
                fresh_items.append(item)
            else:
                stale_count += 1

        selected = fresh_items[:limit]
        meta["status"] = "ok"
        meta["row_count"] = len(selected)
        meta["fresh_row_count"] = len(selected)
        meta["stale_row_count"] = stale_count
        meta["freshest_event_time"] = to_iso(freshest_dt) if freshest_dt else None
        return selected, meta
    except Exception as exc:
        meta["status"] = "error"
        meta["error"] = str(exc)
        return [], meta
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def read_warehouses():
    # Carga y normaliza catalogo de almacenes (cast seguro de coordenadas).
    def _load():
        rows = []
        if not WAREHOUSES_PATH.exists():
            return rows

        with WAREHOUSES_PATH.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    row["latitude"] = float(row.get("latitude")) if row.get("latitude") else None
                except ValueError:
                    row["latitude"] = None
                try:
                    row["longitude"] = float(row.get("longitude")) if row.get("longitude") else None
                except ValueError:
                    row["longitude"] = None
                rows.append(row)
        return rows

    return _get_cached_static_data("warehouses", [WAREHOUSES_PATH], _load)


def warehouse_by_id(warehouses):
    return {w.get("warehouse_id"): w for w in warehouses if w.get("warehouse_id")}


def enrich_vertices_with_coords(vertices, warehouse_index):
    # Fusiona vertices logicos del grafo con coordenadas reales.
    enriched = []
    for vertex in vertices:
        warehouse = warehouse_index.get(vertex.get("id"), {})
        enriched.append(
            {
                **vertex,
                "latitude": warehouse.get("latitude"),
                "longitude": warehouse.get("longitude"),
                "warehouse_name": warehouse.get("warehouse_name", vertex.get("name")),
            }
        )
    return enriched


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "LogisticsDashboard/1.0"

    def _json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _text(self, payload, status=HTTPStatus.OK):
        body = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, request_path: str):
        # Servidor estatico minimo para frontend del dashboard.
        path = request_path.lstrip("/")
        if path == "":
            path = "index.html"

        candidate = (STATIC_DIR / path).resolve()
        if not str(candidate).startswith(str(STATIC_DIR.resolve())) or not candidate.exists() or not candidate.is_file():
            self._text("Not found", HTTPStatus.NOT_FOUND)
            return

        content_type = "text/plain; charset=utf-8"
        if candidate.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif candidate.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif candidate.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"

        body = candidate.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        # Router HTTP principal para health, API y recursos estaticos.
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/health":
            self._json({"status": "ok", "service": "dashboard"})
            return

        if parsed.path.startswith("/api/"):
            self._handle_api(parsed.path, query)
            return

        if parsed.path == "/":
            self._serve_static("/index.html")
            return

        self._serve_static(parsed.path)

    def do_POST(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path.startswith("/api/"):
            self._handle_api_post(parsed.path, query)
            return
        self._json({"error": "metodo no soportado"}, status=HTTPStatus.METHOD_NOT_ALLOWED)

    def _read_json_body(self):
        try:
            raw_len = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            raw_len = 0
        if raw_len <= 0:
            return {}
        try:
            body = self.rfile.read(raw_len)
            return json.loads(body.decode("utf-8"))
        except Exception:
            return {}

    def _handle_api_post(self, path, _query):
        if path == "/api/ml/retrain":
            payload = self._read_json_body()
            trigger = str(payload.get("trigger") or "manual_dashboard")
            started, state = start_retrain_if_idle(trigger=trigger)
            advice = get_retrain_advice_cached()
            persisted_state = load_persisted_retrain_state()
            model_info = build_retrain_model_info(state, persisted_state=persisted_state)
            schedule_info = build_retrain_schedule_info(persisted_state=persisted_state)
            status_code = HTTPStatus.ACCEPTED if started else HTTPStatus.CONFLICT
            self._json(
                {
                    "started": started,
                    "state": state,
                    "advice": advice,
                    "model_info": model_info,
                    "schedule_info": schedule_info,
                    "command": RETRAIN_COMMAND,
                },
                status=status_code,
            )
            return

        if path == "/api/platform/cleanup/trigger":
            payload = self._read_json_body()
            trigger = str(payload.get("trigger") or "manual_dashboard")
            started, reason, cleanup_state = start_disk_cleanup_if_allowed(trigger=trigger)
            self._json(
                {
                    "started": started,
                    "reason": reason,
                    "cleanup_state": cleanup_state,
                },
                status=HTTPStatus.ACCEPTED if started else HTTPStatus.CONFLICT,
            )
            return

        self._json({"error": "endpoint no soportado"}, status=HTTPStatus.NOT_FOUND)

    def _handle_api(self, path, query):
        # Cada request recompone contexto fresco (eventos/clima/grafo) para
        # priorizar consistencia de demo sobre cache agresiva.
        if path == "/api/ml/retrain/status":
            with _RETRAIN_LOCK:
                state = dict(_RETRAIN_STATE)
            state = merge_retrain_state(state)
            advice = get_retrain_advice_cached()
            persisted_state = load_persisted_retrain_state()
            model_info = build_retrain_model_info(state, persisted_state=persisted_state)
            schedule_info = build_retrain_schedule_info(persisted_state=persisted_state)
            self._json(
                {
                    "state": state,
                    "advice": advice,
                    "model_info": model_info,
                    "schedule_info": schedule_info,
                    "command": RETRAIN_COMMAND,
                }
            )
            return

        events = load_gps_events()
        weather_rows, weather_source, weather_cassandra_meta = load_weather_latest(limit=30)
        vertices, edges = load_route_graph()
        weather_factor = compute_weather_factor(weather_rows)
        impact_level = weather_impact_level(weather_factor)
        warehouses = read_warehouses()
        wh_index = warehouse_by_id(warehouses)
        vertices_with_coords = enrich_vertices_with_coords(vertices, wh_index)
        warehouse_aliases = build_warehouse_aliases(vertices)
        vehicle_plans = load_vehicle_path_plans(warehouse_aliases)
        latest_rows_for_network, latest_source_for_network, _latest_meta = load_vehicle_latest_preferred(
            events, warehouse_aliases, limit=200
        )
        attach_vehicle_plans(latest_rows_for_network, vehicle_plans)
        edges_with_live, live_edge_summary = apply_live_edge_telemetry(edges, latest_rows_for_network)
        insights_profile = (query.get("insights_profile") or ["balanced"])[0]
        insights_min_congestion = (query.get("insights_min_congestion") or ["all"])[0]
        network_insights = compute_network_insights(
            vertices_with_coords,
            edges_with_live,
            profile=insights_profile,
            min_congestion=insights_min_congestion,
            weather_factor=weather_factor,
        )

        if path == "/api/overview":
            # KPIs globales + metadatos de fuentes activas.
            overview = (
                build_overview_from_latest_vehicle_state(latest_rows_for_network, weather_rows)
                if latest_rows_for_network and latest_source_for_network == "cassandra"
                else build_overview(events, weather_rows)
            )
            platform_status = get_platform_status_cached()
            self._json({
                "overview": overview,
                "warehouses": warehouses,
                "warehouse_aliases": warehouse_aliases,
                "vehicle_source": latest_source_for_network,
                "weather_source": weather_source,
                "live_edge_summary": live_edge_summary,
                "platform_status": platform_status,
            })
            return

        if path == "/api/vehicles/latest":
            # Snapshot operativo de flota (1 estado por vehiculo).
            limit = int((query.get("limit") or ["200"])[0])
            latest_rows, source, _vehicle_meta = load_vehicle_latest_preferred(events, warehouse_aliases, limit=limit)
            attach_vehicle_plans(latest_rows, vehicle_plans)
            self._json({"items": latest_rows, "source": source})
            return

        if path == "/api/debug/sources":
            latest_cassandra, cassandra_meta = load_vehicle_latest_from_cassandra_with_meta(limit=200)
            latest_files = list_latest_files(GPS_DIR, "gps_*.jsonl", 1)
            latest_weather_files = list_latest_files(WEATHER_DIR, "*", 5)
            history_meta = load_network_insights_history(
                profile=insights_profile,
                min_congestion=insights_min_congestion,
                snapshots=3,
            ).get("meta", {})
            self._json(
                {
                    "vehicles": {
                        "active_source": "cassandra" if latest_cassandra else "nifi_files",
                        "cassandra": cassandra_meta,
                        "fallback_files": {
                            "gps_dir": str(GPS_DIR),
                            "latest_file": latest_files[0].name if latest_files else None,
                            "latest_file_mtime": (
                                datetime.fromtimestamp(latest_files[0].stat().st_mtime, tz=timezone.utc).isoformat()
                                if latest_files
                                else None
                            ),
                        },
                    },
                    "weather": {
                        "active_source": weather_source,
                        "cassandra": weather_cassandra_meta,
                        "weather_dir": str(WEATHER_DIR),
                        "rows_count": len(weather_rows),
                        "latest_observed_at": weather_rows[0].get("observed_at") if weather_rows else None,
                        "latest_files_seen": [p.name for p in latest_weather_files],
                    },
                    "graph": {
                        "vertices_count": len(vertices_with_coords),
                        "edges_count": len(edges),
                        "live_edge_summary": live_edge_summary,
                        "network_insights": network_insights,
                        "insights_history_meta": history_meta,
                        "source_files": {
                            "vertices": str(GRAPH_VERTICES_PATH),
                            "edges": str(GRAPH_EDGES_PATH),
                            "warehouses": str(WAREHOUSES_PATH),
                        },
                    },
                }
            )
            return

        if path == "/api/vehicles/history":
            # Historial temporal para la traza del vehiculo seleccionado.
            vehicle_id = (query.get("vehicle_id") or [None])[0]
            points = int((query.get("points") or ["120"])[0])
            history_rows = build_vehicle_history(events, warehouse_aliases, vehicle_id=vehicle_id, points=points)
            attach_vehicle_plans(history_rows, vehicle_plans)
            self._json({
                "items": history_rows
            })
            return

        if path == "/api/weather/latest":
            limit = int((query.get("limit") or ["20"])[0])
            self._json({"items": weather_rows[:limit], "source": weather_source})
            return

        if path == "/api/network/graph":
            # Grafo y metadatos usados por la vista de red logistica.
            persist_meta = persist_network_insights_snapshot(network_insights)
            self._json({
                "vertices": vertices_with_coords,
                "edges": edges_with_live,
                "warehouse_aliases": warehouse_aliases,
                "weather_factor": round(weather_factor, 3),
                "weather_impact_level": impact_level,
                "live_edge_summary": live_edge_summary,
                "network_insights": network_insights,
                "insights_persist": persist_meta,
            })
            return

        if path == "/api/network/insights/history":
            snapshots = int((query.get("snapshots") or ["10"])[0])
            history = load_network_insights_history(
                profile=insights_profile,
                min_congestion=insights_min_congestion,
                snapshots=snapshots,
            )
            self._json(
                {
                    "items": history.get("items", []),
                    "meta": history.get("meta", {}),
                    "filters": {
                        "profile": insights_profile,
                        "min_congestion": insights_min_congestion,
                        "snapshots": snapshots,
                    },
                }
            )
            return

        if path == "/api/network/best-route":
            # Recalculo de ruta minima segun perfil de optimizacion.
            source = (query.get("source") or [None])[0]
            target = (query.get("target") or [None])[0]
            profile = (query.get("profile") or ["balanced"])[0]
            objective_weights = normalize_objective_weights(
                (query.get("objective_time") or ["1"])[0],
                (query.get("objective_risk") or ["1"])[0],
                (query.get("objective_eco") or ["1"])[0],
            )
            temporal_mode = str((query.get("temporal_mode") or ["auto"])[0] or "auto").strip().lower()
            if temporal_mode not in {"auto", "peak", "offpeak", "night"}:
                temporal_mode = "auto"
            try:
                alternatives = int((query.get("alternatives") or ["3"])[0] or 3)
            except ValueError:
                alternatives = 3
            avoid_nodes_raw = (query.get("avoid_nodes") or [""])[0]
            avoid_nodes = [
                node.strip()
                for node in str(avoid_nodes_raw).split(",")
                if node and node.strip() and node.strip() not in {source, target}
            ]

            if not source or not target:
                self._json({"error": "source y target son obligatorios"}, status=HTTPStatus.BAD_REQUEST)
                return
            if source == target:
                trivial_route = {
                        "profile": profile,
                        "rank": 1,
                        "path": [source],
                        "edges": [],
                        "avoided_nodes": sorted(avoid_nodes),
                        "total_distance_km": 0.0,
                        "expected_delay_minutes": 0.0,
                        "base_travel_minutes": 0.0,
                        "weather_penalty_minutes": 0.0,
                        "estimated_travel_minutes": 0.0,
                        "weather_factor": round(weather_factor, 3),
                        "weather_impact_level": impact_level,
                        "route_weather_factor": round(weather_factor, 3),
                        "route_weather_impact_level": impact_level,
                        "objective_weights": objective_weights,
                        "temporal_mode": temporal_mode,
                        "routing_weight_minutes": 0.0,
                        "risk_score_minutes": 0.0,
                        "eco_score_minutes": 0.0,
                        "uncertainty_score": 0.0,
                        "on_time_probability": 1.0,
                        "explain": [
                            "Origen y destino coinciden.",
                            "No hay tramos intermedios que optimizar.",
                            "Coste total de ruta igual a cero.",
                        ],
                    }
                self._json({"route": trivial_route, "candidates": [trivial_route], "live_edge_summary": live_edge_summary})
                return

            candidates = find_candidate_routes(
                vertices,
                edges_with_live,
                source,
                target,
                profile,
                weather_factor,
                avoid_nodes=avoid_nodes,
                k=alternatives,
                objective_weights=objective_weights,
                temporal_mode=temporal_mode,
            )
            if not candidates:
                self._json({"error": "No se pudo calcular ruta para ese origen/destino"}, status=HTTPStatus.NOT_FOUND)
                return

            best_route = candidates[0]
            for idx, cand in enumerate(candidates, start=1):
                cand["rank"] = idx
                cand["delta_vs_best_minutes"] = round(float(cand["estimated_travel_minutes"]) - float(best_route["estimated_travel_minutes"]), 2)
                cand["delta_vs_best_weight"] = round(float(cand["routing_weight_minutes"]) - float(best_route["routing_weight_minutes"]), 2)

            self._json({"route": best_route, "candidates": candidates, "live_edge_summary": live_edge_summary})
            return

        self._json({"error": "endpoint no soportado"}, status=HTTPStatus.NOT_FOUND)


def main():
    if not STATIC_DIR.exists():
        raise RuntimeError(f"No existe el directorio static: {STATIC_DIR}")

    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print(f"Dashboard activo en http://{HOST}:{PORT}")
    print(f"Project root: {PROJECT_ROOT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
