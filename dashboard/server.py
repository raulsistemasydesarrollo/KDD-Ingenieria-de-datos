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
import json
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

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


def list_latest_files(path: Path, pattern: str, limit: int):
    # Devuelve los ficheros mas recientes por fecha de modificacion.
    if not path.exists():
        return []
    files = sorted(path.glob(pattern), key=lambda f: f.stat().st_mtime, reverse=True)
    return files[:limit]


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


def edge_weight(edge, profile: str, weather_factor: float):
    # Calcula coste de arista para routing segun perfil y clima.
    # Retorna (total, base, penalizacion_meteo).
    distance = edge["distance_km"]
    delay = float(edge.get("effective_avg_delay_minutes", edge.get("avg_delay_minutes", 0.0)))

    if profile == "fastest":
        base = (distance / 88.0) * 60.0 + (delay * 0.9)
        penalty = delay * weather_factor * 1.4
        return base + penalty, base, penalty
    if profile == "resilient":
        base = (distance / 70.0) * 60.0 + (delay * 1.15)
        penalty = delay * weather_factor * 0.7
        return base + penalty, base, penalty
    base = (distance / 75.0) * 60.0 + delay
    penalty = delay * weather_factor * 1.9
    return base + penalty, base, penalty


def route_weather_factor(base_weather_factor: float, path_edges):
    if not path_edges:
        return base_weather_factor
    avg_delay = sum(float(e.get("avg_delay_minutes", 0.0)) for e in path_edges) / len(path_edges)
    avg_distance = sum(float(e.get("distance_km", 0.0)) for e in path_edges) / len(path_edges)
    delay_component = min(avg_delay / 20.0, 1.0)
    distance_component = min(avg_distance / 450.0, 1.0)
    multiplier = 1.0 + (0.55 * delay_component) + (0.25 * distance_component)
    return min(base_weather_factor * multiplier, 3.2)


def dijkstra(vertices, edges, source, target, profile, weather_factor):
    # Motor de ruta minima: construye camino y metricas agregadas del trayecto.
    graph = defaultdict(list)
    for edge in edges:
        total_weight, base_weight, weather_penalty = edge_weight(edge, profile, weather_factor)
        edge_with_costs = {
            **edge,
            "base_minutes": round(base_weight, 2),
            "weather_penalty_minutes": round(weather_penalty, 2),
            "total_minutes": round(total_weight, 2),
        }
        graph[edge["src"]].append((edge["dst"], total_weight, edge_with_costs))
        reverse_edge = {
            "src": edge["dst"],
            "dst": edge["src"],
            "distance_km": edge["distance_km"],
            "avg_delay_minutes": edge["avg_delay_minutes"],
            "base_minutes": round(base_weight, 2),
            "weather_penalty_minutes": round(weather_penalty, 2),
            "total_minutes": round(total_weight, 2),
        }
        graph[reverse_edge["src"]].append((reverse_edge["dst"], total_weight, reverse_edge))

    dist = {node["id"]: math.inf for node in vertices}
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

    total_distance = sum(e["distance_km"] for e in path_edges)
    total_delay = sum(e["avg_delay_minutes"] for e in path_edges)
    total_base = round(sum(float(e.get("base_minutes", 0.0)) for e in path_edges), 2)
    total_weather_penalty = round(sum(float(e.get("weather_penalty_minutes", 0.0)) for e in path_edges), 2)
    effective_weather_factor = route_weather_factor(weather_factor, path_edges)
    if weather_factor > 0:
        scale = effective_weather_factor / weather_factor
        total_weather_penalty = round(total_weather_penalty * scale, 2)
    eta_minutes = round(total_base + total_weather_penalty, 2)

    return {
        "profile": profile,
        "path": path_nodes,
        "edges": path_edges,
        "total_distance_km": round(total_distance, 2),
        "expected_delay_minutes": round(total_delay, 2),
        "base_travel_minutes": total_base,
        "weather_penalty_minutes": total_weather_penalty,
        "estimated_travel_minutes": eta_minutes,
        "weather_factor": round(weather_factor, 3),
        "weather_impact_level": weather_impact_level(weather_factor),
        "route_weather_factor": round(effective_weather_factor, 3),
        "route_weather_impact_level": weather_impact_level(effective_weather_factor),
    }


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
        total_minutes, base_minutes, weather_penalty = edge_weight(edge, profile, weather_factor)
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
        if not vehicle_id or not origin or not destination:
            continue
        plans[str(vehicle_id)] = {
            "planned_origin": str(origin),
            "planned_destination": str(destination),
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

    avg_delay = sum(float(v.get("delay_minutes") or 0.0) for v in latest_rows) / len(latest_rows)
    avg_speed = sum(float(v.get("speed_kmh") or 0.0) for v in latest_rows) / len(latest_rows)
    latest_event_time = max((v.get("event_time") for v in latest_rows if v.get("event_time")), default=None)

    return {
        "vehicles_active": len(latest_rows),
        "events_loaded": len(latest_rows),
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
    cassandra_rows, cassandra_meta = load_vehicle_latest_from_cassandra_with_meta(limit=limit)
    fallback_rows = build_vehicle_latest(events, warehouse_aliases, limit=limit)

    def _latest_vehicle_dt(items):
        parsed = [parse_iso_utc(i.get("event_time")) for i in items if i.get("event_time")]
        parsed = [p for p in parsed if p is not None]
        return max(parsed) if parsed else None

    cass_latest = _latest_vehicle_dt(cassandra_rows)
    fallback_latest = _latest_vehicle_dt(fallback_rows)

    if cassandra_rows and not (fallback_latest and cass_latest and fallback_latest > cass_latest + timedelta(seconds=20)):
        for row in cassandra_rows:
            row["warehouse_id"] = normalize_warehouse_id(row.get("warehouse_id"), warehouse_aliases)
        return cassandra_rows, "cassandra", cassandra_meta

    if fallback_rows:
        sync_vehicle_latest_to_cassandra(fallback_rows)
        reloaded_rows, reloaded_meta = load_vehicle_latest_from_cassandra_with_meta(limit=limit)
        if reloaded_rows:
            for row in reloaded_rows:
                row["warehouse_id"] = normalize_warehouse_id(row.get("warehouse_id"), warehouse_aliases)
            return reloaded_rows, "cassandra", reloaded_meta

    return fallback_rows, "nifi_files", cassandra_meta


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

    def _handle_api(self, path, query):
        # Cada request recompone contexto fresco (eventos/clima/grafo) para
        # priorizar consistencia de demo sobre cache agresiva.
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
            self._json({
                "overview": overview,
                "warehouses": warehouses,
                "warehouse_aliases": warehouse_aliases,
                "vehicle_source": latest_source_for_network,
                "weather_source": weather_source,
                "live_edge_summary": live_edge_summary,
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

            if not source or not target:
                self._json({"error": "source y target son obligatorios"}, status=HTTPStatus.BAD_REQUEST)
                return
            if source == target:
                self._json({
                    "route": {
                        "profile": profile,
                        "path": [source],
                        "edges": [],
                        "total_distance_km": 0.0,
                        "expected_delay_minutes": 0.0,
                        "base_travel_minutes": 0.0,
                        "weather_penalty_minutes": 0.0,
                        "estimated_travel_minutes": 0.0,
                        "weather_factor": round(weather_factor, 3),
                        "weather_impact_level": impact_level,
                        "route_weather_factor": round(weather_factor, 3),
                        "route_weather_impact_level": impact_level,
                    }
                })
                return

            route = dijkstra(vertices, edges_with_live, source, target, profile, weather_factor)
            if route is None:
                self._json({"error": "No se pudo calcular ruta para ese origen/destino"}, status=HTTPStatus.NOT_FOUND)
                return

            self._json({"route": route, "live_edge_summary": live_edge_summary})
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
