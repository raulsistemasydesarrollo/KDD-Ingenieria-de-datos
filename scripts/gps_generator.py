#!/usr/bin/env python3
"""
Generador continuo de eventos GPS simulados para la demo.

Comportamiento:
1) Mantiene estado de trayecto por vehiculo (origen, destino, progreso).
2) Avanza progresivamente por segmentos entre nodos de la red logistica.
3) Emite un evento por vehiculo en cada ciclo y guarda JSONL en /out.
4) Persiste el estado en .vehicle_path_state.json para continuidad.

Notas:
- El dashboard usa este estado para reforzar coherencia de ruta mostrada.
- Diseñado para movimiento suave y sin saltos bruscos.
"""

import csv
import json
import math
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


OUTPUT_DIR = Path("/out")
STATE_FILE = OUTPUT_DIR / ".vehicle_path_state.json"
GRAPH_EDGES_FILE = Path("/opt/generator/data/graph/edges.csv")
MASTER_VEHICLES_FILE = Path("/opt/generator/data/master/vehicles.csv")
SLEEP_SECONDS = 15
SIM_TIME_FACTOR = 6  # 15s reales ~ 1.5 min simulados para movimiento suave.
TARGET_ROUTE_COUNT = 20
ROUTE_REASSIGN_PROBABILITY = 0.35

CITY_COORDS = {
    "MAD": (40.4168, -3.7038),
    "BCN": (41.3874, 2.1686),
    "VAL": (39.4699, -0.3763),
    "SEV": (37.3891, -5.9845),
    "LIS": (38.7223, -9.1393),
    "OPO": (41.1579, -8.6291),
    "ZAR": (41.6488, -0.8891),
    "BIO": (43.2630, -2.9350),
    "ACO": (43.3623, -8.4115),
    "ALM": (36.8340, -2.4637),
    "VLL": (41.6523, -4.7245),
}
WAREHOUSES = list(CITY_COORDS.keys())
DEFAULT_VEHICLES = [f"V{i}" for i in range(1, 14)]
VEHICLES = DEFAULT_VEHICLES[:]
EVENTS_PER_FILE = len(VEHICLES)

DEFAULT_GRAPH_EDGES = [
    ("MAD", "BCN", 620.0, 9.0),
    ("MAD", "VAL", 355.0, 5.0),
    ("VAL", "BCN", 350.0, 6.0),
    ("SEV", "MAD", 530.0, 11.0),
    ("MAD", "ZAR", 320.0, 6.0),
    ("ZAR", "BCN", 300.0, 5.0),
    ("MAD", "LIS", 625.0, 12.0),
    ("LIS", "OPO", 313.0, 7.0),
    ("OPO", "MAD", 560.0, 10.0),
    ("BIO", "MAD", 400.0, 8.0),
    ("BIO", "ACO", 605.0, 9.0),
    ("ACO", "MAD", 590.0, 10.0),
    ("SEV", "ALM", 410.0, 9.0),
    ("VAL", "ALM", 345.0, 7.0),
    ("ALM", "MAD", 550.0, 11.0),
    ("SEV", "LIS", 460.0, 9.0),
    ("VAL", "ZAR", 310.0, 6.0),
    ("VLL", "MAD", 210.0, 5.0),
    ("VLL", "BIO", 280.0, 6.0),
    ("VLL", "ACO", 450.0, 7.0),
    ("VLL", "ZAR", 390.0, 6.0),
]
ROUTE_CATALOG = {}
ROUTES = []


def haversine_km(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    r = 6371.0
    d_lat = math.radians(b_lat - a_lat)
    d_lon = math.radians(b_lon - a_lon)
    lat1 = math.radians(a_lat)
    lat2 = math.radians(b_lat)
    h = (math.sin(d_lat / 2) ** 2) + (math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2)
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def clamp(v: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(max_v, v))


def _safe_float(value, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _safe_int(value, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def load_graph_edges():
    rows = []
    if GRAPH_EDGES_FILE.exists():
        try:
            with GRAPH_EDGES_FILE.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for item in reader:
                    src = (item.get("src") or "").strip().upper()
                    dst = (item.get("dst") or "").strip().upper()
                    if src not in CITY_COORDS or dst not in CITY_COORDS or src == dst:
                        continue
                    rows.append(
                        (
                            src,
                            dst,
                            max(30.0, _safe_float(item.get("distance_km"), 80.0)),
                            max(1.0, _safe_float(item.get("avg_delay_minutes"), 5.0)),
                        )
                    )
        except OSError:
            rows = []
    if rows:
        return rows
    return DEFAULT_GRAPH_EDGES[:]


def build_graph():
    graph = defaultdict(dict)
    for src, dst, distance_km, avg_delay in load_graph_edges():
        graph[src][dst] = {"distance_km": distance_km, "avg_delay_minutes": avg_delay}
        # Simetrico para modelar vias de doble sentido.
        if src not in graph[dst]:
            graph[dst][src] = {"distance_km": distance_km, "avg_delay_minutes": avg_delay}
    return graph


def shortest_path(graph, src: str, dst: str):
    dist = {src: 0.0}
    prev = {}
    visited = set()
    frontier = [(0.0, src)]
    while frontier:
        frontier.sort(key=lambda x: x[0])
        current_cost, node = frontier.pop(0)
        if node in visited:
            continue
        visited.add(node)
        if node == dst:
            break
        for nxt, attrs in graph.get(node, {}).items():
            if nxt in visited:
                continue
            edge_cost = float(attrs["distance_km"]) + float(attrs["avg_delay_minutes"]) * 9.0
            candidate = current_cost + edge_cost
            if candidate < dist.get(nxt, float("inf")):
                dist[nxt] = candidate
                prev[nxt] = node
                frontier.append((candidate, nxt))
    if dst not in prev and dst != src:
        return None
    path = [dst]
    while path[-1] != src:
        parent = prev.get(path[-1])
        if parent is None:
            return None
        path.append(parent)
    path.reverse()
    return path


def route_total_km(graph, path):
    total = 0.0
    for a, b in zip(path, path[1:]):
        edge = graph.get(a, {}).get(b)
        if edge:
            total += float(edge["distance_km"])
        else:
            total += haversine_km(*CITY_COORDS[a], *CITY_COORDS[b])
    return max(40.0, total)


def _route_signature(path):
    rev = tuple(reversed(path))
    direct = tuple(path)
    return min(direct, rev)


def build_route_catalog():
    graph = build_graph()
    nodes = sorted(set(CITY_COORDS.keys()) & set(graph.keys()))
    if len(nodes) < 2:
        # Fallback minimo para no parar el generador.
        return {
            "R01": {
                "path": ["MAD", "VAL", "BCN"],
                "cruise_speed_kmh": 72.0,
                "dwell_cycles": (1, 2),
                "weight": 1.0,
            }
        }

    node_degree = {n: len(graph.get(n, {})) for n in nodes}
    candidates = []
    seen = set()
    for src in nodes:
        for dst in nodes:
            if src == dst:
                continue
            path = shortest_path(graph, src, dst)
            if not path or len(path) < 2:
                continue
            signature = _route_signature(path)
            if signature in seen:
                continue
            seen.add(signature)
            total_km = route_total_km(graph, path)
            hops = max(1, len(path) - 1)
            endpoint_demand = node_degree.get(path[0], 1) + node_degree.get(path[-1], 1)
            centrality = sum(node_degree.get(p, 1) for p in path) / len(path)
            score = (endpoint_demand * 1.8) + (centrality * 1.2) + (hops * 0.9) + (total_km / 240.0)
            candidates.append(
                {
                    "path": path,
                    "total_km": total_km,
                    "score": score,
                    "hops": hops,
                    "endpoint_demand": endpoint_demand,
                }
            )

    candidates.sort(key=lambda c: (-c["score"], -c["hops"], c["total_km"], tuple(c["path"])))
    target = max(6, min(TARGET_ROUTE_COUNT, len(candidates)))
    multi_hop = [c for c in candidates if c["hops"] >= 2]
    one_hop = [c for c in candidates if c["hops"] == 1]

    selected = []
    target_multi = min(len(multi_hop), max(4, int(target * 0.65)))
    selected.extend(multi_hop[:target_multi])
    if len(selected) < target:
        selected.extend(one_hop[: target - len(selected)])
    if len(selected) < target:
        missing = target - len(selected)
        leftovers = [c for c in candidates if c not in selected]
        selected.extend(leftovers[:missing])
    if not selected:
        selected = [{"path": ["MAD", "VAL"], "total_km": 355.0, "score": 1.0, "hops": 1, "endpoint_demand": 2.0}]

    raw_weights = []
    catalog = {}
    for idx, item in enumerate(selected, start=1):
        path = item["path"]
        total_km = item["total_km"]
        hops = item["hops"]
        avg_leg_km = total_km / max(1, hops)
        cruise = clamp(61.0 + min(16.0, avg_leg_km / 25.0), 60.0, 82.0)
        dwell_min = 1
        dwell_max = 3 if len(path) >= 4 else 2
        weight = max(0.15, item["score"])
        route_id = f"R{idx:02d}"
        raw_weights.append(weight)
        catalog[route_id] = {
            "path": path,
            "cruise_speed_kmh": round(cruise, 1),
            "dwell_cycles": (dwell_min, dwell_max),
            "weight": weight,
        }

    total_weight = sum(raw_weights) or 1.0
    for cfg in catalog.values():
        cfg["weight"] = cfg["weight"] / total_weight
    return catalog


def initialize_route_catalog():
    global ROUTE_CATALOG, ROUTES
    ROUTE_CATALOG = build_route_catalog()
    ROUTES = sorted(ROUTE_CATALOG.keys())


def load_vehicle_fleet():
    candidates = [
        MASTER_VEHICLES_FILE,
        Path(__file__).resolve().parents[1] / "data" / "master" / "vehicles.csv",
    ]
    selected = []
    for path in candidates:
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for item in reader:
                    vehicle_id = (item.get("vehicle_id") or "").strip()
                    status = (item.get("status") or "active").strip().lower()
                    if not vehicle_id:
                        continue
                    selected.append((vehicle_id, status))
        except OSError:
            continue
        if selected:
            break

    if not selected:
        return DEFAULT_VEHICLES[:]

    active = [vehicle_id for vehicle_id, status in selected if status not in {"maintenance", "inactive", "retired"}]
    if active:
        return active
    fallback = [vehicle_id for vehicle_id, _ in selected]
    return fallback if fallback else DEFAULT_VEHICLES[:]


def initialize_vehicle_fleet():
    global VEHICLES, EVENTS_PER_FILE
    VEHICLES = load_vehicle_fleet()
    EVENTS_PER_FILE = len(VEHICLES)


def initialize_runtime():
    initialize_route_catalog()
    initialize_vehicle_fleet()


def choose_route_id(preferred_node: str | None = None, exclude_route_id: str | None = None) -> str:
    if not ROUTE_CATALOG:
        initialize_runtime()
    weighted_ids = []
    for route_id, cfg in ROUTE_CATALOG.items():
        if exclude_route_id and route_id == exclude_route_id:
            continue
        path = cfg["path"]
        if preferred_node and preferred_node not in path:
            continue
        weighted_ids.append((route_id, cfg["weight"]))
    if not weighted_ids:
        weighted_ids = [(route_id, cfg["weight"]) for route_id, cfg in ROUTE_CATALOG.items()]
    return random.choices([r for r, _ in weighted_ids], weights=[w for _, w in weighted_ids], k=1)[0]


def current_segment_nodes(path_state: dict) -> tuple[str, str]:
    if not ROUTE_CATALOG:
        initialize_runtime()
    route_cfg = ROUTE_CATALOG[path_state["route_id"]]
    path = route_cfg["path"]
    idx = int(path_state["segment_index"])
    direction = int(path_state["direction"])
    if direction >= 0:
        return path[idx], path[idx + 1]
    return path[idx + 1], path[idx]


def seed_state_for_route(route_id: str, preferred_node: str | None = None) -> dict:
    route_cfg = ROUTE_CATALOG[route_id]
    path = route_cfg["path"]
    direction = random.choice([1, -1])
    segment_index = 0
    if len(path) > 1:
        anchored = []
        if preferred_node:
            for idx in range(0, len(path) - 1):
                if path[idx] == preferred_node:
                    anchored.append((idx, 1))
                if path[idx + 1] == preferred_node:
                    anchored.append((idx, -1))
        if anchored:
            segment_index, direction = random.choice(anchored)
        else:
            segment_index = random.randrange(0, len(path) - 1)
    origin, destination = (
        (path[segment_index], path[segment_index + 1]) if direction >= 0 else (path[segment_index + 1], path[segment_index])
    )
    return {
        "route_id": route_id,
        "segment_index": segment_index,
        "direction": direction,
        "progress": random.uniform(0.05, 0.55),
        "dwell_remaining": random.randint(0, 1),
        # Compatibilidad con dashboard actual.
        "origin": origin,
        "destination": destination,
    }


def init_vehicle_path_state():
    state = {}
    seed_routes = ROUTES[:]
    random.shuffle(seed_routes)
    for idx, vehicle_id in enumerate(VEHICLES):
        route_id = seed_routes[idx] if idx < len(seed_routes) else choose_route_id()
        state[vehicle_id] = seed_state_for_route(route_id)
    return state


def maybe_reassign_route(path_state: dict, anchor_node: str) -> bool:
    if random.random() >= ROUTE_REASSIGN_PROBABILITY:
        return False
    current_route_id = path_state.get("route_id")
    next_route_id = choose_route_id(preferred_node=anchor_node, exclude_route_id=current_route_id)
    if next_route_id == current_route_id:
        return False
    reassigned = seed_state_for_route(next_route_id, preferred_node=anchor_node)
    path_state.update(reassigned)
    return True

def load_vehicle_path_state():
    if not STATE_FILE.exists():
        return init_vehicle_path_state()
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return init_vehicle_path_state()

    state = {}
    for vehicle_id in VEHICLES:
        item = raw.get(vehicle_id) if isinstance(raw, dict) else None
        if not isinstance(item, dict):
            item = {}
        route_id = item.get("route_id") if item.get("route_id") in ROUTES else choose_route_id()
        route_cfg = ROUTE_CATALOG[route_id]
        path = route_cfg["path"]
        last_segment = max(0, len(path) - 2)
        segment_index = clamp(_safe_int(item.get("segment_index"), 0), 0, last_segment)
        direction = 1 if _safe_int(item.get("direction"), 1) >= 0 else -1
        progress = clamp(float(item.get("progress") or random.uniform(0.05, 0.35)), 0.0, 0.98)
        dwell_remaining = clamp(_safe_int(item.get("dwell_remaining"), 0), 0, 8)
        loaded = {
            "route_id": route_id,
            "segment_index": int(segment_index),
            "direction": direction,
            "progress": progress,
            "dwell_remaining": int(dwell_remaining),
        }
        origin, destination = current_segment_nodes(loaded)
        loaded["origin"] = origin
        loaded["destination"] = destination
        state[vehicle_id] = loaded
    return state


def save_vehicle_path_state(state: dict):
    try:
        STATE_FILE.write_text(json.dumps(state, ensure_ascii=True), encoding="utf-8")
    except OSError:
        pass


VEHICLE_PATH_STATE = {}


def _refresh_leg_compat_fields(path_state: dict) -> None:
    origin, destination = current_segment_nodes(path_state)
    path_state["origin"] = origin
    path_state["destination"] = destination


def _advance_segment(path_state: dict) -> None:
    route_cfg = ROUTE_CATALOG[path_state["route_id"]]
    path = route_cfg["path"]
    last_segment = max(0, len(path) - 2)
    direction = int(path_state["direction"])
    idx = int(path_state["segment_index"])

    if direction >= 0:
        if idx < last_segment:
            idx += 1
        else:
            if maybe_reassign_route(path_state, path[-1]):
                return
            direction = -1
    else:
        if idx > 0:
            idx -= 1
        else:
            if maybe_reassign_route(path_state, path[0]):
                return
            direction = 1

    path_state["direction"] = direction
    path_state["segment_index"] = idx
    path_state["progress"] = random.uniform(0.01, 0.06)
    path_state["dwell_remaining"] = random.randint(route_cfg["dwell_cycles"][0], route_cfg["dwell_cycles"][1])
    _refresh_leg_compat_fields(path_state)


def _compute_delay_minutes(segment_km: float, speed: float, disruption: float, dwell_remaining: int) -> int:
    baseline = segment_km / 58.0
    disruption_penalty = disruption * random.uniform(2.0, 7.0)
    dwell_penalty = dwell_remaining * random.uniform(1.0, 2.5)
    noise = random.gauss(0.0, 1.5)
    return max(0, int(round(baseline + disruption_penalty + dwell_penalty + noise)))


def _compute_position(origin: str, destination: str, progress: float, speed: float, parked: bool) -> tuple[float, float]:
    o_lat, o_lon = CITY_COORDS[origin]
    d_lat, d_lon = CITY_COORDS[destination]
    base_lat = o_lat + (d_lat - o_lat) * progress
    base_lon = o_lon + (d_lon - o_lon) * progress

    # Ruido controlado: mas bajo en parada para que no "salte" el marcador.
    if parked:
        jitter = 0.0012
    else:
        jitter = clamp(0.0015 + speed / 85000.0, 0.0015, 0.0038)
    return (
        round(base_lat + random.uniform(-jitter, jitter), 6),
        round(base_lon + random.uniform(-jitter, jitter), 6),
    )


def build_event(counter: int, vehicle_id: str) -> dict:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    path_state = VEHICLE_PATH_STATE[vehicle_id]
    route_cfg = ROUTE_CATALOG[path_state["route_id"]]
    _refresh_leg_compat_fields(path_state)
    origin = path_state["origin"]
    destination = path_state["destination"]
    progress = float(path_state["progress"])

    o_lat, o_lon = CITY_COORDS[origin]
    d_lat, d_lon = CITY_COORDS[destination]
    segment_km = max(40.0, haversine_km(o_lat, o_lon, d_lat, d_lon))

    parked = int(path_state.get("dwell_remaining", 0)) > 0
    disruption = 1.0
    if not parked:
        # Microeventos de trafico/incidencia por tick.
        if random.random() < 0.06:
            disruption = random.uniform(0.58, 0.82)
        elif random.random() < 0.14:
            disruption = random.uniform(0.84, 0.95)

    if parked:
        speed = round(random.uniform(0.0, 8.0), 2)
        path_state["dwell_remaining"] = max(0, int(path_state.get("dwell_remaining", 0)) - 1)
    else:
        speed = round(max(12.0, random.gauss(route_cfg["cruise_speed_kmh"], 8.5) * disruption), 2)
        simulated_hours = (SLEEP_SECONDS / 3600.0) * SIM_TIME_FACTOR
        progress_step = (speed * simulated_hours) / segment_km
        progress += clamp(progress_step + random.uniform(-0.0022, 0.0024), 0.001, 0.055)
        if progress >= 1.0:
            _advance_segment(path_state)
            progress = float(path_state["progress"])
            origin = path_state["origin"]
            destination = path_state["destination"]
        else:
            path_state["progress"] = progress

    delay = _compute_delay_minutes(
        segment_km=segment_km,
        speed=speed,
        disruption=disruption,
        dwell_remaining=int(path_state.get("dwell_remaining", 0)),
    )
    latitude, longitude = _compute_position(origin, destination, progress, speed, parked)

    if parked:
        warehouse_id = origin
    else:
        warehouse_id = destination if progress >= 0.6 else origin

    return {
        "event_id": f"gps_evt_{counter}",
        "vehicle_id": vehicle_id,
        "warehouse_id": warehouse_id,
        "route_id": path_state["route_id"],
        "event_type": "GPS",
        "latitude": latitude,
        "longitude": longitude,
        "delay_minutes": delay,
        "speed_kmh": speed,
        "event_time": now.isoformat().replace("+00:00", "Z"),
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    initialize_runtime()
    global VEHICLE_PATH_STATE
    VEHICLE_PATH_STATE = load_vehicle_path_state()
    counter = int(time.time())
    print(f"GPS generator activo. Escribiendo en {OUTPUT_DIR}")
    while True:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_path = OUTPUT_DIR / f"gps_{ts}.jsonl"
        events = []
        # Un punto por vehiculo y ciclo para que la velocidad visual sea estable.
        shuffled_vehicles = VEHICLES[:]
        random.shuffle(shuffled_vehicles)
        for vehicle_id in shuffled_vehicles:
            counter += 1
            events.append(build_event(counter, vehicle_id))
        with file_path.open("w", encoding="utf-8") as handle:
            for event in events[:EVENTS_PER_FILE]:
                handle.write(json.dumps(event, ensure_ascii=True) + "\n")
        save_vehicle_path_state(VEHICLE_PATH_STATE)
        print(f"Generado {file_path.name} con {EVENTS_PER_FILE} eventos")
        time.sleep(SLEEP_SECONDS)


initialize_runtime()


if __name__ == "__main__":
    main()
