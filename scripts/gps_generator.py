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

import json
import math
import random
import time
from datetime import datetime, timezone
from pathlib import Path


OUTPUT_DIR = Path("/out")
STATE_FILE = OUTPUT_DIR / ".vehicle_path_state.json"
SLEEP_SECONDS = 15
SIM_TIME_FACTOR = 6  # 15s reales ~ 1.5 min simulados para movimiento suave.

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
VEHICLES = [f"V{i}" for i in range(1, 14)]
EVENTS_PER_FILE = len(VEHICLES)

# Rutas logisticas realistas: cada una define un corredor fijo con hubs intermedios.
ROUTE_CATALOG = {
    "R1": {
        "path": ["MAD", "VAL", "BCN"],
        "cruise_speed_kmh": 78.0,
        "dwell_cycles": (1, 2),
        "weight": 0.24,
    },
    "R2": {
        "path": ["MAD", "ZAR", "BCN"],
        "cruise_speed_kmh": 74.0,
        "dwell_cycles": (1, 2),
        "weight": 0.21,
    },
    "R3": {
        "path": ["SEV", "MAD", "VLL", "BIO", "ACO"],
        "cruise_speed_kmh": 70.0,
        "dwell_cycles": (1, 3),
        "weight": 0.22,
    },
    "R4": {
        "path": ["LIS", "OPO", "VLL", "MAD"],
        "cruise_speed_kmh": 69.0,
        "dwell_cycles": (1, 3),
        "weight": 0.17,
    },
    "R5": {
        "path": ["ALM", "VAL", "MAD", "SEV"],
        "cruise_speed_kmh": 67.0,
        "dwell_cycles": (1, 3),
        "weight": 0.16,
    },
}
ROUTES = list(ROUTE_CATALOG.keys())


def haversine_km(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    r = 6371.0
    d_lat = math.radians(b_lat - a_lat)
    d_lon = math.radians(b_lon - a_lon)
    lat1 = math.radians(a_lat)
    lat2 = math.radians(b_lat)
    h = (math.sin(d_lat / 2) ** 2) + (math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2)
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def choose_route_id() -> str:
    weighted_ids = [(route_id, cfg["weight"]) for route_id, cfg in ROUTE_CATALOG.items()]
    return random.choices([r for r, _ in weighted_ids], weights=[w for _, w in weighted_ids], k=1)[0]


def current_segment_nodes(path_state: dict) -> tuple[str, str]:
    route_cfg = ROUTE_CATALOG[path_state["route_id"]]
    path = route_cfg["path"]
    idx = int(path_state["segment_index"])
    direction = int(path_state["direction"])
    if direction >= 0:
        return path[idx], path[idx + 1]
    return path[idx + 1], path[idx]


def seed_state_for_route(route_id: str) -> dict:
    route_cfg = ROUTE_CATALOG[route_id]
    path = route_cfg["path"]
    direction = random.choice([1, -1])
    if len(path) <= 1:
        segment_index = 0
    elif direction >= 0:
        segment_index = random.randrange(0, len(path) - 1)
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
    for vehicle_id in VEHICLES:
        state[vehicle_id] = seed_state_for_route(choose_route_id())
    return state


def clamp(v: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(max_v, v))


def _safe_int(value, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


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
            direction = -1
    else:
        if idx > 0:
            idx -= 1
        else:
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


if __name__ == "__main__":
    main()
