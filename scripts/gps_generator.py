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
CITY_NEIGHBORS = {
    "MAD": ["BCN", "VAL", "SEV", "LIS", "ZAR", "BIO", "OPO", "ACO", "ALM", "VLL"],
    "BCN": ["MAD", "VAL", "ZAR", "VLL"],
    "VAL": ["MAD", "BCN", "ALM", "ZAR"],
    "SEV": ["MAD", "LIS", "ALM", "VLL"],
    "LIS": ["MAD", "OPO", "SEV"],
    "OPO": ["LIS", "MAD", "VLL"],
    "ZAR": ["MAD", "BCN", "VAL", "VLL"],
    "BIO": ["MAD", "ACO", "VLL"],
    "ACO": ["MAD", "BIO", "VLL"],
    "ALM": ["MAD", "VAL", "SEV"],
    "VLL": ["MAD", "BIO", "ACO", "ZAR", "OPO"],
}
WAREHOUSES = list(CITY_COORDS.keys())
ROUTES = ["R1", "R2", "R3", "R4", "R5"]
VEHICLES = [f"V{i}" for i in range(1, 14)]
EVENTS_PER_FILE = len(VEHICLES)


def pick_neighbor(city: str) -> str:
    neighbors = [n for n in (CITY_NEIGHBORS.get(city) or WAREHOUSES) if n != city]
    return random.choice(neighbors) if neighbors else city


def haversine_km(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    import math

    r = 6371.0
    d_lat = math.radians(b_lat - a_lat)
    d_lon = math.radians(b_lon - a_lon)
    lat1 = math.radians(a_lat)
    lat2 = math.radians(b_lat)
    h = (math.sin(d_lat / 2) ** 2) + (math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2)
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def init_vehicle_path_state():
    state = {}
    for vehicle_id in VEHICLES:
        origin = random.choice(WAREHOUSES)
        destination = pick_neighbor(origin)
        state[vehicle_id] = {
            "origin": origin,
            "destination": destination,
            "progress": random.uniform(0.02, 0.18),
            "route_id": random.choice(ROUTES),
        }
    return state


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
        origin = item.get("origin") if item.get("origin") in WAREHOUSES else random.choice(WAREHOUSES)
        destination = item.get("destination") if item.get("destination") in (CITY_NEIGHBORS.get(origin) or WAREHOUSES) else pick_neighbor(origin)
        progress = float(item.get("progress") or random.uniform(0.02, 0.18))
        route_id = item.get("route_id") if item.get("route_id") in ROUTES else random.choice(ROUTES)
        state[vehicle_id] = {
            "origin": origin,
            "destination": destination,
            "progress": clamp(progress, 0.0, 0.98),
            "route_id": route_id,
        }
    return state


def save_vehicle_path_state(state: dict):
    try:
        STATE_FILE.write_text(json.dumps(state, ensure_ascii=True), encoding="utf-8")
    except OSError:
        pass


VEHICLE_PATH_STATE = {}


def start_new_leg(path_state: dict):
    origin = path_state["destination"]
    destination = pick_neighbor(origin)
    path_state["origin"] = origin
    path_state["destination"] = destination
    path_state["progress"] = random.uniform(0.02, 0.12)
    path_state["route_id"] = random.choice(ROUTES)


def build_event(counter: int, vehicle_id: str) -> dict:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    path_state = VEHICLE_PATH_STATE[vehicle_id]
    origin = path_state["origin"]
    destination = path_state["destination"]
    progress = float(path_state["progress"])

    speed = round(max(8.0, random.gauss(62, 12)), 2)
    o_lat, o_lon = CITY_COORDS[origin]
    d_lat, d_lon = CITY_COORDS[destination]
    segment_km = max(20.0, haversine_km(o_lat, o_lon, d_lat, d_lon))
    simulated_hours = (SLEEP_SECONDS / 3600.0) * SIM_TIME_FACTOR
    progress_step = (speed * simulated_hours) / segment_km
    progress += clamp(progress_step + random.uniform(-0.0015, 0.003), 0.001, 0.028)

    if progress >= 1.0:
        start_new_leg(path_state)
        origin = path_state["origin"]
        destination = path_state["destination"]
        progress = float(path_state["progress"])
    else:
        path_state["progress"] = progress

    base_lat = o_lat + (d_lat - o_lat) * progress
    base_lon = o_lon + (d_lon - o_lon) * progress
    base_lat += random.uniform(-0.004, 0.004)
    base_lon += random.uniform(-0.004, 0.004)

    delay = max(0, int(random.gauss(6 + segment_km / 45.0, 4.0)))
    warehouse_id = destination if progress >= 0.55 else origin

    return {
        "event_id": f"gps_evt_{counter}",
        "vehicle_id": vehicle_id,
        "warehouse_id": warehouse_id,
        "route_id": path_state["route_id"],
        "event_type": "GPS",
        "latitude": round(base_lat, 6),
        "longitude": round(base_lon, 6),
        "delay_minutes": delay,
        "speed_kmh": speed,
        "event_time": now.isoformat().replace("+00:00", "Z"),
    }


def clamp(v: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(max_v, v))


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
