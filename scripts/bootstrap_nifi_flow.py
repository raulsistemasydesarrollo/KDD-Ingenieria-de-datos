#!/usr/bin/env python3
"""
Bootstrap automatico de NiFi por API REST.

Que hace:
1) Autentica contra NiFi y obtiene token.
2) Crea/recupera el Process Group principal.
3) Construye subgrupos y procesadores de GPS y Clima.
4) Configura conexiones, relaciones y controller services Kafka.
5) Arranca componentes y deja el flujo operativo.

Uso:
- Invocado por scripts/bootstrap_nifi_flow.sh.
- El nombre del PG se controla con NIFI_PG_NAME.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


NIFI_URL = os.environ.get("NIFI_URL", "https://localhost:8443/nifi-api")
NIFI_USER = os.environ.get("NIFI_USER", "admin")
NIFI_PASS = os.environ.get("NIFI_PASS", "adminadminadmin")
PROCESS_GROUP_NAME = os.environ.get("NIFI_PG_NAME", "kdd_ingestion_auto")


def req(method, path, token=None, payload=None, form=None, expect_json=True):
    # Cliente HTTP basico para NiFi API (soporta JSON y form-urlencoded).
    url = f"{NIFI_URL}{path}"
    headers = {}
    data = None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    if form is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = urllib.parse.urlencode(form).encode("utf-8")
    request = urllib.request.Request(url, method=method, headers=headers, data=data)
    context = None
    if url.startswith("https://"):
        import ssl
        context = ssl._create_unverified_context()
    with urllib.request.urlopen(request, context=context, timeout=30) as response:
        body = response.read()
        if not body:
            return {}
        text = body.decode("utf-8")
        if not expect_json:
            return text
        return json.loads(text)


def get_token():
    # Autenticacion username/password -> bearer token.
    token = req(
        "POST",
        "/access/token",
        form={"username": NIFI_USER, "password": NIFI_PASS},
        expect_json=False,
    )
    if not token or not isinstance(token, str):
        raise RuntimeError("No se pudo obtener token de NiFi.")
    return token


def get_root_pg(token):
    # Recupera el process group root y su flow embebido.
    data = req("GET", "/flow/process-groups/root", token=token)
    return data["processGroupFlow"]["id"], data


def find_pg_by_name(root_flow):
    # Busca PG existente por nombre para idempotencia del bootstrap.
    for pg in root_flow["processGroupFlow"]["flow"].get("processGroups", []):
        component = pg.get("component", {})
        if component.get("name") == PROCESS_GROUP_NAME:
            return component.get("id")
    return None


def create_process_group(token, parent_pg_id, name, x, y):
    # Crea process group en coordenadas de canvas para legibilidad visual.
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "position": {"x": float(x), "y": float(y)},
        },
    }
    data = req("POST", f"/process-groups/{parent_pg_id}/process-groups", token=token, payload=payload)
    return data["id"]


def create_processor(token, pg_id, name, ptype, x, y):
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": ptype,
            "position": {"x": float(x), "y": float(y)},
        },
    }
    data = req("POST", f"/process-groups/{pg_id}/processors", token=token, payload=payload)
    return data["id"]


def create_controller_service(token, pg_id, name, stype):
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": stype,
        },
    }
    data = req("POST", f"/process-groups/{pg_id}/controller-services", token=token, payload=payload)
    return data["id"]


def get_controller_service(token, service_id):
    return req("GET", f"/controller-services/{service_id}", token=token)


def update_controller_service(token, service_id, properties):
    current = get_controller_service(token, service_id)
    component = current["component"]
    config = component.get("properties") or {}
    config.update(properties)
    payload = {
        "revision": {"version": current["revision"]["version"]},
        "component": {
            "id": service_id,
            "name": component["name"],
            "properties": config,
        },
    }
    req("PUT", f"/controller-services/{service_id}", token=token, payload=payload)


def enable_controller_service(token, service_id):
    current = get_controller_service(token, service_id)
    payload = {
        "revision": {"version": current["revision"]["version"]},
        "state": "ENABLED",
        "disconnectedNodeAcknowledged": False,
    }
    req("PUT", f"/controller-services/{service_id}/run-status", token=token, payload=payload)


def get_processor(token, proc_id):
    return req("GET", f"/processors/{proc_id}", token=token)


def update_processor(token, proc_id, properties=None, scheduling_strategy=None, scheduling_period=None):
    current = get_processor(token, proc_id)
    component = current["component"]
    config = component.get("config", {})
    if properties:
        merged = dict(config.get("properties") or {})
        merged.update(properties)
        config["properties"] = merged
    if scheduling_strategy:
        config["schedulingStrategy"] = scheduling_strategy
    if scheduling_period:
        config["schedulingPeriod"] = scheduling_period
    payload = {
        "revision": {"version": current["revision"]["version"]},
        "component": {
            "id": proc_id,
            "name": component["name"],
            "config": config,
        },
    }
    req("PUT", f"/processors/{proc_id}", token=token, payload=payload)


def create_connection(token, pg_id, src_id, dst_id, relationships):
    current = get_processor(token, src_id)
    available = {rel["name"] for rel in current["component"].get("relationships", [])}
    selected = [rel for rel in relationships if rel in available]
    if not selected:
        return []

    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "type": "PROCESSOR", "groupId": pg_id},
            "destination": {"id": dst_id, "type": "PROCESSOR", "groupId": pg_id},
            "selectedRelationships": selected,
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration": "0 sec",
        },
    }
    req("POST", f"/process-groups/{pg_id}/connections", token=token, payload=payload)
    return selected


def set_auto_terminated(token, proc_id, used_source_relationships):
    current = get_processor(token, proc_id)
    component = current["component"]
    relationships = component.get("relationships", [])
    all_rel_names = [rel["name"] for rel in relationships]
    auto = [name for name in all_rel_names if name not in used_source_relationships]
    config = component.get("config", {})
    config["autoTerminatedRelationships"] = auto
    payload = {
        "revision": {"version": current["revision"]["version"]},
        "component": {
            "id": proc_id,
            "name": component["name"],
            "config": config,
        },
    }
    req("PUT", f"/processors/{proc_id}", token=token, payload=payload)


def start_processor(token, proc_id):
    current = get_processor(token, proc_id)
    payload = {
        "revision": {"version": current["revision"]["version"]},
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False,
    }
    req("PUT", f"/processors/{proc_id}/run-status", token=token, payload=payload)


def main():
    token = get_token()
    root_id, root_flow = get_root_pg(token)
    existing_pg_id = find_pg_by_name(root_flow)
    if existing_pg_id:
        print(f"Process Group ya existente: {PROCESS_GROUP_NAME} ({existing_pg_id}). Se mantiene sin recrear.")
        print("Flujos activos: GPS->Kafka raw/filtered y Weather API->Kafka raw/filtered")
        return

    pg_id = create_process_group(token, root_id, PROCESS_GROUP_NAME, 120, 120)
    gps_pg_id = create_process_group(token, pg_id, "gps_ingestion", 80, 100)
    weather_pg_id = create_process_group(token, pg_id, "weather_ingestion", 80, 680)
    kafka_service_id = create_controller_service(
        token, pg_id, "kafka3_connection_service", "org.apache.nifi.kafka.service.Kafka3ConnectionService"
    )
    update_controller_service(token, kafka_service_id, {"bootstrap.servers": "kafka:9092"})
    enable_controller_service(token, kafka_service_id)

    # GPS flow
    p = {}
    p["list_gps"] = create_processor(
        token, gps_pg_id, "list_gps_files", "org.apache.nifi.processors.standard.ListFile", 80, 120
    )
    p["fetch_gps"] = create_processor(
        token, gps_pg_id, "fetch_gps_file", "org.apache.nifi.processors.standard.FetchFile", 320, 120
    )
    p["split_gps"] = create_processor(
        token, gps_pg_id, "split_gps_lines", "org.apache.nifi.processors.standard.SplitText", 560, 120
    )
    p["eval_gps"] = create_processor(
        token, gps_pg_id, "extract_gps_fields", "org.apache.nifi.processors.standard.EvaluateJsonPath", 800, 120
    )
    p["update_gps_archive_name"] = create_processor(
        token, gps_pg_id, "update_gps_archive_name", "org.apache.nifi.processors.attributes.UpdateAttribute", 1040, -40
    )
    p["route_gps"] = create_processor(
        token, gps_pg_id, "route_gps_filtered", "org.apache.nifi.processors.standard.RouteOnAttribute", 1040, 120
    )
    p["kafka_raw"] = create_processor(
        token, gps_pg_id, "publish_gps_raw", "org.apache.nifi.kafka.processors.PublishKafka", 1280, 60
    )
    p["archive_gps_raw"] = create_processor(
        token, gps_pg_id, "archive_gps_raw_local", "org.apache.nifi.processors.standard.PutFile", 1280, -70
    )
    p["kafka_filtered"] = create_processor(
        token, gps_pg_id, "publish_gps_filtered", "org.apache.nifi.kafka.processors.PublishKafka", 1280, 190
    )
    p["gps_failure_sink"] = create_processor(
        token, gps_pg_id, "gps_failure_sink", "org.apache.nifi.processors.standard.PutFile", 1520, 120
    )

    # Weather flow
    p["tick_weather"] = create_processor(
        token, weather_pg_id, "tick_weather", "org.apache.nifi.processors.standard.GenerateFlowFile", 80, 120
    )
    p["invoke_weather"] = create_processor(
        token, weather_pg_id, "invoke_weather_api", "org.apache.nifi.processors.standard.InvokeHTTP", 320, 120
    )
    p["extract_weather"] = create_processor(
        token, weather_pg_id, "extract_weather_fields", "org.apache.nifi.processors.standard.EvaluateJsonPath", 560, 120
    )
    p["update_weather"] = create_processor(
        token, weather_pg_id, "update_weather_attrs", "org.apache.nifi.processors.attributes.UpdateAttribute", 800, 120
    )
    p["attrs_weather"] = create_processor(
        token, weather_pg_id, "attrs_to_weather_json", "org.apache.nifi.processors.standard.AttributesToJSON", 1040, 120
    )
    p["kafka_weather_raw"] = create_processor(
        token, weather_pg_id, "publish_weather_raw", "org.apache.nifi.kafka.processors.PublishKafka", 1280, 60
    )
    p["archive_weather_raw"] = create_processor(
        token, weather_pg_id, "archive_weather_raw_local", "org.apache.nifi.processors.standard.PutFile", 1280, -70
    )
    p["kafka_weather_filtered"] = create_processor(
        token, weather_pg_id, "publish_weather_filtered", "org.apache.nifi.kafka.processors.PublishKafka", 1280, 190
    )
    p["weather_failure_sink"] = create_processor(
        token, weather_pg_id, "weather_failure_sink", "org.apache.nifi.processors.standard.PutFile", 1520, 120
    )

    update_processor(
        token,
        p["list_gps"],
        properties={"Input Directory": "/opt/nifi/nifi-current/input", "File Filter": ".*\\.jsonl"},
        scheduling_strategy="TIMER_DRIVEN",
        scheduling_period="15 sec",
    )
    update_processor(
        token,
        p["split_gps"],
        properties={"Line Split Count": "1", "Remove Trailing Newlines": "true"},
    )
    update_processor(
        token,
        p["eval_gps"],
        properties={
            "Destination": "flowfile-attribute",
            "vehicle_id": "$.vehicle_id",
            "warehouse_id": "$.warehouse_id",
            "event_type": "$.event_type",
            "delay_minutes": "$.delay_minutes",
        },
    )
    update_processor(
        token,
        p["update_gps_archive_name"],
        properties={
            # SplitText conserva el nombre original; añadimos fragment/index + UUID para no pisar ficheros.
            "filename": "${filename}_${fragment.index}_${UUID()}.jsonl",
        },
    )
    update_processor(token, p["route_gps"], properties={"filtered": "${delay_minutes:toNumber():ge(5)}"})
    update_processor(
        token,
        p["kafka_raw"],
        properties={
            "Kafka Connection Service": kafka_service_id,
            "Topic Name": "transport.raw",
            "Transactions Enabled": "false",
            "acks": "1",
        },
    )
    update_processor(
        token,
        p["archive_gps_raw"],
        properties={"Directory": "/opt/nifi/nifi-current/raw-archive/gps", "Conflict Resolution Strategy": "replace"},
    )
    update_processor(
        token,
        p["kafka_filtered"],
        properties={
            "Kafka Connection Service": kafka_service_id,
            "Topic Name": "transport.filtered",
            "Transactions Enabled": "false",
            "acks": "1",
        },
    )
    update_processor(
        token,
        p["gps_failure_sink"],
        properties={"Directory": "/opt/nifi/nifi-current/raw-archive/failures/gps", "Conflict Resolution Strategy": "replace"},
    )

    update_processor(
        token,
        p["tick_weather"],
        scheduling_strategy="TIMER_DRIVEN",
        scheduling_period="60 sec",
    )
    update_processor(
        token,
        p["invoke_weather"],
        properties={
            "HTTP Method": "GET",
            "HTTP URL": "https://api.open-meteo.com/v1/forecast?latitude=40.4168&longitude=-3.7038&current=temperature_2m,precipitation,wind_speed_10m,weather_code",
        },
    )
    update_processor(
        token,
        p["extract_weather"],
        properties={
            "Destination": "flowfile-attribute",
            "temperature_c": "$.current.temperature_2m",
            "precipitation_mm": "$.current.precipitation",
            "wind_kmh": "$.current.wind_speed_10m",
            "weather_code": "$.current.weather_code",
        },
    )
    update_processor(
        token,
        p["update_weather"],
        properties={
            "weather_event_id": "${UUID()}",
            "warehouse_id": "WH1",
            "source": "open-meteo",
            "observation_time": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss'Z'\", \"UTC\")}",
        },
    )
    update_processor(
        token,
        p["attrs_weather"],
        properties={
            "Destination": "flowfile-content",
            "Attributes List": "weather_event_id,warehouse_id,temperature_c,precipitation_mm,wind_kmh,weather_code,source,observation_time",
            "Include Core Attributes": "false",
        },
    )
    update_processor(
        token,
        p["kafka_weather_raw"],
        properties={
            "Kafka Connection Service": kafka_service_id,
            "Topic Name": "transport.weather.raw",
            "Transactions Enabled": "false",
            "acks": "1",
        },
    )
    update_processor(
        token,
        p["archive_weather_raw"],
        properties={"Directory": "/opt/nifi/nifi-current/raw-archive/weather", "Conflict Resolution Strategy": "replace"},
    )
    update_processor(
        token,
        p["kafka_weather_filtered"],
        properties={
            "Kafka Connection Service": kafka_service_id,
            "Topic Name": "transport.weather.filtered",
            "Transactions Enabled": "false",
            "acks": "1",
        },
    )
    update_processor(
        token,
        p["weather_failure_sink"],
        properties={"Directory": "/opt/nifi/nifi-current/raw-archive/failures/weather", "Conflict Resolution Strategy": "replace"},
    )

    connections = [
        (p["list_gps"], p["fetch_gps"], ["success"]),
        (p["fetch_gps"], p["split_gps"], ["success"]),
        (p["split_gps"], p["eval_gps"], ["splits"]),
        (p["eval_gps"], p["route_gps"], ["matched"]),
        (p["eval_gps"], p["kafka_raw"], ["matched"]),
        (p["eval_gps"], p["update_gps_archive_name"], ["matched"]),
        (p["update_gps_archive_name"], p["archive_gps_raw"], ["success"]),
        (p["route_gps"], p["kafka_filtered"], ["filtered"]),
        (p["fetch_gps"], p["gps_failure_sink"], ["failure", "not.found", "permission.denied"]),
        (p["split_gps"], p["gps_failure_sink"], ["failure"]),
        (p["eval_gps"], p["gps_failure_sink"], ["failure", "unmatched"]),
        (p["update_gps_archive_name"], p["gps_failure_sink"], ["failure"]),
        (p["route_gps"], p["gps_failure_sink"], ["failure", "unmatched"]),
        (p["kafka_raw"], p["gps_failure_sink"], ["failure"]),
        (p["kafka_filtered"], p["gps_failure_sink"], ["failure"]),
        (p["archive_gps_raw"], p["gps_failure_sink"], ["failure"]),
        (p["tick_weather"], p["invoke_weather"], ["success"]),
        (p["invoke_weather"], p["kafka_weather_raw"], ["Response"]),
        (p["invoke_weather"], p["archive_weather_raw"], ["Response"]),
        (p["invoke_weather"], p["extract_weather"], ["Response"]),
        (p["extract_weather"], p["update_weather"], ["matched"]),
        (p["update_weather"], p["attrs_weather"], ["success"]),
        (p["attrs_weather"], p["kafka_weather_filtered"], ["success"]),
        (p["invoke_weather"], p["weather_failure_sink"], ["Failure", "Retry", "No Retry", "failure", "retry", "no retry"]),
        (p["extract_weather"], p["weather_failure_sink"], ["failure", "unmatched"]),
        (p["update_weather"], p["weather_failure_sink"], ["failure"]),
        (p["attrs_weather"], p["weather_failure_sink"], ["failure"]),
        (p["kafka_weather_raw"], p["weather_failure_sink"], ["failure"]),
        (p["kafka_weather_filtered"], p["weather_failure_sink"], ["failure"]),
        (p["archive_weather_raw"], p["weather_failure_sink"], ["failure"]),
    ]
    used = {proc_id: set() for proc_id in p.values()}
    gps_proc_ids = {
        p["list_gps"],
        p["fetch_gps"],
        p["split_gps"],
        p["eval_gps"],
        p["update_gps_archive_name"],
        p["route_gps"],
        p["kafka_raw"],
        p["archive_gps_raw"],
        p["kafka_filtered"],
        p["gps_failure_sink"],
    }

    for src, dst, rels in connections:
        connection_pg_id = gps_pg_id if src in gps_proc_ids else weather_pg_id
        selected = create_connection(token, connection_pg_id, src, dst, rels)
        used[src].update(selected)

    for proc_id in p.values():
        set_auto_terminated(token, proc_id, used.get(proc_id, set()))

    for proc_id in p.values():
        start_processor(token, proc_id)

    print(f"NiFi bootstrap completado. Process Group: {PROCESS_GROUP_NAME} ({pg_id})")
    print("Estructura visual: gps_ingestion + weather_ingestion")
    print("Flujos activos: GPS->Kafka raw/filtered y Weather API->Kafka raw/filtered")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            print(
                "ERROR bootstrap NiFi: HTTP 409 Conflict (NiFi aun inicializando o revision en conflicto). Reintenta en unos segundos.",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"ERROR bootstrap NiFi: HTTP {exc.code} {exc.reason}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR bootstrap NiFi: {exc}", file=sys.stderr)
        sys.exit(1)
