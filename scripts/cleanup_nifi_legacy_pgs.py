#!/usr/bin/env python3
from __future__ import annotations

"""
Limpieza de Process Groups legacy de NiFi.

Funcion:
- Identifica PGs por prefijo.
- Conserva solo el PG indicado con --keep.
- Detiene componentes, vacia colas y elimina PGs sobrantes con reintentos.

Objetivo:
- Mantener la UI de NiFi limpia y evitar configuraciones duplicadas antiguas.
"""

import argparse
import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Elimina process groups legacy de NiFi que compartan prefijo, "
            "manteniendo solo el indicado con --keep."
        )
    )
    p.add_argument("--url", default="https://localhost:8443/nifi-api", help="Base URL NiFi API")
    p.add_argument("--user", default="admin", help="Usuario NiFi")
    p.add_argument("--password", default="adminadminadmin", help="Password NiFi")
    p.add_argument("--prefix", default="kdd_ingestion_auto", help="Prefijo de process groups gestionados")
    p.add_argument("--keep", default="kdd_ingestion_auto_v9", help="Nombre del process group a conservar")
    p.add_argument("--max-delete-retries", type=int, default=12, help="Reintentos maximos de borrado por PG")
    return p


class NifiClient:
    def __init__(self, base_url: str, user: str, password: str):
        # Inicializa cliente y obtiene token al construir instancia.
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.ctx = ssl._create_unverified_context() if self.base_url.startswith("https://") else None
        self.token = self._get_token()

    def _request(self, method: str, path: str, payload=None, form=None, expect_json=True):
        # Request autenticada para endpoints que requieren bearer token.
        headers = {"Authorization": f"Bearer {self.token}"}
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        if form is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            data = urllib.parse.urlencode(form).encode("utf-8")
        req = urllib.request.Request(f"{self.base_url}{path}", method=method, headers=headers, data=data)
        with urllib.request.urlopen(req, context=self.ctx, timeout=30) as resp:
            body = resp.read()
            if not body:
                return {}
            text = body.decode("utf-8")
            return json.loads(text) if expect_json else text

    def _request_noauth(self, method: str, path: str, payload=None, form=None, expect_json=True):
        # Request sin auth (usado solo en login/token).
        headers = {}
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        if form is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            data = urllib.parse.urlencode(form).encode("utf-8")
        req = urllib.request.Request(f"{self.base_url}{path}", method=method, headers=headers, data=data)
        with urllib.request.urlopen(req, context=self.ctx, timeout=30) as resp:
            body = resp.read()
            if not body:
                return {}
            text = body.decode("utf-8")
            return json.loads(text) if expect_json else text

    def _get_token(self) -> str:
        token = self._request_noauth(
            "POST",
            "/access/token",
            form={"username": self.user, "password": self.password},
            expect_json=False,
        )
        if not token:
            raise RuntimeError("No se pudo obtener token NiFi.")
        return token

    def root_process_groups(self):
        data = self._request("GET", "/flow/process-groups/root")
        return data["processGroupFlow"]["flow"].get("processGroups", [])

    def stop_process_group(self, pg_id: str):
        payload = {"id": pg_id, "state": "STOPPED", "disconnectedNodeAcknowledged": True}
        self._request("PUT", f"/flow/process-groups/{pg_id}", payload=payload)

    def group_connections(self, pg_id: str):
        data = self._request("GET", f"/flow/process-groups/{pg_id}")
        return data["processGroupFlow"]["flow"].get("connections", [])

    def group_controller_services(self, pg_id: str):
        data = self._request("GET", f"/flow/process-groups/{pg_id}/controller-services")
        return data.get("controllerServices", [])

    def controller_service_entity(self, svc_id: str):
        return self._request("GET", f"/controller-services/{svc_id}")

    def set_controller_service_state(self, svc_id: str, state: str):
        current = self.controller_service_entity(svc_id)
        payload = {
            "revision": {"version": current["revision"]["version"]},
            "state": state,
            "disconnectedNodeAcknowledged": True,
        }
        self._request("PUT", f"/controller-services/{svc_id}/run-status", payload=payload)

    def process_group_entity(self, pg_id: str):
        return self._request("GET", f"/process-groups/{pg_id}")

    def delete_process_group(self, pg_id: str, version: int):
        self._request(
            "DELETE",
            f"/process-groups/{pg_id}?version={version}&clientId=cleanup-legacy&disconnectedNodeAcknowledged=true",
            expect_json=False,
        )

    def create_drop_request(self, conn_id: str):
        payload = {"dropRequest": {"id": conn_id}}
        return self._request("POST", f"/flowfile-queues/{conn_id}/drop-requests", payload=payload)

    def get_drop_request(self, conn_id: str, drop_id: str):
        return self._request("GET", f"/flowfile-queues/{conn_id}/drop-requests/{drop_id}")

    def delete_drop_request(self, conn_id: str, drop_id: str):
        self._request("DELETE", f"/flowfile-queues/{conn_id}/drop-requests/{drop_id}", expect_json=False)


def drop_queues(client: NifiClient, pg_id: str):
    for conn in client.group_connections(pg_id):
        conn_id = conn.get("component", {}).get("id")
        if not conn_id:
            continue
        try:
            drop = client.create_drop_request(conn_id)
            drop_id = drop.get("dropRequest", {}).get("id")
            if not drop_id:
                continue
            for _ in range(30):
                st = client.get_drop_request(conn_id, drop_id)
                if st.get("dropRequest", {}).get("finished", False):
                    break
                time.sleep(1)
            try:
                client.delete_drop_request(conn_id, drop_id)
            except Exception:
                pass
            print(f"  - Cola vaciada: {conn_id}")
        except Exception as exc:
            print(f"  - Aviso vaciando cola {conn_id}: {exc}")


def disable_services(client: NifiClient, pg_id: str):
    for svc in client.group_controller_services(pg_id):
        svc_id = svc.get("id")
        comp = svc.get("component", {})
        name = comp.get("name")
        state = comp.get("state")
        if not svc_id:
            continue
        if state == "DISABLED":
            print(f"  - Service ya deshabilitado: {name}")
            continue
        print(f"  - Deshabilitando service: {name}")
        client.set_controller_service_state(svc_id, "DISABLED")
        for _ in range(25):
            cur = client.controller_service_entity(svc_id)
            if cur.get("component", {}).get("state") == "DISABLED":
                break
            time.sleep(1)


def delete_group_with_retries(client: NifiClient, pg_id: str, retries: int):
    for i in range(1, retries + 1):
        try:
            client.stop_process_group(pg_id)
        except Exception:
            pass
        time.sleep(1)
        try:
            ent = client.process_group_entity(pg_id)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return
            raise
        version = ent["revision"]["version"]
        try:
            client.delete_process_group(pg_id, version)
            print(f"  - PG borrado (intento {i})")
            return
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "ignore")
            print(f"  - Intento {i} -> HTTP {exc.code}")
            if body:
                print(f"    {body[:220].replace(chr(10), ' ')}")
            if exc.code not in (400, 409):
                raise
            time.sleep(2)
    raise RuntimeError(f"No se pudo borrar process group {pg_id} tras {retries} intentos.")


def main() -> int:
    args = build_parser().parse_args()
    client = NifiClient(args.url, args.user, args.password)

    root_groups = client.root_process_groups()
    targets = []
    for pg in root_groups:
        comp = pg.get("component", {})
        name = comp.get("name", "")
        if name.startswith(args.prefix) and name != args.keep:
            targets.append(comp)

    if not targets:
        print("No hay process groups legacy para borrar.")
        return 0

    for comp in targets:
        pg_id = comp["id"]
        name = comp["name"]
        print(f"Limpiando PG legacy: {name} ({pg_id})")
        drop_queues(client, pg_id)
        disable_services(client, pg_id)
        delete_group_with_retries(client, pg_id, args.max_delete_retries)

    print("Root PGs restantes:")
    for pg in client.root_process_groups():
        comp = pg.get("component", {})
        print(f"- {comp.get('name')} ({comp.get('id')})")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR cleanup_nifi_legacy_pgs: {exc}", file=sys.stderr)
        raise SystemExit(1)
