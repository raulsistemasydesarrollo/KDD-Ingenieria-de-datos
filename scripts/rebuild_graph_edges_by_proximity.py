#!/usr/bin/env python3
"""
Regenera data/graph/edges.csv usando vecinos geograficos mas cercanos (k-NN).

Objetivo:
- Evitar red totalmente completa (demasiadas aristas directas).
- Mantener conectividad global de la red.
- Permitir variar k rapidamente para pruebas (k=3/4/5...).

Uso:
  python3 scripts/rebuild_graph_edges_by_proximity.py --k 4

Tambien admite variable de entorno:
  GRAPH_K_NEIGHBORS=5 python3 scripts/rebuild_graph_edges_by_proximity.py
"""

from __future__ import annotations

import argparse
import csv
import math
import os
from collections import Counter
from pathlib import Path

FORBIDDEN_DIRECT_EDGES = {
    ("BCN", "MUR"),
    ("ACO", "BIO"),
    ("VAL", "ALM"),
}

REQUIRED_EDGES = {
    ("BCN", "VAL"),
    ("VAL", "MUR"),
    ("ACO", "GIJ"),
    ("GIJ", "BIO"),
    ("MAD", "SEV"),
    ("MAD", "MAL"),
}

SOUTH_CHAIN = ("CAC", "SEV", "MAL", "ALM", "MUR")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild graph edges using geographic k-nearest neighbors.")
    parser.add_argument(
        "--k",
        type=int,
        default=int(os.getenv("GRAPH_K_NEIGHBORS", "4")),
        help="Numero de vecinos geograficos por nodo (default: env GRAPH_K_NEIGHBORS o 4).",
    )
    parser.add_argument(
        "--input",
        default="data/master/warehouses.csv",
        help="CSV de almacenes con lat/lon.",
    )
    parser.add_argument(
        "--output",
        default="data/graph/edges.csv",
        help="CSV de aristas de salida.",
    )
    return parser.parse_args()


def haversine_km(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    earth_radius_km = 6371.0
    d_lat = math.radians(b_lat - a_lat)
    d_lon = math.radians(b_lon - a_lon)
    lat1 = math.radians(a_lat)
    lat2 = math.radians(b_lat)
    h = (math.sin(d_lat / 2) ** 2) + (math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2)
    return earth_radius_km * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def delay_for_distance(distance_km: float) -> int:
    if distance_km < 160:
        return 4
    if distance_km < 260:
        return 5
    if distance_km < 360:
        return 6
    if distance_km < 480:
        return 7
    if distance_km < 620:
        return 8
    if distance_km < 760:
        return 9
    return 10


def load_nodes(warehouses_csv: Path) -> list[tuple[str, float, float]]:
    nodes: list[tuple[str, float, float]] = []
    with warehouses_csv.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            node_id = str(row.get("warehouse_id") or "").strip().upper()
            if not node_id:
                continue
            try:
                lat = float(row.get("latitude") or "")
                lon = float(row.get("longitude") or "")
            except ValueError:
                continue
            nodes.append((node_id, lat, lon))
    nodes.sort(key=lambda item: item[0])
    return nodes


def build_distance_index(nodes: list[tuple[str, float, float]]) -> dict[tuple[str, str], float]:
    coords = {node_id: (lat, lon) for node_id, lat, lon in nodes}
    ids = sorted(coords.keys())
    distances: dict[tuple[str, str], float] = {}
    for i, left in enumerate(ids):
        for right in ids[i + 1 :]:
            lat1, lon1 = coords[left]
            lat2, lon2 = coords[right]
            distances[(left, right)] = round(haversine_km(lat1, lon1, lat2, lon2), 1)
    return distances


def pair_distance(a: str, b: str, distances: dict[tuple[str, str], float]) -> float:
    key = (a, b) if a < b else (b, a)
    return distances[key]


def build_knn_edges(
    node_ids: list[str], distances: dict[tuple[str, str], float], k: int
) -> set[tuple[str, str]]:
    edge_set: set[tuple[str, str]] = set()
    for node_id in node_ids:
        neighbors = sorted(
            (candidate for candidate in node_ids if candidate != node_id),
            key=lambda candidate: (pair_distance(node_id, candidate, distances), candidate),
        )[:k]
        for neighbor in neighbors:
            edge_set.add((node_id, neighbor) if node_id < neighbor else (neighbor, node_id))
    return edge_set


def normalize_edge(src: str, dst: str) -> tuple[str, str]:
    return (src, dst) if src < dst else (dst, src)


def apply_business_rules(
    edge_set: set[tuple[str, str]], node_ids: list[str]
) -> set[tuple[str, str]]:
    nodes = set(node_ids)
    filtered = set(edge_set)

    # Elimina conexiones directas no permitidas por regla de red.
    for src, dst in FORBIDDEN_DIRECT_EDGES:
        pair = normalize_edge(src, dst)
        filtered.discard(pair)

    # Corredor sur: sin atajos directos entre nodos no consecutivos.
    south_nodes = [node for node in SOUTH_CHAIN if node in nodes]
    allowed_south = set()
    for i in range(len(south_nodes) - 1):
        allowed_south.add(normalize_edge(south_nodes[i], south_nodes[i + 1]))
    for i in range(len(south_nodes)):
        for j in range(i + 1, len(south_nodes)):
            pair = normalize_edge(south_nodes[i], south_nodes[j])
            if pair not in allowed_south:
                filtered.discard(pair)

    # Asegura conexiones obligatorias para corredores definidos.
    for src, dst in REQUIRED_EDGES:
        if src in nodes and dst in nodes:
            filtered.add(normalize_edge(src, dst))
    for pair in allowed_south:
        filtered.add(pair)
    return filtered


def is_connected(node_ids: list[str], edges: set[tuple[str, str]]) -> bool:
    if not node_ids:
        return True
    adjacency = {node_id: set() for node_id in node_ids}
    for src, dst in edges:
        adjacency[src].add(dst)
        adjacency[dst].add(src)
    visited = {node_ids[0]}
    stack = [node_ids[0]]
    while stack:
        current = stack.pop()
        for nxt in adjacency[current]:
            if nxt in visited:
                continue
            visited.add(nxt)
            stack.append(nxt)
    return len(visited) == len(node_ids)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    requested_k = max(1, int(args.k))

    nodes = load_nodes(input_path)
    if len(nodes) < 2:
        raise RuntimeError("Se requieren al menos 2 nodos con coordenadas validas.")
    node_ids = [node_id for node_id, _, _ in nodes]
    distances = build_distance_index(nodes)

    max_k = max(1, len(node_ids) - 1)
    effective_k = min(requested_k, max_k)
    edges = apply_business_rules(build_knn_edges(node_ids, distances, effective_k), node_ids)
    while not is_connected(node_ids, edges) and effective_k < max_k:
        effective_k += 1
        edges = apply_business_rules(build_knn_edges(node_ids, distances, effective_k), node_ids)

    rows = sorted(edges, key=lambda pair: (pair_distance(pair[0], pair[1], distances), pair[0], pair[1]))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["src", "dst", "distance_km", "avg_delay_minutes"])
        for src, dst in rows:
            distance_km = pair_distance(src, dst, distances)
            writer.writerow([src, dst, distance_km, delay_for_distance(distance_km)])

    degree = Counter()
    for src, dst in rows:
        degree[src] += 1
        degree[dst] += 1
    print(
        f"Rebuilt {output_path} | nodes={len(node_ids)} requested_k={requested_k} "
        f"effective_k={effective_k} connected={is_connected(node_ids, edges)} edges={len(rows)} "
        f"degree_min={min(degree.values())} degree_max={max(degree.values())}"
    )


if __name__ == "__main__":
    main()
