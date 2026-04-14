#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "07 - Entrada batch en HDFS (dataset semilla)"
ensure_service hadoop || exit 0

section "Listado HDFS /data/raw /data/master /data/graph"
run_service hadoop hdfs dfs -ls -R /data/raw /data/master /data/graph

section "Muestra gps_events.jsonl (batch input)"
run_service hadoop bash -lc "hdfs dfs -cat /data/raw/gps_events.jsonl | head -n 5"
