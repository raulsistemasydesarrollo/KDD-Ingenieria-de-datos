#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "01 - Origen de datos maestros (CSV locales)"
echo "Ubicacion: data/master/*.csv y data/graph/*.csv"

section "Muestra warehouses.csv"
sed -n '1,8p' data/master/warehouses.csv

section "Muestra vehicles.csv"
sed -n '1,8p' data/master/vehicles.csv

section "Muestra vertices.csv"
sed -n '1,8p' data/graph/vertices.csv

section "Muestra edges.csv"
sed -n '1,8p' data/graph/edges.csv
