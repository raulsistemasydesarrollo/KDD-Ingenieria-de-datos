#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_PDF="/data/docs/entrega-unificada-profesional-2026-03-31.pdf"

cd "${ROOT_DIR}/docs"

docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "${ROOT_DIR}:/data" \
  -w /data/docs \
  pandoc/latex:3.1 \
  cover-profesional.md \
  ENTREGA.md \
  resumen-ejecutivo-memoria.md \
  memoria-tecnica-sistema.md \
  nifi-flow.md \
  dashboard.md \
  operations.md \
  architecture.md \
  release-notes-2026-03-30.md \
  ../README.md \
  ../CHANGELOG.md \
  --from markdown+raw_tex \
  --toc \
  --number-sections \
  --toc-depth=3 \
  --pdf-engine=xelatex \
  -V geometry:margin=2.2cm \
  -V fontsize=11pt \
  -V linestretch=1.15 \
  -V colorlinks=true \
  -V linkcolor=blue \
  -V urlcolor=blue \
  -o "${OUT_PDF}"

echo "${OUT_PDF}"
