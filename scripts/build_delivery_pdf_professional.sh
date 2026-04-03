#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TODAY="$(date +%F)"
OUT_PDF="/data/docs/entrega-unificada-profesional-${TODAY}.pdf"
RELEASE_NOTES_FILE="${RELEASE_NOTES_FILE:-release-notes-2026-04-03.md}"

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
  manual-usuario.md \
  architecture.md \
  "${RELEASE_NOTES_FILE}" \
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
