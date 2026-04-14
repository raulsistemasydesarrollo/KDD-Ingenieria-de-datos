#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BUILD_DIR="docs/.pdf_build"
mkdir -p "${BUILD_DIR}"
PRINT_CSS="${BUILD_DIR}/pdf-print.css"

cat > "${PRINT_CSS}" <<'CSS'
html, body {
  max-width: none !important;
}
body {
  margin: 0 !important;
  padding: 0 !important;
  font-size: 14px;
  line-height: 1.4;
}
img, svg {
  max-width: 100% !important;
  height: auto !important;
}
h1, h2, h3 {
  margin-top: 0.4em;
  margin-bottom: 0.35em;
}
hr {
  margin: 0.7em 0;
}
CSS

copy_assets() {
  cp -f "docs/data-lineage-flow.svg" "${BUILD_DIR}/data-lineage-flow.svg"
  rm -rf "${BUILD_DIR}/lineage-diagrams"
  cp -a "docs/lineage-diagrams" "${BUILD_DIR}/lineage-diagrams"
}

FILES=(
  "docs/data-lineage-empresa.md"
  "docs/data-lineage-empresa-extendida.md"
  "docs/data-lineage-comite-direccion.md"
  "docs/data-lineage-comite-direccion-hablada.md"
)

clean_markdown_mermaid() {
  local in="$1"
  local out="$2"
  awk '
    BEGIN { skip=0 }
    {
      if (skip==0 && $0 ~ /^```mermaid[[:space:]]*$/) { skip=1; next }
      if (skip==1 && $0 ~ /^```[[:space:]]*$/) { skip=0; next }
      if (skip==0) print
    }
  ' "$in" > "$out"
}

normalize_img_tags() {
  local file="$1"
  python3 - "$file" <<'PY'
import pathlib, re, sys
p = pathlib.Path(sys.argv[1])
s = p.read_text(encoding="utf-8")
pat = re.compile(r'<img\s+[^>]*src="([^"]+)"[^>]*alt="([^"]*)"[^>]*>', re.IGNORECASE)
s = pat.sub(lambda m: f'![{m.group(2)}]({m.group(1)})', s)
p.write_text(s, encoding="utf-8")
PY
}

for md in "${FILES[@]}"; do
  if [[ ! -f "$md" ]]; then
    echo "[WARN] No existe $md, se omite"
    continue
  fi

  base="$(basename "$md" .md)"
  clean_md="${BUILD_DIR}/${base}.no-mermaid.md"
  html="${BUILD_DIR}/${base}.html"
  pdf="docs/${base}.pdf"

  echo "[INFO] Limpiando Mermaid: $md"
  copy_assets
  clean_markdown_mermaid "$md" "$clean_md"
  # Corrige rutas relativas de imagen al generar HTML dentro de docs/.pdf_build
  sed -i \
    -e 's#(\\./lineage-diagrams/#(./lineage-diagrams/#g' \
    -e 's#src=\"\\./lineage-diagrams/#src=\"./lineage-diagrams/#g' \
    -e 's#(\\./data-lineage-flow\\.svg)#(./data-lineage-flow.svg)#g' \
    -e 's#src=\"\\./data-lineage-flow\\.svg\"#src=\"./data-lineage-flow.svg\"#g' \
    "$clean_md"
  normalize_img_tags "$clean_md"

  echo "[INFO] Markdown -> HTML: $base"
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "${ROOT_DIR}:/work" \
    -w /work \
    pandoc/core \
    --from markdown+raw_html \
    --to html5 \
    --standalone \
    --resource-path=/work/docs/.pdf_build \
    --css "/work/${PRINT_CSS}" \
    --metadata title="$base" \
    -o "$html" "$clean_md"

  echo "[INFO] HTML -> PDF (solo SVG/imágenes): $base"
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "${ROOT_DIR}:/work" \
    -w /work \
    ghcr.io/surnet/alpine-wkhtmltopdf:3.20.2-0.12.6-full \
      --enable-local-file-access \
      --page-size A4 \
      --margin-top 6mm \
      --margin-bottom 6mm \
      --margin-left 4mm \
      --margin-right 4mm \
      --zoom 1.22 \
      --dpi 300 \
      "$html" "$pdf"

  echo "[OK] Generado: $pdf"
done

echo
echo "PDFs generados en docs/:"
ls -1 docs/data-lineage-*.pdf 2>/dev/null || true
