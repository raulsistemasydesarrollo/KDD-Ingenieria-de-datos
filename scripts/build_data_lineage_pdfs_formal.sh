#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BUILD_DIR="docs/.pdf_build_formal"
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

TODAY="$(date +%d/%m/%Y)"

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

make_cover_html() {
  local title="$1"
  local out="$2"
  cat > "$out" <<HTML
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8" />
<title>${title}</title>
<style>
  body { font-family: Arial, Helvetica, sans-serif; margin: 0; }
  .page { height: 100vh; display: flex; align-items: center; justify-content: center; }
  .box { width: 80%; border: 2px solid #1e3a8a; padding: 48px; }
  h1 { margin: 0 0 12px 0; color: #0f172a; font-size: 34px; line-height: 1.2; }
  h2 { margin: 0 0 28px 0; color: #334155; font-size: 20px; font-weight: 500; }
  .meta { color: #1f2937; font-size: 14px; line-height: 1.7; }
  .line { height: 3px; background: #1e3a8a; margin: 18px 0 22px 0; }
</style>
</head>
<body>
  <div class="page">
    <div class="box">
      <h1>${title}</h1>
      <h2>Edicion formal para presentacion</h2>
      <div class="line"></div>
      <div class="meta">
        <div><strong>Proyecto:</strong> Plataforma KDD Logistica</div>
        <div><strong>Fecha de generacion:</strong> ${TODAY}</div>
        <div><strong>Formato:</strong> PDF A4 con numeracion de pagina</div>
      </div>
    </div>
  </div>
</body>
</html>
HTML
}

for md in "${FILES[@]}"; do
  if [[ ! -f "$md" ]]; then
    echo "[WARN] No existe $md, se omite"
    continue
  fi

  base="$(basename "$md" .md)"
  formal_pdf="docs/${base}-formal.pdf"
  clean_md="${BUILD_DIR}/${base}.no-mermaid.md"
  html="${BUILD_DIR}/${base}.html"
  cover_html="${BUILD_DIR}/${base}.cover.html"

  echo "[INFO] Procesando ${base}"
  copy_assets
  clean_markdown_mermaid "$md" "$clean_md"
  sed -i \
    -e 's#(\./lineage-diagrams/#(./lineage-diagrams/#g' \
    -e 's#src="\./lineage-diagrams/#src="./lineage-diagrams/#g' \
    -e 's#(\./data-lineage-flow\.svg)#(./data-lineage-flow.svg)#g' \
    -e 's#src="\./data-lineage-flow\.svg"#src="./data-lineage-flow.svg"#g' \
    "$clean_md"
  normalize_img_tags "$clean_md"

  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "${ROOT_DIR}:/work" \
    -w /work \
    pandoc/core \
    --from markdown+raw_html \
    --to html5 \
    --standalone \
    --resource-path=/work/docs/.pdf_build_formal \
    --css "/work/${PRINT_CSS}" \
    --metadata title="$base" \
    -o "$html" "$clean_md"

  make_cover_html "$base" "$cover_html"

  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "${ROOT_DIR}:/work" \
    -w /work \
    ghcr.io/surnet/alpine-wkhtmltopdf:3.20.2-0.12.6-full \
      --enable-local-file-access \
      --page-size A4 \
      --margin-top 8mm \
      --margin-bottom 10mm \
      --margin-left 6mm \
      --margin-right 6mm \
      --zoom 1.12 \
      --footer-center "Pagina [page] de [toPage]" \
      --footer-font-size 9 \
      --footer-spacing 4 \
      cover "$cover_html" "$html" "$formal_pdf"

  echo "[OK] Generado: $formal_pdf"
done

echo
echo "PDFs formales generados:"
ls -1 docs/data-lineage-*-formal.pdf 2>/dev/null || true
