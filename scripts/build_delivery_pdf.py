#!/usr/bin/env python3
import re
import textwrap
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / f"entrega-unificada-{date.today().isoformat()}.pdf"

DOCS = [
    ROOT / "docs" / "ENTREGA.md",
    ROOT / "docs" / "resumen-ejecutivo-memoria.md",
    ROOT / "docs" / "memoria-tecnica-sistema.md",
    ROOT / "docs" / "nifi-flow.md",
    ROOT / "docs" / "dashboard.md",
    ROOT / "docs" / "operations.md",
    ROOT / "docs" / "manual-usuario.md",
    ROOT / "docs" / "architecture.md",
    ROOT / "docs" / "release-notes-2026-04-03.md",
    ROOT / "README.md",
    ROOT / "CHANGELOG.md",
]


def md_to_text(md: str) -> list[str]:
    lines = md.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: list[str] = []
    in_code = False

    for raw in lines:
        line = raw.rstrip()
        if line.strip().startswith("```"):
            in_code = not in_code
            out.append("")
            continue
        if in_code:
            out.append("    " + line)
            continue

        line = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"[imagen: \1] (\2)", line)
        line = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", line)
        line = re.sub(r"`([^`]+)`", r"\1", line)
        line = re.sub(r"^\s{0,3}#{1,6}\s*", "", line)
        line = re.sub(r"^\s*[-*]\s+", "- ", line)
        line = re.sub(r"^\s*\d+\.\s+", "- ", line)
        line = line.replace("|---", "|")
        out.append(line)

    wrapped: list[str] = []
    for line in out:
        if not line.strip():
            wrapped.append("")
            continue
        if line.startswith("    "):
            wrapped.extend(textwrap.wrap(line, width=96, drop_whitespace=False) or [""])
            continue
        wrapped.extend(textwrap.wrap(line, width=96) or [""])
    return wrapped


def pdf_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf_from_lines(lines: list[str], out_path: Path) -> None:
    page_w, page_h = 595, 842
    margin_x = 48
    margin_top = 48
    margin_bottom = 52
    line_h = 12
    font_size = 10
    max_lines = int((page_h - margin_top - margin_bottom) // line_h)

    pages: list[list[str]] = []
    cur: list[str] = []
    for line in lines:
        if line == "\f":
            pages.append(cur if cur else [""])
            cur = []
            continue
        cur.append(line)
        if len(cur) >= max_lines:
            pages.append(cur)
            cur = []
    if cur:
        pages.append(cur)
    if not pages:
        pages = [["Documento vacio"]]

    objs: list[bytes] = []

    def add_obj(data: bytes) -> int:
        objs.append(data)
        return len(objs)

    font_obj = add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    content_ids: list[int] = []
    page_ids: list[int] = []

    for page_lines in pages:
        stream_parts = [b"BT", f"/F1 {font_size} Tf".encode("ascii")]
        start_y = page_h - margin_top
        stream_parts.append(f"{margin_x} {start_y} Td".encode("ascii"))
        for i, line in enumerate(page_lines):
            if i > 0:
                stream_parts.append(f"0 -{line_h} Td".encode("ascii"))
            safe = pdf_escape(line)
            stream_parts.append(f"({safe}) Tj".encode("latin-1", errors="replace"))
        stream_parts.append(b"ET")
        stream = b"\n".join(stream_parts)
        content = (
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii")
            + stream
            + b"\nendstream"
        )
        content_ids.append(add_obj(content))

    page_dicts: list[bytes] = []
    for cid in content_ids:
        page_dict = (
            f"<< /Type /Page /Parent __PAGES__ /MediaBox [0 0 {page_w} {page_h}] "
            f"/Resources << /Font << /F1 {font_obj} 0 R >> >> /Contents {cid} 0 R >>"
        ).encode("ascii")
        page_dicts.append(page_dict)

    pages_obj_id = len(objs) + len(page_dicts) + 1
    for page_dict in page_dicts:
        patched = page_dict.replace(b"__PAGES__", f"{pages_obj_id} 0 R".encode("ascii"))
        page_ids.append(add_obj(patched))

    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    pages_obj = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("ascii")
    add_obj(pages_obj)

    catalog_obj = f"<< /Type /Catalog /Pages {pages_obj_id} 0 R >>".encode("ascii")
    catalog_id = add_obj(catalog_obj)

    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for i, obj in enumerate(objs, start=1):
        offsets.append(len(output))
        output.extend(f"{i} 0 obj\n".encode("ascii"))
        output.extend(obj)
        output.extend(b"\nendobj\n")

    xref_pos = len(output)
    output.extend(f"xref\n0 {len(objs)+1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for i in range(1, len(objs) + 1):
        output.extend(f"{offsets[i]:010d} 00000 n \n".encode("ascii"))

    trailer = (
        f"trailer\n<< /Size {len(objs)+1} /Root {catalog_id} 0 R >>\n"
        f"startxref\n{xref_pos}\n%%EOF\n"
    )
    output.extend(trailer.encode("ascii"))
    out_path.write_bytes(output)


def main() -> None:
    lines: list[str] = []
    lines.append("PROYECTO BIG DATA KDD - ENTREGA UNIFICADA")
    lines.append("")
    lines.append("Autor: Rául Meneses Gutiérrez")
    lines.append("Curso: Curso de Especialización en Inteligencia Artificial y BigData")
    lines.append(f"Fecha de generacion: {date.today().isoformat()}")
    lines.append("")
    lines.append("\f")
    lines.append("INDICE DE DOCUMENTOS")
    lines.append("")
    lines.append("Documentos incluidos:")
    for doc in DOCS:
        rel = doc.relative_to(ROOT)
        lines.append(f"- {rel}")
    lines.append("")
    lines.append("=" * 96)
    lines.append("")

    for doc in DOCS:
        rel = doc.relative_to(ROOT)
        lines.append(f"DOCUMENTO: {rel}")
        lines.append("-" * 96)
        text = doc.read_text(encoding="utf-8", errors="replace")
        lines.extend(md_to_text(text))
        lines.append("")
        lines.append("=" * 96)
        lines.append("")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    build_pdf_from_lines(lines, OUT)
    print(str(OUT))


if __name__ == "__main__":
    main()
