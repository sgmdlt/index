"""Concatenate HTML bodies for every path in sql_error_html.txt into one file."""

from io import BytesIO
from pathlib import Path

from warcio.archiveiterator import ArchiveIterator

from src.load_from_db import _loc_parts

PATHS = Path("sql_error_html.txt")
OUT = Path("sql_error_pages.txt")

missing = 0
with OUT.open("w", encoding="utf-8") as out:
    for line in PATHS.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("/app/court_warc/"):
            shard, off, length = _loc_parts(line)
            with open(shard, "rb") as fh:
                fh.seek(off)
                buf = BytesIO(fh.read(length))
            html = b""
            for rec in ArchiveIterator(buf):
                if rec.rec_type == "resource":
                    html = rec.content_stream().read()
        else:
            p = Path(line)
            if not p.exists():
                missing += 1
                out.write(f"\n{'=' * 80}\n=== MISSING: {line}\n{'=' * 80}\n")
                continue
            html = p.read_bytes()
        out.write(f"\n{'=' * 80}\n=== {line}\n{'=' * 80}\n")
        out.write(html.decode("utf-8", errors="replace"))

print(f"done, missing={missing}")
