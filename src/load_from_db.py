"""PostgreSQL outbox source for court case units.

Ports loadFromDb.js. Implements lease-based polling with retry limits.
"""

import json
import logging
import os
from datetime import datetime
from io import BytesIO
from itertools import groupby
from typing import Generator

from warcio.archiveiterator import ArchiveIterator

from src.parse_html_mgs import is_mgs, mgs_identity

logger = logging.getLogger(__name__)

TABLE = os.environ.get("PG_TABLE", "outbox")
REDOWNLOAD_TABLE = os.environ.get("PG_REDOWNLOAD_TABLE", "redownload_queue")
DOCKER_ROOT = "/app/court_data/"
# The prefix to strip differs per crawler: courts writes /app/court_warc/, the
# MGS crawler writes /app/out/. Both reach Postgres as container paths.
DOCKER_WARC_ROOT = os.environ.get("COURTS_WARC_DOCKER_ROOT", "/app/court_warc/")
MAX_ATTEMPTS = 5

VALID_REASONS = {
    "path_missing",
    "docs_missing",
    "bad_manifest_json",
    "build_failed",
    "bad_jsonl_record",
    "sql_error",   # court DB error page stored instead of case HTML
    "empty_html",  # placeholder/banner page with no extractable fields
}


FS_ROOT = os.environ.get("COURTS_FS_ROOT", "/home/ltdmgs/courts_raw_data/")
WARC_FS_ROOT = os.environ.get("COURTS_WARC_ROOT", "/home/ltdmgs/courts_warc/")


def _fix_path(db_path: str | None) -> str:
    """Translate Docker-side path to host filesystem absolute path."""
    if not db_path:
        return ""
    return db_path.replace(DOCKER_ROOT, FS_ROOT)


def _loc_parts(locator: str) -> tuple[str, int, int]:
    """Split "path:offset:length", translating the Docker-side shard path."""
    path, off, length = locator.rsplit(":", 2)
    return path.replace(DOCKER_WARC_ROOT, WARC_FS_ROOT), int(off), int(length)


def poll_batch(conn, worker_id: str, batch_size: int = 5000) -> list[dict]:
    """Lease a batch of NEW/FAILED rows and return them."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {TABLE}
            SET locked_by = %s,
                locked_at = NOW(),
                lease_until = NOW() + interval '5 minutes'
            WHERE id IN (
                SELECT id FROM {TABLE}
                WHERE status IN ('NEW', 'FAILED')
                  AND attempts < %s
                  AND (lease_until IS NULL OR lease_until < NOW())
                ORDER BY id ASC
                LIMIT %s FOR UPDATE SKIP LOCKED
            )
            RETURNING id, filename, manifest, attempts, warc_locator
            """,
            (worker_id, MAX_ATTEMPTS, batch_size),
        )
        return [
            {
                "id": r[0],
                "filename": r[1],
                "manifest": r[2],
                "attempts": r[3],
                "warc_locator": r[4],
            }
            for r in cur.fetchall()
        ]


def mark_done(conn, ids: list[int]) -> None:
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {TABLE} SET status='DONE', locked_by=NULL, lease_until=NULL WHERE id=ANY(%s)",
            (ids,),
        )
    conn.commit()


def mark_failed(conn, row_id: int, attempts: int, error: str) -> None:
    new_attempts = attempts + 1
    if new_attempts >= MAX_ATTEMPTS:
        logger.error(
            "[ID %d] Attempt limit (%d/%d). Excluded.",
            row_id,
            new_attempts,
            MAX_ATTEMPTS,
        )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {TABLE}
            SET status='FAILED', attempts=%s, last_error=%s,
                locked_by=NULL, lease_until=NOW()+interval '10 minutes'
            WHERE id=%s
            """,
            (new_attempts, str(error)[:500], row_id),
        )
    conn.commit()


def queue_redownload(
    conn,
    reason: str,
    *,
    manifest_path: str,
    filename: str | None = None,
    outbox_row: dict | None = None,
) -> None:
    """Insert a redownload request and (if outbox_row given) flip the outbox row to REDOWNLOAD.

    Idempotent: UNIQUE(manifest_path, filename, reason) plus ON CONFLICT DO NOTHING.
    """
    if reason not in VALID_REASONS:
        raise ValueError(f"Unknown redownload reason: {reason}")

    outbox_id = outbox_row["id"] if outbox_row else None
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {REDOWNLOAD_TABLE} (manifest_path, filename, reason, outbox_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (manifest_path, filename, reason) DO NOTHING
            """,
            (manifest_path, filename, reason, outbox_id),
        )
        if outbox_row is not None:
            cur.execute(
                f"""
                UPDATE {TABLE}
                SET status='REDOWNLOAD', locked_by=NULL, lease_until=NULL
                WHERE id=%s
                """,
                (outbox_id,),
            )
    conn.commit()


def rows_to_units(
    rows: list[dict],
) -> Generator[tuple[dict | None, dict, str | None], None, None]:
    """Convert DB rows to (unit, row, reason) tuples for the main pipeline.

    reason is None on success; otherwise one of VALID_REASONS describing why
    the unit could not be assembled. Paths are absolute (translated from the
    Docker prefix to the host root).

    WARC rows (warc_locator set) are read by seeking the shard; the batch is
    grouped per shard and sorted by offset so reads are forward-only and
    readahead-friendly. Legacy rows keep the per-case fs tree path.
    """
    warc_rows = sorted(
        (r for r in rows if r.get("warc_locator")),
        key=lambda r: _loc_parts(r["warc_locator"])[:2],
    )
    for path, group in groupby(
        warc_rows, key=lambda r: _loc_parts(r["warc_locator"])[0]
    ):
        try:
            fh = open(path, "rb")
        except OSError as exc:
            logger.warning("Shard not found: %s (%s)", path, exc)
            for row in group:
                yield None, row, "path_missing"
            continue
        with fh:
            for row in group:  # offsets ascending -> forward seeks
                yield _warc_row_to_unit(fh, row)

    for row in rows:
        if row.get("warc_locator"):
            continue
        manifest_path = _fix_path(row["manifest"])
        file_path = _fix_path(row["filename"])

        manifest_dir = os.path.dirname(manifest_path)
        file_name_only = (
            os.path.relpath(file_path, manifest_dir)
            if file_path and manifest_dir
            else file_path
        )

        if not os.path.exists(manifest_path):
            logger.warning("[ID %d] Manifest not found: %s", row["id"], manifest_path)
            yield None, row, "path_missing"
            continue

        try:
            with open(manifest_path, encoding="utf-8") as fh:
                meta = json.load(fh)
        except Exception as exc:
            logger.warning("[ID %d] Bad manifest JSON: %s", row["id"], exc)
            yield None, row, "bad_manifest_json"
            continue

        yield (
            {
                "manifest_path": manifest_path,
                "html_files": [file_name_only] if file_name_only else [],
                "rel_from_root": os.path.relpath(manifest_path, FS_ROOT),
                "meta": meta,
            },
            row,
            None,
        )


def _warc_date_to_name(warc_date: str | None) -> str | None:
    """WARC-Date (record write time = download time) -> legacy html filename.

    The old-format name DD-MM-YYYY.HH:MM:SS.html is what html_name_to_iso /
    get_files_version derive version_ts from; without it WARC units would
    fall back to date_reg and re-downloads of a case would tie on version.
    """
    if not warc_date:
        return None
    try:
        dt = datetime.fromisoformat(warc_date.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.strftime("%d-%m-%Y.%H:%M:%S.html")


def enrich_mgs_meta(meta: dict, html) -> dict:
    """Fill identity for an archived mos-gorsud card.

    The MGS crawler stores pages without parsing them, so its manifest carries
    only the url, the court derived from that url, and a dump-seeded id when
    there is one. Everything else has to come from the card itself. A seeded id
    always wins: it is what is already in Elasticsearch, and a case renumbered
    since the dump would otherwise land as a second document.
    """
    if not is_mgs(html):
        return meta
    enriched = dict(meta)
    identity = mgs_identity(html, enriched.get("url", ""))
    if enriched.get("id_final") or enriched.get("ID"):
        identity.pop("id_final", None)
    enriched.update({k: v for k, v in identity.items() if v})
    return enriched


def _group_to_unit(locator: str, meta, html, warc_date) -> dict:
    fname = _warc_date_to_name(warc_date) or locator
    return {
        "manifest_path": locator,
        "rel_from_root": locator,
        "meta": enrich_mgs_meta(meta, html),
        "html_files": [fname],
        "html_bytes_by_name": {fname: html},
    }


def units_from_warc(path: str) -> Generator[dict, None, None]:
    """Stream .warc.gz shards start-to-end for a full reindex, no DB.

    `path` is one shard or a directory of shards. Records are paired into
    (metadata, resource) groups; each group becomes one pipeline unit with a
    seek-compatible locator, identical to what db mode produces.
    """
    if os.path.isdir(path):
        shards = sorted(
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.endswith(".warc.gz")
        )
    else:
        shards = [path]

    for shard in shards:
        with open(shard, "rb") as fh:
            it = ArchiveIterator(fh)
            group_off = group_len = meta = None
            for rec in it:
                # content must be read before offset/length are valid
                if rec.rec_type == "metadata":
                    meta = json.load(rec.content_stream())
                    group_off = it.get_record_offset()
                    group_len = it.get_record_length()
                elif rec.rec_type == "resource" and meta is not None:
                    html = rec.content_stream().read()
                    warc_date = rec.rec_headers.get_header("WARC-Date")
                    group_len += it.get_record_length()
                    yield _group_to_unit(
                        f"{shard}:{group_off}:{group_len}", meta, html, warc_date
                    )
                    meta = None


def _warc_row_to_unit(fh, row: dict) -> tuple[dict | None, dict, str | None]:
    """Read one 2-record group (metadata + resource) at the row's locator."""
    locator = row["warc_locator"]
    _, off, length = _loc_parts(locator)
    fh.seek(off)
    buf = BytesIO(fh.read(length))
    meta, html, warc_date, documents = None, None, None, {}
    try:
        for rec in ArchiveIterator(buf):
            if rec.rec_type == "metadata":
                meta = json.load(rec.content_stream())
            elif rec.rec_type == "resource":
                ctype = rec.rec_headers.get_header("Content-Type") or ""
                body = rec.content_stream().read()
                # an MGS group carries the card plus its documents; courts
                # groups carry the page alone, so the html branch still wins
                if documents is not None and html is not None and \
                        not ctype.startswith("text/html"):
                    documents[rec.rec_headers.get_header("WARC-Target-URI")] = (
                        body, rec.rec_headers.get_header("X-Document-Ext"))
                    continue
                html = body
                warc_date = rec.rec_headers.get_header("WARC-Date")
    except Exception as exc:
        logger.warning("[ID %d] Bad WARC group at %s: %s", row["id"], locator, exc)
        return None, row, "docs_missing"
    if meta is None or html is None:
        logger.warning("[ID %d] Incomplete WARC group at %s", row["id"], locator)
        return None, row, "docs_missing"
    fname = _warc_date_to_name(warc_date) or locator
    return (
        {
            "manifest_path": locator,
            "rel_from_root": locator,
            "meta": enrich_mgs_meta(meta, html),
            "html_files": [fname],
            "html_bytes_by_name": {fname: html},
            "documents": documents,
        },
        row,
        None,
    )
