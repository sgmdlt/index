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

logger = logging.getLogger(__name__)

TABLE = os.environ.get("PG_TABLE", "outbox")
REDOWNLOAD_TABLE = os.environ.get("PG_REDOWNLOAD_TABLE", "redownload_queue")
DOCKER_ROOT = "/app/court_data/"
DOCKER_WARC_ROOT = "/app/court_warc/"
MAX_ATTEMPTS = 5

VALID_REASONS = {"path_missing", "docs_missing", "bad_manifest_json", "build_failed"}


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


def _warc_row_to_unit(fh, row: dict) -> tuple[dict | None, dict, str | None]:
    """Read one 2-record group (metadata + resource) at the row's locator."""
    locator = row["warc_locator"]
    _, off, length = _loc_parts(locator)
    fh.seek(off)
    buf = BytesIO(fh.read(length))
    meta, html, warc_date = None, None, None
    try:
        for rec in ArchiveIterator(buf):
            if rec.rec_type == "metadata":
                meta = json.load(rec.content_stream())
            elif rec.rec_type == "resource":
                html = rec.content_stream().read()
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
            "meta": meta,
            "html_files": [fname],
            "html_bytes_by_name": {fname: html},
        },
        row,
        None,
    )
