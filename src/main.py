"""Court case Elasticsearch indexer — main entry point.

Usage:
  python -m src.main --index my-index --root /path/to/court_cases [options]

Options:
  --index     (required) Elasticsearch index name
  --root      (required) Root directory with court cases (filesystem mode)
  --es        Elasticsearch URL (default: http://localhost:9222)
  --user      ES basic auth username
  --pass      ES basic auth password
  --api-key   ES API key
  --filter    Only process units whose path contains this string
  --jsonl     Path to a .jsonl file (JSONL mode instead of filesystem)
  --folders   Path to a text file with relative manifest paths (one per line)
  --db        Process from PostgreSQL outbox (DB mode)
  --insecure  Disable TLS certificate verification
  --workers   Number of parallel HTML parse workers (default: CPU count)
"""

import argparse
import gzip
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

from src.bulk_indexer import BulkIndexer, build_update_item
from src.load_from_db import (
    mark_done,
    mark_failed,
    poll_batch,
    queue_redownload,
    rows_to_units,
    units_from_warc,
)
from src.metrics import Metrics
from src.parse_html import fill_raw_tables
#from src.parse_html_sx import fill_raw_tables as fill_raw_tables
from src.prepare_doc import prepare_doc
from src.to_document import to_document
from src.traverse import traverse_cases
from src.utils.text import normalize_jsonl_text

load_dotenv()

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("elastic_transport.transport").setLevel(logging.ERROR)

_HERE = Path(__file__).parent  # project root (index/)

def _parse_html_bytes(html_bytes: bytes, origin: str) -> dict:
    """Parse raw case HTML into a prepared doc; origin is for error messages."""
    raw = fill_raw_tables(html_bytes)
    prepared = prepare_doc(raw)
    # No-progress docs are fine; skip only docs with no extracted field at all
    # (Postgres outage dumps and truncated pages).
    if not any(v for k, v in prepared.items() if k != "_document_text"):
        kind = "sql_error" if (b"<b>QUERY:</b>" in html_bytes or b"server closed the connection" in html_bytes) else "empty_html"
        raise ValueError(f"{kind}: {origin}")
    return prepared


def _parse_html_file(args: tuple) -> tuple:
    """Worker: read one HTML file, return (prepared_doc, read_ms, n_bytes)."""
    html_path, _manifest_dir = args
    t0 = time.perf_counter()
    with open(html_path, "rb") as fh:
        html_bytes = fh.read()
    read_ms = (time.perf_counter() - t0) * 1000.0
    return _parse_html_bytes(html_bytes, html_path), read_ms, len(html_bytes)


def _parse_warc_payload(args: tuple) -> tuple:
    """Worker: parse HTML bytes already read from a WARC group."""
    html_bytes, origin = args
    return _parse_html_bytes(html_bytes, origin), 0.0, len(html_bytes)


def _normalize_strings(node):
    """Recursively normalize every string leaf to match parser output."""
    if isinstance(node, str):
        return normalize_jsonl_text(node)
    if isinstance(node, list):
        return [_normalize_strings(x) for x in node]
    if isinstance(node, dict):
        return {k: _normalize_strings(v) for k, v in node.items()}
    return node


_ID_FINAL_RE = re.compile(rb'"id_final"\s*:\s*"([^"]+)"')
_LINK_RE = re.compile(rb'"link"\s*:\s*"([^"]+)"')


def _extract_ids_from_raw(buf: bytes) -> tuple[str | None, str | None]:
    """Regex-pull id_final + link from possibly-truncated raw JSONL bytes."""
    id_m = _ID_FINAL_RE.search(buf)
    link_m = _LINK_RE.search(buf)
    id_final = id_m.group(1).decode("utf-8", errors="replace") if id_m else None
    link = link_m.group(1).decode("utf-8", errors="replace") if link_m else None
    return id_final, link


def _enqueue_redownload(pg_conn, id_final: str | None, link: str | None) -> bool:
    """Insert bad JSONL record into redownload_queue. Returns True on success."""
    manifest_path = id_final or link or "unknown"
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO redownload_queue (manifest_path, filename, reason)
                VALUES (%s, %s, 'bad_jsonl_record')
                ON CONFLICT (manifest_path, filename, reason) DO NOTHING
                """,
                (manifest_path, link),
            )
        pg_conn.commit()
        return True
    except Exception as exc:
        logger.warning("redownload_queue insert failed: %s", exc)
        try:
            pg_conn.rollback()
        except Exception:
            pass
        return False


def _open_jsonl(jsonl_path: str):
    """Open JSONL source: '-' = stdin, '*.gz' = gzip, else plain binary."""
    if jsonl_path == "-":
        return sys.stdin.buffer
    if jsonl_path.endswith(".gz"):
        return gzip.open(jsonl_path, "rb")
    return open(jsonl_path, "rb")


def _units_from_jsonl(jsonl_path: str):
    MAX_BUFFER_LINES = 1000
    buf_parts: list[bytes] = []
    buf_start_lineno = 0
    bad_basename = "stdin" if jsonl_path == "-" else os.path.basename(jsonl_path)
    bad_path = bad_basename + ".bad"
    pg_conn = None
    pg_tried = False

    def _get_pg():
        nonlocal pg_conn, pg_tried
        if pg_conn is not None or pg_tried:
            return pg_conn
        pg_tried = True
        try:
            pg_conn = _open_pg()
        except Exception as exc:
            logger.warning("Could not open PG for redownload_queue: %s — falling back to .bad only", exc)
            pg_conn = None
        return pg_conn

    def _dump_bad(buf: bytes, start_lineno: int, reason: str) -> None:
        id_final, link = _extract_ids_from_raw(buf)
        conn = _get_pg()
        if conn is not None:
            _enqueue_redownload(conn, id_final, link)
        try:
            with open(bad_path, "ab") as bfh:
                header = f"# line={start_lineno} reason={reason} id_final={id_final} link={link}\n".encode()
                bfh.write(header)
                bfh.write(buf)
                bfh.write(b"\n")
        except OSError as exc:
            logger.warning("Could not write bad-record file %s: %s", bad_path, exc)

    with _open_jsonl(jsonl_path) as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.rstrip(b"\r\n")
            if not raw and not buf_parts:
                continue
            if not buf_parts:
                buf_start_lineno = lineno
            buf_parts.append(raw)
            buf = b"\n".join(buf_parts)
            try:
                line = buf.decode("utf-8", errors="replace")
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                if len(buf_parts) >= MAX_BUFFER_LINES:
                    logger.error("JSONL giving up on record at line %d after %d buffered lines -> %s", buf_start_lineno, len(buf_parts), bad_path)
                    _dump_bad(buf, buf_start_lineno, f"max_buffer:{exc}")
                    buf_parts = []
                continue
            buf_parts = []
            if "raw_tables" in data:
                data["raw_tables"] = _normalize_strings(data["raw_tables"])
            yield {
                "is_jsonl": True,
                "data": data,
                "rel_from_root": data.get("id_final", "unknown"),
            }
        if buf_parts:
            buf = b"\n".join(buf_parts)
            logger.error("JSONL trailing unparsed record at line %d (%d lines) -> %s", buf_start_lineno, len(buf_parts), bad_path)
            _dump_bad(buf, buf_start_lineno, "trailing_eof")


def _append_failed_unit(rel_from_root: str, log_file: str | None) -> None:
    """Append a relative manifest path to the failed-units log for later reprocessing."""
    if not log_file:
        return
    try:
        with open(log_file, "a", encoding="utf-8") as fh:
            fh.write(rel_from_root + "\n")
    except OSError as exc:
        logger.warning("Could not write to failed-units log %s: %s", log_file, exc)


def _units_from_folders(
    folders_path: str,
    root: str,
    failed_log: str | None = None,
    on_redownload=None,
):
    with open(folders_path, encoding="utf-8") as fh:
        for line in fh:
            rel = line.strip()
            if not rel:
                continue
            abs_manifest = os.path.join(root, rel)
            case_dir = os.path.dirname(abs_manifest)
            if not os.path.exists(abs_manifest):
                logger.warning("Manifest not found: %s", rel)
                if on_redownload:
                    on_redownload("path_missing", abs_manifest)
                else:
                    _append_failed_unit(rel, failed_log)
                continue
            try:
                with open(abs_manifest, encoding="utf-8") as mf:
                    meta = json.load(mf)
            except Exception as exc:
                logger.error("Bad manifest %s: %s", rel, exc)
                if on_redownload:
                    on_redownload("bad_manifest_json", abs_manifest)
                else:
                    _append_failed_unit(rel, failed_log)
                continue
            try:
                html_files = sorted(
                    f for f in os.listdir(case_dir) if f.lower().endswith(".html")
                )
            except Exception as exc:
                logger.error("Cannot list %s: %s", case_dir, exc)
                if on_redownload:
                    on_redownload("docs_missing", abs_manifest)
                else:
                    _append_failed_unit(rel, failed_log)
                continue
            if not html_files:
                logger.warning("Empty case dir: %s", rel)
                if on_redownload:
                    on_redownload("docs_missing", abs_manifest)
                else:
                    _append_failed_unit(rel, failed_log)
                continue
            yield {
                "manifest_path": abs_manifest,
                "rel_from_root": rel,
                "meta": meta,
                "html_files": html_files,
            }


def _bucket_key(unit: dict) -> str:
    path = unit.get("rel_from_root", "")
    m = re.search(r"\d{4}/\d{2}", path)
    return m.group(0) if m else "default"


def _ensure_index(es, index: str) -> None:
    try:
        if es.indices.exists(index=index):
            logger.info("Index %r already exists; skipping creation.", index)
            return
    except Exception:
        pass  # нет прав на view_index_metadata — пробуем создать
    try:
        with open(_HERE / "mapping_new.json", encoding="utf-8") as fh:
            mapping = json.load(fh)
        es.indices.create(index=index, **mapping)
        logger.info("Index %r created.", index)
    except Exception as exc:
        status = getattr(getattr(exc, "meta", None), "status", None)
        if status == 400:
            return  # already exists
        if status == 403:
            logger.warning("No permission to create index %r; assuming it exists.", index)
            return
        logger.error("Index creation error: %s", exc)


def _fast_ingest_on(es, index: str) -> dict:
    try:
        resp = es.indices.get_settings(index=index)
        orig = resp.get(index, {}).get("settings", {}).get("index", {})
        es.indices.put_settings(
            index=index,
            body={"index": {"refresh_interval": "-1", "number_of_replicas": "0"}},
        )
        return {
            "refresh_interval": orig.get("refresh_interval", "1s"),
            "number_of_replicas": orig.get("number_of_replicas", "1"),
        }
    except Exception as exc:
        status = getattr(getattr(exc, "meta", None), "status", None)
        if status == 403:
            logger.info("Fast ingest unsupported (insufficient privileges); continuing without it.")
        else:
            logger.warning("Could not set fast ingest settings: %s", exc)
        return {}


def _fast_ingest_off(es, index: str, orig: dict) -> None:
    if not orig:
        return
    try:
        es.indices.put_settings(
            index=index,
            body={
                "index": {
                    "refresh_interval": orig.get("refresh_interval"),
                    "number_of_replicas": orig.get("number_of_replicas"),
                }
            },
        )
        es.indices.refresh(index=index)
    except Exception as exc:
        logger.warning("Could not restore index settings: %s", exc)


# Unit processing

def _process_unit(
    unit: dict,
    pool: ProcessPoolExecutor,
    indexer: BulkIndexer,
    metrics: Metrics,
    failed_log: str | None = None,
) -> bool:
    """Parse + index a single unit. Returns True if at least one doc was indexed."""
    metrics.inc("unitsSeen")
    html_files = unit.get("html_files", [])
    metrics.inc("htmlFilesSeen", len(html_files))

    manifest_dir = os.path.dirname(unit["manifest_path"])
    html_paths = [(os.path.join(manifest_dir, f), manifest_dir) for f in html_files]

    t0 = metrics.now_ms()
    ordered_futures = [pool.submit(_parse_html_file, args) for args in html_paths]
    pairs: list[tuple[str, dict]] = []
    for future, fname in zip(ordered_futures, html_files):
        try:
            prepared, read_ms, n_bytes = future.result()
            metrics.add_time("readHtmlMs", read_ms)
            metrics.inc("htmlBytes", n_bytes)
            pairs.append((fname, prepared))
        except Exception as exc:
            logger.error("Parse error in %s/%s: %s", unit.get("rel_from_root"), fname, exc)
            metrics.inc("parseErrors")
            # Drop the failed file — do not emit an empty document entry
    html_files = [p[0] for p in pairs]
    parsed_files = [p[1] for p in pairs]
    metrics.add_time("parseMs", metrics.now_ms() - t0)

    t0 = metrics.now_ms()
    doc = to_document(
        manifest_path=unit["manifest_path"],
        rel_from_root=unit["rel_from_root"],
        meta=unit["meta"],
        html_files=html_files,
        parsed_files=parsed_files,
    )
    metrics.add_time("toDocMs", metrics.now_ms() - t0)

    bkey = _bucket_key(unit)
    indexed_any = False
    for filename, parsed in zip(html_files, parsed_files):
        update = build_update_item(
            doc=doc,
            doc_data=parsed,
            unit=unit,
            filename=filename,
            source_path=os.path.join(
                os.path.dirname(unit["rel_from_root"]), filename
            ),
        )
        if update:
            metrics.inc("docsPrepared")
            indexer.add(bkey, update)
            indexed_any = True
        else:
            _append_failed_unit(unit["rel_from_root"], failed_log)
    return indexed_any


def _run_pipelined(
    units_iter,
    pool: ProcessPoolExecutor,
    indexer: BulkIndexer,
    metrics: Metrics,
    max_workers: int,
    failed_log: str | None = None,
    on_unit_complete=None,
    bucket_key_fn=None,
) -> None:

    MAX_INFLIGHT = max_workers * 8

    pending: dict = {}    # future -> (uid, fname)
    unit_state: dict = {} # uid -> {unit, total, order, results, t0}
    units_iter = iter(units_iter)
    units_exhausted = False
    next_uid = 0

    while True:
        # Fill worker queue up to the backpressure limit
        while len(pending) < MAX_INFLIGHT and not units_exhausted:
            try:
                unit = next(units_iter)
            except StopIteration:
                units_exhausted = True
                break

            html_files = unit.get("html_files", [])
            metrics.inc("unitsSeen")
            metrics.inc("htmlFilesSeen", len(html_files))

            if not html_files:
                if on_unit_complete:
                    on_unit_complete(unit, False)
                continue

            uid = next_uid
            next_uid += 1
            unit_state[uid] = {
                "unit": unit,
                "total": len(html_files),
                "order": list(html_files),  # preserve original sort order
                "results": {},              # fname -> parsed dict | None (failed)
                "completed": 0,             # count of futures resolved (handles dup fnames)
                "t0": metrics.now_ms(),
                "had_error": False,
            }
            bytes_map = unit.get("html_bytes_by_name")
            for fname in html_files:
                if bytes_map is not None:
                    # WARC unit: payload already read in rows_to_units;
                    # manifest_path is a locator, not a directory
                    fut = pool.submit(
                        _parse_warc_payload,
                        (bytes_map[fname], unit["manifest_path"]),
                    )
                else:
                    manifest_dir = os.path.dirname(unit["manifest_path"])
                    path = os.path.join(manifest_dir, fname)
                    fut = pool.submit(_parse_html_file, (path, manifest_dir))
                pending[fut] = (uid, fname)

        if not pending:
            break

        # Block until at least one future completes
        done, _ = wait(list(pending), return_when=FIRST_COMPLETED)

        for fut in done:
            uid, fname = pending.pop(fut)
            state = unit_state.get(uid)
            if state is None:
                continue

            try:
                prepared, read_ms, n_bytes = fut.result()
                metrics.add_time("readHtmlMs", read_ms)
                metrics.inc("htmlBytes", n_bytes)
                state["results"][fname] = prepared
            except Exception as exc:
                logger.error("Parse error in %s/%s: %s", state["unit"].get("rel_from_root"), fname, exc)
                metrics.inc("parseErrors")
                state["results"][fname] = None  # drop file, don't emit empty doc
                state["had_error"] = True
                # _parse_html_bytes raises ValueError("<kind>: <origin>");
                # surface the kind so DB mode queues a precise redownload reason
                kind = str(exc).split(":", 1)[0]
                if kind in ("sql_error", "empty_html"):
                    state["unit"]["_fail_reason"] = kind
            state["completed"] += 1

            # Not all futures for this unit resolved yet — keep waiting
            if state["completed"] < state["total"]:
                continue

            # All futures for this unit are done — assemble and index
            metrics.add_time("parseMs", metrics.now_ms() - state["t0"])
            unit = state["unit"]
            rel = unit["rel_from_root"]
            if state["had_error"]:
                _append_failed_unit(rel, failed_log)
            del unit_state[uid]

            # Reconstruct in original timestamp-sorted order, skip failed files
            pairs = [
                (f, state["results"][f])
                for f in state["order"]
                if state["results"][f] is not None
            ]
            if not pairs:
                if on_unit_complete:
                    on_unit_complete(unit, False)
                continue

            html_files_done = [p[0] for p in pairs]
            parsed_files_done = [p[1] for p in pairs]

            t0 = metrics.now_ms()
            doc = to_document(
                manifest_path=unit["manifest_path"],
                rel_from_root=unit["rel_from_root"],
                meta=unit["meta"],
                html_files=html_files_done,
                parsed_files=parsed_files_done,
            )
            metrics.add_time("toDocMs", metrics.now_ms() - t0)

            bkey = bucket_key_fn(unit) if bucket_key_fn else _bucket_key(unit)
            indexed_any = False
            is_warc = unit.get("html_bytes_by_name") is not None
            for filename, parsed in zip(html_files_done, parsed_files_done):
                update = build_update_item(
                    doc=doc,
                    doc_data=parsed,
                    unit=unit,
                    filename=filename,
                    # WARC: rel_from_root is a locator, dirname is meaningless
                    source_path=unit["manifest_path"]
                    if is_warc
                    else os.path.join(
                        os.path.dirname(unit["rel_from_root"]), filename
                    ),
                )
                if update:
                    metrics.inc("docsPrepared")
                    indexer.add(bkey, update)
                    indexed_any = True
                else:
                    _append_failed_unit(unit["rel_from_root"], failed_log)
            if on_unit_complete:
                on_unit_complete(unit, indexed_any)

        metrics.log()


# Source runners

def _derive_id_final(data: dict) -> str:
    if data.get("id_final"):
        return data["id_final"]
    case_number = data.get("case_number", "")
    if not case_number:
        return ""
    court_code = data.get("court_code", "")
    date_reg = data.get("date_reg", "")
    return f"{court_code}_{date_reg}_{case_number.split('/')[0]}"


def _run_jsonl(
    jsonl_path: str,
    indexer: BulkIndexer,
    metrics: Metrics,
    failed_log: str | None = None,
) -> None:
    for unit in _units_from_jsonl(jsonl_path):
        metrics.inc("unitsSeen")
        data = {**unit["data"], "id_final": _derive_id_final(unit["data"])}

        # Extract fields from raw_tables via rule engine
        t0 = metrics.now_ms()
        prepared = prepare_doc(data)
        metrics.add_time("parseMs", metrics.now_ms() - t0)

        # Build document structure with metadata — same path as filesystem mode.
        # JSONL top-level fields (region, court_name, …) serve as manifest metadata;
        # to_document sets group_id, version_ts, meta, and applies instance_case_type_mapping.
        t0 = metrics.now_ms()
        doc = to_document(
            manifest_path="",
            rel_from_root=data["id_final"],
            meta=data,
            html_files=[],
            parsed_files=[prepared],
        )
        metrics.add_time("toDocMs", metrics.now_ms() - t0)

        update = build_update_item(
            doc=doc,
            doc_data=prepared,
            unit=unit,
            filename=os.path.basename(jsonl_path),
            source_path=f"jsonl:{unit['rel_from_root']}",
        )
        if update:
            metrics.inc("docsPrepared")
            indexer.add("jsonl_bucket", update)
        else:
            _append_failed_unit(unit["rel_from_root"], failed_log)

        metrics.log()


def _open_pg():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=int(os.environ.get("PGPORT", "8432")),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def _run_db(
    pool: ProcessPoolExecutor,
    indexer: BulkIndexer,
    metrics: Metrics,
    max_workers: int,
) -> None:
    worker_id = f"worker-{os.getpid()}"
    conn = _open_pg()
    DONE_BATCH = 200
    done_buf: list[int] = []

    def flush_done(force: bool = False) -> None:
        if not done_buf:
            return
        if not force and len(done_buf) < DONE_BATCH:
            return
        mark_done(conn, done_buf)
        done_buf.clear()

    def units_gen():
        while True:
            rows = poll_batch(conn, worker_id)
            if not rows:
                return
            for unit, row, reason in rows_to_units(rows):
                if reason:
                    queue_redownload(
                        conn,
                        reason,
                        manifest_path=row["manifest"],
                        filename=row["filename"],
                        outbox_row=row,
                    )
                    continue
                unit["_row"] = row
                yield unit

    def on_complete(unit: dict, indexed: bool) -> None:
        row = unit.get("_row")
        if row is None:
            return
        if indexed:
            done_buf.append(row["id"])
            flush_done(force=False)
        else:
            queue_redownload(
                conn,
                unit.get("_fail_reason") or "build_failed",
                manifest_path=row["manifest"],
                filename=row["filename"],
                outbox_row=row,
            )

    try:
        _run_pipelined(
            units_gen(),
            pool,
            indexer,
            metrics,
            max_workers,
            on_unit_complete=on_complete,
            bucket_key_fn=lambda _u: "db",
        )
        flush_done(force=True)
    finally:
        flush_done(force=True)
        conn.close()


# CLI

def parse_args():
    p = argparse.ArgumentParser(description="Court case ES indexer")
    p.add_argument("--index", required=True, help="Elasticsearch index name")
    p.add_argument("--root", help="Root directory with court cases")
    p.add_argument("--es", default="http://localhost:9222", help="ES URL")
    p.add_argument("--user", default=None)
    p.add_argument("--pass", dest="password", default=None)
    p.add_argument("--api-key", dest="api_key", default=None)
    p.add_argument("--filter", dest="filter_str", default=None)
    p.add_argument("--jsonl", default=None)
    p.add_argument("--folders", default=None)
    p.add_argument(
        "--warc",
        default=None,
        help="Shard .warc.gz file or directory of shards: stream start-to-end for a full reindex (no DB)",
    )
    p.add_argument("--db", action="store_true")
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--workers", type=int, default=os.cpu_count() or 6)
    p.add_argument(
        "--failed-log",
        dest="failed_log",
        default="failed_units.txt",
        help="File to append failed manifest/HTML paths for later reprocessing (default: failed_units.txt)",
    )
    p.add_argument(
        "--redownload",
        default=True,
        action="store_true",
        help="In fs/folders modes, queue failed cases into Postgres redownload_queue instead of writing to --failed-log",
    )
    return p.parse_args()


def main():
    args = parse_args()

    es_opts: dict = {"request_timeout": 120}
    if args.api_key:
        es_opts["api_key"] = args.api_key
    elif args.user and args.password:
        es_opts["basic_auth"] = (args.user, args.password)
    elif os.environ.get("ES_API_KEY"):
        es_opts["api_key"] = os.environ["ES_API_KEY"]
    elif os.environ.get("ES_USER"):
        es_opts["basic_auth"] = (
            os.environ["ES_USER"],
            os.environ.get("SGMDLT_PASSWORD", ""),
        )
    if args.insecure and args.es.startswith("https://"):
        es_opts["verify_certs"] = False

    es = Elasticsearch(args.es, **es_opts)
    if args.root:
        root = os.path.realpath(args.root)
    metrics = Metrics(log_every_sec=5.0)
    t_start = time.perf_counter()

    _ensure_index(es, args.index)

    bulk_docs = 500 if args.db else (1000 if args.jsonl else 1500)
    indexer = BulkIndexer(
        es_client=es,
        index=args.index,
        metrics=metrics,
        bulk_docs=bulk_docs,
        flush_interval=15.0,
    )

    orig_settings = _fast_ingest_on(es, args.index)

    pg_conn = _open_pg() if args.redownload and not args.db and not args.warc else None
    on_redownload = None
    if pg_conn is not None:
        def on_redownload(reason: str, manifest_path: str, filename: str | None = None):
            queue_redownload(
                pg_conn, reason, manifest_path=manifest_path, filename=filename
            )

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        try:
            if args.jsonl:
                _run_jsonl(args.jsonl, indexer, metrics, failed_log=args.failed_log)
            elif args.warc:
                _run_pipelined(
                    units_from_warc(args.warc),
                    pool, indexer, metrics, args.workers,
                    failed_log=args.failed_log,
                )
            elif args.db:
                _run_db(pool, indexer, metrics, args.workers)
            elif args.folders:
                _run_pipelined(
                    _units_from_folders(
                        args.folders, root,
                        failed_log=args.failed_log,
                        on_redownload=on_redownload,
                    ),
                    pool, indexer, metrics, args.workers,
                    failed_log=args.failed_log,
                )
            else:
                _run_pipelined(
                    traverse_cases(
                        root,
                        filter_str=args.filter_str,
                        failed_log=args.failed_log,
                        on_manifest_ms=lambda ms: metrics.add_time("manifestFetchMs", ms),
                        on_redownload=on_redownload,
                    ),
                    pool, indexer, metrics, args.workers,
                    failed_log=args.failed_log,
                )
        finally:
            indexer.drain()
            logger.info("Indexing complete, flushing remaining buffers and restoring index settings")
            _fast_ingest_off(es, args.index, orig_settings)
            metrics.log(force=True)
            if pg_conn is not None:
                pg_conn.close()
            elapsed = time.perf_counter() - t_start
            c = metrics.counters
            print(
                f"=== Indexation COMPLETED: index={args.index} "
                f"units={c['unitsSeen']} "
                f"docsPrepared={c['docsPrepared']} "
                f"docsIndexed={c['docsIndexed']} "
                f"parseErrors={c['parseErrors']} "
                f"elapsed={elapsed:.1f}s ===",
                flush=True,
            )


if __name__ == "__main__":
    main()
