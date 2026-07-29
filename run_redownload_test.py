"""Throwaway: re-run redownload_queue rows (default reason=build_failed) through
the real pipeline into a test index. Not wired into main.py."""
import os, sys, time
from concurrent.futures import ProcessPoolExecutor
from elasticsearch import Elasticsearch

from src.load_from_db import rows_to_units, _fix_path
from src.metrics import Metrics
from src.bulk_indexer import BulkIndexer
from src.main import _run_pipelined, _ensure_index, _fast_ingest_on, _fast_ingest_off, _open_pg

REASON = sys.argv[1] if len(sys.argv) > 1 else "build_failed"
LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else 0
INDEX = os.environ.get("TEST_INDEX", "test_stop_words")
ES_URL = "http://localhost:9222"
USER, PASS = os.environ["ES_USER"], os.environ["ES_PASS"]


def fetch_rows():
    conn = _open_pg()
    with conn.cursor() as cur:
        q = ("SELECT id, manifest_path, filename FROM redownload_queue "
             "WHERE reason=%s ORDER BY id")
        if LIMIT:
            q += f" LIMIT {LIMIT}"
        cur.execute(q, (REASON,))
        rows = [{"id": r[0], "manifest": r[1], "filename": r[2], "attempts": 0}
                for r in cur.fetchall()]
    conn.close()
    # only rows whose html file actually exists on disk
    rows = [r for r in rows if os.path.exists(_fix_path(r["filename"]))]
    return rows


def units():
    for unit, _row, reason in rows_to_units(fetch_rows()):
        if reason is None:
            yield unit


def main():
    rows = fetch_rows()
    print(f"reason={REASON} rows={len(rows)} index={INDEX}")
    es = Elasticsearch(ES_URL, basic_auth=(USER, PASS), request_timeout=120)
    metrics = Metrics(log_every_sec=5.0)
    _ensure_index(es, INDEX)
    indexer = BulkIndexer(es_client=es, index=INDEX, metrics=metrics,
                          bulk_docs=500, flush_interval=15.0)
    orig = _fast_ingest_on(es, INDEX)
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=int(os.environ.get("WORKERS", "4"))) as pool:
        try:
            _run_pipelined(units(), pool, indexer, metrics,
                           int(os.environ.get("WORKERS", "4")))
        finally:
            indexer.drain()
            _fast_ingest_off(es, INDEX, orig)
    print(f"elapsed={time.perf_counter()-t0:.1f}s")
    metrics.log()


if __name__ == "__main__":
    main()
