"""Court case Elasticsearch indexer — MinIO/S3 source.

Sibling to src/main.py, but reads HTML and manifest.json from an S3-compatible
object store (MinIO) instead of the local filesystem. Imports parse_html /
prepare_doc / to_document / bulk_indexer / metrics unchanged.

Usage:
  python -m src.main_s3 --index sud_s3 [--workers N]
"""

import argparse
import logging
import os
import re
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

import boto3
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

from src.bulk_indexer import BulkIndexer, build_update_item
from src.metrics import Metrics
from src.parse_html import fill_raw_tables
from src.prepare_doc import prepare_doc
from src.to_document import to_document
from src.traverse_s3 import traverse_cases_s3

load_dotenv()

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("elastic_transport.transport").setLevel(logging.ERROR)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

_HERE = Path(__file__).parent

# Hardcoded for the test. Swap to CLI flags only if we keep this past benchmark.
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9100")
S3_BUCKET = os.environ.get("S3_BUCKET", "courts")
S3_PREFIX = os.environ.get("S3_PREFIX", "2026/")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


# Worker-process lazy singleton. boto3 clients are not fork-safe; build inside
# each child the first time the worker fetches an object.
_WORKER_S3 = None


def _get_worker_s3():
    global _WORKER_S3
    if _WORKER_S3 is None:
        _WORKER_S3 = _build_s3_client()
    return _WORKER_S3


def _parse_html_object(args: tuple) -> tuple:
    """Worker: GET one HTML object, return (prepared_doc, fetch_ms, n_bytes)."""
    bucket, key = args
    t0 = time.perf_counter()
    body = _get_worker_s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    fetch_ms = (time.perf_counter() - t0) * 1000.0
    raw = fill_raw_tables(body)
    return prepare_doc(raw), fetch_ms, len(body)


def _bucket_key(unit: dict) -> str:
    path = unit.get("rel_from_root", "")
    m = re.search(r"\d{4}/\d{2}", path)
    return m.group(0) if m else "default"


def _ensure_index(es, index: str) -> None:
    import json as _json
    try:
        if es.indices.exists(index=index):
            logger.info("Index %r already exists; skipping creation.", index)
            return
    except Exception:
        pass
    try:
        with open(_HERE / "mapping_new.json", encoding="utf-8") as fh:
            mapping = _json.load(fh)
        es.indices.create(index=index, **mapping)
        logger.info("Index %r created.", index)
    except Exception as exc:
        status = getattr(getattr(exc, "meta", None), "status", None)
        if status == 400:
            return
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


def _run_pipelined(
    units_iter,
    pool: ProcessPoolExecutor,
    indexer: BulkIndexer,
    metrics: Metrics,
    max_workers: int,
) -> None:
    """Same shape as src.main._run_pipelined, but submits (bucket, key) pairs."""
    MAX_INFLIGHT = max_workers * 8

    pending: dict = {}
    unit_state: dict = {}
    units_iter = iter(units_iter)
    units_exhausted = False

    while True:
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
                continue

            rel = unit["rel_from_root"]
            manifest_dir = os.path.dirname(unit["manifest_path"])  # S3 key prefix
            unit_state[rel] = {
                "unit": unit,
                "total": len(html_files),
                "order": list(html_files),
                "results": {},
                "t0": metrics.now_ms(),
                "had_error": False,
            }
            for fname in html_files:
                key = f"{manifest_dir}/{fname}"
                fut = pool.submit(_parse_html_object, (S3_BUCKET, key))
                pending[fut] = (rel, fname)

        if not pending:
            break

        done, _ = wait(list(pending), return_when=FIRST_COMPLETED)

        for fut in done:
            rel, fname = pending.pop(fut)
            state = unit_state.get(rel)
            if state is None:
                continue

            try:
                prepared, fetch_ms, n_bytes = fut.result()
                metrics.add_time("readHtmlMs", fetch_ms)
                metrics.inc("htmlBytes", n_bytes)
                state["results"][fname] = prepared
            except Exception as exc:
                logger.error("S3 fetch/parse error in %s/%s: %s", rel, fname, exc)
                metrics.inc("parseErrors")
                state["results"][fname] = None
                state["had_error"] = True

            if len(state["results"]) < state["total"]:
                continue

            metrics.add_time("parseMs", metrics.now_ms() - state["t0"])
            unit = state["unit"]
            del unit_state[rel]

            pairs = [
                (f, state["results"][f])
                for f in state["order"]
                if state["results"][f] is not None
            ]
            if not pairs:
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

            bkey = _bucket_key(unit)
            for filename, parsed in zip(html_files_done, parsed_files_done):
                update = build_update_item(
                    doc=doc,
                    doc_data=parsed,
                    unit=unit,
                    filename=filename,
                    source_path=f"s3://{S3_BUCKET}/{os.path.dirname(unit['manifest_path'])}/{filename}",
                )
                if update:
                    metrics.inc("docsPrepared")
                    indexer.add(bkey, update)

        metrics.log()


def parse_args():
    p = argparse.ArgumentParser(description="Court case ES indexer — MinIO source")
    p.add_argument("--index", required=True)
    p.add_argument("--es", default="http://localhost:9222")
    p.add_argument("--user", default=None)
    p.add_argument("--pass", dest="password", default=None)
    p.add_argument("--api-key", dest="api_key", default=None)
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--workers", type=int, default=os.cpu_count() or 4)
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
    metrics = Metrics(log_every_sec=5.0)

    _ensure_index(es, args.index)

    indexer = BulkIndexer(
        es_client=es,
        index=args.index,
        metrics=metrics,
        bulk_docs=1500,
        flush_interval=15.0,
    )

    orig_settings = _fast_ingest_on(es, args.index)

    # Listing client lives in the parent process; safe to share.
    list_client = _build_s3_client()
    units = traverse_cases_s3(
        list_client,
        S3_BUCKET,
        S3_PREFIX,
        on_manifest_ms=lambda ms: metrics.add_time("manifestFetchMs", ms),
    )

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        try:
            _run_pipelined(units, pool, indexer, metrics, args.workers)
        finally:
            indexer.drain()
            _fast_ingest_off(es, args.index, orig_settings)
            metrics.log(force=True)


if __name__ == "__main__":
    main()
