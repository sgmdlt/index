"""Elasticsearch bulk indexer with bucket batching, retry, and Python-side merging."""

import json
import logging
import re
import threading
import time
from datetime import datetime
from pathlib import Path

from .utils.dates import html_name_to_iso
from .utils.paths import norm_path, dir_of_rel

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent.parent  # project root (index/)

_rules: dict = json.loads((Path(__file__).parent / "elastic_index_dict.json").read_text(encoding="utf-8"))
MERGE_FIELDS: set[str] = {
    "html_files",
    "judge",
    *[k for k, v in _rules.items() if v.get("type") == "merge"],
}
del _rules

ARRAY_MERGE_FIELDS: frozenset[str] = frozenset(MERGE_FIELDS)


# ID and document helpers

def resolve_doc_id(doc: dict) -> str:
    """Return the document's id_final verbatim for use as the ES _id."""
    return str(doc.get("id_final") or "")


def _to_array(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return [x for x in v if x is not None and x != ""]
    if isinstance(v, str) and v == "":
        return []
    return [v]



# ---------------------------------------------------------------------------
# Python-side merge logic
# ---------------------------------------------------------------------------


def _merge_array(existing_arr: list, incoming_arr: list) -> list:
    combined = list(existing_arr) + list(incoming_arr)
    # dict.fromkeys preserves insertion order and removes duplicates
    return list(dict.fromkeys(x for x in combined if x is not None and x != ""))


def _doc_canon(text: str) -> str:
    return " ".join(text.split()) if text else ""


def _merge_documents(existing_arr: list, incoming_arr: list) -> list:
    seen_canon: set[str] = {
        _doc_canon(entry["text"])
        for entry in existing_arr
        if isinstance(entry, dict) and entry.get("text")
    }
    result = list(existing_arr)
    for entry in incoming_arr:
        if not isinstance(entry, dict):
            continue
        canon = _doc_canon(entry.get("text", ""))
        if canon and canon not in seen_canon:
            seen_canon.add(canon)
            result.append(entry)
    return result


def merge_docs(existing: dict | None, incoming: dict) -> dict:
    if existing is None:
        result = dict(incoming)
        result["version"] = 1
    else:
        result = dict(existing)
        for key, value in incoming.items():
            if key == "documents":
                result[key] = _merge_documents(existing.get(key) or [], value or [])
            elif key == "progress":
                continue  # handled below
            elif key in ARRAY_MERGE_FIELDS:
                result[key] = _merge_array(_to_array(existing.get(key)), _to_array(value))
            elif value is not None and value != "" and value != [] and value != {}:
                result[key] = value
        result["version"] = (existing.get("version") or 0) + 1

    result["progress"] = merge_progress(
        (existing.get("progress") if existing else None) or [],
        incoming.get("progress") or [],
    )
    result["version_ts"] = max(
        (existing.get("version_ts") if existing else "") or "",
        incoming.get("version_ts") or "",
    )
    result["doc_count"] = len(result.get("documents") or [])
    return result


_PROGRESS_HEAD = ("date", "event")


def _progress_head(row: dict) -> tuple[str, str]:
    return (row.get("date") or "", row.get("event") or "")


def _hashable(v):
    if isinstance(v, list):
        return ("__list__", tuple(_hashable(x) for x in v))
    if isinstance(v, dict):
        return ("__dict__", tuple(sorted((k, _hashable(val)) for k, val in v.items())))
    return v


def _progress_rest(row: dict) -> set:
    return {(k, _hashable(v)) for k, v in row.items() if k not in _PROGRESS_HEAD}


def _progress_sort_key(row: dict):
    date = row.get("date") or ""
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})", date)
    if not m:
        return datetime.max
    dd, mm, yyyy = m.groups()
    try:
        return datetime(int(yyyy), int(mm), int(dd))
    except ValueError:
        return datetime.max


def _progress_to_str(row: dict) -> str:
    head = f"{row.get('date', '')} {row.get('event', '')}".strip()
    rest = [f"{k}: {v}" for k, v in row.items() if k not in _PROGRESS_HEAD]
    return head + (" | " + ", ".join(rest) if rest else "")


def _progress_from_str(s: str) -> dict:
    head, _, rest_str = s.partition(" | ")
    date, _, event = head.partition(" ")
    row: dict = {"date": date, "event": event}
    if rest_str:
        for part in rest_str.split(", "):
            if ": " in part:
                k, v = part.split(": ", 1)
                row[k] = v
    return row


def _as_progress_dict(row) -> dict | None:
    if isinstance(row, dict):
        return row
    if isinstance(row, str) and row:
        return _progress_from_str(row)
    return None


def merge_progress(existing: list, incoming: list) -> list[dict]:
    result: list[dict] = []
    for row in list(existing or []) + list(incoming or []):
        d = _as_progress_dict(row)
        if d is None:
            continue
        head = _progress_head(d)
        rest = _progress_rest(d)
        result = [
            r for r in result
            if _progress_head(r) != head or not _progress_rest(r) <= rest
        ]
        result.append(d)
    result.sort(key=_progress_sort_key)
    return result

def build_doc(
    doc: dict, doc_data: dict, unit: dict, version: int, version_ts: str
) -> dict:
    """Assemble the flat document dict to be indexed."""
    manifest_path = unit.get("manifest_path", "")
    html_files_key = unit.get("html_files")

    result: dict = {}
    result.update(doc.get("meta") or {})
    result.update(doc_data or {})

    result.setdefault("id_final", doc.get("id_final"))
    result.setdefault("id_uid", doc.get("id_uid"))
    result.setdefault("uuid", doc.get("uuid"))
    result["version"] = version
    result["version_ts"] = version_ts
    result["html_files"] = _to_array(doc.get("html_files") or html_files_key)
    result["path_manifest"] = doc.get("path_manifest") or norm_path(manifest_path)
    for field in ARRAY_MERGE_FIELDS:
        if result.get(field) is None:
            result[field] = []
        else:
            result[field] = _to_array(result[field])

    return result


def _build_documents_array(
    doc_texts, filename: str | None, date: str
) -> list[dict]:
    """Shape the prepare_doc `_document_text` payload into the nested documents array."""
    if not doc_texts:
        return []
    if isinstance(doc_texts, list) and doc_texts and isinstance(doc_texts[0], dict):
        return [
            {
                "filename": filename or "",
                "date": date,
                "name": dt["name"],
                "text": dt["text"],
            }
            for dt in doc_texts
        ]
    # Legacy fallback: flat list of strings
    text = " ".join(t for t in doc_texts if isinstance(t, str) and t)
    if text and filename:
        return [{"filename": filename, "date": date, "text": text}]
    return []


def build_update_item(
    doc: dict,
    doc_data: dict,
    unit: dict,
    filename: str | None,
    source_path: str | None = None,
) -> dict | None:
    """Build a single indexer item: {_id, doc, version_ts, metadata?}.

    Returns None when id_final is missing; the caller is responsible for
    recording the unit path so it can be reprocessed.
    """
    rel_from_root = unit.get("rel_from_root", "")
    doc_id = resolve_doc_id(doc)
    if not doc_id:
        logger.error(
            "Missing id_final (no manifest ID and no JSONL id_final); skipping unit: %s/%s",
            rel_from_root,
            filename or "",
        )
        return None

    from_name = html_name_to_iso(filename) if filename else None
    version_ts = from_name or doc.get("version_ts") or ""

    # Each call represents one processing event (one HTML file or one JSONL record).
    # merge_docs recomputes the authoritative version / version_ts from the merged
    # documents array, so the per-item version only needs to be non-zero here.
    assembled = build_doc(doc, doc_data, unit, 1, version_ts)

    doc_texts = assembled.pop("_document_text", None)
    assembled.pop("full_document_texts", None)
    assembled["documents"] = _build_documents_array(
        doc_texts, filename, from_name or version_ts
    )

    item: dict = {
        "_id": doc_id,
        "doc": assembled,
        "version_ts": version_ts,
    }
    if source_path:
        item["metadata"] = {"sourcePath": source_path}
    return item


def mget(es_client, index: str, ids: list[str]) -> dict[str, dict]:
    """Fetch existing documents by ID. Returns {id: _source} for found docs."""
    try:
        resp = es_client.mget(index=index, ids=ids)
        return {
            doc["_id"]: doc["_source"]
            for doc in resp.get("docs", [])
            if doc.get("found")
        }
    except Exception as exc:
        logger.warning("mget failed, treating all as new: %s", exc)
        return {}


def apply_merge(batch: list[dict], existing_by_id: dict[str, dict]) -> list[dict]:
    """Merge batch items against each other and existing ES docs.

    Items sharing the same _id are folded together in order (batch is pre-sorted
    oldest-first) so that all HTML files for one case accumulate into a single doc.
    """
    # Accumulate within-batch merges per id, preserving insertion order
    acc: dict[str, dict] = {}
    for item in batch:
        doc_id = item["_id"]
        base = acc.get(doc_id) or existing_by_id.get(doc_id)
        acc[doc_id] = merge_docs(base, item["doc"])

    result = []
    for doc_id, doc in acc.items():
        doc["_id"] = doc_id
        result.append(doc)
    return result


def build_bulk_index_ops(items: list[dict], index: str) -> list:
    """Convert merged items to the flat list ES bulk API expects (index, not update)."""
    ops = []
    for item in items:
        doc_id = item.pop("_id", None)
        if not doc_id:
            continue
        ops.append({"index": {"_index": index, "_id": str(doc_id)}})
        ops.append(item)
    return ops


# ---------------------------------------------------------------------------
# BulkIndexer
# ---------------------------------------------------------------------------


class BulkIndexer:
    """Bucket-based bulk indexer with time/size flushing and Python-side merging."""

    def __init__(
        self,
        es_client,
        index: str,
        metrics=None,
        bulk_docs: int = 2000,
        flush_interval: float = 15.0,
        failed_log: str = "failed_paths.log",
    ):
        self._es = es_client
        self._index = index
        self._metrics = metrics
        self._bulk_docs = bulk_docs
        self._flush_interval = flush_interval
        self._failed_log = failed_log

        self._buffers: dict[str, list] = {}
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._flush_threads: list[threading.Thread] = []

    def add(self, bucket_key: str, item: dict) -> None:
        with self._lock:
            buf = self._buffers.setdefault(bucket_key, [])
            buf.append(item)
            should_flush = len(buf) >= self._bulk_docs

        if should_flush:
            # Hand off to a daemon thread so the caller is never stalled
            # waiting for mget + es.bulk network I/O.
            t = threading.Thread(target=self._flush_bucket, args=(bucket_key,), daemon=True)
            with self._lock:
                self._flush_threads.append(t)
            t.start()
        else:
            self._schedule_flush()

    def drain(self) -> None:
        """Wait for in-flight background flushes, then flush all remaining buffers."""
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
            threads = list(self._flush_threads)

        for t in threads:
            t.join()

        with self._lock:
            self._flush_threads.clear()
            keys = list(self._buffers.keys())

        for key in keys:
            self._flush_bucket(key)

    def _schedule_flush(self) -> None:
        with self._lock:
            if self._timer is None:
                self._timer = threading.Timer(self._flush_interval, self._timer_fired)
                self._timer.daemon = True
                self._timer.start()

    def _timer_fired(self) -> None:
        with self._lock:
            self._timer = None
            keys = list(self._buffers.keys())
        for key in keys:
            self._flush_bucket(key)

    def _flush_bucket(self, bucket_key: str) -> None:
        with self._lock:
            batch = self._buffers.pop(bucket_key, [])
        if batch:
            self._do_bulk(batch, bucket_key)
        # Prune finished threads from the tracking list
        with self._lock:
            self._flush_threads = [t for t in self._flush_threads if t.is_alive()]

    def _do_bulk(self, batch: list, bucket_key: str, retries: int = 3) -> None:
        # Sort oldest-first so latest values win for non-array fields
        batch.sort(key=lambda x: x.get("version_ts") or "")

        existing_by_id = mget(self._es, self._index, [item["_id"] for item in batch])
        merged_items = apply_merge(batch, existing_by_id)

        ops = build_bulk_index_ops(merged_items, self._index)
        if not ops:
            return

        for attempt in range(retries + 1):
            try:
                t0 = time.monotonic()
                res = self._es.bulk(operations=ops, refresh=False)
                if self._metrics:
                    self._metrics.add_time("bulkMs", (time.monotonic() - t0) * 1000)
                items = res.get("items", [])
                failed = 0
                for action in items:
                    status = action.get("index") or {}
                    if status.get("error"):
                        failed += 1
                        self._log_failure(
                            status.get("_id", ""), json.dumps(status["error"])
                        )
                if self._metrics:
                    self._metrics.inc("docsIndexed", len(batch))
                    self._metrics.inc("bulkDocsTotal", len(merged_items))
                    self._metrics.inc("bulkCalls", 1)
                    if failed:
                        self._metrics.inc("bulkFailedDocs", failed)
                if failed:
                    logger.warning(
                        "Bucket %s: %d/%d docs failed", bucket_key, failed, len(batch)
                    )
                return

            except Exception as exc:
                status_code = getattr(getattr(exc, "meta", None), "status", None) or 0
                if status_code == 413 and len(batch) > 1:
                    half = len(batch) // 2
                    self._do_bulk(batch[:half], bucket_key, retries)
                    self._do_bulk(batch[half:], bucket_key, retries)
                    return
                if status_code in (429, 503) and attempt < retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
                logger.error(
                    "Bulk error bucket=%s attempt=%d: %s", bucket_key, attempt, exc
                )
                if attempt == retries:
                    raise

    def _log_failure(self, doc_id: str, reason: str) -> None:
        try:
            with open(self._failed_log, "a", encoding="utf-8") as fh:
                fh.write(f"{doc_id}\t{reason}\n")
        except OSError:
            pass
