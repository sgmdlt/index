"""Elasticsearch bulk indexer with bucket batching, retry, and Python-side merging."""

import json
import logging
import re
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from .text_filters import broken_text_message, fix_date_reg
from .utils.case_id import case_id_from_fields
from .utils.dates import html_name_to_iso
from .utils.paths import norm_path
from .uuid_group import in_group_scope, pick_group_id

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent.parent  # project root (index/)

_rules: dict = json.loads(
    (Path(__file__).parent / "elastic_index_dict.json").read_text(encoding="utf-8")
)
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
    # без единого пробела: экстракторы расходятся и внутри слов («2- 01/11»,
    # «П Р И Г О В О Р»), а пробел — это вёрстка, не текст акта
    return "".join(text.split()) if text else ""


# Names that carry no information about the document type: "" and the JSONPath
# container key "raw_json" that the old mgs court_decisions format surfaced as.
_PLACEHOLDER_NAMES = frozenset({"", "raw_json"})

_INDENTED_LINE = re.compile(r"^[ \t]+\S", re.M)


def _layout_score(text: str) -> tuple[int, int]:
    """How much of an act's layout the text still carries.

    Line breaks first, then indented lines. Length would be the wrong measure:
    a stray double space makes a text longer without making it laid out.
    """
    return text.count("\n"), len(_INDENTED_LINE.findall(text))


def _merge_documents(existing_arr: list, incoming_arr: list) -> list:
    result = list(existing_arr)
    canon_idx: dict[str, int] = {}
    for i, entry in enumerate(result):
        if isinstance(entry, dict) and entry.get("text"):
            canon_idx.setdefault(_doc_canon(entry["text"]), i)
    for entry in incoming_arr:
        if not isinstance(entry, dict):
            continue
        canon = _doc_canon(entry.get("text", ""))
        if not canon:
            continue
        i = canon_idx.get(canon)
        if i is None:
            canon_idx[canon] = len(result)
            result.append(entry)
            continue
        kept = result[i]
        if (kept.get("name") or "") in _PLACEHOLDER_NAMES and (
            entry.get("name") or ""
        ) not in _PLACEHOLDER_NAMES:
            kept = entry  # same text, properly named copy wins
        if _layout_score(entry.get("text", "")) > _layout_score(kept.get("text", "")):
            # Same text up to whitespace, so the better-laid-out one wins —
            # layout is the formatting of an act. This is what lets a
            # re-extraction heal a text flattened by the old extractor, which
            # collapsed every `\s+` and so left no line breaks at all.
            kept = {**kept, "text": entry["text"]}
        result[i] = kept
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
                result[key] = _merge_array(
                    _to_array(existing.get(key)), _to_array(value)
                )
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
        # Drop degenerate rows with no date and no event (e.g. the stale
        # «История местонахождения»-only row left by the old parser). Purges
        # them on re-index instead of preserving them forever.
        if head == ("", ""):
            continue
        rest = _progress_rest(d)
        result = [
            r
            for r in result
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


def _build_documents_array(doc_texts, filename: str | None, date: str) -> list[dict]:
    """Shape the prepare_doc `_document_text` payload into the nested documents array."""
    if not doc_texts:
        return []
    if isinstance(doc_texts, list) and doc_texts and isinstance(doc_texts[0], dict):
        entries = []
        for dt in doc_texts:
            entry = {
                "filename": filename or "",
                "date": date,
                "name": dt["name"],
                "text": dt["text"],
            }
            broken_msg = broken_text_message(dt["text"])
            if broken_msg:
                entry["text"] = broken_msg
                entry["is_broken"] = True
            entries.append(entry)
        return entries
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
    assembled["date_reg"] = fix_date_reg(
        assembled.get("date_reg"),
        assembled.get("progress") or [],
        assembled.get("date_result"),
        assembled.get("case_number"),
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
    resp = es_client.mget(index=index, ids=ids)
    return {
        doc["_id"]: doc["_source"] for doc in resp.get("docs", []) if doc.get("found")
    }


UUID_CHUNK = 1000  # terms по 1000 uuid и до 10000 хитов — в пределах max_result_window


def uuid_twins(es_client, index: str, uuids: list[str]) -> dict[str, list[dict]]:
    """Документы индекса с этими uuid: {uuid: [mget-документ с _seq_no, …]}.

    search нужен только ради id: он near-real-time, а при refresh_interval=-1
    его _source — копия до прогона, и слияние по ней стёрло бы записанное в
    этом же прогоне. Сами документы и их версия — из mget (real-time).
    """
    ids: list[str] = []
    for i in range(0, len(uuids), UUID_CHUNK):
        resp = es_client.search(
            index=index,
            query={"terms": {"uuid": uuids[i : i + UUID_CHUNK]}},
            size=10000,
            source=False,
        )
        ids += [hit["_id"] for hit in resp["hits"]["hits"]]
    out: dict[str, list[dict]] = {}
    for doc in es_client.mget(index=index, ids=ids).get("docs", []) if ids else []:
        if doc.get("found"):
            out.setdefault(doc["_source"].get("uuid"), []).append(doc)
    return out


def _group_key(doc: dict) -> str:
    # кандидат группы — формульный id документа: старый id из прошлой
    # формулы не должен стать целевым только потому, что он там уже лежит
    src = doc["_source"]
    return (
        case_id_from_fields(
            src.get("case_number"),
            src.get("date_reg"),
            src.get("court_id") or src.get("court_code"),
            src.get("link"),
        )
        or doc["_id"]
    )


def regroup(
    batch: list[dict], twins: dict[str, list[dict]]
) -> tuple[list[dict], dict[str, list[dict]], dict]:
    """Сводит документы одного uuid на id, выбранный правилом группы.

    Возвращает пачку с переписанными _id (исходный — в _orig_id: вызывающий
    ждёт вердикт по нему), {целевой id: _source близнецов} для слияния и
    {id близнеца: (целевой id, _seq_no, _primary_term)} — кого удалить, когда
    целевой документ запишется.
    """
    by_uuid: dict[str, list[dict]] = {}
    for item in batch:
        if in_group_scope(item["doc"]):
            by_uuid.setdefault(item["doc"]["uuid"], []).append(item)

    target_of = {
        u: pick_group_id(
            [
                (_group_key(t), t["_source"].get("version_ts") or "")
                for t in twins.get(u, [])
            ]
            + [(it["_id"], it.get("version_ts") or "") for it in items]
        )
        for u, items in by_uuid.items()
    }

    regrouped = []
    for item in batch:
        item = {**item, "_orig_id": item.get("_orig_id", item["_id"])}
        target = (
            target_of.get(item["doc"]["uuid"]) if in_group_scope(item["doc"]) else None
        )
        if target:
            item = {**item, "_id": target, "doc": {**item["doc"], "id_final": target}}
        regrouped.append(item)

    twins_for: dict[str, list[dict]] = {}
    delete_for: dict = {}
    for u, target in target_of.items():
        others = [t for t in twins.get(u, []) if t["_id"] != target]
        if others:
            twins_for[target] = [t["_source"] for t in others]
        for t in others:
            delete_for[t["_id"]] = (target, t["_seq_no"], t["_primary_term"])
    return regrouped, twins_for, delete_for


def with_group_created_at(merged: dict, docs: list[dict]) -> dict:
    """created_at слитого дела — самый ранний из слитых документов; если хоть
    один записан до created_at (легаси), его нет и у слитого: дело старое."""
    stamps = [d.get("created_at") for d in docs]
    merged = {k: v for k, v in merged.items() if k != "created_at"}
    return merged if None in stamps else {**merged, "created_at": min(stamps)}


def fold_group(docs: list[dict | None], target: str) -> dict:
    """Скачивания одной карточки в один документ: по version_ts, свежее —
    входящим, чтобы старый близнец не перетёр поля свежего."""
    docs = [d for d in docs if d]
    acc = None
    for doc in sorted(docs, key=lambda d: d.get("version_ts") or ""):
        acc = merge_docs(acc, doc)
    return with_group_created_at({**acc, "id_final": target}, docs)


def apply_merge(
    batch: list[dict],
    existing_by_id: dict[str, dict],
    now: str,
    stamp_created_at: bool = True,
) -> list[dict]:
    """Merge batch items against each other and existing ES docs.

    Items sharing the same _id are folded together in order (batch is pre-sorted
    oldest-first) so that all HTML files for one case accumulate into a single doc.
    Каждому результату ставится indexed_at = now; created_at = now только у
    документов, которых ещё нет в ES (у существующих остаётся как есть; слитые
    близнецы лежат в existing_by_id под целевым id — см. fold_group).
    stamp_created_at=False — полная переиндексация: новых дел в ней нет.
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
        doc["indexed_at"] = now
        if stamp_created_at and doc_id not in existing_by_id:
            doc["created_at"] = now
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
        on_result=None,
        stamp_created_at: bool = True,
    ):
        self._es = es_client
        self._index = index
        self._metrics = metrics
        self._bulk_docs = bulk_docs
        self._flush_interval = flush_interval
        self._failed_log = failed_log
        # on_result(indexed_ids, failed_ids, reason): единственный способ для
        # вызывающего узнать вердикт ES. Без него отказ виден только в файле, и
        # строка outbox остаётся в DONE на потерянном деле.
        self._on_result = on_result
        # полная переиндексация (--warc) пишет в пустой индекс: всё — «легаси»,
        # иначе любое старое дело ушло бы подписчикам как новое
        self._stamp_created_at = stamp_created_at

        self._buffers: dict[str, list] = {}
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._flush_threads: list[threading.Thread] = []

    def set_on_result(self, fn) -> None:
        """DB-режим ставит колбэк уже после конструктора: он замыкает соединение
        с Postgres, которое открывается позже самого индексатора."""
        self._on_result = fn

    def add(self, bucket_key: str, item: dict) -> None:
        with self._lock:
            buf = self._buffers.setdefault(bucket_key, [])
            buf.append(item)
            should_flush = len(buf) >= self._bulk_docs

        if should_flush:
            # Hand off to a daemon thread so the caller is never stalled
            # waiting for mget + es.bulk network I/O.
            t = threading.Thread(
                target=self._flush_bucket, args=(bucket_key,), daemon=True
            )
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

        # Документы одного uuid — одно дело: без этого правка даты судом
        # кладёт карточку под второй _id. Сбой поиска валит батч, как и mget.
        uuids = sorted(
            {item["doc"]["uuid"] for item in batch if in_group_scope(item["doc"])}
        )
        try:
            twins = uuid_twins(self._es, self._index, uuids) if uuids else {}
        except Exception as exc:
            self._fail_batch(batch, bucket_key, f"uuid search failed: {exc}")
            return
        batch, twins_for, delete_for = regroup(batch, twins)
        origin: dict[str, list[str]] = {}
        for item in batch:
            origin.setdefault(item["_id"], [])
            if item["_orig_id"] not in origin[item["_id"]]:
                origin[item["_id"]].append(item["_orig_id"])

        def callers(ids: list) -> list:
            # вызывающий ждёт вердикт по своим id, а не по переписанным
            return [o for i in ids for o in origin.get(i, [i])]

        try:
            existing_by_id = mget(
                self._es, self._index, [item["_id"] for item in batch]
            )
        except Exception as exc:
            # Раньше это считалось «все документы новые», и слияние становилось
            # перезаписью: старые документы дела и история терялись, а частичная
            # пометка (id_final + case_hidden) затирала дело целиком. Отказ по
            # батчу — строки перезаявятся, jsonl останется на диске.
            self._fail_batch(batch, bucket_key, f"mget failed: {exc}")
            return
        for target, sources in twins_for.items():
            existing_by_id[target] = fold_group(
                [existing_by_id.get(target), *sources], target
            )
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        merged_items = apply_merge(batch, existing_by_id, now, self._stamp_created_at)

        # снимаем до build_bulk_index_ops: он вынимает _id из элементов
        doc_ids = [item["_id"] for item in merged_items if item.get("_id")]

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
                indexed_ids, failed_ids = [], []
                for action in items:
                    status = action.get("index") or {}
                    doc_id = status.get("_id", "")
                    if status.get("error"):
                        failed_ids.append(doc_id)
                        self._log_failure(doc_id, json.dumps(status["error"]))
                    else:
                        indexed_ids.append(doc_id)
                if self._metrics:
                    # только принятые: считать весь батч значило отчитываться
                    # об успехе там, где ES отверг документ
                    self._metrics.inc("docsIndexed", len(indexed_ids))
                    self._metrics.inc("bulkDocsTotal", len(merged_items))
                    self._metrics.inc("bulkCalls", 1)
                    if failed_ids:
                        self._metrics.inc("bulkFailedDocs", len(failed_ids))
                if failed_ids:
                    logger.warning(
                        "Bucket %s: %d/%d docs failed",
                        bucket_key,
                        len(failed_ids),
                        len(batch),
                    )
                self._report(
                    callers(indexed_ids), callers(failed_ids), "bulk item rejected"
                )
                self._delete_twins(delete_for, set(indexed_ids))
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
                    # Раньше здесь был raise. Он ничего не защищал: _do_bulk
                    # почти всегда крутится в демон-потоке, где исключение
                    # пропадает вместе с потоком, а вызывающий считает батч
                    # записанным. Сообщаем отказ по всему батчу — строки станут
                    # перезаявляемыми, — и даём прогону дойти до конца.
                    failed_ids = doc_ids
                    for doc_id in failed_ids:
                        self._log_failure(doc_id, f"bulk call failed: {exc}")
                    if self._metrics:
                        self._metrics.inc("bulkFailedDocs", len(failed_ids))
                        self._metrics.inc("bulkCalls", 1)
                    self._report([], callers(failed_ids), f"bulk call failed: {exc}")
                    return

    def _fail_batch(self, batch: list, bucket_key: str, reason: str) -> None:
        failed_ids = [
            item.get("_orig_id") or item["_id"] for item in batch if item.get("_id")
        ]
        logger.error("%s bucket=%s, batch not written", reason, bucket_key)
        for doc_id in failed_ids:
            self._log_failure(doc_id, reason)
        if self._metrics:
            self._metrics.inc("bulkFailedDocs", len(failed_ids))
        self._report([], failed_ids, reason)

    def _delete_twins(self, delete_for: dict, written: set) -> None:
        """Удаляет близнецов, чей целевой документ ES уже принял, — и только в
        прочитанной версии: обновлённый после чтения близнец получит 409 и
        останется. Оставшийся близнец — дубль, а не потеря: следующая запись
        этого uuid сольёт его снова."""
        ops = [
            {
                "delete": {
                    "_index": self._index,
                    "_id": twin,
                    "if_seq_no": seq,
                    "if_primary_term": term,
                }
            }
            for twin, (target, seq, term) in delete_for.items()
            if target in written
        ]
        ids = [op["delete"]["_id"] for op in ops]
        if not ops:
            return
        try:
            res = self._es.bulk(operations=ops, refresh=False)
        except Exception as exc:
            for i in ids:
                self._log_failure(i, f"twin delete failed: {exc}")
            return
        for action in res.get("items", []):
            status = action.get("delete") or {}
            if status.get("error"):
                self._log_failure(status.get("_id", ""), json.dumps(status["error"]))

    def _report(self, indexed_ids: list, failed_ids: list, reason: str) -> None:
        if self._on_result is None:
            return
        try:
            self._on_result(indexed_ids, failed_ids, reason)
        except Exception:
            logger.exception("on_result callback raised; continuing")

    def _log_failure(self, doc_id: str, reason: str) -> None:
        try:
            with open(self._failed_log, "a", encoding="utf-8") as fh:
                fh.write(f"{doc_id}\t{reason}\n")
        except OSError:
            pass
