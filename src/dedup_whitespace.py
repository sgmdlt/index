"""One-shot fix: collapse whitespace-duplicate entries in `documents` arrays.

Earlier parser versions produced the same act text with and without extra
spaces, so docs indexed before the fix hold both variants. `_merge_documents`
only dedups incoming against existing, so re-runs never heal them — this
script does, in place.

Usage:
    uv run python -m src.dedup_whitespace --index sud [--es URL] [--apply]

Default is dry-run: prints affected doc ids and counts. Pass --apply to write.
"""

import argparse
import os

from elasticsearch import Elasticsearch

from .bulk_indexer import _doc_canon
from .text_filters import broken_text_message


def dedup_documents(documents: list) -> list:
    """Collapse entries whose text differs only by whitespace.

    Keeps the longest variant (the trimmed one is unreadable — whitespace is
    the formatting), preserves order of first occurrence.
    """
    best: dict[str, dict] = {}
    order: list[str] = []
    for entry in documents:
        canon = _doc_canon(entry.get("text", ""))
        if canon not in best:
            best[canon] = entry
            order.append(canon)
        elif len(entry.get("text", "")) > len(best[canon].get("text", "")):
            best[canon] = entry
    return [best[c] for c in order]


def fix_documents(documents: list) -> list:
    """Replace broken texts (docx binary / server / SQL errors), then dedup."""
    fixed = []
    for entry in documents:
        msg = broken_text_message(entry.get("text", ""))
        if msg:
            entry = {**entry, "text": msg, "is_broken": True}
        fixed.append(entry)
    return dedup_documents(fixed)


_BROKEN_MARKERS = ["docProps", "QUERY", "judicial_web", "server closed the connection"]


def build_broken_query() -> dict:
    should = [
        {"match_phrase": {"documents.text": marker}} for marker in _BROKEN_MARKERS
    ]
    return {
        "bool": {
            "filter": [
                {
                    "nested": {
                        "path": "documents",
                        "query": {"bool": {"should": should, "minimum_should_match": 1}},
                    }
                }
            ]
        }
    }


def build_query(
    ids: list[str] | None, date_from: str | None, date_to: str | None
) -> dict:
    if ids:
        return {"ids": {"values": ids}}
    filters: list[dict] = [{"range": {"doc_count": {"gte": 2}}}]
    if date_from or date_to:
        rng: dict = {}
        if date_from:
            rng["gte"] = date_from
        if date_to:
            rng["lte"] = date_to
        filters.append(
            {"nested": {"path": "documents", "query": {"range": {"documents.date": rng}}}}
        )
    return {"bool": {"filter": filters}}


def run(es: Elasticsearch, index: str, apply: bool, query: dict) -> None:
    fixed = scanned = 0
    resp = es.search(
        index=index, query=query, size=500, scroll="5m", _source=["documents", "id_uid"]
    )
    scroll_id = resp["_scroll_id"]
    while hits := resp["hits"]["hits"]:
        ops = []
        for hit in hits:
            scanned += 1
            docs = hit["_source"].get("documents") or []
            new_docs = fix_documents(docs)
            if new_docs != docs:
                fixed += 1
                n_broken = sum(1 for d in new_docs if d.get("is_broken"))
                id_uid = hit["_source"].get("id_uid") or "-"
                print(f"{hit['_id']} id_uid={id_uid}: {len(docs)} -> {len(new_docs)} broken={n_broken}")
                if apply:
                    ops.append({"update": {"_index": index, "_id": hit["_id"]}})
                    ops.append({"doc": {"documents": new_docs, "doc_count": len(new_docs)}})
        if ops:
            res = es.bulk(operations=ops, refresh=False)
            if res.get("errors"):
                for item in res["items"]:
                    err = item.get("update", {}).get("error")
                    if err:
                        print(f"ERROR {item['update'].get('_id')}: {err}")
        resp = es.scroll(scroll_id=scroll_id, scroll="5m")
    es.clear_scroll(scroll_id=scroll_id)
    print(f"scanned={scanned} with_dupes={fixed} mode={'APPLY' if apply else 'dry-run'}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--index", required=True)
    p.add_argument("--es", default="http://localhost:9222")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--api-key", dest="api_key", default=None)
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--apply", action="store_true", help="write fixes (default: dry-run)")
    p.add_argument("--id", action="append", dest="ids", help="limit to specific _id (repeatable)")
    p.add_argument("--date-from", help="documents.date lower bound, e.g. 2026-05-16")
    p.add_argument("--date-to", help="documents.date upper bound, e.g. 2026-05-20")
    p.add_argument("--broken", action="store_true", help="target docs with broken-text markers")
    args = p.parse_args()

    es_opts: dict = {"request_timeout": 120}
    if args.api_key:
        es_opts["api_key"] = args.api_key
    elif args.user and args.password:
        es_opts["basic_auth"] = (args.user, args.password)
    elif os.environ.get("ES_API_KEY"):
        es_opts["api_key"] = os.environ["ES_API_KEY"]
    elif os.environ.get("ES_USER"):
        es_opts["basic_auth"] = (os.environ["ES_USER"], os.environ.get("SGMDLT_PASSWORD", ""))
    if args.insecure and args.es.startswith("https://"):
        es_opts["verify_certs"] = False
    es = Elasticsearch(args.es, **es_opts)
    query = (
        build_broken_query()
        if args.broken
        else build_query(args.ids, args.date_from, args.date_to)
    )
    run(es, args.index, args.apply, query)


if __name__ == "__main__":
    main()
