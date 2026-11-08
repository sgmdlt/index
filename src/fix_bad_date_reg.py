"""One-shot fix: heal date_reg values outside [2000-01-01, now], or missing, in place.

Docs indexed before fix_date_reg landed in the ingest path carry bad
registration dates or none at all (the old rule nulled unrecoverable ones).
Applies the current rule retroactively: earliest sane date from progress
rows; docs without one keep their original value untouched.

Usage:
    uv run python -m src.fix_bad_date_reg --index sud [--es URL] [--apply]

Default is dry-run: prints affected doc ids with old -> new value. Pass --apply to write.
"""

import argparse
import os

from elasticsearch import Elasticsearch

from .text_filters import fix_date_reg, recover_malformed_date


def id_date_segment(id_final: str) -> str | None:
    """Middle segment of `<court>_<date>_<case>`, or None if not that shape."""
    parts = (id_final or "").split("_")
    return parts[1] if len(parts) == 3 else None


def recover_from_id(id_final: str, today: str | None = None) -> str | None:
    """Last-resort date recovery from the date baked into id_final, for docs
    whose date_reg field is null but whose id still carries a mangled date."""
    return recover_malformed_date(id_date_segment(id_final) or "", today)


def build_query() -> dict:
    return {
        "bool": {
            "should": [
                {"range": {"date_reg": {"lt": "1990-01-01"}}},
                {"range": {"date_reg": {"gt": "now"}}},
                {"bool": {"must_not": {"exists": {"field": "date_reg"}}}},
            ],
            "minimum_should_match": 1,
        }
    }


def run(es: Elasticsearch, index: str, apply: bool) -> None:
    total = recovered = 0
    resp = es.search(
        index=index,
        query=build_query(),
        size=500,
        scroll="5m",
        _source=["date_reg", "progress", "date_result", "case_number", "id_uid"],
    )
    scroll_id = resp["_scroll_id"]
    while hits := resp["hits"]["hits"]:
        ops = []
        for hit in hits:
            total += 1
            src = hit["_source"]
            old = src.get("date_reg")
            new = fix_date_reg(
                old,
                src.get("progress") or [],
                src.get("date_result"),
                src.get("case_number"),
            )
            if new == old:
                # date_reg null/unfixable but the id may still carry the date
                new = recover_from_id(hit["_id"]) or old
            id_uid = src.get("id_uid") or "-"
            if new == old:
                print(f"{hit['_id']} id_uid={id_uid}: {old} UNRECOVERABLE, kept")
                continue
            recovered += 1
            print(f"{hit['_id']} id_uid={id_uid}: {old} -> {new}")
            if apply:
                ops.append({"update": {"_index": index, "_id": hit["_id"]}})
                ops.append({"doc": {"date_reg": new}})
        if ops:
            res = es.bulk(operations=ops, refresh=False)
            if res.get("errors"):
                for item in res["items"]:
                    err = item.get("update", {}).get("error")
                    if err:
                        print(f"ERROR {item['update'].get('_id')}: {err}")
        resp = es.scroll(scroll_id=scroll_id, scroll="5m")
    es.clear_scroll(scroll_id=scroll_id)
    print(f"bad={total} recovered={recovered} kept={total - recovered} mode={'APPLY' if apply else 'dry-run'}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--index", required=True)
    p.add_argument("--es", default="http://localhost:9222")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--api-key", dest="api_key", default=None)
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--apply", action="store_true", help="write fixes (default: dry-run)")
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
    run(es, args.index, args.apply)


if __name__ == "__main__":
    main()
