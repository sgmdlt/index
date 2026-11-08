"""One-shot migration: re-key legacy `_NONE_` docs under healed ids, in place.

These docs came from JSONL exports — no FS/WARC unit to reindex from, so the
doc is copied under the healed id (year from case_number) and the old doc is
deleted. If a doc already exists under the healed id (case re-downloaded later
with a proper date), the NONE doc is left alone and logged as a conflict for
manual review.

Usage:
    uv run python -m src.migrate_none_ids --index sudata --es URL [--api-key KEY] [--apply]

Default is dry-run. Conflicts go to <out>_conflicts.txt.
"""

import argparse
import os

from elasticsearch import Elasticsearch

from .to_document import fix_none_id


def heal_source(src: dict) -> tuple[str, dict] | None:
    """Return (new_id, new_source) for a NONE doc, or None if not healable."""
    old_id = src.get("id_final")
    new_id = fix_none_id(old_id, src.get("case_number"))
    if not new_id or new_id == old_id:
        return None
    return new_id, {**src, "id_final": new_id}


def run(es: Elasticsearch, index: str, apply: bool, out_prefix: str) -> None:
    total = moved = stuck = conflicts = 0
    resp = es.search(
        index=index,
        query={"wildcard": {"id_final": {"value": "*_NONE_*"}}},
        size=200,
        scroll="10m",
    )
    scroll_id = resp["_scroll_id"]
    with open(f"{out_prefix}_conflicts.txt", "w", encoding="utf-8") as cf:
        while hits := resp["hits"]["hits"]:
            healed = []
            for hit in hits:
                total += 1
                res = heal_source(hit["_source"])
                if res is None:
                    stuck += 1
                    continue
                healed.append((hit["_id"], *res))

            existing = set()
            if healed:
                mres = es.mget(index=index, ids=[h[1] for h in healed])
                existing = {d["_id"] for d in mres["docs"] if d.get("found")}

            ops = []
            for old_id, new_id, new_src in healed:
                if new_id in existing:
                    conflicts += 1
                    cf.write(f"{old_id} -> {new_id}\n")
                    continue
                moved += 1
                print(f"{old_id} -> {new_id}")
                if apply:
                    ops.append({"index": {"_index": index, "_id": new_id}})
                    ops.append(new_src)
                    ops.append({"delete": {"_index": index, "_id": old_id}})
            if ops:
                res = es.bulk(operations=ops, refresh=False)
                if res.get("errors"):
                    for item in res["items"]:
                        for action, body in item.items():
                            if body.get("error"):
                                print(f"ERROR {action} {body.get('_id')}: {body['error']}")
            resp = es.scroll(scroll_id=scroll_id, scroll="10m")
    es.clear_scroll(scroll_id=scroll_id)
    print(
        f"total={total} moved={moved} conflicts={conflicts} stuck_none={stuck} "
        f"mode={'APPLY' if apply else 'dry-run'}"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--index", required=True)
    p.add_argument("--es", default="http://localhost:9222")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--api-key", dest="api_key", default=None)
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    p.add_argument("--out", default="none_migration", help="output file prefix")
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
    run(es, args.index, args.apply, args.out)


if __name__ == "__main__":
    main()
