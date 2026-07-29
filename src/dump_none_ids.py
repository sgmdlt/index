"""Dump docs with legacy `_NONE_` ids and where to reindex them from.

Writes three files:
    none_ids.jsonl  - full picture per doc: _id, healed new_id, case_number,
                      court_code, path_manifest, html_files
    none_fs.txt     - manifest paths of FS-sourced docs (for --folders mode)
    none_warc.txt   - WARC locators (path:offset:length) of WARC-sourced docs

Usage:
    uv run python -m src.dump_none_ids --index sudata --es URL [--api-key KEY]
"""

import argparse
import json
import os
import re

from elasticsearch import Elasticsearch

from .to_document import fix_none_id

_LOCATOR_RE = re.compile(r":\d+:\d+$")


def is_warc_locator(path: str) -> bool:
    return bool(_LOCATOR_RE.search(path or ""))


def run(es: Elasticsearch, index: str, out_prefix: str) -> None:
    total = healed = 0
    resp = es.search(
        index=index,
        query={"wildcard": {"id_final": {"value": "*_NONE_*"}}},
        size=500,
        scroll="5m",
        _source=["id_final", "id_uid", "case_number", "court_code", "path_manifest", "html_files"],
    )
    scroll_id = resp["_scroll_id"]
    with (
        open(f"{out_prefix}.jsonl", "w", encoding="utf-8") as jl,
        open(f"{out_prefix}_fs.txt", "w", encoding="utf-8") as fs,
        open(f"{out_prefix}_warc.txt", "w", encoding="utf-8") as warc,
    ):
        while hits := resp["hits"]["hits"]:
            for hit in hits:
                total += 1
                src = hit["_source"]
                new_id = fix_none_id(src.get("id_final"), src.get("case_number"))
                if new_id != src.get("id_final"):
                    healed += 1
                path = src.get("path_manifest") or ""
                jl.write(
                    json.dumps(
                        {"_id": hit["_id"], "new_id": new_id, **src}, ensure_ascii=False
                    )
                    + "\n"
                )
                (warc if is_warc_locator(path) else fs).write(path + "\n")
            resp = es.scroll(scroll_id=scroll_id, scroll="5m")
    es.clear_scroll(scroll_id=scroll_id)
    print(f"total={total} healable={healed} stuck_none={total - healed}")
    print(f"wrote {out_prefix}.jsonl, {out_prefix}_fs.txt, {out_prefix}_warc.txt")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--index", required=True)
    p.add_argument("--es", default="http://localhost:9222")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--api-key", dest="api_key", default=None)
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--out", default="none_ids", help="output file prefix")
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
    run(es, args.index, args.out)


if __name__ == "__main__":
    main()
