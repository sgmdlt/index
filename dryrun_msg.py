"""Dry-run: push first N JSONL records through the real pipeline, no ES.

Prints each assembled doc and a field-coverage summary so we can see what the
indexer captures vs. drops for the MSUDRF metadata-only dataset.
"""

import json
import sys
from collections import Counter

from src.main import _derive_id_final
from src.prepare_doc import prepare_doc
from src.to_document import to_document
from src.bulk_indexer import build_update_item

PATH = sys.argv[1] if len(sys.argv) > 1 else "/home/ltdmgs/MSUDRF-All-02.jsonl"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 10
SUMMARY_N = int(sys.argv[3]) if len(sys.argv) > 3 else 5000

src_keys = Counter()
out_keys = Counter()    # non-empty fields in assembled doc
empty_keys = Counter()  # present but empty
dropped_doc_link = 0
none_id = 0
n = 0

shown = 0
with open(PATH, encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        if n >= SUMMARY_N:
            break
        data = json.loads(line)
        n += 1
        for k in data:
            src_keys[k] += 1
        if data.get("doc_link"):
            dropped_doc_link += 1

        data = {**data, "id_final": _derive_id_final(data)}
        prepared = prepare_doc(data)
        doc = to_document(
            manifest_path="",
            rel_from_root=data["id_final"],
            meta=data,
            html_files=[],
            parsed_files=[prepared],
        )
        item = build_update_item(
            doc=doc, doc_data=prepared, unit={"rel_from_root": data["id_final"]},
            filename="msg.jsonl", source_path=f"jsonl:{data['id_final']}",
        )
        if item is None:
            none_id += 1
            continue
        assembled = item["doc"]
        for k, v in assembled.items():
            if v in (None, "", [], {}):
                empty_keys[k] += 1
            else:
                out_keys[k] += 1

        if shown < N:
            shown += 1
            print(f"\n=== record {shown}  _id={item['_id']!r} ===")
            print("  prepare_doc ->", json.dumps(prepared, ensure_ascii=False))
            print("  assembled   ->")
            for k, v in assembled.items():
                print(f"     {k:14} = {json.dumps(v, ensure_ascii=False)}")

print("\n" + "=" * 60)
print(f"records scanned: {n}")
print(f"source keys:        {dict(src_keys)}")
print(f"non-empty out keys: {dict(out_keys)}")
print(f"empty out keys:     {dict(empty_keys)}")
print(f"records with doc_link in source (DROPPED by to_document): {dropped_doc_link}")
print(f"records with no id_final (skipped): {none_id}")
