"""Test that documents from multiple HTML files for the same case are all preserved.

The problem: apply_merge was merging each batch item independently against the
existing ES doc, so when two items share the same _id the second bulk index op
would overwrite the first, losing the first file's text.
"""

import asyncio
from pathlib import Path

from src.bulk_indexer import apply_merge, build_update_item
from src.parse_html import fill_raw_tables
from src.prepare_doc import prepare_doc


def _fixture(name: str) -> Path:
    return Path(__file__).parent / "nested_test" / name


def _parse(filename: str) -> dict:
    html = _fixture(filename).read_bytes()
    raw = fill_raw_tables(html)
    return prepare_doc(raw)


def _make_item(doc_data: dict, filename: str) -> dict:
    doc = {"id_final": "test-nested-case"}
    unit = {"html_files": ["01-01-2010.03:00:01.html", "02-01-2010.03:00:01.html"], "meta": {"ID": "test-case-id"}}
    return build_update_item(doc, doc_data, unit, filename, tag=None)


def test_documents_accumulate_across_html_files():
    """Both HTML files' texts must appear in the merged documents list."""
    doc_data1 = _parse("01-01-2010.03:00:01.html")
    doc_data2 = _parse("02-01-2010.03:00:01.html")

    item1 = _make_item(doc_data1, "01-01-2010.03:00:01.html")
    item2 = _make_item(doc_data2, "02-01-2010.03:00:01.html")

    assert item1 is not None
    assert item2 is not None
    assert item1["_id"] == item2["_id"], "Both files must map to the same doc _id"

    # Both items have text extracted
    assert item1["doc"]["documents"], "fixture1 should have document text"
    assert item2["doc"]["documents"], "fixture2 should have document text"

    # Simulate _do_bulk: sort oldest-first, no existing ES doc
    batch = sorted([item1, item2], key=lambda x: x.get("version_ts") or "")
    merged_items = apply_merge(batch, existing_by_id={})

    # Only one merged doc should come out (deduplicated by _id)
    assert len(merged_items) == 1, f"Expected 1 merged doc, got {len(merged_items)}"

    docs = merged_items[0].get("documents", [])
    filenames = [d["filename"] for d in docs]

    assert "01-01-2010.03:00:01.html" in filenames, f"fixture1 text missing. documents: {filenames}"
    assert "ПЕРВОЕ РЕШЕНИЕ" in docs[0].get("text", ""), "fixture1 text content missing"

    assert "02-01-2010.03:00:01.html" in filenames, f"fixture2 text missing. documents: {filenames}"
    assert "ВТОРОЕ РЕШЕНИЕ" in docs[1].get("text", ""), "fixture2 text content missing"

    assert len(docs) == 2, f"Expected 2 documents entries, got {len(docs)}"
