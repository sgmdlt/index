"""Integration test: parse HTML fixtures → prepare_doc → merge_docs.

Fixtures in tests/merge_test/ are three HTML snapshots of the same court case
(03RS0004-01-2025-010676-35), simulating how the pipeline processes multiple
HTML files for one unit.
"""

import asyncio
import sys
import io
from pathlib import Path

from src.bulk_indexer import merge_docs
from src.parse_html import fill_raw_tables
from src.prepare_doc import prepare_doc


def _fixture(name: str) -> Path:
    return Path(__file__).parent / "merge_test" / name


def _parse(filename: str) -> dict:
    html = _fixture(filename).read_text(errors="replace")
    raw = fill_raw_tables(html)
    return prepare_doc(raw)


def test_merge():
    doc1 = _parse("merge_fixture1.html")
    doc2 = _parse("merge_fixture2.html")
    doc3 = _parse("merge_fixture3.html")

    # Assign ascending timestamps to simulate files arriving oldest-first
    doc1["version_ts"] = "2025-10-21T00:00:00"
    doc2["version_ts"] = "2025-10-22T00:00:00"
    doc3["version_ts"] = "2025-10-28T00:00:00"

    merged = merge_docs(None, doc1)
    merged = merge_docs(merged, doc2)
    merged = merge_docs(merged, doc3)

    # Scalar fields preserved
    assert merged["id_uid"] == "03RS0004-01-2025-010676-35"
    assert merged["judge"] == ["Судья Первая", "Судья Вторая"]

    # version_ts takes the latest
    assert merged["version_ts"] == "2025-10-28T00:00:00"

    # articles present from fixture2, merged as deduplicated array
    assert isinstance(merged.get("articles"), list)
    assert len(merged["articles"]) > 0
