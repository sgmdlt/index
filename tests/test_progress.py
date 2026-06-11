from src.parse_html import fill_raw_tables
from src.prepare_doc import prepare_doc
from pathlib import Path
import pytest
from src.bulk_indexer import merge_docs



def _get_fixture_path(filename):
    fixture_path = Path(__file__).parent / "progress_test" / filename
    return fixture_path

def _parse(filename: str) -> dict:
    html = _get_fixture_path(filename).read_text(errors="replace")
    raw = fill_raw_tables(html)
    return prepare_doc(raw)

def test_progress():
    doc1 = _parse("progress_fixture.html")
    # print("DOC1 PROGRESS:", doc1.get("progress"))
    doc2 = _parse("progress_fixture_2.html")
    # print("DOC2 PROGRESS:", doc2.get("progress"))

    merged = merge_docs(None, doc1)
    merged = merge_docs(merged, doc2)

    # Updating progress
    assert len(merged["progress"]) == len(doc2["progress"])

def test_retrochange_progress():
    doc1 = _parse("progress_fixture_2.html")
    # print("DOC1 PROGRESS:", doc1.get("progress"))
    doc2 = _parse("progress_fixture_3.html")
    # print("DOC2 PROGRESS:", doc2.get("progress"))

    merged = merge_docs(None, doc1)
    merged = merge_docs(merged, doc2)

    # print("MERGED PROGRESS:", merged.get("progress"))
    # Retrochange 1 progress
    assert len(merged["progress"]) == len(doc1["progress"]) + 1

