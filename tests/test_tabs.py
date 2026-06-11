from src.parse_html import fill_raw_tables
from src.prepare_doc import prepare_doc
from pathlib import Path
import pytest


def _get_fixture_path(filename):
    fixture_path = Path(__file__).parent / "tabs_test" / filename
    return fixture_path

def test_tabs():
    with open(_get_fixture_path("html_tabs_fixture.html"), "r") as f:
        html = f.read()
        filled = fill_raw_tables(html)
        doc = prepare_doc(filled)

        assert doc["_document_text"][0]["name"] == "Судебный акт #1 (Решение)"
        assert doc["_document_text"][1]["name"] == "Судебный акт #2 (Решение)"

