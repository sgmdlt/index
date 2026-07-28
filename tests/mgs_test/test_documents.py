"""Documents come out of WARC as bytes; the indexer extracts their text."""
from pathlib import Path

from src.parse_html_mgs import fill_raw_tables_mgs

CARD = (Path(__file__).parent / "card_civil.html").read_text(encoding="utf-8")
DOC = ("https://mos-gorsud.ru/rs/horoshevskij/cases/docs/content/"
       "4823f0b2-9a83-487c-8fea-b49c2a948770")


def _rows(raw):
    return raw["raw_tables"]["Документы"][0]["Документы из описи"]


def test_without_documents_the_cell_keeps_the_link():
    """Nothing to substitute — the link stays so the loss is visible."""
    linked = [r for r in _rows(fill_raw_tables_mgs(CARD))
              if r["Текст документа"].startswith("http")]
    assert len(linked) == 1


def test_document_bytes_become_the_cell_text():
    docs = {DOC: ("РЕШЕНИЕ ИМЕНЕМ РОССИЙСКОЙ ФЕДЕРАЦИИ".encode("utf-8"), "txt")}
    rows = _rows(fill_raw_tables_mgs(CARD, docs))
    assert any(r["Текст документа"] == "РЕШЕНИЕ ИМЕНЕМ РОССИЙСКОЙ ФЕДЕРАЦИИ"
               for r in rows)
    assert not any(r["Текст документа"].startswith("http") for r in rows)


def test_sudebnye_akty_are_built_from_the_documents():
    """The section the indexer's _document_text rule reads."""
    docs = {DOC: ("РЕШЕНИЕ ИМЕНЕМ".encode("utf-8"), "txt")}
    raw = fill_raw_tables_mgs(CARD, docs)["raw_tables"]
    assert raw["СУДЕБНЫЕ АКТЫ"] == [
        {"Судебный акт #1 (Мотивированное решение)": "РЕШЕНИЕ ИМЕНЕМ"}]


def test_an_unextractable_document_leaves_the_link_alone():
    docs = {DOC: (b"\x00\x01\x02not a document", None)}
    rows = _rows(fill_raw_tables_mgs(CARD, docs))
    assert any(r["Текст документа"].startswith("http") for r in rows)
