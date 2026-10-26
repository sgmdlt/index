"""A partial «hidden since» envelope from the MGS open-data import.

It carries only id_final, case_hidden and hidden_since, and must flag an
indexed visible case without wiping its link, texts or progress.
"""
from src.bulk_indexer import build_update_item, merge_docs
from src.main import _derive_id_final
from src.prepare_doc import prepare_doc
from src.to_document import to_document

MARK = {"id_final": "77RS0001_2026-01-02_1-4", "case_hidden": True,
        "hidden_since": "2026-10-25"}


def _incoming(data: dict) -> dict:
    """The same steps `_run_jsonl` takes for one record."""
    data = {**data, "id_final": _derive_id_final(data)}
    prepared = prepare_doc(data)
    doc = to_document(manifest_path="", rel_from_root=data["id_final"], meta=data,
                      html_files=[], parsed_files=[prepared])
    return build_update_item(doc=doc, doc_data=prepared,
                             unit={"rel_from_root": data["id_final"]},
                             filename="hidden-v285.jsonl", source_path="jsonl:x")


def test_mark_keeps_its_id():
    assert _incoming(MARK)["_id"] == "77RS0001_2026-01-02_1-4"


def test_mark_carries_hidden_since():
    doc = _incoming(MARK)["doc"]
    assert doc["case_hidden"] is True
    assert doc["hidden_since"] == "2026-10-25"


def test_mark_merged_into_visible_doc_keeps_link_texts_and_progress():
    existing = {
        "link": "https://mos-gorsud.ru/rs/x/services/cases/criminal/details/u",
        "case_hidden": False,
        "documents": [{"name": "Приговор", "text": "текст", "date": "2026-02-01"}],
        "progress": [{"date": "01.02.2026", "event": "Вынесен приговор"}],
        "defendants": ["Иванов И.И."],
        "version": 3, "version_ts": "2026-02-01T00:00:00Z",
    }
    merged = merge_docs(existing, _incoming(MARK)["doc"])
    assert merged["link"] == existing["link"]
    assert merged["documents"] == existing["documents"]
    assert merged["progress"] == existing["progress"]
    assert merged["defendants"] == ["Иванов И.И."]
    assert merged["case_hidden"] is True
    assert merged["hidden_since"] == "2026-10-25"
    assert merged["version"] == 4
