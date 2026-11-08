"""Legacy sud_delo MS format: raw_json.court_decisions (no raw_tables) → the
indexer must surface each decision under its real document-type name (from the
decision filename) instead of the placeholder "raw_json"."""

from src.main import _court_decisions_to_raw_tables
from src.prepare_doc import prepare_doc


def _legacy(decisions: list[dict]) -> dict:
    return {
        "id_final": "78MS0164_2012-04-03_2-77",
        "court_code": "78MS0164",
        "raw_json": {"created_date": "03.04.2012", "court_decisions": decisions},
    }


def test_converter_builds_akty_from_filenames_and_keeps_court_decisions():
    data = _legacy([
        {"filename": "Постановление.doc", "created_date": "03.12.2012", "text": "\n\nПОСТАНОВЛЕНИЕ текст"},
        {"filename": "Решение.rtf", "created_date": "10.12.2012", "text": "РЕШЕНИЕ текст"},
    ])
    _court_decisions_to_raw_tables(data)
    assert data["raw_tables"]["СУДЕБНЫЕ АКТЫ"] == [
        {"Судебный акт #1 (Постановление)": "\n\nПОСТАНОВЛЕНИЕ текст"},
        {"Судебный акт #2 (Решение)": "РЕШЕНИЕ текст"},
    ]
    # court_decisions kept so date_result's created_date rule still resolves
    assert data["raw_json"]["court_decisions"][0]["text"] == "\n\nПОСТАНОВЛЕНИЕ текст"


def test_converter_skips_empty_text_and_missing_filename():
    data = _legacy([
        {"filename": "", "text": ""},                      # empty text → dropped
        {"filename": None, "text": "ТЕКСТ без имени"},      # no filename → no parens
    ])
    _court_decisions_to_raw_tables(data)
    assert data["raw_tables"]["СУДЕБНЫЕ АКТЫ"] == [{"Судебный акт #1": "ТЕКСТ без имени"}]


def test_converter_idempotent_when_akty_already_present():
    data = _legacy([{"filename": "Приговор.doc", "text": "T"}])
    data["raw_tables"] = {"СУДЕБНЫЕ АКТЫ": [{"Судебный акт #1 (Приговор)": "T"}]}
    _court_decisions_to_raw_tables(data)
    assert data["raw_tables"]["СУДЕБНЫЕ АКТЫ"] == [{"Судебный акт #1 (Приговор)": "T"}]


def test_prepare_doc_prefers_real_name_over_raw_json_placeholder():
    """Even though court_decisions[*].text is a later _document_text location,
    its 'raw_json' name must not clobber the real СУДЕБНЫЕ АКТЫ name."""
    data = _legacy([
        {"filename": "Постановление.doc", "created_date": "03.12.2012", "text": "ПОСТАНОВЛЕНИЕ о прекращении"},
    ])
    _court_decisions_to_raw_tables(data)
    doc = prepare_doc(data)
    names = [d["name"] for d in doc["_document_text"]]
    texts = [d["text"] for d in doc["_document_text"]]
    assert "ПОСТАНОВЛЕНИЕ о прекращении" in texts
    assert "raw_json" not in names
    assert "Судебный акт #1 (Постановление)" in names
