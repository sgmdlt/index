from src.extract_document import _norm, to_text


def test_keeps_indentation_and_line_breaks():
    """Indentation is the formatting: a flattened act is unreadable."""
    raw = "      ПОСТАНОВЛЕНИЕ\n\n      г. Москва          30 июля 2026 г.\n"
    assert _norm(raw) == "      ПОСТАНОВЛЕНИЕ\n\n      г. Москва          30 июля 2026 г."


def test_normalises_line_endings():
    assert _norm("а\r\nб\rв") == "а\nб\nв"


def test_drops_trailing_spaces_and_nbsp():
    assert _norm("а  \n\xa0б \t\n") == "а\n б"


def test_collapses_runs_of_blank_lines_to_one():
    assert _norm("а\n\n\n\n\nб") == "а\n\nб"


def test_strips_leading_and_trailing_blank_lines():
    assert _norm("\n\n  текст  \n\n\n") == "  текст"


def test_empty_stays_empty():
    assert _norm("   \n\n \t ") == ""


def test_unavailable_marker_found_across_a_line_break():
    """The marker survives now that line breaks are no longer flattened away."""
    body = "Документ временно\nнедоступен".encode("cp1251")
    assert to_text(body, "txt").reason == "unavailable"


def test_extracted_text_keeps_its_layout():
    body = "      ПРИГОВОР\n\n      г. Москва".encode("cp1251")
    assert to_text(body, "txt").text == "      ПРИГОВОР\n\n      г. Москва"


def test_prepare_doc_keeps_the_first_line_indent():
    """The whole-text strip cut the opening indent back off downstream."""
    from src.prepare_doc import prepare_doc
    act = "      ПРИГОВОР\n\n      Именем Российской Федерации"
    doc = prepare_doc({"raw_tables": {"СУДЕБНЫЕ АКТЫ": [{"Судебный акт #1 (Приговор)": act}]}})
    assert doc["_document_text"][0]["text"] == act


def test_prepare_doc_still_drops_a_blank_act():
    from src.prepare_doc import prepare_doc
    doc = prepare_doc({"raw_tables": {"СУДЕБНЫЕ АКТЫ": [{"Судебный акт #1 (Приговор)": "  \n \n"}]}})
    assert not doc.get("_document_text")
