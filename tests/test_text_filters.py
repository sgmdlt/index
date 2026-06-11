from src.bulk_indexer import _build_documents_array
from src.text_filters import broken_text_message, fix_date_reg


def test_documents_array_flags_broken_text():
    doc_texts = [
        {"name": "Судебный акт #1", "text": "PK\x03\x04 docProps/core.xml junk"},
        {"name": "Судебный акт #2", "text": "ПРИГОВОР. Судья Петров"},
    ]
    result = _build_documents_array(doc_texts, "f.html", "2026-05-16T00:00:00Z")
    assert result[0]["text"] == "Вместо текста залит нечитаемый docx"
    assert result[0]["is_broken"] is True
    assert result[1]["text"] == "ПРИГОВОР. Судья Петров"
    assert "is_broken" not in result[1]


def test_docx_binary_detected():
    text = "PK\x03\x04 garbage [Content_Types].xml docProps/core.xml more garbage"
    assert broken_text_message(text) == "Вместо текста залит нечитаемый docx"


def test_server_error_detected():
    text = "judicial_web/some/path Exception in module"
    assert broken_text_message(text) == "Вместо текста отображается ошибка сервера"


def test_judicial_web_with_judge_is_fine():
    text = "решение judicial_web упомянуто, Судья Иванов И.И."
    assert broken_text_message(text) is None


def test_sql_error_detected():
    assert broken_text_message("QUERY: SELECT * FROM x") == "Вместо текста отображается ошибка SQL"
    assert broken_text_message("FATAL: server closed the connection") == "Вместо текста отображается ошибка SQL"


def test_clean_text_passes():
    assert broken_text_message("ПРИГОВОР Именем Российской Федерации. Судья Петров") is None


TODAY = "2026-06-11T00:00:00Z"


def test_valid_date_reg_kept():
    assert fix_date_reg("2025-04-30T00:00:00Z", [], today=TODAY) == "2025-04-30T00:00:00Z"


def test_too_old_date_reg_replaced_from_progress():
    progress = [{"date": "15.03.1999"}, {"date": "20.05.2025"}, {"date": "10.04.2025"}]
    assert fix_date_reg("1970-01-01T00:00:00Z", progress, today=TODAY) == "2025-04-10T00:00:00Z"


def test_future_date_reg_replaced_from_progress():
    progress = [{"date": "20.05.2025"}]
    assert fix_date_reg("2027-09-01T00:00:00Z", progress, today=TODAY) == "2025-05-20T00:00:00Z"


def test_no_valid_progress_gives_none():
    assert fix_date_reg("1970-01-01T00:00:00Z", [{"date": "01.01.1999"}], today=TODAY) is None
