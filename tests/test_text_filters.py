from src.bulk_indexer import _build_documents_array
from src.text_filters import (
    broken_text_message,
    fix_date_reg,
    recover_malformed_date,
)


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
    progress = [{"date": "15.03.1985"}, {"date": "20.05.2025"}, {"date": "10.04.2025"}]
    assert fix_date_reg("1970-01-01T00:00:00Z", progress, today=TODAY) == "2025-04-10T00:00:00Z"


def test_future_date_reg_replaced_from_progress():
    progress = [{"date": "20.05.2025"}]
    assert fix_date_reg("2027-09-01T00:00:00Z", progress, today=TODAY) == "2025-05-20T00:00:00Z"


def test_no_valid_progress_keeps_original():
    assert (
        fix_date_reg("1970-01-01T00:00:00Z", [{"date": "01.01.1985"}], today=TODAY)
        == "1970-01-01T00:00:00Z"
    )


def test_no_valid_progress_falls_back_to_date_result():
    assert (
        fix_date_reg(
            "1970-01-01T00:00:00Z",
            [{"date": "01.01.1985"}],
            date_result="2025-03-15T00:00:00Z",
            today=TODAY,
        )
        == "2025-03-15T00:00:00Z"
    )


def test_valid_progress_wins_over_date_result():
    assert (
        fix_date_reg(
            "1970-01-01T00:00:00Z",
            [{"date": "10.04.2025"}],
            date_result="2025-03-15T00:00:00Z",
            today=TODAY,
        )
        == "2025-04-10T00:00:00Z"
    )


def test_invalid_date_result_keeps_original():
    assert (
        fix_date_reg(
            "1970-01-01T00:00:00Z",
            [],
            date_result="2027-09-01T00:00:00Z",
            today=TODAY,
        )
        == "1970-01-01T00:00:00Z"
    )


def test_date_ge_1990_now_valid():
    assert fix_date_reg("1990-08-01", [], case_number="1-3/2024", today=TODAY) == "1990-08-01"


def test_malformed_year_fixed_from_case_number_keeps_month_day():
    assert (
        fix_date_reg("1010-06-28", [], case_number="2-559/2010-78", today=TODAY)
        == "2010-06-28"
    )


def test_placeholder_uses_case_year_jan_first():
    assert (
        fix_date_reg("1900-01-01", [], case_number="4У-1685/2012", today=TODAY)
        == "2012-01-01"
    )


def test_case_number_year_is_earliest_in_chain():
    assert (
        fix_date_reg(
            "1888-05-10", [], case_number="1-3/2024 (1-3/2023; 1-357/2002;)", today=TODAY
        )
        == "2002-05-10"
    )


def test_malformed_year_last2_no_case_number():
    # year 1015 -> last2 "15" -> 2015, month/day kept; case number has no /YYYY
    assert (
        fix_date_reg("1015-11-30", [], case_number="5-1/1015-159", today=TODAY)
        == "2015-11-30"
    )


def test_malformed_year_last2_preferred_over_case_number():
    # date_reg's own digits (1013 -> 2013) beat the renumbered case year (2014)
    assert (
        fix_date_reg("1013-11-28", [], case_number="2-23/2014-175", today=TODAY)
        == "2013-11-28"
    )


def test_placeholder_not_treated_as_malformed_year():
    # 1900-01-01 is the sentinel, not a real year -> must use case year, not 2000
    assert (
        fix_date_reg("1900-01-01", [], case_number="4У-1685/2012", today=TODAY)
        == "2012-01-01"
    )


def test_recover_malformed_date_last2():
    assert recover_malformed_date("217-08-28", TODAY) == "2017-08-28"
    assert recover_malformed_date("15-02-23", TODAY) == "2015-02-23"
    assert recover_malformed_date("16-06-16", TODAY) == "2016-06-16"


def test_recover_malformed_date_future_rejected():
    # 1973 -> last2 73 -> 2073, beyond today -> None
    assert recover_malformed_date("1973-12-04", TODAY) is None


def test_recover_malformed_date_unparseable():
    assert recover_malformed_date("", TODAY) is None
    assert recover_malformed_date(None, TODAY) is None


def test_genuine_pre_1990_kept_when_case_year_also_old():
    assert (
        fix_date_reg("1973-12-04", [], case_number="22-1/1973", today=TODAY)
        == "1973-12-04"
    )


def test_progress_wins_over_case_number_fallback():
    assert (
        fix_date_reg(
            "1900-01-01", [{"date": "10.04.2025"}], case_number="x/2012", today=TODAY
        )
        == "2025-04-10T00:00:00Z"
    )
