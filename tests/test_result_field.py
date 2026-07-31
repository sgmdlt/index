"""`result` — исход рассмотрения, а не текущий статус.

У sudrf исход лежит в «Результат рассмотрения» (86.1% карточек) и поле пустует,
пока дело не рассмотрено. У mos-gorsud первым в правиле стояло «Текущее
состояние», поэтому result заполнялся на 100% значениями вида «Зарегистрировано,
29.07.2026» — статус с датой. Поле keyword, так что кардинальность росла без
предела, а агрегация по исходам смешивала две шкалы.

Замер на 1312 карточках mos-gorsud: 76% «Текущего состояния» — процессуальный
статус («Зарегистрировано», «Назначено судебное заседание на»), и лишь выделенные
строки шапки несут настоящий исход. Статус целиком уезжает в case_state.

«Текущее состояние», «Решение первой инстанции», «Решение апелляции» и «Решение
по заявлению» на карточках sudrf не встречаются ни разу — правка касается только
московских.
"""
from pathlib import Path

from src.parse_html_mgs import fill_raw_tables_mgs
from src.prepare_doc import prepare_doc

CRIMINAL = (Path(__file__).parent / "mgs_test" / "card_criminal.html").read_text(
    encoding="utf-8")


def _doc(head: dict) -> dict:
    return prepare_doc({"raw_tables": {"Дело": head}})


def test_a_procedural_status_is_not_a_result():
    doc = _doc({"Текущее состояние": "Зарегистрировано, 29.07.2026"})
    assert "result" not in doc


def test_the_status_lands_in_its_own_field_whole():
    doc = _doc({"Текущее состояние": "Назначено судебное заседание на 28.07.2026"})
    assert doc["case_state"] == "Назначено судебное заседание на 28.07.2026"


def test_the_dedicated_outcome_row_fills_result():
    doc = _doc({"Текущее состояние": "Завершено, 21.07.2026",
                "Результат рассмотрения": "Иск удовлетворен"})
    assert doc["result"] == "Иск удовлетворен"
    assert doc["case_state"] == "Завершено, 21.07.2026"


def test_the_moscow_decision_rows_fill_result_too():
    assert _doc({"Решение первой инстанции": "Оставить без изменения"})["result"] \
        == "Оставить без изменения"
    assert _doc({"Решение апелляции": "Отменено"})["result"] == "Отменено"
    assert _doc({"Решение по заявлению": "Удовлетворено"})["result"] == "Удовлетворено"


def test_a_sudrf_card_is_unaffected():
    """У sudrf «Текущего состояния» нет — исход как читался, так и читается."""
    doc = prepare_doc({"raw_tables": {"ДЕЛО": {
        "Результат рассмотрения": "Вынесен ПРИГОВОР"}}})
    assert doc["result"] == "Вынесен ПРИГОВОР"
    assert "case_state" not in doc


def test_a_real_card_in_flight_has_a_state_and_no_result():
    doc = prepare_doc(fill_raw_tables_mgs(CRIMINAL))
    assert doc["case_state"] == "Назначено судебное заседание на 28.07.2026 12:00"
    assert "result" not in doc
