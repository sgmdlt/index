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


# --- дата в хвосте исхода -----------------------------------------------------
# Московские строки исхода дописывают к значению дату: «Удовлетворено,
# 22.07.2026». result — keyword, поэтому каждая дата плодит новое значение, и
# «Удовлетворено» перестаёт собираться в одну корзину. Замер на 1312 карточках:
# дата стоит у 89 из 90 заполненных исходов, и у всех 89 та же дата уже лежит
# в date_result — обрезка ничего не теряет.


def test_the_trailing_date_is_cut_off():
    assert _doc({"Решение по заявлению": "Удовлетворено, 22.07.2026"})["result"] \
        == "Удовлетворено"


def test_a_trailing_date_with_time_goes_too():
    assert _doc({"Результат рассмотрения":
                 "Оставить определение без изменения, 13.04.2026 10:00"})["result"] \
        == "Оставить определение без изменения"


def test_a_date_inside_the_wording_stays():
    """Режем только хвост, иначе исход теряет часть формулировки."""
    assert _doc({"Результат рассмотрения":
                 "Отменить постановление от 01.02.2026 и прекратить"})["result"] \
        == "Отменить постановление от 01.02.2026 и прекратить"


def test_a_sudrf_outcome_is_untouched():
    doc = prepare_doc({"raw_tables": {"ДЕЛО": {
        "Результат рассмотрения": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН"}}})
    assert doc["result"] == "Иск (заявление, жалоба) УДОВЛЕТВОРЕН"


def test_the_date_survives_in_date_result():
    doc = prepare_doc({"raw_tables": {"Дело": {
        "Решение по заявлению": "Удовлетворено, 22.07.2026",
        "Дата окончания": "22.07.2026"}}})
    assert doc["result"] == "Удовлетворено"
    assert doc["date_result"] == "2026-07-22"
