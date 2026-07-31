"""Статья УК с карточки mos-gorsud.

Отдельной строки со статьёй для уголовных дел на карточке нет — она приклеена
к ФИО подсудимого: «Галчабеков М.А. (Ст. 228.1, Ч. 3, п. б)». Замер на 1744
карточках: строка «Подсудимый» несёт статью в 156 случаях из 156, «Осужденный
(оправданный, обвиняемый)» — в 51 из 52, а «Стороны» не несёт никогда (0 из
1513). Кодекс в этих двух строках не назван ни разу, потому что дело уголовное
по определению; «Заявитель» же называет УПК (жалоба в порядке ст. 125) — это
предмет жалобы, а не обвинение, и сюда не попадает.

Формат приводится к тому, в котором статья уже лежит в индексе от sudrf:
«ст.158 ч.2 п.в УК РФ».
"""
from pathlib import Path

from src.parse_html_mgs import card_articles, fill_raw_tables_mgs
from src.prepare_doc import prepare_doc

CRIMINAL = (Path(__file__).parent / "card_criminal.html").read_text(encoding="utf-8")
CIVIL = (Path(__file__).parent / "card_civil.html").read_text(encoding="utf-8")


def test_the_article_is_read_out_of_the_defendant_row():
    assert card_articles({"Подсудимый": "Галчабеков М.А. (Ст. 228.1, Ч. 3, п. б)"}) \
        == "ст.228.1 ч.3 п.б УК РФ"


def test_a_convict_row_counts_too():
    head = {"Осужденный (оправданный, обвиняемый)": "Строчкова Н.Ю. (Ст. 264, Ч. 3)"}
    assert card_articles(head) == "ст.264 ч.3 УК РФ"


def test_every_defendant_contributes_their_own_article():
    """sudrf merges «Перечень статей» across the ЛИЦА rows with '; '."""
    head = {"Подсудимый": "Бойченко Ю.Я. (Ст. 160, Ч. 4), Манков А.А. (Ст. 159, Ч. 2)"}
    assert card_articles(head) == "ст.160 ч.4 УК РФ; ст.159 ч.2 УК РФ"


def test_several_articles_against_one_person_stay_separate():
    head = {"Подсудимый": "Иванов И.И. (Ст. 318, Ч. 1; Ст. 319)"}
    assert card_articles(head) == "ст.318 ч.1 УК РФ; ст.319 УК РФ"


def test_a_named_codex_is_kept_as_stated():
    """Never overwrite a codex the card spells out."""
    assert card_articles({"Подсудимый": "Петров П.П. (Ст. 125 УПК РФ)"}) \
        == "ст.125 УПК РФ"


def test_a_civil_card_yields_nothing():
    """«Стороны» carries no article on any of the 1513 cards measured."""
    assert card_articles({"Стороны": "ООО «Ромашка» (истец), Сидоров (ответчик)"}) == ""


def test_the_head_gains_no_key_when_there_is_no_article():
    raw = fill_raw_tables_mgs(CIVIL)["raw_tables"]
    assert "Перечень всех статей" not in raw["Дело"]


def test_a_real_criminal_card_reaches_the_articles_field():
    raw = fill_raw_tables_mgs(CRIMINAL)
    assert raw["raw_tables"]["Дело"]["Перечень всех статей"] == "ст.228.1 ч.3 п.б УК РФ"
    assert prepare_doc(raw)["articles"] == "ст.228.1 ч.3 п.б УК РФ"
