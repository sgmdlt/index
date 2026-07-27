"""mos-gorsud card parsing at index time."""
from pathlib import Path

from src.parse_html import fill_raw_tables
from src.parse_html_mgs import fill_raw_tables_mgs, is_mgs, mgs_identity

CARD = (Path(__file__).parent / "card_civil.html").read_text(encoding="utf-8")
URL = "https://mos-gorsud.ru/rs/horoshevskij/services/cases/civil/details/x"


def test_is_mgs_recognises_a_moscow_card():
    assert is_mgs(CARD) is True
    assert is_mgs("<div id='cont_doc'></div>") is False


def test_raw_tables_carry_the_head_and_every_tab():
    raw = fill_raw_tables_mgs(CARD)["raw_tables"]
    assert raw["Дело"]["Дата поступления"] == "29.10.2019"
    assert list(raw) == ["Дело", "Движение дела", "Судебные заседания", "Документы"]


def test_movement_keeps_the_section_nesting_prepare_doc_expects():
    raw = fill_raw_tables_mgs(CARD)["raw_tables"]
    assert [next(iter(t)) for t in raw["Движение дела"]] == \
        ["История состояний", "История местонахождения"]
    rows = raw["Движение дела"][0]["История состояний"]
    assert rows[0] == {"Дата": "29.10.2019",
                       "Состояние": "Регистрация поступившего заявления",
                       "Документ-основание": ""}


def test_fill_raw_tables_dispatches_moscow_pages():
    """The sudrf parser produces nothing for a Moscow card; the entry point
    must route it to the MGS parser instead."""
    raw = fill_raw_tables(CARD)["raw_tables"]
    assert raw["Дело"]["Дата поступления"] == "29.10.2019"


def test_identity_follows_the_case_not_the_material():
    """The head describes the материал: number "...∼ М-8486/2019", «Дата
    поступления» 29.10.2019. The дело is 02-0681 registered 23.12.2019."""
    ident = mgs_identity(CARD, URL)
    assert ident["court_code"] == "77RS0031"
    assert ident["case_number"] == "02-0681/2020"
    assert ident["date_reg"] == "2019-12-23"
    assert ident["id_final"] == "77RS0031_2019-12-23_02-0681"


def test_identity_uses_the_url_for_appeals():
    """A Мосгорсуд appeal must key to 77OS0000, not the first-instance court."""
    ident = mgs_identity(CARD, "https://mos-gorsud.ru/mgs/services/cases/appeal-civil/details/y")
    assert ident["court_code"] == "77OS0000"
