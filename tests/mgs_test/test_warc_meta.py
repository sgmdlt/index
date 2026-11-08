"""The crawler cannot supply case_number/date_reg/id_final — it does not parse.
The indexer derives them when it reads the archived card back."""
from pathlib import Path

from src.load_from_db import enrich_mgs_meta

CARD = (Path(__file__).parent / "card_civil.html").read_bytes()
URL = "https://mos-gorsud.ru/rs/horoshevskij/services/cases/civil/details/x"


def test_meta_gains_identity_from_the_archived_card():
    meta = enrich_mgs_meta({"url": URL, "court_code": "77RS0031"}, CARD)
    assert meta["id_final"] == "77RS0031_2019-12-23_02-0681"
    assert meta["case_number"] == "02-0681/2020"
    assert meta["date_reg"] == "2019-12-23"
    assert meta["url"] == URL          # original keys survive


def test_a_seeded_id_is_never_overwritten():
    """Dump-seeded cases carry the id already in Elasticsearch; a court
    renumbering the case must not mint a second document."""
    meta = enrich_mgs_meta({"url": URL, "id_final": "SEEDED"}, CARD)
    assert meta["id_final"] == "SEEDED"


def test_non_mgs_html_is_left_alone():
    meta = enrich_mgs_meta({"ID": "x"}, b"<div id='cont_doc'></div>")
    assert meta == {"ID": "x"}
