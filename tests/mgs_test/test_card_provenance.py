"""link / uuid / court_name / instance for an archived mos-gorsud card.

sudrf documents in the index carry all four; MGS cards indexed without them,
so a Moscow case could not be filtered by instance nor linked back to its
source page. None of the four is in the card markup — every one is derivable
from the URL the crawler already stores in the manifest.
"""
from pathlib import Path

from src.load_from_db import enrich_mgs_meta
from src.parse_html_mgs import mgs_identity
from src.to_document import to_document

CARD = (Path(__file__).parent / "card_civil.html").read_bytes()
UUID = "260ca701-38dc-11f1-9a49-2d939b4e4d75"
URL = (f"https://mos-gorsud.ru/rs/horoshevskij/services/cases/civil/details/{UUID}")


def test_identity_carries_the_source_page():
    identity = mgs_identity(CARD, URL)
    assert identity["link"] == URL
    assert identity["uuid"] == UUID


def test_court_name_comes_from_the_court_code():
    assert mgs_identity(CARD, URL)["court_name"] == "Хорошевский районный суд"


def test_instance_comes_from_the_url_segment():
    """Only the URL says which instance heard the case; the card never does."""
    appeal = URL.replace("/cases/civil/", "/cases/appeal-civil/")
    assert mgs_identity(CARD, URL)["instance"] == "Первая инстанция"
    assert mgs_identity(CARD, appeal)["instance"] == "Апелляция"


def test_the_four_fields_survive_into_the_document():
    meta = enrich_mgs_meta({"url": URL, "court_code": "77RS0031"}, CARD)
    doc_meta = to_document(
        manifest_path="/w/s.warc.gz:0:1", rel_from_root="/w/s.warc.gz:0:1",
        meta=meta, html_files=["10-06-2026.12:00:00.html"], parsed_files=[{}],
    )["meta"]
    assert doc_meta["link"] == URL
    assert doc_meta["uuid"] == UUID
    assert doc_meta["court_name"] == "Хорошевский районный суд"
    assert doc_meta["instance"] == "Первая инстанция"


def test_a_sudrf_link_still_yields_its_own_uuid():
    """The MGS rule must not displace the _uid= form sudrf links use."""
    link = "http://x.sudrf.ru/modules.php?name=sud_delo&case_uid=abc-123&delo_id=1"
    doc_meta = to_document(
        manifest_path="m", rel_from_root="m", meta={"link": link},
        html_files=[], parsed_files=[],
    )["meta"]
    assert doc_meta["uuid"] == "abc-123"
