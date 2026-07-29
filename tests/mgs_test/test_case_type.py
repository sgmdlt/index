"""case_type for an MGS card, without disturbing the courts vocabulary.

meta["case_type"] means a short code ('КоАП') that to_document looks up in
CASE_TYPE_MAPPING together with a numeric instance — that is what courts
manifests carry. MGS has no such code; it knows the resolved name outright.
Putting the name in the same key would leave one field holding two
vocabularies, so the resolved name travels under its own key.
"""
from pathlib import Path

from src.load_from_db import enrich_mgs_meta
from src.to_document import to_document

CARD = (Path(__file__).parent / "card_civil.html").read_bytes()
CRIMINAL = ("https://mos-gorsud.ru/rs/butyrskij/services/cases/criminal/"
            "details/4b4953b0-5b64-11f1-b16f-6fddbefe4fc3")


def _meta_of(meta):
    return to_document(manifest_path="m", rel_from_root="m", meta=meta,
                       html_files=[], parsed_files=[])["meta"]


def test_an_mgs_card_gets_its_case_type():
    meta = enrich_mgs_meta({"url": CRIMINAL}, CARD)
    assert _meta_of(meta)["case_type"] == "Уголовное дело"


def test_a_courts_code_still_resolves_through_the_mapping():
    """The normal courts path: numeric instance + short code."""
    doc_meta = _meta_of({"instance": 2, "case_type": "КоАП"})
    assert doc_meta["case_type"] == "Дело об административном правонарушении"
    assert doc_meta["instance"] == "Апелляция"


def test_an_unmapped_courts_code_never_leaks_into_the_document():
    """No mapping entry means no case_type — never the raw code."""
    assert _meta_of({"instance": 9, "case_type": "КоАП"}).get("case_type") is None
