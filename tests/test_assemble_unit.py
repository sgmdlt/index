"""assemble_unit: распарсенные файлы юнита → элементы для индексатора."""

from pathlib import Path

from src.parse_html import fill_raw_tables
from src.prepare_doc import prepare_doc
from src.to_document import assemble_unit

FIXTURE = Path(__file__).parent / "merge_test" / "merge_fixture1.html"
LOCATOR = "/warc/shard-1.warc.gz:0:123"
FNAME = "10-06-2026.12:00:00.html"


def make_unit(meta: dict) -> dict:
    return {
        "manifest_path": LOCATOR,
        "rel_from_root": LOCATOR,
        "meta": meta,
        "html_files": [FNAME],
    }


def parsed() -> dict:
    return prepare_doc(fill_raw_tables(FIXTURE.read_bytes()))


def test_one_update_per_parsed_file():
    unit = make_unit(
        {"case_number": "1-55/2026", "date_reg": "10.06.2026", "court_id": "03RS0004"}
    )

    updates = assemble_unit(unit, [(FNAME, parsed())], lambda f: f"src:{f}")

    assert len(updates) == 1
    assert updates[0]["_id"] == "03RS0004_2026-06-10_1-55"
    assert updates[0]["version_ts"] == "2026-06-10T12:00:00Z"
    assert updates[0]["doc"]["path_manifest"] == LOCATOR
    assert updates[0]["metadata"] == {"sourcePath": f"src:{FNAME}"}


def test_unit_without_id_yields_nothing():
    unit = make_unit({"court_id": "03RS0004"})

    assert assemble_unit(unit, [(FNAME, parsed())], lambda f: f) == []
