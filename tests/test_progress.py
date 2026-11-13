from pathlib import Path

from src.bulk_indexer import merge_docs
from src.parse_html import fill_raw_tables
from src.prepare_doc import prepare_doc


def _get_fixture_path(filename):
    fixture_path = Path(__file__).parent / "progress_test" / filename
    return fixture_path


def _parse(filename: str) -> dict:
    html = _get_fixture_path(filename).read_text(errors="replace")
    raw = fill_raw_tables(html)
    return prepare_doc(raw)


def test_progress():
    doc1 = _parse("progress_fixture.html")
    # print("DOC1 PROGRESS:", doc1.get("progress"))
    doc2 = _parse("progress_fixture_2.html")
    # print("DOC2 PROGRESS:", doc2.get("progress"))

    merged = merge_docs(None, doc1)
    merged = merge_docs(merged, doc2)

    # Updating progress
    assert len(merged["progress"]) == len(doc2["progress"])


def test_retrochange_progress():
    doc1 = _parse("progress_fixture_2.html")
    # print("DOC1 PROGRESS:", doc1.get("progress"))
    doc2 = _parse("progress_fixture_3.html")
    # print("DOC2 PROGRESS:", doc2.get("progress"))

    merged = merge_docs(None, doc1)
    merged = merge_docs(merged, doc2)

    # print("MERGED PROGRESS:", merged.get("progress"))
    # Retrochange 1 progress
    assert len(merged["progress"]) == len(doc1["progress"]) + 1


def test_two_sudrf_templates_collapse_into_one_row_per_event():
    # Same case seen through both sudrf «Движение дела» templates:
    # «Время»/«Основание…» vs «Время события»/«Основания…».
    old = _parse("template_old.html")
    new = _parse("template_new.html")

    merged = merge_docs(merge_docs(None, old), new)

    assert [(r["date"], r["event"]) for r in merged["progress"]] == [
        ("05.12.2023", "Судебное заседание"),
        ("07.12.2023", "Сдача материалов дела в архив"),
    ]
    assert merged["progress"][1]["Время"] == "17:55"
    assert (
        merged["progress"][0]["Основание для выбранного результата события"]
        == "Назначено наказание"
    )
