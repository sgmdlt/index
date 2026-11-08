"""Эталон формулы id дела: тот же файл лежит в courts (tests/fixtures), и
build_case_id обоих репо обязан давать по нему одно и то же. Ключ — первый
выданный номер карточки (docs/adr/0001-stable-case-id.md, courts-271g)."""
import json
from pathlib import Path

import pytest

from src.utils.case_id import build_case_id

GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures/case_id_golden.json").read_text(encoding="utf-8")
)


@pytest.mark.parametrize("row", GOLDEN, ids=lambda r: r["case_number"])
def test_build_case_id_golden(row):
    assert build_case_id(row["case_number"], row["date_reg"], row["court_id"]) == row["expected"]
