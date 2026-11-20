"""Старые форматы номеров (courts-2ymt.10): номера без четырёхзначного года.

Эталон — все документы sudata_v2 (2026-11-19, без МГС), для которых формула
не строила Индексный ID: `5-266\\14-130`, `2-530`, `219`,
`762/3460/13-ц (2/762/1777/13) ~ Материалы дела`. `indexed_id` — их ID в
индексе, `expected` — решение: номер целиком, «/» и «\\» → «-», без пробелов
(совпадает с indexed_id у 27 357 из 27 373; 15 переезжают, номер без цифр —
отказ, None).
"""

import gzip
import json
from pathlib import Path

from src.utils.case_id import case_id_from_fields

GOLDEN = Path(__file__).parent / "fixtures" / "legacy_number_golden.jsonl.gz"


def test_legacy_number_formats_keep_their_indexed_ids():
    with gzip.open(GOLDEN, "rt", encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh]
    wrong = [
        (r["case_number"], r["expected"], got)
        for r in rows
        if (
            got := case_id_from_fields(
                r["case_number"], r["date_reg"], r["court_code"], ""
            )
        )
        != r["expected"]
    ]
    assert len(rows) == 27373
    assert wrong == [], f"{len(wrong)} из {len(rows)}, первые: {wrong[:5]}"


def test_number_without_digits_is_refused():
    # «Неизвестно», «Материалы дела»: ключ из такого «номера» склеил бы все
    # такие дела суда за день в один документ
    assert case_id_from_fields("Неизвестно", "2016-08-22", "08RS0014", "") is None
