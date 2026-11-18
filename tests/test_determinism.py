"""Индексный ID — функция оригинала сохранений (ADR 0001, 0005, courts-2ymt).

Одни и те же сохранения карточки, поданные в любом порядке, любыми пачками и
прогонами, в любое время, дают одни и те же Индексные ID и один документ на UID
карточки. Сохранения идут тем же путём, что в боевом прогоне: WARC-юнит →
разбор → сборка документа → BulkIndexer → настоящий ES с refresh_interval=-1.
Ожидаемые ID — литералы из правила предметной области, а не пересчёт формулой
и не прогон-«пересборка»: так тест ловит и стабильно неверный ID.

Стенд: `_run_pipelined` с пулом потоков вместо процессов — время читают только
сборка документа и BulkIndexer в родителе, а часы time_machine подменяет на
весь процесс. Ветки main (`--db`, `--warc`) и курсор outbox не покрыты.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import time_machine

from src.bulk_indexer import BulkIndexer
from src.load_from_db import _warc_date_to_name
from src.main import _run_pipelined
from src.metrics import Metrics

FIXTURES = Path(__file__).parent / "determinism_test"


def save(name: str, **meta) -> dict:
    """Сохранение из фикстуры как WARC-юнит; `meta` подменяет поля оригинала."""
    folder = FIXTURES / name
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    fname = _warc_date_to_name(manifest.pop("_warc_date"))
    locator = f"{name}:0:0"
    return {
        "manifest_path": locator,
        "rel_from_root": locator,
        "meta": {**manifest, **meta},
        "html_files": [fname],
        "html_bytes_by_name": {fname: (folder / "card.html").read_bytes()},
    }


def index_runs(es, index: str, runs: list, failed_log: Path) -> None:
    """runs — [(сейчас, [пачка, …]), …]; пачка — список сохранений.

    Пачка заканчивается сбросом индексатора, прогон — refresh индекса, как в
    конце боевого прогона: до него поиск не видит записанного в прогоне.
    """
    # ни размер, ни таймер не сбрасывают буфер сами: граница пачки — drain()
    indexer = BulkIndexer(
        es_client=es,
        index=index,
        bulk_docs=10_000,
        flush_interval=3600,
        failed_log=str(failed_log),
    )
    for now, batches in runs:
        with time_machine.travel(now):
            for batch in batches:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    _run_pipelined(
                        iter(batch), pool, indexer, Metrics(), 1, str(failed_log)
                    )
                indexer.drain()
        es.indices.refresh(index=index)


def ids_by_uid(es, index: str) -> dict[str, list[str]]:
    resp = es.search(index=index, size=100, source=["uuid"], track_total_hits=True)
    hits = resp["hits"]["hits"]
    assert resp["hits"]["total"]["value"] == len(hits), "выдача обрезана по size"
    uids = sorted({h["_source"]["uuid"] for h in hits})
    return {
        u: sorted(h["_id"] for h in hits if h["_source"]["uuid"] == u) for u in uids
    }


UID = "5b97419f-6242-418c-8d3a-9e63d295f5a1"
# 52RS0007 1-427/2026: зарегистрировано 16.11 по местному времени суда;
# первое сохранение — до полуночи UTC, когда 16.11 для UTC ещё «завтра»
FIRST, SECOND = "52RS0007_1-427_1", "52RS0007_1-427_2"
BEFORE_UTC_MIDNIGHT = "2026-11-15T23:35:14Z"
NEXT_MORNING = "2026-11-16T09:30:51Z"
WEEK_LATER = "2026-11-23T12:00:00Z"


def _histories(s1, s2) -> dict:
    return {
        # как было: два прогона
        "as-happened": [(BEFORE_UTC_MIDNIGHT, [[s1]]), (NEXT_MORNING, [[s2]])],
        "reversed": [(BEFORE_UTC_MIDNIGHT, [[s2]]), (NEXT_MORNING, [[s1]])],
        # одна пачка: до полуночи UTC и неделю спустя
        "one-batch-before-utc-midnight": [(BEFORE_UTC_MIDNIGHT, [[s1, s2]])],
        "one-batch-week-later": [(WEEK_LATER, [[s2, s1]])],
        # одна пачка — одно сохранение, обе в одном прогоне без refresh между
        # ними: поиск близнецов не видит записанного в этом прогоне
        "two-batches-one-run": [(NEXT_MORNING, [[s1], [s2]])],
        "repeats": [
            (BEFORE_UTC_MIDNIGHT, [[s1]]),
            (BEFORE_UTC_MIDNIGHT, [[s1]]),
            (NEXT_MORNING, [[s2, s1]]),
            (WEEK_LATER, [[s2]]),
        ],
    }


HISTORIES = list(_histories(None, None))


@pytest.mark.parametrize("history", HISTORIES)
def test_card_registered_tomorrow_by_utc_keeps_its_date(
    test_es, es_index, tmp_path, history
):
    runs = _histories(save(FIRST), save(SECOND))[history]
    index_runs(test_es, es_index, runs, tmp_path / "failed.log")
    assert ids_by_uid(test_es, es_index) == {UID: ["52RS0007_2026-11-16_1-427"]}


@pytest.mark.parametrize("history", HISTORIES)
def test_court_editing_the_date_keeps_the_earliest(
    test_es, es_index, tmp_path, history
):
    # суд поправил дату регистрации между сохранениями: дело остаётся под
    # самой ранней датой карточки (ADR 0001)
    runs = _histories(
        save(FIRST, date_reg="14.11.2026"), save(SECOND, date_reg="16.11.2026")
    )[history]
    index_runs(test_es, es_index, runs, tmp_path / "failed.log")
    assert ids_by_uid(test_es, es_index) == {UID: ["52RS0007_2026-11-14_1-427"]}


# рост номера: первое сохранение — номер, с которым карточка появилась, второе —
# тот же номер, обросший новым; ID держится на первом выданном номере
NUMBER_GROWTH = {
    # гражданское: материал М-196 появился раньше дела 2-408
    "tilde": (
        "М-196/2026",
        "2-408/2026 ~ М-196/2026",
        "16.11.2026",
        "52RS0007_2026-11-16_М-196",
    ),
    # кассация: жалоба сохранена до того, как ей назначили производство
    "cassation": (
        "8Г-7981/2026",
        "8Г-7981/2026 [88-8481/2026]",
        "16.11.2026",
        "52RS0007_2026-11-16_8Г-7981",
    ),
    # переномерация: карточка 2008 года потеряла первый номер без года регистрации
    "renumbered": (
        "2-2/2010",
        "2-1/2011 (2-2/2010;)",
        "09.12.2008",
        "52RS0007_2008-12-09_2-2",
    ),
}


@pytest.mark.parametrize("growth", list(NUMBER_GROWTH))
@pytest.mark.parametrize("history", HISTORIES)
def test_number_growth_keeps_the_first_issued_number(
    test_es, es_index, tmp_path, history, growth
):
    first, grown, date_reg, expected = NUMBER_GROWTH[growth]
    runs = _histories(
        save(FIRST, case_number=first, date_reg=date_reg),
        save(SECOND, case_number=grown, date_reg=date_reg),
    )[history]
    index_runs(test_es, es_index, runs, tmp_path / "failed.log")
    assert ids_by_uid(test_es, es_index) == {UID: [expected]}


def test_stored_id_does_not_override_the_formula(test_es, es_index, tmp_path):
    # ID в манифесте посчитал сборщик (или старая формула) — индексатор его не
    # принимает на веру, Индексный ID строит сам из оригинала (ADR 0005)
    runs = [(NEXT_MORNING, [[save(SECOND, ID="52RS0007_2020-01-01_9-999")]])]
    index_runs(test_es, es_index, runs, tmp_path / "failed.log")
    assert ids_by_uid(test_es, es_index) == {UID: ["52RS0007_2026-11-16_1-427"]}


def test_save_the_formula_cannot_key_is_refused(test_es, es_index, tmp_path):
    # мусорная строка выдачи (31OS0000, ноябрь 2026): формула ID не строит, а
    # сохранённый «…_ID_NOT_PARSED» склеил бы все такие строки суда за день
    garbage = save(
        SECOND,
        case_number="nox.info - поиск персональных данных. Государственный проект.",
        ID="52RS0007_2026-11-16_ID_NOT_PARSED",
    )
    failed = tmp_path / "failed.log"
    index_runs(test_es, es_index, [(NEXT_MORNING, [[garbage]])], failed)
    assert ids_by_uid(test_es, es_index) == {}
    assert failed.read_text(encoding="utf-8").split() == [garbage["rel_from_root"]]
