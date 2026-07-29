"""Card enrichment belongs in the pool, not in the parent.

`enrich_mgs_meta` soup-parses the card, and unit assembly runs single-threaded
in the parent. Measured on live data it cost 42.4 ms/unit against 1.0 ms for
the WARC read it sits next to — 98% of the parent's serial work, capping the
whole pipeline at ~23 units/s while ten pool workers idled at 9%. The same
bytes are parsed again in the worker, so the card was souped twice.

Courts never noticed: `is_mgs` is false for sudrf pages, so the call returned
immediately and courts ran ~102 units/s.
"""
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.load_from_db import _group_to_unit
from src.main import _run_pipelined
from src.metrics import Metrics

CARD = (Path(__file__).parent / "card_civil.html").read_bytes()
UUID = "260ca701-38dc-11f1-9a49-2d939b4e4d75"
URL = f"https://mos-gorsud.ru/rs/horoshevskij/services/cases/civil/details/{UUID}"
LOCATOR = "/warc/shard-1.warc.gz:0:123"
WARC_DATE = "2026-06-10T12:00:00Z"


class FakeIndexer:
    def __init__(self):
        self.items = []

    def add(self, bkey, update):
        self.items.append((bkey, update))


def test_unit_assembly_does_not_parse_the_card():
    """The parent hands the bytes on untouched; no soup on the serial path."""
    meta = {"url": URL, "court_code": "77RS0031"}
    unit = _group_to_unit(LOCATOR, meta, CARD, WARC_DATE)
    assert unit["meta"] == meta, "parent enriched the card instead of the worker"


def test_the_worker_still_delivers_the_enriched_identity(tmp_path):
    """Moving the work must not lose it: the four provenance fields and the
    resolved case type still have to reach the indexed document."""
    unit = _group_to_unit(LOCATOR, {"url": URL, "court_code": "77RS0031"},
                          CARD, WARC_DATE)
    indexer = FakeIndexer()
    with ProcessPoolExecutor(max_workers=1) as pool:
        _run_pipelined([unit], pool, indexer, Metrics(), max_workers=1,
                       failed_log=str(tmp_path / "failed.log"))

    [(_, update)] = indexer.items
    doc = update["doc"]
    assert doc["link"] == URL
    assert doc["uuid"] == UUID
    assert doc["court_name"] == "Хорошевский районный суд"
    assert doc["instance"] == "Первая инстанция"
    assert doc["case_type"] == "Гражданское дело"


def test_a_seeded_id_still_beats_the_parsed_one(tmp_path):
    """The merge rule has to survive the move: an id already in Elasticsearch
    wins, or a case renumbered since the dump lands as a second document."""
    unit = _group_to_unit(LOCATOR, {"url": URL, "court_code": "77RS0031",
                                    "ID": "77RS0031_2026-01-01_seeded"},
                          CARD, WARC_DATE)
    indexer = FakeIndexer()
    with ProcessPoolExecutor(max_workers=1) as pool:
        _run_pipelined([unit], pool, indexer, Metrics(), max_workers=1,
                       failed_log=str(tmp_path / "failed.log"))

    [(_, update)] = indexer.items
    assert update["doc"]["id_final"] == "77RS0031_2026-01-01_seeded"
