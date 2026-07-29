"""A WARC unit's documents must survive the trip into the parse worker.

`rows_to_units` attaches the archived document bytes to the unit, but the only
thing that consumes them is `fill_raw_tables_mgs` — several layers down, behind
a process pool. Dropping them there costs nothing visible: the card still
indexes, just with no decision text at all.
"""
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.main import _run_pipelined
from src.metrics import Metrics

CARD = (Path(__file__).parent / "card_civil.html").read_bytes()
DOC = ("https://mos-gorsud.ru/rs/horoshevskij/cases/docs/content/"
       "4823f0b2-9a83-487c-8fea-b49c2a948770")
LOCATOR = "/warc/shard-1.warc.gz:0:123"
FNAME = "10-06-2026.12:00:00.html"


class FakeIndexer:
    def __init__(self):
        self.items = []

    def add(self, bkey, update):
        self.items.append((bkey, update))


def test_warc_unit_documents_reach_the_parser(tmp_path):
    unit = {
        "manifest_path": LOCATOR,
        "rel_from_root": LOCATOR,
        "meta": {"ID": "mgs-case-1", "court_code": "77RS0022"},
        "html_files": [FNAME],
        "html_bytes_by_name": {FNAME: CARD},
        "documents": {DOC: ("РЕШЕНИЕ ИМЕНЕМ РОССИЙСКОЙ ФЕДЕРАЦИИ".encode(), "txt")},
    }
    indexer = FakeIndexer()
    with ProcessPoolExecutor(max_workers=1) as pool:
        _run_pipelined([unit], pool, indexer, Metrics(), max_workers=1,
                       failed_log=str(tmp_path / "failed.log"))

    [(_, update)] = indexer.items
    [archived] = update["doc"]["documents"]
    assert archived["name"] == "Судебный акт #1 (Мотивированное решение)"
    assert archived["text"] == "РЕШЕНИЕ ИМЕНЕМ РОССИЙСКОЙ ФЕДЕРАЦИИ"
