"""WARC units (html_bytes_by_name) flow through _run_pipelined to the indexer."""

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.main import _run_pipelined
from src.metrics import Metrics

FIXTURE = Path(__file__).parent / "merge_test" / "merge_fixture1.html"
LOCATOR = "/warc/shard-1.warc.gz:0:123"


class FakeIndexer:
    def __init__(self):
        self.items = []

    def add(self, bkey, update):
        self.items.append((bkey, update))


def make_warc_unit():
    html = FIXTURE.read_bytes()
    fname = "10-06-2026.12:00:00.html"
    return {
        "manifest_path": LOCATOR,
        "rel_from_root": LOCATOR,
        "meta": {"ID": "warc-case-1", "court_id": "03RS0004"},
        "html_files": [fname],
        "html_bytes_by_name": {fname: html},
    }


def test_warc_unit_indexes_with_locator_debug_link(tmp_path):
    indexer = FakeIndexer()
    completed = []
    with ProcessPoolExecutor(max_workers=1) as pool:
        _run_pipelined(
            [make_warc_unit()],
            pool,
            indexer,
            Metrics(),
            max_workers=1,
            failed_log=str(tmp_path / "failed.log"),
            on_unit_complete=lambda unit, ok: completed.append(ok),
        )

    assert completed == [True]
    assert len(indexer.items) == 1
    _, update = indexer.items[0]
    assert update["_id"] == "warc-case-1"
    assert update["doc"]["path_manifest"] == LOCATOR
    assert update["version_ts"] == "2026-06-10T12:00:00Z"
