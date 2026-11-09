"""Filesystem units (HTML beside manifest.json) flow through _run_pipelined."""

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import cast

from src.bulk_indexer import BulkIndexer
from src.main import _run_pipelined
from src.metrics import Metrics

FIXTURE = Path(__file__).parent / "merge_test" / "merge_fixture1.html"
FNAME = "10-06-2026.12:00:00.html"
REL = "26/03/03RS0004/1/case-1/manifest.json"


class FakeIndexer:
    def __init__(self):
        self.items = []

    def add(self, bkey, update):
        self.items.append((bkey, update))


def test_fs_unit_reads_html_beside_the_manifest(tmp_path):
    case_dir = tmp_path / "case-1"
    case_dir.mkdir()
    (case_dir / FNAME).write_bytes(FIXTURE.read_bytes())
    unit = {
        "manifest_path": str(case_dir / "manifest.json"),
        "rel_from_root": REL,
        "meta": {"ID": "fs-case-1", "court_id": "03RS0004"},
        "html_files": [FNAME],
    }
    indexer = FakeIndexer()
    metrics = Metrics()
    with ProcessPoolExecutor(max_workers=1) as pool:
        _run_pipelined(
            [unit],
            pool,
            cast(BulkIndexer, indexer),
            metrics,
            max_workers=1,
            failed_log=str(tmp_path / "failed.log"),
        )

    [(_, update)] = indexer.items
    assert update["_id"] == "fs-case-1"
    assert update["metadata"] == {"sourcePath": f"26/03/03RS0004/1/case-1/{FNAME}"}
    assert metrics.counters["htmlBytes"] == FIXTURE.stat().st_size
