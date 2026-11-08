"""Parse rejects (sql_error / empty_html) propagate as redownload reasons."""

from concurrent.futures import ProcessPoolExecutor

from src.load_from_db import VALID_REASONS
from src.main import _run_pipelined
from src.metrics import Metrics

SQL_DUMP_HTML = (
    b'<div id="content"><br><b>QUERY:</b> SELECT A.id FROM adm_case A<br>'
    b"<b>ERROR:</b> server closed the connection unexpectedly</div>"
)

FILLING_BANNER_HTML = (
    b"<body><td>\xd0\x92 \xd0\xbd\xd0\xb0\xd1\x81\xd1\x82\xd0\xbe\xd1\x8f\xd1\x89"
    b"\xd0\xb8\xd0\xb9 \xd0\xbc\xd0\xbe\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82</td></body>"
)


class FakeIndexer:
    def add(self, bkey, update):
        pass


def make_unit(html: bytes, locator: str) -> dict:
    fname = "10-06-2026.12:00:00.html"
    return {
        "manifest_path": locator,
        "rel_from_root": locator,
        "meta": {"ID": "bad-case-1", "court_id": "42RS0021"},
        "html_files": [fname],
        "html_bytes_by_name": {fname: html},
    }


def run_one(unit, tmp_path):
    completed = []
    with ProcessPoolExecutor(max_workers=1) as pool:
        _run_pipelined(
            [unit],
            pool,
            FakeIndexer(),
            Metrics(),
            max_workers=1,
            failed_log=str(tmp_path / "failed.log"),
            on_unit_complete=lambda u, ok, ids: completed.append((u, ok)),
        )
    assert completed and completed[0][1] is False
    return completed[0][0]


def test_sql_dump_payload_reason(tmp_path):
    unit = run_one(make_unit(SQL_DUMP_HTML, "/warc/s1.warc.gz:0:1"), tmp_path)
    assert unit["_fail_reason"] == "sql_error"


def test_empty_html_payload_reason(tmp_path):
    unit = run_one(make_unit(FILLING_BANNER_HTML, "/warc/s1.warc.gz:9:1"), tmp_path)
    assert unit["_fail_reason"] == "empty_html"


def test_reasons_are_valid_for_queue():
    assert {"sql_error", "empty_html"} <= VALID_REASONS
