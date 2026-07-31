"""Отказ bulk-записи должен быть слышен вызывающему.

Наблюдалось на прогоне MGS: ES отклонил весь батч (security_exception на все
200 документов), а строки outbox остались в DONE и прогон отчитался
docsIndexed=200. Правду знал только failed_paths.log. То есть разовый сбой ES
тихо терял дела, и найти их можно было лишь полной переиндексацией.

Причин было две. Поштучные ошибки внутри ответа bulk никуда не сообщались
кроме файла. А отказ всего вызова поднимал исключение из демон-потока, где
его никто не ловил, — поток умирал молча.

Счётчик docsIndexed при этом рос на весь размер батча независимо от отказов.
"""
import pytest

from src.bulk_indexer import BulkIndexer
from src.metrics import Metrics


class FakeES:
    """bulk отвечает по сценарию: dict — ответ ES, Exception — отказ вызова."""

    def __init__(self, *responses):
        self._responses = list(responses)
        self.calls = 0

    def mget(self, index=None, body=None, **kw):
        return {"docs": []}

    def bulk(self, operations=None, refresh=False):
        self.calls += 1
        r = self._responses[min(self.calls - 1, len(self._responses) - 1)]
        if isinstance(r, Exception):
            raise r
        return r


def _item(doc_id: str) -> dict:
    return {"_id": doc_id, "doc": {"id_final": doc_id}, "version_ts": "2026-01-01"}


def _ok(*ids) -> dict:
    return {"items": [{"index": {"_id": i}} for i in ids]}


def _mixed(ok_ids, bad_ids) -> dict:
    items = [{"index": {"_id": i}} for i in ok_ids]
    items += [{"index": {"_id": i, "error": {"type": "mapper_parsing_exception"}}}
              for i in bad_ids]
    return {"items": items}


def _indexer(es, tmp_path, on_result=None, metrics=None):
    return BulkIndexer(es_client=es, index="t", metrics=metrics,
                       bulk_docs=1000, flush_interval=3600,
                       failed_log=str(tmp_path / "failed.log"),
                       on_result=on_result)


def test_a_rejected_item_is_reported_to_the_caller(tmp_path):
    seen = []
    idx = _indexer(FakeES(_mixed(["a"], ["b"])), tmp_path,
                   on_result=lambda ok, bad, reason: seen.append((ok, bad)))
    idx.add("k", _item("a"))
    idx.add("k", _item("b"))
    idx.drain()
    assert seen == [(["a"], ["b"])]


def test_a_rejected_batch_reports_every_document(tmp_path):
    """Отказ всего вызова — тот самый security_exception на 200 документов."""
    seen = []
    idx = _indexer(FakeES(RuntimeError("security_exception")), tmp_path,
                   on_result=lambda ok, bad, reason: seen.append((ok, bad, reason)))
    idx.add("k", _item("a"))
    idx.add("k", _item("b"))
    idx.drain()
    assert len(seen) == 1
    ok, bad, reason = seen[0]
    assert ok == []
    assert sorted(bad) == ["a", "b"]
    assert "security_exception" in reason


def test_a_rejected_batch_does_not_kill_the_run(tmp_path):
    """drain работает в главном потоке: исключение оттуда обрывало бы прогон,
    хотя из фонового потока оно и так пропадало впустую."""
    idx = _indexer(FakeES(RuntimeError("boom")), tmp_path)
    idx.add("k", _item("a"))
    idx.drain()  # не должно бросить


def test_only_accepted_documents_are_counted_as_indexed(tmp_path):
    metrics = Metrics()
    idx = _indexer(FakeES(_mixed(["a", "b"], ["c"])), tmp_path, metrics=metrics)
    for i in ("a", "b", "c"):
        idx.add("k", _item(i))
    idx.drain()
    assert metrics.counters["docsIndexed"] == 2
    assert metrics.counters["bulkFailedDocs"] == 1


def test_nothing_indexed_when_the_whole_batch_is_rejected(tmp_path):
    metrics = Metrics()
    idx = _indexer(FakeES(RuntimeError("nope")), tmp_path, metrics=metrics)
    idx.add("k", _item("a"))
    idx.drain()
    assert metrics.counters["docsIndexed"] == 0


def test_a_clean_batch_reports_success(tmp_path):
    seen = []
    idx = _indexer(FakeES(_ok("a", "b")), tmp_path,
                   on_result=lambda ok, bad, reason: seen.append((ok, bad)))
    idx.add("k", _item("a"))
    idx.add("k", _item("b"))
    idx.drain()
    assert seen == [(["a", "b"], [])]
