"""Очередь на запись ограничена (courts-jyme).

Разбор HTML идёт быстрее записи в ES: без предела очередь пачек копилась в
памяти — прогон 19–20.11 дорос до 43 ГБ и 1,74 млн незаписанных документов.
Индексатор пишет одним потоком, держит не больше max_pending пачек в очереди,
а add() ждёт, пока место не освободится.
"""

import threading
import time

from src.bulk_indexer import BulkIndexer
from tests.test_bulk_uuid_fold import FakeES, _index_ops, _item


class StuckES(FakeES):
    """bulk ждёт, пока тест не отпустит ES."""

    def __init__(self):
        super().__init__()
        self.release = threading.Event()

    def bulk(self, operations=None, refresh=False):
        self.release.wait(timeout=10)
        return super().bulk(operations=operations, refresh=refresh)


def _indexer(es, tmp_path, seen=None, **kw):
    return BulkIndexer(
        es_client=es,
        index="t",
        failed_log=str(tmp_path / "failed.log"),
        on_result=(lambda ok, bad, why: seen.append((ok, bad, why)))
        if seen is not None
        else None,
        **kw,
    )


def _no_uuid(i: int) -> dict:
    # без uuid пачка не ищет близнецов — проверяется только очередь
    return _item(f"A_2026-06-24_{i}", f"{i}.html", "2026-06-25", uuid=None)


def _written(es) -> list[str]:
    return sorted(i for ops in es.bulk_calls for i, _ in _index_ops(ops))


def test_add_waits_while_the_queue_is_full(tmp_path):
    es = StuckES()
    indexer = _indexer(es, tmp_path, bulk_docs=1, flush_interval=3600, max_pending=1)
    indexer.add("k", _no_uuid(1))  # поток записи взял её и завис в ES
    indexer.add("k", _no_uuid(2))  # ждёт в очереди
    third = threading.Thread(target=indexer.add, args=("k", _no_uuid(3)))
    third.start()
    third.join(timeout=0.5)
    assert third.is_alive(), "add() не ждёт: очередь не ограничена"
    es.release.set()
    third.join(timeout=5)
    indexer.drain()
    assert _written(es) == ["A_2026-06-24_1", "A_2026-06-24_2", "A_2026-06-24_3"]


def test_threads_do_not_grow_with_batches(tmp_path):
    es = FakeES()
    indexer = _indexer(es, tmp_path, bulk_docs=1, flush_interval=3600, max_pending=2)
    before = threading.active_count()
    for i in range(200):
        indexer.add("k", _no_uuid(i))
    indexer.drain()
    assert threading.active_count() <= before + 1  # один поток записи
    assert len(_written(es)) == 200


def test_drain_writes_everything_and_can_be_called_twice(tmp_path):
    es = FakeES()
    indexer = _indexer(es, tmp_path, bulk_docs=1000, flush_interval=3600)
    for i in range(5):
        indexer.add("k", _no_uuid(i))
    indexer.drain()
    indexer.drain()  # DB-режим зовёт его из _run_db и из finally в main
    assert len(_written(es)) == 5


def test_old_partial_buffer_goes_out_with_the_next_document(tmp_path):
    # таймерного потока нет: неполный буфер старше flush_interval уходит при
    # следующем add(), остатки — в drain()
    es = FakeES()
    indexer = _indexer(es, tmp_path, bulk_docs=1000, flush_interval=0.1)
    indexer.add("k", _no_uuid(1))
    time.sleep(0.2)
    indexer.add("k", _no_uuid(2))
    deadline = time.monotonic() + 3
    while not es.bulk_calls and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _written(es) == ["A_2026-06-24_1", "A_2026-06-24_2"]
    indexer.drain()


def test_writer_survives_a_batch_that_blows_up(tmp_path):
    # упавший поток записи повесил бы add() навсегда
    seen: list = []
    es = FakeES()
    indexer = _indexer(es, tmp_path, seen, bulk_docs=1, flush_interval=3600)
    real = indexer._do_bulk
    calls = {"n": 0}

    def first_blows_up(batch, bucket_key, retries=3):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return real(batch, bucket_key, retries)

    indexer._do_bulk = first_blows_up
    indexer.add("k", _no_uuid(1))
    indexer.add("k", _no_uuid(2))
    indexer.drain()
    assert _written(es) == ["A_2026-06-24_2"]
    assert seen[0][1] == ["A_2026-06-24_1"] and "boom" in seen[0][2]


def test_drain_from_another_thread_while_a_producer_waits(tmp_path):
    # производитель висит в add() на полной очереди, drain() зовёт другой поток:
    # ни дедлока, ни пачки, оставшейся в очереди без потока записи
    es = StuckES()
    indexer = _indexer(es, tmp_path, bulk_docs=1, flush_interval=3600, max_pending=1)
    producer = threading.Thread(
        target=lambda: [indexer.add("k", _no_uuid(i)) for i in range(6)]
    )
    producer.start()
    time.sleep(0.2)  # производитель упёрся в очередь
    drainer = threading.Thread(target=indexer.drain)
    drainer.start()
    es.release.set()
    producer.join(timeout=5)
    drainer.join(timeout=5)
    assert not producer.is_alive() and not drainer.is_alive()
    indexer.drain()  # всё, что производитель положил после первого drain
    assert len(_written(es)) == 6


def test_old_buffer_of_another_bucket_goes_out_too(tmp_path):
    # FS-режим держит буфер на месяц: обход ушёл дальше — неполный буфер
    # прошлого месяца не должен ждать конца прогона
    es = FakeES()
    indexer = _indexer(es, tmp_path, bulk_docs=1000, flush_interval=0.1)
    indexer.add("2026/03", _no_uuid(1))
    time.sleep(0.2)
    indexer.add("2026/04", _no_uuid(2))
    deadline = time.monotonic() + 3
    while not es.bulk_calls and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _written(es) == ["A_2026-06-24_1"]
    indexer.drain()
