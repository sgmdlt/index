"""Refresh перед поиском близнецов — только когда он нужен (courts-u97m).

Поиск видит записанное в этом прогоне только после refresh, а refresh стоил
~4,2 с на пачку (прод 19–20.11: 58% времени прогона). Нужен он лишь перед
пачкой, где есть UID карточки, уже записанный этим прогоном; полная
пересборка (--warc) помнить 142 млн UID не может и делает refresh всегда.
"""

from src.bulk_indexer import BulkIndexer
from tests.test_bulk_uuid_fold import FakeES, _item


def _indexer(es, tmp_path, **kw):
    return BulkIndexer(
        es_client=es,
        index="t",
        bulk_docs=1000,
        flush_interval=3600,
        failed_log=str(tmp_path / "failed.log"),
        **kw,
    )


def test_batch_of_new_cards_skips_the_refresh(tmp_path):
    es = FakeES()
    indexer = _indexer(es, tmp_path)
    indexer.add("k", _item("A_2026-06-24_1", "a.html", "2026-06-25", uuid="u1"))
    indexer.add("k", _item("A_2026-06-24_2", "b.html", "2026-06-25", uuid="u2"))
    indexer.drain()
    assert es.indices.refresh_calls == 0 and es.search_calls == 1


def test_card_written_earlier_in_the_run_refreshes_first(tmp_path):
    es = FakeES()
    indexer = _indexer(es, tmp_path)
    indexer.add("k", _item("A_2026-06-24_1", "a.html", "2026-06-25", uuid="u1"))
    indexer.drain()
    indexer.add("k", _item("A_2026-06-25_1", "b.html", "2026-06-26", uuid="u1"))
    indexer.add("k", _item("A_2026-06-24_9", "c.html", "2026-06-26", uuid="u9"))
    indexer.drain()
    assert es.indices.refresh_calls == 1  # только перед второй пачкой


def test_full_rebuild_refreshes_before_every_batch(tmp_path):
    es = FakeES()
    indexer = _indexer(es, tmp_path, twin_refresh="always")
    indexer.add("k", _item("A_2026-06-24_1", "a.html", "2026-06-25", uuid="u1"))
    indexer.drain()
    indexer.add("k", _item("A_2026-06-24_2", "b.html", "2026-06-25", uuid="u2"))
    indexer.drain()
    assert es.indices.refresh_calls == 2
