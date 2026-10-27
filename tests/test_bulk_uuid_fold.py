"""Документы одного uuid сливаются перед записью: суд правит дату, карточка
теряет первый номер, и одна карточка иначе ложится под два _id
(«50RS0030_2026-05-20_5-225» и «…_2026-05-19_5-225»). Близнец удаляется только
после того, как ES принял целевой документ — иначе дело теряется."""
from src.bulk_indexer import BulkIndexer

LINK = "https://x.sudrf.ru/modules.php?name_op=case&case_uid=u"


class FakeES:
    """search/mget отвечают из store; bulk — по сценарию и запоминает операции."""

    def __init__(self, store=None, bulk_error_ids=(), search_error=None):
        self.store = store or {}
        self.bulk_error_ids = set(bulk_error_ids)
        self.search_error = search_error
        self.search_calls = 0
        self.bulk_calls = []

    def search(self, index=None, query=None, size=None, **kw):
        self.search_calls += 1
        if self.search_error:
            raise self.search_error
        uuids = set(query["terms"]["uuid"])
        hits = [{"_id": i, "_source": s} for i, s in self.store.items() if s.get("uuid") in uuids]
        return {"hits": {"hits": hits}}

    def mget(self, index=None, ids=None, **kw):
        return {"docs": [{"_id": i, "found": i in self.store, "_source": self.store.get(i)}
                         for i in ids]}

    def bulk(self, operations=None, refresh=False):
        self.bulk_calls.append(operations)
        items = []
        for op in operations:
            for action in ("index", "delete"):
                if action in op:
                    i = op[action]["_id"]
                    status = {"_id": i}
                    if i in self.bulk_error_ids:
                        status["error"] = {"type": "mapper_parsing_exception"}
                    items.append({action: status})
        return {"items": items}


def _doc(doc_id, html, version_ts, uuid="u", link=LINK):
    return {"id_final": doc_id, "uuid": uuid, "link": link,
            "html_files": [html], "version_ts": version_ts}


def _item(doc_id, html, version_ts, **kw):
    return {"_id": doc_id, "doc": _doc(doc_id, html, version_ts, **kw), "version_ts": version_ts}


def _indexer(es, tmp_path, seen=None):
    return BulkIndexer(es_client=es, index="t", bulk_docs=1000, flush_interval=3600,
                       failed_log=str(tmp_path / "failed.log"),
                       on_result=(lambda ok, bad, why: seen.append((ok, bad, why))) if seen is not None else None)


def _index_ops(ops):
    return [(op["index"]["_id"], ops[n + 1]) for n, op in enumerate(ops) if "index" in op]


def _delete_ids(ops):
    return [op["delete"]["_id"] for op in ops if "delete" in op]


def test_incoming_lands_on_earliest_date_and_twin_is_deleted(tmp_path):
    es = FakeES({"50RS0030_2026-05-20_5-225": _doc("50RS0030_2026-05-20_5-225", "old.html", "2026-06-25")})
    _indexer(es, tmp_path)._do_bulk([_item("50RS0030_2026-05-19_5-225", "new.html", "2026-09-28")], "k")
    [(doc_id, source)] = _index_ops(es.bulk_calls[0])
    assert doc_id == source["id_final"] == "50RS0030_2026-05-19_5-225"
    assert source["html_files"] == ["old.html", "new.html"]
    assert _delete_ids(es.bulk_calls[1]) == ["50RS0030_2026-05-20_5-225"]


def test_incoming_with_later_date_folds_into_existing_earlier_doc(tmp_path):
    es = FakeES({"50RS0030_2026-05-19_5-225": _doc("50RS0030_2026-05-19_5-225", "old.html", "2026-06-25")})
    _indexer(es, tmp_path)._do_bulk([_item("50RS0030_2026-05-20_5-225", "new.html", "2026-09-28")], "k")
    [(doc_id, source)] = _index_ops(es.bulk_calls[0])
    assert doc_id == "50RS0030_2026-05-19_5-225"
    assert source["html_files"] == ["old.html", "new.html"]
    assert len(es.bulk_calls) == 1  # удалять нечего


def test_twin_not_deleted_when_target_write_rejected(tmp_path):
    es = FakeES({"50RS0030_2026-05-20_5-225": _doc("50RS0030_2026-05-20_5-225", "old.html", "2026-06-25")},
                bulk_error_ids={"50RS0030_2026-05-19_5-225"})
    _indexer(es, tmp_path)._do_bulk([_item("50RS0030_2026-05-19_5-225", "new.html", "2026-09-28")], "k")
    assert len(es.bulk_calls) == 1


def test_two_items_same_uuid_in_one_batch_share_an_id(tmp_path):
    es = FakeES()
    _indexer(es, tmp_path)._do_bulk([_item("A_2026-06-29_1", "b.html", "2026-10-01"),
                                     _item("A_2026-06-24_1", "a.html", "2026-06-25")], "k")
    assert [i for i, _ in _index_ops(es.bulk_calls[0])] == ["A_2026-06-24_1"]


def test_search_failure_fails_the_batch(tmp_path):
    seen = []
    es = FakeES(search_error=ConnectionError("es read timed out"))
    _indexer(es, tmp_path, seen)._do_bulk([_item("A_2026-06-24_1", "a.html", "2026-06-25")], "k")
    assert es.bulk_calls == []
    assert seen[0][0] == [] and seen[0][1] == ["A_2026-06-24_1"] and "uuid search failed" in seen[0][2]


def test_mgs_and_no_uuid_items_skip_the_search(tmp_path):
    es = FakeES()
    _indexer(es, tmp_path)._do_bulk([_item("77RS0018_2025-01-14_02-3415", "a.html", "2026-01-01",
                                           link="https://mos-gorsud.ru/x"),
                                     _item("61RS0018_2016-03-30_1-76", "b.html", "2026-01-01", uuid=None)], "k")
    assert es.search_calls == 0
    assert sorted(i for i, _ in _index_ops(es.bulk_calls[0])) == ["61RS0018_2016-03-30_1-76",
                                                                   "77RS0018_2025-01-14_02-3415"]
