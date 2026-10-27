"""Документы одного uuid сливаются перед записью: суд правит дату, карточка
теряет первый номер, и одна карточка иначе ложится под два _id
(«50RS0030_2026-05-20_5-225» и «…_2026-05-19_5-225»). Близнец удаляется только
после того, как ES принял целевой документ — иначе дело теряется."""
from src.bulk_indexer import BulkIndexer

LINK = "https://x.sudrf.ru/modules.php?name_op=case&case_uid=u"


class FakeES:
    """search отдаёт id (near-real-time, может быть устаревшим), mget — свежий
    документ с версией; bulk отвечает по сценарию и запоминает операции."""

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
        assert size <= 10000 and len(query["terms"]["uuid"]) <= 1000  # max_result_window
        uuids = set(query["terms"]["uuid"])
        return {"hits": {"hits": [{"_id": i} for i, s in self.store.items() if s.get("uuid") in uuids]}}

    def mget(self, index=None, ids=None, **kw):
        return {"docs": [{"_id": i, "found": i in self.store, "_source": self.store.get(i),
                          "_seq_no": 7, "_primary_term": 1} for i in ids]}

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


def _delete_ops(ops):
    return [op["delete"] for op in ops if "delete" in op]


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


def test_many_uuids_are_searched_in_chunks(tmp_path):
    # пачка FS-режима — 1500 дел; больше 10000 хитов ES не отдаст (max_result_window)
    es = FakeES()
    items = [_item(f"A_2026-01-01_{n}", "a.html", "2026-01-01", uuid=f"u{n}") for n in range(1500)]
    _indexer(es, tmp_path)._do_bulk(items, "k")
    assert es.search_calls == 2
    assert len(_index_ops(es.bulk_calls[0])) == 1500


def test_twin_is_deleted_only_at_the_version_that_was_read(tmp_path):
    # если близнеца обновили после чтения, удаление получит 409, а не сотрёт запись
    es = FakeES({"50RS0030_2026-05-20_5-225": _doc("50RS0030_2026-05-20_5-225", "old.html", "2026-06-25")})
    _indexer(es, tmp_path)._do_bulk([_item("50RS0030_2026-05-19_5-225", "new.html", "2026-09-28")], "k")
    assert _delete_ops(es.bulk_calls[1]) == [
        {"_index": "t", "_id": "50RS0030_2026-05-20_5-225", "if_seq_no": 7, "if_primary_term": 1}]


def test_callers_learn_the_verdict_under_their_own_ids(tmp_path):
    # outbox ждёт ответа по id, который сам отправил; переписанный id его теряет
    seen = []
    es = FakeES({"50RS0030_2026-05-19_5-225": _doc("50RS0030_2026-05-19_5-225", "old.html", "2026-06-25")})
    _indexer(es, tmp_path, seen)._do_bulk([_item("50RS0030_2026-05-20_5-225", "new.html", "2026-09-28")], "k")
    assert seen == [(["50RS0030_2026-05-20_5-225"], [], "bulk item rejected")]


def test_fresher_target_fields_survive_an_older_twin(tmp_path):
    target = {**_doc("50RS0030_2026-05-19_5-225", "b.html", "2026-09-28"), "result": "свежее"}
    twin = {**_doc("50RS0030_2026-05-20_5-225", "a.html", "2026-06-25"), "result": "старое"}
    es = FakeES({target["id_final"]: target, twin["id_final"]: twin})
    _indexer(es, tmp_path)._do_bulk([_item("50RS0030_2026-05-20_5-225", "c.html", "2026-06-26")], "k")
    [(doc_id, source)] = _index_ops(es.bulk_calls[0])
    assert doc_id == "50RS0030_2026-05-19_5-225"
    assert source["result"] == "свежее"


def test_twin_on_a_legacy_id_ranks_by_its_formula_id(tmp_path):
    # старый id «_2-1» свежее входящего, но формула дела даёт «_2-2» — туда и ляжет
    legacy = {**_doc("78OS0000_2008-12-09_2-1", "old.html", "2026-10-01"),
              "case_number": "2-1/2011 (2-2/2010;)", "date_reg": "2008-12-09", "court_code": "78OS0000"}
    es = FakeES({legacy["id_final"]: legacy})
    _indexer(es, tmp_path)._do_bulk([_item("78OS0000_2008-12-09_2-2", "new.html", "2026-06-19")], "k")
    assert [i for i, _ in _index_ops(es.bulk_calls[0])] == ["78OS0000_2008-12-09_2-2"]
    assert _delete_ids(es.bulk_calls[1]) == ["78OS0000_2008-12-09_2-1"]
