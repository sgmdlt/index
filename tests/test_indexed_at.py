from scripts import rekey_case_ids
from scripts.dedup_bracket_ids import fold
from src.bulk_indexer import BulkIndexer, apply_merge
from tests.test_bulk_uuid_fold import FakeES, _doc, _index_ops, _indexer, _item

NOW = "2026-10-26T12:00:00Z"


def test_new_doc_gets_created_and_indexed_at():
    [doc] = apply_merge([{"_id": "a", "doc": {"case_number": "1"}}], {}, NOW)
    assert doc["created_at"] == NOW and doc["indexed_at"] == NOW


def test_existing_doc_keeps_created_at():
    old = {"case_number": "1", "created_at": "2026-01-01T00:00:00Z", "indexed_at": "2026-01-01T00:00:00Z"}
    [doc] = apply_merge([{"_id": "a", "doc": {"case_number": "2"}}], {"a": old}, NOW)
    assert doc["created_at"] == "2026-01-01T00:00:00Z" and doc["indexed_at"] == NOW


def test_legacy_doc_stays_without_created_at():
    [doc] = apply_merge([{"_id": "a", "doc": {"case_number": "2"}}], {"a": {"case_number": "1"}}, NOW)
    assert "created_at" not in doc and doc["indexed_at"] == NOW


def test_two_files_of_one_new_case_in_one_batch():
    batch = [{"_id": "a", "doc": {"case_number": "1"}}, {"_id": "a", "doc": {"judge": ["J"]}}]
    [doc] = apply_merge(batch, {}, NOW)
    assert doc["created_at"] == NOW


# --- слияние близнецов одного uuid: created_at дела, а не свежего скачивания ---

A, B = "2026-01-01T00:00:00Z", "2026-05-01T00:00:00Z"
TARGET, TWIN = "50RS0030_2026-05-19_5-225", "50RS0030_2026-05-20_5-225"


def _written(store, tmp_path, incoming=TWIN):
    es = FakeES(store)
    _indexer(es, tmp_path)._do_bulk([_item(incoming, "new.html", "2026-09-28")], "k")
    [(doc_id, source)] = _index_ops(es.bulk_calls[0])
    assert doc_id == TARGET
    return source


def test_regrouped_target_with_a_legacy_twin_has_no_created_at(tmp_path):
    store = {TARGET: {**_doc(TARGET, "a.html", "2026-06-25"), "created_at": A},
             TWIN: _doc(TWIN, "b.html", "2026-06-26")}
    source = _written(store, tmp_path)
    assert "created_at" not in source and source["indexed_at"]


def test_regrouped_twins_keep_the_oldest_created_at(tmp_path):
    # свежее скачивание несёт поздний created_at — дело всё равно старое
    store = {TARGET: {**_doc(TARGET, "a.html", "2026-06-25"), "created_at": A},
             TWIN: {**_doc(TWIN, "b.html", "2026-06-26"), "created_at": B}}
    assert _written(store, tmp_path)["created_at"] == A


def test_twin_folded_into_an_absent_target_keeps_its_created_at(tmp_path):
    store = {TWIN: {**_doc(TWIN, "b.html", "2026-06-26"), "created_at": B}}
    assert _written(store, tmp_path, incoming=TARGET)["created_at"] == B


def test_brand_new_case_gets_created_at_now(tmp_path):
    source = _written({}, tmp_path, incoming=TARGET)
    assert source["created_at"] == source["indexed_at"]


def test_rekey_fold_keeps_the_oldest_created_at():
    old = {"version_ts": "2026-06-25", "created_at": A}
    new = {"version_ts": "2026-09-28", "created_at": B}
    assert rekey_case_ids.merged_doc(new, old, "X")["created_at"] == A


def test_rekey_fold_with_a_legacy_doc_has_no_created_at():
    old = {"version_ts": "2026-06-25"}
    new = {"version_ts": "2026-09-28", "created_at": B}
    assert "created_at" not in rekey_case_ids.merged_doc(new, old, "X")


def test_dedup_fold_keeps_the_oldest_created_at():
    assert fold({"created_at": A}, {"created_at": B})["created_at"] == A


def test_rekey_move_carries_created_and_indexed_at(monkeypatch, tmp_path):
    src = {"uuid": "u", "created_at": A, "indexed_at": B}
    writes = []
    monkeypatch.setattr(rekey_case_ids, "sources", lambda key, ids: {
        "old": {"_source": src, "_seq_no": 1, "_primary_term": 1}})

    def bulk(key, ops):
        writes.extend(ops)
        return [{next(iter(op)): {"status": 201 if "create" in op else 200}} for op in ops[::2]] \
            if "create" in ops[0] else [{"delete": {"status": 200}} for _ in ops]

    monkeypatch.setattr(rekey_case_ids, "bulk", bulk)
    ops = tmp_path / "ops.jsonl"
    ops.write_text('{"op": "move", "from": "old", "to": "new"}\n')
    rekey_case_ids.apply("key", str(ops))
    assert writes[:2] == [{"create": {"_id": "new"}}, {**src, "id_final": "new"}]


def test_full_reindex_leaves_created_at_unset(tmp_path):
    # --warc в пустой индекс: все дела старые, created_at=now разослал бы их подписчикам
    es = FakeES()
    indexer = BulkIndexer(es_client=es, index="t", bulk_docs=1000, flush_interval=3600,
                          failed_log=str(tmp_path / "failed.log"), stamp_created_at=False)
    indexer._do_bulk([_item(TARGET, "a.html", "2026-09-28")], "k")
    [(_, source)] = _index_ops(es.bulk_calls[0])
    assert "created_at" not in source and source["indexed_at"]
