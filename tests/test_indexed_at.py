from src.bulk_indexer import apply_merge

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
