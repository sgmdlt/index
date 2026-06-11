from src.dedup_whitespace import build_broken_query, build_query, dedup_documents, fix_documents


def test_fix_documents_replaces_broken_and_dedups():
    docs = [
        {"name": "Акт #1", "text": "PK\x03\x04 docProps/core.xml junk", "filename": "a.html"},
        {"name": "Акт #2", "text": "ПРИГОВОР  Судья Петров", "filename": "a.html"},
        {"name": "Акт #2", "text": "ПРИГОВОР Судья Петров", "filename": "b.html"},
    ]
    result = fix_documents(docs)
    assert len(result) == 2
    assert result[0]["text"] == "Вместо текста залит нечитаемый docx"
    assert result[0]["is_broken"] is True
    assert result[1]["text"] == "ПРИГОВОР  Судья Петров"


def test_fix_documents_clean_unchanged():
    docs = [{"name": "n", "text": "Судья Петров", "filename": "f"}]
    assert fix_documents(docs) == docs


def test_build_broken_query_is_nested_match():
    q = build_broken_query()
    nested = q["bool"]["filter"][0]["nested"]
    assert nested["path"] == "documents"
    assert "should" in nested["query"]["bool"]


def test_collapses_whitespace_variants_keeping_longest():
    docs = [
        {"name": "Судебный акт #1 (Приговор)", "text": "ПРИГОВОР Именем Российской Федерации", "filename": "a.html"},
        {"name": "Судебный акт #1 (Приговор)", "text": "ПРИГОВОР\nИменем Российской Федерации\n", "filename": "b.html"},
        {"name": "Постановление", "text": "Постановление суда", "filename": "a.html"},
    ]
    result = dedup_documents(docs)
    assert len(result) == 2
    assert result[0]["text"] == "ПРИГОВОР\nИменем Российской Федерации\n"
    assert result[1]["text"] == "Постановление суда"


def test_build_query_date_range_is_nested():
    q = build_query(None, "2026-05-16", "2026-05-20")
    assert q == {
        "bool": {
            "filter": [
                {"range": {"doc_count": {"gte": 2}}},
                {
                    "nested": {
                        "path": "documents",
                        "query": {
                            "range": {
                                "documents.date": {"gte": "2026-05-16", "lte": "2026-05-20"}
                            }
                        },
                    }
                },
            ]
        }
    }


def test_build_query_ids_wins():
    assert build_query(["x"], "2026-05-16", None) == {"ids": {"values": ["x"]}}


def test_no_duplicates_unchanged():
    docs = [{"name": "n", "text": "a b", "filename": "f"}]
    assert dedup_documents(docs) == docs
