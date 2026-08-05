"""Re-extracted acts must be able to heal a flattened text already in ES.

`_doc_canon` ignores whitespace, so an act re-extracted with its indentation
intact lands on the same canon as the flat copy indexed before the extractor
was fixed. Without a rule the merge keeps the existing entry and re-indexing
heals nothing.
"""

from src.bulk_indexer import _merge_documents

FLAT = "ПОСТАНОВЛЕНИЕ г. Москва 30 июля 2026 г. Судья Дашковская Н.В."
LAID_OUT = "      ПОСТАНОВЛЕНИЕ\n\n      г. Москва          30 июля 2026 г.\n\n      Судья Дашковская Н.В."


def test_laid_out_text_replaces_the_flattened_one():
    existing = [{"name": "Судебный акт #1", "text": FLAT, "filename": "a.html"}]
    incoming = [{"name": "Судебный акт #1", "text": LAID_OUT, "filename": "a.html"}]
    result = _merge_documents(existing, incoming)
    assert len(result) == 1
    assert result[0]["text"] == LAID_OUT


def test_flattened_text_never_replaces_a_laid_out_one():
    existing = [{"name": "Судебный акт #1", "text": LAID_OUT, "filename": "a.html"}]
    incoming = [{"name": "Судебный акт #1", "text": FLAT, "filename": "a.html"}]
    assert _merge_documents(existing, incoming)[0]["text"] == LAID_OUT


def test_named_copy_still_wins_over_placeholder():
    existing = [{"name": "", "text": FLAT, "filename": "a.html"}]
    incoming = [{"name": "Судебный акт #1", "text": FLAT, "filename": "b.html"}]
    result = _merge_documents(existing, incoming)
    assert result[0]["name"] == "Судебный акт #1"


def test_name_and_layout_both_taken_from_the_better_copy():
    existing = [{"name": "", "text": FLAT, "filename": "a.html"}]
    incoming = [{"name": "Судебный акт #1", "text": LAID_OUT, "filename": "b.html"}]
    result = _merge_documents(existing, incoming)
    assert result[0]["name"] == "Судебный акт #1"
    assert result[0]["text"] == LAID_OUT


def test_restored_indent_wins_at_equal_line_count():
    """The first-line indent alone is still layout the flat copy had lost."""
    lost_indent = "ПРИГОВОР\n\n      Именем Российской Федерации"
    full = "      ПРИГОВОР\n\n      Именем Российской Федерации"
    existing = [{"name": "Акт", "text": lost_indent, "filename": "a.html"}]
    incoming = [{"name": "Акт", "text": full, "filename": "b.html"}]
    assert _merge_documents(existing, incoming)[0]["text"] == full


def test_extra_spaces_alone_do_not_win():
    """Longer is not the test — a stray double space is noise, not layout."""
    existing = [{"name": "Акт", "text": "ТЕКСТ АКТА", "filename": "a.html"}]
    incoming = [{"name": "Акт", "text": "ТЕКСТ  АКТА", "filename": "b.html"}]
    assert _merge_documents(existing, incoming)[0]["text"] == "ТЕКСТ АКТА"


def test_a_genuinely_different_act_is_still_appended():
    existing = [{"name": "Акт #1", "text": FLAT, "filename": "a.html"}]
    incoming = [{"name": "Акт #2", "text": "СОВСЕМ ДРУГОЙ ТЕКСТ", "filename": "b.html"}]
    assert len(_merge_documents(existing, incoming)) == 2
