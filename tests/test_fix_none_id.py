from src.to_document import fix_none_id


def test_none_id_healed_from_case_number_year():
    assert fix_none_id("73RS0013_NONE_5-151", "5-151/2026") == "73RS0013_2026_5-151"


def test_normal_id_untouched():
    assert fix_none_id("73RS0013_2026-05-12_5-151", "5-151/2026") == "73RS0013_2026-05-12_5-151"


def test_none_id_without_year_in_number_kept():
    assert fix_none_id("73RS0013_NONE_5-151", "5-151") == "73RS0013_NONE_5-151"


def test_missing_id_passthrough():
    assert fix_none_id(None, "5-151/2026") is None
