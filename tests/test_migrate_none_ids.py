from src.migrate_none_ids import heal_source


def test_heals_none_id_source():
    src = {"id_final": "73RS0013_NONE_5-151", "case_number": "5-151/2026", "judge": "X"}
    new_id, new_src = heal_source(src)
    assert new_id == "73RS0013_2026_5-151"
    assert new_src["id_final"] == "73RS0013_2026_5-151"
    assert new_src["judge"] == "X"


def test_stuck_none_returns_none():
    assert heal_source({"id_final": "73RS0013_NONE_5-151", "case_number": "5-151"}) is None


def test_normal_id_returns_none():
    assert heal_source({"id_final": "73RS0013_2026-05-12_5-151", "case_number": "5-151/2026"}) is None
