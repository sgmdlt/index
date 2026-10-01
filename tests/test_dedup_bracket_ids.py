from scripts.dedup_bracket_ids import covered, fixed_id, fold, fold_ids, move_ids, moved


def test_fixed_id_rekeys_bracketed_cassation_number():
    got = fixed_id("77KJ0002_2025-05-22_8а-15348-2025 [88а-14799", "8а-15348/2025 [88а-14799/2025]")
    assert got == "77KJ0002_2025-05-22_88а-14799"


def test_twin_must_hold_every_version_of_the_bad_doc():
    assert covered(["a.html"], ["a.html", "b.html"])
    assert not covered(["a.html", "c.html"], ["a.html"])


def test_fold_keeps_old_versions_and_takes_fresh_fields_from_twin():
    bad = {"id_final": "X_2025-05-22_8а-1-2025 [88а-1", "result": "старое",
           "html_files": ["19-06-2026.html", "21-06-2026.html"], "version_ts": "2026-06-21"}
    twin = {"id_final": "X_2025-05-22_88а-1", "result": "свежее",
            "html_files": ["21-06-2026.html"], "version_ts": "2026-06-21"}
    got = fold(bad, twin)
    assert covered(bad["html_files"], got["html_files"])
    assert got["id_final"] == "X_2025-05-22_88а-1"
    assert got["result"] == "свежее"


def test_fold_takes_every_pair_with_a_twin_covered_or_not():
    # docs from the old JSONL import have no html_files, so "covered" holds
    # vacuously and says nothing about what only the bad doc carries
    rows = [{"_id": "a", "twin": True, "covered": True},
            {"_id": "b", "twin": True, "covered": False},
            {"_id": "c", "twin": False, "covered": False}]
    assert fold_ids(rows) == ["a", "b"]


def test_moved_doc_carries_its_new_id_and_nothing_else_changes():
    bad = {"id_final": "56RS0007_2013-04-17_1]-649", "result": "приговор"}
    got = moved(bad, "56RS0007_2013-04-17_М[1]-649")
    assert got == {"id_final": "56RS0007_2013-04-17_М[1]-649", "result": "приговор"}


def test_move_takes_only_rekeyable_docs_without_a_twin():
    rows = [{"_id": "a", "new_id": "A", "twin": False},
            {"_id": "b", "new_id": "B", "twin": True},
            {"_id": "c", "new_id": "c", "twin": False},
            {"_id": "d", "new_id": "X_ID_NOT_PARSED", "twin": False}]
    assert move_ids(rows) == ["a"]
