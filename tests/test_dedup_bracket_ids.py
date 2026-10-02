from scripts.dedup_bracket_ids import (
    collision_groups,
    covered,
    fixed_id,
    fold,
    fold_ids,
    move_ids,
    moved,
    rekey_candidates,
    source_id,
    unmerge_groups,
)


def test_fixed_id_rekeys_bracketed_cassation_number():
    got = fixed_id("77KJ0002_2025-05-22_8а-15348-2025 [88а-14799", "8а-15348/2025 [88а-14799/2025]")
    assert got == "77KJ0002_2025-05-22_8а-15348"


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


def test_unmerge_groups_unknown_date_cases_by_the_wrong_id_they_landed_on():
    rows = [{"_id": "33OS0000_1900-01-01_4У-0-2011 [44У-113", "new_id": "33OS0000_1900-01-01_4У-0"},
            {"_id": "33OS0000_1900-01-01_4У-0-2009 [44У-97", "new_id": "33OS0000_1900-01-01_4У-0"},
            {"_id": "77KJ0002_2025-05-22_8а-1-2025 [88а-1", "new_id": "77KJ0002_2025-05-22_88а-1"}]
    got = unmerge_groups(rows)
    assert list(got) == ["33OS0000_1900-01-01_4У-0"]
    assert [r["_id"] for r in got["33OS0000_1900-01-01_4У-0"]] == [rows[0]["_id"], rows[1]["_id"]]


def test_source_id_recomputes_from_the_docs_own_fields():
    src = {"case_number": "4У-140/2011 [44У-7/2011]", "court_code": "33OS0000",
           "date_reg": "1900-01-01T00:00:00Z"}
    assert source_id(src) == "33OS0000_1900-01-01_4У-140"


def test_collision_groups_are_proceedings_shared_by_several_complaints():
    rows = [{"_id": "06OS0000_2013-08-12_4У-92-2013 [44У-44", "new_id": "06OS0000_2013-08-12_44У-44"},
            {"_id": "06OS0000_2013-08-12_4У-93-2013 [44У-44", "new_id": "06OS0000_2013-08-12_44У-44"},
            {"_id": "77KJ0002_2025-05-22_8а-1-2025 [88а-1", "new_id": "77KJ0002_2025-05-22_88а-1"}]
    assert list(collision_groups(rows)) == ["06OS0000_2013-08-12_44У-44"]


def test_rekey_candidates_are_where_the_docs_sit_now_minus_collisions():
    rows = [{"_id": "77KJ0002_2025-05-22_8а-1-2025 [88а-1", "new_id": "77KJ0002_2025-05-22_88а-1"},
            {"_id": "06OS0000_2013-08-12_4У-92-2013 [44У-44", "new_id": "06OS0000_2013-08-12_44У-44"},
            {"_id": "06OS0000_2013-08-12_4У-93-2013 [44У-44", "new_id": "06OS0000_2013-08-12_44У-44"},
            # reindexed at 14:22 under the bracket year and number
            {"_id": "33OS0000_1900-01-01_4У-140-2011 [44У-7", "new_id": "33OS0000_1900-01-01_4У-140"}]
    assert rekey_candidates(rows) == ["77KJ0002_2025-05-22_88а-1", "33OS0000_2011-01-01_44У-7"]
