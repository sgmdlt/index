from scripts.dedup_bracket_ids import covered, fixed_id, fold


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
