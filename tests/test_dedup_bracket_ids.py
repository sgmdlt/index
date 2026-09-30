from scripts.dedup_bracket_ids import covered, fixed_id


def test_fixed_id_rekeys_bracketed_cassation_number():
    got = fixed_id("77KJ0002_2025-05-22_8а-15348-2025 [88а-14799", "8а-15348/2025 [88а-14799/2025]")
    assert got == "77KJ0002_2025-05-22_88а-14799"


def test_twin_must_hold_every_version_of_the_bad_doc():
    assert covered(["a.html"], ["a.html", "b.html"])
    assert not covered(["a.html", "c.html"], ["a.html"])
