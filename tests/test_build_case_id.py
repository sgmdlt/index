from src.utils.case_id import build_case_id


def test_simple_id_matches_courts_format():
    # real manifest: case_number "1-76/2016", date_reg "30.03.2016"
    assert build_case_id("1-76/2016", "30.03.2016", "61RS0018") == "61RS0018_2016-03-30_1-76"


def test_missing_date_reg_returns_none():
    assert build_case_id("1-76/2016", "", "61RS0018") is None


def test_tilde_prefixed_case_number():
    # "~" separates a prefix; the formula keeps the part after it
    got = build_case_id("12 ~ 2-345/2016", "30.03.2016", "61RS0018")
    assert got == "61RS0018_2016-03-30_2-345"


def test_unparseable_case_number_marked_not_parsed():
    # "(" forces the else-branch; no "/20" match -> exception -> sentinel
    got = build_case_id("(abc)", "30.03.2016", "61RS0018")
    assert got == "61RS0018_2016-03-30_ID_NOT_PARSED"


def test_cassation_number_in_square_brackets():
    # cassation courts write "<lower-court number> [<cassation number>]";
    # the id keys on the cassation one, same as the "(...)" form
    got = build_case_id("8а-15348/2025 [88а-14799/2025]", "22.05.2025", "77KJ0002")
    assert got == "77KJ0002_2025-05-22_88а-14799"


def test_bracket_inside_the_number_is_not_a_second_number():
    # Orenburg courts put brackets inside the number itself: "1[2]-50/2022"
    got = build_case_id("1[2]-50/2022", "30.05.2022", "56RS0038")
    assert got == "56RS0038_2022-05-30_1[2]-50"
