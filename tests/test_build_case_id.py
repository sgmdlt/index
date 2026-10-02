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


def test_cassation_card_keys_on_the_complaint_number():
    # "<complaint> [<proceeding>]": a card is first scraped as "8Г-7981/2026"
    # and gets the proceeding number later, so the complaint number is the
    # one that stays put
    early = build_case_id("8Г-7981/2026", "27.07.2026", "26KJ0005")
    late = build_case_id("8Г-7981/2026 [88-8481/2026]", "27.07.2026", "26KJ0005")
    assert early == late == "26KJ0005_2026-07-27_8Г-7981"


def test_complaints_of_one_proceeding_stay_apart():
    a = build_case_id("4У-92/2013 [44У-44/2013]", "12.08.2013", "06OS0000")
    b = build_case_id("4У-93/2013 [44У-44/2013]", "12.08.2013", "06OS0000")
    assert (a, b) == ("06OS0000_2013-08-12_4У-92", "06OS0000_2013-08-12_4У-93")


def test_bracket_inside_the_number_is_not_a_second_number():
    # Orenburg courts put brackets inside the number itself: "1[2]-50/2022"
    got = build_case_id("1[2]-50/2022", "30.05.2022", "56RS0038")
    assert got == "56RS0038_2022-05-30_1[2]-50"


def test_placeholder_complaint_keys_on_the_proceeding_and_its_year():
    # "4У-0" is a placeholder complaint number and 01.01.1900 a placeholder
    # date — every such case of a court would share "1900-01-01_4У-0"
    got = build_case_id("4У-0/2011 [44У-113/2011]", "01.01.1900", "33OS0000")
    assert got == "33OS0000_2011-01-01_44У-113"


def test_unknown_date_cases_of_different_years_stay_apart():
    a = build_case_id("4У-0/2011 [44У-113/2011]", "01.01.1900", "33OS0000")
    b = build_case_id("4У-0/2012 [44У-113/2012]", "01.01.1900", "33OS0000")
    assert a != b


def test_unknown_date_with_a_real_complaint_keeps_the_placeholder_date():
    # the early scrape has no brackets to take a year from, so the late one
    # must not invent a date either, or the card splits in two
    early = build_case_id("4У-140/2011", "01.01.1900", "33OS0000")
    late = build_case_id("4У-140/2011 [44У-7/2011]", "01.01.1900", "33OS0000")
    assert early == late == "33OS0000_1900-01-01_4У-140"


def test_bracket_inside_the_number_survives_round_brackets():
    # Orenburg "М[1]-1" inside "(...)": the capture used to stop at "[" and
    # cut the key down to "1]-1"
    got = build_case_id("1[1]-38/2013 (М[1]-1/2013)", "09.01.2013", "56RS0038")
    assert got == "56RS0038_2013-01-09_М[1]-1"
