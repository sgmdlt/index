from src.fix_bad_date_reg import id_date_segment, recover_from_id

TODAY = "2026-06-15T00:00:00Z"


def test_id_date_segment_extracts_middle():
    assert id_date_segment("63OS0000_217-08-28_22-13-217") == "217-08-28"
    assert id_date_segment("68RS0013_1990-06-07_М-854") == "1990-06-07"


def test_id_date_segment_bad_shape():
    assert id_date_segment("nounderscores") is None
    assert id_date_segment("a_b_c_d") is None


def test_recover_from_id_last2():
    assert recover_from_id("63OS0000_217-08-28_22-13-217", TODAY) == "2017-08-28"
    assert recover_from_id("63OS0000_15-02-23_22-11-15", TODAY) == "2015-02-23"


def test_recover_from_id_no_date():
    assert recover_from_id("03MS0154_NONE_", TODAY) is None
