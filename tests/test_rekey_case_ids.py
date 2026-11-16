from scripts.rekey_case_ids import (
    classify_healed,
    merged_doc,
    plan_groups,
    plan_rekey,
    uuid_groups,
)

ROW = {
    "_id": "78OS0000_2008-12-09_2-1",
    "case_number": "2-1/2011 (2-2/2010;)",
    "date_reg": "2008-12-09",
    "court_code": "78OS0000",
    "uuid": "a5f1",
    "link": "",
}


def test_renumbered_case_folds_into_twin_on_earliest_number():
    twin = {"78OS0000_2008-12-09_2-2": {"uuid": "a5f1"}}
    assert plan_rekey([ROW], twin) == [
        {
            "op": "fold",
            "from": "78OS0000_2008-12-09_2-1",
            "to": "78OS0000_2008-12-09_2-2",
        }
    ]


def test_no_twin_is_a_move():
    assert plan_rekey([ROW], {})[0]["op"] == "move"


def test_twin_with_other_uuid_goes_manual():
    assert (
        plan_rekey([ROW], {"78OS0000_2008-12-09_2-2": {"uuid": "zzz"}})[0]["op"]
        == "manual"
    )


def test_twins_without_uuid_fold():
    row = {**ROW, "uuid": None}
    assert plan_rekey([row], {"78OS0000_2008-12-09_2-2": {}})[0]["op"] == "fold"


def test_formula_id_unchanged_is_skipped():
    assert plan_rekey([{**ROW, "_id": "78OS0000_2008-12-09_2-2"}], {}) == []


def test_mgs_rows_are_skipped():
    assert plan_rekey([{**ROW, "link": "https://mos-gorsud.ru/x"}], {}) == []


def test_unparsed_formula_is_skipped():
    # мусор в номере — не повод переносить дело на «…_ID_NOT_PARSED»
    assert plan_rekey([{**ROW, "case_number": "Материалы дела"}], {}) == []


def test_groups_fold_into_earliest_date():
    rows = [
        {
            "_id": "50RS0030_2026-05-20_5-225",
            "formula": "50RS0030_2026-05-20_5-225",
            "uuid": "u",
            "version_ts": "2026-06-25",
        },
        {
            "_id": "50RS0030_2026-05-19_5-225",
            "formula": "50RS0030_2026-05-19_5-225",
            "uuid": "u",
            "version_ts": "2026-09-28",
        },
    ]
    assert plan_groups(rows) == [
        {
            "op": "fold",
            "from": "50RS0030_2026-05-20_5-225",
            "to": "50RS0030_2026-05-19_5-225",
        }
    ]


def test_single_doc_group_is_left_alone():
    assert (
        plan_groups(
            [
                {
                    "_id": "A_2026-01-01_1",
                    "formula": "A_2026-01-01_1",
                    "uuid": "u",
                    "version_ts": "x",
                }
            ]
        )
        == []
    )


def test_group_target_is_a_formula_id_not_a_legacy_one():
    # «_2-1» — старый id свежего скачивания; формула обоих даёт «_2-2»
    rows = [
        {
            "_id": "78OS0000_2008-12-09_2-1",
            "formula": "78OS0000_2008-12-09_2-2",
            "uuid": "u",
            "version_ts": "2026-10-01",
        },
        {
            "_id": "78OS0000_2008-12-09_2-2",
            "formula": "78OS0000_2008-12-09_2-2",
            "uuid": "u",
            "version_ts": "2026-06-19",
        },
    ]
    assert plan_groups(rows) == [
        {
            "op": "fold",
            "from": "78OS0000_2008-12-09_2-1",
            "to": "78OS0000_2008-12-09_2-2",
        }
    ]


def test_uuid_groups_split_a_sorted_stream():
    lines = [
        "u1\tA_2026-01-01_1\tA_2026-01-01_1\t2026-01-02",
        "u1\tA_2026-01-02_1\tA_2026-01-02_1\t2026-01-03",
        "u2\tB_2026-01-01_1\tB_2026-01-01_1\t",
    ]
    assert [[r["_id"] for r in g] for g in uuid_groups(lines)] == [
        ["A_2026-01-01_1", "A_2026-01-02_1"],
        ["B_2026-01-01_1"],
    ]


def test_merged_doc_keeps_both_downloads_and_takes_the_target_id():
    old = {
        "id_final": "X_1",
        "result": "старое",
        "html_files": ["a.html"],
        "version_ts": "2026-06-25",
    }
    new = {
        "id_final": "X_2",
        "result": "свежее",
        "html_files": ["b.html"],
        "version_ts": "2026-09-28",
    }
    got = merged_doc(new, old, "X_0")
    assert got["id_final"] == "X_0"
    assert got["result"] == "свежее"
    assert got["html_files"] == ["a.html", "b.html"]


# --- courts-1ctj: дела, чья вылеченная date_reg попала в ID дела ---

CARD = {
    "_id": "52RS0007_2026-01-01_1-427",
    "case_number": "1-427/2026",
    "date_reg": "2026-11-16",
    "court_code": "52RS0007",
    "uuid": "5b97419f-6242-418c-8d3a-9e63d295f5a1",
    "link": "https://sovetsky--nnov.sudrf.ru/modules.php?name=sud_delo&case_uid=5b97419f-6242-418c-8d3a-9e63d295f5a1",
    "created_at": "2026-11-15T23:35:14.000Z",
}


def test_case_keyed_on_a_healed_date_goes_to_rekey():
    assert classify_healed(CARD) == "rekey"
    assert plan_rekey([CARD], {}) == [
        {
            "op": "move",
            "from": "52RS0007_2026-01-01_1-427",
            "to": "52RS0007_2026-11-16_1-427",
        }
    ]


def test_case_with_a_healed_body_date_goes_to_date_fix():
    # ночь 15.11: id уже по дате источника, а в теле осталась вылеченная
    row = {**CARD, "_id": "52RS0007_2026-11-16_1-427", "date_reg": "2026-01-01"}
    assert classify_healed(row) == "date"


def test_case_really_registered_on_january_1st_is_left_alone():
    row = {**CARD, "_id": "52RS0007_2026-01-01_1-427", "date_reg": "2026-01-01"}
    assert classify_healed(row) is None


def test_legacy_id_of_another_year_is_left_alone():
    # 1900-01-01 в id — пустая дата старой загрузки, не этот баг (courts-3xc2)
    row = {**CARD, "_id": "70OS0000_1900-01-01_44У-45", "date_reg": "2010-03-17"}
    assert classify_healed(row) is None


def test_mgs_case_is_left_alone():
    assert (
        classify_healed({**CARD, "link": "https://mos-gorsud.ru/rs/x/cases/y"}) is None
    )


def test_case_without_created_at_is_left_alone():
    # created_at появился у дел, записанных уже с правилом группы — баг только у них
    assert classify_healed({k: v for k, v in CARD.items() if k != "created_at"}) is None
