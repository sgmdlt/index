"""Документы одного uuid — одно дело. Суд правит дату регистрации задним
числом, карточка теряет первый номер, поэтому формула от одной записи даёт
разные id; побеждает самая ранняя дата, и выбор не зависит от порядка записей —
пересборка индекса даёт те же id."""

from src.uuid_group import in_group_scope, pick_group_id


def test_earliest_date_wins():
    assert (
        pick_group_id(
            [
                ("50RS0030_2026-05-20_5-225", "2026-06-25"),
                ("50RS0030_2026-05-19_5-225", "2026-09-28"),
            ]
        )
        == "50RS0030_2026-05-19_5-225"
    )


def test_same_date_freshest_record_wins():
    # свежая запись несёт самый полный номер («2-98/2026 (2-1898/2025;) ~ М-1591/2025»)
    assert (
        pick_group_id(
            [
                ("42RS0008_2025-08-21_2-1898", "2024-01-10"),
                ("42RS0008_2025-08-21_М-1591", "2026-10-01"),
            ]
        )
        == "42RS0008_2025-08-21_М-1591"
    )


def test_order_does_not_matter():
    m = [
        ("A_2026-06-24_1", "2026-06-25"),
        ("A_2026-06-29_1", "2026-10-01"),
        ("A_2026-06-24_2", "2026-06-25"),
    ]
    assert pick_group_id(m) == pick_group_id(list(reversed(m))) == "A_2026-06-24_1"


def test_scope_excludes_mgs_and_no_uuid():
    assert in_group_scope({"uuid": "u", "link": "https://x.sudrf.ru/?case_uid=u"})
    assert not in_group_scope({"uuid": "u", "link": "https://mos-gorsud.ru/x"})
    assert not in_group_scope({"link": "https://x.sudrf.ru/?_uid="})
