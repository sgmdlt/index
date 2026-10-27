"""Правило группы: документы одного uuid (case_uid карточки sudrf) — одно дело.

Формула id от одной записи для них нестабильна: суд правит дату регистрации
задним числом, карточка теряет первый номер. Поэтому id группы — функция всех
её записей: самая ранняя дата, при равной — формула от самой свежей записи
(она несёт самый полный номер), затем лексикографически меньший id. Выбор не
зависит от порядка прихода записей. МГС ключуется своим правилом и сюда не
входит. Спека: docs/superpowers/specs/2026-10-27-stable-case-id-design.md.
"""


def in_group_scope(doc: dict) -> bool:
    return bool(doc.get("uuid")) and "mos-gorsud" not in (doc.get("link") or "")


def pick_group_id(members: list[tuple[str, str]]) -> str:
    """members — [(_id, version_ts), …]; _id вида court_YYYY-MM-DD_номер."""
    ordered = sorted(members, key=lambda m: m[0])
    # sorted стабилен и с reverse=True: при равном version_ts остаётся порядок по id
    ordered = sorted(ordered, key=lambda m: m[1] or "", reverse=True)
    return min(ordered, key=lambda m: m[0].split("_")[1])[0]
