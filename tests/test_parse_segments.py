"""JSONPath-подобные пути правил: ключ в кавычках может содержать запятую.

Запятая внутри [...] разделяет альтернативные ключи, но только вне кавычек.
Разбор без учёта кавычек делал недостижимыми ключи, которые сами содержат
запятую, — и молча, потому что путь оставался синтаксически валидным:

  'Суд первой инстанции, судья'          -> first_court_name, first_judge
  'Осужденный (оправданный, обвиняемый)' -> defendants, participants
  'Взыскатель, должник'                  -> participants
  'Список статей кодекса (КоАП, ТК ...)' -> articles
  'Вид информации, подлежащей ограничению (гл. 27.1, 27.2 КАС)' -> censorship

На карточках mos-gorsud строка «Осужденный (оправданный, обвиняемый)» есть у
90% дел appeal-criminal и 64% criminal-materials.
"""
import json
from pathlib import Path

from src.prepare_doc import RULES, _parse_segments, prepare_doc


def test_a_quoted_key_keeps_its_comma():
    assert _parse_segments("$.raw_tables['Дело']['Суд первой инстанции, судья']") == [
        ("keys", ["raw_tables"]),
        ("keys", ["Дело"]),
        ("keys", ["Суд первой инстанции, судья"]),
    ]


def test_commas_between_quoted_keys_still_separate_them():
    assert _parse_segments("$.raw_tables['Дело', 'ДЕЛО']") == [
        ("keys", ["raw_tables"]),
        ("keys", ["Дело", "ДЕЛО"]),
    ]


def test_both_kinds_of_comma_in_one_segment():
    path = "$.raw_tables['Дело']['Осужденный (оправданный, обвиняемый)', 'Лицо']"
    assert _parse_segments(path)[-1] == (
        "keys", ["Осужденный (оправданный, обвиняемый)", "Лицо"])


def test_wildcards_and_bare_keys_are_untouched():
    assert _parse_segments("$.raw_tables['Лица'][*][*]") == [
        ("keys", ["raw_tables"]), ("keys", ["Лица"]), ("wildcard",), ("wildcard",)]
    assert _parse_segments("$.id_final") == [("keys", ["id_final"])]


def test_the_convict_row_reaches_defendants_and_participants():
    raw = {"raw_tables": {"Дело": {
        "Осужденный (оправданный, обвиняемый)": "Иванов И.И."}}}
    doc = prepare_doc(raw)
    assert doc["defendants"] == "Иванов И.И."
    assert doc["participants"] == "Иванов И.И."


def test_the_first_instance_row_reaches_court_and_judge():
    raw = {"raw_tables": {"Дело": {
        "Суд первой инстанции, судья": "Черемушкинский районный суд, Ганин А.В."}}}
    doc = prepare_doc(raw)
    assert doc["first_court_name"] == "Черемушкинский районный суд, Ганин А.В."
    assert doc["first_judge"] == "Черемушкинский районный суд, Ганин А.В."


def test_every_rule_path_still_parses():
    """Разбор не должен ронять ни один путь из словаря правил."""
    for field, config in RULES.items():
        for location in config.get("locations", []):
            path = location["key"] if isinstance(location, dict) else location
            assert _parse_segments(path), f"{field}: {path}"
