from src.parse_html import fill_raw_tables
from pathlib import Path
import pytest


def _get_fixture_path(filename):
    fixture_path = Path(__file__).parent / "parse_test" / filename
    return fixture_path

def test_html_parse():
    with open(_get_fixture_path("html_parse_fixture.html"), "r") as f:
        html = f.read()
        filled = fill_raw_tables(html)
        raw_tables = filled["raw_tables"]
        participants = []
        for d in raw_tables["СТОРОНЫ"]:
            participants.extend(d.values())

        assert "Прокурор Прокурорович" in participants
        assert "Адвокат Адвокатов" in participants

        assert raw_tables["ДЕЛО"]["Дата поступления"] == "01.08.2025"
        assert raw_tables["ДЕЛО"]["Результат рассмотрения"] == "Вынесен ПРИГОВОР"

        print("Result = ", raw_tables["СУДЕБНЫЕ АКТЫ"][0].get("Судебный акт #1 (Приговор)"))
        assert raw_tables["СУДЕБНЫЕ АКТЫ"][0].get("Судебный акт #1 (Приговор)") == """Уголовное дело № 1 – 67/2025
УИД 02RS0005-01-2025-000469-73
ПРИГОВОР
ИМЕНЕМ РОССИЙСКОЙ ФЕДЕРАЦИИ
с. Турочак 02 октября 2025 года
Турочакский районный суд Республики Алтай в составе председательствующего судьи – Судьяновой Судьи"""
