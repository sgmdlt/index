from pathlib import Path

from src.parse_html import fill_raw_tables


def _get_fixture_path(filename):
    fixture_path = Path(__file__).parent / "parse_test" / filename
    return fixture_path


def test_html_parse():
    with open(_get_fixture_path("html_parse_fixture.html")) as f:
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

        # The act keeps its layout: trailing spaces stay, edge newlines are
        # dropped later by prepare_doc (strip("\n")), so compare that form.
        act = raw_tables["СУДЕБНЫЕ АКТЫ"][0].get("Судебный акт #1 (Приговор)")
        assert act.strip("\n") == (
            "Уголовное дело № 1 – 67/2025 \n"
            "УИД 02RS0005-01-2025-000469-73\n"
            "ПРИГОВОР\n"
            "ИМЕНЕМ РОССИЙСКОЙ ФЕДЕРАЦИИ\n"
            "с. Турочак 02 октября 2025 года \n"
            "Турочакский районный суд Республики Алтай в составе "
            "председательствующего судьи – Судьяновой Судьи"
        )
