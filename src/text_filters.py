"""Filters for broken case texts and invalid date_reg values."""

from datetime import datetime, timezone

from .utils.dates import date_value_to_iso

BROKEN_DOCX = "Вместо текста залит нечитаемый docx"
BROKEN_SERVER = "Вместо текста отображается ошибка сервера"
BROKEN_SQL = "Вместо текста отображается ошибка SQL"

_MIN_DATE = "2000-01-01"


def broken_text_message(text: str) -> str | None:
    """Return a replacement message if the text is broken, else None."""
    if text.startswith("PK") and "docProps/" in text:
        return BROKEN_DOCX
    if "QUERY:" in text or "server closed the connection" in text:
        return BROKEN_SQL
    if "judicial_web" in text and "судья" not in text.lower():
        return BROKEN_SERVER
    return None


def _is_valid(iso: str | None, today: str) -> bool:
    return bool(iso) and _MIN_DATE <= iso[:10] <= today[:10]


def fix_date_reg(date_reg: str | None, progress: list, today: str | None = None) -> str | None:
    """Keep date_reg if sane (2000..today), else earliest sane date from progress."""
    today = today or datetime.now(timezone.utc).isoformat()
    if _is_valid(date_reg, today):
        return date_reg
    candidates = [
        iso
        for row in progress
        if isinstance(row, dict)
        for iso in [date_value_to_iso(row.get("date"))]
        if _is_valid(iso, today)
    ]
    return min(candidates) if candidates else None
