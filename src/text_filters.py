"""Filters for broken case texts and invalid date_reg values."""

import re
from datetime import datetime, timezone

from .utils.dates import date_value_to_iso

BROKEN_DOCX = "Вместо текста залит нечитаемый docx"
BROKEN_SERVER = "Вместо текста отображается ошибка сервера"
BROKEN_SQL = "Вместо текста отображается ошибка SQL"

_MIN_DATE = "1990-01-01"
# Sentinel the source/parser writes when a case has no registration date.
_PLACEHOLDER_DATE = "1900-01-01"

# Years appear in case numbers as "/2018"; chained/renumbered cases list many
# ("1-3/2024 (...; 1-357/2002;)") — registration is the earliest, so take min.
_CASE_YEAR_RE = re.compile(r"/\s*((?:19|20)\d{2})")
# Month-day tail of an ISO-ish date_reg, kept when only the year is mangled.
_MONTH_DAY_RE = re.compile(r"-(\d{2})-(\d{2})")
# A date whose year was mangled to 1-4 digits: 0217-08-28, 1010-06-28, 15-02-23.
_MANGLED_DATE_RE = re.compile(r"^0*(\d{1,4})-(\d{2})-(\d{2})")


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


def _case_number_year(case_number: str | None) -> str | None:
    """Earliest 4-digit year in a case number (registration year), or None."""
    years = _CASE_YEAR_RE.findall(str(case_number or ""))
    return min(years) if years else None


def recover_malformed_date(date_str: str | None, today: str | None = None) -> str | None:
    """Fix a date whose year lost its century: take the last two year digits as
    a 20xx year, keep month/day. "217-08-28" -> "2017-08-28", "15-02-23" ->
    "2015-02-23". Returns None if unparseable or the result is out of range."""
    m = _MANGLED_DATE_RE.match(date_str or "")
    if not m:
        return None
    today = today or datetime.now(timezone.utc).isoformat()
    cand = f"20{m.group(1)[-2:]:0>2}-{m.group(2)}-{m.group(3)}"
    return cand if _is_valid(cand, today) else None


def fix_date_reg(
    date_reg: str | None,
    progress: list,
    date_result: str | None = None,
    case_number: str | None = None,
    today: str | None = None,
) -> str | None:
    """Keep date_reg if sane (_MIN_DATE..today), else recover.

    Fallback chain: earliest sane date from progress -> sane date_result ->
    year from case number with date_reg's month-day (or Jan 1) -> original
    value. A coarse-but-right-year date beats a hole or a mangled year.
    """
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
    if candidates:
        return min(candidates)
    if _is_valid(date_result, today):
        return date_result
    # Mangled year (e.g. 0217-08-28) — but not the no-info placeholder, whose
    # "1900" would wrongly become 2000; that one falls through to case number.
    if date_reg and date_reg[:10] != _PLACEHOLDER_DATE:
        rec = recover_malformed_date(date_reg, today)
        if rec:
            return rec
    year = _case_number_year(case_number)
    if year and year >= _MIN_DATE[:4]:
        md = _MONTH_DAY_RE.search(date_reg or "")
        rebuilt = f"{year}-{md.group(1)}-{md.group(2)}" if md else f"{year}-01-01"
        if _is_valid(rebuilt, today):
            return rebuilt
        return f"{year}-01-01"
    return date_reg
