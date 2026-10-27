"""Canonical case-id formula.

VERBATIM PORT of `build_case_id` from the courts project
(src/courts/parser/search_result_parser.py) — the single source of truth for
the `{court_id}_{YYYY-MM-DD}_{shortcase}` id used as the ES `_id`.

Kept here so the indexer can RECOMPUTE the id from a manifest's component
fields (case_number/date_reg/court_id) instead of trusting a possibly-stale
`meta["ID"]`. Legacy manifests carry wrong ids from an old parse bug; recomputing
with the corrected formula re-keys them and folds old/new collision scrapes onto
one `_id`. If courts changes the formula, re-port it here (consistency risk).
"""

import re
from datetime import datetime


def case_id_from_fields(case_number: str | None, date_reg: str | None,
                        court: str | None, link: str | None) -> str | None:
    """id из полей записи любого источника: манифест даёт дату «дд.мм.гггг»,
    JSONL и ES — «гггг-мм-дд[T…]». МГС ключуется своим правилом
    (parse_html_mgs.card_case_id) — для него None, берётся сохранённый id.
    None и тогда, когда формула не нашла номер или дату не удалось разобрать."""
    if "mos-gorsud" in (link or "") or not (case_number and date_reg and court):
        return None
    iso = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_reg)
    ru = f"{iso.group(3)}.{iso.group(2)}.{iso.group(1)}" if iso else date_reg
    try:
        case_id = build_case_id(case_number, ru, court)
    except ValueError:  # дата не разобралась — пусть решает сохранённый id
        return None
    # «…_ID_NOT_PARSED» склеил бы все такие дела суда за день в один документ
    return None if case_id is None or case_id.endswith("ID_NOT_PARSED") else case_id


# "<number>/<year>" inside a case number string; the number may itself hold
# "/" ("М-2/6/2025") or brackets ("1[2]-50/2022")
_NUMBER_YEAR = re.compile(r"([^\s~∼(){};,]+)\s?[/\\|]\s?((?:19|20)\d{2})(?!\d)")


def build_case_id(case_number: str, date_reg: str, court_id: str) -> str | None:
    # the key is the first number the card was given, so every scrape of one
    # card lands on one id however the number grows later
    if not date_reg:
        return None
    # "4У-0 [44У-113/2011]": the complaint number is a placeholder, so the case
    # keys on the proceeding in brackets — and if the date is the 01.01.1900
    # placeholder too, on that proceeding's year, else every such case of a
    # court shares one id
    placeholder = re.match(r"[^\[]*-0\s*/[^\[]* \[[^\]]*/\s*((?:19|20)\d{2})\s*\]", case_number or "")
    if date_reg == "01.01.1900" and placeholder:
        date_reg = f"01.01.{placeholder.group(1)}"
    id_date = datetime.strptime(date_reg, "%d.%m.%Y").strftime("%Y-%m-%d")
    case_id = court_id + "_" + id_date + "_"

    # "2-408/2026 ~ М-196/2026": the материал М-196 came first; but
    # "2-1043/2009 ~ Материалы дела" has no number after "~"
    text = case_number or ""
    parts = re.split(r"[~∼]", text, maxsplit=1)
    text = parts[1] if len(parts) == 2 and _NUMBER_YEAR.search(parts[1]) else parts[0]

    # "8Г-7981/2026 [88-8481/2026]": a card is scraped as "8Г-7981/2026"
    # first and gets its proceeding later, so the complaint number is what
    # stays put; complaints of one proceeding stay apart
    if " [" in text:
        head, proceeding = text.split(" [", 1)
        text = proceeding if placeholder else head

    pairs = _NUMBER_YEAR.findall(text)
    if not pairs:
        # "2-5/20": a two-digit year — the number is what precedes the last "/"
        single = text.strip()
        if "/" in single and not re.search(r"[\s~∼(){};,]", single):
            return case_id + single[: single.rfind("/")].replace("/", "-")
        return case_id + "ID_NOT_PARSED"
    # "4У-47/2017 - (4У-2186/2016)", registered 2016: the number of the
    # registration year; "2-1/2011 (2-2/2010;)", registered 2008, lost its
    # first number — the earliest year left is the closest to it
    same_year = [number for number, year in pairs if year == date_reg[-4:]]
    number = same_year[-1] if same_year else min(reversed(pairs), key=lambda p: p[1])[0]
    return case_id + number.replace("/", "-")
