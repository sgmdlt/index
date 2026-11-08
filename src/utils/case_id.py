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


def build_case_id(case_number: str, date_reg: str, court_id: str) -> str | None:
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
    try:
        normalized = case_number
        if any(char in normalized for char in ["~", "∼"]):
            normalized = re.split(r"[~∼]", normalized, maxsplit=1)[1].strip()

        if " [" in normalized:
            # "<complaint> [<proceeding>]": a card is scraped as "8Г-7981/2026"
            # first and gets its proceeding later, so the complaint number is
            # what stays put; complaints of one proceeding stay apart
            head, proceeding = normalized.split(" [", 1)
            number = proceeding if placeholder else head
            case_id += number[: number.rfind("/")].replace("/", "-")
        elif all(char not in normalized for char in ["(", "~", "∼"]):
            case_id += normalized[: normalized.rfind("/")].replace("/", "-")
        elif date_reg[-4:] in normalized:
            case_id += re.findall(
                r"([^ ~∼\(\{]+) ?[/\\\|] ?" + date_reg[-4:],
                normalized,
            )[-1].replace("/", "-")
        else:
            case_id += (
                re.search(r"([^ ~∼\(]+)/20", normalized).group(1).replace("/", "-")
            )
    except Exception:
        case_id += "ID_NOT_PARSED"
    return case_id
