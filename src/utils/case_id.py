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
    id_date = datetime.strptime(date_reg, "%d.%m.%Y").strftime("%Y-%m-%d")
    case_id = court_id + "_" + id_date + "_"
    try:
        normalized = case_number
        if any(char in normalized for char in ["~", "∼"]):
            normalized = re.split(r"[~∼]", normalized, maxsplit=1)[1].strip()

        if all(char not in normalized for char in ["(", " [", "~", "∼"]):
            case_id += normalized[: normalized.rfind("/")].replace("/", "-")
        elif date_reg[-4:] in normalized:
            case_id += re.findall(
                r"([^ ~∼\(\[\{]+) ?[/\\\|] ?" + date_reg[-4:],
                normalized,
            )[-1].replace("/", "-")
        else:
            case_id += (
                re.search(r"([^ ~∼\(]+)/20", normalized).group(1).replace("/", "-")
            )
    except Exception:
        case_id += "ID_NOT_PARSED"
    return case_id
