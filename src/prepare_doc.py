"""Field extraction and transformation using elastic_index_dict.json rules.

Ports prepare_doc.js. Safe to import with no side effects.
"""

import json
import re
from pathlib import Path
from datetime import datetime

_HERE = Path(__file__).parent  # project root (index/)

with open(_HERE / "elastic_index_dict.json", encoding="utf-8") as _f:
    RULES: dict = json.load(_f)

with open(_HERE / "elastic_type_instance_dict.json", encoding="utf-8") as _f:
    TYPE_INSTANCE_RULES: dict = json.load(_f)

_PROGRESS_DATE_KEYS = ("Дата", "Дата события", "Дата и время")
_PROGRESS_EVENT_KEYS = (
    "Наименование события", "Тип события", "Вид события", "Событие",
    "Состояние", "Стадия",
)
# Moscow (mos-gorsud/mos-sud) cards nest movement as {section: [rows]} inside
# `Движение дела`/`Судебные заседания`. Expand only these sections into progress
# rows; other wrappers (e.g. «История местонахождения») are skipped.
_PROGRESS_SECTIONS = ("История состояний", "Судебные заседания и беседы")

_ONHOLD_PATTERNS = {
    "date_onhold_defendant_wanted": re.compile(r"скрылся", re.IGNORECASE),
    "date_onhold_defendant_located": re.compile(
        r"Место\s*нахождени[ея]?\s+обвиняемого\s+известно", re.IGNORECASE
    ),
    "date_onhold_defendant_military": re.compile(r"военн", re.IGNORECASE),
}


# \s* first, so a quoted key after ", " is still seen as quoted rather than
# swallowed whole by the bare branch
_KEY = re.compile(r"\s*(?:'([^']*)'|\"([^\"]*)\"|([^,]+))")


def _split_keys(content: str) -> list:
    """Alternative keys inside [...] — commas separate them, but only outside
    quotes. Splitting on every comma made any key that contains one
    unreachable, and silently, because the path stayed syntactically valid:
    'Суд первой инстанции, судья' became two keys that match nothing.
    """
    keys = []
    for single, double, bare in _KEY.findall(content):
        key = single or double or bare.strip()
        if key:
            keys.append(key)
    return keys


def _parse_segments(path: str) -> list:
    """Parse a JSONPath expression into a list of segments.

    Each segment is either:
      ('keys', ['a', 'b', ...])   – try each key in order
      ('wildcard',)               – iterate all items
    """
    path = path.lstrip("$")
    segments = []
    i = 0
    n = len(path)

    while i < n:
        ch = path[i]
        if ch == ".":
            i += 1
        elif ch == "[":
            j = path.index("]", i)
            content = path[i + 1 : j]
            i = j + 1
            if content == "*":
                segments.append(("wildcard",))
            else:
                segments.append(("keys", _split_keys(content)))
        else:
            j = i
            while j < n and path[j] not in ".[":
                j += 1
            key = path[i:j]
            if key:
                segments.append(("keys", [key]))
            i = j

    return segments


def _resolve(data, segments: list) -> list:
    if not segments:
        return [data] if data is not None else []

    seg = segments[0]
    rest = segments[1:]
    results = []

    if seg[0] == "wildcard":
        if isinstance(data, list):
            for item in data:
                results.extend(_resolve(item, rest))
        elif isinstance(data, dict):
            for v in data.values():
                results.extend(_resolve(v, rest))
    elif seg[0] == "keys":
        if isinstance(data, list):
            for key in seg[1]:
                try:
                    idx = int(key)
                    results.extend(_resolve(data[idx], rest))
                    break
                except (ValueError, IndexError):
                    pass
        elif isinstance(data, dict):
            for key in seg[1]:
                if key in data:
                    results.extend(_resolve(data[key], rest))

    return results


def _resolve_named(data, segments: list, name: str = "") -> list[tuple[str, object]]:
    """Like _resolve but returns (matched_key, value) pairs.

    *name* tracks the most specific tab/document key seen on the way down.
    Explicit keys set the name only once (first-wins), so schema keys like
    "Документы" establish a tab name without being clobbered by field-level
    lookups. Wildcard iteration over a dict overrides the name with the
    current key — this is how nested document tabs (e.g. "Судебный акт #1
    (Приговор)") surface instead of their outer category ("СУДЕБНЫЕ АКТЫ").
    """
    if not segments:
        return [(name, data)] if data is not None else []

    seg = segments[0]
    rest = segments[1:]
    results: list[tuple[str, object]] = []

    if seg[0] == "wildcard":
        if isinstance(data, list):
            for item in data:
                results.extend(_resolve_named(item, rest, name))
        elif isinstance(data, dict):
            for k, v in data.items():
                results.extend(_resolve_named(v, rest, k))
    elif seg[0] == "keys":
        if isinstance(data, list):
            for key in seg[1]:
                try:
                    idx = int(key)
                    results.extend(_resolve_named(data[idx], rest, name))
                    break
                except (ValueError, IndexError):
                    pass
        elif isinstance(data, dict):
            for key in seg[1]:
                if key in data:
                    results.extend(_resolve_named(data[key], rest, name or key))

    return results


def get_value_by_path(data: dict, path: str) -> list:
    segments = _parse_segments(path)
    return _resolve(data, segments)


def get_named_values_by_path(data: dict, path: str) -> list[tuple[str, object]]:
    """Resolve a JSONPath and return (tab_name, value) pairs."""
    segments = _parse_segments(path)
    # Skip the first segment (usually "raw_tables") so that *name*
    # captures the actual tab key, not the container key.
    if segments and segments[0] == ("keys", ["raw_tables"]):
        inner = data.get("raw_tables")
        if inner is None:
            return []
        return _resolve_named(inner, segments[1:])
    return _resolve_named(data, segments)


# ---------------------------------------------------------------------------
# Type handlers
# ---------------------------------------------------------------------------


def _is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, list):
        if not value:
            return True
        return all(
            (v is None or (isinstance(v, str) and v.strip() == "")) for v in value
        )
    return False


def _get_string(values: list) -> str:
    for v in values:
        if v is None:
            continue
        if isinstance(v, str):
            return v.strip()
        if isinstance(v, (int, float, bool)):
            return str(v).strip()
    return ""


def _get_merge(values: list) -> str:
    parts = []
    for v in values:
        if not v:
            continue
        if isinstance(v, list):
            items = []
            for item in v:
                if isinstance(item, dict):
                    items.append(item.get("name", ""))
                elif item:
                    items.append(str(item))
            parts.append("; ".join(x for x in items if x))
        else:
            parts.append(str(v))
    return "\n".join(p for p in parts if p)


def _get_list(values: list) -> list[str]:
    return [v.strip() for v in values if v and (not isinstance(v, str) or v.strip())]


def _parse_date(s: str, fmt: str) -> str | None:
    if not s:
        return None
    s = s.strip()
    if fmt == "yyyy-MM-dd":
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return s[:10]
        return None
    if fmt == "dd.MM.yyyy":
        m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})", s)
        if not m:
            return None
        dd, mm, yyyy = m.groups()
        try:
            datetime(int(yyyy), int(mm), int(dd))
            return f"{yyyy}-{mm}-{dd}"
        except ValueError:
            return None
    try:
        from dateutil import parser as du_parser

        dt = du_parser.parse(s, dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _get_date(values: list, fmt: str | None) -> str | None:
    if not values or not values[0]:
        return None
    return _parse_date(str(values[0]), fmt or "dd.MM.yyyy")


def _get_boolean(values: list) -> bool:
    return values[0] is True if values else False


def _get_progress_rows(values: list) -> list[dict]:
    result: dict[tuple[str, str], dict] = {}
    long_key = "Дата размещения Информация о размещении событий в движении дела предоставляется на основе сведений, хранящихся в учетной системе судебного делопроизводства"
    for row in values:
        if not isinstance(row, dict):
            continue
        # Moscow nested wrapper {section: [inner rows]}: expand wanted sections,
        # skip the rest. Flat regional rows fall through unchanged.
        if len(row) == 1 and isinstance(next(iter(row.values())), list):
            section, inner = next(iter(row.items()))
            if section not in _PROGRESS_SECTIONS:
                continue
            inner_rows = [r for r in inner if isinstance(r, dict)]
        else:
            inner_rows = [row]
        for r in inner_rows:
            if long_key in r:
                r["Дата размещения"] = r.pop(long_key)
            date = next((str(r[k]).strip() for k in _PROGRESS_DATE_KEYS if r.get(k)), "")
            name = next((str(r[k]).strip() for k in _PROGRESS_EVENT_KEYS if r.get(k)), "")
            entry = {"date": date, "event": name}
            for k, v in r.items():
                if v and k not in _PROGRESS_DATE_KEYS and k not in _PROGRESS_EVENT_KEYS:
                    entry[k] = v
            result[(date, name)] = entry
    return list(result.values())


_TYPE_HANDLERS = {
    "string": _get_string,
    "merge": _get_merge,
    "list": _get_list,
    "date": _get_date,
    "boolean": _get_boolean,
    "progress_rows": _get_progress_rows,
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

# Document-type names carrying no information: "" and the JSONPath container key
# "raw_json" that the legacy court_decisions location surfaces. These must never
# overwrite a real "Судебный акт #N (…)" name during _document_text dedup.
_PLACEHOLDER_DOC_NAMES = frozenset({"", "raw_json"})


def prepare_doc(raw: dict) -> dict:
    """Apply elastic_index_dict rules to raw data and return a document dict."""
    doc: dict = {}

    for field, config in RULES.items():
        field_type = config.get("type", "string")
        locations = config.get("locations", [])

        # For _document_text: resolve with tab names preserved.
        # Multiple locations can match the same text (generic "СУДЕБНЫЕ АКТЫ"
        # tab + specific "Судебный акт #N (…)" nested tab); keep one entry per
        # text and let the later (more specific) location win the name.
        if field == "_document_text":
            name_by_text: dict[str, str] = {}
            text_order: list[str] = []
            for location in locations:
                path = location["key"] if isinstance(location, dict) else location
                for tab_name, text_val in get_named_values_by_path(raw, path):
                    if not isinstance(text_val, str):
                        continue
                    text = text_val.strip()
                    if not text:
                        continue
                    if text not in name_by_text:
                        text_order.append(text)
                        name_by_text[text] = tab_name
                    elif tab_name not in _PLACEHOLDER_DOC_NAMES:
                        # later real name wins; a placeholder (raw_json/"") from
                        # the legacy court_decisions location can't clobber it
                        name_by_text[text] = tab_name
            doc["_document_text"] = [
                {"name": name_by_text[t], "text": t} for t in text_order
            ]
            continue

        for location in locations:
            fmt = None
            if isinstance(location, dict):
                fmt = location.get("format")
                path = location["key"]
            else:
                path = location

            values = get_value_by_path(raw, path)
            if _is_empty(values):
                continue

            handler = _TYPE_HANDLERS.get(field_type, _get_string)
            result = handler(values, fmt) if field_type == "date" else handler(values)

            if not _is_empty(result):
                doc[field] = result
                break

    # Override case_type/instance from type_instance mapping if present
    type_instance = raw.get("type_instance")
    if type_instance and type_instance in TYPE_INSTANCE_RULES:
        info = TYPE_INSTANCE_RULES[type_instance]
        doc["case_type"] = info["case_type"]
        doc["instance"] = info["instance"]

    for field, iso in _derive_onhold_dates(doc.get("progress") or []).items():
        doc.setdefault(field, iso)

    return doc


def _derive_onhold_dates(progress: list) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in progress:
        if not isinstance(row, dict):
            continue
        blob = " ".join(str(v) for v in row.values() if isinstance(v, str))
        for field, pattern in _ONHOLD_PATTERNS.items():
            if field in out or not pattern.search(blob):
                continue
            iso = _parse_date(row.get("date") or "", "dd.MM.yyyy")
            if iso:
                out[field] = iso
        if len(out) == len(_ONHOLD_PATTERNS):
            break
    return out
