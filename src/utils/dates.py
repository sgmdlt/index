"""Date parsing utilities for court case HTML filenames, paths, and manifest fields."""

import re
from datetime import datetime, timezone


def html_name_to_iso(name: str) -> str | None:
    """Parse HTML filename -> ISO string.

    New format: <unix_ts>_DD-MM-YYYY.HH:MM:SS.html  → uses unix timestamp
    Old format: DD-MM-YYYY.HH:MM:SS.html             → uses date/time from name
    """
    if not isinstance(name, str):
        return None

    # New format: unix timestamp prefix is the exact download time
    m = re.match(r"^(\d{10,})_\d{2}-\d{2}-\d{4}\.\d{2}:\d{2}:\d{2}", name)
    if m:
        try:
            dt = datetime.fromtimestamp(int(m.group(1)), tz=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        except (ValueError, OSError):
            pass

    # Old format: parse date/time from the filename itself
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})\.(\d{2}):(\d{2}):(\d{2})", name)
    if not m:
        return None
    dd, mm, yyyy, hh, mi, ss = m.groups()
    try:
        dt = datetime(
            int(yyyy), int(mm), int(dd), int(hh), int(mi), int(ss), tzinfo=timezone.utc
        )
        return dt.isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def dir_to_iso(rel_dir: str) -> str | None:
    """Extract YYYY/MM/DD from a path string -> ISO string."""
    m = re.search(r"(^|/)(\d{4})/(\d{2})/(\d{2})(/|$)", str(rel_dir))
    if not m:
        return None
    yyyy, mm, dd = m.group(2), m.group(3), m.group(4)
    try:
        dt = datetime(int(yyyy), int(mm), int(dd), tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def date_value_to_iso(value) -> str | None:
    """Normalize a raw date value to an ES-friendly ISO string, or None.

    Handles both DD.MM.YYYY (filesystem manifests) and YYYY-MM-DD (JSONL records).
    """
    if not value:
        return None
    cand = str(value)

    # DD.MM.YYYY (filesystem manifest format)
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})", cand)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            return datetime(int(yyyy), int(mm), int(dd), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            return None

    # YYYY-MM-DD (JSONL / ISO format)
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", cand)
    if m:
        yyyy, mm, dd = m.groups()
        try:
            return datetime(int(yyyy), int(mm), int(dd), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            return None

    return None


def get_files_version(html_files: list[str], rel_dir: str, meta: dict) -> dict:
    """Resolve version count and version_ts from HTML files, path, and manifest.

    Priority: filename timestamp > path date > manifest date > now.
    """
    from_names = sorted(filter(None, (html_name_to_iso(f) for f in (html_files or []))))
    version_ts = (
        (from_names[-1] if from_names else None)
        or dir_to_iso(rel_dir)
        or date_value_to_iso((meta or {}).get("date_reg"))
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    return {
        "version_ts": version_ts,
        "version": len(from_names),
    }
