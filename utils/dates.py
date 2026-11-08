"""Date parsing utilities for court case HTML filenames, paths, and manifest fields."""

import re
from datetime import datetime, timezone


def html_name_to_iso(name: str) -> str | None:
    """Parse HTML filename like DD-MM-YYYY.HH:MM:SS.html -> ISO string."""
    if not isinstance(name, str):
        return None
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


def manifest_to_iso(meta: dict) -> str | None:
    """Extract date from manifest dict (entry_date_to or date_reg) -> ISO string."""
    cand = meta.get("entry_date_to") or meta.get("date_reg")
    if not cand:
        return None
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})", str(cand))
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        dt = datetime(int(yyyy), int(mm), int(dd), tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def get_files_version(html_files: list[str], rel_dir: str, meta: dict) -> dict:
    """Resolve version count and version_ts from HTML files, path, and manifest.

    Priority: filename timestamp > path date > manifest date > now.
    """
    from_names = sorted(filter(None, (html_name_to_iso(f) for f in (html_files or []))))
    version_ts = (
        from_names[-1]
        or dir_to_iso(rel_dir)
        or manifest_to_iso(meta or {})
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    return {
        "version_ts": version_ts,
        "version": len(from_names),
    }
