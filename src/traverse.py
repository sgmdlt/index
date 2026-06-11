"""Filesystem traversal yielding case units.

Ports traverse.js using iterative DFS (no recursion depth limit).
"""

import json
import logging
import os
import time
from typing import Callable, Generator

from .utils.paths import norm_path

logger = logging.getLogger(__name__)


def _parse_html_timestamp(name: str) -> tuple:
    """Parse DD-MM-YYYY.HH:MM:SS.html or <ts>_DD-MM-YYYY.HH:MM:SS.html → sortable tuple."""
    stem = name[:-5] if name.lower().endswith(".html") else name
    # Strip optional unix timestamp prefix (new format)
    if "_" in stem:
        stem = stem.split("_", 1)[1]
    parts = stem.split(".")
    if len(parts) != 2:
        return (0,) * 6
    date_part, time_part = parts
    date_bits = date_part.split("-")
    time_bits = time_part.split(":")
    if len(date_bits) != 3 or len(time_bits) != 3:
        return (0,) * 6
    try:
        dd, mm, yyyy = int(date_bits[0]), int(date_bits[1]), int(date_bits[2])
        hh, mi, ss = int(time_bits[0]), int(time_bits[1]), int(time_bits[2])
        return (yyyy, mm, dd, hh, mi, ss)
    except ValueError:
        return (0,) * 6


def _list_html_files(directory: str) -> list[str]:
    try:
        entries = os.listdir(directory)
    except OSError:
        return []
    html = [
        e
        for e in entries
        if e.lower().endswith(".html") and os.path.isfile(os.path.join(directory, e))
    ]
    html.sort(key=_parse_html_timestamp)
    return html


def _append_failed(path: str, log_file: str) -> None:
    """Append one relative manifest path to a file for later reprocessing."""
    try:
        with open(log_file, "a", encoding="utf-8") as fh:
            fh.write(path + "\n")
    except OSError as exc:
        logger.warning("Could not write to failed-units log %s: %s", log_file, exc)


def traverse_cases(
    root: str,
    filter_str: str | None = None,
    failed_log: str | None = None,
    on_manifest_ms: Callable[[float], None] | None = None,
    on_redownload: Callable[[str, str, str | None], None] | None = None,
) -> Generator[dict, None, None]:
    """Iterative DFS traversal yielding one unit dict per manifest.json found.

    Each yielded dict has:
      manifest_path, rel_from_root, meta (dict), html_files (list[str])

    If *failed_log* is given, any directory whose manifest.json cannot be read
    is appended to that file (one relative path per line) for later reprocessing.
    """
    root_abs = os.path.realpath(root)
    if not os.path.isdir(root_abs):
        raise ValueError(f"--root must be an existing directory: {root_abs}")

    stack = [root_abs]

    while stack:
        cur = stack.pop()

        try:
            entries_iter = os.scandir(cur)
        except OSError:
            continue

        dirs = []
        has_manifest = False

        with entries_iter:
            for entry in entries_iter:
                if entry.is_dir(follow_symlinks=False):
                    dirs.append(entry.path)
                elif entry.is_file() and entry.name == "manifest.json":
                    has_manifest = True

        # Push dirs in reverse-sorted order for alphabetical ascending traversal
        dirs.sort(reverse=True)
        stack.extend(dirs)

        if not has_manifest:
            continue

        manifest_path = os.path.join(cur, "manifest.json")
        rel = norm_path(os.path.relpath(manifest_path, root_abs))

        if filter_str and filter_str not in rel:
            continue

        try:
            t0 = time.perf_counter()
            with open(manifest_path, encoding="utf-8") as fh:
                meta = json.load(fh)
            if on_manifest_ms is not None:
                on_manifest_ms((time.perf_counter() - t0) * 1000.0)
        except Exception as exc:
            logger.warning("Skip (bad manifest): %s: %s", manifest_path, exc)
            if on_redownload is not None:
                on_redownload("bad_manifest_json", manifest_path, None)
            elif failed_log:
                _append_failed(rel, failed_log)
            continue

        html_files = _list_html_files(cur)
        if not html_files:
            logger.warning("Empty case dir: %s", rel)
            if on_redownload is not None:
                on_redownload("docs_missing", manifest_path, None)
            elif failed_log:
                _append_failed(rel, failed_log)
            continue

        yield {
            "manifest_path": manifest_path,
            "rel_from_root": rel,
            "meta": meta,
            "html_files": html_files,
        }
