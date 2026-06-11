"""Assemble the final document structure from a unit + list of parsed files.

Ports toDocument.js.
"""

import json
import re
from pathlib import Path

from .utils.dates import date_value_to_iso, get_files_version
from .utils.paths import dir_of_rel, norm_path

_UUID_RE = re.compile(r"_uid=([0-9a-f-]+)")

_HERE = Path(__file__).parent.parent  # project root (index/)

with open(_HERE / "instance_case_type_mapping.json", encoding="utf-8") as _f:
    CASE_TYPE_MAPPING: dict = json.load(_f)


def to_document(
    manifest_path: str,
    rel_from_root: str,
    meta: dict,
    html_files: list[str],
    parsed_files: list[dict],
) -> dict:
    """Build the ES document container from a parsed unit."""
    rel_dir = dir_of_rel(rel_from_root)
    id_final = meta.get("ID") or meta.get("id_final")
    version_info = get_files_version(html_files, rel_dir, meta)

    instance_key = str(meta.get("instance", ""))
    case_type_key = str(meta.get("case_type", ""))
    instance_info = CASE_TYPE_MAPPING.get(instance_key, {}).get(case_type_key)

    court_code = meta.get("court_id") or meta.get("court_code")
    court_tag = meta.get("court_tag") or (court_code[2:4] if court_code else None)
    link = meta.get("link") or ""
    m = _UUID_RE.search(link)
    uuid = m.group(1) if m else None
    base_meta = {
        "case_number": meta.get("case_number"),
        "instance": meta.get("instance"),
        "court_name": meta.get("court_name"),
        "court_code": court_code,
        "tag": court_tag,
        "region": meta.get("region"),
        "link": link or None,
        "uuid": uuid,
        "date_reg": date_value_to_iso(meta.get("date_reg")),
    }
    new_meta = (
        {**base_meta, "case_type": instance_info["case_type"], "instance": instance_info["instance"]}
        if instance_info
        else base_meta
    )

    return {
        "files": parsed_files,
        "version": version_info["version"],
        "version_ts": version_info["version_ts"],
        "id_final": id_final,
        "path_manifest": norm_path(manifest_path),
        "html_files": html_files,
        "meta": new_meta,
    }
