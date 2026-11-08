"""S3 analogue of `traverse_cases`.

Single-pass `list_objects_v2` over the prefix, streaming keys in lexicographic
order. HTML filenames sort before `manifest.json` within a case dir (digits
< 'm'), so we can buffer html names per-parent and emit a unit when the
matching manifest arrives. One LIST stream → ~1000 keys per page, vs. one
LIST per directory if we walked with Delimiter='/'.
"""

import json
import logging
import time
from typing import Callable, Generator

from .traverse import _parse_html_timestamp

logger = logging.getLogger(__name__)


def traverse_cases_s3(
    s3_client,
    bucket: str,
    prefix: str,
    on_manifest_ms: Callable[[float], None] | None = None,
) -> Generator[dict, None, None]:
    """Yield one unit per case dir found under s3://bucket/prefix.

    Unit shape matches `traverse_cases`: manifest_path (full key),
    rel_from_root (key minus prefix), meta (parsed JSON), html_files (sorted).
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    cur_parent: str | None = None
    htmls: list[str] = []
    plen = len(prefix)

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", ()):
            key = obj["Key"]
            slash = key.rfind("/")
            if slash < 0:
                continue
            parent, fname = key[:slash], key[slash + 1 :]

            if parent != cur_parent:
                cur_parent = parent
                htmls = []

            if fname == "manifest.json":
                if not htmls:
                    continue
                t0 = time.perf_counter()
                try:
                    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                    meta = json.loads(body)
                except Exception as exc:
                    logger.warning("Skip (bad manifest): %s: %s", key, exc)
                    htmls = []
                    continue
                if on_manifest_ms is not None:
                    on_manifest_ms((time.perf_counter() - t0) * 1000.0)

                yield {
                    "manifest_path": key,
                    "rel_from_root": key[plen:] if key.startswith(prefix) else key,
                    "meta": meta,
                    "html_files": sorted(htmls, key=_parse_html_timestamp),
                }
                htmls = []
            elif fname.lower().endswith(".html"):
                htmls.append(fname)
