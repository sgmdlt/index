import re

_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def normalize_jsonl_text(t: str) -> str:
    """Normalize JSONL-sourced text to match parser output.

    Handles two input shapes:
    1. Already JSON-decoded (real \\n, real "): only collapse runs of spaces.
    2. Raw dumped JSON string (literal \\n, \\"): unescape, drop wrap-newlines,
       restore doc newlines, then collapse.

    Strips C0 control chars (except \\t, \\n, \\r).
    """
    if "\\n" in t or '\\"' in t:
        t = t.replace('\\"', '"')
        t = t.replace("\n", " ")
        t = t.replace("\\n", "\n")
    t = _CTRL_RE.sub("", t)
    t = re.sub(r" {2,}", " ", t)
    return t.strip()
