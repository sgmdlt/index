"""Court document bytes → text.

Ported from the MGS downloader when extraction moved here: the crawler stores
raw .doc/.docx/.rtf/.pdf bytes in WARC and never extracts, so a better
extractor or a newer OCR costs CPU rather than another 6.6M downloads.

OCR is the expensive path (measured ~1.5 s versus 4 ms for a text layer) but a
rare one: 383 documents out of 1.2M cases needed it in the reference corpus, so
results are recomputed rather than cached.

`.doc` needs the `antiword` binary on PATH; without it those documents fall
through to the remaining extractors and usually come back empty.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractResult:
    text: str
    reason: str | None


def sniff_ext(body: bytes) -> str | None:
    if body.startswith(b"%PDF"):
        return "pdf"
    if body.startswith(b"PK\x03\x04"):
        return "docx"
    if body.startswith(b"{\\rtf"):
        return "rtf"
    if body.startswith(b"\xD0\xCF\x11\xE0"):
        return "doc"
    return None


import os
import re
import subprocess
import tempfile

_UNAVAILABLE = "временно недоступ"
_WS = re.compile(r"\s+")
_IMAGE_EXTS = {"png", "jpg", "jpeg", "tiff", "gif", "bmp"}

# OCR fallback for scanned PDFs (no text layer). Russian by default.
_OCR_LANG = os.getenv("OCR_LANG", "rus")
_OCR_DPI = int(os.getenv("OCR_DPI", "300"))
_OCR_TESSDATA = os.getenv("OCR_TESSDATA", "/usr/share/tesseract-ocr/5/tessdata")


def _norm(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = _WS.sub(" ", text)
    return text.strip()


def _txt(body: bytes) -> str:
    for enc in ("cp1251", "utf-8"):
        try:
            return body.decode(enc)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", "ignore")


def _rtf(body: bytes) -> str:
    from striprtf.striprtf import rtf_to_text
    return rtf_to_text(body.decode("cp1251", "ignore"), errors="ignore")


def _docx(body: bytes) -> str:
    import docx2txt
    with tempfile.NamedTemporaryFile(suffix=".docx") as tmp:
        tmp.write(body)
        tmp.flush()
        return docx2txt.process(tmp.name) or ""


def _antiword(body: bytes) -> str:
    with tempfile.NamedTemporaryFile(suffix=".doc") as tmp:
        tmp.write(body)
        tmp.flush()
        out = subprocess.run(
            ["antiword", "-m", "UTF-8.txt", "-w", "0", tmp.name],
            capture_output=True,
        )
    return out.stdout.decode("utf-8", "ignore")


def _pdf(body: bytes) -> str:
    import fitz
    parts = []
    with fitz.open(stream=body, filetype="pdf") as doc:
        for page in doc:
            parts.append(page.get_text())
    return "\n".join(parts)


def _pdf_ocr(body: bytes) -> str:
    """OCR a scanned PDF (no text layer) via pymupdf's built-in Tesseract."""
    import fitz
    parts = []
    with fitz.open(stream=body, filetype="pdf") as doc:
        for page in doc:
            tp = page.get_textpage_ocr(
                language=_OCR_LANG, dpi=_OCR_DPI, full=True, tessdata=_OCR_TESSDATA)
            parts.append(page.get_text(textpage=tp))
    return "\n".join(parts)


def _img_ocr(body: bytes, ext: str) -> str:
    """OCR a raw image attachment (jpg/png/tiff…) — open as 1-page doc, OCR it."""
    import fitz
    with fitz.open(stream=body, filetype=ext) as doc:
        page = doc[0]
        tp = page.get_textpage_ocr(
            language=_OCR_LANG, dpi=_OCR_DPI, full=True, tessdata=_OCR_TESSDATA)
        return page.get_text(textpage=tp)


# ext → extractor function name (resolved via globals() so monkeypatch on the
# module attribute is honored)
_EXTRACTORS = {"txt": "_txt", "rtf": "_rtf", "docx": "_docx",
               "doc": "_antiword", "pdf": "_pdf"}


def to_text(body: bytes, ext: str | None) -> ExtractResult:
    if ext in _IMAGE_EXTS:
        try:
            ocr = _norm(globals()["_img_ocr"](body, ext))
        except Exception:
            ocr = ""
        if ocr:
            return ExtractResult(ocr, None)
        return ExtractResult("", "image")
    if ext is None:
        ext = sniff_ext(body)
    fn_name = _EXTRACTORS.get(ext)
    if fn_name is None:
        return ExtractResult("", "extract_fail")
    try:
        raw = globals()[fn_name](body)
    except Exception:
        raw = ""
    text = _norm(raw)
    if _UNAVAILABLE in text:
        return ExtractResult("", "unavailable")
    if not text:
        if ext == "pdf":
            # no text layer = scanned: try OCR before giving up as image
            try:
                ocr = _norm(globals()["_pdf_ocr"](body))
            except Exception:
                ocr = ""
            if ocr:
                return ExtractResult(ocr, None)
            return ExtractResult("", "image")
        return ExtractResult("", "extract_fail")
    return ExtractResult(text, None)
