"""HTML → raw_tables dict extractor (selectolax/lexbor port).

Drop-in alternative to src.parse_html.fill_raw_tables. Same signature,
same return shape. Uses selectolax (lexbor) instead of bs4+lxml.

A/B test: swap the import in src/main.py from `src.parse_html` to
`src.parse_html_sx` and rerun. If parity holds and parse_ms drops,
promote.
"""

import logging
import re
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.parse_html_mgs import fill_raw_tables_mgs, is_mgs

logger = logging.getLogger(__name__)


# ---------- helpers ----------

def _text(el: LexborNode | None) -> str:
    if el is None:
        return ""
    return re.sub(r"\s+", " ", el.text(deep=True, separator=" ", strip=False) or "").strip()


def _direct_children(node: LexborNode, tag: str | None = None) -> list[LexborNode]:
    out = []
    for child in node.iter(include_text=False):
        if tag is None or child.tag == tag:
            out.append(child)
    return out


def _has_descendant(node: LexborNode, sel: str) -> bool:
    return node.css_first(sel) is not None


def _find_parent_with_class(node: LexborNode, cls: str) -> LexborNode | None:
    p = node.parent
    while p is not None:
        classes = (p.attributes.get("class") or "") if p.attributes else ""
        if cls in classes.split():
            return p
        p = p.parent
    return None


def _find_parent_with_tag(node: LexborNode, tag: str) -> LexborNode | None:
    p = node.parent
    while p is not None:
        if p.tag == tag:
            return p
        p = p.parent
    return None


def _find_parent_with_id(node: LexborNode, id_: str) -> LexborNode | None:
    p = node.parent
    while p is not None:
        if p.attributes and p.attributes.get("id") == id_:
            return p
        p = p.parent
    return None


def _find_prev_sibling(node: LexborNode, tag: str) -> LexborNode | None:
    p = node.prev
    while p is not None:
        if p.tag == tag:
            return p
        p = p.prev
    return None


def _has_nested_table(cell: LexborNode) -> bool:
    return _first_descendant(cell, "table") is not None


def _first_descendant(node: LexborNode, sel: str) -> LexborNode | None:
    """Return first matching descendant, excluding `node` itself.
    Lexbor's .css() includes self when the node matches the selector,
    which differs from bs4 and breaks recursion when searching for a
    nested table inside a node that is itself a <table>. Uses `==`
    rather than `is` because selectolax returns a fresh Python wrapper
    per .css() call, so identity check fails for the same lexbor node.
    """
    for n in node.css(sel):
        if n != node:
            return n
    return None


def _descendants(node: LexborNode, sel: str) -> list[LexborNode]:
    return [n for n in node.css(sel) if n != node]


# ---------- table parsing (Format 2) ----------

def _table_rows(table: LexborNode) -> list[LexborNode]:
    rows: list[LexborNode] = []
    for child in _direct_children(table):
        if child.tag in ("thead", "tbody", "tfoot"):
            rows.extend(_direct_children(child, "tr"))
        elif child.tag == "tr":
            rows.append(child)
    return rows


def _row_tds(tr: LexborNode) -> list[LexborNode]:
    return _direct_children(tr, "td")


def _row_ths(tr: LexborNode) -> list[LexborNode]:
    return _direct_children(tr, "th")


def _parse_table(table: LexborNode):
    all_rows = _table_rows(table)
    if not all_rows:
        return {}

    data_rows = []
    for tr in all_rows:
        ths = _row_ths(tr)
        if any((th.attributes or {}).get("colspan") for th in ths):
            continue
        tds = _row_tds(tr)
        if not tds:
            continue
        data_rows.append(tr)

    if not data_rows:
        return {}

    first_tds = _row_tds(data_rows[0])

    # Dict mode: 2 columns → {key: value}
    if len(first_tds) == 2:
        result = {}
        for tr in data_rows:
            tds = _row_tds(tr)
            if len(tds) != 2:
                continue
            k_cell, v_cell = tds
            key = _text(k_cell)
            if not key:
                continue
            if _has_nested_table(v_cell):
                nested = _first_descendant(v_cell, "table")
                result[key] = _parse_table(nested)
            else:
                result[key] = _text(v_cell)
        return result

    # Registry mode: N columns → [{col: val}]
    header_tr = None
    headers: list[str] = []

    for tr in data_rows:
        bolds = []
        for td in _direct_children(tr, "td"):
            bolds.extend(_direct_children(td, "b"))
        if len(bolds) >= 2:
            header_tr = tr
            headers = [_text(b) for b in bolds]
            break

    if header_tr is None:
        header_tr = data_rows[0]
        headers = [_text(td) for td in first_tds]

    rows = []
    header_seen = False
    for tr in data_rows:
        if not header_seen:
            if tr is header_tr:
                header_seen = True
            continue
        tds = _row_tds(tr)
        if not tds:
            continue
        row = {}
        for i, key in enumerate(headers):
            if not key or i >= len(tds):
                continue
            cell = tds[i]
            if _has_nested_table(cell):
                nested = _first_descendant(cell, "table")
                row[key] = _parse_table(nested)
            else:
                row[key] = _text(cell)
        if row:
            rows.append(row)

    return rows


# ---------- tab_to_dict (Format 1) ----------

def _direct_trs(tab: LexborNode) -> list[LexborNode]:
    """Return logical direct <tr> children of a <table>, descending through
    implicit <tbody>/<thead>/<tfoot> wrappers that the HTML5 parser inserts.
    Mirrors what bs4+lxml sees for `:scope > tr` on source HTML without
    explicit tbody.
    """
    out: list[LexborNode] = []
    for child in _direct_children(tab):
        if child.tag in ("tbody", "thead", "tfoot"):
            out.extend(_direct_children(child, "tr"))
        elif child.tag == "tr":
            out.append(child)
    return out


def _direct_trs_no_colspan(tab: LexborNode) -> list[LexborNode]:
    out = []
    for tr in _direct_trs(tab):
        if not _has_descendant(tr, "[colspan]"):
            out.append(tr)
    return out


def _explicit_thead(tab: LexborNode) -> bool:
    """True only if source HTML had an explicit <thead> (still survives as
    direct child even after lexbor adds implicit <tbody>)."""
    return bool(_direct_children(tab, "thead"))


def tab_to_dict(tab: LexborNode):
    parsed_tab: list | dict = []

    has_thead = _explicit_thead(tab)

    if has_thead:
        thead_tds = tab.css("thead td")
        for row in _direct_trs(tab):
            row_dict = {}
            row_tds = row.css("td")
            for key, value in zip(thead_tds, row_tds, strict=True):
                row_dict[_text(key)] = _text(value)
            parsed_tab.append(row_dict)

            if "table>" in row.html:
                raise ValueError

    else:
        direct_trs = _direct_trs(tab)
        if direct_trs and _has_descendant(direct_trs[0], "[colspan]"):
            direct_trs[0].decompose(recursive=True)

        no_colspan = _direct_trs_no_colspan(tab)

        if no_colspan:
            first_tds = no_colspan[0].css("td")

            if len(first_tds) == 2:
                parsed_tab = {}
                for row in no_colspan:
                    tds = row.css("td")
                    parsed_tab[_text(tds[0])] = _text(tds[1])
                nested_tbl = _first_descendant(tab, "table")
                if nested_tbl is not None:
                    colspan_node = tab.css_first("[colspan]")
                    if colspan_node is not None:
                        parsed_tab[_text(colspan_node)] = tab_to_dict(nested_tbl)

            elif len(first_tds) > 2:
                first_row_bolds = no_colspan[0].css("td > b")

                if len(first_row_bolds) > 1:
                    header_tds = no_colspan[0].css("td")
                    for row in no_colspan[1:]:
                        row_dict = {}
                        row_tds = row.css("td")
                        for key, value in zip(header_tds, row_tds, strict=True):
                            row_dict[_text(key)] = _text(value)
                        parsed_tab.append(row_dict)
                        if "table>" in row.html:
                            raise ValueError

                else:
                    col_bolds = []
                    for tr in no_colspan:
                        col_bolds.extend(tr.css("td > b"))
                    if len(col_bolds) > 1:
                        for row in no_colspan:
                            tds = row.css("td")
                            if not tds:
                                continue
                            label = _text(tds[0])
                            for index, column in enumerate(tds[1:]):
                                col_text = _text(column)
                                if len(parsed_tab) < index + 1:
                                    parsed_tab.append({label: col_text})
                                else:
                                    parsed_tab[index][label] = col_text
                            if "table>" in row.html:
                                raise ValueError

            else:
                raise ValueError

        elif len(_direct_trs(tab)) == 2 and _first_descendant(tab, "table") is not None:
            parsed_tab = {}
            colspan_node = tab.css_first("[colspan]")
            nested_tbl = _first_descendant(tab, "table")
            if colspan_node is not None and nested_tbl is not None:
                parsed_tab[_text(colspan_node)] = tab_to_dict(nested_tbl)

        elif tab.css("tr"):
            logger.debug("tab_to_dict: unrecognized table structure: %s", tab.html[:200])
            raise ValueError

    return parsed_tab


# ---------- public entry point ----------

def fill_raw_tables(html: bytes | str) -> dict:
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")

    # mos-gorsud shares no markup with sudrf; the selectors below would all
    # miss and the page would parse to an empty raw_tables. Delegating keeps
    # this a true drop-in for src.parse_html, parity test included.
    if is_mgs(html):
        return fill_raw_tables_mgs(html)

    # Fix malformed pattern seen in the wild
    html = re.sub(r"</th>\s+<tr>\s+<tr>", "</th></tr><tr>", html, flags=re.IGNORECASE)

    parser = LexborHTMLParser(html)
    root = parser.root
    if root is None:
        return {"raw_tables": {}}

    formatted_case: dict = {"raw_tables": {}}

    try:
        # Формат 2: ul#case_bookmarks + div.lawcase-tab-content[id=tab_content_X]
        if root.css_first("ul#case_bookmarks") is not None:
            for label_el in root.css("ul#case_bookmarks li[id^=tab_id_]"):
                lid = (label_el.attributes or {}).get("id") or ""
                suffix = lid.replace("tab_id_", "")
                tab_name = _text(label_el)
                content_div = root.css_first(f"#tab_content_{suffix}")
                if content_div is None:
                    continue
                table = content_div.css_first("table")
                if table is None:
                    for meta in content_div.css("meta"):
                        meta.decompose(recursive=True)
                    formatted_case["raw_tables"][tab_name] = content_div.text(
                        deep=True, separator="", strip=False
                    )
                    continue
                # Таблица без строк — обёртка текста документа.
                # Lexbor (HTML5-spec) foster-parents text from inside <table>
                # to outside, so the act text now lives in content_div
                # rather than the empty table. Use content_div text to match
                # what bs4+lxml extracts via table.get_text() (lxml keeps text
                # inside the table).
                if table.css_first("tr") is None:
                    for meta in content_div.css("meta"):
                        meta.decompose(recursive=True)
                    formatted_case["raw_tables"][tab_name] = content_div.text(
                        deep=True, separator="", strip=False
                    )
                else:
                    formatted_case["raw_tables"][tab_name] = _parse_table(table)

        # Формат 1: ul.tabs li + div.contentt > div[id^=cont]
        else:
            tab_labels = root.css("ul.tabs li")
            tabs = root.css("div.contentt > div[id^=cont]")

            for tab_name_el, tab in zip(tab_labels, tabs, strict=True):
                tab_name = _text(tab_name_el)
                formatted_case["raw_tables"][tab_name] = []

                if tab.css_first("table") is None:
                    inner_li = tab.css("li[id^=tab], li[id^=doctab]")
                    if inner_li:
                        inner_div = tab.css("div[id^=cont_doc], div[id^=doctab_content]")
                        for doc_name, doc in zip(inner_li, inner_div, strict=True):
                            for meta in doc.css("meta"):
                                meta.decompose(recursive=True)
                            formatted_case["raw_tables"][tab_name].append({
                                _text(doc_name):
                                    doc.text(deep=True, separator="", strip=False)
                            })
                    else:
                        for meta in tab.css("meta"):
                            meta.decompose(recursive=True)
                        formatted_case["raw_tables"][tab_name] = tab.text(
                            deep=True, separator="", strip=False
                        )
                else:
                    direct_tables = _direct_children(tab, "table")
                    direct_tables_none_mobile = [
                        t for t in direct_tables
                        if "none-mobile" in ((t.attributes or {}).get("class") or "").split()
                    ]
                    if len(direct_tables) == 1 or len(direct_tables_none_mobile) == 1:
                        formatted_case["raw_tables"][tab_name] = tab_to_dict(direct_tables[0])
                    else:
                        # Multiple tables: name from each table's first-tr [colspan] node
                        names = []
                        for t in direct_tables:
                            first_tr = t.css_first("tr")
                            if first_tr is None:
                                continue
                            colspan = first_tr.css_first("[colspan]")
                            if colspan is not None:
                                names.append(colspan)
                        if len(names) != len(direct_tables):
                            raise ValueError("table-name/table count mismatch")
                        for table_name, table in zip(names, direct_tables, strict=True):
                            formatted_case["raw_tables"][tab_name].append({
                                _text(table_name):
                                    tab_to_dict(table)
                            })
    except Exception as e:
        logger.warning("Error parsing HTML: %s", e)

    return formatted_case
