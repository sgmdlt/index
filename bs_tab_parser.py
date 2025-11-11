from __future__ import annotations

from bs4 import BeautifulSoup

try:
    # Optional: better encoding detection if available
    from bs4 import UnicodeDammit  # type: ignore
except Exception:  # pragma: no cover
    UnicodeDammit = None  # type: ignore


def read_html_with_encoding(path: str) -> str:
    with open(path, "rb") as fh:
        raw = fh.read()
    if UnicodeDammit is None:
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("cp1251", errors="replace")
    dammit = UnicodeDammit(raw, is_html=True)
    if dammit.unicode_markup:
        return dammit.unicode_markup
    # Fallback
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1251", errors="replace")


async def tab_to_dict(tab) -> dict | list | str:
    headers = [
        c.get_text(" ", strip=True)
        for c in tab.select(":scope > thead th, :scope > thead td")
    ]
    body_rows = tab.select(":scope > tbody > tr") or tab.select(":scope > tr")
    parsed_tab: list[dict] = []

    if headers and len(headers) > 1 and body_rows:
        for row in body_rows:
            cells = row.select(":scope > td")
            if not cells:
                cells = [
                    c
                    for c in row.select(":scope > th, :scope > td")
                    if c.name.lower() != "th"
                ]
            if not cells:
                continue

            row_dict: dict = {}
            for k, cell in zip(headers, cells):
                nested = cell.select_one("table")
                row_dict[k] = (
                    await tab_to_dict(nested)
                    if nested
                    else cell.get_text(" ", strip=True)
                )
            if row_dict:
                parsed_tab.append(row_dict)

        if parsed_tab:
            return parsed_tab

    # 2) No thead: remove header rows with colspan if possible
    rows_all = tab.select(":scope > tr")
    rows = [r for r in rows_all if not r.select(":scope > [colspan]")] or rows_all

    if rows:
        # 2.1) Classic 2-column table => dict key->value
        first_cells = rows[0].select(":scope > td, :scope > th")
        if len(first_cells) == 2:
            out: dict = {}
            for r in rows:
                cells = r.select(":scope > td, :scope > th")
                if len(cells) < 2:
                    continue
                key = cells[0].get_text(" ", strip=True)
                if not key:
                    continue
                val_cells = cells[1:]
                sub = None
                for c in val_cells:
                    sub = c.select_one("table")
                    if sub:
                        break
                if sub:
                    out[key] = await tab_to_dict(sub)
                else:
                    vals = [
                        c.get_text(" ", strip=True)
                        for c in val_cells
                        if c.get_text(strip=True)
                    ]
                    out[key] = " ".join(vals)
            if out:
                return out

        # 2.2) >2 columns: first row bold headers => row dicts
        first_row = rows[0]
        bold_in_first = first_row.select(":scope > td > b, :scope > th > b")
        all_first_cells = first_row.select(":scope > td, :scope > th")
        if bold_in_first and len(all_first_cells) > 1:
            headers = [c.get_text(" ", strip=True) for c in all_first_cells]
            for r in rows[1:]:
                values = [
                    c.get_text(" ", strip=True)
                    for c in r.select(":scope > td, :scope > th")
                ]
                if not values:
                    continue
                row_dict = {}
                for k, v in zip(headers, values):
                    row_dict[k] = v
                if row_dict:
                    parsed_tab.append(row_dict)
            if parsed_tab:
                return parsed_tab

        # 2.3) Fallback column-wise: key is first cell, build list of column dicts
        cols_out: list[dict] = []
        for r in rows:
            cells = r.select(":scope > td, :scope > th")
            if len(cells) < 2:
                continue
            key = cells[0].get_text(" ", strip=True)
            for i, c in enumerate(cells[1:]):
                val = c.get_text(" ", strip=True)
                if not val:
                    continue
                if len(cols_out) <= i:
                    cols_out.append({key: val})
                else:
                    cols_out[i][key] = val
        if cols_out:
            return cols_out

    # 3) Only nested tables
    nested = tab.select(":scope table")
    if nested:
        out: dict = {}
        for sub in nested:
            tname_el = sub.find(attrs={"colspan": True})
            tname = tname_el.get_text(" ", strip=True) if tname_el else "table"
            out[tname] = await tab_to_dict(sub)
        return out

    # 4) Plain text fallback
    return tab.get_text(" ", strip=True)
