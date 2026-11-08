"""HTML → raw_tables dict extractor.

Ports html_to_raw.js using BeautifulSoup4 + lxml.
Safe to import with no side effects (used in multiprocessing workers).
"""

import logging
import re
from bs4 import BeautifulSoup, Tag

from src.parse_html_mgs import fill_raw_tables_mgs, is_mgs

logger = logging.getLogger(__name__)


def _text(el) -> str:
    return re.sub(r"\s+", " ", el.get_text() or "").strip()


def _has_nested_table(cell: Tag) -> bool:
    return bool(cell.find("table"))


def _table_rows(table: Tag) -> list[Tag]:
    rows = []
    for child in table.children:
        if not isinstance(child, Tag):
            continue
        tag = child.name.lower() if child.name else ""
        if tag in ("thead", "tbody", "tfoot"):
            rows.extend(tr for tr in child.find_all("tr", recursive=False))
        elif tag == "tr":
            rows.append(child)
    return rows


def _row_tds(tr: Tag) -> list[Tag]:
    return tr.find_all("td", recursive=False)


def _row_ths(tr: Tag) -> list[Tag]:
    return tr.find_all("th", recursive=False)


def _parse_table(table: Tag):
    all_rows = _table_rows(table)
    if not all_rows:
        return {}

    # Drop section headers (<th colspan>) and empty rows
    data_rows = []
    for tr in all_rows:
        ths = _row_ths(tr)
        if any(th.get("colspan") for th in ths):
            continue
        tds = _row_tds(tr)
        if not tds:
            continue
        data_rows.append(tr)

    if not data_rows:
        return {}

    first_tds = _row_tds(data_rows[0])

    # Dict mode: exactly 2 columns → {key: value}
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
                nested = v_cell.find("table")
                result[key] = _parse_table(nested)
            else:
                result[key] = _text(v_cell)
        return result

    # Registry mode: N columns → [{col: val, ...}]
    header_tr = None
    headers = []

    for tr in data_rows:
        bolds = [
            b
            for td in tr.find_all("td", recursive=False)
            for b in td.find_all("b", recursive=False)
        ]
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
                nested = cell.find("table")
                row[key] = _parse_table(nested)
            else:
                row[key] = _text(cell)
        if row:
            rows.append(row)

    return rows


def _resolve_table_name(soup: BeautifulSoup, table: Tag) -> str:
    # Try <th> in first row
    first_row = table.find("tr")
    if first_row:
        th = first_row.find("th")
        if th:
            txt = _text(th)
            if txt:
                return txt

    # Try <caption>
    caption = table.find("caption", recursive=False)
    if caption:
        txt = _text(caption)
        if txt:
            return txt

    # Try parent tab label
    tab_content = table.find_parent(class_="lawcase-tab-content")
    if tab_content and tab_content.get("id", "").startswith("tab_content_"):
        suffix = tab_content["id"].replace("tab_content_", "")
        tab_label = soup.find(id=f"tab_id_{suffix}")
        if tab_label:
            txt = _text(tab_label)
            if txt:
                return txt

    # Try preceding sibling div
    prev = table.find_previous_sibling("div")
    if prev:
        txt = _text(prev)
        if txt:
            return txt

    return "Таблица"


def _ensure_unique_name(existing: dict, name: str) -> str:
    base = name or "Таблица"
    if base not in existing:
        return base
    idx = 2
    while f"{base} ({idx})" in existing:
        idx += 1
    return f"{base} ({idx})"


def _should_include_table(table: Tag) -> bool:
    if table.find_parent("table"):
        return False
    if table.find_parent(class_="mobile-lawcase-item"):
        return False
    if table.find_parent(id="printVersionBodyCase"):
        return False
    return True


def fill_raw_tables(html: bytes | str, documents: dict | None = None) -> dict:
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")

    # mos-gorsud shares no markup with sudrf — none of the selectors below
    # match, and the page would silently parse to an empty raw_tables.
    # `documents` are the raw files the MGS crawler archived beside the card.
    if is_mgs(html):
        return fill_raw_tables_mgs(html, documents)

    # Fix malformed HTML pattern seen in the wild
    html = re.sub(r"</th>\s+<tr>\s+<tr>", "</th></tr><tr>", html, flags=re.IGNORECASE)
    soup = BeautifulSoup(html, "lxml")
    for tbody in soup.find_all("tbody"):
        tbody.unwrap()
    tab_container = soup.select_one("div[id^=cont_doc]")
    formatted_case = {}
    formatted_case["raw_tables"] = {}

    try:
        # Формат 2: ul#case_bookmarks + div.lawcase-tab-content[id=tab_content_X]
        if soup.select_one("ul#case_bookmarks"):
            for label_el in soup.select("ul#case_bookmarks li[id^=tab_id_]"):
                suffix = label_el["id"].replace("tab_id_", "")
                tab_name = label_el.get_text(" ", strip=True)
                content_div = soup.find(id=f"tab_content_{suffix}")
                if not content_div:
                    continue

                # Вложенные документы: ul#doc_bookmarks > li[id^=doctab_id_] +
                # div[id^=doctab_content_]. Без этой ветки content_div.find("table")
                # натыкается на подпись внизу акта и парсит её как таблицу-реестр.
                doc_labels = content_div.select("ul#doc_bookmarks li[id^=doctab_id_]")
                if doc_labels:
                    docs = []
                    for doc_label_el in doc_labels:
                        doc_suffix = doc_label_el["id"].replace("doctab_id_", "")
                        doc_div = content_div.find(id=f"doctab_content_{doc_suffix}")
                        if not doc_div:
                            continue
                        for meta in doc_div.find_all("meta"):
                            meta.decompose()
                        docs.append({
                            doc_label_el.get_text(" ", strip=True): doc_div.get_text()
                        })
                    formatted_case["raw_tables"][tab_name] = docs
                    continue

                table = content_div.find("table")
                if not table:
                    for meta in content_div.find_all("meta"):
                        meta.decompose()
                    formatted_case["raw_tables"][tab_name] = content_div.get_text()
                    continue
                # Таблица без строк — обёртка текста документа
                if not table.find("tr"):
                    for meta in table.find_all("meta"):
                        meta.decompose()
                    formatted_case["raw_tables"][tab_name] = table.get_text()
                else:
                    formatted_case["raw_tables"][tab_name] = _parse_table(table)

        # Формат 1: ul.tabs li + div.contentt > div[id^=cont]
        else:
            for tab_name, tab in zip(
                soup.select("ul.tabs li"),
                soup.select("div.contentt > div[id^=cont]"),
            ):
                tab_name = tab_name.get_text(" ", strip=True)
                formatted_case["raw_tables"][tab_name] = []
                # Вкладки-тексты (нет таблицы)
                if not tab.find("table"):
                    # Если несколько текстов (вложенные вкладки)
                    if tab.select("li[id^=tab], li[id^=doctab]"):
                        for doc_name, doc in zip(
                            tab.select("li[id^=tab], li[id^=doctab]"),
                            tab.select("div[id^=cont_doc], div[id^=doctab_content]"),
                                    ):
                            for meta in doc.find_all("meta"):
                                meta.decompose()
                            formatted_case["raw_tables"][tab_name].append(
                                {doc_name.get_text(" ", strip=True): doc.get_text()}
                            )
                    # Если один текст
                    else:
                        for meta in tab.find_all("meta"):
                            meta.decompose()
                        formatted_case["raw_tables"][tab_name] = tab.get_text()
                # Вкладки-таблицы
                else:
                    # Если таблица одна
                    if (
                        len(tab.select(":scope > table")) == 1
                        or len(tab.select(":scope > table.none-mobile")) == 1
                    ):
                        formatted_case["raw_tables"][tab_name] = tab_to_dict(
                            tab.select_one(":scope > table")
                        )
                    # Если таблиц несколько
                    else:
                        for table_name, table in zip(
                            tab.select(":scope > table tr:first-child [colspan]"),
                            tab.select(":scope > table"),
                                    ):
                            formatted_case["raw_tables"][tab_name].append(
                                {
                                    table_name.get_text(" ", strip=True): tab_to_dict(table)
                                }
                            )
    except Exception as e:
        logger.warning("Error parsing HTML: %s", e)

    return formatted_case


def tab_to_dict(tab):
    parsed_tab = []

    # Когда есть шапка, пишем стандартный список словарей
    if tab.select(":scope > thead"):
        for row in tab.select(":scope > tr"):
            row_dict = {}
            for key, value in zip(
                tab.select("thead td"), row.select("td"), strict=True
            ):
                row_dict[key.get_text(" ", strip=True)] = value.get_text(
                    " ", strip=True
                )
            parsed_tab.append(row_dict)

            # ПРОВЕРКА
            if "table>" in str(row):
                raise ValueError

    # Без шапки
    else:
        # Если первый ряд растянут, убираем его
        if tab.select(":scope > tr:first-child [colspan]"):
            tab.select_one(":scope > tr:first-child").decompose()

        # Если в таблице только два столбца, пишем как один словарь, пропуская новые шапки и таблицы
        if tab.select(":scope > tr:not(:has([colspan]))"):
            if (
                len(tab.select_one(":scope > tr:not(:has([colspan]))").select("td"))
                == 2
            ):
                parsed_tab = {}
                for row in tab.select(":scope > tr:not(:has([colspan]))"):
                    parsed_tab[row.select("td")[0].get_text(" ", strip=True)] = (
                        row.select("td")[1].get_text(" ", strip=True)
                    )
                # Отдельно для вложенных таблиц
                if tab.select(":scope table"):
                    parsed_tab[
                        tab.select_one(":scope [colspan]").get_text(" ", strip=True)
                    ] = tab_to_dict(tab.select_one(":scope table"))

            # Если несколько столбцов, пишем списком словарей
            elif (
                len(tab.select_one(":scope > tr:not(:has([colspan]))").select("td")) > 2
            ):
                # Если заголовки с жирным шрифтом в первой строке — по строкам
                if (
                    len(
                        tab.select_one(":scope > tr:not(:has([colspan]))").select(
                            "td > b"
                        )
                    )
                    > 1
                ):
                    for row in tab.select(":scope > tr:not(:has([colspan]))")[1:]:
                        row_dict = {}
                        for key, value in zip(
                            tab.select(":scope > tr:not(:has([colspan]))")[0].select(
                                "td"
                            ),
                            row.select("td"),
                                    ):
                            row_dict[key.get_text(" ", strip=True)] = value.get_text(
                                " ", strip=True
                            )
                        parsed_tab.append(row_dict)

                        # ПРОВЕРКА
                        if "table>" in str(row):
                            raise ValueError

                # Если заголовки с жирным шрифтом в первом столбце — по столбцам
                elif len(tab.select(":scope > tr:not(:has([colspan])) > td > b")) > 1:
                    for row in tab.select(":scope > tr:not(:has([colspan]))"):
                        index = 0
                        for column in row.select("td")[1:]:
                            if len(parsed_tab) < index + 1:
                                parsed_tab.append(
                                    {
                                        row.select("td")[0].get_text(
                                            " ", strip=True
                                        ): column.get_text(" ", strip=True)
                                    }
                                )
                            else:
                                parsed_tab[index][
                                    row.select("td")[0].get_text(" ", strip=True)
                                ] = column.get_text(" ", strip=True)
                            index += 1

                        # ПРОВЕРКА
                        if "table>" in str(row):
                            raise ValueError

            # Если ничего не подошло — ошибка
            else:
                raise ValueError

        # Если в таблице только другая таблица с шапкой, сохраняем её
        elif len(tab.select(":scope > tr")) == 2 and tab.select(":scope table"):
            parsed_tab = {}
            parsed_tab[
                tab.select_one(":scope [colspan]").get_text(" ", strip=True)
            ] = tab_to_dict(tab.select_one(":scope table"))

        # Если ничего не подошло — ошибка
        elif len(tab.select("tr")) > 0:
            logger.debug("tab_to_dict: unrecognized table structure: %s", str(tab)[:200])
            raise ValueError

    return parsed_tab
