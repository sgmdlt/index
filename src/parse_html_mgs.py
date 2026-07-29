"""mos-gorsud card → raw_tables, for pages the MGS crawler archives.

`src/parse_html.py` handles sudrf markup (`div[id^=cont_doc]`,
`ul#case_bookmarks`); mos-gorsud shares none of it. This module is the port of
the crawler's parser, so HTML→raw_tables happens here at index time and the
crawler only has to store pages.

`prepare_doc.py` already understands the shape produced here — it has explicit
handling for Moscow cards nesting movement as {section: [rows]}.
"""
import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

from src.extract_document import to_text

BASE = "https://mos-gorsud.ru"

# UI chrome that sits inside the very nodes we read: clipboard hints on card
# values, help tooltips inside <th>. get_text() would splice them into the data.
_NOISE = ".hint, .copy__copy, .copy__copied, .tooltip, .info-circle-icon"

_DOWNLOAD = "Скачать файл"


@dataclass(frozen=True)
class Card:
    head: dict
    tables: dict
    links: tuple


def _text(node) -> str:
    for junk in node.select(_NOISE):
        junk.decompose()
    # get_text(strip=True) only trims each string's edges; cell values wrap
    # across source lines, so collapse the inner runs too
    return " ".join(node.get_text(" ").replace("\xa0", " ").split())


def _cell(td, base: str) -> str:
    for a in td.select("a[href]"):
        if _DOWNLOAD in a.get_text():
            href = a["href"]
            return href if href.startswith("http") else base + href
    return _text(td)


def _rows(table, base: str) -> list:
    headers = [_text(th) for th in table.select("thead th")]
    rows = []
    for tr in table.select("tbody tr"):
        values = [_cell(td, base) for td in tr.select("td")]
        values += [""] * (len(headers) - len(values))
        rows.append(dict(zip(headers, values)))
    return rows


def _panel(panel, tab_name: str, base: str) -> list:
    """[{<h3 or tab name>: rows}, ...] — walked in document order so a table
    without a preceding h3 still lands under a sensible label."""
    out, label = [], tab_name
    for el in panel.find_all(["h3", "table"]):
        if el.name == "h3":
            label = _text(el)
        else:
            out.append({label: _rows(el, base)})
            label = tab_name
    return out


def parse_card(html: str, base: str = BASE) -> Card:
    soup = BeautifulSoup(html, "lxml")
    head = {}
    for row in soup.select("div.detail-cart div.row"):
        left, right = row.select_one("div.left"), row.select_one("div.right")
        if left is None or right is None:
            continue
        # a head row may link a related case (lower instance); the visible
        # text is just its number, so carry the href alongside it
        related = right.select_one("a[href]")
        value = _text(right)
        if related is not None:
            href = related["href"]
            value += "\n\n" + (href if href.startswith("http") else base + href)
        head[_text(left)] = value

    tab_names = {li["data-tab"]: _text(li)
                 for li in soup.select("ul.tablist__tabs li[data-tab]")}
    tables = {}
    for panel in soup.select("ul.tablist__panels li[data-tab]"):
        key = panel["data-tab"]
        name = tab_names.get(key, key)
        tables[name] = _panel(panel, name, base)

    links = []
    for groups in tables.values():
        for group in groups:
            for rows in group.values():
                for row in rows:
                    for value in row.values():
                        if value.startswith(base) and value not in links:
                            links.append(value)
    return Card(head, tables, tuple(links))


def fill_texts(tables: dict, texts: dict) -> dict:
    """Replace download URLs sitting in cells with their extracted text."""
    return {
        tab: [{name: [{k: texts.get(v, v) for k, v in row.items()} for row in rows]}
              for group in groups for name, rows in group.items()]
        for tab, groups in tables.items()
    }




def _documents(card) -> list:
    """Every downloadable document, labelled by its own опись row.

    card.links alone loses the label, and СУДЕБНЫЕ АКТЫ is keyed by it.
    """
    docs, seen = [], set()
    for groups in card.tables.values():
        for group in groups:
            for rows in group.values():
                for row in rows:
                    link = next((v for v in row.values()
                                 if isinstance(v, str) and v in card.links), None)
                    if link is None or link in seen:
                        continue
                    seen.add(link)
                    docs.append({"link": link,
                                 "name": row.get("Вид документа", ""),
                                 "date": row.get("Дата документа", "")})
    return docs


def _acts(card, texts: dict) -> list:
    """raw_tables["СУДЕБНЫЕ АКТЫ"], the section the _document_text rule reads."""
    acts = []
    for doc in _documents(card):
        text = texts.get(doc["link"])
        if not text:
            continue
        label = f"Судебный акт #{len(acts) + 1}"
        if doc["name"]:
            label += f" ({doc['name']})"
        acts.append({label: text})
    return acts


def fill_raw_tables_mgs(html, documents: dict | None = None) -> dict:
    """The `fill_raw_tables` contract: {"raw_tables": {...}}.

    `documents` maps a document URL to `(bytes, ext)` as archived by the
    crawler — plain bytes are accepted too, and then the format is sniffed.
    Extraction happens here, so improving it never costs another crawl. A
    document that will not extract keeps its link in the cell, which makes the
    loss visible instead of silently blank.
    """
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    card = parse_card(html)
    texts = {}
    for url, value in (documents or {}).items():
        body, ext = value if isinstance(value, tuple) else (value, None)
        extracted = to_text(body, ext)
        if extracted.text:
            texts[url] = extracted.text
    tables = fill_texts(card.tables, texts) if texts else card.tables
    raw = {"Дело": card.head, **tables}
    if texts:
        raw["СУДЕБНЫЕ АКТЫ"] = _acts(card, texts)
    return {"raw_tables": raw}


def is_mgs(html) -> bool:
    """mos-gorsud cards carry detail-cart; sudrf pages never do."""
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    return "detail-cart" in html and "card-sud" in html


_NUMBER_KEYS = ("Номер дела", "Номер жалобы", "Номер заявления", "Номер материала",
                "Номер производства", "Номер дела ~ материала")


def _head_number(head: dict) -> str:
    for key, value in head.items():
        if key.startswith(_NUMBER_KEYS) and "инстанц" not in key:
            return " ".join(value.replace("\n", " ").split())
    return ""


def _states(card) -> list:
    for group in card.tables.get("Движение дела", []):
        if "История состояний" in group:
            return group["История состояний"]
    return []


def _iso(ru_date: str) -> str:
    day, month, year = ru_date.split(".")
    return f"{year}-{month}-{day}"


def card_case_id(card, url: str) -> tuple:
    """(court_code, date_reg_iso, case_number) matching the dump's derivation.

    The head describes the материал, not the дело: its number carries
    "... ∼ М-NNNN/YYYY" (which the id formula would otherwise prefer) and «Дата
    поступления» is the material filing date. The дело is the number before the
    first ~ or (, registered on the second «История состояний» row whenever a
    material exists. Validated at 99.815% against 4 316 live cards.

    The court comes from the URL, never from «Уникальный идентификатор дела» —
    that UID names the court of first instance, so appeals would be keyed to the
    district court instead of Мосгорсуд.
    """
    head = card.head
    court_code = court_from_url(url)
    number = _head_number(head)
    case_number = re.split(r"[~∼(]", number)[0].strip()

    rows = _states(card)
    index = 1 if (re.search(r"[~∼]", number) and len(rows) > 1) else 0
    date_reg = ""
    if rows and rows[index].get("Дата"):
        date_reg = _iso(rows[index]["Дата"])
    elif head.get("Дата поступления"):        # no state table: best available
        date_reg = _iso(head["Дата поступления"])
    return court_code, date_reg, case_number


def card_documents(card) -> list:
    """Every downloadable document, labelled by its own опись row.

    card.links alone loses the label, and СУДЕБНЫЕ АКТЫ is keyed by it.
    """
    docs, seen = [], set()
    for groups in card.tables.values():
        for group in groups:
            for rows in group.values():
                for row in rows:
                    link = next((v for v in row.values()
                                 if isinstance(v, str) and v in card.links), None)
                    if link is None or link in seen:
                        continue
                    seen.add(link)
                    docs.append({"link": link,
                                 "name": row.get("Вид документа", ""),
                                 "date": row.get("Дата документа", "")})
    return docs


def build_record(url: str, card, doc_texts: dict) -> dict:
    court_code, date_reg, number = card_case_id(card, url)
    return {
        "region": "Город Москва",
        "case_number": number,
        "date_reg": date_reg,
        "court_code": court_code,
        "link": url,
        "id_final": custom_id(court_code, date_reg, number),
        "raw_tables": {"Дело": card.head, **fill_texts(card.tables, doc_texts)},
    }




def mgs_identity(html, url: str) -> dict:
    """Identity and provenance for one archived card.

    The crawler cannot supply these — it does not parse — so the manifest is
    enriched here. The rule is the crawler's, validated at 99.815% against
    4 316 live cards: the case number is the head number up to the first ~ or (,
    and the registration date is the second «История состояний» row whenever a
    материал exists, because the head describes the материал rather than the дело.

    link / uuid / court_name / instance all come from the URL, because the card
    markup carries none of them: no head row names the instance, and a card
    never states its own address.
    """
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    card = parse_card(html)
    court_code, date_reg, case_number = card_case_id(card, url)
    # The resolved case type travels under its own key: meta["case_type"] means
    # a short code ('КоАП') that to_document looks up in CASE_TYPE_MAPPING, and
    # mos-gorsud has no such code — it gives the name outright.
    instance, case_type = proceeding_from_url(url)
    return {"court_code": court_code, "case_number": case_number,
            "date_reg": date_reg,
            "id_final": custom_id(court_code, date_reg, case_number),
            "link": url,
            "uuid": uuid_from_url(url),
            "court_name": COURT_NAME.get(court_code, ""),
            "instance": instance,
            "case_type_name": case_type}


# --- ported so this module stands alone -------------------------------------
# Court code by URL alias, from 9 174 real records across all 36 Moscow courts.
# Never read it from «Уникальный идентификатор дела»: that UID names the court
# of FIRST instance, so appeals would key to the district court. Two courts were
# respelled ё→е and the site 301s the old form, so both spellings are mapped.
_ALIAS = re.compile(r"https://mos-gorsud\.ru/(?:rs/([a-z]+)|(mgs))/")

COURT_BY_ALIAS = {
    "babushkinskij": "77RS0001",
    "basmannyj": "77RS0002",
    "butyrskij": "77RS0003",
    "cheremushkinskij": "77RS0032",   # site's current spelling
    "chertanovskij": "77RS0033",
    "cheryomushkinskij": "77RS0032",  # dump's spelling; the site 301s it
    "dorogomilovskij": "77RS0006",
    "gagarinskij": "77RS0004",
    "golovinskij": "77RS0005",
    "hamovnicheskij": "77RS0030",
    "horoshevskij": "77RS0031",
    "izmajlovskij": "77RS0010",
    "koptevskij": "77RS0011",
    "kuncevskij": "77RS0013",
    "kuzminskij": "77RS0012",
    "lefortovskij": "77RS0014",
    "lyublinskij": "77RS0015",
    "meshchanskij": "77RS0016",
    "mgs": "77OS0000",
    "nagatinskij": "77RS0017",
    "nikulinskij": "77RS0018",
    "ostankinskij": "77RS0019",
    "perovskij": "77RS0020",
    "preobrazhenskij": "77RS0022",
    "presnenskij": "77RS0021",
    "savelovskij": "77RS0023",   # site's current spelling
    "savyolovskij": "77RS0023",  # dump's spelling; the site 301s it
    "shcherbinskij": "77RS0034",
    "simonovskij": "77RS0024",
    "solncevskij": "77RS0025",
    "taganskij": "77RS0026",
    "timiryazevskij": "77RS0028",
    "troickij": "77RS0035",
    "tushinskij": "77RS0029",
    "tverskoj": "77RS0027",
    "zamoskvoreckij": "77RS0007",
    "zelenogradskij": "77RS0008",
    "zyuzinskij": "77RS0009",
}


def court_from_url(url: str) -> str:
    match = _ALIAS.search(url or "")
    if not match:
        return ""
    return COURT_BY_ALIAS.get(match.group(1) or match.group(2), "")


# Court names as already spelled in the index, taken from the aggregation over
# every Moscow document there. Two courts appear in both ё and е spellings; the
# dominant one wins, so re-indexing a case does not change its court name.
COURT_NAME = {
    "77OS0000": "Московский городской суд",
    "77RS0001": "Бабушкинский районный суд",
    "77RS0002": "Басманный районный суд",
    "77RS0003": "Бутырский районный суд",
    "77RS0004": "Гагаринский районный суд",
    "77RS0005": "Головинский районный суд",
    "77RS0006": "Дорогомиловский районный суд",
    "77RS0007": "Замоскворецкий районный суд",
    "77RS0008": "Зеленоградский районный суд",
    "77RS0009": "Зюзинский районный суд",
    "77RS0010": "Измайловский районный суд",
    "77RS0011": "Коптевский районный суд",
    "77RS0012": "Кузьминский районный суд",
    "77RS0013": "Кунцевский районный суд",
    "77RS0014": "Лефортовский районный суд",
    "77RS0015": "Люблинский районный суд",
    "77RS0016": "Мещанский районный суд",
    "77RS0017": "Нагатинский районный суд",
    "77RS0018": "Никулинский районный суд",
    "77RS0019": "Останкинский районный суд",
    "77RS0020": "Перовский районный суд",
    "77RS0021": "Пресненский районный суд",
    "77RS0022": "Преображенский районный суд",
    "77RS0023": "Савёловский районный суд",
    "77RS0024": "Симоновский районный суд",
    "77RS0025": "Солнцевский районный суд",
    "77RS0026": "Таганский районный суд",
    "77RS0027": "Тверской районный суд",
    "77RS0028": "Тимирязевский районный суд",
    "77RS0029": "Тушинский районный суд",
    "77RS0030": "Хамовнический районный суд",
    "77RS0031": "Хорошевский районный суд",
    "77RS0032": "Черемушкинский районный суд",
    "77RS0033": "Чертановский районный суд",
    "77RS0034": "Щербинский районный суд",
    "77RS0035": "Троицкий районный суд",
}

_UUID_PATH = re.compile(r"/details/([0-9a-f]{8}-[0-9a-f-]+)")


def uuid_from_url(url: str) -> str:
    """mos-gorsud puts the case uuid last in the path, not in a query string."""
    match = _UUID_PATH.search(url or "")
    return match.group(1) if match else ""


_SEGMENT = re.compile(r"/services/cases/([a-z0-9-]+)/details/")

# (instance, case_type) per URL segment — the only place either is stated, since
# no head row on the card names the instance.
#
# Read off the site's own classification rather than guessed from the names:
# every segment below was matched to documents already in the index that carry
# the instance the search API reported, sample size in the comment. Two would
# have been guessed wrong — `review-not-yet` is Кассация, not Апелляция, and
# `magistrate-cassation-*` is Надзор despite the name.
_FIRST, _APPEAL = "Первая инстанция", "Апелляция"
_CASSATION, _SUPERVISION = "Кассация", "Надзор"
_CIVIL, _CRIMINAL = "Гражданское дело", "Уголовное дело"
_ADMIN_CASE = "Административное дело"
_ADMIN_OFFENCE = "Дело об административном правонарушении"
_MATERIALS = "Производство по материалам"

PROCEEDING_BY_SEGMENT = {
    # measured
    "admin": (_FIRST, _ADMIN_OFFENCE),                    # 12
    "admin-exec": (_FIRST, _MATERIALS),                   # 21
    "appeal-admin": (_APPEAL, _ADMIN_CASE),               # 1
    "appeal-civil": (_APPEAL, _CIVIL),                    # 378
    "appeal-kas": (_APPEAL, _ADMIN_CASE),                 # 1
    "cassation-civil": (_CASSATION, _CIVIL),              # 1
    "cassation-criminal": (_CASSATION, _CRIMINAL),        # 1
    "civil": (_FIRST, _CIVIL),                            # 48
    "claim-admin": (_FIRST, _ADMIN_CASE),                 # 76
    "claim-civil": (_FIRST, _CIVIL),                      # 78
    "claim-criminal": (_FIRST, _CRIMINAL),                # 2
    "criminal": (_FIRST, _CRIMINAL),                      # 6
    "criminal-materials": (_FIRST, _MATERIALS),           # 108
    "first-admin": (_FIRST, _ADMIN_CASE),                 # 18
    "first-admin-exec": (_FIRST, _MATERIALS),             # 4
    "first-civil": (_FIRST, _CIVIL),                      # 5
    "first-civil-exec": (_FIRST, _MATERIALS),             # 7
    "first-criminal": (_FIRST, _CRIMINAL),                # 2
    "first-criminal-exec": (_FIRST, _MATERIALS),          # 19
    "kas": (_FIRST, _ADMIN_CASE),                         # 42
    "magistrate-cassation-civil": (_SUPERVISION, _CIVIL),  # 3
    "review-not-yet": (_CASSATION, _ADMIN_OFFENCE),       # 159
    # no document in the index to match against; read off the sibling segments,
    # which agree unanimously within each family
    "appeal-criminal": (_APPEAL, _CRIMINAL),
    "civil-exec": (_FIRST, _MATERIALS),
    "magistrate-cassation-admin": (_SUPERVISION, _ADMIN_OFFENCE),
    "magistrate-cassation-criminal": (_SUPERVISION, _CRIMINAL),
    "review-supervision": (_SUPERVISION, _ADMIN_OFFENCE),
}


def proceeding_from_url(url: str) -> tuple:
    """(instance, case_type). Empty pair for a segment we have no reading for —
    better an absent field than a wrong one."""
    match = _SEGMENT.search(url or "")
    if not match:
        return "", ""
    return PROCEEDING_BY_SEGMENT.get(match.group(1), ("", ""))


_SANITIZE = str.maketrans({" ": "", "/": "-", "\\": "-",
                           "(": "_", ")": "_", "[": "_", "]": "_",
                           "{": "_", "}": "_"})


def custom_id(court_code: str, date_reg: str, case_number: str) -> str:
    id_date = date_reg or "NONE"
    tail = case_number
    if id_date != "NONE":
        year = re.escape(date_reg[:4])
        if re.search(r"[/\\|] ?" + year, case_number):
            tail = re.findall(r"([^ ~∼(\[{]+) ?[/\\|] ?" + year, case_number)[-1]
        elif re.search(r"[/\\|] ?(?:20|19)", case_number):
            tail = re.findall(r"([^ ~∼(\[{]+) ?[/\\|] ?(?:20|19)", case_number)[-1]
    return f"{court_code}_{id_date}_{tail}".translate(_SANITIZE)
