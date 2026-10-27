# Stable case id — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Один детерминированный id дела во всех репо: формула «первый выданный номер» + правило группы по `uuid` (побеждает самая ранняя дата), пересчёт id для любого источника и скрипт миграции `sudata`.

**Architecture:** Формула — чистая функция `build_case_id` в courts (источник истины) и побайтовая копия в index; обе проверяются одним эталоном `case_id_golden.json`. Выбор id в группе одного `uuid` — чистая функция в index; `BulkIndexer` применяет её перед записью, миграционный скрипт — к существующему индексу. courts меняет только формулу и поиск уже скачанной карточки в `watch`.

**Tech Stack:** Python 3.13, uv, pytest, elasticsearch-py (index), asyncpg/SQLAlchemy (courts), прямые HTTP-вызовы ES в `scripts/` (как `dedup_bracket_ids.py`).

**Spec:** `docs/superpowers/specs/2026-10-27-stable-case-id-design.md`

## Global Constraints

- Тело `build_case_id` в `courts/src/courts/parser/search_result_parser.py` и `index/src/utils/case_id.py` побайтово одинаковое (проверка: `diff` из Task 2).
- Сигнатура `build_case_id(case_number: str, date_reg: str, court_id: str) -> str | None`, `date_reg` в `дд.мм.гггг` — не меняется.
- id никогда не содержит пробела, `(`, `~`, ` [`; не найден номер → `…_ID_NOT_PARSED`; пустой `date_reg` → `None`.
- МГС (ссылка на `mos-gorsud.ru`) не пересчитывается и не участвует в правиле группы.
- Схему/маппинг ES не меняем; поля при неудаче не обнуляем.
- Скрипты против `sudata` запускает пользователь; по умолчанию — dry-run.
- Комментарии и тексты логов — по-русски, как в окружающем коде.

## Review Focus

- Оренбургские номера `1[2]-50/2022`, `(М[1]-1/2013)` — `[` внутри номера не кассация → эталон Task 1.
- МГС-запись (JSONL или манифест с `mos-gorsud`) сохраняет свой `id_final` → тест Task 3.
- Дата в JSONL как `2008-12-09T00:00:00Z` → тот же id, что из `09.12.2008` → тест Task 3.
- Удаление близнеца не должно случиться, если запись целевого документа отклонена → тест Task 5.
- В пачке два элемента с одним `uuid` и разными датами → оба ложатся на один id → тест Task 5.

## Execution setup

- index: worktree `feat/stable-case-id` от `feat/mgs-card-parser` (там `6f45dfb`, `523c985`; `main` отстаёт на 44 коммита).
- courts: worktree `feat/stable-case-id` от `main`.
- Беads-эпик `courts-271g` (база в courts): подзадача на каждый Task, `parent-child` к эпику, `blocks` по порядку 1→2→3→4→5→6; Task 7 независим.

---

### Task 1: Формула «первый выданный номер» в courts + эталон

**Files:**
- Modify: `courts/src/courts/parser/search_result_parser.py:78-116` (`build_case_id`)
- Create: `courts/tests/fixtures/case_id_golden.json`
- Create: `courts/tests/test_case_id_golden.py`

**Interfaces:**
- Produces: `build_case_id(case_number, date_reg, court_id) -> str | None` (контракт прежний), файл `case_id_golden.json` — список `{"case_number", "date_reg", "court_id", "expected"}`, `expected` может быть `null`.
- Все вызывающие в courts (`downloader/watch.py`, `scripts/backfill.py`, `scripts/backfill_downloaded_cases.py`, `scripts/fix_case_ids.py`) уже импортируют её из `search_result_parser` — проверено, правок не нужно.

- [ ] **Step 1: Создать `case_id_golden.json`** с этими строками (`court_id` указан там, где отличается от предыдущей строки группы):

| case_number | date_reg | court_id | expected |
|---|---|---|---|
| `1-76/2016` | 30.03.2016 | 61RS0018 | `61RS0018_2016-03-30_1-76` |
| `1-76/2016` | `` | 61RS0018 | `null` |
| `12 ~ 2-345/2016` | 30.03.2016 | 61RS0018 | `61RS0018_2016-03-30_2-345` |
| `(abc)` | 30.03.2016 | 61RS0018 | `61RS0018_2016-03-30_ID_NOT_PARSED` |
| `Материалы дела` | 01.01.2010 | 61RS0018 | `61RS0018_2010-01-01_ID_NOT_PARSED` |
| `2-408/2026 ~ М-196/2026` | 18.03.2026 | 24RS0044 | `24RS0044_2026-03-18_М-196` |
| `2-1043/2009 ~ Материалы дела` | 13.03.2009 | 24RS0032 | `24RS0032_2009-03-13_2-1043` |
| `2-2955/2010 ~ Вх. № -2098/2010` | 18.06.2010 | 47RS0005 | `47RS0005_2010-06-18_-2098` |
| `2а-2/21/2025 ~ М-2/6/2025` | 28.12.2024 | 43RS0013 | `43RS0013_2024-12-28_М-2-6` |
| `2-17/2026 (2-1260/2025;) ~ М-46/2025` | 13.01.2025 | 66RS0002 | `66RS0002_2025-01-13_М-46` |
| `8Г-7981/2026` | 27.07.2026 | 26KJ0005 | `26KJ0005_2026-07-27_8Г-7981` |
| `8Г-7981/2026 [88-8481/2026]` | 27.07.2026 | 26KJ0005 | `26KJ0005_2026-07-27_8Г-7981` |
| `4У-92/2013 [44У-44/2013]` | 12.08.2013 | 06OS0000 | `06OS0000_2013-08-12_4У-92` |
| `8Г-10025/2020 [88-1265/2021 - (88-10622/2020)]` | 30.11.2020 | 25KJ0009 | `25KJ0009_2020-11-30_8Г-10025` |
| `4У-2188/2016 [44У-9/2017 - (44У-135/2016)]` | 28.11.2016 | 78OS0000 | `78OS0000_2016-11-28_4У-2188` |
| `4У-48/2017 - (4У-2189/2016) [44У-19/2017]` | 29.11.2016 | 78OS0000 | `78OS0000_2016-11-29_4У-2189` |
| `4У-0/2011 [44У-113/2011]` | 01.01.1900 | 33OS0000 | `33OS0000_2011-01-01_44У-113` |
| `4У-0/2010 [44У-51/2010]` | 28.04.2010 | 78OS0000 | `78OS0000_2010-04-28_44У-51` |
| `4У-140/2011` | 01.01.1900 | 33OS0000 | `33OS0000_1900-01-01_4У-140` |
| `4У-140/2011 [44У-7/2011]` | 01.01.1900 | 33OS0000 | `33OS0000_1900-01-01_4У-140` |
| `4У-47/2017 - (4У-2186/2016)` | 25.11.2016 | 78OS0000 | `78OS0000_2016-11-25_4У-2186` |
| `33-83/2012 (33-19377/2011;)` | 10.01.2012 | 78OS0000 | `78OS0000_2012-01-10_33-83` |
| `22-344/2011 (22-8552/2010;)` | 21.12.2010 | 78OS0000 | `78OS0000_2010-12-21_22-8552` |
| `2-1/2011 (2-2/2010;)` | 09.12.2008 | 78OS0000 | `78OS0000_2008-12-09_2-2` |
| `2-1/2012 (2-3/2011; 2-12/2010;)` | 21.09.2009 | 78OS0000 | `78OS0000_2009-09-21_2-12` |
| `4/17-578/2013 (4/17-81/2012;)` | 30.01.2011 | 03RS0002 | `03RS0002_2011-01-30_4-17-81` |
| `1[2]-50/2022` | 30.05.2022 | 56RS0038 | `56RS0038_2022-05-30_1[2]-50` |
| `1[1]-38/2013 (М[1]-1/2013)` | 09.01.2013 | 56RS0038 | `56RS0038_2013-01-09_М[1]-1` |

- [ ] **Step 2: Написать тест**

```python
# tests/test_case_id_golden.py
GOLDEN = json.loads((Path(__file__).parent / "fixtures/case_id_golden.json").read_text(encoding="utf-8"))

@pytest.mark.parametrize("row", GOLDEN, ids=lambda r: r["case_number"])
def test_build_case_id_golden(row):
    assert build_case_id(row["case_number"], row["date_reg"], row["court_id"]) == row["expected"]
```

- [ ] **Step 3: Запустить — падает**

Run: `uv run pytest tests/test_case_id_golden.py -q` (в courts)
Expected: FAIL как минимум на `2-1043/2009 ~ Материалы дела`, `4У-48/2017 - (4У-2189/2016) [44У-19/2017]`, `2-1/2011 (2-2/2010;)`, `2-1/2012 (…)`, `Материалы дела`.

- [ ] **Step 4: Переписать `build_case_id` по таблице правил спеки (ветки 0–7)**

Порядок: пустая дата → `None`; заглушка `-0 [...]` (как сейчас); тильда — если после первого `~`/`∼` есть пара «номер/год», ключ из неё, иначе работаем с частью до `~`; кассация — разрезать по первому ` [` (с пробелом) и дальше разбирать только голову; из оставшейся строки собрать пары «номер/год» (номер может содержать `/`, `[`, `]`, но не пробел, `(`, `)`, `;`, `,`, `~`); есть пара с годом регистрации — последняя такая; иначе есть пары — с минимальным годом, при равенстве последняя; иначе `ID_NOT_PARSED`. В id `/` → `-`. Комментарий над каждой веткой — один пример номера, как сейчас.

- [ ] **Step 5: Прогнать эталон и старые тесты парсера**

Run: `uv run pytest tests/test_case_id_golden.py tests/test_search_parser.py tests/test_downloader_watch.py -q`
Expected: PASS. Если старый тест противоречит эталону — эталон прав (он из спеки); тест поправить и указать это в сообщении коммита.

- [ ] **Step 6: Commit** (courts)

```bash
git add src/courts/parser/search_result_parser.py tests/fixtures/case_id_golden.json tests/test_case_id_golden.py
git commit -m "fix(parser): ключ дела — первый выданный номер; эталон case_id_golden (courts-271g)"
```

### Task 2: Копия формулы и эталона в index

**Files:**
- Modify: `index/src/utils/case_id.py` (тело `build_case_id`)
- Create: `index/tests/fixtures/case_id_golden.json` (копия из courts)
- Create: `index/tests/test_case_id_golden.py` (тот же тест)
- Modify: `index/tests/test_build_case_id.py` (только если ожидания противоречат эталону)

**Interfaces:**
- Consumes: Task 1 — `build_case_id` и `case_id_golden.json` из courts.
- Produces: `src.utils.case_id.build_case_id` идентична courts.

- [ ] **Step 1: Скопировать эталон и тест**, путь фикстуры — `tests/fixtures/case_id_golden.json`.
- [ ] **Step 2: Run** `uv run pytest tests/test_case_id_golden.py -q` → FAIL (старая формула).
- [ ] **Step 3: Скопировать тело `build_case_id` из courts** без изменений; докстринг модуля не трогать.
- [ ] **Step 4: Проверить идентичность и тесты**

Run:
```bash
diff <(sed -n '/^def build_case_id/,/^    return case_id/p' ../courts/src/courts/parser/search_result_parser.py) <(sed -n '/^def build_case_id/,/^    return case_id/p' src/utils/case_id.py) && cmp ../courts/tests/fixtures/case_id_golden.json tests/fixtures/case_id_golden.json && uv run pytest tests/test_case_id_golden.py tests/test_build_case_id.py tests/test_to_document_id.py tests/test_dedup_bracket_ids.py -q
```
(пути courts — от worktree courts). Expected: diff пуст, cmp молчит, PASS.

- [ ] **Step 5: Commit** — `fix(case_id): порт формулы «первый выданный номер» из courts (courts-271g)`.

### Task 3: Пересчёт id для любого источника

**Files:**
- Modify: `index/src/utils/case_id.py` (новая функция ниже `build_case_id`)
- Modify: `index/src/to_document.py:45-60`
- Modify: `index/src/main.py:638-646` (`_derive_id_final`)
- Test: `index/tests/test_to_document_id.py`

**Interfaces:**
- Consumes: Task 2 `build_case_id`.
- Produces: `case_id_from_fields(case_number: str | None, date_reg: str | None, court: str | None, link: str | None) -> str | None` — `None` для МГС (`"mos-gorsud" in link`), при пустом номере/дате/суде; `date_reg` принимает `дд.мм.гггг`, `гггг-мм-дд` и `гггг-мм-ддT…`.

- [ ] **Step 1: Тесты**

```python
def test_jsonl_fields_recompute_over_stale_id_final():
    meta = {"case_number": "2-1/2011 (2-2/2010;)", "date_reg": "2008-12-09T00:00:00Z",
            "court_code": "78OS0000", "id_final": "78OS0000_2008-12-09_2-1"}
    assert to_document("", "x", meta, [], [])["id_final"] == "78OS0000_2008-12-09_2-2"

def test_iso_and_ru_dates_give_one_id():
    assert case_id_from_fields("1-76/2016", "2016-03-30", "61RS0018", "") == \
           case_id_from_fields("1-76/2016", "30.03.2016", "61RS0018", "") == "61RS0018_2016-03-30_1-76"

def test_mgs_keeps_its_own_id():
    meta = {"case_number": "02-3415/2025", "date_reg": "2025-01-14", "court_code": "77RS0018",
            "link": "https://mos-gorsud.ru/rs/x/services/cases/civil/details/abc",
            "id_final": "77RS0018_2025-01-14_02-3415"}
    assert to_document("", "x", meta, [], [])["id_final"] == "77RS0018_2025-01-14_02-3415"

def test_derive_id_final_recomputes_jsonl_record():
    data = {"case_number": "2-1/2011 (2-2/2010;)", "date_reg": "2008-12-09",
            "court_code": "78OS0000", "id_final": "78OS0000_2008-12-09_2-1"}
    assert _derive_id_final(data) == "78OS0000_2008-12-09_2-2"
```

- [ ] **Step 2: Run** `uv run pytest tests/test_to_document_id.py -q` → FAIL (ImportError / старый id).
- [ ] **Step 3: Реализовать `case_id_from_fields`**; `to_document` вызывает её с `meta.get("court_id") or meta.get("court_code")` и `meta.get("link")`, при `None` — прежний fallback `fix_none_id(meta.get("ID") or meta.get("id_final"), …)`; `_derive_id_final` — `case_id_from_fields(...) or data.get("id_final") or <прежняя склейка>`.
- [ ] **Step 4: Run** `uv run pytest -q` → PASS (весь набор).
- [ ] **Step 5: Commit** — `fix(index): id пересчитывается из полей и для JSONL; МГС не трогаем (courts-271g)`.

### Task 4: Правило группы одного `uuid`

**Files:**
- Create: `index/src/uuid_group.py`
- Test: `index/tests/test_uuid_group.py`

**Interfaces:**
- Produces:
  - `in_group_scope(doc: dict) -> bool` — есть `uuid` и `link` не содержит `mos-gorsud`.
  - `pick_group_id(members: list[tuple[str, str]]) -> str` — `members` = `[(_id, version_ts), …]`; самая ранняя дата из `_id` (второй сегмент `court_YYYY-MM-DD_…`); при равной дате — самый свежий `version_ts`; затем лексикографический минимум `_id`.

- [ ] **Step 1: Тесты**

```python
def test_earliest_date_wins():
    assert pick_group_id([("50RS0030_2026-05-20_5-225", "2026-06-25"),
                          ("50RS0030_2026-05-19_5-225", "2026-09-28")]) == "50RS0030_2026-05-19_5-225"

def test_same_date_freshest_record_wins():
    assert pick_group_id([("42RS0008_2025-08-21_2-1898", "2024-01-10"),
                          ("42RS0008_2025-08-21_М-1591", "2026-10-01")]) == "42RS0008_2025-08-21_М-1591"

def test_order_does_not_matter():
    m = [("A_2026-06-24_1", "2026-06-25"), ("A_2026-06-29_1", "2026-10-01"), ("A_2026-06-24_2", "2026-06-25")]
    assert pick_group_id(m) == pick_group_id(list(reversed(m))) == "A_2026-06-24_1"

def test_scope_excludes_mgs_and_no_uuid():
    assert in_group_scope({"uuid": "u", "link": "https://x.sudrf.ru/?case_uid=u"})
    assert not in_group_scope({"uuid": "u", "link": "https://mos-gorsud.ru/x"})
    assert not in_group_scope({"link": "https://x.sudrf.ru/?_uid="})
```

- [ ] **Step 2: Run** `uv run pytest tests/test_uuid_group.py -q` → FAIL (нет модуля).
- [ ] **Step 3: Реализовать** — `min` по ключу `(date, invert(version_ts), _id)`; для «свежее раньше» сортировать по `version_ts` убыв. внутри одной даты (двухпроходно или ключ с отрицанием через сортировку — на выбор).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(index): правило группы одного uuid — самая ранняя дата (courts-271g)`.

### Task 5: Слияние по `uuid` в `BulkIndexer`

**Files:**
- Modify: `index/src/bulk_indexer.py` (`_do_bulk`, новые функции рядом с `mget`/`apply_merge`)
- Test: `index/tests/test_bulk_uuid_fold.py`

**Interfaces:**
- Consumes: Task 4 `in_group_scope`, `pick_group_id`; существующие `mget`, `apply_merge`, `merge_docs`, `build_bulk_index_ops`.
- Produces:
  - `uuid_twins(es_client, index: str, uuids: list[str]) -> dict[str, list[dict]]` — `{uuid: [{"_id", "_source"}…]}` из одного `search` с `terms` по `uuid`.
  - `regroup(batch: list[dict], twins: dict[str, list[dict]]) -> tuple[list[dict], dict[str, dict], list[str]]` — чистая: возвращает пачку с переписанными `_id` (по `pick_group_id`, входящие записи — кандидаты со своим `version_ts`), `{target_id: слитый_source}` для подмешивания к результату `mget`, и список `_id` близнецов на удаление. Близнецы вливаются по возрастанию `version_ts` через `merge_docs`.

- [ ] **Step 0:** Context7: `resolve-library-id elasticsearch-py` → `query-docs` «search terms query size _source», «bulk delete action response».
- [ ] **Step 1: Тесты** (FakeES по образцу `tests/test_bulk_failure_reporting.py`, с `search`, `mget`, `bulk`, запоминает операции)

```python
def test_incoming_lands_on_earliest_date_and_twin_is_deleted():
    # в ES: 50RS0030_2026-05-20_5-225 (uuid u); приходит 50RS0030_2026-05-19_5-225 (uuid u)
    ...
    assert index_ids(es.bulk_calls[0]) == ["50RS0030_2026-05-19_5-225"]
    assert delete_ids(es.bulk_calls[1]) == ["50RS0030_2026-05-20_5-225"]
    assert merged_source["html_files"] == ["old.html", "new.html"]   # близнец влит

def test_twin_not_deleted_when_target_write_rejected():
    # bulk #1 отвечает error на целевой id → второго bulk нет
    assert len(es.bulk_calls) == 1

def test_two_items_same_uuid_in_one_batch_share_an_id():
    assert index_ids(es.bulk_calls[0]) == ["A_2026-06-24_1"]

def test_search_failure_fails_the_batch():
    # search бросает → bulk не вызывается, _report([], ids, "uuid search failed: …")
    ...

def test_mgs_and_no_uuid_items_skip_the_search():
    assert es.search_calls == 0
```

- [ ] **Step 2: Run** `uv run pytest tests/test_bulk_uuid_fold.py -q` → FAIL.
- [ ] **Step 3: Реализовать.** В `_do_bulk` до `mget`: собрать `uuid` элементов в scope → `uuid_twins` (исключение → отказ пачки, как у `mget`) → `regroup` → `mget` по новым `_id` → подмешать слитые близнецы как базу (`existing_by_id[target] = merge_docs(existing_by_id.get(target), twin_merged)` по `version_ts`) → `apply_merge` → bulk `index`. Затем отдельный bulk `delete` только для близнецов, чей целевой `_id` записан без ошибки. Ответы `delete` не попадают в `indexed_ids/failed_ids`; ошибка удаления (кроме `not_found`) — `_log_failure`.
- [ ] **Step 4: Run** `uv run pytest -q` → PASS (весь набор, включая `test_bulk_failure_reporting.py`).
- [ ] **Step 5: Commit** — `feat(bulk): слияние документов одного uuid перед записью (courts-271g)`.

### Task 6: Скрипт миграции `sudata`

**Files:**
- Create: `index/scripts/rekey_case_ids.py`
- Test: `index/tests/test_rekey_case_ids.py`

**Interfaces:**
- Consumes: Task 3 `case_id_from_fields`, Task 4 `pick_group_id`/`in_group_scope`, `merge_docs`; I/O-хелперы `api_key`, `call`, `bulk`, `fetch_by_ids` из `scripts/dedup_bracket_ids.py` (импорт, не копия).
- Produces (чистое ядро):
  - `formula_id(row: dict) -> str | None` — `row` из выгрузки `{_id, case_number, date_reg (YYYY-MM-DD), court_code, uuid, link}`.
  - `plan_rekey(rows: list[dict], existing: dict[str, dict]) -> list[dict]` — для каждой строки с `formula_id ≠ _id`: `{"op": "move" | "fold" | "manual", "from": _id, "to": target}`; `fold`, если на `to` уже есть документ с тем же `uuid` (или у обоих нет `uuid`); `manual`, если `uuid` разные.
  - `plan_groups(rows: list[dict]) -> list[dict]` — группы одного `uuid` в scope с >1 `_id`: `{"op": "fold", "from": id, "to": pick_group_id(...)}` для каждого не-целевого.

CLI (как `dedup_bracket_ids`): `--export OUT.jsonl` (scroll по `sudata`, `docvalue_fields`: `case_number`, `date_reg` с `format: yyyy-MM-dd`, `court_code`, `uuid`, `link` — без `_source`; пишет строки, где `formula_id ≠ _id`, и все строки с `uuid` для группового прохода), `--check` (без записи: сводка по `op`), `--apply` (выполняет), `--groups` (второй проход). `--apply` удаляет старый `_id` с `if_seq_no`/`if_primary_term`, полученными при чтении, и только после успешной записи целевого; конфликт → строка в `*_retry.jsonl`. `manual` → `*_manual.jsonl`, не трогаем. МГС пропускаются.

- [ ] **Step 1: Тесты ядра**

```python
def test_renumbered_case_moves_to_earliest_number():
    row = {"_id": "78OS0000_2008-12-09_2-1", "case_number": "2-1/2011 (2-2/2010;)",
           "date_reg": "2008-12-09", "court_code": "78OS0000", "uuid": "a5f1", "link": ""}
    twin = {"78OS0000_2008-12-09_2-2": {"uuid": "a5f1"}}
    assert plan_rekey([row], twin) == [{"op": "fold", "from": "78OS0000_2008-12-09_2-1", "to": "78OS0000_2008-12-09_2-2"}]

def test_no_twin_is_a_move():
    assert plan_rekey([row], {})[0]["op"] == "move"

def test_twin_with_other_uuid_goes_manual():
    assert plan_rekey([row], {"78OS0000_2008-12-09_2-2": {"uuid": "zzz"}})[0]["op"] == "manual"

def test_formula_id_unchanged_is_skipped():
    assert plan_rekey([{**row, "_id": "78OS0000_2008-12-09_2-2"}], {}) == []

def test_mgs_rows_are_skipped():
    assert plan_rekey([{**row, "link": "https://mos-gorsud.ru/x"}], {}) == []

def test_groups_fold_into_earliest_date():
    rows = [{"_id": "50RS0030_2026-05-20_5-225", "uuid": "u", "link": "", "version_ts": "2026-06-25"},
            {"_id": "50RS0030_2026-05-19_5-225", "uuid": "u", "link": "", "version_ts": "2026-09-28"}]
    assert plan_groups(rows) == [{"op": "fold", "from": "50RS0030_2026-05-20_5-225", "to": "50RS0030_2026-05-19_5-225"}]
```
(`plan_groups` нуждается в `version_ts` — добавить его в `docvalue_fields` выгрузки.)

- [ ] **Step 2: Run** `uv run pytest tests/test_rekey_case_ids.py -q` → FAIL.
- [ ] **Step 3: Реализовать ядро и CLI.** Докстринг модуля — порядок запуска для пользователя (export → check → apply → groups → контрольная выборка), как в `dedup_bracket_ids.py`.
- [ ] **Step 4: Run** `uv run pytest -q` → PASS; `uv run python -m scripts.rekey_case_ids --help` печатает CLI.
- [ ] **Step 5: Commit** — `feat(scripts): rekey_case_ids — перенос на формульный id и слияние групп uuid (courts-271g)`.

### Task 7: courts `watch` — поиск скачанной карточки без точной даты

**Files:**
- Modify: `courts/src/courts/database.py:433-445` (`CASE_BY_UID_SQL`, `load_case_by_uid`)
- Test: `courts/tests/test_write_downloaded_watch.py::test_load_case_by_uid_finds_search_row`

**Interfaces:**
- Produces: `load_case_by_uid(court_id: str, date_reg: date, uid: str)` — сигнатура прежняя; ищет в окне `date_reg ± 90 дней` (использует индекс `ix_downloaded_cases_court_date_type`), ближайшая по дате, затем свежая по `downloaded_at`.

- [ ] **Step 1: Поменять ожидания теста**: `load_case_by_uid("99TT0001", date(2026, 1, 2), uid)` находит ту же строку; `date(2026, 6, 1)` (вне окна) → `None`.
- [ ] **Step 2: Run** `uv run pytest tests/test_write_downloaded_watch.py -q` → FAIL (нужен Postgres тестов courts, как в conftest).
- [ ] **Step 3: SQL**: `WHERE court_id = $1 AND date_reg BETWEEN $2::date - 90 AND $2::date + 90 AND url ILIKE …  ORDER BY abs($2::date - date_reg), downloaded_at DESC LIMIT 1`.
- [ ] **Step 4: Run** `uv run pytest tests/test_write_downloaded_watch.py tests/test_downloader_watch.py -q` → PASS.
- [ ] **Step 5: Commit** — `fix(watch): карточку ищем по uid в окне ±90 дней — суд правит дату (courts-271g)`.

---

## Раскатка (пользователь, после мержа)

1. courts: `python -m courts.scripts.backfill_downloaded_cases` (dry-run) → посмотреть число изменённых `case_id_new` → `--apply` — **до** деплоя новой формулы в поисковик. Строки, у которых нет манифеста на диске (WARC-эпоха), не обновятся: такие карточки скачаются повторно один раз — оценить по dry-run.
2. Деплой courts и index.
3. `python -m scripts.rekey_case_ids --export rekey.jsonl` → `--check` → `--apply` → `--groups`.
4. Контроль: доля `_id ≠ формула` и групп одного `uuid` в случайной выборке ≈ 0.
