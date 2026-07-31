"""Убрать застрявшее «Текущее состояние» из поля result у московских карточек.

До правки c07cc6f правило result читало «Текущее состояние» первым, поэтому у
карточек mos-gorsud поле заполнялось статусом с датой («Зарегистрировано,
29.07.2026») вместо исхода. result — keyword, так что кардинальность росла без
предела, а агрегация по исходам смешивала две шкалы: у sudrf там «Вынесен
ПРИГОВОР», а до вынесения решения поле пустует.

Переиндексация одна это не чинит: merge_docs обходит поля входящего документа,
а result в нём теперь отсутствует, поэтому старое значение остаётся нетронутым.

Отбор точный, ничего угадывать не надо: застрявший result дословно равен новому
case_state, куда «Текущее состояние» теперь и пишется. Там, где карточка несёт
настоящий исход, переиндексация уже перезаписала result, и условие не сработает.

toLowerCase нужен, потому что result — keyword с нормализатором lowercase, а
case_state замапился динамически и хранит значение как есть.

Запуск:
    python scripts/clean_stale_result.py --dry-run   # только посчитать
    python scripts/clean_stale_result.py             # выполнить
"""
import argparse
import json
import time
import urllib.request
from pathlib import Path

ES = "http://10.13.12.1:9200"
INDEX = "sudata"
ENV = Path(__file__).resolve().parent.parent / ".env"

STALE = {
    "bool": {
        "filter": [{"exists": {"field": "case_state"}}],
        "must": [{"script": {"script": {"source":
            'if (doc["result"].size()==0 || doc["case_state.keyword"].size()==0)'
            ' { return false; }'
            ' return doc["result"].value == doc["case_state.keyword"].value.toLowerCase();'
        }}}],
    }
}


def api_key() -> str:
    for line in ENV.read_text(encoding="utf-8").splitlines():
        if line.startswith("ES_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("ES_API_KEY не найден в .env")


def call(key: str, path: str, body=None, timeout=900):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{ES}/{path}", data=data, method="POST" if data else "GET",
        headers={"Content-Type": "application/json", "Authorization": "ApiKey " + key})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"), strict=False)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--rps", type=int, default=3000, help="троттлинг, чтобы не душить прод")
    args = p.parse_args()

    key = api_key()
    before = call(key, f"{INDEX}/_count", {"query": STALE})["count"]
    print(f"документов с застрявшим статусом в result: {before}")
    if args.dry_run or before == 0:
        return

    task = call(key, f"{INDEX}/_update_by_query?conflicts=proceed"
                     f"&wait_for_completion=false&requests_per_second={args.rps}",
                {"query": STALE, "script": {"source": 'ctx._source.remove("result")'}})["task"]
    print(f"задача: {task}")

    while True:
        time.sleep(20)
        info = call(key, f"_tasks/{task}")
        status = info.get("task", {}).get("status", {})
        done = info.get("completed", False)
        print(f"  обновлено {status.get('updated', 0)} / {status.get('total', '?')}"
              f"  конфликтов {status.get('version_conflicts', 0)}")
        if done:
            # failures смотрим отдельно: updated растёт и при частичных отказах
            failures = info.get("response", {}).get("failures", [])
            print(f"готово. отказов: {len(failures)}")
            for f in failures[:5]:
                print("   ", str(f)[:200])
            break

    after = call(key, f"{INDEX}/_count", {"query": STALE})["count"]
    print(f"осталось совпадений: {after}  (было {before})")


if __name__ == "__main__":
    main()
