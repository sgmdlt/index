"""Свести два названия ХМАО к одному: «… - Югра (Тюменская область)» → «… - Югра».

Регион приходит в манифест из courts.region скрейпера, а тот — из
court_list.csv. Там районные суды 86RS* были записаны с «(Тюменская область)»,
а областной 86OS0000 — без, поэтому в индексе один регион жил под двумя
значениями. В court_list.csv это исправлено; здесь правятся уже
проиндексированные карточки.

region — keyword без нормализатора, поэтому term по точному значению.

Запуск:
    python scripts/fix_hmao_region.py --dry-run   # только посчитать
    python scripts/fix_hmao_region.py             # выполнить
"""
import argparse
import json
import time
import urllib.request
from pathlib import Path

ES = "http://10.13.12.1:9200"
INDEX = "sudata"
ENV = Path(__file__).resolve().parent.parent / ".env"

OLD = "Ханты-Мансийский автономный округ - Югра (Тюменская область)"
NEW = "Ханты-Мансийский автономный округ - Югра"
QUERY = {"term": {"region": OLD}}


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
    before = call(key, f"{INDEX}/_count", {"query": QUERY})["count"]
    print(f"документов с «(Тюменская область)»: {before}")
    if args.dry_run or before == 0:
        return

    task = call(key, f"{INDEX}/_update_by_query?conflicts=proceed"
                     f"&wait_for_completion=false&requests_per_second={args.rps}",
                {"query": QUERY, "script": {"source": "ctx._source.region = params.new",
                                            "params": {"new": NEW}}})["task"]
    print(f"задача: {task}")

    # Как в clean_stale_result: _tasks ключу недоступен (403), следим счётчиком.
    remaining, stalled = before, 0
    while True:
        time.sleep(20)
        previous, remaining = remaining, call(
            key, f"{INDEX}/_count", {"query": QUERY})["count"]
        print(f"  осталось {remaining}  (-{previous - remaining})")
        if remaining == 0:
            print("готово")
            break
        stalled = stalled + 1 if remaining == previous else 0
        if stalled >= 3:
            print(f"счётчик стоит третью проверку подряд; осталось {remaining}. "
                  f"Задача {task} могла завершиться с отказами — "
                  f"перезапустите скрипт, чтобы добрать остаток.")
            break

    print(f"итог: {remaining} совпадений осталось (было {before})")


if __name__ == "__main__":
    main()
