"""Убрать дубли с кривым id у кассационных дел.

Кассация пишет номер как «8а-15348/2025 [88а-14799/2025]»: номер нижестоящего
суда, а в квадратных скобках — свой. build_case_id видел составной номер только
по «(» и «~», поэтому такой номер резался до последнего «/», и дело получало id
«77KJ0002_2025-05-22_8а-15348-2025 [88а-14799». Если то же дело приходило с
номером без скобок, оно ложилось на правильный «..._88а-14799» — отсюда дубль.

Порядок:
    1. python -m scripts.dedup_bracket_ids            # выгрузить, ничего не менять
    2. переиндексировать bracket_ids_fs.txt через --folders (и WARC-список,
       если он не пуст) — уже с исправленной формулой документы лягут на
       правильный id и сольются с тем, что там лежит
    3. python -m scripts.dedup_bracket_ids            # проверить: twin=N из N
    4. python -m scripts.dedup_bracket_ids --delete   # удалить кривые

Удаляется только тот кривой документ, у которого правильный близнец уже есть в
индексе. Что не переиндексировалось — остаётся и видно в выводе.
"""
import argparse
import json
import time
import urllib.request
from pathlib import Path

from src.dump_none_ids import is_warc_locator
from src.utils.case_id import build_case_id

ES = "http://10.13.12.1:9200"
INDEX = "sudata"
ENV = Path(__file__).resolve().parent.parent / ".env"
BRACKET = {"wildcard": {"id_final": {"value": "* [*"}}}
PAGE = 10000  # потолок scroll; каждая страница — полный перебор словаря, их должно быть мало


def fixed_id(bad_id: str, case_number: str) -> str | None:
    court, iso_date, _ = bad_id.split("_", 2)
    yyyy, mm, dd = iso_date.split("-")
    return build_case_id(case_number, f"{dd}.{mm}.{yyyy}", court)


def covered(bad_files: list[str], twin_files: list[str]) -> bool:
    # одно дело собирается из нескольких WARC-записей, а path_manifest хранит
    # только одну — удалять можно, лишь когда близнец уже держит все версии
    return set(bad_files) <= set(twin_files)


def api_key() -> str:
    for line in ENV.read_text(encoding="utf-8").splitlines():
        if line.startswith("ES_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("ES_API_KEY не найден в .env")


def call(key: str, path: str, body=None, timeout=900):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"{ES}/{path}", data=data, method="POST" if data else "GET",
        headers={"Content-Type": "application/json", "Authorization": "ApiKey " + key})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"), strict=False)


def scan_bracket(key: str):
    resp = call(key, f"{INDEX}/_search?scroll=5m", {
        "query": BRACKET, "size": PAGE,
        "_source": ["case_number", "path_manifest", "html_files"]})
    print(f"найдено {resp['hits']['total']['value']}", flush=True)
    while hits := resp["hits"]["hits"]:
        yield from hits
        # ведущий «*» заново перебирает словарь терминов на каждой странице —
        # неполная страница значит, что больше ничего нет, лишний заход не нужен
        if len(hits) < PAGE:
            return
        print(f"  {time.strftime('%H:%M:%S')} страница из {len(hits)}", flush=True)
        resp = call(key, "_search/scroll", {"scroll": "5m", "scroll_id": resp["_scroll_id"]})


def existing(key: str, ids: list[str]) -> dict[str, list[str]]:
    resp = call(key, f"{INDEX}/_mget?_source_includes=html_files", {"ids": ids})
    return {d["_id"]: d["_source"].get("html_files") or [] for d in resp["docs"] if d.get("found")}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--delete", action="store_true",
                   help="удалить кривые документы, у которых есть правильный близнец")
    p.add_argument("--out", default="bracket_ids", help="префикс выходных файлов")
    args = p.parse_args()

    key = api_key()
    rows = [
        {"_id": h["_id"], "new_id": fixed_id(h["_id"], h["_source"]["case_number"]),
         "path_manifest": h["_source"].get("path_manifest") or "",
         "html_files": h["_source"].get("html_files") or []}
        for h in scan_bracket(key)
    ]
    # new_id == _id значит формула номер не починила — такой документ близнецом
    # сам себе не считается, иначе --delete удалил бы единственную копию
    new_ids = [r["new_id"] for r in rows if r["new_id"] and r["new_id"] != r["_id"]]
    found = {k: v for i in range(0, len(new_ids), 1000)
             for k, v in existing(key, new_ids[i:i + 1000]).items()}
    rows = [{**r, "twin": r["new_id"] in found and r["new_id"] != r["_id"],
             "covered": r["new_id"] in found and r["new_id"] != r["_id"]
                        and covered(r["html_files"], found[r["new_id"]])} for r in rows]

    with (open(f"{args.out}.jsonl", "w", encoding="utf-8") as jl,
          open(f"{args.out}_fs.txt", "w", encoding="utf-8") as fs,
          open(f"{args.out}_warc.txt", "w", encoding="utf-8") as warc):
        for r in rows:
            jl.write(json.dumps(r, ensure_ascii=False) + "\n")
            (warc if is_warc_locator(r["path_manifest"]) else fs).write(r["path_manifest"] + "\n")

    twins = [r["_id"] for r in rows if r["covered"]]
    print(f"кривых id: {len(rows)}, с правильным близнецом: {sum(r['twin'] for r in rows)}, "
          f"из них близнец держит все версии (будут удалены): {len(twins)}")
    print(f"записано {args.out}.jsonl, {args.out}_fs.txt, {args.out}_warc.txt")
    if not args.delete or not twins:
        return

    body = "".join(json.dumps({"delete": {"_id": i}}, ensure_ascii=False) + "\n" for i in twins)
    data = body.encode()
    req = urllib.request.Request(
        f"{ES}/{INDEX}/_bulk", data=data, method="POST",
        headers={"Content-Type": "application/x-ndjson", "Authorization": "ApiKey " + key})
    with urllib.request.urlopen(req, timeout=900) as r:
        resp = json.loads(r.read().decode("utf-8"))
    deleted = sum(1 for it in resp["items"] if it["delete"]["result"] == "deleted")
    print(f"удалено {deleted} из {len(twins)}; осталось кривых {len(rows) - deleted}")


if __name__ == "__main__":
    main()
