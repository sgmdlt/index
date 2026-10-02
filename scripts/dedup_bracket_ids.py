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
    3. python -m scripts.dedup_bracket_ids --ids bracket_ids.jsonl --out bracket_check
                                                      # проверить: twin=N из N
    4. python -m scripts.dedup_bracket_ids --ids bracket_check.jsonl --delete
                                                      # удалить кривые
    5. python -m scripts.dedup_bracket_ids --ids bracket_check.jsonl --fold
                                                      # у кого остались скачивания, которых
                                                      # нет у близнеца: слить их прямо из ES

Удаляется только тот кривой документ, у которого правильный близнец уже есть в
индексе. Что не переиндексировалось — остаётся и видно в выводе.
"""
import argparse
import json
import re
import time
import urllib.request
from pathlib import Path

from src.bulk_indexer import merge_docs
from src.dump_none_ids import is_warc_locator
from src.utils.case_id import build_case_id

ES = "http://10.13.12.1:9200"
INDEX = "sudata"
ENV = Path(__file__).resolve().parent.parent / ".env"
PAGE = 10000  # потолок scroll; каждая страница — полный перебор словаря, их должно быть мало


def fixed_id(bad_id: str, case_number: str) -> str | None:
    court, iso_date, _ = bad_id.split("_", 2)
    yyyy, mm, dd = iso_date.split("-")
    return build_case_id(case_number, f"{dd}.{mm}.{yyyy}", court)


def covered(bad_files: list[str], twin_files: list[str]) -> bool:
    # одно дело собирается из нескольких WARC-записей, а path_manifest хранит
    # только одну — удалять можно, лишь когда близнец уже держит все версии
    return set(bad_files) <= set(twin_files)


def fold(bad: dict, twin: dict) -> dict:
    # близнец идёт входящим: его поля свежие (переиндексирован новым кодом),
    # а кривой докладывает старые скачивания, до которых WARC-локатора у нас нет
    return merge_docs(bad, twin)


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


def scan_bracket(key: str, pattern: str):
    resp = call(key, f"{INDEX}/_search?scroll=5m", {
        "query": {"wildcard": {"id_final": {"value": pattern}}}, "size": PAGE,
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


def fetch_by_ids(key: str, ids: list[str]):
    # без перебора словаря: id уже известны из прошлой выгрузки, _mget по _id дешёвый
    for i in range(0, len(ids), 1000):
        resp = call(key, f"{INDEX}/_mget?_source_includes=case_number,path_manifest,html_files",
                    {"ids": ids[i:i + 1000]})
        yield from (d for d in resp["docs"] if d.get("found"))
        print(f"  {time.strftime('%H:%M:%S')} {i + 1000} из {len(ids)}", flush=True)


def fold_ids(rows: list[dict]) -> list[str]:
    # все пары, а не только непокрытые: у документов старого импорта JSONL
    # html_files пуст, и «покрыт» для них выполняется впустую. Уже удалённые
    # кривые повторно не тронутся — _mget их просто не найдёт
    return [r["_id"] for r in rows if r["twin"]]


def moved(bad: dict, new_id: str) -> dict:
    return {**bad, "id_final": new_id}


def move_ids(rows: list[dict]) -> list[str]:
    # близнеца нет — сливать не с чем, документ просто переезжает на свой id
    return [r["_id"] for r in rows
            if not r["twin"] and r["new_id"] and r["new_id"] != r["_id"]
            and "ID_NOT_PARSED" not in r["new_id"]]


def unmerge_groups(rows: list[dict]) -> dict[str, list[dict]]:
    # дела с датой-заглушкой 1900: формула до исправления клала их на номер
    # нижестоящего суда, и разные дела одного суда склеились в один документ
    groups: dict[str, list[dict]] = {}
    for r in rows:
        if "_1900-01-01_" in r["_id"]:
            groups.setdefault(r["new_id"], []).append(r)
    return groups


def source_id(src: dict) -> str | None:
    # из полей самого документа, а не из его id: у дел с датой 1900 текущий id
    # несёт год производства, а по новой формуле дата у них остаётся 1900
    yyyy, mm, dd = src["date_reg"][:10].split("-")
    return build_case_id(src["case_number"], f"{dd}.{mm}.{yyyy}", src["court_code"])


def collision_groups(rows: list[dict]) -> dict[str, list[dict]]:
    # одно производство, несколько жалоб — разные карточки, склеенные ключом
    # по номеру производства
    groups: dict[str, list[dict]] = {}
    for r in rows:
        if "_1900-01-01_" not in r["_id"]:
            groups.setdefault(r["new_id"], []).append(r)
    return {k: v for k, v in groups.items() if len(v) > 1}


def rekey_candidates(rows: list[dict]) -> list[str]:
    # где документы лежат сейчас: под номером производства; дела с датой 1900
    # переиндексированы под годом и номером из скобок. Склеенные группы
    # разбирает --split, сюда они не идут
    shared = set(collision_groups(rows))
    out = []
    for r in rows:
        if "_1900-01-01_" in r["_id"]:
            m = re.search(r"-(\d{4}) \[(.+)$", r["_id"])
            if m:
                out.append(f"{r['_id'][:8]}_{m.group(1)}-01-01_{m.group(2)}")
        elif r["new_id"] not in shared:
            out.append(r["new_id"])
    return list(dict.fromkeys(out))


def rekey_all(key: str, ids: list[str], batch: int = 200) -> None:
    rekeyed = deleted = 0
    for i in range(0, len(ids), batch):
        docs = sources(key, ids[i:i + batch])
        new_id = {d: source_id(x["_source"]) for d, x in docs.items()}
        new_id = {d: n for d, n in new_id.items() if n and n != d and "ID_NOT_PARSED" not in n}
        targets = sources(key, list(set(new_id.values())))
        # под новым id уже лежит ранняя версия карточки (без номера
        # производства) — сливаем, поздняя идёт входящей; нет — переезд
        ops = [op for d, n in new_id.items() for op in (
            ({"index": {"_id": n, "if_seq_no": targets[n]["_seq_no"],
                        "if_primary_term": targets[n]["_primary_term"]}},
             fold(targets[n]["_source"], moved(docs[d]["_source"], n)))
            if n in targets else
            ({"create": {"_id": n}}, moved(docs[d]["_source"], n)))]
        items = bulk(key, ops) if ops else []
        ok = [d for d, it in zip(new_id, items) if next(iter(it.values())).get("status") in (200, 201)]
        rekeyed += len(ok)
        deleted += delete_ids(key, ok)
        print(f"  {time.strftime('%H:%M:%S')} {i + batch} из {len(ids)}: найдено {len(docs)}, "
              f"к переносу {len(new_id)}, перенесено {rekeyed}, удалено {deleted}{first_error(items)}",
              flush=True)
    print(f"итог: перенесено {rekeyed}, удалено старых {deleted} из {len(ids)}")


def unmerge(key: str, groups: dict[str, list[dict]], out: str, delete: bool) -> None:
    targets = list(groups)
    found = {k: v for i in range(0, len(targets), 1000)
             for k, v in existing(key, targets[i:i + 1000]).items()}
    # удаляем только документ, собранный целиком из наших дел; если в нём есть
    # чужие скачивания — это уже не наш мусор, его разбирать руками
    ours = [t for t, files in found.items()
            if covered(files, [f for r in groups[t] for f in r["html_files"]])]
    foreign = [t for t in found if t not in set(ours)]
    with open(f"{out}_requeue.txt", "w", encoding="utf-8") as f:
        f.writelines(r["path_manifest"] + "\n" for rs in groups.values() for r in rs)
    with open(f"{out}_foreign.txt", "w", encoding="utf-8") as f:
        f.writelines(t + "\n" for t in foreign)
    print(f"неверных id: {len(targets)}, есть в индексе: {len(found)}, "
          f"только из наших дел: {len(ours)}, с чужими скачиваниями: {len(foreign)}")
    print(f"дел на переиндексацию: {sum(map(len, groups.values()))} -> {out}_requeue.txt; "
          f"чужие -> {out}_foreign.txt")
    if delete:
        print(f"удалено: {delete_ids(key, ours)} из {len(ours)}")


def sources(key: str, ids: list[str]) -> dict[str, dict]:
    # пачка уже разобрана прошлым запуском — на пустой _mget ES отвечает 400
    if not ids:
        return {}
    resp = call(key, f"{INDEX}/_mget", {"ids": ids})
    return {d["_id"]: d for d in resp["docs"] if d.get("found")}


def bulk(key: str, ops: list[dict]) -> list[dict]:
    body = "".join(json.dumps(op, ensure_ascii=False) + "\n" for op in ops).encode()
    req = urllib.request.Request(
        f"{ES}/{INDEX}/_bulk", data=body, method="POST",
        headers={"Content-Type": "application/x-ndjson", "Authorization": "ApiKey " + key})
    with urllib.request.urlopen(req, timeout=900) as r:
        return json.loads(r.read().decode("utf-8"))["items"]


def first_error(items: list[dict]) -> str:
    errs = [v for it in items for v in it.values() if v.get("status") not in (200, 201)]
    return f"; отказов {len(errs)}, первый: {errs[0].get('status')} {errs[0].get('error')}" if errs else ""


def fold_all(key: str, bad_ids: list[str], batch: int = 200) -> None:
    folded = deleted = found = 0
    for i in range(0, len(bad_ids), batch):
        bads = sources(key, bad_ids[i:i + batch])
        found += len(bads)
        new_id = {b: fixed_id(b, d["_source"]["case_number"]) for b, d in bads.items()}
        twins = sources(key, [n for n in new_id.values() if n])
        pairs = {b: n for b, n in new_id.items() if n in twins and n != b}
        # if_seq_no: если cron успел обновить близнеца после нашего чтения, ES
        # ответит 409, кривой останется и доберётся повторным запуском
        items = bulk(key, [op for b, n in pairs.items() for op in (
            {"index": {"_id": n, "if_seq_no": twins[n]["_seq_no"],
                       "if_primary_term": twins[n]["_primary_term"]}},
            fold(bads[b]["_source"], twins[n]["_source"]))]) if pairs else []
        # кривой удаляем, только если запись слитого документа ES принял
        ok = [b for b, it in zip(pairs, items) if it["index"].get("status") in (200, 201)]
        folded += len(ok)
        deleted += delete_ids(key, ok)
        print(f"  {time.strftime('%H:%M:%S')} {i + batch} из {len(bad_ids)}: "
              f"найдено кривых {len(bads)}, пар {len(pairs)}, слито {folded}, удалено {deleted}"
              f"{first_error(items)}", flush=True)
    print(f"итог: найдено кривых {found}, слито {folded}, удалено {deleted} из {len(bad_ids)}")


def move_all(key: str, bad_ids: list[str], batch: int = 200) -> None:
    created = deleted = 0
    for i in range(0, len(bad_ids), batch):
        bads = sources(key, bad_ids[i:i + batch])
        new_id = {b: fixed_id(b, d["_source"]["case_number"]) for b, d in bads.items()}
        # create, а не index: если под новым id документ уже появился (два
        # кривых id на одно дело, или cron успел), ES ответит 409 и ничего не
        # перезапишет — такой кривой останется для --fold
        items = bulk(key, [op for b, n in new_id.items() for op in (
            {"create": {"_id": n}}, moved(bads[b]["_source"], n))]) if new_id else []
        ok = [b for b, it in zip(new_id, items) if it["create"].get("status") == 201]
        created += len(ok)
        deleted += delete_ids(key, ok)
        print(f"  {time.strftime('%H:%M:%S')} {i + batch} из {len(bad_ids)}: "
              f"перенесено {created}, удалено {deleted}{first_error(items)}", flush=True)
    print(f"итог: перенесено {created}, удалено кривых {deleted} из {len(bad_ids)}")


def delete_ids(key: str, ids: list[str]) -> int:
    if not ids:
        return 0
    return sum(it["delete"]["result"] == "deleted"
               for it in bulk(key, [{"delete": {"_id": i}} for i in ids]))


def existing(key: str, ids: list[str]) -> dict[str, list[str]]:
    resp = call(key, f"{INDEX}/_mget?_source_includes=html_files", {"ids": ids})
    return {d["_id"]: d["_source"].get("html_files") or [] for d in resp["docs"] if d.get("found")}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--delete", action="store_true",
                   help="удалить кривые документы, у которых есть правильный близнец")
    p.add_argument("--out", default="bracket_ids", help="префикс выходных файлов")
    p.add_argument("--ids", help="взять кривые id из прошлого .jsonl вместо поиска по индексу")
    p.add_argument("--pattern", default="* [*",
                   help="шаблон wildcard по id_final; '*]-*' — оренбургские «2]-50» при «1[2]-50»")
    p.add_argument("--move", action="store_true",
                   help="перенести кривые без близнеца (из --ids) на правильный id")
    p.add_argument("--rekey", action="store_true",
                   help="перенести документы (из --ids) с номера производства на номер жалобы")
    p.add_argument("--split", action="store_true",
                   help="склеенные жалобы одного производства (из --ids): план; с --delete удалить")
    p.add_argument("--unmerge", action="store_true",
                   help="дела с датой 1900 (из --ids): план разборки склеенных; с --delete удалить их")
    p.add_argument("--fold", action="store_true",
                   help="слить оставшиеся кривые (из --ids) в близнеца из ES и удалить их")
    args = p.parse_args()

    key = api_key()
    if args.rekey:
        rekey_all(key, rekey_candidates(list(map(json.loads, open(args.ids, encoding="utf-8")))))
        return
    if args.split:
        unmerge(key, collision_groups(list(map(json.loads, open(args.ids, encoding="utf-8")))),
                args.out, args.delete)
        return
    if args.unmerge:
        unmerge(key, unmerge_groups(list(map(json.loads, open(args.ids, encoding="utf-8")))),
                args.out, args.delete)
        return
    if args.move:
        move_all(key, move_ids(list(map(json.loads, open(args.ids, encoding="utf-8")))))
        return
    if args.fold:
        fold_all(key, fold_ids(list(map(json.loads, open(args.ids, encoding="utf-8")))))
        return
    rows = [
        {"_id": h["_id"], "new_id": fixed_id(h["_id"], h["_source"]["case_number"]),
         "path_manifest": h["_source"].get("path_manifest") or "",
         "html_files": h["_source"].get("html_files") or []}
        for h in (fetch_by_ids(key, [r["_id"] for r in map(json.loads, open(args.ids, encoding="utf-8"))
                                     if " [" in r["_id"]])
                  if args.ids else scan_bracket(key, args.pattern))
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

    deleted = delete_ids(key, twins)
    print(f"удалено {deleted} из {len(twins)}; осталось кривых {len(rows) - deleted}")


if __name__ == "__main__":
    main()
