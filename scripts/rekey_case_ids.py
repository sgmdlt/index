"""Перенести дела на стабильный id и слить документы одной карточки.

Формула id теперь берёт первый выданный номер карточки, а документы одного
uuid — одно дело, id группы — самая ранняя дата (решение
docs/adr/0001-stable-case-id.md, эпик courts-271g). Индекс собирался
старыми формулами, поэтому часть документов лежит не на своём id, а одна
карточка — под несколькими.

Порядок:
    1. python -m scripts.rekey_case_ids --export rk [--slice I --slices N]
                         # выгрузка без _source; N процессов с I = 0..N-1 параллельно.
                         # пишет rk[.I].rekey.jsonl (id ≠ формула) и rk[.I].uuid.tsv
    2. python -m scripts.rekey_case_ids --check rk*.rekey.jsonl --out rk
                         # без записи: rk.ops.jsonl, rk.manual.jsonl, сводка
    3. python -m scripts.rekey_case_ids --apply rk.ops.jsonl
                         # конфликты (409) — в rk.ops.retry.jsonl; подать в --apply снова.
                         # Повторный --apply идемпотентен: перенесённые уже не найдутся
    4. повторить шаг 1 с новым префиксом (rk2): после шага 3 старые _id в
       uuid.tsv устарели, план групп по ним пропустил бы перенесённые документы
    5. cat rk2*.uuid.tsv | LC_ALL=C sort -t$'\\t' -k1,1 -S 20G > rk2.uuid.sorted.tsv
       python -m scripts.rekey_case_ids --groups rk2.uuid.sorted.tsv --out rk2
                         # без записи: rk2.groups.ops.jsonl
    6. python -m scripts.rekey_case_ids --apply rk2.groups.ops.jsonl
                         # два слияния в один целевой id в одной пачке — второе
                         # получит 409 и уйдёт в retry: подать retry в --apply ещё раз
    7. контрольная выборка: доля id ≠ формула и групп одного uuid ≈ 0

МГС (mos-gorsud) не трогаем: у него свой ключ. Документ на id с «чужим» uuid
не сливается — уходит в *.manual.jsonl. Старый id удаляется только после того,
как ES принял целевой документ, и только если его не обновили после чтения.
"""
import argparse
import json
import time
from itertools import groupby

from scripts.dedup_bracket_ids import INDEX, api_key, bulk, call, first_error, sources
from src.bulk_indexer import merge_docs, with_group_created_at
from src.utils.case_id import case_id_from_fields
from src.uuid_group import in_group_scope, pick_group_id

DOCVALUES = ["case_number", {"field": "date_reg", "format": "yyyy-MM-dd"}, "court_code",
             "uuid", "link", {"field": "version_ts", "format": "strict_date_optional_time"}]


def formula_id(row: dict) -> str | None:
    return case_id_from_fields(row.get("case_number"), row.get("date_reg"),
                               row.get("court_code"), row.get("link"))


def plan_rekey(rows: list[dict], existing: dict[str, dict]) -> list[dict]:
    """rows — выгрузка; existing — {id: _source} документов на формульных id."""
    ops = []
    for row in rows:
        target = formula_id(row)
        if not target or target.endswith("ID_NOT_PARSED") or target == row["_id"]:
            continue
        twin = existing.get(target)
        if twin is None:
            op = "move"
        elif twin.get("uuid") == row.get("uuid"):
            op = "fold"
        else:
            op = "manual"
        ops.append({"op": op, "from": row["_id"], "to": target})
    return ops


def plan_groups(rows: list[dict]) -> list[dict]:
    """rows — документы одного uuid; кандидаты — их формульные id, чтобы
    старый id из прошлой формулы не стал целевым только потому, что занят."""
    ids = {r["_id"] for r in rows}
    if len(ids) < 2:
        return []
    target = pick_group_id([(r.get("formula") or r["_id"], r.get("version_ts") or "") for r in rows])
    return [{"op": "fold", "from": i, "to": target} for i in sorted(ids) if i != target]


def uuid_groups(lines):
    """Строки «uuid\t_id\tформульный id\tversion_ts», отсортированные по uuid → группы."""
    rows = ({"uuid": u, "_id": i, "formula": f, "version_ts": v}
            for u, i, f, v in (line.rstrip("\n").split("\t") for line in lines))
    for _, group in groupby(rows, key=lambda r: r["uuid"]):
        yield list(group)


def merged_doc(a: dict, b: dict, target: str) -> dict:
    """Слитый документ двух скачиваний одной карточки: свежий идёт входящим."""
    older, newer = sorted((a, b), key=lambda d: d.get("version_ts") or "")
    return with_group_created_at({**merge_docs(older, newer), "id_final": target}, [a, b])


# --- выгрузка / проверка / применение ---------------------------------------

def export(key: str, prefix: str, slice_id: int, slices: int) -> None:
    body = {"size": 10000, "sort": ["_doc"], "_source": False, "docvalue_fields": DOCVALUES,
            "query": {"match_all": {}}}
    if slices > 1:
        body["slice"] = {"id": slice_id, "max": slices}
        prefix = f"{prefix}.{slice_id}"
    seen = candidates = 0
    resp = call(key, f"{INDEX}/_search?scroll=10m", body)
    with open(f"{prefix}.rekey.jsonl", "w", encoding="utf-8") as rk, \
         open(f"{prefix}.uuid.tsv", "w", encoding="utf-8") as uu:
        while hits := resp["hits"]["hits"]:
            for h in hits:
                row = {"_id": h["_id"], **{k: v[0] for k, v in h.get("fields", {}).items()}}
                target = formula_id(row)
                if target and not target.endswith("ID_NOT_PARSED") and target != row["_id"]:
                    rk.write(json.dumps(row, ensure_ascii=False) + "\n")
                    candidates += 1
                if in_group_scope(row):
                    uu.write(f"{row['uuid']}\t{row['_id']}\t{target or row['_id']}\t{row.get('version_ts') or ''}\n")
            seen += len(hits)
            print(f"  {time.strftime('%H:%M:%S')} просмотрено {seen}, к переносу {candidates}", flush=True)
            resp = call(key, "_search/scroll", {"scroll": "10m", "scroll_id": resp["_scroll_id"]})
    print(f"итог: просмотрено {seen}, к переносу {candidates}")


def check(key: str, paths: list[str], out: str, batch: int = 1000) -> None:
    rows = [json.loads(line) for p in paths for line in open(p, encoding="utf-8")]
    counts = {"move": 0, "fold": 0, "manual": 0}
    with open(f"{out}.ops.jsonl", "w", encoding="utf-8") as ops_f, \
         open(f"{out}.manual.jsonl", "w", encoding="utf-8") as manual_f:
        for i in range(0, len(rows), batch):
            chunk = rows[i:i + batch]
            targets = sources(key, sorted({t for r in chunk if (t := formula_id(r))}))
            for op in plan_rekey(chunk, {i: d["_source"] for i, d in targets.items()}):
                counts[op["op"]] += 1
                (manual_f if op["op"] == "manual" else ops_f).write(json.dumps(op, ensure_ascii=False) + "\n")
            print(f"  {time.strftime('%H:%M:%S')} {i + len(chunk)} из {len(rows)}: {counts}", flush=True)
    print(f"итог: {counts}")


def groups(path: str, out: str) -> None:
    n = 0
    with open(path, encoding="utf-8") as f, open(f"{out}.groups.ops.jsonl", "w", encoding="utf-8") as ops_f:
        for group in uuid_groups(f):
            for op in plan_groups(group):
                ops_f.write(json.dumps(op, ensure_ascii=False) + "\n")
                n += 1
    print(f"итог: слияний {n}")


def apply(key: str, path: str, batch: int = 200) -> None:
    ops = [json.loads(line) for line in open(path, encoding="utf-8")]
    done = conflicts = 0
    with open(path.removesuffix(".jsonl") + ".retry.jsonl", "w", encoding="utf-8") as retry:
        for i in range(0, len(ops), batch):
            chunk = ops[i:i + batch]
            docs = sources(key, sorted({o["from"] for o in chunk} | {o["to"] for o in chunk}))
            writes, pending = [], []
            for o in chunk:
                src, dst = docs.get(o["from"]), docs.get(o["to"])
                if src is None:
                    continue  # уже перенесён прошлым запуском
                if dst is None:
                    # create: если под целевым id документ успел появиться, 409 — ничего не затрём
                    writes += [{"create": {"_id": o["to"]}}, {**src["_source"], "id_final": o["to"]}]
                else:
                    if dst["_source"].get("uuid") != src["_source"].get("uuid"):
                        continue  # чужое дело на целевом id — только вручную
                    writes += [{"index": {"_id": o["to"], "if_seq_no": dst["_seq_no"],
                                          "if_primary_term": dst["_primary_term"]}},
                               merged_doc(src["_source"], dst["_source"], o["to"])]
                pending.append(o)
            items = bulk(key, writes) if writes else []
            ok = [o for o, it in zip(pending, items) if next(iter(it.values())).get("status") in (200, 201)]
            for o in pending:
                if o not in ok:
                    retry.write(json.dumps(o, ensure_ascii=False) + "\n")
            # старый id — только если его не обновили после нашего чтения
            deletes = bulk(key, [{"delete": {"_id": o["from"], "if_seq_no": docs[o["from"]]["_seq_no"],
                                             "if_primary_term": docs[o["from"]]["_primary_term"]}}
                                 for o in ok]) if ok else []
            for o, it in zip(ok, deletes):
                if it["delete"].get("status") != 200:
                    retry.write(json.dumps(o, ensure_ascii=False) + "\n")
            done += sum(it["delete"].get("status") == 200 for it in deletes)
            conflicts += len(pending) - len(ok)
            print(f"  {time.strftime('%H:%M:%S')} {i + len(chunk)} из {len(ops)}: перенесено {done}, "
                  f"конфликтов {conflicts}{first_error(items)}", flush=True)
    print(f"итог: перенесено {done}, в retry {conflicts}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--export", metavar="PREFIX", help="выгрузить кандидатов и uuid")
    mode.add_argument("--check", nargs="+", metavar="REKEY_JSONL", help="план переноса без записи")
    mode.add_argument("--groups", metavar="SORTED_TSV", help="план слияния групп uuid без записи")
    mode.add_argument("--apply", metavar="OPS_JSONL", help="выполнить план")
    p.add_argument("--out", default="rk", help="префикс файлов плана")
    p.add_argument("--slice", type=int, default=0)
    p.add_argument("--slices", type=int, default=1)
    args = p.parse_args()

    if args.groups:
        groups(args.groups, args.out)
        return
    key = api_key()
    if args.export:
        export(key, args.export, args.slice, args.slices)
    elif args.check:
        check(key, args.check, args.out)
    else:
        apply(key, args.apply)


if __name__ == "__main__":
    main()
