"""Delete outbox rows whose manifest path no longer exists on disk.

One-time cleanup after case_id refactor moved files. Run before enabling the
self-healing redownload pipeline so post-move debris is not interpreted as
genuine missing data.

Usage:
  uv run python -m scripts.trim_orphans [--dry-run]

Resolves DB-side Docker paths via load_from_db._fix_path (translated to host
absolute via $COURTS_FS_ROOT, default /home/ltdmgs/courts_raw_data/).
"""

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from dotenv import load_dotenv

from src.load_from_db import TABLE, _fix_path

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Trim outbox rows pointing at missing manifests")
    p.add_argument("--dry-run", action="store_true", help="Report counts, do not delete")
    p.add_argument("--batch", type=int, default=10000, help="Scan batch size")
    p.add_argument("--workers", type=int, default=64, help="Parallel stat threads")
    return p.parse_args()


def iter_candidates(conn, batch_size: int):
    last_id = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, manifest FROM {TABLE}
                WHERE id > %s AND status IN ('NEW', 'FAILED')
                ORDER BY id ASC LIMIT %s
                """,
                (last_id, batch_size),
            )
            rows = cur.fetchall()
        if not rows:
            return
        for row in rows:
            yield row
        last_id = rows[-1][0]


def main():
    args = parse_args()

    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=int(os.environ.get("PGPORT", "8432")),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )
    def check(row):
        row_id, manifest = row
        abs_path = _fix_path(manifest)
        return row_id if not os.path.exists(abs_path) else None

    try:
        scanned = 0
        orphan_ids: list[int] = []
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for result in pool.map(check, iter_candidates(conn, args.batch), chunksize=256):
                scanned += 1
                if result is not None:
                    orphan_ids.append(result)
                if scanned % 50000 == 0:
                    logger.info("Scanned %d, orphans so far %d", scanned, len(orphan_ids))

        logger.info("Scan complete: %d rows checked, %d orphans", scanned, len(orphan_ids))
        if not orphan_ids:
            return
        if args.dry_run:
            logger.info("Dry run — no deletes")
            return

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE} WHERE id = ANY(%s)", (orphan_ids,))
            deleted = cur.rowcount
        conn.commit()
        logger.info("Deleted %d orphan rows", deleted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
