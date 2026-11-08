"""Mirror /home/ltdmgs/courts_raw_data/2026/ → s3://courts/2026/.

Read-only on the local FS. Uploads only — never deletes or moves originals.
Idempotent: skips keys that already exist with matching size, so it can be
resumed.

Usage:
  uv run python scripts/upload_to_minio.py
"""

import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("upload")
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

SOURCE_ROOT = Path("/home/ltdmgs/courts_raw_data/2026")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9100")
S3_BUCKET = os.environ.get("S3_BUCKET", "courts")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
WORKERS = int(os.environ.get("UPLOAD_WORKERS", "16"))
BATCH = WORKERS * 64
LOG_EVERY = 5000  # files


@dataclass(frozen=True)
class Pair:
    path: Path
    key: str
    size: int


@dataclass(frozen=True)
class Result:
    bytes_uploaded: int
    skipped: bool


def _build_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def _iter_pairs(root: Path) -> Iterable[Pair]:
    key_root = root.parent
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        yield Pair(path=path, key=path.relative_to(key_root).as_posix(), size=size)


_tls = threading.local()


def _thread_client():
    c = getattr(_tls, "client", None)
    if c is None:
        c = _build_client()
        _tls.client = c
    return c


def _upload_one(pair: Pair) -> Result:
    client = _thread_client()
    try:
        head = client.head_object(Bucket=S3_BUCKET, Key=pair.key)
        if int(head.get("ContentLength", -1)) == pair.size:
            return Result(bytes_uploaded=0, skipped=True)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise
    client.upload_file(str(pair.path), S3_BUCKET, pair.key)
    return Result(bytes_uploaded=pair.size, skipped=False)


def _chunks(it: Iterator, n: int) -> Iterator[list]:
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch


def _ensure_bucket(client) -> None:
    try:
        client.head_bucket(Bucket=S3_BUCKET)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket", "NotFound"):
            logger.info("Creating bucket %s", S3_BUCKET)
            client.create_bucket(Bucket=S3_BUCKET)
        else:
            raise


def main() -> int:
    if not SOURCE_ROOT.is_dir():
        logger.error("Source not found: %s", SOURCE_ROOT)
        return 1

    _ensure_bucket(_build_client())

    started = time.monotonic()
    n_files = 0
    n_skipped = 0
    bytes_up = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for batch in _chunks(iter(_iter_pairs(SOURCE_ROOT)), BATCH):
            for r in pool.map(_upload_one, batch):
                n_files += 1
                if r.skipped:
                    n_skipped += 1
                else:
                    bytes_up += r.bytes_uploaded
                if n_files % LOG_EVERY == 0:
                    now = time.monotonic()
                    rate = n_files / max(0.001, now - started)
                    mb = bytes_up / (1024 * 1024)
                    logger.info(
                        "uploaded=%d (skipped=%d) %.0f files/s %.1f MB elapsed=%.1fs",
                        n_files, n_skipped, rate, mb, now - started,
                    )

    elapsed = time.monotonic() - started
    mb = bytes_up / (1024 * 1024)
    logger.info(
        "DONE files=%d uploaded=%d skipped=%d total=%.1f MB rate=%.1f MB/s elapsed=%.1fs",
        n_files, n_files - n_skipped, n_skipped, mb, mb / max(0.001, elapsed), elapsed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
