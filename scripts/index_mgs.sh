#!/usr/bin/env bash
# Drain the mos-gorsud crawler's outbox into Elasticsearch.
#
# Same indexer as the courts cron, pointed at a different Postgres and a
# different WARC root. The crawler stores container paths (/app/out/...) in the
# outbox, so both halves of the prefix rewrite have to be given here.
#
# Exits when the outbox is empty — it is a batch job, not a daemon.
set -euo pipefail

ROOT=${ROOT:-/home/ltdmgs/index}
UV=${UV:-/home/ltdmgs/.local/bin/uv}

# MGS crawler's Postgres (loopback-only, see mgs_downloader/docker-compose.yml).
# Set before main.py runs load_dotenv(), which does not override the
# environment — otherwise .env's PGPASSWORD (courts) would be the one used.
export PGHOST=${PGHOST:-127.0.0.1}
export PGPORT=${PGPORT:-5433}
export PGDATABASE=${PGDATABASE:-mgs}
export PGUSER=${PGUSER:-mgs}
export PGPASSWORD=${PGPASSWORD:-mgs}

export COURTS_WARC_DOCKER_ROOT=${COURTS_WARC_DOCKER_ROOT:-/app/out/}
export COURTS_WARC_ROOT=${COURTS_WARC_ROOT:-/home/ltdmgs/mgs_downloader/warc/}

# Without this the progress lines sit in a 8 KB block buffer for the whole run,
# because the log is a file rather than a tty.
export PYTHONUNBUFFERED=1

ES=${ES:-http://10.13.12.1:9200}
INDEX=${INDEX:-sudata}

cd "$ROOT"
echo "=== $(date -Is) indexing MGS outbox → $INDEX ==="
exec "$UV" run python -m src.main --es "$ES" --index "$INDEX" --db "$@"
