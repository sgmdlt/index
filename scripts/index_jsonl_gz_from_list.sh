#!/usr/bin/env bash
# Index a curated subset of *.jsonl.gz files (one court code per line in LIST).
# Usage: ./index_jsonl_gz_from_list.sh <ROOT> <LIST>
#   ROOT: directory holding <code>.jsonl.gz files
#   LIST: text file with one court code per line (no extension)
set -u
unset TERMINFO

ROOT="${1:-}"
LIST="${2:-}"
ES="${ES:-http://10.13.12.1:9200}"
INDEX="${INDEX:-sudata}"
LOG_DIR="${LOG_DIR:-logs_retry3}"

if [ -z "$ROOT" ] || [ -z "$LIST" ]; then
  echo "usage: $0 <root_dir> <list_file>" >&2
  exit 1
fi
if [ ! -f "$LIST" ]; then
  echo "list file not found: $LIST" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
run_log="$LOG_DIR/_run.log"

while IFS= read -r code; do
    [ -z "$code" ] && continue
    f="$ROOT/$code.jsonl.gz"
    log="$LOG_DIR/$code.log"
    echo "[$(date -Is)] start $f -> $log" | tee -a "$run_log"
    sudo zcat "$f" \
      | uv run python -m src.main --es "$ES" --index "$INDEX" --jsonl - \
      > "$log" 2>&1
    rc=$?
    echo "[$(date -Is)] done  $code exit=$rc" | tee -a "$run_log"
done < "$LIST"
