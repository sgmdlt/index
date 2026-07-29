#!/usr/bin/env bash
# Re-index files listed in a file (one path per line). Handles .jsonl and .jsonl.gz.
# Usage: ./index_failed.sh failed_files.txt
set -u
unset TERMINFO

LIST="${1:-failed_files.txt}"
ES="${ES:-http://10.13.12.1:9200}"
INDEX="${INDEX:-sudata}"
LOG_DIR="${LOG_DIR:-logs_retry}"

if [ ! -f "$LIST" ]; then
  echo "list file not found: $LIST" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
run_log="$LOG_DIR/_run.log"

while IFS= read -r f; do
  [ -z "$f" ] && continue
  case "$f" in
    *.jsonl.gz) name=$(basename "$f" .jsonl.gz); reader="sudo zcat" ;;
    *.jsonl)    name=$(basename "$f" .jsonl);    reader="sudo find $f -maxdepth 0 -exec cat {} +" ;;
    *) echo "[$(date -Is)] skip unknown ext: $f" | tee -a "$run_log"; continue ;;
  esac
  log="$LOG_DIR/$name.log"
  echo "[$(date -Is)] start $f -> $log" | tee -a "$run_log"
  if [ "${f##*.}" = "gz" ]; then
    sudo zcat "$f" \
      | uv run python -m src.main --es "$ES" --index "$INDEX" --jsonl - \
      > "$log" 2>&1
  else
    sudo find "$f" -maxdepth 0 -exec cat {} + \
      | uv run python -m src.main --es "$ES" --index "$INDEX" --jsonl - \
      > "$log" 2>&1
  fi
  rc=$?
  echo "[$(date -Is)] done  $f exit=$rc" | tee -a "$run_log"
done < "$LIST"
