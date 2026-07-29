#!/usr/bin/env bash
# Index every *.jsonl under ROOT, one file at a time, with per-file log.
# Usage: ROOT=/path ES=http://host:9200 INDEX=sudata LOG_DIR=logs ./index_jsonl.sh
# Or:    ./index_jsonl.sh /path
set -u
unset TERMINFO  # sudoers env_check blocks this var

ROOT="${1:-${ROOT:-}}"
ES="${ES:-http://10.13.12.1:9200}"
INDEX="${INDEX:-sudata}"
LOG_DIR="${LOG_DIR:-logs}"
NAME_RE="${NAME_RE:-.*\.jsonl}"  # filename regex, e.g. '(13|16)MS.*\.jsonl'

if [ -z "$ROOT" ]; then
  echo "usage: $0 <root_dir>   (or set ROOT=)" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
run_log="$LOG_DIR/_run.log"

# No sudo precheck: NOPASSWD rules are command-specific. If they're missing,
# the per-file cat will fail and the failure ends up in that file's log.

sudo find "$ROOT" -name '*.jsonl' -print0 \
| grep -zE "/$NAME_RE\$" \
| while IFS= read -r -d '' f; do
    name=$(basename "$f" .jsonl)
    log="$LOG_DIR/$name.log"
    echo "[$(date -Is)] start $f -> $log" | tee -a "$run_log"
    sudo find "$f" -maxdepth 0 -exec cat {} + \
      | uv run python -m src.main --es "$ES" --index "$INDEX" --jsonl - \
      > "$log" 2>&1
    rc=$?
    echo "[$(date -Is)] done  $f exit=$rc" | tee -a "$run_log"
  done
