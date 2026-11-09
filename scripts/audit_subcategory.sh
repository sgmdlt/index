#!/usr/bin/env bash
# Find courts whose bulk load never produced case_subcategory.
#
# Two stages, cheap first:
#   1. ES only — a court is suspect when NO document older than OLD_BEFORE
#      carries case_subcategory. Bulk loads wrote the old cases; if none of
#      them have the field, that run predates the working rule.
#   2. Source only for suspects — count records that actually carry a
#      subcategory key, so courts that simply have no such data are dropped.
#
# Usage: ./audit_subcategory.sh <ROOT>
#   ROOT: directory holding <code>.jsonl.gz (same one index_jsonl_gz_from_list.sh takes)
# Writes REPORT (tsv) and LIST (court codes) — feed LIST to index_jsonl_gz_from_list.sh.
set -u
unset TERMINFO  # sudoers env_check blocks this var

ROOT="${1:-}"
ES="${ES:-http://10.13.12.1:9200}"
INDEX="${INDEX:-sudata}"
RUN_LOG="${RUN_LOG:-logs/_run.log}"
OLD_BEFORE="${OLD_BEFORE:-2026-01-01}"
JOBS="${JOBS:-8}"
REPORT="${REPORT:-subcategory_audit.tsv}"
LIST="${LIST:-subcategory_reindex.txt}"

if [ -z "$ROOT" ]; then
  echo "usage: $0 <root_dir>" >&2
  exit 1
fi
if [ ! -f "$RUN_LOG" ]; then
  echo "run log not found: $RUN_LOG" >&2
  exit 1
fi

# One run at a time: two instances share REPORT and race on REPORT.src.
exec 9>"${REPORT:-subcategory_audit.tsv}.lock"
if ! flock -n 9; then
  echo "another audit is already running (holding ${REPORT:-subcategory_audit.tsv}.lock)" >&2
  exit 1
fi

# ES_API_KEY / ES_USER live in .env, same as the indexer expects.
# shellcheck source=/dev/null  # .env is local and untracked
if [ -f .env ]; then set -a; . ./.env; set +a; fi
if [ -n "${ES_API_KEY:-}" ]; then
  auth=(-H "Authorization: ApiKey $ES_API_KEY")
elif [ -n "${ES_USER:-}" ]; then
  auth=(-u "$ES_USER:${SGMDLT_PASSWORD:-}")
else
  echo "no ES_API_KEY or ES_USER in environment/.env" >&2
  exit 1
fi

es_count() {  # $1 = lucene query
  curl -sG "${auth[@]}" "$ES/$INDEX/_count" --data-urlencode "q=$1" \
    | grep -oE '"count":[0-9]+' | cut -d: -f2
}

# Stage 0: court codes this indexer actually loaded, successfully.
codes=$(awk '$2 == "done" && $NF == "exit=0" { print $3 }' "$RUN_LOG" \
        | xargs -r -n1 basename \
        | sed -E 's/\.jsonl(\.gz)?$//' \
        | sort -u)
total_codes=$(printf '%s\n' "$codes" | grep -c .)
# Courts whose run-log path really sits under ROOT. Everything else cannot be
# read from here, so it is NO_SOURCE without spending a sudo call on it — and
# those stray calls are exactly what trips the password prompt.
in_root=$(awk '$2 == "done" && $NF == "exit=0" { print $3 }' "$RUN_LOG" \
          | grep -F "$ROOT/" \
          | xargs -r -n1 basename \
          | sed -E 's/\.jsonl(\.gz)?$//' \
          | sort -u)
echo "courts loaded per $RUN_LOG: $total_codes"

# Stage 1: ES only.
: > "$REPORT"
printf 'code\tdocs\tsubcat\tsubcat_before_%s\tsrc_records\tverdict\n' "$OLD_BEFORE" >> "$REPORT"
suspects=""
i=0
for code in $codes; do
  i=$((i + 1))
  docs=$(es_count "court_code:$code")
  [ -z "$docs" ] && docs=0
  if [ "$docs" -eq 0 ]; then
    printf '%s\t0\t0\t0\t-\tNO_DOCS\n' "$code" >> "$REPORT"
    continue
  fi
  sub=$(es_count "court_code:$code AND _exists_:case_subcategory")
  old=$(es_count "court_code:$code AND _exists_:case_subcategory AND version_ts:<$OLD_BEFORE")
  [ -z "$sub" ] && sub=0
  [ -z "$old" ] && old=0
  if [ "$old" -eq 0 ]; then
    suspects="$suspects $code"
    printf '%s\t%s\t%s\t%s\t?\tSUSPECT\n' "$code" "$docs" "$sub" "$old" >> "$REPORT"
  else
    printf '%s\t%s\t%s\t%s\t-\tOK\n' "$code" "$docs" "$sub" "$old" >> "$REPORT"
  fi
  [ $((i % 100)) -eq 0 ] && echo "  es-scanned $i/$total_codes"
done
# shellcheck disable=SC2086  # suspects is a space-separated list: splitting is the point
n_suspect=$(printf '%s\n' $suspects | grep -c .)
echo "suspects after ES scan: $n_suspect"

# Stage 2: confirm suspects against the source. Counts lines carrying any
# subcategory key — one record per line, so this is exact for well-formed
# files and slightly low for the rare record split across lines.
# Reads one court's source file. `-n` is the load-bearing flag: the NOPASSWD
# rule covers the RS paths, so a name outside it makes sudo prompt, and a
# background run that blocks on that prompt takes SIGTTIN and stops the whole
# job. `zcat -f` is the form index_jsonl_gz_from_list.sh already proves works,
# and -f also passes plain (non-gzip) files straight through.
src_stream() {
  sudo -n zcat -f "$1" 2>/dev/null
}
export -f src_stream

# Preflight-only twin that MAY prompt. Run this under tmux (or any tty): sudo
# asks for the password once, and every later call refreshes the timestamp, so
# the -n workers never hit a prompt. Without this priming call, -n would fail
# on the very first read and mark every court NO_SOURCE.
src_stream_prompt() {
  # stderr deliberately left alone: sudo may write its password prompt there,
  # and swallowing it would leave you staring at a silent hang.
  sudo zcat -f "$1"
}

count_src() {
  local code="$1" n rc
  # No [ -e ] probe: ROOT may be unreadable to this user and readable only via
  # sudo, so a plain test lies. Try the read and let its exit code decide.
  # PIPESTATUS has to be captured inside the subshell that ran the pipeline —
  # read back out here it would only describe the assignment.
  for cand in "$ROOT/$code.jsonl.gz" "$ROOT/$code.jsonl"; do
    { read -r n; read -r rc; } < <(
      src_stream "$cand" | grep -cF \
        -e 'Предмет представления, ходатайства, жалобы' \
        -e 'Сущность заявленного требования' \
        -e 'Материал (в порядке исполнения решений и иные)'
      echo "${PIPESTATUS[0]}"
    )
    if [ "${rc:-1}" -eq 0 ]; then
      printf '%s\t%s\n' "$code" "${n:-0}"
      return
    fi
  done
  printf '%s\t-1\n' "$code"
}
export -f count_src
export ROOT

: > "$LIST"
if [ "$n_suspect" -gt 0 ]; then
  # shellcheck disable=SC2086  # suspects is a space-separated list: splitting is the point
  work=$(comm -12 <(printf '%s\n' $suspects | sort -u) <(printf '%s\n' "$in_root" | sort -u))
  n_work=$(printf '%s\n' "$work" | grep -c .)
  echo "suspects readable from $ROOT: $n_work (of $n_suspect)"

  # Fail loudly now rather than mislabel every readable court as NO_SOURCE.
  probe_ok=0
  step=$(( n_work / 20 )); [ "$step" -lt 1 ] && step=1
  for probe_code in $(printf '%s\n' "$work" | awk -v s="$step" 'NR % s == 0' | head -20); do
    for probe in "$ROOT/$probe_code.jsonl.gz" "$ROOT/$probe_code.jsonl"; do
      src_stream_prompt "$probe" | head -c 1 >/dev/null 2>&1
      # 141 = SIGPIPE: head closed the stream early, so the read did work.
      case "${PIPESTATUS[0]}" in 0|141) probe_ok=1; break 2 ;; esac
    done
  done
  if [ "$probe_ok" -eq 0 ]; then
    echo "sudo -n could not read any of 20 sampled files under $ROOT" >&2
    echo "either the files live elsewhere, or sudo was refused." >&2
    echo "this needs a tty for the one password prompt — run it under tmux:" >&2
    echo "  tmux new -d -s subcat_audit \"cd $PWD && $0 $ROOT\"" >&2
    echo "  tmux attach -t subcat_audit   # type the password once" >&2
    echo "stage 1 results are already in $REPORT" >&2
    exit 1
  fi

  printf '%s\n' "$work" \
    | xargs -r -P "$JOBS" -I{} bash -c 'count_src "$@"' _ {} \
    | sort > "$REPORT.src"
  # Single join pass: one rewrite of the report, not one per suspect.
  awk -F'\t' -v OFS='\t' -v list="$LIST" '
    NR == FNR { n[$1] = $2; next }
    FNR == 1  { print; next }
    $6 == "SUSPECT" {
      c = $1; $5 = (c in n) ? n[c] : -1
      if      (!(c in n) || n[c] == -1) $6 = "NO_SOURCE"
      else if (n[c] ==  0) $6 = "NO_SUBCAT_DATA"
      else                { $6 = "REINDEX"; print c > list }
    }
    { print }
  ' "$REPORT.src" "$REPORT" > "$REPORT.tmp" && mv "$REPORT.tmp" "$REPORT"
  rm -f "$REPORT.src"
fi

echo
echo "report: $REPORT"
echo "reindex list: $LIST ($(grep -c . "$LIST" 2>/dev/null || echo 0) courts)"
echo "next: ./scripts/index_jsonl_gz_from_list.sh $ROOT $LIST"
