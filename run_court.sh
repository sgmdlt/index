#!/usr/bin/env bash
# Reindex one Moscow district court shard from ikuzminkin gz.
# Usage: run_court.sh 77RS0016
code="${1:?usage: run_court.sh <court_code, e.g. 77RS0016>}"
cd /home/ltdmgs/index || exit 1
NAME_RE="${code}.*" exec ./scripts/index_jsonl_gz.sh /home/ikuzminkin/sud_delo/cases_by_court/raw/RS
