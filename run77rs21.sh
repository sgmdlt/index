#!/usr/bin/env bash
# One-shot: reindex Moscow district court 77RS0021 from ikuzminkin gz.
cd /home/ltdmgs/index || exit 1
NAME_RE='77RS0021.*' exec ./scripts/index_jsonl_gz.sh /home/ikuzminkin/sud_delo/cases_by_court/raw/RS
