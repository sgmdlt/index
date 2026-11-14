# index

A court-case Elasticsearch indexer. Russian court HTML pages + a `manifest.json`
sit on disk (or in S3); we parse them, assemble per-case documents, and bulk-write
into Elasticsearch. Production work happens in Python under `src/`. Loose `.js`
files at the repo root (`loadFromDb.js`, `parser.js`, `prepare_doc.js`, etc.)
and the `sud_import/` directory are the legacy Node implementation kept for
reference — do not edit them; their Python equivalents in `src/` are the active
code path.

## Common commands

```bash
# Stack
docker compose up -d                    # ES (9222), Kibana (5615), MinIO (9100/9101)
make minio_init                         # create the `courts` bucket in MinIO

# Indexer — filesystem source (default)
uv run python -m src.main --index sud --root ./court_cases [--workers N] [--tag X] [--filter substr]

# Indexer — JSONL source
uv run python -m src.main --index sud --jsonl path/to/file.jsonl

# Indexer — PostgreSQL outbox source
uv run python -m src.main --index sud --root ./court_cases --db

# Indexer — MinIO/S3 source (reads s3://courts/2026/)
uv run python -m src.main_s3 --index sud_s3 [--workers N] [--tag X]

# Reprocess just the manifests listed in failed_units.txt
uv run python -m src.main --index sud --root ./court_cases --folders failed_units.txt

# ES housekeeping
make delete_index                       # DELETE /sud
make refresh_index                      # POST /sud/_refresh

# Tests (pytest via uv; -vsrP shows skip/xfail reasons)
make test
uv run pytest tests/test_html_parse.py             # one file
uv run pytest tests/test_merge.py::test_merge_arrays  # one test

# MinIO upload (mirror /home/ltdmgs/courts_raw_data/2026/ → s3://courts/2026/)
make upload_2026                        # idempotent, skips keys with matching size
```

ES URL is `http://localhost:9222`; credentials come from `.env`
(`ELASTIC_PASSWORD`, `KIBANA_PASSWORD`, `SGMDLT_PASSWORD`, `ENCRYPTION_KEY`,
optional `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`). The indexer reads
`ES_USER` + `SGMDLT_PASSWORD` env vars when `--user/--pass` aren't passed.

## Pipeline architecture

The pipeline is six sequential stages, with parallelism only in HTML parse:

1. **Source** yields `unit` dicts. A unit always has the same shape regardless
   of source: `{manifest_path, rel_from_root, meta (parsed JSON), html_files
   (sorted filenames)}`. Sources:
   - `src/traverse.py:traverse_cases` — iterative DFS over `court_cases/`
     yielding one unit per `manifest.json` found.
   - `src/traverse_s3.py:traverse_cases_s3` — single-pass `list_objects_v2`
     over an S3 prefix; relies on lex-ordering of keys to group HTML names
     under their parent and emit a unit when the matching `manifest.json` is
     seen (HTML filenames start with digits, sort before `manifest.json`).
   - `src/load_from_db.py` — PostgreSQL outbox lease/poll loop (`SKIP LOCKED`
     with a 5-min lease; up to 5 attempts before giving up).
   - JSONL mode in `src/main.py:_units_from_jsonl` — one record per line.
2. **HTML parse** (`src/parse_html.py:fill_raw_tables`) runs in
   `ProcessPoolExecutor` workers. Returns `(prepared_doc, fetch_ms, n_bytes)`
   so the parent can attribute fetch time to `metrics.readHtmlMs`.
3. **Field extraction** (`src/prepare_doc.py:prepare_doc`) applies the rule
   engine in `src/elastic_index_dict.json` to the raw tables; produces a flat
   doc.
4. **Document assembly** (`src/to_document.py:to_document`) wraps prepared
   files with `meta` from manifest, derives `version_ts` and `id_final`, and
   applies the `instance_case_type_mapping.json` lookup.
5. **Bulk indexer** (`src/bulk_indexer.py:BulkIndexer`) buffers per-bucket
   updates, applies Python-side merging for `MERGE_FIELDS` (driven by the
   `"type": "merge"` entries in `elastic_index_dict.json`), and flushes via
   ES `_bulk` from daemon threads. Bucket key is `\d{4}/\d{2}` extracted from
   `rel_from_root` — `2026/03` etc. — so bulks group cases by month for
   locality.
6. **ES** at `localhost:9222`. The runner toggles `refresh_interval=-1` and
   `number_of_replicas=0` for the duration of the run
   (`_fast_ingest_on/_off`), then restores.

`_run_pipelined` keeps `MAX_INFLIGHT = workers * 8` futures live across many
units to prevent the worker pool from going idle while a unit assembles.

## Two entry points, parallel by design

`src/main.py` and `src/main_s3.py` are intentional siblings, not a refactor.
The S3 path is a benchmark/test target — when it proves itself, we will cut
over fully. **Do not** introduce a shared abstraction layer between them
prematurely; both call into the same `parse_html` / `prepare_doc` /
`to_document` / `bulk_indexer` modules, which is the only shared surface that
matters. `main_s3.py` hardcodes `S3_ENDPOINT`, `S3_BUCKET`, `S3_PREFIX` at the
top — overridable via env, no CLI flags.

Boto3 clients are not fork-safe. `main_s3.py` uses a lazy module-global
`_WORKER_S3` initialized on first call inside each worker process; the parent
process has its own `list_client` for traversal.

## Data layout conventions

```
# Live FS layout (legacy)
court_cases/<YY>/<TAG>/<COURT>/<TYPE>/<CASE_ID>/manifest.json
court_cases/<YY>/<TAG>/<COURT>/<TYPE>/<CASE_ID>/<DD-MM-YYYY.HH:MM:SS>.html

# 2026 dataset (separate root, used for MinIO benchmark)
/home/ltdmgs/courts_raw_data/2026/<MM>/<DD>/<TAG>/<COURT>/<TYPE>/<CASE_ID>/...

# S3 keys mirror the 2026 layout
s3://courts/2026/<MM>/<DD>/<TAG>/<COURT>/<TYPE>/<CASE_ID>/...
```

HTML filenames may carry an optional `<unix_ts>_` prefix
(`src/traverse.py:_parse_html_timestamp` strips it). Sort order via that
helper is event-time ascending, which is how `to_document` derives
`version_ts`.

## Document identity & merging

`id_final` (set in `to_document`) is the ES `_id`. When the same `id_final`
appears with multiple HTML files (e.g. one case has several rulings), the
bulk indexer **merges Python-side** before sending to ES rather than relying
on update scripts: see `bulk_indexer.MERGE_FIELDS`, `_merge_array`,
`_merge_documents`. This is why a re-run with the same input is idempotent —
duplicate doc fragments collapse.

## Metrics

`src/metrics.py:Metrics` is a single object passed through the pipeline.
Notable timers: `parseMs` (wall time across the worker pool for HTML parse),
`readHtmlMs` (sum of per-file fetch time, meaningful for FS↔MinIO
comparison), `manifestFetchMs`, `toDocMs`, `bulkMs`. Counters: `unitsSeen`,
`htmlBytes`, `docsPrepared`, `docsIndexed`. Logs every 5s to stdout — no
persistence, no Prometheus.

## Tests

`tests/<area>_test/` directories hold HTML / JSON fixtures consumed by the
matching `test_<area>.py`. Fixtures are real court page snippets, often with
non-ASCII content; tests assert on Russian strings directly. Keep that —
don't rewrite assertions to use fixture-derived constants.

## Quality gate

`mise run check` (`mise run fix` applies the safe autofixes). The existing `# noqa`,
`# ty: ignore` and config allowlists are the adoption baseline, not a pattern
to extend.

<!-- BEGIN SHARED PRACTICES (source: ~/.agents/AGENTS.md) -->
## Rules

- Every task has a ticket in the repo's tracker. A spec is an epic and a plan is its child tickets; neither lives in a file.
- A side quest gets its own ticket linked to the current one (beads: `bd create -t bug ... --deps discovered-from:<current-id>`).
- Production code starts with a failing test: red, green, refactor.
- Before using a library or framework, read its current docs (Context7 where available: `resolve-library-id` → `query-docs`) and trust them over memory.
- A change is done when the repo's quality gate passes; CI runs the same command.
- Report what you verified (command and result) and what you could not verify.

## Code practices

- **Minimalism**: "No code, no problems." Cut what is not needed and reduce the number of possible states rather than handling them all.
- **Explicit state**: when flags and conditionals hide a state machine, name it and propose a version with explicit states.
- **Functional core, imperative shell**: keep logic pure and push side effects to the boundaries.
- **Consistency over cleverness**: follow the patterns already in the codebase; uniformity beats local optimisation.
- **Explicit over implicit**: state what happened instead of hiding or guessing it — a model output says «Суть из текста не ясна» rather than being dropped; a missing setting disables the feature rather than falling back to another one. Name every assumption that changes behaviour.
- **Readable abstractions**: clear, non-leaky abstractions first; readable code is easier to optimise than fast code is to fix.
- **Root cause**: fix the cause, not the symptom.
- **Mature libraries**: prefer a maintained library to a hand-rolled implementation.
- **Integration tests**: test against real dependencies (a real database, stubs only at the edges).
- **Strict checks**: keep every lint rule and check at full strength. An exception is narrow (one path and rule, or one named value), carries its reason beside it, and a human approves it.
- **Generated files**: change a lockfile only through its tool (`uv lock`, `npm install`).
<!-- END SHARED PRACTICES -->

## Agent skills

### Issue tracker

Issues live in beads of the courts repo (`cd /home/ltdmgs/courts && bd ...`), labelled `repo:index`; a spec is an epic. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary, applied as beads labels. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: root `GLOSSARY.md` + `docs/adr/`. Terms match courts' `GLOSSARY.md` for shared concepts. See `docs/agents/domain.md`.

### Workflow

The global Matt Pocock flow applies (`~/.claude/CLAUDE.md`). Historical specs live in their epics' `design` field, historical plans in their epics' comments (`bd comments <id>`).
