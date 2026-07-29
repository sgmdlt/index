# Outbox Self-Healing Loop — Design

**Date:** 2026-05-06
**Status:** Approved (brainstorm phase)

## Problem

The PostgreSQL outbox source (`src/load_from_db.py`) feeds the indexer with `(filename, manifest)` paths. After a recent case_id refactor, many outbox rows now point at paths that no longer exist on disk — orphans. The current behavior marks these `FAILED` and retries up to 5 times, wasting cycles.

Separately, when a manifest's content is corrupt (bad JSON, or the document fails to assemble downstream), the indexer logs the path to `failed_units.txt` and moves on. There is no mechanism to request a fresh download from the upstream scraper.

## Goal

A self-healing pipeline:

1. Detect three failure modes in the indexer.
2. Route each into a `redownload_queue` table that an external worker (out of scope here) will consume to re-fetch the underlying HTML/manifest.
3. Stop retrying within the outbox — once redownload is queued, the outbox row is terminal.

## Failure modes

| Condition                                          | Reason tag           | Modes               |
|----------------------------------------------------|----------------------|---------------------|
| `manifest.json` path missing on disk               | `path_missing`       | db                  |
| Manifest dir exists but contains no HTML files     | `docs_missing`       | filesystem, folders |
| `manifest.json` exists but `json.load` fails       | `bad_manifest_json`  | all                 |
| `_process_unit` runs but `build_update_item` is `None` (no usable doc) | `build_failed`       | all                 |

HTML parse errors on individual files remain parser-side concerns: they are already logged to `failed_units.txt` and do **not** trigger redownload (a unit can have several HTML files; one bad file does not invalidate the case).

## Schema changes (`init.sql`)

```sql
CREATE TYPE redownload_reason AS ENUM (
    'path_missing',
    'docs_missing',
    'bad_manifest_json',
    'build_failed'
);

CREATE TABLE redownload_queue (
    id SERIAL PRIMARY KEY,
    manifest_path VARCHAR NOT NULL,
    filename VARCHAR,
    reason redownload_reason NOT NULL,
    outbox_id INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (manifest_path, filename, reason)
);

ALTER TYPE outbox_status ADD VALUE 'REDOWNLOAD';
```

`UNIQUE` dedupes repeated inserts for the same (manifest, filename, reason). Inserts use `ON CONFLICT DO NOTHING`.

`REDOWNLOAD` is a new terminal status for outbox rows whose underlying data needs re-fetching.

## One-time orphan trim

Before the new code runs in production, run a script (or ad-hoc SQL) to delete outbox rows whose manifest path no longer exists. This clears post-move debris so the new `path_missing` → `redownload` path only sees genuinely missing data.

```python
# scripts/trim_orphans.py — sketch
for row in SELECT id, manifest FROM outbox WHERE status IN ('NEW', 'FAILED'):
    if not os.path.exists(_fix_path(row.manifest)):
        DELETE FROM outbox WHERE id = row.id
```

Out of scope for the indexer behavior change; documented here so it is not forgotten.

## Code changes

### `src/load_from_db.py`

1. **`rows_to_units`** — change yield signature from `(unit | None, row)` to `(unit | None, row, reason | None)`:
   - missing manifest path → `(None, row, 'path_missing')`
   - JSON decode error → `(None, row, 'bad_manifest_json')`
   - happy path → `(unit, row, None)`

2. **New `queue_redownload(conn, row, reason, filename=None)`**:
   - `INSERT INTO redownload_queue (...) ON CONFLICT DO NOTHING`
   - `UPDATE outbox SET status='REDOWNLOAD', locked_by=NULL, lease_until=NULL WHERE id=%s`
   - single transaction, commit at end.

3. `mark_failed` is no longer called from the DB runner for these three reasons. It remains for unexpected exceptions in `_process_unit` that are not `build_failed` (e.g., transient ES errors) — those still benefit from the retry/backoff path.

### `src/main.py`

1. **`_process_unit`** gains a return value: `bool` — `True` if at least one document was added to the indexer, `False` if every `build_update_item` returned `None` (i.e., `build_failed`).
   - In filesystem/folders/jsonl modes the return value drives `queue_redownload(..., reason='build_failed', outbox_id=None)` instead of `_append_failed_unit`.
   - In DB mode the runner uses it to decide between `mark_done` and `queue_redownload`.

2. **Empty-directory detection** (filesystem + folders modes):
   - `_units_from_folders`: after listing dir, if `html_files` empty → `queue_redownload(conn, None, 'docs_missing', manifest_path=rel)` and skip yielding.
   - `traverse_cases` (`src/traverse.py`): same — when manifest.json found but sibling HTML list is empty for that dir, queue + skip. (DB mode is unaffected — it uses explicit filename rows.)
   - These modes need a Postgres connection. Add it via the runner: `_run_pipelined` and `_units_from_folders` accept an optional `conn`. If `conn is None` (e.g., user opts out), fall back to existing `_append_failed_unit` behavior.

3. **`queue_redownload` signature** revised to support no-outbox callers:
   ```python
   def queue_redownload(
       conn,
       reason: str,
       *,
       manifest_path: str,
       filename: str | None = None,
       outbox_row: dict | None = None,
   ) -> None
   ```
   When `outbox_row` is supplied → also flip outbox status to `REDOWNLOAD`. Otherwise insert-only.

4. **`_run_db`** updated:
   ```python
   for unit, row, reason in rows_to_units(rows, root):
       if reason:
           queue_redownload(conn, reason, manifest_path=row["manifest"], filename=row["filename"], outbox_row=row)
           continue
       try:
           ok = _process_unit(unit, pool, indexer, metrics, tag)
           if ok:
               mark_done(conn, [row["id"]])
           else:
               queue_redownload(conn, 'build_failed', manifest_path=row["manifest"], filename=row["filename"], outbox_row=row)
       except Exception as exc:
           logger.error("DB unit id=%d: %s", row["id"], exc)
           mark_failed(conn, row["id"], row["attempts"], str(exc))
   ```

5. `_run_pipelined` is **not** used for DB mode (current code uses `_process_unit`); leave its DB plumbing alone but pass `conn` for empty-dir + build_failed reporting in fs/folders modes.

## Testing

Integration test against a real Postgres (per project convention — no mocks). Fixture: three outbox rows covering each failure mode plus one happy-path row. Assert:

- happy row → `status='DONE'`, no redownload entry.
- missing path → `status='REDOWNLOAD'`, redownload row with `reason='path_missing'`.
- bad JSON manifest → `status='REDOWNLOAD'`, reason `'bad_manifest_json'`.
- valid manifest but no usable HTML → `status='REDOWNLOAD'`, reason `'build_failed'`.
- repeat run on same data → no duplicate redownload rows (UNIQUE constraint).

Add filesystem-mode test: empty case directory (manifest.json present, no HTML) → `redownload_queue` row with `reason='docs_missing'`, `outbox_id IS NULL`.

## Out of scope

- Redownload worker itself.
- Backfill from `failed_units.txt` into `redownload_queue`.
- Migration tooling for existing deployments (single-dev project — `init.sql` re-run is fine).
- Metrics for redownload queue depth (add later if useful).


