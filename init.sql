CREATE TYPE status AS ENUM ('NEW', 'IN_PROGRESS', 'DONE', 'FAILED', 'REDOWNLOAD');

CREATE TYPE redownload_reason AS ENUM (
    'path_missing',
    'docs_missing',
    'bad_manifest_json',
    'build_failed',
    'bad_jsonl_record',
    'sql_error',
    'empty_html'
);

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR,
    filename VARCHAR NOT NULL,
    manifest VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    status status NOT NULL DEFAULT 'NEW',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    locked_by VARCHAR(64),
    locked_at TIMESTAMP WITH TIME ZONE,
    lease_until TIMESTAMP WITH TIME ZONE,
    warc_locator VARCHAR,
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE redownload_queue (
    id SERIAL PRIMARY KEY,
    manifest_path VARCHAR NOT NULL,
    filename VARCHAR,
    reason redownload_reason NOT NULL,
    outbox_id INTEGER REFERENCES outbox(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (manifest_path, filename, reason)
);

INSERT INTO outbox (filename, manifest, run_id, status) VALUES
('/app/court_data/22/RS/03RS0004/УК/03RS0004_2025-10-22_1-234/23-10-2025.07:35:36.html', '/app/court_data/22/RS/03RS0004/УК/03RS0004_2025-10-22_1-234/manifest.json', 'test_run_001', 'NEW'),
('/app/court_data/22/RS/03RS0004/УК/03RS0004_2025-10-22_1-234/24-10-2025.10:04:57.html', '/app/court_data/22/RS/03RS0004/УК/03RS0004_2025-10-22_1-234/manifest.json', 'test_run_001', 'NEW'),
('/app/court_data/22/RS/03RS0004/УК/03RS0004_2025-10-22_1-234/28-10-2025.10:11:45.html', '/app/court_data/22/RS/03RS0004/УК/03RS0004_2025-10-22_1-234/manifest.json', 'test_run_001', 'NEW');
