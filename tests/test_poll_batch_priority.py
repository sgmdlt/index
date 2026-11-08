import os

import psycopg2
import pytest

from src.load_from_db import poll_batch

DSN = os.environ.get("INDEX_TEST_PG_DSN")
if not DSN:
    pytest.skip("INDEX_TEST_PG_DSN not set", allow_module_level=True)


@pytest.fixture
def conn():
    try:
        c = psycopg2.connect(DSN)
    except psycopg2.OperationalError:
        pytest.skip("test Postgres unavailable")
    with c.cursor() as cur:
        cur.execute("TRUNCATE outbox RESTART IDENTITY CASCADE")
    c.commit()
    yield c
    c.rollback()
    c.close()


def insert(conn, rows, status="NEW", lease_until="NULL"):
    with conn.cursor() as cur:
        for filename, priority in rows:
            cur.execute(
                f"INSERT INTO outbox (filename, manifest, run_id, status, priority, lease_until)"
                f" VALUES (%s, 'm', 'r', %s, %s, {lease_until})",
                (filename, status, priority),
            )
    conn.commit()


def test_priority_rows_leased_first(conn):
    insert(conn, [("a", 0), ("b", 5), ("c", 0)])
    assert [r["filename"] for r in poll_batch(conn, "w", batch_size=1)] == ["b"]


def test_priority_only_skips_plain_rows(conn):
    insert(conn, [("a", 0), ("b", 5)])
    assert [r["filename"] for r in poll_batch(conn, "w", priority_only=True)] == ["b"]


def test_priority_only_empty(conn):
    insert(conn, [("a", 0)])
    assert poll_batch(conn, "w", priority_only=True) == []


def test_failed_priority_row_with_expired_lease_is_retaken(conn):
    insert(conn, [("b", 5)], status="FAILED", lease_until="now() - interval '1 minute'")
    assert [r["filename"] for r in poll_batch(conn, "w", priority_only=True)] == ["b"]
