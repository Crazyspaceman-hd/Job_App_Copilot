"""
tests/test_intake.py — smoke tests for the intake pipeline.
Run with:  pytest
"""

import sqlite3
import pytest

from app.db import init_db, get_conn, DB_PATH
from app.services.intake import normalise, ingest, _detect_remote_policy


# ── Unit tests ────────────────────────────────────────────────────────────────

def test_remote_policy_detection():
    assert _detect_remote_policy("This is a fully remote role.") == "remote"
    assert _detect_remote_policy("Hybrid work model available.")  == "hybrid"
    assert _detect_remote_policy("Must work on-site in Austin.")  == "onsite"
    assert _detect_remote_policy("No mention at all.")            is None


def test_normalise_sets_title_from_first_line():
    jd = "Senior Software Engineer\n\nWe are looking for..."
    record = normalise(jd)
    assert record.title == "Senior Software Engineer"
    assert record.raw_text == jd.strip()


def test_normalise_with_url():
    record = normalise("Some JD", source_url="https://example.com/job/42")
    assert record.source_url == "https://example.com/job/42"


# ── Integration test (uses a real in-memory DB) ───────────────────────────────

@pytest.fixture
def mem_conn():
    """In-memory SQLite connection with schema applied."""
    from pathlib import Path
    schema = (Path(__file__).parent.parent / "sql" / "schema.sql").read_text()
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(schema)
    yield conn
    conn.close()


def test_ingest_inserts_row(mem_conn):
    jd = "Staff ML Engineer — Remote\n\nWe build cool things."
    job_id = ingest(jd, mem_conn, source_url="https://example.com/job/1")
    assert job_id == 1

    row = mem_conn.execute("SELECT * FROM jobs WHERE id = 1").fetchone()
    assert row is not None
    assert "Staff ML Engineer" in row["title"]
    assert row["remote_policy"] == "remote"
    assert row["status"] == "new"
    assert row["source_url"] == "https://example.com/job/1"


def test_ingest_multiple_rows(mem_conn):
    ingest("Job one\nFully remote", mem_conn)
    ingest("Job two\nOn-site role", mem_conn)
    count = mem_conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    assert count == 2
