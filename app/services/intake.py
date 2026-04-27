"""
app/services/intake.py — Ingest a raw job description and persist a jobs row.

Normalisation is intentionally minimal at this stage.
Fields that require LLM extraction (company, title, etc.) are stubbed out and
clearly marked so they can be wired up later without changing the interface.
"""

import re
import sqlite3
from dataclasses import dataclass, field
from typing import Optional


# ── Data transfer object ──────────────────────────────────────────────────────

@dataclass
class JobRecord:
    raw_text:       str
    source_url:     Optional[str] = None
    company:        Optional[str] = None
    title:          Optional[str] = None
    location:       Optional[str] = None
    remote_policy:  Optional[str] = None
    status:         str = "new"


# ── Lightweight heuristic normalisation (no LLM required) ────────────────────

_REMOTE_PATTERNS = [
    (r"\bfully[- ]remote\b",   "remote"),
    (r"\bremote[- ]first\b",   "remote"),
    (r"\b100%\s*remote\b",     "remote"),
    (r"\bhybrid\b",            "hybrid"),
    (r"\bin[- ]office\b",      "onsite"),
    (r"\bon[- ]site\b",        "onsite"),
    (r"\bonsite\b",            "onsite"),
    # Catch-all: standalone "remote" not already matched above
    (r"\bremote\b",            "remote"),
]


def _detect_remote_policy(text: str) -> Optional[str]:
    lower = text.lower()
    for pattern, policy in _REMOTE_PATTERNS:
        if re.search(pattern, lower):
            return policy
    return None


def _first_nonempty_line(text: str) -> Optional[str]:
    """Return the first non-blank line; used as a fallback title hint."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def normalise(raw_text: str, source_url: Optional[str] = None) -> JobRecord:
    """
    Build a JobRecord from raw JD text.

    TODO(LLM): replace the heuristic stubs below with an LLM extraction call
               (services/extractor.py) that populates company, title, location.
    """
    record = JobRecord(
        raw_text=raw_text.strip(),
        source_url=source_url,
        remote_policy=_detect_remote_policy(raw_text),
        # --- LLM stubs (heuristic placeholders) ---
        company=None,           # TODO(LLM): extract via LLM
        title=_first_nonempty_line(raw_text),  # very rough stand-in
        location=None,          # TODO(LLM): extract via LLM
    )
    return record


# ── Persistence ───────────────────────────────────────────────────────────────

def insert_job(conn: sqlite3.Connection, record: JobRecord) -> int:
    """Insert a JobRecord and return the new row id."""
    cur = conn.execute(
        """
        INSERT INTO jobs (source_url, company, title, location, remote_policy, raw_text, status)
        VALUES (:source_url, :company, :title, :location, :remote_policy, :raw_text, :status)
        """,
        {
            "source_url":    record.source_url,
            "company":       record.company,
            "title":         record.title,
            "location":      record.location,
            "remote_policy": record.remote_policy,
            "raw_text":      record.raw_text,
            "status":        record.status,
        },
    )
    conn.commit()
    return cur.lastrowid


def ingest(
    raw_text: str,
    conn: sqlite3.Connection,
    source_url: Optional[str] = None,
) -> int:
    """
    Full intake pipeline: normalise → insert → return job_id.

    This is the single entry-point other modules should call.
    """
    record = normalise(raw_text, source_url=source_url)
    job_id = insert_job(conn, record)
    return job_id
