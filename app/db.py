"""
app/db.py — SQLite connection helper and schema initialiser.

Usage:
    from app.db import get_conn, init_db

    init_db()               # create tables if they don't exist
    conn = get_conn()       # get a connection (caller responsible for closing)
"""

import sqlite3
from pathlib import Path

# Paths are relative to the project root, not this file.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH       = _PROJECT_ROOT / "data" / "copilot.db"
SCHEMA_PATH   = _PROJECT_ROOT / "sql" / "schema.sql"


def get_conn() -> sqlite3.Connection:
    """Return a connection with foreign-key enforcement and row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def apply_migrations(conn: sqlite3.Connection) -> None:
    """
    Incremental schema migrations.  Safe to re-run on every startup.

    Public so test fixtures can call it on an in-memory connection without
    going through init_db() (which writes to the file-system DB).
    """
    # v1.1 – verdict / confidence / scores_json columns on fit_assessments
    for col, coltype in [
        ("verdict",     "TEXT"),
        ("confidence",  "TEXT"),
        ("scores_json", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE fit_assessments ADD COLUMN {col} {coltype}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already present

    # v1.2 – source_phrase on extracted_requirements
    try:
        conn.execute("ALTER TABLE extracted_requirements ADD COLUMN source_phrase TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    # v1.2 – extraction_runs table (idempotent via CREATE IF NOT EXISTS)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS extraction_runs (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id                INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            extracted_at          TEXT    NOT NULL DEFAULT (datetime('now')),
            extraction_confidence TEXT,
            extraction_notes_json TEXT,
            seniority             TEXT,
            min_years_experience  INTEGER,
            max_years_experience  INTEGER,
            logistics_json        TEXT,
            ats_keywords_json     TEXT,
            summary_json          TEXT
        )
    """)
    conn.commit()

    # v1.3 – evidence_json on fit_assessments (machine-readable evidence buckets)
    try:
        conn.execute("ALTER TABLE fit_assessments ADD COLUMN evidence_json TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already present

    # v1.4 – base resume and cover letter ingestion tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS base_resumes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at     TEXT    NOT NULL DEFAULT (datetime('now')),
            label           TEXT    NOT NULL DEFAULT 'default',
            raw_text        TEXT    NOT NULL,
            normalized_json TEXT    NOT NULL,
            section_count   INTEGER NOT NULL DEFAULT 0,
            bullet_count    INTEGER NOT NULL DEFAULT 0,
            skill_count     INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resume_bullets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            resume_id   INTEGER NOT NULL REFERENCES base_resumes(id) ON DELETE CASCADE,
            section     TEXT    NOT NULL,
            text        TEXT    NOT NULL,
            skills_json TEXT    NOT NULL DEFAULT '[]',
            source_line INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS base_cover_letters (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at     TEXT    NOT NULL DEFAULT (datetime('now')),
            label           TEXT    NOT NULL DEFAULT 'default',
            raw_text        TEXT    NOT NULL,
            normalized_json TEXT    NOT NULL,
            fragment_count  INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cover_letter_fragments (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cover_letter_id INTEGER NOT NULL
                                REFERENCES base_cover_letters(id) ON DELETE CASCADE,
            kind            TEXT    NOT NULL,
            text            TEXT    NOT NULL,
            source_line     INTEGER NOT NULL
        )
    """)
    conn.commit()

    # v1.5 – metadata_json on generated_assets (provenance for generated resumes/CLs)
    try:
        conn.execute("ALTER TABLE generated_assets ADD COLUMN metadata_json TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already present

    # v1.5 – base_resume_id / assessment_id on generated_assets (optional back-links)
    for col, coltype in [
        ("base_resume_id",  "INTEGER"),
        ("assessment_id",   "INTEGER"),
        ("label",           "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE generated_assets ADD COLUMN {col} {coltype}")
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # v1.6 – base_cl_id on generated_assets (back-link to ingested base cover letter)
    try:
        conn.execute("ALTER TABLE generated_assets ADD COLUMN base_cl_id INTEGER")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    # v1.7 – richer columns on project_recommendations (replaces TODO stub)
    for col, coltype in [
        ("recommendation_type",      "TEXT"),
        ("why_this_matches",         "TEXT"),
        ("business_problem",         "TEXT"),
        ("target_gap_or_signal",     "TEXT"),
        ("stack_json",               "TEXT"),
        ("scoped_version",           "TEXT"),
        ("measurable_outcomes_json", "TEXT"),
        ("resume_value",             "TEXT"),
        ("implementation_notes",     "TEXT"),
        ("label",                    "TEXT"),
        ("assessment_id",            "INTEGER"),
        ("metadata_json",            "TEXT"),
    ]:
        try:
            conn.execute(
                f"ALTER TABLE project_recommendations ADD COLUMN {col} {coltype}"
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # v1.8 – richer tracker columns on applications
    for col, coltype in [
        ("status",                   "TEXT"),
        ("profile_id",               "INTEGER"),
        ("follow_up_date",           "TEXT"),
        ("recommendation_ids_json",  "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE applications ADD COLUMN {col} {coltype}")
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # v1.9 – evidence_items (Evidence Bank)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS evidence_items (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at            TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at            TEXT    NOT NULL DEFAULT (datetime('now')),
            title                 TEXT    NOT NULL,
            raw_text              TEXT    NOT NULL,
            source_type           TEXT    NOT NULL DEFAULT 'other',
            skill_tags            TEXT    NOT NULL DEFAULT '[]',
            domain_tags           TEXT    NOT NULL DEFAULT '[]',
            business_problem_tags TEXT    NOT NULL DEFAULT '[]',
            evidence_strength     TEXT    NOT NULL DEFAULT 'adjacent',
            allowed_uses          TEXT    NOT NULL DEFAULT '[]',
            confidence            TEXT,
            notes                 TEXT,
            profile_id            INTEGER
        )
    """)
    conn.commit()

    # v2.0 – candidate_assessments (Candidate Assessment)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candidate_assessments (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at           TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at           TEXT    NOT NULL DEFAULT (datetime('now')),
            source_type          TEXT    NOT NULL DEFAULT 'manual',
            source_label         TEXT,
            assessment_kind      TEXT    NOT NULL DEFAULT 'working_assessment',
            raw_text             TEXT    NOT NULL DEFAULT '',
            strengths            TEXT    NOT NULL DEFAULT '[]',
            growth_areas         TEXT    NOT NULL DEFAULT '[]',
            demonstrated_skills  TEXT    NOT NULL DEFAULT '[]',
            demonstrated_domains TEXT    NOT NULL DEFAULT '[]',
            work_style           TEXT,
            role_fit             TEXT,
            confidence           TEXT,
            allowed_uses         TEXT    NOT NULL DEFAULT '[]',
            is_preferred         INTEGER NOT NULL DEFAULT 0,
            profile_id           INTEGER
        )
    """)
    conn.commit()

    # v2.1 – prompt metadata on candidate_assessments
    for col, coltype in [
        ("prompt_type",    "TEXT"),
        ("prompt_version", "TEXT"),
        ("source_model",   "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE candidate_assessments ADD COLUMN {col} {coltype}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already present


# Keep the private alias so any internal callers are not broken.
_apply_migrations = apply_migrations


def init_db() -> None:
    """Create all tables from schema.sql if they don't already exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    with get_conn() as conn:
        conn.executescript(schema)
        apply_migrations(conn)
    print(f"[db] Database ready at {DB_PATH}")


if __name__ == "__main__":
    init_db()
