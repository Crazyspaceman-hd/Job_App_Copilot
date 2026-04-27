"""
tests/test_tracker.py — Unit tests for app/services/tracker.py
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from app.db import apply_migrations
from app.services.tracker import (
    VALID_STATUSES,
    ApplicationPackage,
    ApplicationRecord,
    AssetRef,
    RecommendationRef,
    load_application_package,
    load_latest_decision,
    save_application_decision,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    source_url    TEXT,
    raw_text      TEXT    NOT NULL DEFAULT '',
    title         TEXT,
    company       TEXT,
    remote_policy TEXT,
    status        TEXT    NOT NULL DEFAULT 'new'
);

CREATE TABLE IF NOT EXISTS candidate_profiles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    version     TEXT    NOT NULL DEFAULT '1.0',
    profile_json TEXT   NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS fit_assessments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    profile_id    INTEGER REFERENCES candidate_profiles(id),
    assessed_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    overall_score REAL,
    verdict       TEXT,
    confidence    TEXT,
    scores_json   TEXT,
    evidence_json TEXT
);

CREATE TABLE IF NOT EXISTS generated_assets (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    generated_at TEXT    NOT NULL DEFAULT (datetime('now')),
    asset_type   TEXT    NOT NULL,
    content      TEXT    NOT NULL DEFAULT '',
    label        TEXT,
    metadata_json TEXT,
    base_resume_id INTEGER,
    assessment_id  INTEGER,
    base_cl_id     INTEGER
);

CREATE TABLE IF NOT EXISTS project_recommendations (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id                   INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at               TEXT    NOT NULL DEFAULT (datetime('now')),
    project_title            TEXT    NOT NULL DEFAULT '',
    recommendation_type      TEXT,
    why_this_matches         TEXT,
    business_problem         TEXT,
    target_gap_or_signal     TEXT,
    stack_json               TEXT,
    scoped_version           TEXT,
    measurable_outcomes_json TEXT,
    resume_value             TEXT,
    implementation_notes     TEXT,
    label                    TEXT,
    assessment_id            INTEGER,
    metadata_json            TEXT
);

CREATE TABLE IF NOT EXISTS applications (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id                 INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    applied_at             TEXT,
    platform               TEXT,
    resume_asset_id        INTEGER REFERENCES generated_assets(id),
    cover_letter_asset_id  INTEGER REFERENCES generated_assets(id),
    notes                  TEXT,
    last_updated           TEXT,
    status                 TEXT,
    profile_id             INTEGER,
    follow_up_date         TEXT,
    recommendation_ids_json TEXT
);
"""


@pytest.fixture
def mem_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    apply_migrations(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_job_id(mem_conn):
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company, remote_policy, status) "
        "VALUES (?, ?, ?, ?, ?)",
        ("Senior Python Engineer at Acme", "Senior Python Engineer", "Acme Corp", "remote", "new"),
    )
    mem_conn.commit()
    return cur.lastrowid


@pytest.fixture
def job_with_assets(mem_conn, sample_job_id):
    """Job that has an assessment, resume, cover letter, and recommendation."""
    jid = sample_job_id

    # assessment
    cur = mem_conn.execute(
        "INSERT INTO fit_assessments (job_id, overall_score, verdict, confidence, evidence_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (jid, 0.82, "strong_fit", "high",
         json.dumps({"direct_evidence": ["python", "fastapi"],
                     "adjacent_evidence": ["kafka"],
                     "unsupported_gaps": ["kubernetes"]})),
    )
    mem_conn.commit()

    # resume asset
    cur = mem_conn.execute(
        "INSERT INTO generated_assets (job_id, asset_type, content, label) VALUES (?, ?, ?, ?)",
        (jid, "resume", "# My Resume\n- Built APIs", "targeted"),
    )
    mem_conn.commit()

    # cover letter asset
    mem_conn.execute(
        "INSERT INTO generated_assets (job_id, asset_type, content, label) VALUES (?, ?, ?, ?)",
        (jid, "cover_letter", "Dear Hiring Manager,\nI am excited...", "targeted"),
    )
    mem_conn.commit()

    # recommendation
    mem_conn.execute(
        "INSERT INTO project_recommendations "
        "(job_id, project_title, recommendation_type, target_gap_or_signal, business_problem) "
        "VALUES (?, ?, ?, ?, ?)",
        (jid, "Kubernetes Operator Demo", "new_project", "kubernetes",
         "Demonstrate container orchestration skills"),
    )
    mem_conn.commit()

    return jid


# ── VALID_STATUSES ────────────────────────────────────────────────────────────

def test_valid_statuses_contains_expected_values():
    assert VALID_STATUSES == {"apply", "hold", "skip"}


# ── save_application_decision — happy paths ───────────────────────────────────

def test_save_apply_returns_int(mem_conn, sample_job_id):
    app_id = save_application_decision(sample_job_id, mem_conn, "apply")
    assert isinstance(app_id, int)
    assert app_id > 0


def test_save_hold_returns_int(mem_conn, sample_job_id):
    app_id = save_application_decision(sample_job_id, mem_conn, "hold")
    assert app_id > 0


def test_save_skip_returns_int(mem_conn, sample_job_id):
    app_id = save_application_decision(sample_job_id, mem_conn, "skip")
    assert app_id > 0


def test_apply_sets_applied_at(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "apply")
    row = mem_conn.execute(
        "SELECT applied_at FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["applied_at"] is not None


def test_hold_leaves_applied_at_null(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold")
    row = mem_conn.execute(
        "SELECT applied_at FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["applied_at"] is None


def test_skip_leaves_applied_at_null(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "skip")
    row = mem_conn.execute(
        "SELECT applied_at FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["applied_at"] is None


def test_notes_persisted(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold", notes="Waiting for referral")
    row = mem_conn.execute(
        "SELECT notes FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["notes"] == "Waiting for referral"


def test_follow_up_date_persisted(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "apply", follow_up_date="2026-05-01")
    row = mem_conn.execute(
        "SELECT follow_up_date FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["follow_up_date"] == "2026-05-01"


def test_platform_persisted(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "apply", platform="LinkedIn")
    row = mem_conn.execute(
        "SELECT platform FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["platform"] == "LinkedIn"


def test_status_persisted_in_applications(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "skip")
    row = mem_conn.execute(
        "SELECT status FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["status"] == "skip"


# ── jobs.status mirroring ─────────────────────────────────────────────────────

def test_apply_mirrors_to_jobs_applied(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "apply")
    row = mem_conn.execute("SELECT status FROM jobs WHERE id = ?", (sample_job_id,)).fetchone()
    assert row["status"] == "applied"


def test_hold_mirrors_to_jobs_reviewing(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold")
    row = mem_conn.execute("SELECT status FROM jobs WHERE id = ?", (sample_job_id,)).fetchone()
    assert row["status"] == "reviewing"


def test_skip_mirrors_to_jobs_archived(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "skip")
    row = mem_conn.execute("SELECT status FROM jobs WHERE id = ?", (sample_job_id,)).fetchone()
    assert row["status"] == "archived"


# ── auto-linking ──────────────────────────────────────────────────────────────

def test_auto_links_latest_resume(mem_conn, job_with_assets):
    jid = job_with_assets
    resume_id = mem_conn.execute(
        "SELECT id FROM generated_assets WHERE job_id = ? AND asset_type = 'resume' "
        "ORDER BY id DESC LIMIT 1",
        (jid,),
    ).fetchone()["id"]

    save_application_decision(jid, mem_conn, "apply")
    row = mem_conn.execute(
        "SELECT resume_asset_id FROM applications WHERE job_id = ?", (jid,)
    ).fetchone()
    assert row["resume_asset_id"] == resume_id


def test_auto_links_latest_cover_letter(mem_conn, job_with_assets):
    jid = job_with_assets
    cl_id = mem_conn.execute(
        "SELECT id FROM generated_assets WHERE job_id = ? AND asset_type = 'cover_letter' "
        "ORDER BY id DESC LIMIT 1",
        (jid,),
    ).fetchone()["id"]

    save_application_decision(jid, mem_conn, "apply")
    row = mem_conn.execute(
        "SELECT cover_letter_asset_id FROM applications WHERE job_id = ?", (jid,)
    ).fetchone()
    assert row["cover_letter_asset_id"] == cl_id


def test_auto_links_latest_recommendations(mem_conn, job_with_assets):
    jid = job_with_assets
    rec_id = mem_conn.execute(
        "SELECT id FROM project_recommendations WHERE job_id = ? ORDER BY id DESC LIMIT 1",
        (jid,),
    ).fetchone()["id"]

    save_application_decision(jid, mem_conn, "apply")
    row = mem_conn.execute(
        "SELECT recommendation_ids_json FROM applications WHERE job_id = ?", (jid,)
    ).fetchone()
    ids = json.loads(row["recommendation_ids_json"])
    assert rec_id in ids


def test_explicit_resume_id_overrides_auto_link(mem_conn, sample_job_id):
    # Insert a resume
    cur = mem_conn.execute(
        "INSERT INTO generated_assets (job_id, asset_type, content) VALUES (?, 'resume', 'x')",
        (sample_job_id,),
    )
    mem_conn.commit()
    explicit_id = cur.lastrowid

    save_application_decision(sample_job_id, mem_conn, "apply", resume_asset_id=explicit_id)
    row = mem_conn.execute(
        "SELECT resume_asset_id FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["resume_asset_id"] == explicit_id


def test_no_resume_auto_link_gives_null(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "apply")
    row = mem_conn.execute(
        "SELECT resume_asset_id FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["resume_asset_id"] is None


def test_no_cl_auto_link_gives_null(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "apply")
    row = mem_conn.execute(
        "SELECT cover_letter_asset_id FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert row["cover_letter_asset_id"] is None


def test_no_recommendations_gives_empty_json(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "apply")
    row = mem_conn.execute(
        "SELECT recommendation_ids_json FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()
    assert json.loads(row["recommendation_ids_json"]) == []


# ── error cases ───────────────────────────────────────────────────────────────

def test_invalid_status_raises(mem_conn, sample_job_id):
    with pytest.raises(ValueError, match="Invalid status"):
        save_application_decision(sample_job_id, mem_conn, "banana")


def test_missing_job_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        save_application_decision(9999, mem_conn, "apply")


# ── multiple decisions append ─────────────────────────────────────────────────

def test_multiple_decisions_append_rows(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold")
    save_application_decision(sample_job_id, mem_conn, "apply")
    count = mem_conn.execute(
        "SELECT COUNT(*) FROM applications WHERE job_id = ?", (sample_job_id,)
    ).fetchone()[0]
    assert count == 2


def test_second_decision_overwrites_job_status(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold")
    save_application_decision(sample_job_id, mem_conn, "apply")
    row = mem_conn.execute("SELECT status FROM jobs WHERE id = ?", (sample_job_id,)).fetchone()
    assert row["status"] == "applied"


# ── load_latest_decision ──────────────────────────────────────────────────────

def test_load_latest_decision_returns_dict(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold", notes="paused")
    dec = load_latest_decision(sample_job_id, mem_conn)
    assert isinstance(dec, dict)
    assert dec["status"] == "hold"
    assert dec["notes"] == "paused"


def test_load_latest_decision_returns_most_recent(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold")
    save_application_decision(sample_job_id, mem_conn, "apply", notes="final")
    dec = load_latest_decision(sample_job_id, mem_conn)
    assert dec["status"] == "apply"
    assert dec["notes"] == "final"


def test_load_latest_decision_no_row_returns_none(mem_conn, sample_job_id):
    dec = load_latest_decision(sample_job_id, mem_conn)
    assert dec is None


# ── load_application_package — job basics ─────────────────────────────────────

def test_load_package_raises_for_missing_job(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        load_application_package(9999, mem_conn)


def test_load_package_job_basics(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert pkg.job_id == sample_job_id
    assert pkg.job_title == "Senior Python Engineer"
    assert pkg.job_company == "Acme Corp"
    assert pkg.job_remote_policy == "remote"
    assert pkg.job_status == "new"


# ── load_application_package — assessment ────────────────────────────────────

def test_load_package_no_assessment_fields_are_none(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert pkg.assessment_id is None
    assert pkg.verdict is None
    assert pkg.overall_score is None


def test_load_package_with_assessment(mem_conn, job_with_assets):
    pkg = load_application_package(job_with_assets, mem_conn)
    assert pkg.assessment_id is not None
    assert pkg.verdict == "strong_fit"
    assert abs(pkg.overall_score - 0.82) < 0.001
    assert pkg.confidence == "high"
    assert "python" in pkg.direct_evidence
    assert "kafka" in pkg.adjacent_evidence
    assert "kubernetes" in pkg.unsupported_gaps


# ── load_application_package — assets ────────────────────────────────────────

def test_load_package_no_resume_is_none(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert pkg.resume is None


def test_load_package_with_resume(mem_conn, job_with_assets):
    pkg = load_application_package(job_with_assets, mem_conn)
    assert pkg.resume is not None
    assert isinstance(pkg.resume, AssetRef)
    assert pkg.resume.asset_type == "resume"
    assert pkg.resume.label == "targeted"
    assert "My Resume" in pkg.resume.content_preview


def test_load_package_no_cover_letter_is_none(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert pkg.cover_letter is None


def test_load_package_with_cover_letter(mem_conn, job_with_assets):
    pkg = load_application_package(job_with_assets, mem_conn)
    assert pkg.cover_letter is not None
    assert isinstance(pkg.cover_letter, AssetRef)
    assert pkg.cover_letter.asset_type == "cover_letter"


def test_resume_content_preview_capped_at_200(mem_conn, sample_job_id):
    long_content = "x" * 500
    mem_conn.execute(
        "INSERT INTO generated_assets (job_id, asset_type, content) VALUES (?, 'resume', ?)",
        (sample_job_id, long_content),
    )
    mem_conn.commit()
    pkg = load_application_package(sample_job_id, mem_conn)
    assert len(pkg.resume.content_preview) == 200


# ── load_application_package — recommendations ───────────────────────────────

def test_load_package_no_recommendations_is_empty_list(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert pkg.recommendations == []


def test_load_package_with_recommendations(mem_conn, job_with_assets):
    pkg = load_application_package(job_with_assets, mem_conn)
    assert len(pkg.recommendations) == 1
    rec = pkg.recommendations[0]
    assert isinstance(rec, RecommendationRef)
    assert rec.title == "Kubernetes Operator Demo"
    assert rec.recommendation_type == "new_project"
    assert rec.target_gap_or_signal == "kubernetes"


def test_load_package_recommendations_capped_at_10(mem_conn, sample_job_id):
    for i in range(15):
        mem_conn.execute(
            "INSERT INTO project_recommendations (job_id, project_title) VALUES (?, ?)",
            (sample_job_id, f"Project {i}"),
        )
    mem_conn.commit()
    pkg = load_application_package(sample_job_id, mem_conn)
    assert len(pkg.recommendations) <= 10


# ── load_application_package — application record ────────────────────────────

def test_load_package_no_decision_gives_empty_record(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert isinstance(pkg.application, ApplicationRecord)
    assert pkg.application.application_id is None
    assert pkg.application.status is None


def test_load_package_with_decision(mem_conn, sample_job_id):
    app_id = save_application_decision(
        sample_job_id, mem_conn, "apply",
        notes="Good fit", platform="LinkedIn", follow_up_date="2026-05-15",
    )
    pkg = load_application_package(sample_job_id, mem_conn)
    assert pkg.application.application_id == app_id
    assert pkg.application.status == "apply"
    assert pkg.application.notes == "Good fit"
    assert pkg.application.platform == "LinkedIn"
    assert pkg.application.follow_up_date == "2026-05-15"
    assert pkg.application.last_updated is not None


def test_load_package_returns_latest_decision(mem_conn, sample_job_id):
    save_application_decision(sample_job_id, mem_conn, "hold")
    app_id2 = save_application_decision(sample_job_id, mem_conn, "apply", notes="upgraded")
    pkg = load_application_package(sample_job_id, mem_conn)
    assert pkg.application.application_id == app_id2
    assert pkg.application.status == "apply"


# ── ApplicationPackage type check ─────────────────────────────────────────────

def test_load_package_returns_application_package_type(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert isinstance(pkg, ApplicationPackage)


def test_load_package_direct_evidence_is_list(mem_conn, sample_job_id):
    pkg = load_application_package(sample_job_id, mem_conn)
    assert isinstance(pkg.direct_evidence, list)
    assert isinstance(pkg.adjacent_evidence, list)
    assert isinstance(pkg.unsupported_gaps, list)
