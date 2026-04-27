"""
tests/test_candidate_assessment.py — Unit tests for the Candidate Assessment service.

Coverage:
  - CRUD round-trips (create / get / list / update / delete)
  - Tag normalization (via normalize_tags)
  - Input validation (invalid source_type, assessment_kind, allowed_uses)
  - Filtering by source_type and assessment_kind
  - set_preferred / get_preferred / clear-others behaviour
  - get_assessments_for_use retrieval hook
"""

from __future__ import annotations

import sqlite3

import pytest

from app.db import apply_migrations
from app.services.candidate_assessment import (
    ALLOWED_USE_VALUES,
    ASSESSMENT_KINDS,
    SOURCE_TYPES,
    CandidateAssessment,
    create_assessment,
    delete_assessment,
    get_assessment,
    get_assessments_for_use,
    get_preferred,
    list_assessments,
    normalize_tags,
    set_preferred,
    update_assessment,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

_BASE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    raw_text      TEXT    NOT NULL DEFAULT '',
    status        TEXT    NOT NULL DEFAULT 'new',
    title         TEXT,
    company       TEXT,
    location      TEXT,
    remote_policy TEXT,
    source_url    TEXT
);
CREATE TABLE IF NOT EXISTS candidate_profiles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    version      TEXT    NOT NULL DEFAULT '1.0',
    profile_json TEXT    NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS fit_assessments (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id               INTEGER NOT NULL,
    candidate_profile_id INTEGER,
    assessed_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    overall_score        REAL
);
CREATE TABLE IF NOT EXISTS generated_assets (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER NOT NULL,
    generated_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    asset_type    TEXT    NOT NULL DEFAULT 'resume',
    content       TEXT    NOT NULL DEFAULT '',
    label         TEXT,
    metadata_json TEXT,
    base_resume_id INTEGER,
    assessment_id  INTEGER,
    base_cl_id     INTEGER
);
CREATE TABLE IF NOT EXISTS project_recommendations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id         INTEGER NOT NULL,
    recommended_at TEXT    NOT NULL DEFAULT (datetime('now')),
    project_title  TEXT    NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS applications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL,
    last_updated TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""


@pytest.fixture
def mem_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_BASE_SCHEMA)
    apply_migrations(conn)
    return conn


def _make(conn: sqlite3.Connection, **kwargs) -> CandidateAssessment:
    defaults = dict(
        source_type     = "claude",
        assessment_kind = "working_assessment",
        raw_text        = "Strong systems thinker who ships iteratively.",
        strengths       = ["systems thinking", "shipping"],
        allowed_uses    = ["resume", "interview"],
    )
    defaults.update(kwargs)
    return create_assessment(conn, **defaults)


# ── normalize_tags ────────────────────────────────────────────────────────────

def test_normalize_strips_whitespace():
    assert normalize_tags([" Python ", " fastapi "]) == ["python", "fastapi"]


def test_normalize_lowercases():
    assert normalize_tags(["Django", "REACT"]) == ["django", "react"]


def test_normalize_deduplicates():
    assert normalize_tags(["python", "Python", "PYTHON"]) == ["python"]


def test_normalize_drops_empty():
    assert normalize_tags(["", "  ", "go"]) == ["go"]


# ── create / get ──────────────────────────────────────────────────────────────

def test_create_returns_dataclass(mem_conn):
    a = _make(mem_conn)
    assert isinstance(a, CandidateAssessment)
    assert a.id > 0


def test_create_round_trip(mem_conn):
    a = _make(mem_conn, source_type="chatgpt", source_label="GPT-4o session")
    fetched = get_assessment(mem_conn, a.id)
    assert fetched.source_type == "chatgpt"
    assert fetched.source_label == "GPT-4o session"
    assert fetched.assessment_kind == "working_assessment"
    assert "systems thinking" in fetched.strengths


def test_create_normalizes_tags(mem_conn):
    a = _make(mem_conn, strengths=["  Python ", "Python", "PYTHON"])
    assert a.strengths == ["python"]


def test_create_defaults(mem_conn):
    a = create_assessment(mem_conn, raw_text="Minimal entry.")
    assert a.source_type == "manual"
    assert a.assessment_kind == "working_assessment"
    assert a.is_preferred is False
    assert a.allowed_uses == []


def test_get_missing_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        get_assessment(mem_conn, 9999)


def test_create_stores_all_fields(mem_conn):
    a = _make(
        mem_conn,
        source_type="gemini",
        assessment_kind="skill_observation",
        growth_areas=["public speaking"],
        demonstrated_skills=["python", "sql"],
        demonstrated_domains=["data engineering"],
        work_style="async-first, deep focus",
        role_fit="IC engineer over manager",
        confidence="high",
        profile_id=42,
    )
    f = get_assessment(mem_conn, a.id)
    assert f.growth_areas == ["public speaking"]
    assert f.demonstrated_skills == ["python", "sql"]
    assert f.demonstrated_domains == ["data engineering"]
    assert f.work_style == "async-first, deep focus"
    assert f.role_fit == "IC engineer over manager"
    assert f.confidence == "high"
    assert f.profile_id == 42


# ── list / filter ─────────────────────────────────────────────────────────────

def test_list_empty(mem_conn):
    assert list_assessments(mem_conn) == []


def test_list_returns_all(mem_conn):
    _make(mem_conn)
    _make(mem_conn)
    assert len(list_assessments(mem_conn)) == 2


def test_list_filter_source_type(mem_conn):
    _make(mem_conn, source_type="claude")
    _make(mem_conn, source_type="chatgpt")
    result = list_assessments(mem_conn, source_type="claude")
    assert len(result) == 1
    assert result[0].source_type == "claude"


def test_list_filter_assessment_kind(mem_conn):
    _make(mem_conn, assessment_kind="working_assessment")
    _make(mem_conn, assessment_kind="growth_assessment")
    result = list_assessments(mem_conn, assessment_kind="growth_assessment")
    assert len(result) == 1
    assert result[0].assessment_kind == "growth_assessment"


def test_list_preferred_first(mem_conn):
    a1 = _make(mem_conn)
    a2 = _make(mem_conn)
    set_preferred(mem_conn, a2.id)
    items = list_assessments(mem_conn)
    assert items[0].id == a2.id


# ── update ────────────────────────────────────────────────────────────────────

def test_update_raw_text(mem_conn):
    a = _make(mem_conn)
    updated = update_assessment(mem_conn, a.id, raw_text="Updated raw text.")
    assert updated.raw_text == "Updated raw text."


def test_update_preserves_other_fields(mem_conn):
    a = _make(mem_conn, source_type="claude", assessment_kind="skill_observation")
    updated = update_assessment(mem_conn, a.id, raw_text="New text.")
    assert updated.source_type == "claude"
    assert updated.assessment_kind == "skill_observation"


def test_update_normalizes_tags(mem_conn):
    a = _make(mem_conn)
    updated = update_assessment(mem_conn, a.id, strengths=["  Leadership ", "leadership"])
    assert updated.strengths == ["leadership"]


def test_update_missing_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        update_assessment(mem_conn, 9999, raw_text="x")


# ── delete ────────────────────────────────────────────────────────────────────

def test_delete_removes_record(mem_conn):
    a = _make(mem_conn)
    delete_assessment(mem_conn, a.id)
    with pytest.raises(ValueError, match="not found"):
        get_assessment(mem_conn, a.id)


def test_delete_missing_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        delete_assessment(mem_conn, 9999)


# ── set_preferred / get_preferred ─────────────────────────────────────────────

def test_set_preferred_marks_one(mem_conn):
    a = _make(mem_conn)
    set_preferred(mem_conn, a.id)
    assert get_assessment(mem_conn, a.id).is_preferred is True


def test_set_preferred_clears_others(mem_conn):
    a1 = _make(mem_conn)
    a2 = _make(mem_conn)
    set_preferred(mem_conn, a1.id)
    set_preferred(mem_conn, a2.id)
    assert get_assessment(mem_conn, a1.id).is_preferred is False
    assert get_assessment(mem_conn, a2.id).is_preferred is True


def test_get_preferred_returns_none_when_empty(mem_conn):
    assert get_preferred(mem_conn) is None


def test_get_preferred_returns_marked(mem_conn):
    a = _make(mem_conn)
    set_preferred(mem_conn, a.id)
    pref = get_preferred(mem_conn)
    assert pref is not None
    assert pref.id == a.id


def test_set_preferred_missing_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        set_preferred(mem_conn, 9999)


# ── validation ────────────────────────────────────────────────────────────────

def test_invalid_source_type_raises(mem_conn):
    with pytest.raises(ValueError, match="source_type"):
        _make(mem_conn, source_type="unknown_bot")


def test_invalid_assessment_kind_raises(mem_conn):
    with pytest.raises(ValueError, match="assessment_kind"):
        _make(mem_conn, assessment_kind="random_kind")


def test_invalid_allowed_use_raises(mem_conn):
    with pytest.raises(ValueError, match="allowed_uses"):
        _make(mem_conn, allowed_uses=["bad_use"])


def test_valid_all_source_types(mem_conn):
    for st in SOURCE_TYPES:
        a = _make(mem_conn, source_type=st)
        assert a.source_type == st


def test_valid_all_assessment_kinds(mem_conn):
    for kind in ASSESSMENT_KINDS:
        a = _make(mem_conn, assessment_kind=kind)
        assert a.assessment_kind == kind


# ── get_assessments_for_use ───────────────────────────────────────────────────

def test_get_for_use_filters_correctly(mem_conn):
    _make(mem_conn, allowed_uses=["resume"])
    _make(mem_conn, allowed_uses=["interview"])
    _make(mem_conn, allowed_uses=["resume", "interview"])
    result = get_assessments_for_use(mem_conn, "resume")
    assert len(result) == 2
    for a in result:
        assert "resume" in a.allowed_uses


def test_get_for_use_preferred_first(mem_conn):
    a1 = _make(mem_conn, allowed_uses=["resume"])
    a2 = _make(mem_conn, allowed_uses=["resume"])
    set_preferred(mem_conn, a2.id)
    result = get_assessments_for_use(mem_conn, "resume")
    assert result[0].id == a2.id


def test_get_for_use_empty_when_no_match(mem_conn):
    _make(mem_conn, allowed_uses=["interview"])
    assert get_assessments_for_use(mem_conn, "cover_letter") == []
