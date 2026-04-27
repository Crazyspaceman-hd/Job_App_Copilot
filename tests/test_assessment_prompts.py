"""
tests/test_assessment_prompts.py — Tests for the Candidate Assessment prompt registry.

Coverage:
  - All prompt types are registered and retrievable
  - Prompt text is stable (same call → same result)
  - Full text includes master instruction, context, and output schema sections
  - list_prompts() returns all types, sorted
  - get_prompt() raises ValueError for unknown type / unknown version
  - PromptRecord fields are all populated
  - DB round-trip: prompt_type / prompt_version / source_model persist correctly
  - API: GET /api/assessment-prompts returns all prompts with correct shape
  - API: GET /api/assessment-prompts/{type} returns single prompt
  - API: POST /api/assessments stores and returns prompt metadata
"""

from __future__ import annotations

import sqlite3

import pytest

from app.db import apply_migrations
from app.services.candidate_assessment_prompts import (
    CURRENT_VERSION,
    PROMPT_TYPES,
    PromptRecord,
    get_prompt,
    list_prompts,
)
from app.services.candidate_assessment import create_assessment, get_assessment


# ── Base schema (mirrors test_candidate_assessment.py) ────────────────────────

_BASE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    raw_text      TEXT    NOT NULL DEFAULT '',
    status        TEXT    NOT NULL DEFAULT 'new',
    title         TEXT, company TEXT, location TEXT, remote_policy TEXT, source_url TEXT
);
CREATE TABLE IF NOT EXISTS candidate_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    version TEXT NOT NULL DEFAULT '1.0',
    profile_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS fit_assessments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    candidate_profile_id INTEGER,
    assessed_at TEXT NOT NULL DEFAULT (datetime('now')),
    overall_score REAL
);
CREATE TABLE IF NOT EXISTS generated_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL,
    generated_at TEXT NOT NULL DEFAULT (datetime('now')),
    asset_type TEXT NOT NULL DEFAULT 'resume', content TEXT NOT NULL DEFAULT '',
    label TEXT, metadata_json TEXT, base_resume_id INTEGER,
    assessment_id INTEGER, base_cl_id INTEGER
);
CREATE TABLE IF NOT EXISTS project_recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL,
    recommended_at TEXT NOT NULL DEFAULT (datetime('now')),
    project_title TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL,
    last_updated TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


@pytest.fixture
def mem_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_BASE_SCHEMA)
    apply_migrations(conn)
    return conn


# ── Registry: get_prompt ──────────────────────────────────────────────────────

def test_get_prompt_returns_prompt_record():
    p = get_prompt("working_assessment")
    assert isinstance(p, PromptRecord)


def test_get_prompt_all_types():
    for pt in PROMPT_TYPES:
        p = get_prompt(pt)
        assert p.prompt_type == pt
        assert p.version == CURRENT_VERSION


def test_get_prompt_unknown_type_raises():
    with pytest.raises(ValueError, match="Unknown prompt_type"):
        get_prompt("made_up_assessment")


def test_get_prompt_unknown_version_raises():
    with pytest.raises(ValueError, match="No prompt registered"):
        get_prompt("working_assessment", version="99.99")


def test_get_prompt_explicit_version():
    p = get_prompt("working_assessment", version=CURRENT_VERSION)
    assert p.version == CURRENT_VERSION


# ── Registry: stability ───────────────────────────────────────────────────────

def test_prompt_text_is_stable():
    p1 = get_prompt("working_assessment")
    p2 = get_prompt("working_assessment")
    assert p1.full_text == p2.full_text


def test_all_prompt_texts_are_stable():
    for pt in PROMPT_TYPES:
        assert get_prompt(pt).full_text == get_prompt(pt).full_text


def test_different_types_have_different_text():
    texts = {get_prompt(pt).full_text for pt in PROMPT_TYPES}
    assert len(texts) == len(PROMPT_TYPES), "Each prompt type must have unique text"


# ── Registry: content checks ──────────────────────────────────────────────────

def test_full_text_contains_master_instruction():
    for pt in PROMPT_TYPES:
        assert "TRUTHFULNESS OVER FLATTERY" in get_prompt(pt).full_text
        assert "DIRECT" in get_prompt(pt).full_text
        assert "ADJACENT" in get_prompt(pt).full_text
        assert "INFERRED" in get_prompt(pt).full_text


def test_full_text_contains_output_schema():
    for pt in PROMPT_TYPES:
        text = get_prompt(pt).full_text
        assert '"assessment_kind"' in text
        assert '"strengths"' in text
        assert '"growth_areas"' in text
        assert '"demonstrated_skills"' in text
        assert '"confidence"' in text


def test_full_text_contains_task_keyword():
    task_keywords = {
        "working_assessment":          "Working Assessment",
        "skill_observation":           "Skill Observation",
        "project_delivery_assessment": "Project Delivery",
        "growth_assessment":           "Growth Assessment",
    }
    for pt, keyword in task_keywords.items():
        assert keyword in get_prompt(pt).full_text, f"{pt} missing '{keyword}'"


def test_prompt_has_nonempty_description():
    for pt in PROMPT_TYPES:
        assert get_prompt(pt).description.strip()


def test_prompt_has_nonempty_title():
    for pt in PROMPT_TYPES:
        assert get_prompt(pt).title.strip()


# ── Registry: list_prompts ────────────────────────────────────────────────────

def test_list_prompts_returns_all():
    prompts = list_prompts()
    assert len(prompts) == len(PROMPT_TYPES)


def test_list_prompts_sorted():
    prompts = list_prompts()
    types = [p.prompt_type for p in prompts]
    assert types == sorted(types)


def test_list_prompts_all_have_current_version():
    for p in list_prompts():
        assert p.version == CURRENT_VERSION


def test_list_prompts_unknown_version_returns_empty():
    prompts = list_prompts(version="99.99")
    assert prompts == []


# ── DB round-trip: prompt metadata ────────────────────────────────────────────

def test_create_stores_prompt_type(mem_conn):
    a = create_assessment(
        mem_conn,
        raw_text="Test.",
        prompt_type="working_assessment",
        prompt_version=CURRENT_VERSION,
    )
    fetched = get_assessment(mem_conn, a.id)
    assert fetched.prompt_type == "working_assessment"
    assert fetched.prompt_version == CURRENT_VERSION


def test_create_stores_source_model(mem_conn):
    a = create_assessment(
        mem_conn,
        raw_text="Test.",
        source_model="claude-opus-4",
    )
    fetched = get_assessment(mem_conn, a.id)
    assert fetched.source_model == "claude-opus-4"


def test_prompt_type_defaults_to_none(mem_conn):
    a = create_assessment(mem_conn, raw_text="No prompt.")
    assert a.prompt_type is None
    assert a.prompt_version is None
    assert a.source_model is None


def test_invalid_prompt_type_raises(mem_conn):
    with pytest.raises(ValueError, match="prompt_type"):
        create_assessment(mem_conn, raw_text="x", prompt_type="bad_type")


def test_all_prompt_types_valid_for_create(mem_conn):
    for pt in PROMPT_TYPES:
        a = create_assessment(
            mem_conn,
            raw_text=f"Assessment using {pt}.",
            prompt_type=pt,
            prompt_version=CURRENT_VERSION,
        )
        assert a.prompt_type == pt


# ── API-level tests (via test_api.py fixtures) ────────────────────────────────
# These are pure service/registry; HTTP tests live in test_api.py.

def test_current_version_is_string():
    assert isinstance(CURRENT_VERSION, str)
    assert len(CURRENT_VERSION) > 0


def test_prompt_types_frozenset():
    assert isinstance(PROMPT_TYPES, frozenset)
    expected = {"working_assessment", "skill_observation",
                "project_delivery_assessment", "growth_assessment"}
    assert PROMPT_TYPES == expected
