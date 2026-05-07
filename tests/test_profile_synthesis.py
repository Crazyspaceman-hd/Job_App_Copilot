"""
tests/test_profile_synthesis.py — Unit tests for app/services/profile_synthesis.py
and the GET /api/profile/synthesize endpoint.

Uses direct sqlite3 connections (same-thread) for the service tests, and a
TestClient with the _ConnCtx patch for the endpoint test.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

# ── Shared schema (subset needed by synthesis) ────────────────────────────────

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS base_resumes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    label           TEXT    NOT NULL DEFAULT 'default',
    raw_text        TEXT    NOT NULL,
    normalized_json TEXT    NOT NULL DEFAULT '{}',
    section_count   INTEGER NOT NULL DEFAULT 0,
    bullet_count    INTEGER NOT NULL DEFAULT 0,
    skill_count     INTEGER NOT NULL DEFAULT 0
);
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
);
CREATE TABLE IF NOT EXISTS pr_sources (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT    NOT NULL DEFAULT (datetime('now')),
    title      TEXT    NOT NULL DEFAULT '',
    raw_text   TEXT    NOT NULL,
    source_type TEXT   NOT NULL DEFAULT 'other'
);
CREATE TABLE IF NOT EXISTS pr_observations (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at            TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at            TEXT    NOT NULL DEFAULT (datetime('now')),
    source_id             INTEGER NOT NULL REFERENCES pr_sources(id) ON DELETE CASCADE,
    text                  TEXT    NOT NULL,
    skill_tags            TEXT    NOT NULL DEFAULT '[]',
    domain_tags           TEXT    NOT NULL DEFAULT '[]',
    business_problem_tags TEXT    NOT NULL DEFAULT '[]',
    evidence_strength     TEXT    NOT NULL DEFAULT 'adjacent',
    confidence            TEXT    NOT NULL DEFAULT 'medium',
    allowed_uses          TEXT    NOT NULL DEFAULT '[]',
    review_state          TEXT    NOT NULL DEFAULT 'pending',
    notes                 TEXT
);
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
    profile_id           INTEGER,
    prompt_type          TEXT,
    prompt_version       TEXT,
    source_model         TEXT
);
"""


def _make_conn() -> sqlite3.Connection:
    """Open an in-memory SQLite connection with the synthesis schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def _insert_resume(conn: sqlite3.Connection, bullets: list[list[str]]) -> None:
    """
    Insert a base_resume row whose normalized_json contains a bullet_bank.
    Each element of *bullets* is a list of skill-term strings for one bullet.
    """
    bullet_bank = [{"text": f"bullet {i}", "skills": skills}
                   for i, skills in enumerate(bullets)]
    norm = json.dumps({"bullet_bank": bullet_bank})
    conn.execute(
        "INSERT INTO base_resumes (raw_text, normalized_json) VALUES (?,?)",
        ("raw", norm),
    )
    conn.commit()


def _insert_evidence(conn: sqlite3.Connection,
                     skill_tags: list[str],
                     domain_tags: list[str],
                     strength: str = "adjacent") -> None:
    conn.execute(
        "INSERT INTO evidence_items "
        "(title, raw_text, skill_tags, domain_tags, evidence_strength) VALUES (?,?,?,?,?)",
        ("ev", "raw", json.dumps(skill_tags), json.dumps(domain_tags), strength),
    )
    conn.commit()


def _insert_pr_obs(conn: sqlite3.Connection,
                   skill_tags: list[str],
                   domain_tags: list[str],
                   strength: str = "adjacent",
                   review_state: str = "accepted") -> None:
    src_id = conn.execute(
        "INSERT INTO pr_sources (raw_text) VALUES (?)", ("raw",)
    ).lastrowid
    conn.execute(
        "INSERT INTO pr_observations "
        "(source_id, text, skill_tags, domain_tags, evidence_strength, review_state) "
        "VALUES (?,?,?,?,?,?)",
        (src_id, "obs", json.dumps(skill_tags), json.dumps(domain_tags), strength, review_state),
    )
    conn.commit()


def _insert_assessment(conn: sqlite3.Connection,
                       skills: list[str],
                       domains: list[str],
                       confidence: str = "high") -> None:
    conn.execute(
        "INSERT INTO candidate_assessments "
        "(demonstrated_skills, demonstrated_domains, confidence) VALUES (?,?,?)",
        (json.dumps(skills), json.dumps(domains), confidence),
    )
    conn.commit()


# ── Import the service ────────────────────────────────────────────────────────

from app.services.profile_synthesis import synthesize_profile


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestEmptyDB:
    def test_empty_db_returns_empty_result(self):
        conn = _make_conn()
        result = synthesize_profile(conn)
        assert result.skills_found == 0
        assert result.languages == []
        assert result.frameworks == []
        assert result.databases == []
        assert result.cloud == []
        assert result.tools == []
        assert result.practices == []
        assert result.domains == []
        assert result.sources_used == []


class TestResumeBullets:
    def test_two_bullets_same_term_gives_direct(self):
        conn = _make_conn()
        # "python" appears in 2 bullets → direct
        _insert_resume(conn, [["python"], ["python"]])
        result = synthesize_profile(conn)
        langs = {s.name: s for s in result.languages}
        assert "Python" in langs
        assert langs["Python"].level == "direct"

    def test_one_bullet_gives_adjacent(self):
        conn = _make_conn()
        _insert_resume(conn, [["python"]])
        result = synthesize_profile(conn)
        langs = {s.name: s for s in result.languages}
        assert "Python" in langs
        assert langs["Python"].level == "adjacent"

    def test_multiple_distinct_terms_registered(self):
        conn = _make_conn()
        _insert_resume(conn, [["python", "docker"], ["python"]])
        result = synthesize_profile(conn)
        langs  = {s.name: s for s in result.languages}
        tools  = {s.name: s for s in result.tools}
        assert "Python" in langs
        assert langs["Python"].level == "direct"
        assert "Docker" in tools
        assert tools["Docker"].level == "adjacent"

    def test_sources_used_includes_resume(self):
        conn = _make_conn()
        _insert_resume(conn, [["python"]])
        result = synthesize_profile(conn)
        assert any("resume" in s.lower() for s in result.sources_used)

    def test_resume_source_trace_on_skill(self):
        conn = _make_conn()
        _insert_resume(conn, [["python"], ["python"]])
        result = synthesize_profile(conn)
        py = next(s for s in result.languages if s.name == "Python")
        assert any("resume" in src.lower() for src in py.sources)


class TestEvidenceBank:
    def test_direct_strength_gives_direct(self):
        conn = _make_conn()
        _insert_evidence(conn, ["fastapi"], [], strength="direct")
        result = synthesize_profile(conn)
        fws = {s.name: s for s in result.frameworks}
        assert "FastAPI" in fws
        assert fws["FastAPI"].level == "direct"

    def test_adjacent_strength_gives_adjacent(self):
        conn = _make_conn()
        _insert_evidence(conn, ["fastapi"], [], strength="adjacent")
        result = synthesize_profile(conn)
        fws = {s.name: s for s in result.frameworks}
        assert fws["FastAPI"].level == "adjacent"

    def test_inferred_strength_gives_familiar(self):
        conn = _make_conn()
        _insert_evidence(conn, ["fastapi"], [], strength="inferred")
        result = synthesize_profile(conn)
        fws = {s.name: s for s in result.frameworks}
        assert fws["FastAPI"].level == "familiar"

    def test_domain_tag_goes_to_domains(self):
        conn = _make_conn()
        _insert_evidence(conn, [], ["machine learning"], strength="adjacent")
        result = synthesize_profile(conn)
        names = [s.name.lower() for s in result.domains]
        assert any("machine learning" in n for n in names)

    def test_sources_used_includes_evidence_bank(self):
        conn = _make_conn()
        _insert_evidence(conn, ["python"], [], strength="adjacent")
        result = synthesize_profile(conn)
        assert any("evidence bank" in s.lower() for s in result.sources_used)


class TestPRObservations:
    def test_accepted_pr_obs_registered(self):
        conn = _make_conn()
        _insert_pr_obs(conn, ["postgresql"], [], strength="direct", review_state="accepted")
        result = synthesize_profile(conn)
        dbs = {s.name: s for s in result.databases}
        assert "PostgreSQL" in dbs
        assert dbs["PostgreSQL"].level == "direct"

    def test_rejected_pr_obs_ignored(self):
        conn = _make_conn()
        _insert_pr_obs(conn, ["postgresql"], [], strength="direct", review_state="rejected")
        result = synthesize_profile(conn)
        dbs = {s.name: s for s in result.databases}
        assert "PostgreSQL" not in dbs

    def test_pending_pr_obs_ignored(self):
        conn = _make_conn()
        _insert_pr_obs(conn, ["postgresql"], [], strength="direct", review_state="pending")
        result = synthesize_profile(conn)
        dbs = {s.name: s for s in result.databases}
        assert "PostgreSQL" not in dbs

    def test_sources_used_includes_pr(self):
        conn = _make_conn()
        _insert_pr_obs(conn, ["python"], [], strength="adjacent")
        result = synthesize_profile(conn)
        assert any("observation" in s.lower() for s in result.sources_used)


class TestCandidateAssessments:
    def test_high_confidence_gives_adjacent(self):
        conn = _make_conn()
        _insert_assessment(conn, ["Python development"], [], confidence="high")
        result = synthesize_profile(conn)
        langs = {s.name: s for s in result.languages}
        assert "Python" in langs
        assert langs["Python"].level == "adjacent"

    def test_medium_confidence_gives_adjacent(self):
        conn = _make_conn()
        _insert_assessment(conn, ["FastAPI usage"], [], confidence="medium")
        result = synthesize_profile(conn)
        fws = {s.name: s for s in result.frameworks}
        assert "FastAPI" in fws
        assert fws["FastAPI"].level == "adjacent"

    def test_low_confidence_gives_familiar(self):
        conn = _make_conn()
        _insert_assessment(conn, ["Python scripting"], [], confidence="low")
        result = synthesize_profile(conn)
        langs = {s.name: s for s in result.languages}
        assert "Python" in langs
        assert langs["Python"].level == "familiar"

    def test_assessment_never_produces_direct(self):
        """Even repeated assessments with high confidence cannot produce 'direct'."""
        conn = _make_conn()
        _insert_assessment(conn, ["Python"], [], confidence="high")
        _insert_assessment(conn, ["Python"], [], confidence="high")
        result = synthesize_profile(conn)
        langs = {s.name: s for s in result.languages}
        assert "Python" in langs
        assert langs["Python"].level != "direct"

    def test_sources_used_includes_assessment(self):
        conn = _make_conn()
        _insert_assessment(conn, ["Python"], [], confidence="high")
        result = synthesize_profile(conn)
        assert any("assessment" in s.lower() for s in result.sources_used)

    def test_assessment_domain_tags(self):
        conn = _make_conn()
        _insert_assessment(conn, [], ["FinTech Platform"], confidence="high")
        result = synthesize_profile(conn)
        names = [s.name.lower() for s in result.domains]
        assert any("fintech" in n for n in names)


class TestLevelMerging:
    def test_harder_evidence_wins_over_assessment(self):
        """Resume (direct) + assessment (adjacent) → direct."""
        conn = _make_conn()
        _insert_resume(conn, [["python"], ["python"]])   # direct
        _insert_assessment(conn, ["Python"], [], confidence="high")  # adjacent
        result = synthesize_profile(conn)
        langs = {s.name: s for s in result.languages}
        assert langs["Python"].level == "direct"

    def test_evidence_bank_upgrades_resume_adjacent(self):
        """Resume 1 bullet (adjacent) + evidence bank direct → direct."""
        conn = _make_conn()
        _insert_resume(conn, [["fastapi"]])              # adjacent
        _insert_evidence(conn, ["fastapi"], [], strength="direct")  # direct
        result = synthesize_profile(conn)
        fws = {s.name: s for s in result.frameworks}
        assert fws["FastAPI"].level == "direct"

    def test_multiple_sources_merged_in_trace(self):
        """When two sources fire for the same term both appear in sources list."""
        conn = _make_conn()
        _insert_resume(conn, [["python"]])
        _insert_evidence(conn, ["python"], [], strength="adjacent")
        result = synthesize_profile(conn)
        py = next(s for s in result.languages if s.name == "Python")
        joined = " ".join(py.sources).lower()
        assert "resume" in joined
        assert "evidence" in joined

    def test_skills_found_count_correct(self):
        conn = _make_conn()
        _insert_resume(conn, [["python", "docker"]])
        _insert_evidence(conn, ["fastapi"], ["machine learning"])
        result = synthesize_profile(conn)
        # python (lang), docker (tool), fastapi (framework), machine learning (domain) = 4
        assert result.skills_found == 4


class TestSortOrder:
    def test_direct_before_adjacent_before_familiar(self):
        conn = _make_conn()
        _insert_evidence(conn, ["python"], [], strength="inferred")   # familiar
        _insert_evidence(conn, ["django"], [], strength="adjacent")   # adjacent
        _insert_evidence(conn, ["fastapi"], [], strength="direct")    # direct → frameworks
        result = synthesize_profile(conn)
        # frameworks: fastapi=direct, django=adjacent
        assert result.frameworks[0].level == "direct"
        if len(result.frameworks) > 1:
            assert result.frameworks[1].level in ("adjacent", "familiar")


class TestCICDRouting:
    def test_cicd_in_domain_tags_goes_to_practices(self):
        """ci/cd is a 'practice' vocab term; even if placed in domain_tags it should
        land in result.practices (via skill_acc), not result.domains."""
        conn = _make_conn()
        _insert_evidence(conn, [], ["ci/cd"], strength="adjacent")
        result = synthesize_profile(conn)
        practice_names = [s.name for s in result.practices]
        domain_names   = [s.name for s in result.domains]
        assert any("CI/CD" in n or "ci/cd" in n.lower() for n in practice_names), \
            f"expected ci/cd in practices, got practices={practice_names}, domains={domain_names}"


# ── Endpoint test ─────────────────────────────────────────────────────────────

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "synth_test.db")


@pytest.fixture
def setup_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    # Also run the real migrations so all other tables are present
    from app.db import apply_migrations
    apply_migrations(conn)
    yield conn
    conn.close()


@pytest.fixture
def client(db_path, setup_conn):
    from app import api as api_module

    original = api_module.get_conn

    class _ConnCtx:
        def __enter__(self):
            self._conn = sqlite3.connect(db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA foreign_keys = ON")
            return self._conn
        def __exit__(self, *_):
            self._conn.commit()
            self._conn.close()

    api_module.get_conn = _ConnCtx
    c = __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(api_module.app)
    yield c
    api_module.get_conn = original


def test_synthesize_endpoint_empty(client):
    r = client.get("/api/profile/synthesize")
    assert r.status_code == 200
    body = r.json()
    assert body["skills_found"] == 0
    assert body["languages"] == []
    assert body["sources_used"] == []


def test_synthesize_endpoint_with_resume(client, setup_conn):
    bullet_bank = [
        {"text": "Built Python API", "skills": ["python"]},
        {"text": "More Python work", "skills": ["python"]},
        {"text": "Used Docker",      "skills": ["docker"]},
    ]
    norm = json.dumps({"bullet_bank": bullet_bank})
    setup_conn.execute(
        "INSERT INTO base_resumes (raw_text, normalized_json) VALUES (?,?)",
        ("raw", norm),
    )
    setup_conn.commit()

    r = client.get("/api/profile/synthesize")
    assert r.status_code == 200
    body = r.json()
    assert body["skills_found"] >= 2

    langs = {s["name"]: s for s in body["languages"]}
    tools = {s["name"]: s for s in body["tools"]}

    assert "Python" in langs
    assert langs["Python"]["level"] == "direct"
    assert "Docker" in tools
    assert tools["Docker"]["level"] == "adjacent"
    assert len(body["sources_used"]) >= 1
