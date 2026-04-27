"""
tests/test_api.py — Smoke tests for the FastAPI JSON endpoints.

Uses FastAPI TestClient so no server needs to be running.
Each test gets its own temp-file SQLite DB because TestClient runs routes in a
separate thread and SQLite in-memory connections are not thread-safe.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    source_url TEXT, company TEXT, title TEXT,
    location TEXT, remote_policy TEXT,
    raw_text TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'new'
);
CREATE TABLE IF NOT EXISTS candidate_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    version TEXT NOT NULL DEFAULT '1.0',
    profile_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS fit_assessments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    profile_id INTEGER,
    assessed_at TEXT NOT NULL DEFAULT (datetime('now')),
    overall_score REAL, verdict TEXT, confidence TEXT,
    scores_json TEXT, evidence_json TEXT
);
CREATE TABLE IF NOT EXISTS generated_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    generated_at TEXT NOT NULL DEFAULT (datetime('now')),
    asset_type TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    label TEXT, metadata_json TEXT,
    base_resume_id INTEGER, assessment_id INTEGER, base_cl_id INTEGER
);
CREATE TABLE IF NOT EXISTS project_recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    project_title TEXT NOT NULL DEFAULT '',
    recommendation_type TEXT, why_this_matches TEXT,
    business_problem TEXT, target_gap_or_signal TEXT,
    stack_json TEXT, scoped_version TEXT,
    measurable_outcomes_json TEXT, resume_value TEXT,
    implementation_notes TEXT, label TEXT,
    assessment_id INTEGER, metadata_json TEXT
);
CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    applied_at TEXT, platform TEXT,
    resume_asset_id INTEGER, cover_letter_asset_id INTEGER,
    notes TEXT, last_updated TEXT,
    status TEXT, profile_id INTEGER,
    follow_up_date TEXT, recommendation_ids_json TEXT
);
CREATE TABLE IF NOT EXISTS extraction_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    extracted_at TEXT NOT NULL DEFAULT (datetime('now')),
    extraction_confidence TEXT,
    extraction_notes_json TEXT,
    seniority TEXT, min_years_experience INTEGER, max_years_experience INTEGER,
    logistics_json TEXT, ats_keywords_json TEXT, summary_json TEXT
);
CREATE TABLE IF NOT EXISTS extracted_requirements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    category TEXT NOT NULL, requirement TEXT NOT NULL, source_phrase TEXT,
    extracted_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS base_resumes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    label TEXT NOT NULL DEFAULT 'default',
    raw_text TEXT NOT NULL,
    normalized_json TEXT NOT NULL DEFAULT '{}',
    section_count INTEGER NOT NULL DEFAULT 0,
    bullet_count INTEGER NOT NULL DEFAULT 0,
    skill_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS resume_bullets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    resume_id INTEGER NOT NULL REFERENCES base_resumes(id) ON DELETE CASCADE,
    section TEXT NOT NULL, text TEXT NOT NULL,
    skills_json TEXT NOT NULL DEFAULT '[]', source_line INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS base_cover_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    label TEXT NOT NULL DEFAULT 'default',
    raw_text TEXT NOT NULL,
    normalized_json TEXT NOT NULL DEFAULT '{}',
    fragment_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cover_letter_fragments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cover_letter_id INTEGER NOT NULL REFERENCES base_cover_letters(id) ON DELETE CASCADE,
    kind TEXT NOT NULL, text TEXT NOT NULL, source_line INTEGER NOT NULL
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
);
"""


def _make_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    from app.db import apply_migrations
    apply_migrations(conn)
    return conn


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def setup_conn(db_path):
    """Connection used by the test to seed data (same thread)."""
    conn = _make_conn(db_path)
    yield conn
    conn.close()


@pytest.fixture
def client(db_path, setup_conn):
    """TestClient with get_conn patched to open a new connection to the temp file DB.

    Each API call gets its own connection so SQLite's per-thread check passes.
    setup_conn must be passed so the DB is initialised before the client starts.
    """
    from app import api as api_module

    original_get_conn = api_module.get_conn

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

    c = TestClient(api_module.app)
    yield c

    api_module.get_conn = original_get_conn


@pytest.fixture
def job_id(setup_conn):
    cur = setup_conn.execute(
        "INSERT INTO jobs (raw_text, title, company, remote_policy, status) VALUES (?,?,?,?,?)",
        ("Senior Engineer at Acme", "Senior Engineer", "Acme", "remote", "new"),
    )
    setup_conn.commit()
    return cur.lastrowid


# ── GET /api/jobs ─────────────────────────────────────────────────────────────

def test_list_jobs_empty(client):
    r = client.get("/api/jobs")
    assert r.status_code == 200
    assert r.json() == []


def test_list_jobs_returns_jobs(client, job_id):
    r = client.get("/api/jobs")
    assert r.status_code == 200
    jobs = r.json()
    assert len(jobs) == 1
    assert jobs[0]["id"] == job_id
    assert jobs[0]["title"] == "Senior Engineer"
    assert jobs[0]["company"] == "Acme"


def test_list_jobs_verdict_null_when_no_assessment(client, job_id):
    r = client.get("/api/jobs")
    assert r.json()[0]["verdict"] is None


def test_list_jobs_includes_verdict_when_assessed(client, setup_conn, job_id):
    setup_conn.execute(
        "INSERT INTO fit_assessments (job_id, verdict, overall_score, confidence) "
        "VALUES (?,?,?,?)",
        (job_id, "strong_fit", 0.85, "high"),
    )
    setup_conn.commit()
    r = client.get("/api/jobs")
    assert r.json()[0]["verdict"] == "strong_fit"
    assert abs(r.json()[0]["overall_score"] - 0.85) < 0.001


# ── GET /api/jobs/{id}/package ────────────────────────────────────────────────

def test_get_package_404_for_missing_job(client):
    r = client.get("/api/jobs/9999/package")
    assert r.status_code == 404


def test_get_package_returns_job_basics(client, job_id):
    r = client.get(f"/api/jobs/{job_id}/package")
    assert r.status_code == 200
    data = r.json()
    assert data["job_id"] == job_id
    assert data["job_title"] == "Senior Engineer"
    assert data["job_company"] == "Acme"
    assert data["job_status"] == "new"


def test_get_package_no_assessment_fields_null(client, job_id):
    r = client.get(f"/api/jobs/{job_id}/package")
    data = r.json()
    assert data["assessment_id"] is None
    assert data["verdict"] is None
    assert data["direct_evidence"] == []


def test_get_package_with_assessment(client, setup_conn, job_id):
    ev = json.dumps({"direct_evidence": ["python"], "adjacent_evidence": ["kafka"],
                     "unsupported_gaps": ["k8s"]})
    setup_conn.execute(
        "INSERT INTO fit_assessments (job_id, verdict, overall_score, confidence, evidence_json) "
        "VALUES (?,?,?,?,?)",
        (job_id, "reach_but_viable", 0.62, "medium", ev),
    )
    setup_conn.commit()
    r = client.get(f"/api/jobs/{job_id}/package")
    data = r.json()
    assert data["verdict"] == "reach_but_viable"
    assert data["direct_evidence"] == ["python"]
    assert data["adjacent_evidence"] == ["kafka"]
    assert data["unsupported_gaps"] == ["k8s"]


def test_get_package_no_assets_returns_null(client, job_id):
    r = client.get(f"/api/jobs/{job_id}/package")
    data = r.json()
    assert data["resume"] is None
    assert data["cover_letter"] is None


def test_get_package_includes_full_resume_content(client, setup_conn, job_id):
    setup_conn.execute(
        "INSERT INTO generated_assets (job_id, asset_type, content, label) VALUES (?,?,?,?)",
        (job_id, "resume", "# Resume\nBullet one", "v1"),
    )
    setup_conn.commit()
    r = client.get(f"/api/jobs/{job_id}/package")
    resume = r.json()["resume"]
    assert resume is not None
    assert resume["content"] == "# Resume\nBullet one"
    assert resume["label"] == "v1"


def test_get_package_includes_full_cl_content(client, setup_conn, job_id):
    setup_conn.execute(
        "INSERT INTO generated_assets (job_id, asset_type, content, label) VALUES (?,?,?,?)",
        (job_id, "cover_letter", "Dear Hiring Manager", "v1"),
    )
    setup_conn.commit()
    r = client.get(f"/api/jobs/{job_id}/package")
    cl = r.json()["cover_letter"]
    assert cl is not None
    assert cl["content"] == "Dear Hiring Manager"


def test_get_package_no_decision_gives_null_status(client, job_id):
    r = client.get(f"/api/jobs/{job_id}/package")
    app_rec = r.json()["application"]
    assert app_rec["application_id"] is None
    assert app_rec["status"] is None


def test_get_package_with_decision(client, setup_conn, job_id):
    setup_conn.execute(
        "INSERT INTO applications (job_id, status, notes, last_updated) VALUES (?,?,?,?)",
        (job_id, "hold", "Waiting for referral", "2026-04-24 10:00:00"),
    )
    setup_conn.commit()
    r = client.get(f"/api/jobs/{job_id}/package")
    app_rec = r.json()["application"]
    assert app_rec["status"] == "hold"
    assert app_rec["notes"] == "Waiting for referral"


def test_get_package_recommendations(client, setup_conn, job_id):
    setup_conn.execute(
        "INSERT INTO project_recommendations "
        "(job_id, project_title, recommendation_type, target_gap_or_signal) "
        "VALUES (?,?,?,?)",
        (job_id, "K8s Demo", "new_project", "kubernetes"),
    )
    setup_conn.commit()
    r = client.get(f"/api/jobs/{job_id}/package")
    recs = r.json()["recommendations"]
    assert len(recs) == 1
    assert recs[0]["title"] == "K8s Demo"
    assert recs[0]["target_gap_or_signal"] == "kubernetes"


# ── POST /api/jobs/{id}/decision ──────────────────────────────────────────────

def test_post_decision_apply(client, job_id):
    r = client.post(f"/api/jobs/{job_id}/decision",
                    json={"status": "apply"})
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert isinstance(r.json()["application_id"], int)


def test_post_decision_hold_with_notes(client, job_id):
    r = client.post(f"/api/jobs/{job_id}/decision",
                    json={"status": "hold", "notes": "waiting", "platform": "LinkedIn"})
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_post_decision_skip(client, job_id):
    r = client.post(f"/api/jobs/{job_id}/decision", json={"status": "skip"})
    assert r.status_code == 200


def test_post_decision_invalid_status_422(client, job_id):
    r = client.post(f"/api/jobs/{job_id}/decision", json={"status": "banana"})
    assert r.status_code == 422


def test_post_decision_missing_job_404(client):
    r = client.post("/api/jobs/9999/decision", json={"status": "apply"})
    assert r.status_code == 404


def test_post_decision_updates_job_status(client, setup_conn, job_id):
    client.post(f"/api/jobs/{job_id}/decision", json={"status": "apply"})
    row = setup_conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "applied"


def test_post_decision_reflected_in_package(client, job_id):
    client.post(f"/api/jobs/{job_id}/decision",
                json={"status": "hold", "notes": "paused", "follow_up_date": "2026-06-01"})
    r = client.get(f"/api/jobs/{job_id}/package")
    app_rec = r.json()["application"]
    assert app_rec["status"] == "hold"
    assert app_rec["notes"] == "paused"
    assert app_rec["follow_up_date"] == "2026-06-01"


# ── POST /api/jobs (create job) ───────────────────────────────────────────────

JD_TEXT = """\
Senior Python Engineer — Fully Remote

We are looking for a Senior Python Engineer to join our team.

Requirements:
- 5+ years Python experience
- FastAPI or Django
- PostgreSQL
- Docker
- Experience with CI/CD pipelines

Nice to have:
- Kubernetes
- Redis
- Apache Kafka
"""

def test_create_job_returns_201(client):
    r = client.post("/api/jobs", json={"raw_text": JD_TEXT})
    assert r.status_code == 201


def test_create_job_returns_job_id(client):
    r = client.post("/api/jobs", json={"raw_text": JD_TEXT})
    data = r.json()
    assert data["ok"] is True
    assert isinstance(data["job_id"], int)
    assert data["job_id"] > 0


def test_create_job_extracted_is_true(client):
    r = client.post("/api/jobs", json={"raw_text": JD_TEXT})
    assert r.json()["extracted"] is True


def test_create_job_with_metadata(client):
    r = client.post("/api/jobs", json={
        "raw_text": JD_TEXT,
        "company": "Acme Corp",
        "title": "Senior Python Engineer",
        "location": "Remote, USA",
        "source_url": "https://example.com/jobs/1",
        "remote_policy": "remote",
    })
    assert r.status_code == 201
    job_id = r.json()["job_id"]

    r2 = client.get(f"/api/jobs/{job_id}/package")
    assert r2.status_code == 200
    data = r2.json()
    assert data["job_company"] == "Acme Corp"
    assert data["job_title"] == "Senior Python Engineer"
    assert data["job_location"] == "Remote, USA"


def test_create_job_appears_in_list(client):
    client.post("/api/jobs", json={"raw_text": JD_TEXT, "company": "TestCo"})
    r = client.get("/api/jobs")
    companies = [j["company"] for j in r.json()]
    assert "TestCo" in companies


def test_create_job_empty_text_422(client):
    r = client.post("/api/jobs", json={"raw_text": "   "})
    assert r.status_code == 422


def test_create_job_assessed_false_without_profile(client, tmp_path, monkeypatch):
    # Ensure no profile file exists for this test
    fake_profile = tmp_path / "candidate_profile.json"
    monkeypatch.setattr("app.services.profile_loader.DEFAULT_PROFILE", fake_profile)
    monkeypatch.setattr("app.api.DEFAULT_PROFILE", fake_profile)
    r = client.post("/api/jobs", json={"raw_text": JD_TEXT})
    assert r.status_code == 201
    assert r.json()["assessed"] is False


def test_create_job_package_has_extraction(client):
    r = client.post("/api/jobs", json={"raw_text": JD_TEXT})
    job_id = r.json()["job_id"]
    # If extraction ran, the package endpoint works
    r2 = client.get(f"/api/jobs/{job_id}/package")
    assert r2.status_code == 200


def test_create_job_detects_remote_policy_from_text(client):
    r = client.post("/api/jobs", json={"raw_text": "Fully remote role at Acme.\n" + JD_TEXT})
    job_id = r.json()["job_id"]
    pkg = client.get(f"/api/jobs/{job_id}/package").json()
    assert pkg["job_remote_policy"] == "remote"


# ── GET /api/profile ──────────────────────────────────────────────────────────

def test_get_profile_returns_dict(client):
    r = client.get("/api/profile")
    assert r.status_code == 200
    data = r.json()
    assert "personal" in data
    assert "skills" in data
    assert "job_targets" in data


def test_get_profile_includes_completeness(client):
    r = client.get("/api/profile")
    assert "_completeness" in r.json()
    assert 0.0 <= r.json()["_completeness"] <= 1.0


# ── POST /api/profile ─────────────────────────────────────────────────────────

MINIMAL_PROFILE = {
    "version": "1.1",
    "personal": {"name": "Jane Smith", "location": "NYC", "linkedin": "", "github": ""},
    "job_targets": {
        "titles": ["Senior Engineer"],
        "seniority_self_assessed": "senior",
        "desired_remote_policy": "remote",
        "willing_to_relocate": False,
        "work_authorization": "us_citizen",
    },
    "skills": {
        "languages": [{"name": "Python", "years": 5, "evidence": "direct"}],
        "frameworks": [],
        "databases": [{"name": "PostgreSQL", "years": 3, "evidence": "direct"}],
        "cloud": [],
        "tools": [],
        "practices": [],
    },
    "domains": [{"name": "data engineering", "evidence": "adjacent"}],
    "experience": [],
    "education": [],
    "certifications": [],
    "hard_constraints": {"no_travel": False, "no_equity_only": False, "min_salary_usd": None},
}


def test_save_profile_ok(client, tmp_path, monkeypatch):
    fake_profile = tmp_path / "candidate_profile.json"
    monkeypatch.setattr("app.services.profile_loader.DEFAULT_PROFILE", fake_profile)
    monkeypatch.setattr("app.api.DEFAULT_PROFILE", fake_profile)
    r = client.post("/api/profile", json=MINIMAL_PROFILE)
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_save_profile_returns_completeness(client, tmp_path, monkeypatch):
    fake_profile = tmp_path / "candidate_profile.json"
    monkeypatch.setattr("app.services.profile_loader.DEFAULT_PROFILE", fake_profile)
    monkeypatch.setattr("app.api.DEFAULT_PROFILE", fake_profile)
    r = client.post("/api/profile", json=MINIMAL_PROFILE)
    assert 0.0 <= r.json()["completeness"] <= 1.0


def test_save_profile_missing_keys_422(client):
    r = client.post("/api/profile", json={"version": "1.1"})
    assert r.status_code == 422


def test_save_profile_written_to_disk(client, tmp_path, monkeypatch):
    fake_profile = tmp_path / "candidate_profile.json"
    monkeypatch.setattr("app.services.profile_loader.DEFAULT_PROFILE", fake_profile)
    monkeypatch.setattr("app.api.DEFAULT_PROFILE", fake_profile)
    client.post("/api/profile", json=MINIMAL_PROFILE)
    assert fake_profile.exists()
    saved = json.loads(fake_profile.read_text())
    assert saved["personal"]["name"] == "Jane Smith"


# ── POST /api/ingest/resume ───────────────────────────────────────────────────

SAMPLE_RESUME = """\
Jane Smith | jane@example.com

EXPERIENCE

Senior Software Engineer — Acme Corp (2022–present)
- Built Python microservices with FastAPI handling 10k req/s
- Migrated PostgreSQL database with zero downtime
- Deployed Docker containers on Kubernetes

SKILLS
Python, FastAPI, PostgreSQL, Docker, Kubernetes
"""


def test_ingest_resume_ok(client):
    r = client.post("/api/ingest/resume", json={"text": SAMPLE_RESUME})
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["resume_id"] > 0
    assert data["bullet_count"] > 0


def test_ingest_resume_empty_text_422(client):
    r = client.post("/api/ingest/resume", json={"text": "  "})
    assert r.status_code == 422


def test_ingest_resume_label_stored(client):
    r = client.post("/api/ingest/resume", json={"text": SAMPLE_RESUME, "label": "v2"})
    assert r.json()["label"] == "v2"


# ── POST /api/ingest/cover-letter ────────────────────────────────────────────

SAMPLE_CL = """\
Dear Hiring Manager,

I am excited to apply for the Senior Python Engineer position.

Over five years building production Python systems with FastAPI and PostgreSQL,
I have consistently delivered reliable, well-tested APIs.

I look forward to discussing how I can contribute to your team.

Best regards,
Jane Smith
"""


def test_ingest_cl_ok(client):
    r = client.post("/api/ingest/cover-letter", json={"text": SAMPLE_CL})
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["cl_id"] > 0
    assert data["fragment_count"] > 0


def test_ingest_cl_empty_text_422(client):
    r = client.post("/api/ingest/cover-letter", json={"text": ""})
    assert r.status_code == 422


# ── GET /api/ingest/status ────────────────────────────────────────────────────

def test_ingest_status_nothing_ingested(client):
    r = client.get("/api/ingest/status")
    assert r.status_code == 200
    data = r.json()
    assert data["has_resume"] is False
    assert data["has_cover_letter"] is False


def test_ingest_status_after_resume(client):
    client.post("/api/ingest/resume", json={"text": SAMPLE_RESUME})
    r = client.get("/api/ingest/status")
    data = r.json()
    assert data["has_resume"] is True
    assert data["resume_id"] is not None
    assert data["resume_bullets"] > 0


def test_ingest_status_after_cl(client):
    client.post("/api/ingest/cover-letter", json={"text": SAMPLE_CL})
    r = client.get("/api/ingest/status")
    data = r.json()
    assert data["has_cover_letter"] is True
    assert data["cl_id"] is not None
    assert data["cl_fragments"] > 0


def test_ingest_status_both_ingested(client):
    client.post("/api/ingest/resume", json={"text": SAMPLE_RESUME})
    client.post("/api/ingest/cover-letter", json={"text": SAMPLE_CL})
    r = client.get("/api/ingest/status")
    data = r.json()
    assert data["has_resume"] is True
    assert data["has_cover_letter"] is True


# ── GET /api/evidence ─────────────────────────────────────────────────────────

SAMPLE_EVIDENCE = {
    "title":             "Led migration to microservices",
    "raw_text":          "Migrated monolith to 12 services, cutting deploy time 40%.",
    "source_type":       "resume_bullet",
    "evidence_strength": "direct",
    "allowed_uses":      ["resume", "cover_letter"],
    "skill_tags":        ["python", "docker"],
}


def test_list_evidence_empty(client):
    r = client.get("/api/evidence")
    assert r.status_code == 200
    assert r.json() == []


def test_list_evidence_returns_items(client):
    client.post("/api/evidence", json=SAMPLE_EVIDENCE)
    r = client.get("/api/evidence")
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["title"] == "Led migration to microservices"


def test_list_evidence_filter_by_source_type(client):
    client.post("/api/evidence", json={**SAMPLE_EVIDENCE, "source_type": "brag_note"})
    client.post("/api/evidence", json={**SAMPLE_EVIDENCE, "source_type": "resume_bullet"})
    r = client.get("/api/evidence?source_type=brag_note")
    items = r.json()
    assert len(items) == 1
    assert items[0]["source_type"] == "brag_note"


def test_list_evidence_filter_by_strength(client):
    client.post("/api/evidence", json={**SAMPLE_EVIDENCE, "evidence_strength": "direct"})
    client.post("/api/evidence", json={**SAMPLE_EVIDENCE, "evidence_strength": "adjacent"})
    r = client.get("/api/evidence?evidence_strength=adjacent")
    items = r.json()
    assert len(items) == 1
    assert items[0]["evidence_strength"] == "adjacent"


# ── POST /api/evidence ────────────────────────────────────────────────────────

def test_create_evidence_201(client):
    r = client.post("/api/evidence", json=SAMPLE_EVIDENCE)
    assert r.status_code == 201


def test_create_evidence_returns_item(client):
    r = client.post("/api/evidence", json=SAMPLE_EVIDENCE)
    data = r.json()
    assert data["item_id"] > 0
    assert data["title"]             == SAMPLE_EVIDENCE["title"]
    assert data["evidence_strength"] == "direct"
    assert set(data["allowed_uses"]) == {"resume", "cover_letter"}
    assert set(data["skill_tags"])   == {"python", "docker"}


def test_create_evidence_normalizes_tags(client):
    r = client.post("/api/evidence", json={
        **SAMPLE_EVIDENCE,
        "skill_tags": ["Python", "PYTHON", " Go "],
    })
    assert r.json()["skill_tags"] == ["python", "go"]


def test_create_evidence_invalid_source_type_422(client):
    r = client.post("/api/evidence", json={**SAMPLE_EVIDENCE, "source_type": "made_up"})
    assert r.status_code == 422


def test_create_evidence_invalid_strength_422(client):
    r = client.post("/api/evidence", json={**SAMPLE_EVIDENCE, "evidence_strength": "invented"})
    assert r.status_code == 422


def test_create_evidence_invalid_allowed_use_422(client):
    r = client.post("/api/evidence", json={**SAMPLE_EVIDENCE, "allowed_uses": ["bad_use"]})
    assert r.status_code == 422


# ── PUT /api/evidence/{item_id} ───────────────────────────────────────────────

def test_update_evidence_ok(client):
    created = client.post("/api/evidence", json=SAMPLE_EVIDENCE).json()
    item_id = created["item_id"]

    updated_body = {**SAMPLE_EVIDENCE, "title": "Updated title", "evidence_strength": "adjacent"}
    r = client.put(f"/api/evidence/{item_id}", json=updated_body)
    assert r.status_code == 200
    data = r.json()
    assert data["title"]             == "Updated title"
    assert data["evidence_strength"] == "adjacent"


def test_update_evidence_not_found_404(client):
    r = client.put("/api/evidence/9999", json=SAMPLE_EVIDENCE)
    assert r.status_code == 404


# ── DELETE /api/evidence/{item_id} ───────────────────────────────────────────

def test_delete_evidence_ok(client):
    created = client.post("/api/evidence", json=SAMPLE_EVIDENCE).json()
    item_id = created["item_id"]

    r = client.delete(f"/api/evidence/{item_id}")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_delete_evidence_removes_from_list(client):
    created = client.post("/api/evidence", json=SAMPLE_EVIDENCE).json()
    item_id = created["item_id"]

    client.delete(f"/api/evidence/{item_id}")
    r = client.get("/api/evidence")
    assert r.json() == []


def test_delete_evidence_not_found_404(client):
    r = client.delete("/api/evidence/9999")
    assert r.status_code == 404


# ── Candidate Assessments API ─────────────────────────────────────────────────

SAMPLE_ASSESSMENT = {
    "source_type":     "claude",
    "source_label":    "Claude session 2026-04-24",
    "assessment_kind": "working_assessment",
    "raw_text":        "Strong systems thinker who ships iteratively.",
    "strengths":       ["systems thinking", "shipping"],
    "allowed_uses":    ["resume", "interview"],
}


def test_list_assessments_empty(client):
    r = client.get("/api/assessments")
    assert r.status_code == 200
    assert r.json() == []


def test_create_assessment_201(client):
    r = client.post("/api/assessments", json=SAMPLE_ASSESSMENT)
    assert r.status_code == 201


def test_create_assessment_returns_data(client):
    r = client.post("/api/assessments", json=SAMPLE_ASSESSMENT)
    data = r.json()
    assert data["id"] > 0
    assert data["source_type"] == "claude"
    assert data["assessment_kind"] == "working_assessment"
    assert "systems thinking" in data["strengths"]
    assert data["is_preferred"] is False


def test_create_assessment_invalid_source_type_422(client):
    r = client.post("/api/assessments", json={**SAMPLE_ASSESSMENT, "source_type": "bad_bot"})
    assert r.status_code == 422


def test_create_assessment_invalid_kind_422(client):
    r = client.post("/api/assessments", json={**SAMPLE_ASSESSMENT, "assessment_kind": "random"})
    assert r.status_code == 422


def test_list_assessments_filter_source_type(client):
    client.post("/api/assessments", json={**SAMPLE_ASSESSMENT, "source_type": "claude"})
    client.post("/api/assessments", json={**SAMPLE_ASSESSMENT, "source_type": "chatgpt"})
    r = client.get("/api/assessments?source_type=claude")
    items = r.json()
    assert len(items) == 1
    assert items[0]["source_type"] == "claude"


def test_list_assessments_filter_kind(client):
    client.post("/api/assessments", json={**SAMPLE_ASSESSMENT, "assessment_kind": "working_assessment"})
    client.post("/api/assessments", json={**SAMPLE_ASSESSMENT, "assessment_kind": "growth_assessment"})
    r = client.get("/api/assessments?assessment_kind=growth_assessment")
    items = r.json()
    assert len(items) == 1
    assert items[0]["assessment_kind"] == "growth_assessment"


def test_update_assessment_ok(client):
    created = client.post("/api/assessments", json=SAMPLE_ASSESSMENT).json()
    aid = created["id"]
    r = client.put(f"/api/assessments/{aid}", json={**SAMPLE_ASSESSMENT, "raw_text": "Updated."})
    assert r.status_code == 200
    assert r.json()["raw_text"] == "Updated."


def test_update_assessment_not_found_404(client):
    r = client.put("/api/assessments/9999", json=SAMPLE_ASSESSMENT)
    assert r.status_code == 404


def test_delete_assessment_ok(client):
    created = client.post("/api/assessments", json=SAMPLE_ASSESSMENT).json()
    aid = created["id"]
    r = client.delete(f"/api/assessments/{aid}")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_delete_assessment_not_found_404(client):
    r = client.delete("/api/assessments/9999")
    assert r.status_code == 404


def test_set_preferred_marks_assessment(client):
    a1 = client.post("/api/assessments", json=SAMPLE_ASSESSMENT).json()
    a2 = client.post("/api/assessments", json=SAMPLE_ASSESSMENT).json()
    r = client.post(f"/api/assessments/{a2['id']}/set-preferred")
    assert r.status_code == 200
    assert r.json()["is_preferred"] is True


def test_set_preferred_clears_others(client):
    a1 = client.post("/api/assessments", json=SAMPLE_ASSESSMENT).json()
    a2 = client.post("/api/assessments", json=SAMPLE_ASSESSMENT).json()
    client.post(f"/api/assessments/{a1['id']}/set-preferred")
    client.post(f"/api/assessments/{a2['id']}/set-preferred")
    r = client.get("/api/assessments")
    items = {item["id"]: item for item in r.json()}
    assert items[a1["id"]]["is_preferred"] is False
    assert items[a2["id"]]["is_preferred"] is True


def test_get_preferred_returns_marked(client):
    created = client.post("/api/assessments", json=SAMPLE_ASSESSMENT).json()
    client.post(f"/api/assessments/{created['id']}/set-preferred")
    r = client.get("/api/assessments/preferred")
    assert r.status_code == 200
    assert r.json()["id"] == created["id"]


def test_get_preferred_returns_null_when_none(client):
    r = client.get("/api/assessments/preferred")
    assert r.status_code == 200
    assert r.json() is None
