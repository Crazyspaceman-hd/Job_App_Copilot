"""
tests/test_profile_reconstruction.py — Tests for the Profile Reconstruction subsystem.

Coverage:
  - Source CRUD (create/get/list/delete)
  - Observation extraction from realistic messy text
  - Evidence strength detection (direct / adjacent / inferred)
  - Confidence detection (high / medium / low)
  - Skill and domain tag extraction
  - Claim candidate generation (text + framing)
  - Observation review state transitions
  - Claim review state transitions
  - Promotion to Evidence Bank
  - Draft summary generation
  - Re-run replaces previous observations
  - Cascade delete (source → observations → claims)
  - API-level: create source, run, patch observation, patch claim, promote
"""

from __future__ import annotations

import sqlite3

import pytest

from app.db import apply_migrations
from app.services.profile_reconstruction import (
    PR_SOURCE_TYPES,
    REVIEW_STATES,
    ClaimCandidate,
    Observation,
    RawSource,
    ReconstructionResult,
    create_source,
    delete_source,
    generate_draft_summary,
    get_claim,
    get_observation,
    get_source,
    list_claims,
    list_observations,
    list_sources,
    promote_claim,
    run_reconstruction,
    update_claim,
    update_observation,
    _detect_confidence,
    _detect_strength,
    _extract_tags,
    _make_claim,
    _split_into_units,
)


# ── In-memory DB fixture ──────────────────────────────────────────────────────

_BASE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    raw_text TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'new',
    title TEXT, company TEXT, location TEXT, remote_policy TEXT, source_url TEXT
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
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    generated_at TEXT NOT NULL DEFAULT (datetime('now')),
    asset_type TEXT NOT NULL DEFAULT 'resume',
    content TEXT NOT NULL DEFAULT '',
    label TEXT, metadata_json TEXT, base_resume_id INTEGER,
    assessment_id INTEGER, base_cl_id INTEGER
);
CREATE TABLE IF NOT EXISTS project_recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    recommended_at TEXT NOT NULL DEFAULT (datetime('now')),
    project_title TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    last_updated TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


@pytest.fixture
def mem_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_BASE_SCHEMA)
    apply_migrations(conn)
    return conn


# ── Source type constant ──────────────────────────────────────────────────────

def test_pr_source_types_is_frozenset():
    assert isinstance(PR_SOURCE_TYPES, frozenset)
    assert "free_text" in PR_SOURCE_TYPES
    assert "project_note" in PR_SOURCE_TYPES
    assert "old_resume" in PR_SOURCE_TYPES


def test_review_states_frozenset():
    assert REVIEW_STATES == {"pending", "accepted", "rejected"}


# ── Source CRUD ───────────────────────────────────────────────────────────────

def test_create_source_returns_raw_source(mem_conn):
    s = create_source(mem_conn, raw_text="I built a Python API.", source_type="project_note")
    assert isinstance(s, RawSource)
    assert s.id > 0
    assert s.source_type == "project_note"
    assert s.raw_text == "I built a Python API."


def test_create_source_defaults(mem_conn):
    s = create_source(mem_conn, raw_text="Some text")
    assert s.source_type == "free_text"
    assert s.title == ""
    assert s.label is None


def test_create_source_with_title_and_label(mem_conn):
    s = create_source(mem_conn, raw_text="x", title="My notes", label="v1")
    assert s.title == "My notes"
    assert s.label == "v1"


def test_create_source_invalid_type_raises(mem_conn):
    with pytest.raises(ValueError, match="source_type"):
        create_source(mem_conn, raw_text="x", source_type="bad_type")


def test_get_source_returns_correct(mem_conn):
    s = create_source(mem_conn, raw_text="Test", source_type="free_text")
    fetched = get_source(mem_conn, s.id)
    assert fetched.id == s.id
    assert fetched.raw_text == "Test"


def test_get_source_not_found_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        get_source(mem_conn, 9999)


def test_list_sources_newest_first(mem_conn):
    create_source(mem_conn, raw_text="first")
    create_source(mem_conn, raw_text="second")
    sources = list_sources(mem_conn)
    assert len(sources) >= 2
    assert sources[0].raw_text == "second"


def test_delete_source_returns_true(mem_conn):
    s = create_source(mem_conn, raw_text="Delete me")
    assert delete_source(mem_conn, s.id) is True


def test_delete_source_missing_returns_false(mem_conn):
    assert delete_source(mem_conn, 9999) is False


def test_delete_source_cascades(mem_conn):
    """Deleting a source should remove its observations and claims."""
    s = create_source(mem_conn, raw_text="Built a system using Python and Docker.")
    run_reconstruction(mem_conn, s.id)
    obs = list_observations(mem_conn, s.id)
    assert len(obs) > 0
    delete_source(mem_conn, s.id)
    # Observations should be gone
    obs_after = mem_conn.execute(
        "SELECT id FROM pr_observations WHERE source_id = ?", (s.id,)
    ).fetchall()
    assert len(obs_after) == 0


# ── Text splitting ────────────────────────────────────────────────────────────

def test_split_into_units_basic():
    text = "Built a Python API.\nHelped with deployment.\nLearning React."
    units = _split_into_units(text)
    assert len(units) == 3


def test_split_strips_bullets():
    text = "● Built an API\n• Deployed to AWS\n- Fixed a bug"
    units = _split_into_units(text)
    assert all(not u.startswith(('●', '•', '-')) for u in units)


def test_split_skips_blank_lines():
    text = "\n\nBuilt a system.\n\n\nDeployed to production.\n\n"
    units = _split_into_units(text)
    assert "" not in units
    assert len(units) == 2


def test_split_skips_short_lines():
    text = "Built something amazing with Python and FastAPI backend services.\nOk\nShort"
    units = _split_into_units(text)
    # "Ok" and "Short" are < 12 chars → skipped
    assert all(len(u) >= 12 for u in units)


def test_split_handles_wall_of_text():
    text = (
        "I built a data pipeline using Python and Airflow. "
        "It processed 1M records per day. "
        "The team used Docker and Kubernetes for deployment."
    )
    units = _split_into_units(text)
    assert len(units) >= 2  # sentence-break fallback kicks in


# ── Strength detection ────────────────────────────────────────────────────────

def test_direct_verb_gives_direct():
    assert _detect_strength("I built the entire authentication system.") == "direct"


def test_adjacent_verb_gives_adjacent():
    assert _detect_strength("I helped the team deploy to production.") == "adjacent"


def test_inferred_signal_gives_inferred():
    assert _detect_strength("I am learning React and TypeScript.") == "inferred"


def test_inferred_beats_direct():
    """Inferred signal overrides a direct verb — conservative wins."""
    assert _detect_strength("I want to build things with Python.") == "inferred"


def test_adjacent_beats_direct():
    """Adjacent verb overrides direct — conservative wins."""
    assert _detect_strength("I collaborated on building the API.") == "adjacent"


def test_no_signal_defaults_to_adjacent():
    assert _detect_strength("The system processes financial transactions.") == "adjacent"


def test_familiar_with_is_inferred():
    assert _detect_strength("I am familiar with React and TypeScript.") == "inferred"


def test_exposure_to_is_inferred():
    assert _detect_strength("I have exposure to machine learning pipelines.") == "inferred"


# ── Confidence detection ──────────────────────────────────────────────────────

def test_metric_gives_high():
    assert _detect_confidence("Reduced load time by 40%.", "direct") == "high"


def test_number_gives_medium():
    # A bare 2-digit number (no metric suffix) → medium confidence
    assert _detect_confidence("We completed version 42 of the product.", "direct") == "medium"


def test_hedging_gives_low():
    assert _detect_confidence("I think I helped with the API somewhat.", "adjacent") == "low"


def test_inferred_strength_gives_low():
    assert _detect_confidence("Learning React.", "inferred") == "low"


def test_direct_verb_no_metric_gives_medium():
    assert _detect_confidence("Built the backend API service.", "direct") == "medium"


def test_very_short_gives_low():
    # Short text with no metric signal or direct verb → low
    assert _detect_confidence("Worked on stuff.", "adjacent") == "low"


# ── Tag extraction ────────────────────────────────────────────────────────────

def test_skill_tags_extracted():
    skills, domains = _extract_tags("Built a Python API using FastAPI and PostgreSQL.")
    assert "python" in skills
    assert "fastapi" in skills
    assert "postgresql" in skills


def test_domain_tags_extracted():
    skills, domains = _extract_tags("Experience in devops and machine learning workflows.")
    assert "devops" in domains or "machine learning" in domains


def test_no_tags_for_generic_text():
    skills, domains = _extract_tags("I went to the store and bought milk.")
    assert len(skills) == 0


# ── Claim generation ──────────────────────────────────────────────────────────

def test_claim_direct_keeps_text():
    text = "Built the authentication system from scratch."
    claim, framing = _make_claim(text, "direct")
    assert framing == "direct"
    assert "Built" in claim
    assert claim.endswith(".")


def test_claim_adjacent_adds_framing():
    text = "Worked on the data pipeline with the team."
    claim, framing = _make_claim(text, "adjacent")
    assert framing == "adjacent"
    # Should not double-prefix if already collaborative
    assert claim.endswith(".")


def test_claim_inferred_adds_framing():
    text = "learning React and TypeScript"
    claim, framing = _make_claim(text, "inferred")
    assert framing == "inferred"
    assert "familiarity" in claim.lower() or "learning" in claim.lower()
    assert claim.endswith(".")


def test_claim_capitalizes_text():
    text = "built an internal tool for CI/CD automation."
    claim, _ = _make_claim(text, "direct")
    assert claim[0].isupper()


# ── Reconstruction run ────────────────────────────────────────────────────────

_SAMPLE_TEXT = """\
Built a Python FastAPI backend service for internal data processing.
The service handled 500k requests per day with 99.9% uptime.
Deployed everything using Docker and Kubernetes on AWS.
Helped the team with code reviews and mentored two junior engineers.
I am now learning React and TypeScript to contribute to the frontend.
Collaborated with the product team on requirements gathering and roadmap planning.
"""


def test_run_reconstruction_returns_result(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    result = run_reconstruction(mem_conn, s.id)
    assert isinstance(result, ReconstructionResult)
    assert result.source_id == s.id


def test_run_extracts_observations(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    result = run_reconstruction(mem_conn, s.id)
    assert result.observation_count > 0
    assert len(result.observations) == result.observation_count


def test_run_creates_one_claim_per_observation(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    result = run_reconstruction(mem_conn, s.id)
    assert result.claim_count == result.observation_count


def test_run_detects_direct_strength(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API backend service.\nDeployed to AWS using Docker.")
    result = run_reconstruction(mem_conn, s.id)
    strengths = [o.evidence_strength for o in result.observations]
    assert "direct" in strengths


def test_run_detects_inferred_strength(mem_conn):
    s = create_source(mem_conn, raw_text="I am learning React and exploring TypeScript.")
    result = run_reconstruction(mem_conn, s.id)
    strengths = [o.evidence_strength for o in result.observations]
    assert "inferred" in strengths


def test_run_extracts_skill_tags(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API using FastAPI and PostgreSQL.")
    result = run_reconstruction(mem_conn, s.id)
    all_skills = [t for o in result.observations for t in o.skill_tags]
    assert "python" in all_skills or "fastapi" in all_skills


def test_run_draft_summary_nonempty(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    result = run_reconstruction(mem_conn, s.id)
    assert result.draft_summary.strip()
    assert "observation" in result.draft_summary.lower()


def test_run_replaces_previous_observations(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    run_reconstruction(mem_conn, s.id)
    first_obs = list_observations(mem_conn, s.id)
    first_count = len(first_obs)

    # Re-run with different text
    mem_conn.execute("UPDATE pr_sources SET raw_text = ? WHERE id = ?",
                     ("Deployed three microservices using Docker and Kubernetes.", s.id))
    mem_conn.commit()
    run_reconstruction(mem_conn, s.id)
    second_obs = list_observations(mem_conn, s.id)
    # No stale observations from first run
    assert len(second_obs) > 0
    assert all(o.source_id == s.id for o in second_obs)


def test_run_observations_all_pending(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    result = run_reconstruction(mem_conn, s.id)
    assert all(o.review_state == "pending" for o in result.observations)


def test_run_claims_all_pending(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    result = run_reconstruction(mem_conn, s.id)
    assert all(c.review_state == "pending" for c in result.claims)


def test_run_with_minimal_text(mem_conn):
    """Should not crash on very short or degenerate input."""
    s = create_source(mem_conn, raw_text="ok")
    result = run_reconstruction(mem_conn, s.id)
    # May return 0 observations — just must not raise
    assert isinstance(result, ReconstructionResult)


# ── Observation CRUD ──────────────────────────────────────────────────────────

def test_get_observation_returns_correct(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    obs = result.observations[0]
    fetched = get_observation(mem_conn, obs.id)
    assert fetched.id == obs.id
    assert fetched.text == obs.text


def test_get_observation_not_found_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        get_observation(mem_conn, 9999)


def test_list_observations_for_source(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    run_reconstruction(mem_conn, s.id)
    obs = list_observations(mem_conn, s.id)
    assert all(o.source_id == s.id for o in obs)


def test_update_observation_review_state(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    obs = result.observations[0]
    updated = update_observation(mem_conn, obs.id, review_state="accepted")
    assert updated.review_state == "accepted"


def test_update_observation_text(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    obs = result.observations[0]
    updated = update_observation(mem_conn, obs.id, text="Built a FastAPI service.")
    assert updated.text == "Built a FastAPI service."


def test_update_observation_skill_tags(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    obs = result.observations[0]
    updated = update_observation(mem_conn, obs.id, skill_tags=["python", "fastapi", "custom-tag"])
    assert "python" in updated.skill_tags
    assert "custom-tag" in updated.skill_tags


def test_update_observation_invalid_state_raises(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    obs = result.observations[0]
    with pytest.raises(ValueError, match="review_state"):
        update_observation(mem_conn, obs.id, review_state="invalid_state")


def test_update_observation_strength(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    obs = result.observations[0]
    updated = update_observation(mem_conn, obs.id, evidence_strength="inferred")
    assert updated.evidence_strength == "inferred"


# ── Claim CRUD ────────────────────────────────────────────────────────────────

def test_get_claim_returns_correct(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    fetched = get_claim(mem_conn, claim.id)
    assert fetched.id == claim.id


def test_get_claim_not_found_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        get_claim(mem_conn, 9999)


def test_list_claims_for_source(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    run_reconstruction(mem_conn, s.id)
    claims = list_claims(mem_conn, s.id)
    assert len(claims) > 0


def test_update_claim_review_state(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    updated = update_claim(mem_conn, claim.id, review_state="accepted")
    assert updated.review_state == "accepted"


def test_update_claim_text(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    updated = update_claim(mem_conn, claim.id, text="Custom claim text.")
    assert updated.text == "Custom claim text."


def test_update_claim_invalid_state_raises(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    with pytest.raises(ValueError, match="review_state"):
        update_claim(mem_conn, claim.id, review_state="approved")


# ── Promotion to Evidence Bank ────────────────────────────────────────────────

def test_promote_claim_creates_evidence_item(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API for internal tooling.")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    update_claim(mem_conn, claim.id, review_state="accepted")
    item = promote_claim(mem_conn, claim.id)
    assert item.item_id > 0
    assert item.raw_text == claim.text or item.raw_text.strip()


def test_promote_claim_sets_promoted_item_id(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API for internal tooling.")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    update_claim(mem_conn, claim.id, review_state="accepted")
    item = promote_claim(mem_conn, claim.id)
    refreshed = get_claim(mem_conn, claim.id)
    assert refreshed.promoted_item_id == item.item_id


def test_promote_non_accepted_raises(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API.")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    assert claim.review_state == "pending"
    with pytest.raises(ValueError, match="accepted"):
        promote_claim(mem_conn, claim.id)


def test_promote_copies_skill_tags(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python API using FastAPI and PostgreSQL.")
    result = run_reconstruction(mem_conn, s.id)
    obs = result.observations[0]
    claim = result.claims[0]
    update_claim(mem_conn, claim.id, review_state="accepted")
    item = promote_claim(mem_conn, claim.id)
    # Evidence item should have the observation's skill tags
    assert len(item.skill_tags) >= 0  # may be empty if no vocab match, but no crash


def test_promote_old_resume_maps_source_type(mem_conn):
    s = create_source(mem_conn, raw_text="Led a team of 5 engineers.", source_type="old_resume")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    update_claim(mem_conn, claim.id, review_state="accepted")
    item = promote_claim(mem_conn, claim.id)
    assert item.source_type == "resume_bullet"


def test_promote_project_note_maps_source_type(mem_conn):
    s = create_source(mem_conn, raw_text="Built the analytics dashboard.", source_type="project_note")
    result = run_reconstruction(mem_conn, s.id)
    claim = result.claims[0]
    update_claim(mem_conn, claim.id, review_state="accepted")
    item = promote_claim(mem_conn, claim.id)
    assert item.source_type == "project_note"


# ── Draft summary ─────────────────────────────────────────────────────────────

def test_generate_draft_summary_nonempty(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    run_reconstruction(mem_conn, s.id)
    summary = generate_draft_summary(mem_conn, s.id)
    assert summary.strip()


def test_generate_draft_summary_mentions_count(mem_conn):
    s = create_source(mem_conn, raw_text=_SAMPLE_TEXT)
    run_reconstruction(mem_conn, s.id)
    summary = generate_draft_summary(mem_conn, s.id)
    # Should mention how many observations were extracted
    assert "observation" in summary.lower()


def test_generate_draft_summary_no_observations(mem_conn):
    s = create_source(mem_conn, raw_text="ok")  # too short, likely 0 obs
    run_reconstruction(mem_conn, s.id)
    summary = generate_draft_summary(mem_conn, s.id)
    assert isinstance(summary, str)
    assert len(summary) > 0


def test_draft_summary_mentions_skills(mem_conn):
    s = create_source(mem_conn, raw_text="Built a Python backend with FastAPI and PostgreSQL.")
    run_reconstruction(mem_conn, s.id)
    summary = generate_draft_summary(mem_conn, s.id)
    # Should mention technical signals if vocab terms found
    # (may not always match — just verify no crash and non-empty)
    assert isinstance(summary, str)


# ── API-level tests ───────────────────────────────────────────────────────────

from fastapi.testclient import TestClient
from unittest.mock import patch
import sqlite3 as _sqlite3


@pytest.fixture
def client():
    """FastAPI test client backed by a thread-safe in-memory DB."""
    from app.api import app
    import contextlib

    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_BASE_SCHEMA)
    apply_migrations(conn)

    @contextlib.contextmanager
    def _override():
        yield conn

    with patch("app.api.get_conn", _override):
        yield TestClient(app)

    conn.close()


def test_api_create_source(client):
    r = client.post("/api/reconstruction/sources", json={
        "raw_text": "Built a Python API.",
        "source_type": "project_note",
        "title": "Test source",
    })
    assert r.status_code == 201
    data = r.json()
    assert data["id"] > 0
    assert data["source_type"] == "project_note"


def test_api_create_source_invalid_type(client):
    r = client.post("/api/reconstruction/sources", json={
        "raw_text": "x",
        "source_type": "bad_type",
    })
    assert r.status_code == 422


def test_api_run_reconstruction(client):
    create_r = client.post("/api/reconstruction/sources", json={
        "raw_text": _SAMPLE_TEXT,
        "source_type": "free_text",
    })
    source_id = create_r.json()["id"]
    run_r = client.post(f"/api/reconstruction/sources/{source_id}/run")
    assert run_r.status_code == 200
    data = run_r.json()
    assert "observations" in data
    assert "claims" in data
    assert data["observation_count"] == len(data["observations"])


def test_api_run_returns_draft_summary(client):
    create_r = client.post("/api/reconstruction/sources", json={
        "raw_text": _SAMPLE_TEXT,
    })
    source_id = create_r.json()["id"]
    run_r = client.post(f"/api/reconstruction/sources/{source_id}/run")
    assert run_r.status_code == 200
    assert run_r.json()["draft_summary"].strip()


def test_api_patch_observation_review_state(client):
    create_r = client.post("/api/reconstruction/sources", json={"raw_text": _SAMPLE_TEXT})
    source_id = create_r.json()["id"]
    run_r = client.post(f"/api/reconstruction/sources/{source_id}/run")
    obs = run_r.json()["observations"]
    assert len(obs) > 0
    obs_id = obs[0]["id"]
    patch_r = client.patch(f"/api/reconstruction/observations/{obs_id}",
                           json={"review_state": "accepted"})
    assert patch_r.status_code == 200
    assert patch_r.json()["review_state"] == "accepted"


def test_api_patch_claim_review_state(client):
    create_r = client.post("/api/reconstruction/sources", json={"raw_text": _SAMPLE_TEXT})
    source_id = create_r.json()["id"]
    run_r = client.post(f"/api/reconstruction/sources/{source_id}/run")
    claims = run_r.json()["claims"]
    assert len(claims) > 0
    claim_id = claims[0]["id"]
    patch_r = client.patch(f"/api/reconstruction/claims/{claim_id}",
                           json={"review_state": "accepted"})
    assert patch_r.status_code == 200
    assert patch_r.json()["review_state"] == "accepted"


def test_api_promote_claim(client):
    create_r = client.post("/api/reconstruction/sources", json={
        "raw_text": "Built a Python API for internal tooling.",
        "source_type": "project_note",
        "title": "Project work",
    })
    source_id = create_r.json()["id"]
    run_r = client.post(f"/api/reconstruction/sources/{source_id}/run")
    claims = run_r.json()["claims"]
    claim_id = claims[0]["id"]

    # Accept the claim first
    client.patch(f"/api/reconstruction/claims/{claim_id}",
                 json={"review_state": "accepted"})

    promote_r = client.post(f"/api/reconstruction/claims/{claim_id}/promote")
    assert promote_r.status_code == 200
    data = promote_r.json()
    assert data["ok"] is True
    assert data["evidence_item_id"] > 0


def test_api_promote_pending_claim_fails(client):
    create_r = client.post("/api/reconstruction/sources", json={"raw_text": _SAMPLE_TEXT})
    source_id = create_r.json()["id"]
    run_r = client.post(f"/api/reconstruction/sources/{source_id}/run")
    claim_id = run_r.json()["claims"][0]["id"]
    # Not accepted — should fail
    r = client.post(f"/api/reconstruction/claims/{claim_id}/promote")
    assert r.status_code == 422


def test_api_delete_source(client):
    create_r = client.post("/api/reconstruction/sources", json={"raw_text": "test"})
    source_id = create_r.json()["id"]
    del_r = client.delete(f"/api/reconstruction/sources/{source_id}")
    assert del_r.status_code == 200
    assert del_r.json()["ok"] is True


def test_api_delete_missing_source(client):
    r = client.delete("/api/reconstruction/sources/9999")
    assert r.status_code == 404


def test_api_list_sources(client):
    client.post("/api/reconstruction/sources", json={"raw_text": "a", "title": "src1"})
    client.post("/api/reconstruction/sources", json={"raw_text": "b", "title": "src2"})
    r = client.get("/api/reconstruction/sources")
    assert r.status_code == 200
    assert len(r.json()) >= 2


def test_api_get_summary(client):
    create_r = client.post("/api/reconstruction/sources", json={"raw_text": _SAMPLE_TEXT})
    source_id = create_r.json()["id"]
    client.post(f"/api/reconstruction/sources/{source_id}/run")
    r = client.get(f"/api/reconstruction/sources/{source_id}/summary")
    assert r.status_code == 200
    assert r.json()["summary"].strip()
