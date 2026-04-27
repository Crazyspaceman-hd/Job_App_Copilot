"""
tests/test_evidence_bank.py — Unit + integration tests for the Evidence Bank service.

Coverage:
  - CRUD round-trips (create / get / list / update / delete)
  - Tag normalization (strip, lowercase, deduplicate, empty-string filtering)
  - Input validation (invalid source_type, evidence_strength, allowed_uses)
  - Filtering by source_type and evidence_strength (SQL and Python paths)
  - get_usable_items() retrieval helper (allowed_use + min_strength)
  - Integration hook: _append_evidence_highlights added to resume markdown
"""

from __future__ import annotations

import sqlite3

import pytest

from app.db import apply_migrations
from app.services.evidence_bank import (
    ALLOWED_USE_VALUES,
    EVIDENCE_STRENGTHS,
    SOURCE_TYPES,
    EvidenceItem,
    create_item,
    delete_item,
    get_item,
    get_usable_items,
    list_items,
    normalize_tags,
    update_item,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

# Minimal tables required by apply_migrations (ALTER TABLE) and the services
# used in end-to-end tests (generate_targeted_resume needs jobs + generated_assets).
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
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL,
    generated_at TEXT    NOT NULL DEFAULT (datetime('now')),
    asset_type   TEXT    NOT NULL DEFAULT 'resume',
    content      TEXT    NOT NULL DEFAULT '',
    label        TEXT,
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
    """In-memory SQLite with base tables + all migrations applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_BASE_SCHEMA)
    apply_migrations(conn)
    return conn


def _make_item(conn: sqlite3.Connection, **kwargs) -> EvidenceItem:
    """Create an evidence item with sensible defaults; override with kwargs."""
    defaults = dict(
        title             = "Led migration to microservices",
        raw_text          = "Migrated monolith to 12 microservices, cutting deploy time 40%.",
        source_type       = "resume_bullet",
        evidence_strength = "direct",
        allowed_uses      = ["resume"],
    )
    defaults.update(kwargs)
    return create_item(conn, **defaults)


# ── normalize_tags ────────────────────────────────────────────────────────────

def test_normalize_strips_whitespace():
    assert normalize_tags([" Python ", " fastapi "]) == ["python", "fastapi"]


def test_normalize_lowercases():
    assert normalize_tags(["FastAPI", "POSTGRESQL"]) == ["fastapi", "postgresql"]


def test_normalize_deduplicates():
    assert normalize_tags(["python", "Python", "PYTHON"]) == ["python"]


def test_normalize_drops_empty_strings():
    assert normalize_tags(["", "  ", "go", ""]) == ["go"]


def test_normalize_preserves_first_occurrence_order():
    result = normalize_tags(["redis", "postgres", "redis"])
    assert result == ["redis", "postgres"]


def test_normalize_empty_list():
    assert normalize_tags([]) == []


# ── create_item / get_item ────────────────────────────────────────────────────

def test_create_returns_evidence_item(mem_conn):
    item = _make_item(mem_conn)
    assert isinstance(item, EvidenceItem)
    assert item.item_id > 0


def test_create_round_trip_basic_fields(mem_conn):
    item = _make_item(mem_conn, title="Auth service", raw_text="Built OAuth2 flow.")
    assert item.title    == "Auth service"
    assert item.raw_text == "Built OAuth2 flow."
    assert item.source_type       == "resume_bullet"
    assert item.evidence_strength == "direct"


def test_create_normalizes_skill_tags(mem_conn):
    item = create_item(
        mem_conn,
        title             = "Python work",
        raw_text          = "wrote a lot of python",
        skill_tags        = ["Python", "FASTAPI", "  python  "],
        evidence_strength = "direct",
        allowed_uses      = ["resume"],
    )
    assert item.skill_tags == ["python", "fastapi"]


def test_create_normalizes_domain_tags(mem_conn):
    item = create_item(
        mem_conn,
        title             = "FinTech work",
        raw_text          = "some text",
        domain_tags       = ["FinTech", "fintech", " Healthcare "],
        evidence_strength = "direct",
        allowed_uses      = [],
    )
    assert item.domain_tags == ["fintech", "healthcare"]


def test_create_normalizes_business_problem_tags(mem_conn):
    item = create_item(
        mem_conn,
        title                 = "perf work",
        raw_text              = "fixed slowdowns",
        business_problem_tags = ["Latency", "LATENCY", "throughput"],
        evidence_strength     = "direct",
        allowed_uses          = [],
    )
    assert item.business_problem_tags == ["latency", "throughput"]


def test_create_stores_all_allowed_uses(mem_conn):
    item = create_item(
        mem_conn,
        title             = "versatile item",
        raw_text          = "text",
        evidence_strength = "direct",
        allowed_uses      = ["resume", "cover_letter", "interview_prep"],
    )
    assert set(item.allowed_uses) == {"resume", "cover_letter", "interview_prep"}


def test_create_stores_confidence_and_notes(mem_conn):
    item = create_item(
        mem_conn,
        title             = "item with meta",
        raw_text          = "text",
        evidence_strength = "adjacent",
        allowed_uses      = [],
        confidence        = "high",
        notes             = "verified in 2024 perf review",
    )
    assert item.confidence == "high"
    assert item.notes      == "verified in 2024 perf review"


def test_create_defaults_source_type_and_strength(mem_conn):
    item = create_item(
        mem_conn,
        title        = "minimal item",
        raw_text     = "some text",
        allowed_uses = [],
    )
    assert item.source_type       == "other"
    assert item.evidence_strength == "adjacent"


def test_get_item_by_id(mem_conn):
    created = _make_item(mem_conn)
    fetched = get_item(mem_conn, created.item_id)
    assert fetched is not None
    assert fetched.item_id == created.item_id
    assert fetched.title   == created.title


def test_get_item_not_found_returns_none(mem_conn):
    assert get_item(mem_conn, 999) is None


# ── list_items ────────────────────────────────────────────────────────────────

def test_list_empty(mem_conn):
    assert list_items(mem_conn) == []


def test_list_returns_all(mem_conn):
    _make_item(mem_conn, title="A")
    _make_item(mem_conn, title="B")
    _make_item(mem_conn, title="C")
    items = list_items(mem_conn)
    assert len(items) == 3


def test_list_ordered_newest_first(mem_conn):
    _make_item(mem_conn, title="First")
    _make_item(mem_conn, title="Second")
    items = list_items(mem_conn)
    assert items[0].title == "Second"
    assert items[1].title == "First"


# ── Filtering ─────────────────────────────────────────────────────────────────

def test_filter_by_source_type(mem_conn):
    _make_item(mem_conn, source_type="resume_bullet")
    _make_item(mem_conn, source_type="brag_note")
    _make_item(mem_conn, source_type="resume_bullet")
    result = list_items(mem_conn, source_type="resume_bullet")
    assert len(result) == 2
    assert all(i.source_type == "resume_bullet" for i in result)


def test_filter_by_evidence_strength(mem_conn):
    _make_item(mem_conn, evidence_strength="direct")
    _make_item(mem_conn, evidence_strength="adjacent")
    _make_item(mem_conn, evidence_strength="direct")
    result = list_items(mem_conn, evidence_strength="direct")
    assert len(result) == 2
    assert all(i.evidence_strength == "direct" for i in result)


def test_filter_combined(mem_conn):
    _make_item(mem_conn, source_type="brag_note", evidence_strength="direct")
    _make_item(mem_conn, source_type="brag_note", evidence_strength="adjacent")
    _make_item(mem_conn, source_type="resume_bullet", evidence_strength="direct")
    result = list_items(mem_conn, source_type="brag_note", evidence_strength="direct")
    assert len(result) == 1
    assert result[0].source_type       == "brag_note"
    assert result[0].evidence_strength == "direct"


def test_filter_returns_all_when_no_filter(mem_conn):
    _make_item(mem_conn)
    _make_item(mem_conn)
    assert len(list_items(mem_conn)) == 2


# ── update_item ───────────────────────────────────────────────────────────────

def test_update_item_changes_fields(mem_conn):
    item = _make_item(mem_conn, title="Old title", raw_text="old text")
    updated = update_item(
        mem_conn,
        item_id           = item.item_id,
        title             = "New title",
        raw_text          = "new text",
        source_type       = "rewrite",
        evidence_strength = "adjacent",
        allowed_uses      = ["cover_letter"],
        skill_tags        = ["python"],
    )
    assert updated.title             == "New title"
    assert updated.raw_text          == "new text"
    assert updated.source_type       == "rewrite"
    assert updated.evidence_strength == "adjacent"
    assert updated.allowed_uses      == ["cover_letter"]
    assert updated.skill_tags        == ["python"]


def test_update_item_persisted_to_db(mem_conn):
    item = _make_item(mem_conn)
    update_item(
        mem_conn,
        item_id           = item.item_id,
        title             = "Persisted update",
        raw_text          = "new text",
        evidence_strength = "direct",
        allowed_uses      = [],
    )
    fetched = get_item(mem_conn, item.item_id)
    assert fetched is not None
    assert fetched.title == "Persisted update"


def test_update_item_not_found_raises(mem_conn):
    with pytest.raises(ValueError, match="not found"):
        update_item(
            mem_conn,
            item_id           = 999,
            title             = "x",
            raw_text          = "y",
            evidence_strength = "direct",
            allowed_uses      = [],
        )


def test_update_normalizes_tags(mem_conn):
    item = _make_item(mem_conn)
    updated = update_item(
        mem_conn,
        item_id           = item.item_id,
        title             = item.title,
        raw_text          = item.raw_text,
        skill_tags        = ["PYTHON", "python", " Go "],
        evidence_strength = "direct",
        allowed_uses      = ["resume"],
    )
    assert updated.skill_tags == ["python", "go"]


# ── delete_item ───────────────────────────────────────────────────────────────

def test_delete_item_removes_row(mem_conn):
    item = _make_item(mem_conn)
    deleted = delete_item(mem_conn, item.item_id)
    assert deleted is True
    assert get_item(mem_conn, item.item_id) is None


def test_delete_item_returns_false_when_missing(mem_conn):
    assert delete_item(mem_conn, 999) is False


def test_delete_removes_only_target(mem_conn):
    a = _make_item(mem_conn, title="keep")
    b = _make_item(mem_conn, title="delete me")
    delete_item(mem_conn, b.item_id)
    remaining = list_items(mem_conn)
    assert len(remaining) == 1
    assert remaining[0].item_id == a.item_id


# ── Validation ────────────────────────────────────────────────────────────────

def test_invalid_source_type_raises(mem_conn):
    with pytest.raises(ValueError, match="source_type"):
        create_item(
            mem_conn,
            title             = "x",
            raw_text          = "y",
            source_type       = "not_a_valid_type",
            evidence_strength = "direct",
            allowed_uses      = [],
        )


def test_invalid_evidence_strength_raises(mem_conn):
    with pytest.raises(ValueError, match="evidence_strength"):
        create_item(
            mem_conn,
            title             = "x",
            raw_text          = "y",
            evidence_strength = "made_up",
            allowed_uses      = [],
        )


def test_invalid_allowed_use_raises(mem_conn):
    with pytest.raises(ValueError, match="allowed_uses"):
        create_item(
            mem_conn,
            title             = "x",
            raw_text          = "y",
            evidence_strength = "direct",
            allowed_uses      = ["resume", "not_a_real_use"],
        )


def test_update_invalid_source_type_raises(mem_conn):
    item = _make_item(mem_conn)
    with pytest.raises(ValueError, match="source_type"):
        update_item(
            mem_conn,
            item_id           = item.item_id,
            title             = "x",
            raw_text          = "y",
            source_type       = "bad_type",
            evidence_strength = "direct",
            allowed_uses      = [],
        )


# ── get_usable_items ──────────────────────────────────────────────────────────

def test_get_usable_all_returns_everything(mem_conn):
    _make_item(mem_conn, allowed_uses=["resume"])
    _make_item(mem_conn, allowed_uses=["cover_letter"])
    assert len(get_usable_items(mem_conn)) == 2


def test_get_usable_filter_by_allowed_use(mem_conn):
    _make_item(mem_conn, allowed_uses=["resume"], title="A")
    _make_item(mem_conn, allowed_uses=["cover_letter"], title="B")
    _make_item(mem_conn, allowed_uses=["resume", "cover_letter"], title="C")

    resume_items = get_usable_items(mem_conn, allowed_use="resume")
    assert len(resume_items) == 2
    assert all("resume" in i.allowed_uses for i in resume_items)

    cl_items = get_usable_items(mem_conn, allowed_use="cover_letter")
    assert len(cl_items) == 2


def test_get_usable_filter_by_min_strength_direct_only(mem_conn):
    _make_item(mem_conn, evidence_strength="direct",   title="D")
    _make_item(mem_conn, evidence_strength="adjacent", title="A")
    _make_item(mem_conn, evidence_strength="inferred", title="I")

    result = get_usable_items(mem_conn, min_strength="direct")
    assert len(result) == 1
    assert result[0].evidence_strength == "direct"


def test_get_usable_filter_by_min_strength_adjacent_includes_direct(mem_conn):
    _make_item(mem_conn, evidence_strength="direct",   title="D")
    _make_item(mem_conn, evidence_strength="adjacent", title="A")
    _make_item(mem_conn, evidence_strength="inferred", title="I")

    result = get_usable_items(mem_conn, min_strength="adjacent")
    assert len(result) == 2
    strengths = {i.evidence_strength for i in result}
    assert strengths == {"direct", "adjacent"}


def test_get_usable_combined_filters(mem_conn):
    _make_item(mem_conn, evidence_strength="direct",   allowed_uses=["resume"])
    _make_item(mem_conn, evidence_strength="adjacent", allowed_uses=["resume"])
    _make_item(mem_conn, evidence_strength="direct",   allowed_uses=["cover_letter"])

    result = get_usable_items(mem_conn, allowed_use="resume", min_strength="direct")
    assert len(result) == 1
    assert result[0].evidence_strength == "direct"
    assert "resume" in result[0].allowed_uses


def test_get_usable_empty_when_no_match(mem_conn):
    _make_item(mem_conn, evidence_strength="inferred", allowed_uses=["interview_prep"])
    result = get_usable_items(mem_conn, allowed_use="resume", min_strength="direct")
    assert result == []


# ── Integration hook: _append_evidence_highlights ────────────────────────────

def test_append_evidence_highlights_adds_section(mem_conn):
    from app.services.resume_tailor import _append_evidence_highlights

    item = _make_item(
        mem_conn,
        title             = "Led cost reduction",
        raw_text          = "Reduced infra spend by $200k/year via spot instances.",
        evidence_strength = "direct",
        allowed_uses      = ["resume"],
    )
    base_md = "# Jane Smith\n\n## Experience\n\n- Did things\n"
    result  = _append_evidence_highlights(base_md, [item])

    assert "## Key Evidence" in result
    assert "Led cost reduction" in result
    assert "Reduced infra spend by $200k/year" in result


def test_append_evidence_highlights_verbatim(mem_conn):
    from app.services.resume_tailor import _append_evidence_highlights

    item = _make_item(
        mem_conn,
        raw_text = "Line one.\nLine two — exact phrasing preserved.",
    )
    result = _append_evidence_highlights("# Name\n", [item])

    assert "Line one." in result
    assert "Line two — exact phrasing preserved." in result


def test_generate_targeted_resume_with_evidence_items(mem_conn):
    """End-to-end: evidence items with direct+resume marking appear in output."""
    from app.services.base_asset_ingest import ingest_resume
    from app.services.resume_tailor import generate_targeted_resume

    raw_resume = (
        "# Jane Smith\n\n"
        "## Experience\n\n"
        "- Built Python microservices handling 10k RPS.\n"
        "- Designed PostgreSQL schemas for reporting pipeline.\n"
    )
    base = ingest_resume(raw_resume, mem_conn)

    evidence = _make_item(
        mem_conn,
        title             = "Cost optimisation",
        raw_text          = "Saved $150k annually by migrating to spot instances.",
        evidence_strength = "direct",
        allowed_uses      = ["resume"],
    )

    profile = {
        "version": "1.0",
        "personal": {"name": "Jane Smith", "location": "NYC", "linkedin": "", "github": ""},
        "job_targets": {
            "titles": ["Senior Engineer"],
            "seniority_self_assessed": "senior",
            "desired_remote_policy": "remote",
            "willing_to_relocate": False,
            "work_authorization": "US Citizen",
        },
        "skills": {
            "languages":  [{"name": "python", "years": 5, "evidence": "direct"}],
            "frameworks": [],
            "databases":  [{"name": "postgresql", "years": 3, "evidence": "direct"}],
            "cloud":      [],
            "tools":      [],
            "practices":  [],
        },
        "domains": [],
        "experience": [],
        "education": [],
        "certifications": [],
        "hard_constraints": {"no_travel": False, "no_equity_only": False, "min_salary_usd": None},
    }

    mem_conn.execute(
        "INSERT INTO jobs (raw_text, status) VALUES (?, 'new')",
        ("We need a Python engineer with PostgreSQL skills.",),
    )
    mem_conn.commit()
    job_id = mem_conn.execute("SELECT MAX(id) FROM jobs").fetchone()[0]

    result = generate_targeted_resume(
        job_id         = job_id,
        conn           = mem_conn,
        profile        = profile,
        base_resume    = base,
        evidence_items = [evidence],
    )
    assert "## Key Evidence" in result.markdown
    assert "Cost optimisation" in result.markdown
    assert "Saved $150k" in result.markdown


def test_generate_targeted_resume_no_evidence_items_unchanged(mem_conn):
    """Without evidence_items, output does not contain Key Evidence section."""
    from app.services.base_asset_ingest import ingest_resume
    from app.services.resume_tailor import generate_targeted_resume

    raw_resume = (
        "# Jane Smith\n\n"
        "## Experience\n\n"
        "- Built Python APIs.\n"
    )
    base = ingest_resume(raw_resume, mem_conn)
    profile = {
        "version": "1.0",
        "personal": {"name": "Jane Smith", "location": "", "linkedin": "", "github": ""},
        "job_targets": {
            "titles": [], "seniority_self_assessed": "", "desired_remote_policy": "",
            "willing_to_relocate": False, "work_authorization": "",
        },
        "skills": {
            "languages":  [{"name": "python", "years": 3, "evidence": "direct"}],
            "frameworks": [], "databases": [], "cloud": [], "tools": [], "practices": [],
        },
        "domains": [], "experience": [], "education": [], "certifications": [],
        "hard_constraints": {"no_travel": False, "no_equity_only": False, "min_salary_usd": None},
    }
    mem_conn.execute(
        "INSERT INTO jobs (raw_text, status) VALUES (?, 'new')", ("Need python.",),
    )
    mem_conn.commit()
    job_id = mem_conn.execute("SELECT MAX(id) FROM jobs").fetchone()[0]

    result = generate_targeted_resume(
        job_id      = job_id,
        conn        = mem_conn,
        profile     = profile,
        base_resume = base,
    )
    assert "## Key Evidence" not in result.markdown
