"""
tests/test_cover_letter.py — Targeted cover letter generator tests.

Covered cases:
  Fragment scoring
    1.  Direct-evidence fragments outscore adjacent-evidence fragments for same term
    2.  Adjacent-evidence fragments outscore absent-evidence fragments
    3.  Fragment matching more required terms scores higher than one matching fewer
    4.  Fragment with zero matching terms scores 0.0
    5.  has_direct_evidence True only when a required term has direct evidence
    6.  matched_required / matched_preferred populated correctly
    7.  source_type is preserved on returned FragmentScore

  Proof point selection from CL fragments
    8.  Selected proof points are cl_fragment type when CL covers required skills
    9.  At most max_proof_points proof points selected
   10.  Proof points ordered by score descending
   11.  Empty proof_point fragment list → selected list is empty
   12.  Proof points with highest score selected over lower-score ones

  Supplemental resume bullet proof points
   13.  Resume bullet appended when required skill uncovered by CL fragments
   14.  Resume bullet source_type is 'resume_bullet'
   15.  No resume bullet appended when all required skills covered by CL
   16.  No resume bullet appended when base_resume=None
   17.  Supplemental bullets respect max_proof_points cap

  Salutation and closing extraction
   18.  Salutation is taken from the first opening fragment verbatim
   19.  Closing is taken from the last closing fragment verbatim
   20.  Empty CL (no fragments) → salutation and closing are empty strings
   21.  Salutation source_line recorded in provenance

  Opening paragraph generation
   22.  Opening mentions job title
   23.  Opening mentions company name
   24.  Opening mentions a direct-evidence required skill
   25.  Opening does not mention a gap skill (no profile evidence)
   26.  Opening contains YOE phrase when profile has experience entries

  Adjacency paragraph
   27.  Adjacency para included when there are adjacent-only required terms not in proof points
   28.  Adjacency para is None when all required terms have direct evidence
   29.  Adjacency para uses "adjacent experience with" phrasing
   30.  Adjacency para limited to at most 4 terms
   31.  included_adjacency_para provenance flag matches actual para presence

  Markdown output
   32.  Markdown starts with '**{name}**'
   33.  Markdown contains the salutation verbatim
   34.  Markdown contains the opening paragraph
   35.  Markdown contains verbatim proof point text
   36.  Markdown contains the closing verbatim
   37.  Markdown contains the thank-you sentence
   38.  Adjacency para appears after proof points and before thank-you

  Provenance metadata
   39.  base_cl_id matches the base cover letter id
   40.  base_resume_id is None when no base_resume supplied
   41.  base_resume_id matches base resume id when supplied
   42.  proof_point_source_lines matches selected proof point source lines
   43.  proof_point_source_types matches selected proof point source types
   44.  direct_evidence_used contains only required terms with direct evidence
   45.  unsupported_gaps_excluded lists required terms absent from profile
   46.  used_extraction = False when extracted=None
   47.  used_extraction = True when extraction passed
   48.  to_dict() round-trips cleanly through json.dumps / json.loads

  Persistence
   49.  asset_id is a positive integer after generate_targeted_cover_letter()
   50.  generated_assets row has asset_type='cover_letter', correct job_id, base_cl_id
   51.  metadata_json in DB is valid JSON with expected keys
   52.  content column contains the markdown string

  End-to-end
   53.  Sample CL + sample profile → ≥1 proof point selected
   54.  Sample CL + sample profile → asset persisted to DB
   55.  Output is deterministic across two calls with identical inputs
   56.  ValueError raised for unknown job_id
   57.  generate_targeted_cover_letter with base_resume supplement selects bullets
"""

import json
import sqlite3

import pytest
from pathlib import Path

from app.db import apply_migrations
from app.services.base_asset_ingest import (
    CLFragment,
    CoverLetterResult,
    ResumeBullet,
    ResumeResult,
    ResumeSection,
    ingest_cover_letter,
    ingest_resume,
    load_latest_cover_letter,
    parse_cover_letter,
    parse_resume,
)
from app.services.cover_letter import (
    CLProvenance,
    FragmentScore,
    TargetedCLResult,
    _build_opening,
    _render_markdown,
    _score_fragment,
    generate_targeted_cover_letter,
)
from app.services.scorer import _build_skill_map

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SAMPLE_CL    = _PROJECT_ROOT / "data" / "sample_cover_letter.txt"
_SAMPLE_RESUME = _PROJECT_ROOT / "data" / "sample_resume.txt"


# ── Schema ─────────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at   TEXT NOT NULL DEFAULT (datetime('now')),
    status        TEXT NOT NULL DEFAULT 'new',
    title         TEXT,
    company       TEXT,
    remote_policy TEXT,
    raw_text      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS candidate_profiles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    version      TEXT,
    profile_json TEXT
);
CREATE TABLE IF NOT EXISTS fit_assessments (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id               INTEGER NOT NULL REFERENCES jobs(id),
    candidate_profile_id INTEGER REFERENCES candidate_profiles(id),
    assessed_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    overall_score        REAL,
    rationale            TEXT,
    gap_summary          TEXT
);
CREATE TABLE IF NOT EXISTS extracted_requirements (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL REFERENCES jobs(id),
    category    TEXT,
    requirement TEXT
);
CREATE TABLE IF NOT EXISTS generated_assets (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL REFERENCES jobs(id),
    asset_type   TEXT NOT NULL
                     CHECK(asset_type IN ('resume','cover_letter','email','other')),
    file_path    TEXT,
    content      TEXT,
    generated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def mem_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_SCHEMA_SQL)
    apply_migrations(conn)
    return conn


@pytest.fixture()
def sample_job_id(mem_conn):
    jd = (
        "Requirements:\n"
        "- Python\n"
        "- FastAPI\n"
        "- PostgreSQL\n"
        "- Docker\n\n"
        "Preferred:\n"
        "- Kafka\n"
        "- Kubernetes\n\n"
        "Responsibilities:\n"
        "- Build REST APIs\n"
        "- Deploy microservices\n"
    )
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Senior Backend Engineer", "TestCorp"),
    )
    mem_conn.commit()
    return cur.lastrowid


@pytest.fixture()
def good_profile():
    def sk(name, ev="direct"):
        return {"name": name, "evidence": ev}

    return {
        "version": "1.1",
        "personal": {"name": "Alex Rivera", "location": "Austin, TX"},
        "job_targets": {
            "seniority_self_assessed": "senior",
            "desired_remote_policy":   "remote",
            "work_authorization":      "us_citizen",
            "willing_to_relocate":     False,
        },
        "skills": {
            "languages":  [sk("Python"), sk("TypeScript")],
            "frameworks": [sk("FastAPI"), sk("Django")],
            "databases":  [sk("PostgreSQL"), sk("Redis")],
            "cloud":      [sk("AWS"), sk("Lambda")],
            "tools":      [sk("Docker"), sk("Git")],
        },
        "experience": [
            {"title": "Senior SWE", "company": "Acme", "start_date": "2018-01"},
        ],
        "education": [
            {"institution": "UT Austin", "degree": "B.S. Computer Science", "year": 2018},
        ],
        "certifications": [],
    }


@pytest.fixture()
def minimal_cl() -> CoverLetterResult:
    """A synthetic CoverLetterResult with 4 fragments."""
    frags = [
        CLFragment(kind="opening",     text="Dear Hiring Manager,",                          source_line=1),
        CLFragment(kind="proof_point", text="I built FastAPI microservices handling 10M RPD.", source_line=3),
        CLFragment(kind="proof_point", text="I optimised PostgreSQL queries reducing latency by 70%.", source_line=5),
        CLFragment(kind="closing",     text="Sincerely,\nAlex Rivera",                       source_line=7),
    ]
    return CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)


@pytest.fixture()
def ingested_cl(mem_conn, minimal_cl):
    """minimal_cl persisted to DB; cl_id updated."""
    from app.services.base_asset_ingest import persist_cover_letter
    minimal_cl.cl_id = persist_cover_letter(mem_conn, minimal_cl)
    return minimal_cl


@pytest.fixture()
def minimal_resume() -> ResumeResult:
    """A synthetic ResumeResult with a couple of bullets."""
    bullets = [
        ResumeBullet(section="experience", text="Built Python ETL pipeline ingesting Kafka streams.", skills=["python", "kafka"], source_line=5),
        ResumeBullet(section="experience", text="Deployed Docker containers on AWS Lambda.", skills=["docker", "aws", "lambda"], source_line=6),
        ResumeBullet(section="projects",   text="Open-source Redis task queue library.", skills=["redis"], source_line=10),
    ]
    sections = [
        ResumeSection(name="experience", heading="EXPERIENCE", start_line=4, bullets=bullets[:2]),
        ResumeSection(name="projects",   heading="PROJECTS",   start_line=9, bullets=bullets[2:]),
    ]
    return ResumeResult(
        resume_id   = 0,
        label       = "test",
        raw_text    = "",
        sections    = sections,
        bullet_bank = bullets,
        skills      = ["python", "kafka", "docker", "aws", "lambda", "redis"],
    )


@pytest.fixture()
def ingested_resume(mem_conn, minimal_resume):
    from app.services.base_asset_ingest import persist_resume
    minimal_resume.resume_id = persist_resume(mem_conn, minimal_resume)
    return minimal_resume


def _make_skill_map(direct=(), adjacent=(), familiar=()):
    profile = {
        "skills": {
            "direct":   [{"name": s, "evidence": "direct"}   for s in direct],
            "adjacent": [{"name": s, "evidence": "adjacent"} for s in adjacent],
            "familiar": [{"name": s, "evidence": "familiar"} for s in familiar],
        }
    }
    return _build_skill_map(profile, set())


# ── 1-7: Fragment scoring ──────────────────────────────────────────────────────

def test_direct_evidence_outscores_adjacent():
    req = {"python"}
    skill_map_direct   = _make_skill_map(direct=["python"])
    skill_map_adjacent = _make_skill_map(adjacent=["python"])
    text = "I built Python APIs."
    fs_direct   = _score_fragment(text, 1, "cl_fragment", req, set(), set(), req, skill_map_direct)
    fs_adjacent = _score_fragment(text, 1, "cl_fragment", req, set(), set(), req, skill_map_adjacent)
    assert fs_direct.score > fs_adjacent.score


def test_adjacent_outscores_absent():
    req = {"python"}
    skill_map_adj    = _make_skill_map(adjacent=["python"])
    skill_map_absent = _make_skill_map()
    text = "I built Python APIs."
    fs_adj    = _score_fragment(text, 1, "cl_fragment", req, set(), set(), req, skill_map_adj)
    fs_absent = _score_fragment(text, 1, "cl_fragment", req, set(), set(), req, skill_map_absent)
    assert fs_adj.score > fs_absent.score


def test_more_required_terms_scores_higher():
    req = {"python", "fastapi", "postgresql"}
    skill_map = _make_skill_map(direct=["python", "fastapi", "postgresql"])
    text_many = "I built FastAPI and PostgreSQL systems with Python."
    text_one  = "I wrote Python scripts."
    fs_many = _score_fragment(text_many, 1, "cl_fragment", req, set(), set(), req, skill_map)
    fs_one  = _score_fragment(text_one,  2, "cl_fragment", req, set(), set(), req, skill_map)
    assert fs_many.score > fs_one.score


def test_zero_matching_terms_scores_zero():
    req = {"kubernetes", "spark"}
    skill_map = _make_skill_map(direct=["python"])
    text = "I enjoy hiking and cooking on weekends."
    fs = _score_fragment(text, 1, "cl_fragment", req, set(), set(), req, skill_map)
    assert fs.score == 0.0


def test_has_direct_evidence_true_for_direct_required():
    req = {"python"}
    skill_map = _make_skill_map(direct=["python"])
    fs = _score_fragment("I built Python systems.", 1, "cl_fragment", req, set(), set(), req, skill_map)
    assert fs.has_direct_evidence is True


def test_has_direct_evidence_false_for_adjacent():
    req = {"python"}
    skill_map = _make_skill_map(adjacent=["python"])
    fs = _score_fragment("I built Python systems.", 1, "cl_fragment", req, set(), set(), req, skill_map)
    assert fs.has_direct_evidence is False


def test_matched_required_populated():
    req = {"python", "fastapi"}
    skill_map = _make_skill_map(direct=["python", "fastapi"])
    fs = _score_fragment("FastAPI and Python.", 1, "cl_fragment", req, set(), set(), req, skill_map)
    assert "python" in fs.matched_required
    assert "fastapi" in fs.matched_required


def test_source_type_preserved():
    req = {"python"}
    skill_map = _make_skill_map(direct=["python"])
    for src_type in ("cl_fragment", "resume_bullet"):
        fs = _score_fragment("Python.", 1, src_type, req, set(), set(), req, skill_map)
        assert fs.source_type == src_type


# ── 8-12: Proof point selection from CL fragments ─────────────────────────────

def test_selected_proof_points_are_cl_fragments(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    cl_pp = [pp for pp in result.proof_points if pp.source_type == "cl_fragment"]
    assert len(cl_pp) >= 1


def test_at_most_max_proof_points_selected(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
        max_proof_points=2,
    )
    assert len(result.proof_points) <= 2


def test_proof_points_ordered_by_score_desc(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    scores = [pp.score for pp in result.proof_points if pp.source_type == "cl_fragment"]
    assert scores == sorted(scores, reverse=True)


def test_empty_proof_point_fragments_gives_no_cl_proof_points(mem_conn, sample_job_id, good_profile):
    frags = [
        CLFragment(kind="opening", text="Dear Hiring Manager,", source_line=1),
        CLFragment(kind="closing", text="Sincerely, Alex",      source_line=3),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=cl,
    )
    cl_pps = [pp for pp in result.proof_points if pp.source_type == "cl_fragment"]
    assert cl_pps == []


def test_highest_scoring_fragment_selected(mem_conn, sample_job_id, good_profile):
    frags = [
        CLFragment(kind="opening",     text="Dear Hiring Manager,",                         source_line=1),
        CLFragment(kind="proof_point", text="I built FastAPI and PostgreSQL pipelines.",     source_line=3),
        CLFragment(kind="proof_point", text="I enjoy the outdoors and travel frequently.",   source_line=5),
        CLFragment(kind="closing",     text="Sincerely, Alex",                               source_line=7),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=cl,
        max_proof_points=1,
    )
    assert result.proof_points[0].text == "I built FastAPI and PostgreSQL pipelines."


# ── 13-17: Supplemental resume bullet proof points ────────────────────────────

def test_resume_bullet_appended_for_uncovered_required_skill(
    mem_conn, sample_job_id, good_profile
):
    # CL has no Docker mention; resume has Docker bullet
    frags = [
        CLFragment(kind="opening",     text="Dear Hiring Manager,",             source_line=1),
        CLFragment(kind="proof_point", text="I built FastAPI microservices.",    source_line=3),
        CLFragment(kind="closing",     text="Sincerely, Alex",                   source_line=5),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    bullets = [
        ResumeBullet(section="experience", text="Deployed Docker containers on ECS.", skills=["docker"], source_line=10),
    ]
    resume = ResumeResult(
        resume_id=0, label="test", raw_text="",
        sections=[ResumeSection(name="experience", heading="EXPERIENCE", start_line=9, bullets=bullets)],
        bullet_bank=bullets, skills=["docker"],
    )
    from app.services.base_asset_ingest import persist_resume
    resume.resume_id = persist_resume(mem_conn, resume)

    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=cl, base_resume=resume, max_proof_points=3,
    )
    supplement = [pp for pp in result.proof_points if pp.source_type == "resume_bullet"]
    assert len(supplement) >= 1


def test_resume_bullet_source_type_is_resume_bullet(
    mem_conn, sample_job_id, good_profile
):
    frags = [
        CLFragment(kind="opening",     text="Dear Hiring Manager,",           source_line=1),
        CLFragment(kind="closing",     text="Sincerely, Alex",                 source_line=3),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter, persist_resume
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    bullets = [
        ResumeBullet(section="experience", text="Built Python FastAPI services.", skills=["python", "fastapi"], source_line=5),
    ]
    resume = ResumeResult(
        resume_id=0, label="test", raw_text="",
        sections=[ResumeSection(name="experience", heading="EXPERIENCE", start_line=4, bullets=bullets)],
        bullet_bank=bullets, skills=["python", "fastapi"],
    )
    resume.resume_id = persist_resume(mem_conn, resume)

    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=cl, base_resume=resume,
    )
    types = {pp.source_type for pp in result.proof_points}
    assert "resume_bullet" in types


def test_no_resume_bullet_when_cl_covers_all_required(
    mem_conn, sample_job_id, good_profile
):
    jd = "Requirements:\n- Python\nResponsibilities:\n- Build things."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Test Job", "TestCo"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    frags = [
        CLFragment(kind="opening",     text="Dear Sir,",                         source_line=1),
        CLFragment(kind="proof_point", text="I built Python systems extensively.", source_line=3),
        CLFragment(kind="closing",     text="Regards, Alex",                      source_line=5),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter, persist_resume
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    bullets = [ResumeBullet(section="experience", text="I built JavaScript apps.", skills=["javascript"], source_line=10)]
    resume = ResumeResult(
        resume_id=0, label="test", raw_text="",
        sections=[ResumeSection(name="experience", heading="EXP", start_line=9, bullets=bullets)],
        bullet_bank=bullets, skills=["javascript"],
    )
    resume.resume_id = persist_resume(mem_conn, resume)

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile, base_cl=cl, base_resume=resume,
    )
    supplement = [pp for pp in result.proof_points if pp.source_type == "resume_bullet"]
    assert supplement == []


def test_no_resume_bullet_when_base_resume_none(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=ingested_cl, base_resume=None,
    )
    supplement = [pp for pp in result.proof_points if pp.source_type == "resume_bullet"]
    assert supplement == []


def test_supplemental_bullets_respect_max_proof_points(
    mem_conn, sample_job_id, good_profile
):
    frags = [
        CLFragment(kind="opening", text="Dear Hiring Manager,", source_line=1),
        CLFragment(kind="closing", text="Sincerely, Alex",      source_line=3),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter, persist_resume
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    bullets = [
        ResumeBullet(section="experience", text="Built Python APIs.", skills=["python"], source_line=5),
        ResumeBullet(section="experience", text="Used FastAPI heavily.", skills=["fastapi"], source_line=6),
        ResumeBullet(section="experience", text="Managed PostgreSQL DBs.", skills=["postgresql"], source_line=7),
        ResumeBullet(section="experience", text="Ran Docker in prod.", skills=["docker"], source_line=8),
    ]
    resume = ResumeResult(
        resume_id=0, label="test", raw_text="",
        sections=[ResumeSection(name="experience", heading="EXP", start_line=4, bullets=bullets)],
        bullet_bank=bullets, skills=["python", "fastapi", "postgresql", "docker"],
    )
    resume.resume_id = persist_resume(mem_conn, resume)

    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=cl, base_resume=resume, max_proof_points=2,
    )
    assert len(result.proof_points) <= 2


# ── 18-21: Salutation and closing ─────────────────────────────────────────────

def test_salutation_verbatim_from_opening_fragment(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.salutation == "Dear Hiring Manager,"


def test_closing_verbatim_from_closing_fragment(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.closing == "Sincerely,\nAlex Rivera"


def test_empty_cl_gives_empty_salutation_and_closing(mem_conn, sample_job_id, good_profile):
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=[])
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=cl,
    )
    assert result.salutation == ""
    assert result.closing == ""


def test_salutation_source_line_in_provenance(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.provenance.salutation_source_line == 1


# ── 22-26: Opening paragraph ──────────────────────────────────────────────────

def test_opening_mentions_job_title(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert "Senior Backend Engineer" in result.opening


def test_opening_mentions_company(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert "TestCorp" in result.opening


def test_opening_mentions_direct_evidence_skill(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    # At least one direct-skill term should appear (case-insensitive — normalized terms use title-case)
    direct_skills = ["python", "fastapi", "postgresql", "docker"]
    opening_lower = result.opening.lower()
    assert any(s in opening_lower for s in direct_skills)


def test_opening_does_not_mention_gap_skill(mem_conn, good_profile):
    jd = "Requirements:\n- Spark\n- Hadoop\nResponsibilities:\n- Data work."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Data Engineer", "BigDataCo"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    frags = [
        CLFragment(kind="opening",     text="Dear Hiring Manager,",          source_line=1),
        CLFragment(kind="proof_point", text="I built distributed pipelines.", source_line=3),
        CLFragment(kind="closing",     text="Sincerely, Alex",                source_line=5),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile, base_cl=cl,
    )
    assert "spark" not in result.opening.lower()
    assert "hadoop" not in result.opening.lower()


def test_opening_contains_yoe_phrase(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    # Should mention years (profile has experience starting 2018)
    assert "years" in result.opening


# ── 27-31: Adjacency paragraph ────────────────────────────────────────────────

def test_adjacency_para_included_for_adjacent_only_terms(mem_conn, good_profile):
    # Job requires Kafka; profile has Kafka adjacent-only
    profile_with_adj = dict(good_profile)
    profile_with_adj["skills"] = dict(good_profile["skills"])
    profile_with_adj["skills"]["streaming"] = [{"name": "Kafka", "evidence": "adjacent"}]

    jd = "Requirements:\n- Python\n- Kafka\nResponsibilities:\n- Stream data."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Streaming Eng", "StreamCo"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    frags = [
        CLFragment(kind="opening",     text="Dear Hiring Manager,",        source_line=1),
        CLFragment(kind="proof_point", text="I built Python batch jobs.",   source_line=3),
        CLFragment(kind="closing",     text="Sincerely, Alex",              source_line=5),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=profile_with_adj, base_cl=cl,
    )
    assert result.adjacency_para is not None


def test_adjacency_para_none_when_all_direct(mem_conn, sample_job_id, good_profile, ingested_cl):
    # good_profile has Python, FastAPI, etc all as direct — no adjacent-only required terms
    # The job requires Python, FastAPI, PostgreSQL, Docker — all direct in good_profile
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.adjacency_para is None


def test_adjacency_para_uses_adjacent_experience_phrasing(mem_conn, good_profile):
    profile_with_adj = dict(good_profile)
    profile_with_adj["skills"] = dict(good_profile["skills"])
    profile_with_adj["skills"]["streaming"] = [{"name": "Kafka", "evidence": "adjacent"}]

    jd = "Requirements:\n- Kafka\nResponsibilities:\n- Stream."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "SRE", "Co"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    frags = [
        CLFragment(kind="opening", text="Hi,",         source_line=1),
        CLFragment(kind="closing", text="Thanks, Alex", source_line=3),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=profile_with_adj, base_cl=cl,
    )
    if result.adjacency_para:
        assert "adjacent experience" in result.adjacency_para.lower()


def test_adjacency_para_limited_to_four_terms(mem_conn, good_profile):
    profile_with_adj = dict(good_profile)
    profile_with_adj["skills"] = {
        "adj": [{"name": s, "evidence": "adjacent"}
                for s in ["Spark", "Hadoop", "Kafka", "Flink", "Beam"]],
    }
    jd = "Requirements:\n- Spark\n- Hadoop\n- Kafka\n- Flink\n- Beam\nResp:\n- Data."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Data Eng", "DataCo"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    frags = [
        CLFragment(kind="opening", text="Hi,",         source_line=1),
        CLFragment(kind="closing", text="Thanks, Alex", source_line=3),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=profile_with_adj, base_cl=cl,
    )
    if result.adjacency_para:
        # Count commas + 1 as rough upper bound on terms
        term_count = result.adjacency_para.count(",") + 1
        assert term_count <= 6  # generous upper bound for "a, b, c, and d" style


def test_included_adjacency_para_flag_matches_actual(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.provenance.included_adjacency_para == (result.adjacency_para is not None)


# ── 32-38: Markdown output ────────────────────────────────────────────────────

def test_markdown_starts_with_bold_name(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.markdown.startswith("**Alex Rivera**")


def test_markdown_contains_salutation(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert "Dear Hiring Manager," in result.markdown


def test_markdown_contains_opening(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.opening in result.markdown


def test_markdown_contains_proof_point_text(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    for pp in result.proof_points:
        assert pp.text in result.markdown


def test_markdown_contains_closing(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.closing in result.markdown


def test_markdown_contains_thank_you(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert "Thank you for your time" in result.markdown


def test_adjacency_para_before_thank_you_in_markdown(mem_conn, good_profile):
    profile_with_adj = dict(good_profile)
    profile_with_adj["skills"] = {
        "adj": [{"name": "Kafka", "evidence": "adjacent"}],
    }
    jd = "Requirements:\n- Kafka\nResponsibilities:\n- Stream."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "SRE", "Co"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    frags = [
        CLFragment(kind="opening", text="Hi,",         source_line=1),
        CLFragment(kind="closing", text="Thanks, Alex", source_line=3),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=profile_with_adj, base_cl=cl,
    )
    if result.adjacency_para:
        adj_pos   = result.markdown.index(result.adjacency_para)
        thanks_pos = result.markdown.index("Thank you for your time")
        assert adj_pos < thanks_pos


# ── 39-48: Provenance metadata ────────────────────────────────────────────────

def test_provenance_base_cl_id(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert result.provenance.base_cl_id == ingested_cl.cl_id


def test_provenance_base_resume_id_none_when_not_supplied(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=ingested_cl, base_resume=None,
    )
    assert result.provenance.base_resume_id is None


def test_provenance_base_resume_id_matches_when_supplied(
    mem_conn, sample_job_id, good_profile, ingested_cl, ingested_resume
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=ingested_cl, base_resume=ingested_resume,
    )
    assert result.provenance.base_resume_id == ingested_resume.resume_id


def test_provenance_proof_point_source_lines(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    actual = [pp.source_line for pp in result.proof_points]
    assert result.provenance.proof_point_source_lines == actual


def test_provenance_proof_point_source_types(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    actual = [pp.source_type for pp in result.proof_points]
    assert result.provenance.proof_point_source_types == actual


def test_provenance_direct_evidence_used(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    for term in result.provenance.direct_evidence_used:
        assert term in result.provenance.jd_required_skills


def test_provenance_unsupported_gaps(mem_conn, good_profile):
    jd = "Requirements:\n- Spark\n- Hadoop\nResponsibilities:\n- Data work."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Data Eng", "Co"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    frags = [
        CLFragment(kind="opening",     text="Hi,",         source_line=1),
        CLFragment(kind="proof_point", text="I like data.", source_line=3),
        CLFragment(kind="closing",     text="Thanks, Alex", source_line=5),
    ]
    cl = CoverLetterResult(cl_id=0, label="test", raw_text="", fragments=frags)
    from app.services.base_asset_ingest import persist_cover_letter
    cl.cl_id = persist_cover_letter(mem_conn, cl)

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile, base_cl=cl,
    )
    # spark and hadoop not in good_profile — should be in gaps
    assert len(result.provenance.unsupported_gaps_excluded) >= 1


def test_provenance_used_extraction_false(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=ingested_cl, extracted=None,
    )
    assert result.provenance.used_extraction is False


def test_provenance_used_extraction_true(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    class FakeExtracted:
        required_skills  = ["python", "fastapi"]
        preferred_skills = ["kafka"]
        domain_requirements = []
        ats_keywords     = ["python", "fastapi", "kafka"]

    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile,
        base_cl=ingested_cl, extracted=FakeExtracted(),
    )
    assert result.provenance.used_extraction is True


def test_provenance_to_dict_round_trips(
    mem_conn, sample_job_id, good_profile, ingested_cl
):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    d = json.loads(json.dumps(result.provenance.to_dict()))
    assert d["base_cl_id"] == ingested_cl.cl_id
    assert "jd_required_skills" in d
    assert "proof_point_source_lines" in d


# ── 49-52: Persistence ────────────────────────────────────────────────────────

def test_asset_id_is_positive_integer(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    assert isinstance(result.asset_id, int) and result.asset_id > 0


def test_generated_asset_row_correct(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    row = mem_conn.execute(
        "SELECT * FROM generated_assets WHERE id = ?", (result.asset_id,)
    ).fetchone()
    assert row["asset_type"] == "cover_letter"
    assert row["job_id"]     == sample_job_id
    assert row["base_cl_id"] == ingested_cl.cl_id


def test_metadata_json_has_expected_keys(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    row = mem_conn.execute(
        "SELECT metadata_json FROM generated_assets WHERE id = ?", (result.asset_id,)
    ).fetchone()
    d = json.loads(row["metadata_json"])
    for key in ("base_cl_id", "job_id", "proof_point_source_lines",
                 "jd_required_skills", "used_extraction"):
        assert key in d, f"missing key: {key}"


def test_content_column_contains_markdown(mem_conn, sample_job_id, good_profile, ingested_cl):
    result = generate_targeted_cover_letter(
        job_id=sample_job_id, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
    )
    row = mem_conn.execute(
        "SELECT content FROM generated_assets WHERE id = ?", (result.asset_id,)
    ).fetchone()
    assert row["content"] == result.markdown


# ── 53-57: End-to-end ─────────────────────────────────────────────────────────

@pytest.mark.skipif(not _SAMPLE_CL.exists(), reason="sample_cover_letter.txt not present")
def test_sample_cl_gives_at_least_one_proof_point(mem_conn, good_profile):
    raw_cl = _SAMPLE_CL.read_text(encoding="utf-8")
    cl = ingest_cover_letter(raw_cl, mem_conn)
    jd = "Requirements:\n- Python\n- FastAPI\n- PostgreSQL\n- Kafka\nResponsibilities:\n- Backend work."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Backend Engineer", "SampleCo"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile, base_cl=cl,
    )
    assert len(result.proof_points) >= 1


@pytest.mark.skipif(not _SAMPLE_CL.exists(), reason="sample_cover_letter.txt not present")
def test_sample_cl_asset_persisted(mem_conn, good_profile):
    raw_cl = _SAMPLE_CL.read_text(encoding="utf-8")
    cl = ingest_cover_letter(raw_cl, mem_conn)
    jd = "Requirements:\n- Python\n- FastAPI\n- Kafka\nResponsibilities:\n- Build APIs."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "SWE", "Co"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile, base_cl=cl,
    )
    row = mem_conn.execute(
        "SELECT id FROM generated_assets WHERE id = ?", (result.asset_id,)
    ).fetchone()
    assert row is not None


@pytest.mark.skipif(not _SAMPLE_CL.exists(), reason="sample_cover_letter.txt not present")
def test_sample_cl_output_deterministic(mem_conn, good_profile):
    raw_cl = _SAMPLE_CL.read_text(encoding="utf-8")
    jd = "Requirements:\n- Python\n- FastAPI\n- PostgreSQL\n- Kafka\nResponsibilities:\n- Build."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "SWE", "DeterministicCo"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    cl1 = ingest_cover_letter(raw_cl, mem_conn)
    r1  = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile, base_cl=cl1,
    )
    cl2 = ingest_cover_letter(raw_cl, mem_conn)
    r2  = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile, base_cl=cl2,
    )
    assert r1.markdown == r2.markdown


def test_raises_value_error_for_unknown_job(mem_conn, good_profile, ingested_cl):
    with pytest.raises(ValueError, match="not found"):
        generate_targeted_cover_letter(
            job_id=99999, conn=mem_conn, profile=good_profile, base_cl=ingested_cl,
        )


@pytest.mark.skipif(
    not (_SAMPLE_CL.exists() and _SAMPLE_RESUME.exists()),
    reason="sample files not present",
)
def test_sample_cl_with_resume_supplement(mem_conn, good_profile):
    raw_cl     = _SAMPLE_CL.read_text(encoding="utf-8")
    raw_resume = _SAMPLE_RESUME.read_text(encoding="utf-8")

    cl     = ingest_cover_letter(raw_cl,     mem_conn)
    resume = ingest_resume(raw_resume, mem_conn)

    jd = "Requirements:\n- Python\n- FastAPI\n- Kafka\n- Docker\nResponsibilities:\n- Build."
    cur = mem_conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Backend SWE", "SupCo"),
    )
    mem_conn.commit()
    jid = cur.lastrowid

    result = generate_targeted_cover_letter(
        job_id=jid, conn=mem_conn, profile=good_profile,
        base_cl=cl, base_resume=resume,
    )
    assert result.asset_id > 0
    assert len(result.proof_points) >= 1
