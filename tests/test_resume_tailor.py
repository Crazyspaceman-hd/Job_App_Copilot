"""
tests/test_resume_tailor.py — Targeted resume generator tests.

Covered cases:
  Bullet scoring
    1.  Direct-evidence bullets outscore adjacent-evidence bullets for same term
    2.  Adjacent-evidence bullets outscore bullets with no profile evidence
    3.  A bullet matching more required terms scores higher than one matching fewer
    4.  A bullet with zero matching terms scores 0.0
    5.  has_direct_evidence is True only when a required term has direct evidence
    6.  matched_required / matched_preferred populated correctly

  Bullet selection
    7.  Selected bullets are a subset of all scored bullets
    8.  At most max_bullets_per_section bullets per section are returned
    9.  Selected bullets within a section are in source_line order
   10.  Sections in _SKIP_SECTIONS are excluded from selection
   11.  Zero bullet_bank → empty selected list

  Skills section reordering
   12.  Required-skill matches appear before preferred-skill matches
   13.  Preferred-skill matches appear before unmatched skills
   14.  TODO entries excluded from skills section
   15.  Returns empty dict for empty profile skills

  Summary generation
   16.  Summary contains at least one required-skill term (when profile covers it)
   17.  Summary does not mention a skill that is in unsupported_gaps
   18.  Summary is a non-empty string
   19.  Seniority label appears in summary when profile has it

  Markdown output
   20.  Markdown starts with '# {candidate name}'
   21.  Markdown contains '## Summary'
   22.  Markdown contains '## Skills' when skills present
   23.  Markdown contains bullet text from selected bullets
   24.  Markdown does NOT contain text from excluded bullets when max < total
   25.  Unsupported-gap skills do not appear in the skills section of the markdown

  Provenance metadata
   26.  selected_bullet_source_lines matches actual selected bullet source_lines
   27.  excluded_bullet_source_lines = all source_lines − selected source_lines
   28.  direct_evidence_used contains only required terms with direct evidence
   29.  adjacent_evidence_referenced contains only adj/familiar required/pref terms
   30.  unsupported_gaps_excluded lists required terms absent from profile
   31.  used_extraction = False when no extraction passed
   32.  used_extraction = True when extraction passed
   33.  total_bullets_available = len(base_resume.bullet_bank)
   34.  total_bullets_selected = len(selected_bullets)

  Persistence
   35.  asset_id is a positive integer after generate_targeted_resume()
   36.  generated_assets row exists with correct job_id
   37.  metadata_json in DB is valid JSON containing provenance keys
   38.  content column contains the markdown string
   39.  base_resume_id column matches base resume id

  Generation without structured extraction
   40.  Works when extracted=None
   41.  used_extraction=False when extracted=None
   42.  Output shape identical (summary / skills / bullets / markdown)

  Generation with structured extraction
   43.  Works when valid ExtractionResult passed
   44.  used_extraction=True
   45.  Required skills from extraction used for ranking

  End-to-end with sample files
   46.  Sample resume + sample profile → output has ≥1 selected bullet
   47.  Sample resume + sample profile → asset persisted to DB
   48.  Sample resume + sample profile → provenance round-trips from DB JSON
   49.  Output markdown is deterministic across two calls with identical inputs
   50.  Two calls for different jobs produce different asset rows
"""

import json
import sqlite3

import pytest

from app.db import apply_migrations
from app.services.base_asset_ingest import (
    ResumeBullet,
    ResumeResult,
    ResumeSection,
    ingest_resume,
    load_latest_base_resume,
    parse_resume,
    persist_resume,
)
from app.services.resume_tailor import (
    BulletScore,
    TailoredResumeProvenance,
    TailoredResumeResult,
    _build_skills_section,
    _generate_summary,
    _render_markdown,
    _score_bullet,
    _select_bullets,
    _SKIP_SECTIONS,
    generate_targeted_resume,
)
from app.services.scorer import _build_skill_map, _normalize
from pathlib import Path

_PROJECT_ROOT  = Path(__file__).resolve().parent.parent
_SAMPLE_RESUME = _PROJECT_ROOT / "data" / "sample_resume.txt"


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def mem_conn():
    """In-memory SQLite with full schema + all migrations applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
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
    """)
    apply_migrations(conn)
    return conn


@pytest.fixture()
def sample_job_id(mem_conn):
    """Insert a minimal job with a requirements section and return its id."""
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
    """A well-filled candidate profile."""
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
            "languages":  [sk("Python"), sk("SQL")],
            "frameworks": [sk("FastAPI"), sk("Django", "adjacent")],
            "databases":  [sk("PostgreSQL"), sk("Redis")],
            "cloud":      [sk("AWS")],
            "tools":      [sk("Docker"), sk("Kafka", "adjacent")],
        },
        "domains": [{"name": "fintech", "evidence": "adjacent"}],
        "experience": [
            {
                "company":    "Acme Corp",
                "title":      "Senior Software Engineer",
                "start_date": "2020-03",
                "end_date":   "present",
                "bullets":    ["Built payment APIs in Python + FastAPI"],
            },
            {
                "company":    "DataFlow Inc",
                "title":      "Software Engineer",
                "start_date": "2017-06",
                "end_date":   "2020-02",
                "bullets":    ["Built ETL pipelines"],
            },
        ],
        "education":      [{"institution": "UT Austin", "degree": "B.S. CS", "year": "2017"}],
        "certifications": [{"name": "AWS Certified Developer", "year": "2021"}],
        "hard_constraints": {},
    }


@pytest.fixture()
def sample_base_resume(mem_conn):
    """Ingest a minimal multi-section resume and return the ResumeResult."""
    text = (
        "EXPERIENCE\n"
        "- Built high-throughput REST APIs using Python and FastAPI\n"
        "- Deployed containerised services with Docker and Kubernetes\n"
        "- Optimised PostgreSQL queries reducing latency by 60%\n"
        "- Migrated legacy monolith to microservices architecture\n"
        "- Worked with Kafka for real-time event streaming pipelines\n"
        "\n"
        "PROJECTS\n"
        "- Open-source SQL formatter in Python with 400+ GitHub stars\n"
        "- Built async task scheduler using Redis Streams\n"
    )
    return ingest_resume(text, mem_conn, label="test")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_bullet(text: str, section: str = "experience", source_line: int = 1) -> ResumeBullet:
    from app.services.scorer import _extract_vocab_terms
    return ResumeBullet(
        section=section, text=text,
        skills=_extract_vocab_terms(text), source_line=source_line,
    )


def _make_skill_map_direct(*terms: str) -> dict:
    return {_normalize(t): "direct" for t in terms}


def _make_skill_map_adjacent(*terms: str) -> dict:
    return {_normalize(t): "adjacent" for t in terms}


# ═══════════════════════════════════════════════════════════════════════════════
# 1–6  Bullet scoring
# ═══════════════════════════════════════════════════════════════════════════════

def test_direct_evidence_outscores_adjacent():
    """Same required term, direct evidence > adjacent evidence."""
    bullet = _make_bullet("Deployed services with Python and FastAPI")
    required = {"python", "fastapi"}

    direct_map   = _make_skill_map_direct("python", "fastapi")
    adjacent_map = _make_skill_map_adjacent("python", "fastapi")

    s_direct   = _score_bullet(bullet, required, set(), set(), set(), direct_map)
    s_adjacent = _score_bullet(bullet, required, set(), set(), set(), adjacent_map)

    assert s_direct.score > s_adjacent.score, (
        f"direct score {s_direct.score} should exceed adjacent score {s_adjacent.score}"
    )


def test_adjacent_outscores_absent():
    """Adjacent evidence > no profile match (absent weight 0.3 < adjacent 0.5)."""
    bullet = _make_bullet("Built APIs using Python")
    required = {"python"}

    adjacent_map = _make_skill_map_adjacent("python")
    empty_map: dict = {}

    s_adjacent = _score_bullet(bullet, required, set(), set(), set(), adjacent_map)
    s_absent   = _score_bullet(bullet, required, set(), set(), set(), empty_map)

    assert s_adjacent.score > s_absent.score


def test_more_required_matches_score_higher():
    """A bullet matching 2 required terms scores higher than one matching 1."""
    b2 = _make_bullet("Built FastAPI service with PostgreSQL backend")
    b1 = _make_bullet("Wrote unit tests for the project")

    required = {"fastapi", "postgresql"}
    skill_map = _make_skill_map_direct("fastapi", "postgresql")

    s2 = _score_bullet(b2, required, set(), set(), set(), skill_map)
    s1 = _score_bullet(b1, required, set(), set(), set(), skill_map)

    assert s2.score > s1.score


def test_zero_matching_terms_scores_zero():
    """A bullet with no vocabulary terms at all scores 0."""
    bullet    = _make_bullet("Collaborated across teams")   # no vocab terms
    skill_map = _make_skill_map_direct("python")

    bs = _score_bullet(bullet, {"python"}, set(), set(), set(), skill_map)
    assert bs.score == 0.0


def test_has_direct_evidence_true():
    bullet    = _make_bullet("Deployed Python microservices")
    skill_map = _make_skill_map_direct("python")
    bs        = _score_bullet(bullet, {"python"}, set(), set(), set(), skill_map)
    assert bs.has_direct_evidence is True


def test_has_direct_evidence_false_for_adjacent():
    bullet    = _make_bullet("Deployed Python microservices")
    skill_map = _make_skill_map_adjacent("python")
    bs        = _score_bullet(bullet, {"python"}, set(), set(), set(), skill_map)
    assert bs.has_direct_evidence is False


def test_matched_required_populated():
    bullet    = _make_bullet("Built APIs with Python and FastAPI")
    required  = {"python", "fastapi"}
    skill_map = _make_skill_map_direct("python", "fastapi")
    bs        = _score_bullet(bullet, required, set(), set(), set(), skill_map)
    assert set(bs.matched_required) == {"python", "fastapi"}
    assert bs.matched_preferred     == []


# ═══════════════════════════════════════════════════════════════════════════════
# 7–11  Bullet selection
# ═══════════════════════════════════════════════════════════════════════════════

def test_selected_are_subset_of_scored(good_profile, sample_base_resume):
    skill_map   = _build_skill_map(good_profile, set())
    required    = {"python", "fastapi", "postgresql", "docker"}
    scored      = [
        _score_bullet(b, required, set(), set(), set(), skill_map)
        for b in sample_base_resume.bullet_bank
    ]
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))
    selected = _select_bullets(scored, max_per_section=8)

    selected_lines = {bs.bullet.source_line for bs in selected}
    all_lines      = {bs.bullet.source_line for bs in scored
                      if bs.bullet.section not in _SKIP_SECTIONS}
    assert selected_lines.issubset(all_lines)


def test_max_bullets_per_section_capped():
    """Never more than max_per_section bullets from any single section."""
    bullets = [
        _make_bullet(f"Built API with Python item {i}", "experience", source_line=i)
        for i in range(1, 15)
    ]
    skill_map = _make_skill_map_direct("python")
    scored    = [
        _score_bullet(b, {"python"}, set(), set(), set(), skill_map)
        for b in bullets
    ]
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))

    selected = _select_bullets(scored, max_per_section=4)
    exp_count = sum(1 for bs in selected if bs.bullet.section == "experience")
    assert exp_count <= 4


def test_selected_bullets_in_source_line_order():
    """Selected bullets within a section are in ascending source_line order."""
    bullets = [
        _make_bullet("Deployed Python services", "experience", source_line=10),
        _make_bullet("Built FastAPI APIs",        "experience", source_line=5),
        _make_bullet("Used PostgreSQL databases",  "experience", source_line=15),
    ]
    skill_map = _make_skill_map_direct("python", "fastapi", "postgresql")
    scored    = [
        _score_bullet(b, {"python", "fastapi", "postgresql"}, set(), set(), set(), skill_map)
        for b in bullets
    ]
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))
    selected = _select_bullets(scored, max_per_section=10)

    exp_selected = [bs for bs in selected if bs.bullet.section == "experience"]
    lines        = [bs.bullet.source_line for bs in exp_selected]
    assert lines == sorted(lines)


def test_skip_sections_excluded():
    """Bullets in skills/education/etc. sections are never selected."""
    bullets = [
        _make_bullet("Python, SQL, FastAPI",   "skills",     source_line=1),
        _make_bullet("B.S. Computer Science",  "education",  source_line=2),
        _make_bullet("Built APIs with Python", "experience", source_line=3),
    ]
    skill_map = _make_skill_map_direct("python", "fastapi")
    scored    = [
        _score_bullet(b, {"python"}, set(), set(), set(), skill_map)
        for b in bullets
    ]
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))
    selected  = _select_bullets(scored, max_per_section=10)

    sec_names = {bs.bullet.section for bs in selected}
    assert "skills"    not in sec_names
    assert "education" not in sec_names
    assert "experience" in sec_names


def test_zero_bullet_bank_gives_empty_selection():
    selected = _select_bullets([], max_per_section=8)
    assert selected == []


# ═══════════════════════════════════════════════════════════════════════════════
# 12–15  Skills section reordering
# ═══════════════════════════════════════════════════════════════════════════════

def test_required_skills_first_in_category():
    profile = {
        "skills": {
            "frameworks": [
                {"name": "Django",   "evidence": "direct"},
                {"name": "FastAPI",  "evidence": "direct"},
                {"name": "Celery",   "evidence": "direct"},
            ]
        }
    }
    result = _build_skills_section(profile, required_set={"fastapi"}, preferred_set=set())
    frameworks = result["frameworks"]
    assert frameworks[0] == "FastAPI", f"Expected FastAPI first, got {frameworks}"


def test_preferred_before_unmatched():
    profile = {
        "skills": {
            "tools": [
                {"name": "Terraform", "evidence": "direct"},
                {"name": "Kafka",     "evidence": "direct"},
                {"name": "Docker",    "evidence": "direct"},
            ]
        }
    }
    result = _build_skills_section(profile, required_set=set(), preferred_set={"kafka"})
    tools = result["tools"]
    assert tools[0] == "Kafka"


def test_todo_entries_excluded():
    profile = {
        "skills": {
            "languages": [
                {"name": "TODO: fill this in", "evidence": "direct"},
                {"name": "Python",              "evidence": "direct"},
            ]
        }
    }
    result = _build_skills_section(profile, set(), set())
    langs  = result["languages"]
    assert not any("TODO" in s for s in langs)
    assert "Python" in langs


def test_empty_skills_returns_empty_dict():
    profile = {"skills": {}}
    result  = _build_skills_section(profile, set(), set())
    assert result == {}


# ═══════════════════════════════════════════════════════════════════════════════
# 16–19  Summary generation
# ═══════════════════════════════════════════════════════════════════════════════

def test_summary_contains_required_skill_when_covered(good_profile):
    skill_map     = _build_skill_map(good_profile, set())
    required      = ["python", "fastapi"]
    direct_evid   = ["python", "fastapi"]
    summary       = _generate_summary(good_profile, required, skill_map, direct_evid)
    summary_lower = summary.lower()
    assert "python" in summary_lower or "fastapi" in summary_lower


def test_summary_does_not_mention_gap_skill(good_profile):
    """A skill with no profile evidence should not appear in the summary."""
    skill_map = _build_skill_map(good_profile, set())
    # spark is not in good_profile at all
    required    = ["spark"]
    direct_evid = []
    summary     = _generate_summary(good_profile, required, skill_map, direct_evid)
    # 'spark' should not surface in the summary text since there's no evidence
    assert "spark" not in summary.lower()


def test_summary_is_non_empty(good_profile):
    skill_map = _build_skill_map(good_profile, set())
    summary   = _generate_summary(good_profile, [], skill_map, [])
    assert len(summary.strip()) > 0


def test_summary_includes_seniority(good_profile):
    skill_map = _build_skill_map(good_profile, set())
    summary   = _generate_summary(good_profile, ["python"], skill_map, ["python"])
    assert "senior" in summary.lower()


# ═══════════════════════════════════════════════════════════════════════════════
# 20–25  Markdown output
# ═══════════════════════════════════════════════════════════════════════════════

def test_markdown_starts_with_name(good_profile, sample_base_resume):
    skill_map      = _build_skill_map(good_profile, set())
    required       = {"python", "fastapi"}
    scored         = [
        _score_bullet(b, required, set(), set(), set(), skill_map)
        for b in sample_base_resume.bullet_bank
    ]
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))
    selected       = _select_bullets(scored, 8)
    skills_section = _build_skills_section(good_profile, required, set())
    summary        = "Test summary."

    md = _render_markdown(good_profile, summary, skills_section, selected, sample_base_resume)
    assert md.startswith("# Alex Rivera"), f"Unexpected start: {md[:50]!r}"


def test_markdown_contains_summary_section(good_profile, sample_base_resume):
    selected = []
    md = _render_markdown(good_profile, "My summary.", {}, selected, sample_base_resume)
    assert "## Summary" in md
    assert "My summary." in md


def test_markdown_contains_skills_section(good_profile, sample_base_resume):
    skills = {"languages": ["Python", "SQL"]}
    md     = _render_markdown(good_profile, "Sum.", skills, [], sample_base_resume)
    assert "## Skills" in md
    assert "Python" in md


def test_markdown_contains_selected_bullet_text(good_profile, sample_base_resume):
    skill_map = _build_skill_map(good_profile, set())
    required  = {"python", "fastapi"}
    scored    = [
        _score_bullet(b, required, set(), set(), set(), skill_map)
        for b in sample_base_resume.bullet_bank
    ]
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))
    selected  = _select_bullets(scored, 8)

    md = _render_markdown(good_profile, "Sum.", {}, selected, sample_base_resume)
    for bs in selected:
        assert bs.bullet.text in md, f"Expected bullet in markdown: {bs.bullet.text!r}"


def test_markdown_excludes_low_score_bullet_when_capped(good_profile, sample_base_resume):
    """When max=1 per section, the lowest-scored bullet should NOT appear."""
    skill_map = _build_skill_map(good_profile, set())
    required  = {"python", "fastapi", "postgresql"}
    scored    = [
        _score_bullet(b, required, set(), set(), set(), skill_map)
        for b in sample_base_resume.bullet_bank
    ]
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))

    # Select only 1 bullet per section
    selected = _select_bullets(scored, max_per_section=1)
    selected_lines = {bs.bullet.source_line for bs in selected}

    # Find bullets that were NOT selected
    excluded = [bs for bs in scored if bs.bullet.section not in _SKIP_SECTIONS
                and bs.bullet.source_line not in selected_lines]

    if not excluded:
        pytest.skip("All bullets selected — need more bullets to test exclusion")

    md = _render_markdown(good_profile, "Sum.", {}, selected, sample_base_resume)

    # At least one excluded bullet's text should not be in the markdown
    at_least_one_excluded = any(bs.bullet.text not in md for bs in excluded)
    assert at_least_one_excluded, "Expected at least one excluded bullet to be absent"


def test_unsupported_gap_not_in_skills_section(good_profile):
    """A skill that is a gap (not in profile) should not appear in the skills section."""
    # 'spark' is not in good_profile
    skills = _build_skills_section(good_profile, {"spark"}, set())
    all_skill_names = [name for names in skills.values() for name in names]
    assert "spark" not in [n.lower() for n in all_skill_names], (
        f"Gap skill 'spark' should not appear in skills section: {all_skill_names}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 26–34  Provenance metadata
# ═══════════════════════════════════════════════════════════════════════════════

def test_provenance_selected_lines_match(sample_job_id, mem_conn, good_profile, sample_base_resume):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    prov            = result.provenance
    actual_lines    = sorted(bs.bullet.source_line for bs in result.selected_bullets)
    assert prov.selected_bullet_source_lines == actual_lines


def test_provenance_excluded_lines_correct(sample_job_id, mem_conn, good_profile, sample_base_resume):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    prov = result.provenance
    all_lines  = {bs.bullet.source_line for bs in result.scored_bullets
                  if bs.bullet.section not in _SKIP_SECTIONS}
    sel_lines  = set(prov.selected_bullet_source_lines)
    excl_lines = set(prov.excluded_bullet_source_lines)
    assert sel_lines | excl_lines == all_lines
    assert sel_lines & excl_lines == set()


def test_provenance_direct_evidence_only_direct(sample_job_id, mem_conn, good_profile, sample_base_resume):
    result     = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    skill_map  = _build_skill_map(good_profile, set())
    for term in result.provenance.direct_evidence_used:
        from app.services.scorer import _lookup_skill
        ev = _lookup_skill(term, skill_map)
        assert ev == "direct", f"Expected direct evidence for {term!r}, got {ev!r}"


def test_provenance_adjacent_evidence_only_adj(sample_job_id, mem_conn, good_profile, sample_base_resume):
    result    = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    skill_map = _build_skill_map(good_profile, set())
    from app.services.scorer import _lookup_skill
    for term in result.provenance.adjacent_evidence_referenced:
        ev = _lookup_skill(term, skill_map)
        assert ev in ("adjacent", "familiar"), (
            f"Expected adjacent/familiar evidence for {term!r}, got {ev!r}"
        )


def test_provenance_gaps_absent_from_profile(sample_job_id, mem_conn, good_profile, sample_base_resume):
    result    = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    skill_map = _build_skill_map(good_profile, set())
    from app.services.scorer import _lookup_skill
    for term in result.provenance.unsupported_gaps_excluded:
        ev = _lookup_skill(term, skill_map)
        assert ev is None, f"Gap term {term!r} should have no profile evidence, got {ev!r}"


def test_provenance_used_extraction_false_without_extracted(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=None,
    )
    assert result.provenance.used_extraction is False


def test_provenance_used_extraction_true_with_extracted(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    from app.services.extractor import ExtractionResult
    fake_ext = ExtractionResult(
        job_id=sample_job_id,
        required_skills=["python", "fastapi"],
        preferred_skills=["kafka"],
        ats_keywords=["python", "fastapi", "postgresql", "kafka"],
    )
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=fake_ext,
    )
    assert result.provenance.used_extraction is True


def test_provenance_total_bullets_available(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    assert result.provenance.total_bullets_available == len(sample_base_resume.bullet_bank)


def test_provenance_total_bullets_selected(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    assert result.provenance.total_bullets_selected == len(result.selected_bullets)


# ═══════════════════════════════════════════════════════════════════════════════
# 35–39  Persistence
# ═══════════════════════════════════════════════════════════════════════════════

def test_asset_id_positive(sample_job_id, mem_conn, good_profile, sample_base_resume):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    assert isinstance(result.asset_id, int) and result.asset_id > 0


def test_generated_assets_row_exists(sample_job_id, mem_conn, good_profile, sample_base_resume):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    row = mem_conn.execute(
        "SELECT id, job_id, asset_type FROM generated_assets WHERE id = ?",
        (result.asset_id,),
    ).fetchone()
    assert row is not None
    assert row["job_id"]     == sample_job_id
    assert row["asset_type"] == "resume"


def test_metadata_json_is_valid_and_has_provenance_keys(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    row  = mem_conn.execute(
        "SELECT metadata_json FROM generated_assets WHERE id = ?",
        (result.asset_id,),
    ).fetchone()
    assert row["metadata_json"] is not None
    data = json.loads(row["metadata_json"])
    for key in (
        "base_resume_id", "job_id",
        "selected_bullet_source_lines", "excluded_bullet_source_lines",
        "direct_evidence_used", "adjacent_evidence_referenced",
        "unsupported_gaps_excluded", "jd_required_skills",
        "used_extraction", "total_bullets_available", "total_bullets_selected",
    ):
        assert key in data, f"Provenance key {key!r} missing from metadata_json"


def test_content_column_contains_markdown(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    row = mem_conn.execute(
        "SELECT content FROM generated_assets WHERE id = ?",
        (result.asset_id,),
    ).fetchone()
    assert row["content"] == result.markdown
    assert "## Summary" in row["content"]


def test_base_resume_id_column_correct(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
    )
    row = mem_conn.execute(
        "SELECT base_resume_id FROM generated_assets WHERE id = ?",
        (result.asset_id,),
    ).fetchone()
    assert row["base_resume_id"] == sample_base_resume.resume_id


# ═══════════════════════════════════════════════════════════════════════════════
# 40–42  Generation without structured extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_generation_works_without_extraction(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=None,
    )
    assert result.asset_id > 0
    assert len(result.markdown) > 0


def test_used_extraction_false_without_extraction(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=None,
    )
    assert result.provenance.used_extraction is False


def test_output_shape_same_with_and_without_extraction(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    """Both paths produce the same structural fields."""
    from app.services.extractor import ExtractionResult
    fake_ext = ExtractionResult(
        job_id=sample_job_id,
        required_skills=["python", "fastapi"],
        preferred_skills=["kafka"],
        ats_keywords=["python", "fastapi", "postgresql", "kafka"],
    )
    r_no_ext  = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=None,
    )
    r_with_ext = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=fake_ext,
    )
    # Both should have a summary, skills section, selected bullets, and markdown
    for r in (r_no_ext, r_with_ext):
        assert r.summary
        assert r.markdown
        assert isinstance(r.selected_bullets, list)
        assert isinstance(r.skills_section, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# 43–45  Generation with structured extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_generation_works_with_extraction(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    from app.services.extractor import ExtractionResult
    ext = ExtractionResult(
        job_id=sample_job_id,
        required_skills=["python", "fastapi", "postgresql"],
        preferred_skills=["kafka"],
        ats_keywords=["python", "fastapi", "postgresql", "kafka", "docker"],
    )
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=ext,
    )
    assert result.asset_id > 0
    assert result.provenance.used_extraction is True


def test_extraction_required_skills_drive_ranking(
    sample_job_id, mem_conn, good_profile, sample_base_resume
):
    """
    When extraction says 'python' is required, bullets containing 'python'
    should rank at the top.
    """
    from app.services.extractor import ExtractionResult
    ext = ExtractionResult(
        job_id=sample_job_id,
        required_skills=["python"],
        ats_keywords=["python"],
    )
    result = generate_targeted_resume(
        job_id=sample_job_id, conn=mem_conn,
        profile=good_profile, base_resume=sample_base_resume,
        extracted=ext,
    )
    # The top-scored bullet must mention python (via matched_required)
    top = result.scored_bullets[0]
    assert "python" in top.matched_required, (
        f"Top bullet should match 'python', matched: {top.matched_required}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 46–50  End-to-end with sample files
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def _sample_resume_text():
    return _SAMPLE_RESUME.read_text(encoding="utf-8")


@pytest.fixture()
def e2e_conn():
    """Fresh in-memory connection for end-to-end tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL DEFAULT 'new',
            title TEXT, company TEXT, remote_policy TEXT, raw_text TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS candidate_profiles (id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT, profile_json TEXT);
        CREATE TABLE IF NOT EXISTS fit_assessments (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL REFERENCES jobs(id), candidate_profile_id INTEGER, assessed_at TEXT NOT NULL DEFAULT (datetime('now')), overall_score REAL, rationale TEXT, gap_summary TEXT);
        CREATE TABLE IF NOT EXISTS extracted_requirements (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL REFERENCES jobs(id), category TEXT, requirement TEXT);
        CREATE TABLE IF NOT EXISTS generated_assets (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL REFERENCES jobs(id), asset_type TEXT NOT NULL CHECK(asset_type IN ('resume','cover_letter','email','other')), file_path TEXT, content TEXT, generated_at TEXT NOT NULL DEFAULT (datetime('now')));
    """)
    apply_migrations(conn)
    return conn


def _insert_job(conn, jd_text: str = None) -> int:
    jd = jd_text or (
        "Requirements:\n"
        "- 5+ years Python experience\n"
        "- FastAPI or Django\n"
        "- PostgreSQL\n"
        "- Docker\n\n"
        "Nice to have:\n"
        "- Kafka\n"
        "- AWS\n"
    )
    cur = conn.execute(
        "INSERT INTO jobs (raw_text, title, company) VALUES (?, ?, ?)",
        (jd, "Senior Backend Engineer", "SampleCorp"),
    )
    conn.commit()
    return cur.lastrowid


def _good_profile_full():
    def sk(name, ev="direct"):
        return {"name": name, "evidence": ev}
    return {
        "version": "1.1",
        "personal": {"name": "Alex Rivera", "location": "Austin, TX"},
        "job_targets": {
            "seniority_self_assessed": "senior",
            "desired_remote_policy":   "remote",
            "work_authorization":      "us_citizen",
        },
        "skills": {
            "languages":  [sk("Python"), sk("SQL")],
            "frameworks": [sk("FastAPI"), sk("Django", "adjacent")],
            "databases":  [sk("PostgreSQL"), sk("Redis")],
            "cloud":      [sk("AWS")],
            "tools":      [sk("Docker"), sk("Kafka", "adjacent")],
        },
        "domains":      [{"name": "fintech", "evidence": "adjacent"}],
        "experience": [
            {"company": "Acme",     "title": "SWE", "start_date": "2020-03", "end_date": "present", "bullets": []},
            {"company": "DataFlow", "title": "SWE", "start_date": "2017-06", "end_date": "2020-02", "bullets": []},
        ],
        "education":      [{"institution": "UT Austin", "degree": "B.S. CS", "year": "2017"}],
        "certifications": [{"name": "AWS Certified Developer", "year": "2021"}],
        "hard_constraints": {},
    }


def test_e2e_sample_resume_has_selected_bullets(e2e_conn, _sample_resume_text):
    job_id = _insert_job(e2e_conn)
    br     = ingest_resume(_sample_resume_text, e2e_conn, label="sample")
    result = generate_targeted_resume(
        job_id=job_id, conn=e2e_conn,
        profile=_good_profile_full(), base_resume=br,
    )
    assert len(result.selected_bullets) >= 1


def test_e2e_asset_persisted(e2e_conn, _sample_resume_text):
    job_id = _insert_job(e2e_conn)
    br     = ingest_resume(_sample_resume_text, e2e_conn, label="sample")
    result = generate_targeted_resume(
        job_id=job_id, conn=e2e_conn,
        profile=_good_profile_full(), base_resume=br,
    )
    row = e2e_conn.execute(
        "SELECT id FROM generated_assets WHERE id = ?", (result.asset_id,)
    ).fetchone()
    assert row is not None


def test_e2e_provenance_roundtrips_from_db(e2e_conn, _sample_resume_text):
    job_id = _insert_job(e2e_conn)
    br     = ingest_resume(_sample_resume_text, e2e_conn, label="sample")
    result = generate_targeted_resume(
        job_id=job_id, conn=e2e_conn,
        profile=_good_profile_full(), base_resume=br,
    )
    row  = e2e_conn.execute(
        "SELECT metadata_json FROM generated_assets WHERE id = ?", (result.asset_id,)
    ).fetchone()
    data = json.loads(row["metadata_json"])
    # Check key provenance fields survived the DB round-trip
    assert data["base_resume_id"]             == br.resume_id
    assert data["job_id"]                      == job_id
    assert data["total_bullets_available"]     == len(br.bullet_bank)
    assert data["total_bullets_selected"]      == len(result.selected_bullets)


def test_e2e_output_is_deterministic(e2e_conn, _sample_resume_text):
    """Two calls with identical inputs produce identical markdown."""
    job_id  = _insert_job(e2e_conn)
    profile = _good_profile_full()

    br1 = ingest_resume(_sample_resume_text, e2e_conn, label="v1")
    r1  = generate_targeted_resume(
        job_id=job_id, conn=e2e_conn, profile=profile, base_resume=br1
    )

    br2 = ingest_resume(_sample_resume_text, e2e_conn, label="v2")
    r2  = generate_targeted_resume(
        job_id=job_id, conn=e2e_conn, profile=profile, base_resume=br2
    )

    # Strip the asset_id header since they will differ (different DB rows)
    # Compare the actual content of the resume
    assert r1.summary          == r2.summary
    assert r1.skills_section   == r2.skills_section
    selected1 = [bs.bullet.text for bs in r1.selected_bullets]
    selected2 = [bs.bullet.text for bs in r2.selected_bullets]
    assert selected1 == selected2


def test_e2e_two_jobs_produce_separate_rows(e2e_conn, _sample_resume_text):
    """Generating for two different jobs produces two separate asset rows."""
    jd2 = (
        "Requirements:\n"
        "- Spark\n"
        "- Scala\n"
        "- Kubernetes\n"
    )
    job_id1 = _insert_job(e2e_conn)
    job_id2 = _insert_job(e2e_conn, jd_text=jd2)

    br      = ingest_resume(_sample_resume_text, e2e_conn, label="sample")
    profile = _good_profile_full()

    r1 = generate_targeted_resume(job_id=job_id1, conn=e2e_conn, profile=profile, base_resume=br)
    r2 = generate_targeted_resume(job_id=job_id2, conn=e2e_conn, profile=profile, base_resume=br)

    assert r1.asset_id != r2.asset_id
    rows = e2e_conn.execute(
        "SELECT id FROM generated_assets WHERE asset_type = 'resume'"
    ).fetchall()
    assert len(rows) >= 2
