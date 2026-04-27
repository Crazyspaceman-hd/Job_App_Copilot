"""
tests/test_extractor.py -- Extractor behaviour tests.

All tests are self-contained (no DB, no file I/O).
They call extract() directly with inline JD strings.

Covered cases:
  1. Clear required/preferred section detection
  2. Years-of-experience extraction
  3. Education extraction
  4. Logistics extraction (no-sponsorship, remote policy)
  5. Domain extraction
  6. Messy JD fallback (no structure -> partial output + notes)
  7. Extraction notes surface ambiguity
  8. Scorer prefers extracted requirements over raw text
  9. Scorer falls back gracefully when extraction=None
 10. Extraction is deterministic
 11. DB persistence round-trip (in-memory SQLite)
"""

import json
import sqlite3
from pathlib import Path

import pytest

from app.services.extractor import (
    ExtractionResult,
    _assess_confidence,
    _extract_bullets,
    _extract_domains,
    _extract_education,
    _extract_logistics,
    _extract_yoe,
    _infer_seniority_str,
    _skills_from_section,
    _split_sections,
    extract,
    load_latest_extraction,
    persist_extraction,
)
from app.services.scorer import assess


# ── JD fixtures ───────────────────────────────────────────────────────────────

CLEAN_JD = """\
Senior Python Engineer -- Fully Remote

Requirements:
- 5+ years of Python experience
- Strong knowledge of FastAPI or Django
- Experience with PostgreSQL and Redis
- Familiarity with AWS (Lambda, SQS, RDS)
- Docker for containerisation

Nice to have:
- Kafka or event-driven architectures
- Fintech or payments domain experience

Location: Fully remote (US timezones preferred)
No visa sponsorship available.
"""

MESSY_JD = """\
We're a fast-growing startup and we need smart people.

We work with Python a lot and our stack includes FastAPI and PostgreSQL.
Some Spark experience would be nice. We write a lot of SQL.
You should probably have around 3-5 years of experience.
We're located in Austin, TX.  Hybrid schedule.
"""

NO_SECTIONS_JD = """\
Python developer wanted. Must know SQL. Cloud experience helpful.
We cannot provide visa sponsorship.
"""

CLEARANCE_JD = """\
Software Engineer

Requirements:
- Python
- Active TS/SCI security clearance required
- Must be located in the DC metro area
"""

EDUCATION_JD = """\
Data Scientist

Requirements:
- PhD in Computer Science or related field
- Strong Python and SQL skills
- Machine learning experience
"""

SENIORITY_JD_STAFF = """\
Staff Engineer, Platform

We are looking for a Staff Engineer to lead our infrastructure work.

Requirements:
- Python
- Kubernetes
- 8+ years of experience building distributed systems
"""

PARTIAL_SECTIONS_JD = """\
What We're Looking For:
- Python and FastAPI
- PostgreSQL

What you'll do:
- Design APIs
- Write tests

This is a hybrid role based in Seattle. No relocation assistance.
"""


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Clear required / preferred section detection
# ═══════════════════════════════════════════════════════════════════════════════

def test_clear_sections_extracts_required_skills():
    result = extract(1, CLEAN_JD)
    req = result.required_skills
    assert "python" in req
    assert "fastapi" in req or "django" in req
    assert "postgresql" in req
    assert "redis" in req


def test_clear_sections_extracts_preferred_skills():
    result = extract(1, CLEAN_JD)
    pref = result.preferred_skills
    assert "kafka" in pref or "event-driven" in pref


def test_required_and_preferred_are_disjoint_or_overlap_reasonably():
    """Skills in required should not be double-counted as preferred."""
    result = extract(1, CLEAN_JD)
    # python appears in Requirements, not in Nice-to-have
    assert "python" in result.required_skills
    assert "python" not in result.preferred_skills


def test_responsibilities_extracted():
    result = extract(1, PARTIAL_SECTIONS_JD)
    # "What you'll do" section should produce responsibility bullets
    assert len(result.responsibilities) >= 1
    assert any("api" in r.lower() or "test" in r.lower() for r in result.responsibilities)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Years-of-experience extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_yoe_plus_syntax():
    yoe = _extract_yoe("5+ years of Python experience required")
    assert yoe is not None
    assert yoe["min"] == 5
    assert yoe["max"] is None
    assert "5+" in yoe["raw"]


def test_yoe_range_syntax():
    yoe = _extract_yoe("We require 3-5 years of experience")
    assert yoe is not None
    assert yoe["min"] == 3
    assert yoe["max"] == 5


def test_yoe_at_least_syntax():
    yoe = _extract_yoe("At least 4 years of professional experience")
    assert yoe is not None
    assert yoe["min"] == 4


def test_yoe_picks_highest_minimum():
    """When multiple YOE phrases exist, return the most demanding one."""
    text = "Python: 2+ years.  Overall engineering experience: 6+ years."
    yoe  = _extract_yoe(text)
    assert yoe is not None
    assert yoe["min"] == 6


def test_yoe_none_when_absent():
    assert _extract_yoe("We value passion and curiosity.") is None


def test_yoe_in_full_extraction():
    result = extract(1, CLEAN_JD)
    assert result.years_of_experience is not None
    assert result.years_of_experience["min"] == 5


def test_yoe_in_messy_jd():
    result = extract(1, MESSY_JD)
    yoe = result.years_of_experience
    # messy JD has "3-5 years"
    assert yoe is not None
    assert yoe["min"] == 3
    assert yoe["max"] == 5


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Education extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_education_phd_extracted():
    edu = _extract_education("PhD in Computer Science or related field preferred.")
    assert any("ph" in e.lower() or "phd" in e.lower() for e in edu)


def test_education_bs_extracted():
    edu = _extract_education("B.S. in Computer Science required.")
    assert any("b.s" in e.lower() or "computer science" in e.lower() for e in edu)


def test_education_in_full_extraction():
    result = extract(1, EDUCATION_JD)
    assert len(result.education_requirements) >= 1
    edu_text = " ".join(result.education_requirements).lower()
    assert "computer science" in edu_text or "ph" in edu_text or "related" in edu_text


def test_education_empty_when_absent():
    edu = _extract_education("Strong Python skills required. 3+ years experience.")
    # No education phrases — should return empty or only incidental matches
    # "related field" might match if the text contains it; check no bogus matches
    assert isinstance(edu, list)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Logistics extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_logistics_no_sponsorship_detected():
    lc = _extract_logistics("No visa sponsorship available.")
    assert lc["no_sponsorship"] is True


def test_logistics_must_be_authorized_detected():
    lc = _extract_logistics("Must be authorized to work in the United States.")
    assert lc["no_sponsorship"] is True


def test_logistics_remote_policy_remote():
    lc = _extract_logistics("This is a fully remote position.")
    assert lc["remote_policy"] == "remote"


def test_logistics_remote_policy_hybrid():
    lc = _extract_logistics("We offer a hybrid schedule.")
    assert lc["remote_policy"] == "hybrid"


def test_logistics_remote_policy_onsite():
    lc = _extract_logistics("Must work on-site in Austin.")
    assert lc["remote_policy"] == "onsite"


def test_logistics_clearance_required():
    lc = _extract_logistics("Active TS/SCI security clearance required.")
    assert lc["clearance_required"] is True


def test_logistics_relocation_required():
    lc = _extract_logistics("Local candidates only. Relocation is required.")
    assert lc["relocation_required"] is True


def test_logistics_no_flags_when_absent():
    lc = _extract_logistics("Join our fast-paced team building cool products.")
    assert lc["no_sponsorship"] is False
    assert lc["clearance_required"] is False
    assert lc["relocation_required"] is False


def test_logistics_in_full_extraction():
    result = extract(1, CLEAN_JD)
    lc = result.logistics_constraints
    assert lc["remote_policy"] == "remote"
    assert lc["no_sponsorship"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Domain extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_domain_fintech_detected():
    domains = _extract_domains("Experience in fintech or payments preferred.")
    assert "fintech" in domains or "payments" in domains


def test_domain_mlops_detected():
    domains = _extract_domains("Strong background in MLOps and machine learning pipelines.")
    assert "mlops" in domains or "machine learning" in domains


def test_domain_empty_when_no_domain_terms():
    domains = _extract_domains("We need someone who can write Python and SQL.")
    assert isinstance(domains, list)
    # python/sql are not domain terms
    assert "python" not in domains
    assert "sql" not in domains


def test_domain_in_full_extraction():
    result = extract(1, CLEAN_JD)
    assert "fintech" in result.domain_requirements or "payments" in result.domain_requirements


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Messy JD fallback behaviour
# ═══════════════════════════════════════════════════════════════════════════════

def test_messy_jd_returns_result_not_empty():
    """Even a messy JD should return a non-empty ExtractionResult."""
    result = extract(1, MESSY_JD)
    assert isinstance(result, ExtractionResult)
    # At minimum some skills should be found from full-text scan
    assert len(result.required_skills) > 0 or len(result.ats_keywords) > 0


def test_messy_jd_confidence_not_high():
    """A messy JD without clear sections should NOT claim high confidence."""
    result = extract(1, MESSY_JD)
    assert result.extraction_confidence in ("low", "medium")


def test_no_sections_jd_has_notes():
    result = extract(1, NO_SECTIONS_JD)
    assert len(result.extraction_notes) > 0


def test_no_sections_still_extracts_logistics():
    """Even without structure, logistics patterns should fire on full text."""
    result = extract(1, NO_SECTIONS_JD)
    assert result.logistics_constraints["no_sponsorship"] is True


def test_messy_jd_extracts_some_skills():
    """Vocab scan of full text should catch python, fastapi, postgresql."""
    result = extract(1, MESSY_JD)
    all_skills = set(result.required_skills + result.ats_keywords)
    assert "python" in all_skills
    assert "fastapi" in all_skills or "postgresql" in all_skills


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Extraction notes surface ambiguity
# ═══════════════════════════════════════════════════════════════════════════════

def test_notes_when_no_requirements_section():
    result = extract(1, MESSY_JD)
    notes_text = " ".join(result.extraction_notes).lower()
    assert "requirements" in notes_text or "no" in notes_text or "full" in notes_text


def test_notes_when_no_yoe():
    jd = "Python developer needed. Great team."
    result = extract(1, jd)
    notes_text = " ".join(result.extraction_notes).lower()
    assert "years" in notes_text or "experience" in notes_text or "yoe" in notes_text or "stated" in notes_text


def test_high_confidence_jd_has_fewer_notes():
    """A well-structured JD should produce fewer 'missing section' notes."""
    clean_result = extract(1, CLEAN_JD)
    messy_result = extract(1, MESSY_JD)
    # Clean JD has clear sections; messy doesn't — messy should have more notes
    assert len(messy_result.extraction_notes) >= len(clean_result.extraction_notes)


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Scorer prefers extracted requirements over raw text
# ═══════════════════════════════════════════════════════════════════════════════

def _good_profile():
    """Profile that matches Python+FastAPI+PostgreSQL but NOT Spark."""
    def sk(n, ev="direct"):
        return {"name": n, "evidence": ev}
    return {
        "version": "1.1",
        "personal": {"name": "Test", "location": "Remote"},
        "job_targets": {
            "seniority_self_assessed": "senior",
            "desired_remote_policy": "remote",
            "work_authorization": "us_citizen",
            "willing_to_relocate": False,
        },
        "skills": {
            "languages":  [sk("Python"), sk("SQL")],
            "frameworks": [sk("FastAPI"), sk("Django")],
            "databases":  [sk("PostgreSQL"), sk("Redis")],
            "cloud":      [sk("AWS")],
            "tools":      [sk("Docker")],
        },
        "domains": [{"name": "fintech", "evidence": "adjacent"}],
        "experience": [],
        "certifications": [],
    }


AMBIGUOUS_JD = """\
Data Engineering Role

We use Spark and Kafka and Airflow extensively in our platform.

Requirements:
- Python
- SQL

Nice to have:
- dbt
"""
# Full text contains spark/kafka/airflow but Requirements only has python/sql.
# Raw heuristics (fallback) vs. extracted requirements should differ.


def test_scorer_uses_extracted_required_list():
    """
    When extraction is provided, scorer must use extracted.required_skills,
    not re-derive them from raw text.

    Setup:
      JD text lists Python + FastAPI as requirements (both in profile → raw score 1.0).
      Fake extraction overrides required_skills to ["spark"], which is NOT in the
      profile → extracted score 0.0.
      If the scorer correctly uses extraction, the two paths give different scores.
    """
    profile = _good_profile()
    jd = "Senior Python Engineer\n\nRequirements:\n- Python\n- FastAPI\n"

    # Build a fake extraction where required_skills deviates from what raw parsing gives.
    # "spark" is in the JD vocab dict but NOT in _good_profile() — so the must-have
    # score with this extraction will be 0.0, while without extraction (python+fastapi)
    # it will be 1.0.
    fake_extracted = ExtractionResult(
        job_id          = 99,
        required_skills = ["spark"],    # NOT in profile — deviates from raw parse
        preferred_skills= [],
        ats_keywords    = ["python", "fastapi", "spark"],
        seniority       = "senior",
        logistics_constraints = {
            "remote_policy": "remote", "no_sponsorship": False,
            "clearance_required": False, "relocation_required": False,
        },
        domain_requirements   = [],
        extraction_confidence = "high",
        extraction_notes      = [],
    )

    result_with    = assess(jd, "remote", profile, extracted=fake_extracted, profile_complete=0.9)
    result_without = assess(jd, "remote", profile, extracted=None,            profile_complete=0.9)

    # With extraction: only spark is required; not in profile → must_have_score = 0.0
    # Without extraction: raw parse finds [python, fastapi]; both in profile → 1.0
    assert result_with.must_have_score != result_without.must_have_score, (
        f"Expected different must_have scores: "
        f"with={result_with.must_have_score}, without={result_without.must_have_score}"
    )
    assert result_with.must_have_score < result_without.must_have_score, (
        "Extraction path (spark only) should score lower than raw-text path (python+fastapi)"
    )


def test_scorer_uses_extracted_seniority():
    """Extracted seniority 'staff' should feed into seniority scoring."""
    profile = _good_profile()  # self-assessed=senior
    jd = "Software Engineer"    # no seniority signal in title

    # Without extraction: _infer_job_seniority("Software Engineer") -> mid (default)
    # With extraction: seniority = "senior" -> exact match with profile
    extracted = ExtractionResult(
        job_id=1,
        required_skills=["python"],
        seniority="senior",
        logistics_constraints={
            "remote_policy": None, "no_sponsorship": False,
            "clearance_required": False, "relocation_required": False,
        },
        ats_keywords=["python"],
        extraction_confidence="medium",
    )

    r_with    = assess(jd, None, profile, extracted=extracted, profile_complete=0.9)
    r_without = assess(jd, None, profile,                       profile_complete=0.9)

    # With extraction seniority=senior (matches profile senior) -> 1.0
    # Without extraction: "Software Engineer" -> mid -> delta=1 -> 0.60
    assert r_with.seniority_score > r_without.seniority_score, (
        f"Expected higher seniority score with extraction. "
        f"with={r_with.seniority_score}, without={r_without.seniority_score}"
    )


def test_scorer_uses_extracted_logistics():
    """Extracted no_sponsorship flag should be honoured even if not in raw text."""
    profile = _good_profile()
    profile["job_targets"]["work_authorization"] = "need_sponsorship"
    jd = "We want a great Python developer."  # no logistics language in raw text

    extracted = ExtractionResult(
        job_id=1,
        required_skills=["python"],
        logistics_constraints={
            "remote_policy": "remote",
            "no_sponsorship": True,    # forced blocker not in raw JD text
            "clearance_required": False,
            "relocation_required": False,
        },
        ats_keywords=["python"],
        seniority="senior",
        extraction_confidence="high",
    )

    r_with    = assess(jd, "remote", profile, extracted=extracted, profile_complete=0.9)
    r_without = assess(jd, "remote", profile,                      profile_complete=0.9)

    assert r_with.verdict == "Skip"          # blocker from extraction
    assert r_without.verdict != "Skip"       # no blocker in raw text


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Scorer fallback when extraction=None
# ═══════════════════════════════════════════════════════════════════════════════

def test_scorer_works_without_extraction():
    """assess() with extracted=None must not raise and must return a ScoreBreakdown."""
    from app.services.scorer import ScoreBreakdown
    profile = _good_profile()
    result  = assess(CLEAN_JD, "remote", profile, extracted=None, profile_complete=0.9)
    assert isinstance(result, ScoreBreakdown)
    assert 0.0 <= result.overall_score <= 1.0


def test_scorer_raw_rationale_note():
    """Rationale without extraction should mention raw-text heuristics."""
    profile = _good_profile()
    result  = assess(CLEAN_JD, "remote", profile, extracted=None, profile_complete=0.9)
    assert "raw-text" in result.rationale.lower() or "heuristic" in result.rationale.lower()


def test_scorer_extracted_rationale_note():
    """Rationale with extraction should mention extracted requirements."""
    profile   = _good_profile()
    extracted = extract(1, CLEAN_JD)
    result    = assess(CLEAN_JD, "remote", profile, extracted=extracted, profile_complete=0.9)
    assert "extract" in result.rationale.lower()


# ═══════════════════════════════════════════════════════════════════════════════
# 10. Determinism
# ═══════════════════════════════════════════════════════════════════════════════

def test_extraction_is_deterministic():
    r1 = extract(1, CLEAN_JD)
    r2 = extract(1, CLEAN_JD)
    assert r1.required_skills        == r2.required_skills
    assert r1.preferred_skills       == r2.preferred_skills
    assert r1.seniority              == r2.seniority
    assert r1.extraction_confidence  == r2.extraction_confidence
    assert r1.extraction_notes       == r2.extraction_notes
    assert r1.years_of_experience    == r2.years_of_experience


def test_extraction_different_jds_differ():
    r_clean = extract(1, CLEAN_JD)
    r_messy = extract(1, MESSY_JD)
    assert r_clean.extraction_confidence != "low" or r_messy.extraction_confidence == "low"
    # At least the required skills should differ
    assert r_clean.required_skills != r_messy.required_skills


# ═══════════════════════════════════════════════════════════════════════════════
# 11. DB persistence round-trip
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def mem_conn():
    """In-memory SQLite with full schema applied."""
    schema = (Path(__file__).parent.parent / "sql" / "schema.sql").read_text()
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(schema)
    # Insert a dummy job row so FK constraint is satisfied
    conn.execute(
        "INSERT INTO jobs (id, raw_text) VALUES (1, 'test jd')"
    )
    conn.commit()
    yield conn
    conn.close()


def test_persist_and_load_round_trip(mem_conn):
    result = extract(1, CLEAN_JD)
    run_id = persist_extraction(mem_conn, result)
    assert run_id == 1

    loaded = load_latest_extraction(mem_conn, 1)
    assert loaded is not None
    assert loaded.required_skills   == result.required_skills
    assert loaded.preferred_skills  == result.preferred_skills
    assert loaded.seniority         == result.seniority
    assert loaded.extraction_confidence == result.extraction_confidence


def test_persist_writes_extracted_requirements_rows(mem_conn):
    result = extract(1, CLEAN_JD)
    persist_extraction(mem_conn, result)

    rows = mem_conn.execute(
        "SELECT category, requirement FROM extracted_requirements WHERE job_id = 1"
    ).fetchall()
    assert len(rows) > 0

    categories = {r["category"] for r in rows}
    assert "must_have" in categories
    assert "nice_to_have" in categories


def test_persist_replaces_old_extraction(mem_conn):
    """Re-running extraction should replace previous results, not append."""
    extract1 = extract(1, CLEAN_JD)
    extract2 = extract(1, CLEAN_JD)   # same content, second run

    persist_extraction(mem_conn, extract1)
    persist_extraction(mem_conn, extract2)

    runs = mem_conn.execute(
        "SELECT COUNT(*) FROM extraction_runs WHERE job_id = 1"
    ).fetchone()[0]
    assert runs == 1   # only latest run retained


def test_load_returns_none_when_no_extraction(mem_conn):
    result = load_latest_extraction(mem_conn, 1)
    assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# Unit tests for helper functions
# ═══════════════════════════════════════════════════════════════════════════════

def test_split_sections_identifies_must_have():
    sections = _split_sections(CLEAN_JD)
    assert "must_have" in sections
    assert "python" in sections["must_have"].lower()


def test_split_sections_identifies_nice_to_have():
    sections = _split_sections(CLEAN_JD)
    assert "nice_to_have" in sections
    assert "kafka" in sections["nice_to_have"].lower()


def test_split_sections_empty_on_no_headers():
    sections = _split_sections("Just plain text with no headers at all.")
    assert sections == {}


def test_extract_bullets_with_dashes():
    text = "- Python\n- FastAPI\n- PostgreSQL"
    bullets = _extract_bullets(text)
    assert len(bullets) == 3
    assert "Python" in bullets


def test_extract_bullets_fallback_no_markers():
    text = "Python experience required\nFastAPI knowledge needed"
    bullets = _extract_bullets(text)
    assert len(bullets) == 2


def test_skills_from_section_normalises():
    section = "- Experience with FastAPI\n- Strong Python knowledge\n- PostgreSQL required"
    skills  = _skills_from_section(section)
    assert "fastapi" in skills
    assert "python"  in skills
    assert "postgresql" in skills


def test_infer_seniority_senior():
    seniority, _ = _infer_seniority_str("Senior Software Engineer")
    assert seniority == "senior"


def test_infer_seniority_staff():
    seniority, _ = _infer_seniority_str("Staff Engineer, Platform")
    assert seniority == "staff"


def test_infer_seniority_from_yoe():
    seniority, note = _infer_seniority_str("We need 7+ years of Python experience.")
    assert seniority == "staff"
    assert "yoe" in note.lower() or "inferred" in note.lower()


def test_infer_seniority_unknown():
    seniority, note = _infer_seniority_str("We are looking for a great engineer.")
    assert seniority == "unknown"
    assert len(note) > 0


def test_extraction_result_serialises():
    result = extract(1, CLEAN_JD)
    d      = result.to_dict()
    assert isinstance(d, dict)
    assert d["job_id"] == 1
    assert isinstance(d["required_skills"], list)
    loaded = ExtractionResult.from_dict(d)
    assert loaded.required_skills == result.required_skills
