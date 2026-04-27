"""
tests/test_scorer.py — Scorer behaviour tests.

Tests are intentionally self-contained: they build inline profiles and JD
strings rather than depending on the live data files.  This keeps them fast
and deterministic.

Five mandated cases:
  1. Strong overlap          — most required skills present → Strong fit
  2. Partial overlap         — roughly half the skills present → Reach / Long shot
  3. Obvious blocker         — no-sponsorship JD + candidate needs sponsorship → Skip
  4. Deterministic runs      — identical inputs produce identical output
  5. Empty / weak profile    — sparse profile → low confidence, honest score
"""

import pytest
from app.services.scorer import (
    assess,
    _build_skill_map,
    _extract_vocab_terms,
    _infer_job_seniority,
    _parse_jd_sections,
    _score_logistics,
    _score_seniority,
    ScoreBreakdown,
    WEIGHTS,
)


# ── Shared fixtures ───────────────────────────────────────────────────────────

def _profile(
    *,
    languages=None,
    frameworks=None,
    databases=None,
    cloud=None,
    tools=None,
    domains=None,
    seniority="senior",
    remote="remote",
    work_auth="us_citizen",
    willing_to_relocate=False,
) -> dict:
    """Build a minimal but valid profile dict for testing."""
    def skill(name, ev="direct"):
        return {"name": name, "evidence": ev}

    return {
        "version": "1.1",
        "personal": {"name": "Test User", "location": "Austin, TX"},
        "job_targets": {
            "titles": ["Senior Engineer"],
            "seniority_self_assessed": seniority,
            "desired_remote_policy": remote,
            "work_authorization": work_auth,
            "willing_to_relocate": willing_to_relocate,
        },
        "skills": {
            "languages":  [skill(n) for n in (languages or [])],
            "frameworks": [skill(n) for n in (frameworks or [])],
            "databases":  [skill(n) for n in (databases or [])],
            "cloud":      [skill(n) for n in (cloud or [])],
            "tools":      [skill(n) for n in (tools or [])],
        },
        "domains": [{"name": d, "evidence": "direct"} for d in (domains or [])],
        "experience":     [],
        "education":      [],
        "certifications": [],
        "hard_constraints": {},
    }


STRONG_MATCH_JD = """\
Senior Python Engineer — Fully Remote

Requirements:
- 5+ years of Python experience
- Strong knowledge of FastAPI and Django
- Experience with PostgreSQL and Redis
- Familiarity with AWS (Lambda, SQS, RDS)
- Docker and Kubernetes for containerisation

Nice to have:
- Kafka or event-driven architectures
- Fintech or payments domain experience

Location: Fully remote (US timezones preferred)
"""

PARTIAL_MATCH_JD = """\
Senior Data Engineer

Requirements:
- Python proficiency
- Experience with Spark and Kafka
- Snowflake or Redshift data warehousing
- Airflow for pipeline orchestration
- AWS or GCP cloud experience

Nice to have:
- dbt for data transformations
- Kubernetes
"""

BLOCKER_JD = """\
Senior Backend Engineer

Requirements:
- 5+ years Python
- PostgreSQL
- Must be authorized to work in the United States; no visa sponsorship available.

Hybrid role, Austin TX.
"""

ONSITE_RELOCATION_JD = """\
Software Engineer

Requirements:
- Python
- Django
- Must relocate to our San Francisco headquarters. Local candidates preferred.

This is a fully on-site position.
"""


# ═════════════════════════════════════════════════════════════════════════════
# Case 1: Strong overlap
# ═════════════════════════════════════════════════════════════════════════════

def test_strong_overlap_verdict():
    """Candidate with most required skills → Strong fit or Reach but viable."""
    profile = _profile(
        languages  = ["Python"],
        frameworks = ["FastAPI", "Django"],
        databases  = ["PostgreSQL", "Redis"],
        cloud      = ["AWS", "Lambda", "SQS", "RDS"],
        tools      = ["Docker", "Kubernetes"],
        domains    = ["fintech"],
        seniority  = "senior",
        remote     = "remote",
    )
    result = assess(STRONG_MATCH_JD, "remote", profile)

    assert result.verdict in ("Strong fit", "Reach but viable"), (
        f"Expected strong result, got: {result.verdict} (overall={result.overall_score})"
    )
    assert result.overall_score >= 0.60
    assert result.must_have_score >= 0.70
    assert not result.red_flags


def test_strong_overlap_strengths_populated():
    profile = _profile(
        languages  = ["Python"],
        frameworks = ["FastAPI", "Django"],
        databases  = ["PostgreSQL", "Redis"],
        cloud      = ["AWS"],
        tools      = ["Docker", "Kubernetes"],
    )
    result = assess(STRONG_MATCH_JD, "remote", profile)
    assert len(result.strengths) > 0
    skill_names = [s.lower() for s in result.strengths]
    assert any("python" in s for s in skill_names)


# ═════════════════════════════════════════════════════════════════════════════
# Case 2: Partial overlap
# ═════════════════════════════════════════════════════════════════════════════

def test_partial_overlap_verdict():
    """Candidate with ~40 % of required skills → Long shot or Reach but viable."""
    profile = _profile(
        languages = ["Python"],         # matches
        databases = ["PostgreSQL"],     # matches
        # missing: spark, kafka, snowflake, airflow, aws/gcp
    )
    result = assess(PARTIAL_MATCH_JD, None, profile)

    assert result.verdict in ("Long shot", "Reach but viable"), (
        f"Expected partial result, got: {result.verdict} (score={result.overall_score})"
    )
    assert result.overall_score < 0.70
    assert len(result.gaps) > 0


def test_partial_overlap_gaps_named():
    profile = _profile(languages=["Python"])
    result  = assess(PARTIAL_MATCH_JD, None, profile)
    gap_text = " ".join(result.gaps).lower()
    # Spark, Kafka, Snowflake/Redshift, Airflow should all appear as gaps
    assert any(g in gap_text for g in ("spark", "kafka", "snowflake", "airflow"))


# ═════════════════════════════════════════════════════════════════════════════
# Case 3: Obvious blocker
# ═════════════════════════════════════════════════════════════════════════════

def test_sponsorship_blocker_forces_skip():
    """JD says no sponsorship; candidate needs sponsorship → Skip."""
    profile = _profile(
        languages = ["Python"],
        databases = ["PostgreSQL"],
        work_auth = "need_sponsorship",
    )
    result = assess(BLOCKER_JD, "hybrid", profile)

    assert result.verdict == "Skip", (
        f"Expected Skip due to sponsorship blocker, got: {result.verdict}"
    )
    assert any("sponsorship" in rf.lower() for rf in result.red_flags)
    assert result.logistics_score == 0.0


def test_onsite_required_remote_only_blocker():
    """JD requires relocation; candidate won't relocate → blocker."""
    profile = _profile(
        languages           = ["Python", "Django"],
        remote              = "remote",
        willing_to_relocate = False,
    )
    result = assess(ONSITE_RELOCATION_JD, "onsite", profile)

    assert result.verdict == "Skip"
    assert result.red_flags


def test_blocker_overrides_high_skill_score():
    """Even if skill overlap is excellent, a blocker must produce Skip."""
    profile = _profile(
        languages  = ["Python"],
        frameworks = ["FastAPI", "Django"],
        databases  = ["PostgreSQL", "Redis"],
        cloud      = ["AWS"],
        tools      = ["Docker", "Kubernetes"],
        work_auth  = "need_sponsorship",   # ← blocker
    )
    result = assess(BLOCKER_JD, "hybrid", profile)

    # Skill coverage is good but the logistics blocker forces verdict to Skip.
    # overall_score reflects the actual weighted components (logistics=0.0 drags it
    # down but doesn't zero the whole score — that is intentional and honest).
    assert result.verdict == "Skip"
    assert result.must_have_score > 0.5       # skill overlap is real
    assert result.logistics_score == 0.0      # logistics component zeroed by blocker
    assert result.red_flags


# ═════════════════════════════════════════════════════════════════════════════
# Case 4: Deterministic repeated runs
# ═════════════════════════════════════════════════════════════════════════════

def test_deterministic_identical_inputs():
    """Same inputs must produce identical ScoreBreakdown every time."""
    profile = _profile(
        languages  = ["Python"],
        frameworks = ["FastAPI"],
        databases  = ["PostgreSQL"],
    )
    r1 = assess(STRONG_MATCH_JD, "remote", profile)
    r2 = assess(STRONG_MATCH_JD, "remote", profile)

    assert r1.overall_score    == r2.overall_score
    assert r1.must_have_score  == r2.must_have_score
    assert r1.verdict          == r2.verdict
    assert r1.confidence       == r2.confidence
    assert r1.strengths        == r2.strengths
    assert r1.gaps             == r2.gaps
    assert r1.red_flags        == r2.red_flags


def test_deterministic_different_jds_differ():
    """Different JDs must produce different scores for the same profile."""
    profile = _profile(languages=["Python"])
    r1 = assess(STRONG_MATCH_JD, "remote", profile)
    r2 = assess(PARTIAL_MATCH_JD, None,    profile)
    # Scores should differ (the JDs have different required skill sets)
    assert r1.overall_score != r2.overall_score


# ═════════════════════════════════════════════════════════════════════════════
# Case 5: Empty / weak profile
# ═════════════════════════════════════════════════════════════════════════════

def test_empty_profile_confidence_low():
    """A profile with no real skills should yield confidence='low'."""
    empty_profile = {
        "version": "1.1",
        "personal": {"name": "TODO"},
        "job_targets": {
            "seniority_self_assessed": "TODO: junior|mid|senior|staff|principal",
            "desired_remote_policy":   "TODO: remote|hybrid|onsite|any",
            "work_authorization":      "TODO: us_citizen|...",
        },
        "skills": {
            "languages":  [],
            "frameworks": [],
            "databases":  [],
        },
        "domains":        [],
        "experience":     [],
        "certifications": [],
    }
    result = assess(STRONG_MATCH_JD, "remote", empty_profile, profile_complete=0.0)

    assert result.confidence == "low"
    assert result.overall_score < 0.50   # gaps should drag the score down
    assert len(result.gaps) > 0


def test_empty_profile_verdict_not_strong():
    """An empty profile must not yield 'Strong fit'."""
    empty_profile = {
        "version": "1.1",
        "personal": {},
        "job_targets": {},
        "skills": {},
        "domains": [],
    }
    result = assess(STRONG_MATCH_JD, "remote", empty_profile, profile_complete=0.0)
    assert result.verdict != "Strong fit"


def test_weak_profile_no_fake_strengths():
    """With no skills listed, strengths list should be empty (seniority warnings go to gaps)."""
    empty_profile = {
        "version": "1.1",
        "personal": {},
        "job_targets": {},
        "skills": {},
        "domains": [],
    }
    result = assess(STRONG_MATCH_JD, "remote", empty_profile, profile_complete=0.0)
    assert result.strengths == [], (
        f"Expected no strengths for empty profile, got: {result.strengths}"
    )
    # Seniority 'cannot score' warning must appear in gaps, not strengths
    assert any("seniority" in g.lower() for g in result.gaps)


# ═════════════════════════════════════════════════════════════════════════════
# Unit tests for helper functions
# ═════════════════════════════════════════════════════════════════════════════

def test_extract_vocab_terms_multi_word():
    terms = _extract_vocab_terms("experience with machine learning and apache kafka")
    assert "machine learning" in terms
    # "apache kafka" is matched as a multi-word term; bare "kafka" is consumed by it
    assert "apache kafka" in terms or "kafka" in terms


def test_extract_vocab_terms_deduplication():
    # "spark" appears twice; should only appear once
    terms = _extract_vocab_terms("spark spark spark python")
    assert terms.count("spark") == 1
    assert terms.count("python") == 1


def test_build_skill_map_evidence_levels():
    profile = _profile(
        languages  = ["Python"],
        frameworks = ["FastAPI"],
    )
    # Make one skill adjacent
    profile["skills"]["frameworks"] = [{"name": "FastAPI", "evidence": "adjacent"}]
    sm = _build_skill_map(profile, set())
    assert sm.get("python")  == "direct"
    assert sm.get("fastapi") == "adjacent"


def test_build_skill_map_project_skills_fill_gaps():
    """Project skills should add 'adjacent' entries for skills not in profile."""
    profile  = _profile(languages=["Python"])
    sm       = _build_skill_map(profile, project_skills={"Kafka", "dbt"})
    assert sm.get("kafka") == "adjacent"
    assert sm.get("dbt")   == "adjacent"
    assert sm.get("python") == "direct"   # profile entry takes precedence


def test_build_skill_map_ignores_todo():
    """TODO placeholder entries must not appear as real skills."""
    profile = _profile()
    profile["skills"]["languages"] = [
        {"name": "TODO: e.g. Python", "evidence": "TODO: direct|adjacent|familiar"}
    ]
    sm = _build_skill_map(profile, set())
    assert not any("todo" in k for k in sm)


def test_infer_job_seniority_from_title():
    assert _infer_job_seniority("Senior Software Engineer — Remote")  == 3
    assert _infer_job_seniority("Staff Engineer, Platform")           == 4
    assert _infer_job_seniority("Principal Data Scientist")           == 5
    assert _infer_job_seniority("Junior Python Developer")            == 1


def test_infer_job_seniority_from_yoe():
    assert _infer_job_seniority("Requires 5+ years of Python experience") == 3
    assert _infer_job_seniority("3 to 5 years of experience required")    == 2
    assert _infer_job_seniority("10+ years preferred")                     == 4


def test_parse_jd_sections_splits_correctly():
    sections = _parse_jd_sections(STRONG_MATCH_JD)
    assert "python" in sections["must_have"].lower()
    assert "kafka"  in sections["nice_to_have"].lower()


def test_parse_jd_sections_no_headers():
    """JDs without section headers should not crash; must_have will be empty."""
    sections = _parse_jd_sections("We need someone who knows Python and Docker.")
    assert isinstance(sections["must_have"], str)
    assert isinstance(sections["nice_to_have"], str)


def test_score_logistics_no_blocker():
    profile  = _profile(remote="remote", work_auth="us_citizen")
    score, flags = _score_logistics("fully remote role", "remote", profile)
    assert score > 0.0
    assert flags == []


def test_score_logistics_sponsorship_blocker():
    profile = _profile(work_auth="need_sponsorship")
    score, flags = _score_logistics(
        "must be authorized to work in the US; no visa sponsorship", "hybrid", profile
    )
    assert score == 0.0
    assert any("sponsorship" in f.lower() for f in flags)


def test_weights_sum_to_one():
    total = sum(WEIGHTS.values())
    assert abs(total - 1.0) < 1e-9, f"Weights sum to {total}, expected 1.0"


def test_score_breakdown_to_json_round_trips():
    profile = _profile(languages=["Python"])
    result  = assess(STRONG_MATCH_JD, "remote", profile)
    import json
    parsed  = json.loads(result.to_json())
    assert parsed["verdict"]       == result.verdict
    assert parsed["overall_score"] == result.overall_score
    assert isinstance(parsed["strengths"], list)
    assert isinstance(parsed["gaps"],      list)
