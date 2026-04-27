"""
tests/test_fit_pipeline.py — End-to-end fit pipeline tests.

Covered cases:
  1.  direct_evidence populated when required skills have direct profile coverage
  2.  adjacent_evidence populated when required skills have adjacent coverage
  3.  unsupported_gaps populated when required skills are absent from profile
  4.  hard_blockers property is an alias for red_flags
  5.  to_json() includes hard_blockers key
  6.  evidence_dict() returns all four bucket keys
  7.  DB persistence: persist_assessment() writes evidence_json column
  8.  evidence_json contains all four machine-readable bucket keys
  9.  Evidence buckets work when using ExtractionResult (structured extraction)
 10.  Evidence buckets work when extraction=None (raw-text fallback)
 11.  Verdict is restricted to the four allowed strings
 12.  Score and confidence are independent (low-confidence profile can have high score)
 13.  Full pipeline: load profile → extract → assess → persist (clean JD)
 14.  Full pipeline: load profile → assess → persist (messy JD, no extraction)
 15.  ScoreBreakdown fields are all present and typed correctly
"""

import json
import sqlite3
from pathlib import Path

import pytest

from app.services.extractor import ExtractionResult, extract
from app.services.profile_loader import load_profile, completeness
from app.services.scorer import (
    ScoreBreakdown,
    assess,
    persist_assessment,
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_VALID_VERDICTS = {"Strong fit", "Reach but viable", "Long shot", "Skip"}


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mem_conn():
    """In-memory SQLite with full schema + all migrations applied."""
    from app.db import apply_migrations
    schema = (_PROJECT_ROOT / "sql" / "schema.sql").read_text(encoding="utf-8")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(schema)
    apply_migrations(conn)
    conn.execute("INSERT INTO jobs (id, raw_text, title) VALUES (1, 'test jd', 'Test Job')")
    conn.commit()
    yield conn
    conn.close()


def _insert_profile_snapshot(conn, profile: dict) -> int:
    """Insert a candidate_profiles row and return its id."""
    cur = conn.execute(
        "INSERT INTO candidate_profiles (version, profile_json) VALUES (?, ?)",
        (profile.get("version", "1.0"), json.dumps(profile)),
    )
    conn.commit()
    return cur.lastrowid


def _profile(*, direct=None, adjacent=None, seniority="senior",
             remote="remote", work_auth="us_citizen") -> dict:
    """Build a minimal test profile."""
    def sk(name, ev="direct"):
        return {"name": name, "evidence": ev}

    skills: dict = {"languages": [], "frameworks": [], "databases": [], "cloud": [], "tools": []}
    for name in (direct or []):
        skills["languages"].append(sk(name, "direct"))
    for name in (adjacent or []):
        skills["frameworks"].append(sk(name, "adjacent"))

    return {
        "version": "1.1",
        "personal": {"name": "Test User", "location": "Remote"},
        "job_targets": {
            "seniority_self_assessed": seniority,
            "desired_remote_policy": remote,
            "work_authorization": work_auth,
            "willing_to_relocate": False,
        },
        "skills": skills,
        "domains":        [],
        "experience":     [],
        "certifications": [],
        "hard_constraints": {},
    }


def _extraction(required=None, preferred=None, seniority="senior",
                remote_policy="remote", no_sponsorship=False) -> ExtractionResult:
    skills = list(required or [])
    return ExtractionResult(
        job_id                = 1,
        required_skills       = skills,
        preferred_skills      = list(preferred or []),
        ats_keywords          = skills,
        seniority             = seniority,
        logistics_constraints = {
            "remote_policy":      remote_policy,
            "no_sponsorship":     no_sponsorship,
            "clearance_required": False,
            "relocation_required": False,
        },
        extraction_confidence = "high",
        extraction_notes      = [],
    )


CLEAN_JD = """\
Senior Python Engineer -- Fully Remote

Requirements:
- 5+ years of Python experience
- Strong knowledge of FastAPI
- Experience with PostgreSQL and Redis

Nice to have:
- Kafka

Location: Fully remote
No visa sponsorship available.
"""

MESSY_JD = """\
We need someone good at Python. FastAPI knowledge helpful.
You should have 3-5 years experience. Hybrid.
"""


# ═══════════════════════════════════════════════════════════════════════════════
# 1–3. Evidence bucket population
# ═══════════════════════════════════════════════════════════════════════════════

def test_direct_evidence_populated():
    """Required skills with direct profile coverage appear in direct_evidence."""
    profile  = _profile(direct=["python", "fastapi", "postgresql"])
    ext      = _extraction(required=["python", "fastapi", "postgresql"])
    result   = assess(CLEAN_JD, "remote", profile, extracted=ext, profile_complete=0.9)

    assert "python"     in result.direct_evidence
    assert "fastapi"    in result.direct_evidence
    assert "postgresql" in result.direct_evidence
    assert result.adjacent_evidence == []
    assert result.unsupported_gaps  == []


def test_adjacent_evidence_populated():
    """Required skills with adjacent coverage appear in adjacent_evidence, not direct."""
    profile = _profile(adjacent=["fastapi"])   # fastapi is adjacent
    ext     = _extraction(required=["python", "fastapi"])
    result  = assess("Software Engineer", "remote", profile, extracted=ext,
                     profile_complete=0.8)

    assert "fastapi" in result.adjacent_evidence
    assert "fastapi" not in result.direct_evidence
    # python has no evidence at all → gap
    assert "python" in result.unsupported_gaps


def test_unsupported_gaps_populated():
    """Required skills not in profile at all appear in unsupported_gaps."""
    profile = _profile(direct=["python"])
    ext     = _extraction(required=["python", "spark", "kafka"])
    result  = assess("Data Engineer", "remote", profile, extracted=ext,
                     profile_complete=0.8)

    assert "python" in result.direct_evidence
    assert "spark"  in result.unsupported_gaps
    assert "kafka"  in result.unsupported_gaps


def test_all_buckets_disjoint():
    """A skill should appear in at most one evidence bucket."""
    profile = _profile(direct=["python"], adjacent=["fastapi"])
    ext     = _extraction(required=["python", "fastapi", "spark"])
    result  = assess("Engineer", "remote", profile, extracted=ext, profile_complete=0.8)

    all_skills = (
        set(result.direct_evidence)
        | set(result.adjacent_evidence)
        | set(result.unsupported_gaps)
    )
    # No skill should appear in two buckets
    total_items = (
        len(result.direct_evidence)
        + len(result.adjacent_evidence)
        + len(result.unsupported_gaps)
    )
    assert total_items == len(all_skills), "A skill appeared in more than one bucket"


# ═══════════════════════════════════════════════════════════════════════════════
# 4–6. hard_blockers / evidence_dict
# ═══════════════════════════════════════════════════════════════════════════════

def test_hard_blockers_aliases_red_flags():
    """hard_blockers property must return exactly the same list as red_flags."""
    profile = _profile(work_auth="need_sponsorship")
    ext     = _extraction(required=["python"], no_sponsorship=True)
    result  = assess("Engineer", "remote", profile, extracted=ext, profile_complete=0.8)

    assert result.hard_blockers is result.red_flags


def test_hard_blockers_populated_on_blocker():
    """A no-sponsorship blocker should appear in hard_blockers."""
    profile = _profile(work_auth="need_sponsorship")
    ext     = _extraction(required=["python"], no_sponsorship=True)
    result  = assess("Engineer", "remote", profile, extracted=ext, profile_complete=0.8)

    assert len(result.hard_blockers) > 0
    assert any("sponsorship" in b.lower() for b in result.hard_blockers)


def test_to_json_includes_hard_blockers():
    """ScoreBreakdown.to_json() must include a hard_blockers key."""
    profile = _profile(direct=["python"])
    result  = assess(CLEAN_JD, "remote", profile, profile_complete=0.5)
    parsed  = json.loads(result.to_json())
    assert "hard_blockers" in parsed
    assert isinstance(parsed["hard_blockers"], list)


def test_evidence_dict_has_all_four_keys():
    """evidence_dict() must return all four machine-readable keys."""
    profile  = _profile(direct=["python"])
    result   = assess(CLEAN_JD, "remote", profile, profile_complete=0.5)
    ev       = result.evidence_dict()
    assert set(ev.keys()) >= {"direct_evidence", "adjacent_evidence",
                               "unsupported_gaps", "hard_blockers"}


# ═══════════════════════════════════════════════════════════════════════════════
# 7–8. DB persistence
# ═══════════════════════════════════════════════════════════════════════════════

def test_persist_assessment_returns_row_id(mem_conn):
    profile    = _profile(direct=["python"])
    profile_id = _insert_profile_snapshot(mem_conn, profile)
    result     = assess(CLEAN_JD, "remote", profile, profile_complete=0.8)

    assessment_id = persist_assessment(mem_conn, 1, profile_id, result)
    assert isinstance(assessment_id, int)
    assert assessment_id >= 1


def test_persist_assessment_writes_core_columns(mem_conn):
    """All primary score columns should be stored."""
    profile    = _profile(direct=["python", "fastapi"])
    profile_id = _insert_profile_snapshot(mem_conn, profile)
    result     = assess(CLEAN_JD, "remote", profile, profile_complete=0.8)
    aid        = persist_assessment(mem_conn, 1, profile_id, result)

    row = mem_conn.execute(
        "SELECT overall_score, verdict, confidence, rationale, gap_summary "
        "FROM fit_assessments WHERE id = ?", (aid,)
    ).fetchone()
    assert row is not None
    assert row["verdict"] in _VALID_VERDICTS
    assert row["overall_score"] is not None
    assert row["confidence"] in ("low", "medium", "high")


def test_persist_assessment_writes_evidence_json(mem_conn):
    """evidence_json column must be stored and parseable."""
    profile    = _profile(direct=["python"], adjacent=["fastapi"])
    profile_id = _insert_profile_snapshot(mem_conn, profile)
    ext        = _extraction(required=["python", "fastapi", "spark"])
    result     = assess(CLEAN_JD, "remote", profile, extracted=ext, profile_complete=0.8)
    aid        = persist_assessment(mem_conn, 1, profile_id, result)

    row = mem_conn.execute(
        "SELECT evidence_json FROM fit_assessments WHERE id = ?", (aid,)
    ).fetchone()
    assert row["evidence_json"] is not None

    ev = json.loads(row["evidence_json"])
    assert "direct_evidence"   in ev
    assert "adjacent_evidence" in ev
    assert "unsupported_gaps"  in ev
    assert "hard_blockers"     in ev


def test_persist_assessment_evidence_values_correct(mem_conn):
    """The stored evidence_json values should match the result's evidence buckets."""
    profile    = _profile(direct=["python"])
    profile_id = _insert_profile_snapshot(mem_conn, profile)
    ext        = _extraction(required=["python", "spark"])
    result     = assess("Engineer", "remote", profile, extracted=ext, profile_complete=0.8)
    aid        = persist_assessment(mem_conn, 1, profile_id, result)

    row = mem_conn.execute(
        "SELECT evidence_json FROM fit_assessments WHERE id = ?", (aid,)
    ).fetchone()
    ev = json.loads(row["evidence_json"])

    assert ev["direct_evidence"]  == result.direct_evidence
    assert ev["adjacent_evidence"] == result.adjacent_evidence
    assert ev["unsupported_gaps"]  == result.unsupported_gaps
    assert ev["hard_blockers"]     == result.hard_blockers


# ═══════════════════════════════════════════════════════════════════════════════
# 9–10. Evidence buckets with and without extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_evidence_buckets_with_extraction():
    """Buckets are populated using extracted.required_skills when extraction is provided."""
    profile = _profile(direct=["python"], adjacent=["fastapi"])
    ext     = _extraction(required=["python", "fastapi", "spark"])
    result  = assess(CLEAN_JD, "remote", profile, extracted=ext, profile_complete=0.9)

    assert "python"  in result.direct_evidence
    assert "fastapi" in result.adjacent_evidence
    assert "spark"   in result.unsupported_gaps


def test_evidence_buckets_without_extraction_no_crash():
    """Without extraction, buckets should still be populated (raw-text path)."""
    profile = _profile(direct=["python"])
    result  = assess(CLEAN_JD, "remote", profile, extracted=None, profile_complete=0.8)

    # Result must have evidence bucket fields (even if populated from raw-text heuristics)
    assert isinstance(result.direct_evidence,   list)
    assert isinstance(result.adjacent_evidence, list)
    assert isinstance(result.unsupported_gaps,  list)


def test_evidence_buckets_without_extraction_captures_python():
    """Raw-text path should capture 'python' in direct_evidence for a direct-profile."""
    profile = _profile(direct=["python"])
    result  = assess(CLEAN_JD, "remote", profile, extracted=None, profile_complete=0.8)
    # python is in the JD and in the profile as direct
    assert "python" in result.direct_evidence


# ═══════════════════════════════════════════════════════════════════════════════
# 11–12. Verdict + score/confidence independence
# ═══════════════════════════════════════════════════════════════════════════════

def test_verdict_restricted_to_valid_values():
    """Verdict must always be one of the four allowed strings."""
    for profile, jd in [
        (_profile(direct=["python", "fastapi", "postgresql"]), CLEAN_JD),
        (_profile(), CLEAN_JD),
        (_profile(work_auth="need_sponsorship"), CLEAN_JD),
    ]:
        result = assess(jd, "remote", profile, profile_complete=0.5)
        assert result.verdict in _VALID_VERDICTS, \
            f"Unexpected verdict: {result.verdict!r}"


def test_score_and_confidence_are_independent():
    """
    A well-skilled profile with incomplete metadata fields should have
    high must-have coverage (high score component) but only medium confidence
    because the completeness score is below the high threshold.
    """
    # Strong skills, but set profile_complete to 0.45 (below 0.75 threshold for 'high')
    profile = _profile(direct=["python", "fastapi", "postgresql"])
    ext     = _extraction(required=["python", "fastapi", "postgresql"])
    result  = assess(CLEAN_JD, "remote", profile, extracted=ext, profile_complete=0.45)

    # Must-have coverage should be perfect
    assert result.must_have_score == 1.0
    # But confidence should NOT be 'high' (completeness < 0.75)
    assert result.confidence in ("low", "medium"), \
        f"Expected non-high confidence at completeness=0.45, got {result.confidence!r}"


# ═══════════════════════════════════════════════════════════════════════════════
# 13–14. Full pipeline (load → extract/assess → persist)
# ═══════════════════════════════════════════════════════════════════════════════

def test_full_pipeline_clean_jd_with_good_profile(mem_conn):
    """
    Load the sample_profile_good.json → run extraction on CLEAN_JD →
    assess → persist.  Confirms the whole pipeline connects end-to-end.
    """
    good_path = _PROJECT_ROOT / "data" / "sample_profile_good.json"
    if not good_path.exists():
        pytest.skip("sample_profile_good.json not present")

    profile   = load_profile(good_path)
    prof_comp = completeness(profile)
    extracted = extract(1, CLEAN_JD)
    result    = assess(
        job_raw_text      = CLEAN_JD,
        job_remote_policy = "remote",
        profile           = profile,
        profile_complete  = prof_comp,
        extracted         = extracted,
    )

    assert isinstance(result, ScoreBreakdown)
    assert result.verdict in _VALID_VERDICTS
    assert 0.0 <= result.overall_score <= 1.0

    # Persist and verify
    profile_id    = _insert_profile_snapshot(mem_conn, profile)
    assessment_id = persist_assessment(mem_conn, 1, profile_id, result)

    row = mem_conn.execute(
        "SELECT verdict, overall_score, evidence_json "
        "FROM fit_assessments WHERE id = ?", (assessment_id,)
    ).fetchone()
    assert row["verdict"] in _VALID_VERDICTS
    ev = json.loads(row["evidence_json"])
    assert set(ev) >= {"direct_evidence", "adjacent_evidence",
                       "unsupported_gaps", "hard_blockers"}


def test_full_pipeline_messy_jd_no_extraction(mem_conn):
    """
    Raw-text fallback path: assess MESSY_JD without extraction.
    Pipeline must complete and persist without errors.
    """
    profile    = _profile(direct=["python", "fastapi"])
    result     = assess(MESSY_JD, "hybrid", profile, extracted=None, profile_complete=0.7)
    profile_id = _insert_profile_snapshot(mem_conn, profile)
    aid        = persist_assessment(mem_conn, 1, profile_id, result)

    row = mem_conn.execute(
        "SELECT verdict, scores_json, evidence_json FROM fit_assessments WHERE id = ?",
        (aid,),
    ).fetchone()
    assert row is not None
    assert row["verdict"] in _VALID_VERDICTS
    # scores_json must be parseable
    scores = json.loads(row["scores_json"])
    assert "overall_score" in scores
    assert "hard_blockers" in scores   # included by to_json()


# ═══════════════════════════════════════════════════════════════════════════════
# 15. ScoreBreakdown field completeness
# ═══════════════════════════════════════════════════════════════════════════════

def test_score_breakdown_has_all_required_fields():
    """All expected fields must be present and correctly typed."""
    profile = _profile(direct=["python"])
    result  = assess(CLEAN_JD, "remote", profile, profile_complete=0.6)

    assert isinstance(result.must_have_score,    float)
    assert isinstance(result.nice_to_have_score, float)
    assert isinstance(result.domain_score,       float)
    assert isinstance(result.seniority_score,    float)
    assert isinstance(result.logistics_score,    float)
    assert isinstance(result.ats_score,          float)
    assert isinstance(result.overall_score,      float)
    assert isinstance(result.verdict,            str)
    assert isinstance(result.confidence,         str)
    assert isinstance(result.strengths,          list)
    assert isinstance(result.gaps,               list)
    assert isinstance(result.red_flags,          list)
    assert isinstance(result.rationale,          str)
    assert isinstance(result.direct_evidence,    list)
    assert isinstance(result.adjacent_evidence,  list)
    assert isinstance(result.unsupported_gaps,   list)
    assert isinstance(result.hard_blockers,      list)
    assert 0.0 <= result.overall_score <= 1.0
