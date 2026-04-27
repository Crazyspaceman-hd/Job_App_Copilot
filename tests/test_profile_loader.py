"""
tests/test_profile_loader.py — Profile loader behaviour tests.

Covered cases:
  1.  Load valid profile from explicit path
  2.  Load the default template file (exists on disk, full of TODOs)
  3.  Load data/sample_profile_good.json (full, non-TODO profile)
  4.  FileNotFoundError for missing file
  5.  JSONDecodeError for invalid JSON
  6.  ValueError for missing required top-level keys
  7.  ValueError when skills is not a dict
  8.  ValueError when job_targets is not a dict
  9.  completeness = 0.0 for an all-TODO profile
 10.  completeness = 1.0 for a fully-filled profile
 11.  completeness is strictly between 0 and 1 for a partial profile
 12.  TODO strings are never counted as real values
 13.  _has_real_skills returns False when all skill entries are TODOs
 14.  _has_real_skills returns True when at least one real entry exists
 15.  _has_real_domains / _has_real_experience behave correctly
 16.  Profile without optional keys (domains, experience) still loads
"""

import json
import pytest
from pathlib import Path

from app.services.profile_loader import (
    load_profile,
    completeness,
    _field_filled,
    _is_real,
    _has_real_skills,
    _has_real_domains,
    _has_real_experience,
    DEFAULT_PROFILE,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _write_profile(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "profile.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def _minimal_valid() -> dict:
    """Smallest dict that passes _validate()."""
    return {
        "version": "1.1",
        "personal": {"name": "Alice"},
        "job_targets": {"seniority_self_assessed": "senior"},
        "skills": {
            "languages": [{"name": "Python", "evidence": "direct"}]
        },
    }


def _good_profile() -> dict:
    """Fully-filled profile (mirrors sample_profile_good.json structure)."""
    def sk(name, ev="direct"):
        return {"name": name, "evidence": ev}

    return {
        "version": "1.1",
        "personal": {"name": "Alex Rivera", "location": "Austin, TX"},
        "job_targets": {
            "titles": ["Senior Backend Engineer"],
            "seniority_self_assessed": "senior",
            "desired_remote_policy": "remote",
            "work_authorization": "us_citizen",
            "willing_to_relocate": False,
        },
        "skills": {
            "languages":  [sk("Python"), sk("SQL")],
            "frameworks": [sk("FastAPI"), sk("Django", "adjacent")],
            "databases":  [sk("PostgreSQL"), sk("Redis")],
            "cloud":      [sk("AWS")],
            "tools":      [sk("Docker")],
        },
        "domains": [{"name": "fintech", "evidence": "adjacent"}],
        "experience": [
            {
                "company":    "Acme Corp",
                "title":      "Senior Software Engineer",
                "start_date": "2020-03",
                "end_date":   "present",
                "bullets":    ["Built payment APIs in Python + FastAPI"],
            }
        ],
        "education":      [{"institution": "UT Austin", "degree": "B.S. CS", "year": "2018"}],
        "certifications": [],
        "hard_constraints": {},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 1–3. Happy-path loads
# ═══════════════════════════════════════════════════════════════════════════════

def test_load_profile_valid_explicit_path(tmp_path):
    p = _write_profile(tmp_path, _minimal_valid())
    profile = load_profile(p)
    assert profile["version"] == "1.1"
    assert "skills" in profile


def test_load_profile_default_template_exists():
    """The shipped template (full of TODOs) must load without raising."""
    assert DEFAULT_PROFILE.exists(), "data/candidate_profile.json is missing from repo"
    profile = load_profile()   # no path → uses default
    assert "version" in profile
    assert "skills"  in profile


def test_load_good_sample_profile():
    """data/sample_profile_good.json must load and have real (non-TODO) skills."""
    good = _PROJECT_ROOT / "data" / "sample_profile_good.json"
    assert good.exists(), "data/sample_profile_good.json is missing"
    profile = load_profile(good)
    assert _has_real_skills(profile), "sample_profile_good.json has no real skills"


# ═══════════════════════════════════════════════════════════════════════════════
# 4–8. Error handling
# ═══════════════════════════════════════════════════════════════════════════════

def test_load_profile_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_profile(tmp_path / "does_not_exist.json")


def test_load_profile_invalid_json(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        load_profile(bad)


def test_load_profile_missing_required_key(tmp_path):
    """A profile missing 'skills' top-level key must raise ValueError."""
    data = {"version": "1.1", "personal": {}, "job_targets": {}}  # missing 'skills'
    p = _write_profile(tmp_path, data)
    with pytest.raises(ValueError, match="missing required"):
        load_profile(p)


def test_load_profile_skills_not_dict(tmp_path):
    data = _minimal_valid()
    data["skills"] = ["python", "sql"]   # list, not dict
    p = _write_profile(tmp_path, data)
    with pytest.raises(ValueError, match="skills must be"):
        load_profile(p)


def test_load_profile_job_targets_not_dict(tmp_path):
    data = _minimal_valid()
    data["job_targets"] = "senior"       # string, not dict
    p = _write_profile(tmp_path, data)
    with pytest.raises(ValueError, match="job_targets must be"):
        load_profile(p)


# ═══════════════════════════════════════════════════════════════════════════════
# 9–11. completeness()
# ═══════════════════════════════════════════════════════════════════════════════

def test_completeness_all_todo_is_zero():
    """The shipped template profile (all TODOs) should have completeness 0."""
    profile = load_profile()   # default template
    assert completeness(profile) == 0.0


def test_completeness_fully_filled_profile_is_high(tmp_path):
    p = _write_profile(tmp_path, _good_profile())
    profile = load_profile(p)
    score = completeness(profile)
    assert score == 1.0, f"Expected 1.0 for fully-filled profile, got {score}"


def test_completeness_partial_profile_is_between_0_and_1(tmp_path):
    """A profile with some fields filled, others missing: 0 < completeness < 1."""
    data = _minimal_valid()
    # Has: personal.name, job_targets.seniority, skills (real)
    # Missing: personal.location, desired_remote_policy, work_authorization, domains, experience
    p = _write_profile(tmp_path, data)
    profile = load_profile(p)
    score = completeness(profile)
    assert 0.0 < score < 1.0, f"Expected partial completeness, got {score}"


def test_completeness_increases_with_more_fields(tmp_path):
    """Adding more filled fields should not decrease completeness."""
    d1 = tmp_path / "p1"; d1.mkdir()
    d2 = tmp_path / "p2"; d2.mkdir()

    p_minimal = _write_profile(d1, _minimal_valid())
    p_full    = _write_profile(d2, _good_profile())

    c_minimal = completeness(load_profile(p_minimal))
    c_full    = completeness(load_profile(p_full))
    assert c_full >= c_minimal


# ═══════════════════════════════════════════════════════════════════════════════
# 12. _is_real
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("value,expected", [
    ("TODO: fill this in",     False),
    ("TODO",                   False),
    ("",                       False),
    (None,                     False),
    ("senior",                 True),
    ("us_citizen",             True),
    ("remote",                 True),
    (True,                     True),
    (False,                    True),   # False is a real value (willing_to_relocate=False)
    ([],                       False),
    (["item"],                 True),
    ({},                       False),
    ({"k": "v"},               True),
])
def test_is_real(value, expected):
    assert _is_real(value) == expected, f"_is_real({value!r}) expected {expected}"


# ═══════════════════════════════════════════════════════════════════════════════
# 13–14. _has_real_skills
# ═══════════════════════════════════════════════════════════════════════════════

def test_has_real_skills_all_todos_returns_false():
    profile = {
        "skills": {
            "languages": [
                {"name": "TODO: e.g. Python", "evidence": "TODO: direct|adjacent|familiar"}
            ]
        }
    }
    assert _has_real_skills(profile) is False


def test_has_real_skills_one_real_entry_returns_true():
    profile = {
        "skills": {
            "languages": [{"name": "Python", "evidence": "direct"}]
        }
    }
    assert _has_real_skills(profile) is True


def test_has_real_skills_empty_categories_returns_false():
    profile = {"skills": {"languages": [], "frameworks": []}}
    assert _has_real_skills(profile) is False


# ═══════════════════════════════════════════════════════════════════════════════
# 15. _has_real_domains / _has_real_experience
# ═══════════════════════════════════════════════════════════════════════════════

def test_has_real_domains_false_when_todo():
    profile = {"domains": [{"name": "TODO: e.g. data engineering", "evidence": "direct"}]}
    assert _has_real_domains(profile) is False


def test_has_real_domains_true_when_real():
    profile = {"domains": [{"name": "fintech", "evidence": "adjacent"}]}
    assert _has_real_domains(profile) is True


def test_has_real_experience_false_when_todo():
    profile = {"experience": [{"company": "TODO: Company Name"}]}
    assert _has_real_experience(profile) is False


def test_has_real_experience_true_when_real():
    profile = {"experience": [{"company": "Acme Corp", "title": "Engineer"}]}
    assert _has_real_experience(profile) is True


# ═══════════════════════════════════════════════════════════════════════════════
# 16. Optional keys
# ═══════════════════════════════════════════════════════════════════════════════

def test_profile_without_optional_keys_loads(tmp_path):
    """A profile missing optional keys (domains, experience, education) must load."""
    data = {
        "version": "1.1",
        "personal": {"name": "Bob"},
        "job_targets": {"seniority_self_assessed": "mid"},
        "skills": {"languages": [{"name": "Python", "evidence": "direct"}]},
        # no domains, no experience, no education, no certifications
    }
    p = _write_profile(tmp_path, data)
    profile = load_profile(p)
    assert profile["version"] == "1.1"
    # completeness should not crash
    score = completeness(profile)
    assert 0.0 <= score <= 1.0
