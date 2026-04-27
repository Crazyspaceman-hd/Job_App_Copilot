"""
app/services/profile_loader.py — Load and validate data/candidate_profile.json.

The profile is the single source of truth about the candidate.  The loader
enforces a minimum structure so the scorer never has to guess about missing
keys, while remaining lenient enough that a partially-filled profile still
runs (with confidence=low).
"""

import json
from pathlib import Path
from typing import Any

_PROJECT_ROOT    = Path(__file__).resolve().parent.parent.parent
DEFAULT_PROFILE  = _PROJECT_ROOT / "data" / "candidate_profile.json"

# Keys that must exist at the top level
_REQUIRED_KEYS = {"version", "personal", "job_targets", "skills"}


# ── Public API ────────────────────────────────────────────────────────────────

def load_profile(path: Path | None = None) -> dict[str, Any]:
    """
    Load candidate_profile.json from *path* (default: data/candidate_profile.json).
    Returns a plain dict. Raises FileNotFoundError or ValueError on bad input.
    """
    path = Path(path) if path else DEFAULT_PROFILE
    if not path.exists():
        raise FileNotFoundError(
            f"Candidate profile not found at {path}. "
            "Copy data/candidate_profile.json and fill in your details."
        )
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    _validate(data)
    return data


def completeness(profile: dict[str, Any]) -> float:
    """
    Return a rough 0–1 completeness score based on how many key scoring fields
    are actually filled in vs. left as TODO placeholders.

    This feeds the scorer's confidence rating — it is NOT a quality score.
    """
    checks = [
        _field_filled(profile, ["personal", "name"]),
        _field_filled(profile, ["personal", "location"]),
        _field_filled(profile, ["job_targets", "seniority_self_assessed"]),
        _field_filled(profile, ["job_targets", "desired_remote_policy"]),
        _field_filled(profile, ["job_targets", "work_authorization"]),
        _has_real_skills(profile),
        _has_real_domains(profile),
        _has_real_experience(profile),
    ]
    return sum(checks) / len(checks)


# ── Validation ────────────────────────────────────────────────────────────────

def _validate(data: dict) -> None:
    if not isinstance(data, dict):
        raise ValueError("Profile JSON must be a top-level object.")
    missing = _REQUIRED_KEYS - data.keys()
    if missing:
        raise ValueError(f"Profile is missing required top-level keys: {missing}")
    if not isinstance(data.get("skills"), dict):
        raise ValueError("profile.skills must be an object (dict).")
    if not isinstance(data.get("job_targets"), dict):
        raise ValueError("profile.job_targets must be an object (dict).")


# ── Completeness helpers ──────────────────────────────────────────────────────

def _field_filled(profile: dict, path: list[str]) -> bool:
    """Return True if the nested field exists and is not a TODO placeholder."""
    node: Any = profile
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return False
        node = node[key]
    return _is_real(node)


def _is_real(value: Any) -> bool:
    """A value is 'real' if it's not empty, not None, and not a TODO string."""
    if value is None:
        return False
    if isinstance(value, str):
        stripped = value.strip().lower()
        return bool(stripped) and not stripped.startswith("todo")
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def _has_real_skills(profile: dict) -> bool:
    """True if at least one skill category has a non-TODO entry."""
    for cat_items in profile.get("skills", {}).values():
        if not isinstance(cat_items, list):
            continue
        for item in cat_items:
            name = item.get("name", "") if isinstance(item, dict) else str(item)
            ev   = item.get("evidence", "") if isinstance(item, dict) else "direct"
            if _is_real(name) and _is_real(ev):
                return True
    return False


def _has_real_domains(profile: dict) -> bool:
    for d in profile.get("domains", []):
        name = d.get("name", "") if isinstance(d, dict) else str(d)
        if _is_real(name):
            return True
    return False


def _has_real_experience(profile: dict) -> bool:
    for exp in profile.get("experience", []):
        if isinstance(exp, dict) and _is_real(exp.get("company")):
            return True
    return False
