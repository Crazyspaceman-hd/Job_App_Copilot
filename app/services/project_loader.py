"""
app/services/project_loader.py — Load and validate data/project_inventory.json.

Projects contribute 'adjacent' skill evidence to the scorer when a skill is
listed in a project but not explicitly in the candidate's skills section.
"""

import json
from pathlib import Path
from typing import Any

_PROJECT_ROOT      = Path(__file__).resolve().parent.parent.parent
DEFAULT_INVENTORY  = _PROJECT_ROOT / "data" / "project_inventory.json"


# ── Public API ────────────────────────────────────────────────────────────────

def load_projects(path: Path | None = None) -> list[dict[str, Any]]:
    """
    Load project_inventory.json and return the projects list.
    Returns an empty list if the file doesn't exist (projects are optional).
    """
    path = Path(path) if path else DEFAULT_INVENTORY
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict) or "projects" not in data:
        raise ValueError("project_inventory.json must be an object with a 'projects' key.")
    raw = data["projects"]
    if not isinstance(raw, list):
        raise ValueError("project_inventory.json: 'projects' must be a list.")
    return [p for p in raw if _is_real_project(p)]


def extract_project_skills(projects: list[dict[str, Any]]) -> set[str]:
    """
    Return a flat set of normalised skill names found across all real projects.
    Used by the scorer to grant 'adjacent' evidence for skills not listed
    directly in the candidate profile.
    """
    skills: set[str] = set()
    for proj in projects:
        for s in proj.get("skills", []):
            if isinstance(s, str) and not s.lower().startswith("todo"):
                skills.add(s.lower().strip())
    return skills


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_real_project(proj: dict) -> bool:
    """Filter out template/TODO placeholder entries."""
    if not isinstance(proj, dict):
        return False
    title = proj.get("title", "")
    return isinstance(title, str) and bool(title) and not title.lower().startswith("todo")
