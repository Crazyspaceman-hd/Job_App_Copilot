"""
app/services/tracker.py — Application decision tracker and package viewer.

Design contract:
  - Simple: one row per decision; show-package pulls the latest of everything.
  - Non-destructive: decisions append to applications; history is preserved.
  - Deterministic: same inputs produce the same package output.
  - No external dependencies beyond app.db utilities.

Public API:
  save_application_decision(job_id, conn, status, ...)  -> int (application row id)
  load_application_package(job_id, conn)                -> ApplicationPackage
  load_latest_decision(job_id, conn)                    -> dict | None
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Optional


# ── Valid user-facing statuses ────────────────────────────────────────────────

VALID_STATUSES = frozenset({"apply", "hold", "skip"})

# Maps tracker status → jobs.status value
_JOB_STATUS_MAP = {
    "apply": "applied",
    "hold":  "reviewing",
    "skip":  "archived",
}


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class AssetRef:
    """Lightweight reference to a generated_assets row."""
    asset_id:        int
    asset_type:      str
    label:           str | None
    generated_at:    str
    content_preview: str   # first 200 chars of content


@dataclass
class RecommendationRef:
    """Lightweight reference to a project_recommendations row."""
    rec_id:               int
    recommendation_type:  str | None
    title:                str
    target_gap_or_signal: str | None
    business_problem:     str | None


@dataclass
class ApplicationRecord:
    """Latest application decision for a job."""
    application_id: int | None  = None
    status:         str | None  = None   # apply | hold | skip
    notes:          str | None  = None
    follow_up_date: str | None  = None
    platform:       str | None  = None
    last_updated:   str | None  = None


@dataclass
class ApplicationPackage:
    """Full single-pane-of-glass summary for one job."""
    # Job basics
    job_id:            int
    job_title:         str | None
    job_company:       str | None
    job_remote_policy: str | None
    job_status:        str

    # Fit assessment (latest)
    assessment_id:    int | None        = None
    assessed_at:      str | None        = None
    verdict:          str | None        = None
    overall_score:    float | None      = None
    confidence:       str | None        = None
    direct_evidence:  list[str]         = field(default_factory=list)
    adjacent_evidence: list[str]        = field(default_factory=list)
    unsupported_gaps: list[str]         = field(default_factory=list)

    # Generated assets (latest)
    resume:           AssetRef | None   = None
    cover_letter:     AssetRef | None   = None

    # Project recommendations (all for job)
    recommendations:  list[RecommendationRef] = field(default_factory=list)

    # Application decision (latest)
    application:      ApplicationRecord = field(default_factory=ApplicationRecord)


# ── Public entry points ───────────────────────────────────────────────────────

def save_application_decision(
    job_id:                int,
    conn:                  sqlite3.Connection,
    status:                str,
    notes:                 str | None         = None,
    follow_up_date:        str | None         = None,
    profile_id:            int | None         = None,
    resume_asset_id:       int | None         = None,
    cover_letter_asset_id: int | None         = None,
    recommendation_ids:    list[int] | None   = None,
    platform:              str | None         = None,
) -> int:
    """
    Record an application decision for *job_id*.

    Args:
        job_id:                DB id of the target job.
        conn:                  Open SQLite connection (caller owns lifecycle).
        status:                'apply' | 'hold' | 'skip'
        notes:                 Free-text reason or note.
        follow_up_date:        Optional ISO date string (YYYY-MM-DD).
        profile_id:            FK to candidate_profiles (optional).
        resume_asset_id:       FK to generated_assets resume row.  If None, the
                               latest resume for the job is linked automatically.
        cover_letter_asset_id: FK to generated_assets cover_letter row.  Same
                               auto-link logic as resume_asset_id.
        recommendation_ids:    Explicit list of project_recommendation ids.  If
                               None, the latest two recommendations are linked.
        platform:              Where the application was submitted (optional).

    Returns:
        The new applications row id.

    Raises:
        ValueError: if status is not one of the valid values or job not found.
    """
    if status not in VALID_STATUSES:
        raise ValueError(
            f"Invalid status {status!r}. Must be one of: {sorted(VALID_STATUSES)}"
        )

    # Verify job exists
    row = conn.execute("SELECT id FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        raise ValueError(f"Job id={job_id} not found in database.")

    # Auto-link latest resume if not provided
    if resume_asset_id is None:
        r = conn.execute(
            "SELECT id FROM generated_assets "
            "WHERE job_id = ? AND asset_type = 'resume' ORDER BY id DESC LIMIT 1",
            (job_id,),
        ).fetchone()
        resume_asset_id = r["id"] if r else None

    # Auto-link latest cover letter if not provided
    if cover_letter_asset_id is None:
        r = conn.execute(
            "SELECT id FROM generated_assets "
            "WHERE job_id = ? AND asset_type = 'cover_letter' ORDER BY id DESC LIMIT 1",
            (job_id,),
        ).fetchone()
        cover_letter_asset_id = r["id"] if r else None

    # Auto-link latest project recommendations if not provided
    if recommendation_ids is None:
        rows = conn.execute(
            "SELECT id FROM project_recommendations "
            "WHERE job_id = ? ORDER BY id DESC LIMIT 2",
            (job_id,),
        ).fetchall()
        recommendation_ids = [r["id"] for r in rows] if rows else []

    applied_at = None
    if status == "apply":
        applied_at = _now()

    rec_ids_json = json.dumps(recommendation_ids) if recommendation_ids else "[]"

    cur = conn.execute(
        """INSERT INTO applications
           (job_id, applied_at, platform, resume_asset_id, cover_letter_asset_id,
            notes, last_updated, status, profile_id, follow_up_date,
            recommendation_ids_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            job_id, applied_at, platform, resume_asset_id, cover_letter_asset_id,
            notes, _now(), status, profile_id, follow_up_date, rec_ids_json,
        ),
    )
    conn.commit()
    application_id = cur.lastrowid

    # Mirror to jobs.status
    new_job_status = _JOB_STATUS_MAP[status]
    conn.execute("UPDATE jobs SET status = ? WHERE id = ?", (new_job_status, job_id))
    conn.commit()

    return application_id


def load_application_package(
    job_id: int,
    conn:   sqlite3.Connection,
) -> ApplicationPackage:
    """
    Assemble the full application package for *job_id*.

    Pulls the latest row from each relevant table (fit_assessments,
    generated_assets, project_recommendations, applications) and returns
    a structured ApplicationPackage.

    Raises:
        ValueError: if *job_id* is not found.
    """
    job = conn.execute(
        "SELECT id, title, company, remote_policy, status FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    if not job:
        raise ValueError(f"Job id={job_id} not found in database.")

    pkg = ApplicationPackage(
        job_id            = job_id,
        job_title         = job["title"],
        job_company       = job["company"],
        job_remote_policy = job["remote_policy"],
        job_status        = job["status"],
    )

    # ── Latest fit assessment ─────────────────────────────────────────────────
    arow = conn.execute(
        "SELECT id, assessed_at, verdict, confidence, overall_score, evidence_json "
        "FROM fit_assessments WHERE job_id = ? ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if arow:
        pkg.assessment_id = arow["id"]
        pkg.assessed_at   = arow["assessed_at"]
        pkg.verdict       = arow["verdict"]
        pkg.overall_score = arow["overall_score"]
        pkg.confidence    = arow["confidence"]
        if arow["evidence_json"]:
            ev = json.loads(arow["evidence_json"])
            pkg.direct_evidence   = ev.get("direct_evidence",   [])
            pkg.adjacent_evidence = ev.get("adjacent_evidence", [])
            pkg.unsupported_gaps  = ev.get("unsupported_gaps",  [])

    # ── Latest generated resume ───────────────────────────────────────────────
    rrow = conn.execute(
        "SELECT id, label, generated_at, content FROM generated_assets "
        "WHERE job_id = ? AND asset_type = 'resume' ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if rrow:
        pkg.resume = AssetRef(
            asset_id        = rrow["id"],
            asset_type      = "resume",
            label           = rrow["label"],
            generated_at    = rrow["generated_at"],
            content_preview = (rrow["content"] or "")[:200],
        )

    # ── Latest generated cover letter ─────────────────────────────────────────
    crow = conn.execute(
        "SELECT id, label, generated_at, content FROM generated_assets "
        "WHERE job_id = ? AND asset_type = 'cover_letter' ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if crow:
        pkg.cover_letter = AssetRef(
            asset_id        = crow["id"],
            asset_type      = "cover_letter",
            label           = crow["label"],
            generated_at    = crow["generated_at"],
            content_preview = (crow["content"] or "")[:200],
        )

    # ── Project recommendations ───────────────────────────────────────────────
    recrows = conn.execute(
        "SELECT id, recommendation_type, project_title, target_gap_or_signal, "
        "business_problem FROM project_recommendations "
        "WHERE job_id = ? ORDER BY id DESC LIMIT 10",
        (job_id,),
    ).fetchall()
    pkg.recommendations = [
        RecommendationRef(
            rec_id               = r["id"],
            recommendation_type  = r["recommendation_type"],
            title                = r["project_title"],
            target_gap_or_signal = r["target_gap_or_signal"],
            business_problem     = r["business_problem"],
        )
        for r in recrows
    ]

    # ── Latest application decision ───────────────────────────────────────────
    approw = conn.execute(
        "SELECT id, status, notes, follow_up_date, platform, last_updated "
        "FROM applications WHERE job_id = ? ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if approw:
        pkg.application = ApplicationRecord(
            application_id = approw["id"],
            status         = approw["status"],
            notes          = approw["notes"],
            follow_up_date = approw["follow_up_date"],
            platform       = approw["platform"],
            last_updated   = approw["last_updated"],
        )

    return pkg


def load_latest_decision(
    job_id: int,
    conn:   sqlite3.Connection,
) -> dict | None:
    """Return the most recent applications row for *job_id* as a plain dict, or None."""
    row = conn.execute(
        "SELECT * FROM applications WHERE job_id = ? ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    return dict(row) if row else None


# ── Internal helpers ──────────────────────────────────────────────────────────

def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
