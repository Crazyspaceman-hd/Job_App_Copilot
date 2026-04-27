"""app/services/candidate_assessment.py — CRUD for candidate_assessments table."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

SOURCE_TYPES = frozenset({"chatgpt", "claude", "gemini", "manual", "other"})
ASSESSMENT_KINDS = frozenset({
    "working_assessment",
    "skill_observation",
    "project_delivery_assessment",
    "growth_assessment",
})
ALLOWED_USE_VALUES = frozenset({"resume", "cover_letter", "interview", "internal"})


def normalize_tags(tags: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for t in tags:
        t = t.strip().lower()
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


@dataclass
class CandidateAssessment:
    id: int
    created_at: str
    updated_at: str
    source_type: str
    source_label: Optional[str]
    assessment_kind: str
    raw_text: str
    strengths: list[str]
    growth_areas: list[str]
    demonstrated_skills: list[str]
    demonstrated_domains: list[str]
    work_style: Optional[str]
    role_fit: Optional[str]
    confidence: Optional[str]
    allowed_uses: list[str]
    is_preferred: bool
    profile_id: Optional[int]
    # v2.1 prompt metadata
    prompt_type: Optional[str] = None
    prompt_version: Optional[str] = None
    source_model: Optional[str] = None


def _validate(
    source_type: str,
    assessment_kind: str,
    allowed_uses: list[str],
    prompt_type: Optional[str] = None,
) -> None:
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"source_type must be one of {sorted(SOURCE_TYPES)}")
    if assessment_kind not in ASSESSMENT_KINDS:
        raise ValueError(f"assessment_kind must be one of {sorted(ASSESSMENT_KINDS)}")
    bad = [u for u in allowed_uses if u not in ALLOWED_USE_VALUES]
    if bad:
        raise ValueError(f"Invalid allowed_uses values: {bad}")
    if prompt_type is not None:
        from app.services.candidate_assessment_prompts import PROMPT_TYPES
        if prompt_type not in PROMPT_TYPES:
            raise ValueError(f"prompt_type must be one of {sorted(PROMPT_TYPES)}")


def _row_to_assessment(row: sqlite3.Row) -> CandidateAssessment:
    keys = row.keys()
    return CandidateAssessment(
        id=row["id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        source_type=row["source_type"],
        source_label=row["source_label"],
        assessment_kind=row["assessment_kind"],
        raw_text=row["raw_text"],
        strengths=json.loads(row["strengths"] or "[]"),
        growth_areas=json.loads(row["growth_areas"] or "[]"),
        demonstrated_skills=json.loads(row["demonstrated_skills"] or "[]"),
        demonstrated_domains=json.loads(row["demonstrated_domains"] or "[]"),
        work_style=row["work_style"],
        role_fit=row["role_fit"],
        confidence=row["confidence"],
        allowed_uses=json.loads(row["allowed_uses"] or "[]"),
        is_preferred=bool(row["is_preferred"]),
        profile_id=row["profile_id"],
        prompt_type=row["prompt_type"] if "prompt_type" in keys else None,
        prompt_version=row["prompt_version"] if "prompt_version" in keys else None,
        source_model=row["source_model"] if "source_model" in keys else None,
    )


def create_assessment(
    conn: sqlite3.Connection,
    *,
    source_type: str = "manual",
    source_label: Optional[str] = None,
    assessment_kind: str = "working_assessment",
    raw_text: str = "",
    strengths: list[str] | None = None,
    growth_areas: list[str] | None = None,
    demonstrated_skills: list[str] | None = None,
    demonstrated_domains: list[str] | None = None,
    work_style: Optional[str] = None,
    role_fit: Optional[str] = None,
    confidence: Optional[str] = None,
    allowed_uses: list[str] | None = None,
    profile_id: Optional[int] = None,
    prompt_type: Optional[str] = None,
    prompt_version: Optional[str] = None,
    source_model: Optional[str] = None,
) -> CandidateAssessment:
    strengths = normalize_tags(strengths or [])
    growth_areas = normalize_tags(growth_areas or [])
    demonstrated_skills = normalize_tags(demonstrated_skills or [])
    demonstrated_domains = normalize_tags(demonstrated_domains or [])
    allowed_uses = normalize_tags(allowed_uses or [])

    _validate(source_type, assessment_kind, allowed_uses, prompt_type)

    cur = conn.execute(
        """
        INSERT INTO candidate_assessments
            (source_type, source_label, assessment_kind, raw_text,
             strengths, growth_areas, demonstrated_skills, demonstrated_domains,
             work_style, role_fit, confidence, allowed_uses, profile_id,
             prompt_type, prompt_version, source_model)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source_type, source_label, assessment_kind, raw_text,
            json.dumps(strengths), json.dumps(growth_areas),
            json.dumps(demonstrated_skills), json.dumps(demonstrated_domains),
            work_style, role_fit, confidence, json.dumps(allowed_uses), profile_id,
            prompt_type, prompt_version, source_model,
        ),
    )
    conn.commit()
    return get_assessment(conn, cur.lastrowid)


def get_assessment(conn: sqlite3.Connection, assessment_id: int) -> CandidateAssessment:
    row = conn.execute(
        "SELECT * FROM candidate_assessments WHERE id = ?", (assessment_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Assessment {assessment_id} not found")
    return _row_to_assessment(row)


def list_assessments(
    conn: sqlite3.Connection,
    *,
    source_type: Optional[str] = None,
    assessment_kind: Optional[str] = None,
    profile_id: Optional[int] = None,
) -> list[CandidateAssessment]:
    query = "SELECT * FROM candidate_assessments WHERE 1=1"
    params: list = []
    if source_type is not None:
        query += " AND source_type = ?"
        params.append(source_type)
    if assessment_kind is not None:
        query += " AND assessment_kind = ?"
        params.append(assessment_kind)
    if profile_id is not None:
        query += " AND profile_id = ?"
        params.append(profile_id)
    query += " ORDER BY is_preferred DESC, created_at DESC"
    rows = conn.execute(query, params).fetchall()
    return [_row_to_assessment(r) for r in rows]


def update_assessment(
    conn: sqlite3.Connection,
    assessment_id: int,
    *,
    source_type: Optional[str] = None,
    source_label: Optional[str] = None,
    assessment_kind: Optional[str] = None,
    raw_text: Optional[str] = None,
    strengths: list[str] | None = None,
    growth_areas: list[str] | None = None,
    demonstrated_skills: list[str] | None = None,
    demonstrated_domains: list[str] | None = None,
    work_style: Optional[str] = None,
    role_fit: Optional[str] = None,
    confidence: Optional[str] = None,
    allowed_uses: list[str] | None = None,
    profile_id: Optional[int] = None,
    prompt_type: Optional[str] = None,
    prompt_version: Optional[str] = None,
    source_model: Optional[str] = None,
) -> CandidateAssessment:
    existing = get_assessment(conn, assessment_id)

    source_type = source_type if source_type is not None else existing.source_type
    source_label = source_label if source_label is not None else existing.source_label
    assessment_kind = assessment_kind if assessment_kind is not None else existing.assessment_kind
    raw_text = raw_text if raw_text is not None else existing.raw_text
    strengths = normalize_tags(strengths if strengths is not None else existing.strengths)
    growth_areas = normalize_tags(growth_areas if growth_areas is not None else existing.growth_areas)
    demonstrated_skills = normalize_tags(demonstrated_skills if demonstrated_skills is not None else existing.demonstrated_skills)
    demonstrated_domains = normalize_tags(demonstrated_domains if demonstrated_domains is not None else existing.demonstrated_domains)
    work_style = work_style if work_style is not None else existing.work_style
    role_fit = role_fit if role_fit is not None else existing.role_fit
    confidence = confidence if confidence is not None else existing.confidence
    allowed_uses = normalize_tags(allowed_uses if allowed_uses is not None else existing.allowed_uses)
    profile_id = profile_id if profile_id is not None else existing.profile_id
    prompt_type = prompt_type if prompt_type is not None else existing.prompt_type
    prompt_version = prompt_version if prompt_version is not None else existing.prompt_version
    source_model = source_model if source_model is not None else existing.source_model

    _validate(source_type, assessment_kind, allowed_uses, prompt_type)

    conn.execute(
        """
        UPDATE candidate_assessments
        SET source_type=?, source_label=?, assessment_kind=?, raw_text=?,
            strengths=?, growth_areas=?, demonstrated_skills=?, demonstrated_domains=?,
            work_style=?, role_fit=?, confidence=?, allowed_uses=?, profile_id=?,
            prompt_type=?, prompt_version=?, source_model=?,
            updated_at=datetime('now')
        WHERE id=?
        """,
        (
            source_type, source_label, assessment_kind, raw_text,
            json.dumps(strengths), json.dumps(growth_areas),
            json.dumps(demonstrated_skills), json.dumps(demonstrated_domains),
            work_style, role_fit, confidence, json.dumps(allowed_uses), profile_id,
            prompt_type, prompt_version, source_model,
            assessment_id,
        ),
    )
    conn.commit()
    return get_assessment(conn, assessment_id)


def delete_assessment(conn: sqlite3.Connection, assessment_id: int) -> None:
    existing = get_assessment(conn, assessment_id)  # raises if not found
    conn.execute("DELETE FROM candidate_assessments WHERE id = ?", (assessment_id,))
    conn.commit()


def set_preferred(conn: sqlite3.Connection, assessment_id: int) -> None:
    """Mark one assessment as preferred; clear is_preferred on all others."""
    get_assessment(conn, assessment_id)  # raises ValueError if not found
    conn.execute("UPDATE candidate_assessments SET is_preferred = 0")
    conn.execute(
        "UPDATE candidate_assessments SET is_preferred = 1 WHERE id = ?",
        (assessment_id,),
    )
    conn.commit()


def get_preferred(conn: sqlite3.Connection) -> Optional[CandidateAssessment]:
    row = conn.execute(
        "SELECT * FROM candidate_assessments WHERE is_preferred = 1 LIMIT 1"
    ).fetchone()
    return _row_to_assessment(row) if row else None


def get_assessments_for_use(
    conn: sqlite3.Connection,
    allowed_use: str,
) -> list[CandidateAssessment]:
    """Return assessments whose allowed_uses includes the given use value."""
    rows = conn.execute(
        "SELECT * FROM candidate_assessments ORDER BY is_preferred DESC, created_at DESC"
    ).fetchall()
    result = []
    for row in rows:
        uses = json.loads(row["allowed_uses"] or "[]")
        if allowed_use in uses:
            result.append(_row_to_assessment(row))
    return result
