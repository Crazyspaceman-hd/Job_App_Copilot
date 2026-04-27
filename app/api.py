"""
app/api.py — FastAPI JSON API for the Job Application Copilot UI.

Run with:
    uvicorn app.api:app --port 8080 --reload

All routes are prefixed /api. The React dev server proxies /api -> :8080.
Business logic lives entirely in app/services/*; this layer only wires HTTP.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.db import get_conn, init_db
from app.services.profile_loader import DEFAULT_PROFILE, completeness, load_profile
from app.services.tracker import (
    VALID_STATUSES,
    load_application_package,
    save_application_decision,
)
from app.services.base_asset_ingest import ingest_resume, ingest_cover_letter
from app.services.evidence_bank import (
    create_item, delete_item, get_item, list_items, update_item,
)
from app.services.candidate_assessment import (
    create_assessment, delete_assessment, get_assessment,
    get_preferred, list_assessments, set_preferred, update_assessment,
)
from app.services.candidate_assessment_prompts import (
    get_prompt, list_prompts, CURRENT_VERSION, PROMPT_TYPES,
)

# Initialise DB on startup (idempotent)
init_db()

app = FastAPI(title="Job Application Copilot API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic response models ──────────────────────────────────────────────────

class JobSummary(BaseModel):
    id: int
    title: Optional[str]
    company: Optional[str]
    location: Optional[str]
    remote_policy: Optional[str]
    status: str
    ingested_at: str
    source_url: Optional[str]
    # Latest fit assessment (may be absent)
    verdict: Optional[str] = None
    overall_score: Optional[float] = None
    confidence: Optional[str] = None


class AssetRefOut(BaseModel):
    asset_id: int
    asset_type: str
    label: Optional[str]
    generated_at: str
    content_preview: str
    content: Optional[str] = None      # full content for preview panel


class RecOut(BaseModel):
    rec_id: int
    recommendation_type: Optional[str]
    title: str
    target_gap_or_signal: Optional[str]
    business_problem: Optional[str]


class ApplicationRecordOut(BaseModel):
    application_id: Optional[int]
    status: Optional[str]
    notes: Optional[str]
    follow_up_date: Optional[str]
    platform: Optional[str]
    last_updated: Optional[str]


class PackageOut(BaseModel):
    job_id: int
    job_title: Optional[str]
    job_company: Optional[str]
    job_remote_policy: Optional[str]
    job_status: str
    job_location: Optional[str] = None
    job_source_url: Optional[str] = None
    # Assessment
    assessment_id: Optional[int]
    assessed_at: Optional[str]
    verdict: Optional[str]
    overall_score: Optional[float]
    confidence: Optional[str]
    direct_evidence: list[str]
    adjacent_evidence: list[str]
    unsupported_gaps: list[str]
    # Assets
    resume: Optional[AssetRefOut]
    cover_letter: Optional[AssetRefOut]
    # Recommendations
    recommendations: list[RecOut]
    # Application
    application: ApplicationRecordOut


class DecisionIn(BaseModel):
    status: str
    notes: Optional[str] = None
    follow_up_date: Optional[str] = None
    platform: Optional[str] = None


class GenerateResumeIn(BaseModel):
    label: str = "targeted"
    resume_id: Optional[int] = None


class GenerateCLIn(BaseModel):
    label: str = "targeted"
    cl_id: Optional[int] = None
    resume_id: Optional[int] = None


class GenerateRecsIn(BaseModel):
    label: str = "targeted"


class CreateJobIn(BaseModel):
    raw_text: str
    company:       Optional[str] = None
    title:         Optional[str] = None
    location:      Optional[str] = None
    source_url:    Optional[str] = None
    remote_policy: Optional[str] = None
    # accepted but not stored in separate columns (no schema column yet)
    platform:      Optional[str] = None
    salary_text:   Optional[str] = None
    posted_date:   Optional[str] = None


class IngestTextIn(BaseModel):
    text:  str
    label: str = "default"


# ── Helper: load full content for assets ─────────────────────────────────────

def _asset_with_content(conn: sqlite3.Connection, job_id: int, asset_type: str
                        ) -> Optional[AssetRefOut]:
    row = conn.execute(
        "SELECT id, label, generated_at, content FROM generated_assets "
        "WHERE job_id = ? AND asset_type = ? ORDER BY id DESC LIMIT 1",
        (job_id, asset_type),
    ).fetchone()
    if not row:
        return None
    return AssetRefOut(
        asset_id        = row["id"],
        asset_type      = asset_type,
        label           = row["label"],
        generated_at    = row["generated_at"],
        content_preview = (row["content"] or "")[:200],
        content         = row["content"] or "",
    )


def _job_location(conn: sqlite3.Connection, job_id: int) -> Optional[str]:
    row = conn.execute("SELECT location FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return row["location"] if row else None


def _job_source_url(conn: sqlite3.Connection, job_id: int) -> Optional[str]:
    row = conn.execute("SELECT source_url FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return row["source_url"] if row else None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/api/jobs", response_model=list[JobSummary])
def list_jobs():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT j.id, j.title, j.company, j.location, j.remote_policy,
                   j.status, j.ingested_at, j.source_url,
                   fa.verdict, fa.overall_score, fa.confidence
            FROM jobs j
            LEFT JOIN (
                SELECT job_id, verdict, overall_score, confidence,
                       MAX(id) as mid
                FROM fit_assessments GROUP BY job_id
            ) fa ON fa.job_id = j.id
            ORDER BY j.id DESC
            LIMIT 200
            """,
        ).fetchall()
    return [JobSummary(**dict(r)) for r in rows]


@app.get("/api/jobs/{job_id}/package", response_model=PackageOut)
def get_package(job_id: int):
    with get_conn() as conn:
        try:
            pkg = load_application_package(job_id, conn)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

        # Fetch full asset content and extra job fields
        resume_out = _asset_with_content(conn, job_id, "resume")
        cl_out     = _asset_with_content(conn, job_id, "cover_letter")
        location   = _job_location(conn, job_id)
        source_url = _job_source_url(conn, job_id)

    recs = [
        RecOut(
            rec_id               = r.rec_id,
            recommendation_type  = r.recommendation_type,
            title                = r.title,
            target_gap_or_signal = r.target_gap_or_signal,
            business_problem     = r.business_problem,
        )
        for r in pkg.recommendations
    ]

    app_rec = pkg.application
    app_out = ApplicationRecordOut(
        application_id = app_rec.application_id,
        status         = app_rec.status,
        notes          = app_rec.notes,
        follow_up_date = app_rec.follow_up_date,
        platform       = app_rec.platform,
        last_updated   = app_rec.last_updated,
    )

    return PackageOut(
        job_id            = pkg.job_id,
        job_title         = pkg.job_title,
        job_company       = pkg.job_company,
        job_remote_policy = pkg.job_remote_policy,
        job_status        = pkg.job_status,
        job_location      = location,
        job_source_url    = source_url,
        assessment_id     = pkg.assessment_id,
        assessed_at       = pkg.assessed_at,
        verdict           = pkg.verdict,
        overall_score     = pkg.overall_score,
        confidence        = pkg.confidence,
        direct_evidence   = pkg.direct_evidence,
        adjacent_evidence = pkg.adjacent_evidence,
        unsupported_gaps  = pkg.unsupported_gaps,
        resume            = resume_out,
        cover_letter      = cl_out,
        recommendations   = recs,
        application       = app_out,
    )


@app.post("/api/jobs/{job_id}/decision")
def set_decision(job_id: int, body: DecisionIn):
    if body.status not in VALID_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of {sorted(VALID_STATUSES)}",
        )
    with get_conn() as conn:
        try:
            app_id = save_application_decision(
                job_id         = job_id,
                conn           = conn,
                status         = body.status,
                notes          = body.notes,
                follow_up_date = body.follow_up_date,
                platform       = body.platform,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
    return {"ok": True, "application_id": app_id}


@app.post("/api/jobs/{job_id}/generate-resume")
def generate_resume(job_id: int, body: GenerateResumeIn):
    from app.services.resume_tailor import generate_targeted_resume
    from app.services.base_asset_ingest import load_latest_base_resume
    from app.services.profile_loader import load_profile, completeness
    from app.services.extractor import load_latest_extraction

    with get_conn() as conn:
        row = conn.execute("SELECT id FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        extracted  = load_latest_extraction(conn, job_id)
        base_resume = load_latest_base_resume(conn, resume_id=body.resume_id)

    if not base_resume:
        raise HTTPException(status_code=400, detail="No base resume found. Run ingest-resume first.")

    try:
        profile = load_profile(None)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    with get_conn() as conn:
        result = generate_targeted_resume(
            job_id      = job_id,
            conn        = conn,
            profile     = profile,
            base_resume = base_resume,
            extracted   = extracted,
            label       = body.label,
        )
    return {"ok": True, "asset_id": result.asset_id}


@app.post("/api/jobs/{job_id}/generate-cover-letter")
def generate_cover_letter(job_id: int, body: GenerateCLIn):
    from app.services.cover_letter import generate_targeted_cover_letter
    from app.services.base_asset_ingest import load_latest_cover_letter, load_latest_base_resume
    from app.services.profile_loader import load_profile
    from app.services.extractor import load_latest_extraction

    with get_conn() as conn:
        row = conn.execute("SELECT id FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        extracted   = load_latest_extraction(conn, job_id)
        base_cl     = load_latest_cover_letter(conn, cl_id=body.cl_id)
        base_resume = load_latest_base_resume(conn, resume_id=body.resume_id)

    if not base_cl:
        raise HTTPException(status_code=400, detail="No base cover letter found. Run ingest-cover-letter first.")

    try:
        profile = load_profile(None)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    with get_conn() as conn:
        result = generate_targeted_cover_letter(
            job_id      = job_id,
            conn        = conn,
            profile     = profile,
            base_cl     = base_cl,
            extracted   = extracted,
            base_resume = base_resume,
            label       = body.label,
        )
    return {"ok": True, "asset_id": result.asset_id}


@app.post("/api/jobs/{job_id}/recommend-project")
def recommend_project(job_id: int, body: GenerateRecsIn):
    from app.services.project_recommender import recommend_project as _recommend
    from app.services.project_loader import load_projects
    from app.services.profile_loader import load_profile
    from app.services.extractor import load_latest_extraction

    with get_conn() as conn:
        row = conn.execute("SELECT id FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        extracted = load_latest_extraction(conn, job_id)

    try:
        profile  = load_profile(None)
        projects = load_projects(None)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    with get_conn() as conn:
        result = _recommend(
            job_id    = job_id,
            conn      = conn,
            profile   = profile,
            extracted = extracted,
            projects  = projects,
            label     = body.label,
        )

    new_id  = result.new_project.asset_id
    repo_id = result.reposition_existing.asset_id if result.reposition_existing else None
    return {"ok": True, "new_project_id": new_id, "reposition_id": repo_id}


# ── Job intake ────────────────────────────────────────────────────────────────

@app.post("/api/jobs", status_code=201)
def create_job(body: CreateJobIn):
    """
    Create a new job from pasted text plus optional metadata fields.
    Automatically runs extraction and fit assessment (if an active profile exists).
    Returns enough data for the UI to navigate to the new package immediately.
    """
    from app.services.intake import insert_job, JobRecord, _detect_remote_policy
    from app.services.extractor import extract, persist_extraction
    from app.services.scorer import assess, persist_assessment
    from app.services.project_loader import load_projects, extract_project_skills

    raw = body.raw_text.strip()
    if not raw:
        raise HTTPException(status_code=422, detail="raw_text must not be empty")

    # Detect remote policy from text if not explicitly given
    remote = body.remote_policy or _detect_remote_policy(raw)

    record = JobRecord(
        raw_text      = raw,
        source_url    = body.source_url or None,
        company       = body.company or None,
        title         = body.title or None,
        location      = body.location or None,
        remote_policy = remote,
    )

    with get_conn() as conn:
        job_id = insert_job(conn, record)

    # Extract requirements (always)
    extracted = extract(job_id, raw)
    with get_conn() as conn:
        persist_extraction(conn, extracted)

    # Attempt fit assessment (gracefully skip if profile is missing or template)
    assessed = False
    verdict  = None
    try:
        profile      = load_profile(None)
        projects     = load_projects(None)
        proj_skills  = extract_project_skills(projects)
        prof_complete = completeness(profile)

        result = assess(
            job_raw_text      = raw,
            job_remote_policy = remote,
            profile           = profile,
            project_skills    = proj_skills,
            profile_complete  = prof_complete,
            extracted         = extracted,
        )

        with get_conn() as conn:
            cur = conn.execute(
                "INSERT INTO candidate_profiles (version, profile_json) VALUES (?, ?)",
                (profile.get("version", "1.0"), json.dumps(profile)),
            )
            profile_id = cur.lastrowid
            persist_assessment(conn, job_id, profile_id, result)

        assessed = True
        verdict  = result.verdict
    except (FileNotFoundError, ValueError):
        pass  # profile missing or template-only — skip assessment silently
    except Exception:
        pass  # other errors in scoring never block job creation

    return {
        "ok":       True,
        "job_id":   job_id,
        "extracted": True,
        "assessed": assessed,
        "verdict":  verdict,
    }


# ── Profile management ────────────────────────────────────────────────────────

def _safe_load_profile_dict() -> dict[str, Any]:
    """Return the on-disk profile dict, or an empty-but-valid template."""
    try:
        return json.loads(DEFAULT_PROFILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {
            "version": "1.1",
            "personal": {"name": "", "location": "", "linkedin": "", "github": ""},
            "job_targets": {
                "titles": [],
                "seniority_self_assessed": "",
                "desired_remote_policy": "",
                "willing_to_relocate": False,
                "work_authorization": "",
            },
            "skills": {
                "languages": [], "frameworks": [], "databases": [],
                "cloud": [], "tools": [], "practices": [],
            },
            "domains": [],
            "experience": [],
            "education": [],
            "certifications": [],
            "hard_constraints": {"no_travel": False, "no_equity_only": False, "min_salary_usd": None},
        }


@app.get("/api/profile")
def get_profile():
    data = _safe_load_profile_dict()
    comp = completeness(data) if _profile_is_filled(data) else 0.0
    return {**data, "_completeness": round(comp, 2)}


@app.post("/api/profile")
async def save_profile(request: Request):
    try:
        body: dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Request body must be valid JSON")

    # Strip internal metadata key before saving
    body.pop("_completeness", None)

    # Basic validation: required top-level keys must exist
    required = {"version", "personal", "job_targets", "skills"}
    missing  = required - body.keys()
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing required keys: {missing}")

    DEFAULT_PROFILE.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_PROFILE.write_text(
        json.dumps(body, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    comp = completeness(body)
    return {"ok": True, "completeness": round(comp, 2)}


def _profile_is_filled(data: dict) -> bool:
    """True when at least one personal field is non-empty and non-TODO."""
    personal = data.get("personal", {})
    for v in personal.values():
        if isinstance(v, str) and v.strip() and not v.lower().startswith("todo"):
            return True
    return False


# ── Base-asset ingestion ──────────────────────────────────────────────────────

@app.post("/api/ingest/resume")
def api_ingest_resume(body: IngestTextIn):
    if not body.text.strip():
        raise HTTPException(status_code=422, detail="text must not be empty")

    with get_conn() as conn:
        result = ingest_resume(body.text, conn, label=body.label)

    return {
        "ok":            True,
        "resume_id":     result.resume_id,
        "label":         result.label,
        "bullet_count":  len(result.bullet_bank),
        "section_count": len(result.sections),
    }


@app.post("/api/ingest/cover-letter")
def api_ingest_cover_letter(body: IngestTextIn):
    if not body.text.strip():
        raise HTTPException(status_code=422, detail="text must not be empty")

    with get_conn() as conn:
        result = ingest_cover_letter(body.text, conn, label=body.label)

    return {
        "ok":             True,
        "cl_id":          result.cl_id,
        "label":          result.label,
        "fragment_count": len(result.fragments),
    }


@app.get("/api/ingest/status")
def get_ingest_status():
    """Return whether a base resume and cover letter have been ingested."""
    with get_conn() as conn:
        resume_row = conn.execute(
            "SELECT id, label, bullet_count FROM base_resumes ORDER BY id DESC LIMIT 1"
        ).fetchone()
        cl_row = conn.execute(
            "SELECT id, label, fragment_count FROM base_cover_letters ORDER BY id DESC LIMIT 1"
        ).fetchone()
        resume_count = conn.execute("SELECT COUNT(*) FROM base_resumes").fetchone()[0]
        cl_count     = conn.execute("SELECT COUNT(*) FROM base_cover_letters").fetchone()[0]

    return {
        "has_resume":      resume_row is not None,
        "resume_id":       resume_row["id"]          if resume_row else None,
        "resume_label":    resume_row["label"]        if resume_row else None,
        "resume_bullets":  resume_row["bullet_count"] if resume_row else 0,
        "resume_count":    resume_count,
        "has_cover_letter": cl_row is not None,
        "cl_id":           cl_row["id"]              if cl_row else None,
        "cl_label":        cl_row["label"]            if cl_row else None,
        "cl_fragments":    cl_row["fragment_count"]   if cl_row else 0,
        "cl_count":        cl_count,
    }


# ── Evidence Bank ─────────────────────────────────────────────────────────────

class EvidenceItemOut(BaseModel):
    item_id:               int
    created_at:            str
    updated_at:            str
    title:                 str
    raw_text:              str
    source_type:           str
    skill_tags:            list[str]
    domain_tags:           list[str]
    business_problem_tags: list[str]
    evidence_strength:     str
    allowed_uses:          list[str]
    confidence:            Optional[str] = None
    notes:                 Optional[str] = None
    profile_id:            Optional[int] = None


class EvidenceItemIn(BaseModel):
    title:                 str
    raw_text:              str
    source_type:           str       = "other"
    skill_tags:            list[str] = []
    domain_tags:           list[str] = []
    business_problem_tags: list[str] = []
    evidence_strength:     str       = "adjacent"
    allowed_uses:          list[str] = []
    confidence:            Optional[str] = None
    notes:                 Optional[str] = None


@app.get("/api/evidence", response_model=list[EvidenceItemOut])
def list_evidence(
    source_type:       Optional[str] = None,
    evidence_strength: Optional[str] = None,
):
    with get_conn() as conn:
        items = list_items(conn, source_type=source_type,
                           evidence_strength=evidence_strength)
    return [EvidenceItemOut(**item.to_dict()) for item in items]


@app.post("/api/evidence", status_code=201, response_model=EvidenceItemOut)
def create_evidence(body: EvidenceItemIn):
    try:
        with get_conn() as conn:
            item = create_item(
                conn                  = conn,
                title                 = body.title,
                raw_text              = body.raw_text,
                source_type           = body.source_type,
                skill_tags            = body.skill_tags,
                domain_tags           = body.domain_tags,
                business_problem_tags = body.business_problem_tags,
                evidence_strength     = body.evidence_strength,
                allowed_uses          = body.allowed_uses,
                confidence            = body.confidence,
                notes                 = body.notes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return EvidenceItemOut(**item.to_dict())


@app.put("/api/evidence/{item_id}", response_model=EvidenceItemOut)
def update_evidence(item_id: int, body: EvidenceItemIn):
    try:
        with get_conn() as conn:
            item = update_item(
                conn                  = conn,
                item_id               = item_id,
                title                 = body.title,
                raw_text              = body.raw_text,
                source_type           = body.source_type,
                skill_tags            = body.skill_tags,
                domain_tags           = body.domain_tags,
                business_problem_tags = body.business_problem_tags,
                evidence_strength     = body.evidence_strength,
                allowed_uses          = body.allowed_uses,
                confidence            = body.confidence,
                notes                 = body.notes,
            )
    except ValueError as exc:
        status = 404 if "not found" in str(exc) else 422
        raise HTTPException(status_code=status, detail=str(exc))
    return EvidenceItemOut(**item.to_dict())


@app.delete("/api/evidence/{item_id}")
def delete_evidence(item_id: int):
    with get_conn() as conn:
        deleted = delete_item(conn, item_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Evidence item {item_id} not found")
    return {"ok": True, "item_id": item_id}


# ── Candidate Assessments ──────────────────────────────────────────────────────

class AssessmentOut(BaseModel):
    id:                   int
    created_at:           str
    updated_at:           str
    source_type:          str
    source_label:         Optional[str]       = None
    assessment_kind:      str
    raw_text:             str
    strengths:            list[str]
    growth_areas:         list[str]
    demonstrated_skills:  list[str]
    demonstrated_domains: list[str]
    work_style:           Optional[str]       = None
    role_fit:             Optional[str]       = None
    confidence:           Optional[str]       = None
    allowed_uses:         list[str]
    is_preferred:         bool
    profile_id:           Optional[int]       = None
    prompt_type:          Optional[str]       = None
    prompt_version:       Optional[str]       = None
    source_model:         Optional[str]       = None


class AssessmentIn(BaseModel):
    source_type:          str        = "manual"
    source_label:         Optional[str]       = None
    assessment_kind:      str        = "working_assessment"
    raw_text:             str        = ""
    strengths:            list[str]  = []
    growth_areas:         list[str]  = []
    demonstrated_skills:  list[str]  = []
    demonstrated_domains: list[str]  = []
    work_style:           Optional[str]       = None
    role_fit:             Optional[str]       = None
    confidence:           Optional[str]       = None
    allowed_uses:         list[str]  = []
    profile_id:           Optional[int]       = None
    prompt_type:          Optional[str]       = None
    prompt_version:       Optional[str]       = None
    source_model:         Optional[str]       = None


class PromptOut(BaseModel):
    prompt_type:  str
    version:      str
    title:        str
    description:  str
    full_text:    str


def _assessment_out(a) -> AssessmentOut:
    return AssessmentOut(
        id=a.id,
        created_at=a.created_at,
        updated_at=a.updated_at,
        source_type=a.source_type,
        source_label=a.source_label,
        assessment_kind=a.assessment_kind,
        raw_text=a.raw_text,
        strengths=a.strengths,
        growth_areas=a.growth_areas,
        demonstrated_skills=a.demonstrated_skills,
        demonstrated_domains=a.demonstrated_domains,
        work_style=a.work_style,
        role_fit=a.role_fit,
        confidence=a.confidence,
        allowed_uses=a.allowed_uses,
        is_preferred=a.is_preferred,
        profile_id=a.profile_id,
        prompt_type=a.prompt_type,
        prompt_version=a.prompt_version,
        source_model=a.source_model,
    )


# ── Assessment Prompts ──────────────────────────────────────────────────────────

@app.get("/api/assessment-prompts", response_model=list[PromptOut])
def list_assessment_prompts(version: Optional[str] = None):
    prompts = list_prompts(version or CURRENT_VERSION)
    return [PromptOut(
        prompt_type=p.prompt_type,
        version=p.version,
        title=p.title,
        description=p.description,
        full_text=p.full_text,
    ) for p in prompts]


@app.get("/api/assessment-prompts/{prompt_type}", response_model=PromptOut)
def get_assessment_prompt(prompt_type: str, version: Optional[str] = None):
    try:
        p = get_prompt(prompt_type, version or CURRENT_VERSION)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return PromptOut(
        prompt_type=p.prompt_type,
        version=p.version,
        title=p.title,
        description=p.description,
        full_text=p.full_text,
    )


@app.get("/api/assessments/preferred", response_model=Optional[AssessmentOut])
def get_preferred_assessment():
    with get_conn() as conn:
        a = get_preferred(conn)
    return _assessment_out(a) if a else None


@app.get("/api/assessments", response_model=list[AssessmentOut])
def list_assessments_route(
    source_type:     Optional[str] = None,
    assessment_kind: Optional[str] = None,
):
    with get_conn() as conn:
        items = list_assessments(conn, source_type=source_type,
                                 assessment_kind=assessment_kind)
    return [_assessment_out(a) for a in items]


@app.post("/api/assessments", status_code=201, response_model=AssessmentOut)
def create_assessment_route(body: AssessmentIn):
    try:
        with get_conn() as conn:
            a = create_assessment(
                conn,
                source_type=body.source_type,
                source_label=body.source_label,
                assessment_kind=body.assessment_kind,
                raw_text=body.raw_text,
                strengths=body.strengths,
                growth_areas=body.growth_areas,
                demonstrated_skills=body.demonstrated_skills,
                demonstrated_domains=body.demonstrated_domains,
                work_style=body.work_style,
                role_fit=body.role_fit,
                confidence=body.confidence,
                allowed_uses=body.allowed_uses,
                profile_id=body.profile_id,
                prompt_type=body.prompt_type,
                prompt_version=body.prompt_version,
                source_model=body.source_model,
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return _assessment_out(a)


@app.put("/api/assessments/{assessment_id}", response_model=AssessmentOut)
def update_assessment_route(assessment_id: int, body: AssessmentIn):
    try:
        with get_conn() as conn:
            a = update_assessment(
                conn,
                assessment_id,
                source_type=body.source_type,
                source_label=body.source_label,
                assessment_kind=body.assessment_kind,
                raw_text=body.raw_text,
                strengths=body.strengths,
                growth_areas=body.growth_areas,
                demonstrated_skills=body.demonstrated_skills,
                demonstrated_domains=body.demonstrated_domains,
                work_style=body.work_style,
                role_fit=body.role_fit,
                confidence=body.confidence,
                allowed_uses=body.allowed_uses,
                profile_id=body.profile_id,
                prompt_type=body.prompt_type,
                prompt_version=body.prompt_version,
                source_model=body.source_model,
            )
    except ValueError as exc:
        status = 404 if "not found" in str(exc) else 422
        raise HTTPException(status_code=status, detail=str(exc))
    return _assessment_out(a)


@app.delete("/api/assessments/{assessment_id}")
def delete_assessment_route(assessment_id: int):
    try:
        with get_conn() as conn:
            delete_assessment(conn, assessment_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Assessment {assessment_id} not found")
    return {"ok": True, "id": assessment_id}


@app.post("/api/assessments/{assessment_id}/set-preferred", response_model=AssessmentOut)
def set_preferred_route(assessment_id: int):
    try:
        with get_conn() as conn:
            set_preferred(conn, assessment_id)
            a = get_assessment(conn, assessment_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Assessment {assessment_id} not found")
    return _assessment_out(a)
