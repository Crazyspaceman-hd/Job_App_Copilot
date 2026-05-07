"""
app/services/cover_letter.py — Deterministic targeted cover letter generator.

Design contract:
  - Grounded: proof points come verbatim from ingested base CL fragments (or base
    resume bullets when a required skill is uncovered by the CL).
  - Honest: adjacent evidence influences ordering only; unsupported gaps are never
    presented as owned qualifications.
  - Tone-preserving: only the opening sentence and (optional) adjacency paragraph
    are template-generated; everything else is verbatim from source material.
  - Deterministic: same inputs always produce the same output.
  - No LLM: pure ranking, templating, and vocabulary lookup.

Public API:
  generate_targeted_cover_letter(job_id, conn, profile, base_cl, ...)
      -> TargetedCLResult   (parses + ranks + persists)

Ranking algorithm:
  Each CL fragment (proof_point kind) is scored the same way resume bullets are:
    score = Σ  category_weight × evidence_weight
  over every vocabulary term in the fragment text.

  Term category weights: required=1.5, preferred=1.0, domain=0.5, ats=0.2
  Profile evidence multipliers: direct=1.0, adjacent=0.5, familiar=0.2, absent=0.3

  Top max_proof_points fragments are selected, then:
    - If a required skill is not covered by any CL fragment, the highest-scoring
      resume bullet that covers it is appended as a supplemental proof point.
    - An adjacency paragraph is appended if there are adjacent-only required terms
      not already named in any proof point.

Persistence:
  Writes one row to generated_assets (asset_type='cover_letter'):
    content       — full markdown string
    metadata_json — CLProvenance serialised to JSON
    base_cl_id    — FK to base_cover_letters
"""

from __future__ import annotations

import datetime
import json
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Optional

from app.services.base_asset_ingest import (
    CLFragment,
    CoverLetterResult,
    ResumeBullet,
    ResumeResult,
)
from app.services.scorer import (
    _ALL_VOCAB,
    _DISPLAY_TERMS,
    _EVIDENCE_WEIGHT,
    _build_skill_map,
    _extract_vocab_terms,
    _lookup_skill,
    _normalize,
    _parse_jd_sections,
)
from app.services.resume_tailor import _detect_role_type, _estimate_yoe_label


# ── Tuning constants ──────────────────────────────────────────────────────────

_CAT_WEIGHT: dict[str, float] = {
    "required":  1.5,
    "preferred": 1.0,
    "domain":    0.5,
    "ats":       0.2,
}

_ABSENT_EVIDENCE_WEIGHT = 0.3

_DEFAULT_MAX_PROOF_POINTS = 3


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class FragmentScore:
    """A single CL fragment or resume bullet with its computed relevance score."""
    text:              str
    source_line:       int
    source_type:       str        # "cl_fragment" | "resume_bullet"
    score:             float
    matched_required:  list[str]
    matched_preferred: list[str]
    matched_ats:       list[str]
    has_direct_evidence: bool


@dataclass
class CLProvenance:
    """
    Lightweight record explaining where the generated cover letter content came from.
    Persisted as metadata_json alongside the markdown in generated_assets.
    """
    base_cl_id:                  int
    base_resume_id:              int | None
    job_id:                      int
    salutation_source_line:      int
    proof_point_source_lines:    list[int]
    proof_point_source_types:    list[str]
    closing_source_line:         int
    direct_evidence_used:        list[str]
    adjacent_evidence_referenced: list[str]
    unsupported_gaps_excluded:   list[str]
    jd_required_skills:          list[str]
    jd_preferred_skills:         list[str]
    used_extraction:             bool
    included_adjacency_para:     bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TargetedCLResult:
    """Full output from generate_targeted_cover_letter()."""
    job_id:         int
    asset_id:       int         # 0 until persisted
    base_cl_id:     int
    label:          str
    salutation:     str
    opening:        str
    proof_points:   list[FragmentScore]
    adjacency_para: str | None
    closing:        str
    markdown:       str
    provenance:     CLProvenance

    def to_dict(self) -> dict:
        return {
            "job_id":        self.job_id,
            "asset_id":      self.asset_id,
            "base_cl_id":    self.base_cl_id,
            "label":         self.label,
            "salutation":    self.salutation,
            "opening":       self.opening,
            "proof_points":  [asdict(pp) for pp in self.proof_points],
            "adjacency_para": self.adjacency_para,
            "closing":       self.closing,
            "markdown":      self.markdown,
            "provenance":    self.provenance.to_dict(),
        }


# ── Public entry point ────────────────────────────────────────────────────────

def generate_targeted_cover_letter(
    job_id:           int,
    conn:             sqlite3.Connection,
    profile:          dict,
    base_cl:          CoverLetterResult,
    extracted:        object | None = None,
    assessment:       object | None = None,
    base_resume:      ResumeResult | None = None,
    label:            str = "targeted",
    max_proof_points: int = _DEFAULT_MAX_PROOF_POINTS,
) -> TargetedCLResult:
    """
    Generate a targeted cover letter for *job_id* and persist it to generated_assets.

    Args:
        job_id:           DB id of the target job.
        conn:             Open SQLite connection (caller owns lifecycle).
        profile:          Candidate profile dict.
        base_cl:          Ingested base cover letter.
        extracted:        Optional ExtractionResult for structured term lists.
        assessment:       Optional ScoreBreakdown for precomputed unsupported_gaps.
        base_resume:      Optional ingested base resume — used to supplement proof
                          points when a required skill is not covered by any CL fragment.
        label:            Short version label stored in generated_assets.
        max_proof_points: Maximum proof point paragraphs in the output.

    Returns:
        TargetedCLResult with asset_id populated (DB write has already happened).

    Raises:
        ValueError: if *job_id* is not found in the database.
    """
    # ── 1. Load job ────────────────────────────────────────────────────────────
    row = conn.execute(
        "SELECT raw_text, title, company FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Job id={job_id} not found in database.")
    job_raw_text = row["raw_text"]
    job_title    = row["title"] or "the role"
    job_company  = row["company"] or "your company"

    # ── 2. Resolve term sets ───────────────────────────────────────────────────
    if extracted is not None:
        required_skills  = list(extracted.required_skills)
        preferred_skills = list(extracted.preferred_skills)
        domain_skills    = list(extracted.domain_requirements)
        ats_keywords     = list(extracted.ats_keywords)
        used_extraction  = True
    else:
        sections         = _parse_jd_sections(job_raw_text)
        required_skills  = _extract_vocab_terms(sections.get("must_have") or job_raw_text)
        preferred_skills = _extract_vocab_terms(sections.get("nice_to_have") or "")
        domain_skills    = [
            t for t in _extract_vocab_terms(job_raw_text)
            if _ALL_VOCAB.get(t) == "domain"
        ]
        ats_keywords     = _extract_vocab_terms(job_raw_text)
        used_extraction  = False

    required_set  = set(required_skills)
    preferred_set = set(preferred_skills)
    domain_set    = set(domain_skills)
    ats_set       = set(ats_keywords)

    # ── 3. Build candidate skill map ──────────────────────────────────────────
    skill_map = _build_skill_map(profile, set())

    # ── 4. Extract salutation and closing from base CL ────────────────────────
    opening_frags = [f for f in base_cl.fragments if f.kind == "opening"]
    closing_frags = [f for f in base_cl.fragments if f.kind == "closing"]
    proof_frags   = [f for f in base_cl.fragments if f.kind == "proof_point"]

    salutation_frag  = opening_frags[0] if opening_frags else None
    closing_frag     = closing_frags[0] if closing_frags else None

    salutation      = salutation_frag.text if salutation_frag else ""
    closing_text    = closing_frag.text    if closing_frag    else ""
    sal_source_line = salutation_frag.source_line if salutation_frag else 0
    clo_source_line = closing_frag.source_line    if closing_frag    else 0

    # ── 5. Score proof_point CL fragments ─────────────────────────────────────
    scored_frags: list[FragmentScore] = [
        _score_fragment(f.text, f.source_line, "cl_fragment",
                        required_set, preferred_set, domain_set, ats_set, skill_map)
        for f in proof_frags
    ]
    scored_frags.sort(key=lambda fs: (-fs.score, fs.source_line))

    # ── 6. Select top N proof points from CL fragments ────────────────────────
    selected: list[FragmentScore] = list(scored_frags[:max_proof_points])

    # ── 7. Supplement with resume bullets for uncovered required skills ────────
    if base_resume is not None:
        covered_terms = set()
        for pp in selected:
            covered_terms.update(_extract_vocab_terms(pp.text))

        uncovered_req = [t for t in required_skills
                         if t not in covered_terms
                         and _lookup_skill(t, skill_map) in ("direct", "adjacent", "familiar")]

        if uncovered_req and len(selected) < max_proof_points:
            bullet_scores: list[FragmentScore] = [
                _score_fragment(b.text, b.source_line, "resume_bullet",
                                required_set, preferred_set, domain_set, ats_set, skill_map)
                for b in base_resume.bullet_bank
            ]
            bullet_scores.sort(key=lambda fs: (-fs.score, fs.source_line))

            used_lines = {pp.source_line for pp in selected}
            slots_left = max_proof_points - len(selected)
            for bs in bullet_scores:
                if slots_left <= 0:
                    break
                if bs.source_line in used_lines:
                    continue
                # Only add if it covers at least one uncovered required term
                bs_terms = set(_extract_vocab_terms(bs.text))
                if bs_terms & set(uncovered_req):
                    selected.append(bs)
                    used_lines.add(bs.source_line)
                    slots_left -= 1

    # ── 8. Build provenance sets ───────────────────────────────────────────────
    direct_used = sorted({
        t
        for pp in selected
        for t in pp.matched_required
        if _lookup_skill(t, skill_map) == "direct"
    })
    adjacent_ref = sorted({
        t
        for pp in selected
        for t in (pp.matched_required + pp.matched_preferred)
        if _lookup_skill(t, skill_map) in ("adjacent", "familiar")
    })

    if assessment is not None:
        unsupported_gaps = list(assessment.unsupported_gaps)
    else:
        unsupported_gaps = [
            t for t in required_skills if not _lookup_skill(t, skill_map)
        ]

    # ── 9. Build adjacency paragraph (optional) ────────────────────────────────
    # Terms with only adjacent evidence that aren't already named in proof points
    selected_text_terms = set()
    for pp in selected:
        selected_text_terms.update(_extract_vocab_terms(pp.text))

    adjacent_only = [
        t for t in required_skills
        if _lookup_skill(t, skill_map) in ("adjacent", "familiar")
        and t not in selected_text_terms
        and t not in direct_used
    ][:4]

    adjacency_para: str | None = None
    if adjacent_only:
        adj_terms  = ", ".join(adjacent_only)
        role_type  = _detect_role_type(job_title, required_skills)
        adj_context = {
            "ml":               "applied ML project work",
            "data_engineering": "adjacent data pipeline projects",
            "devops":           "infrastructure and deployment work",
            "frontend":         "full-stack feature work",
            "fullstack":        "cross-functional project work",
            "backend":          "production engineering work",
        }.get(role_type, "production engineering work")
        adjacency_para = (
            f"I also bring adjacent experience with {adj_terms}, developed through "
            f"{adj_context}. While these are not my primary areas, I am confident in "
            f"my ability to contribute and deepen this experience quickly."
        )

    # ── 10. Build opening paragraph ────────────────────────────────────────────
    role_type = _detect_role_type(job_title, required_skills)
    opening   = _build_opening(profile, job_title, job_company, direct_used, skill_map, role_type)

    # ── 11. Render markdown ────────────────────────────────────────────────────
    markdown = _render_markdown(
        profile, salutation, opening, selected, adjacency_para, closing_text
    )

    # ── 12. Build result + persist ─────────────────────────────────────────────
    provenance = CLProvenance(
        base_cl_id                = base_cl.cl_id,
        base_resume_id            = base_resume.resume_id if base_resume else None,
        job_id                    = job_id,
        salutation_source_line    = sal_source_line,
        proof_point_source_lines  = [pp.source_line for pp in selected],
        proof_point_source_types  = [pp.source_type  for pp in selected],
        closing_source_line       = clo_source_line,
        direct_evidence_used      = direct_used,
        adjacent_evidence_referenced = adjacent_ref,
        unsupported_gaps_excluded = unsupported_gaps,
        jd_required_skills        = required_skills,
        jd_preferred_skills       = preferred_skills,
        used_extraction           = used_extraction,
        included_adjacency_para   = adjacency_para is not None,
    )

    result = TargetedCLResult(
        job_id         = job_id,
        asset_id       = 0,
        base_cl_id     = base_cl.cl_id,
        label          = label,
        salutation     = salutation,
        opening        = opening,
        proof_points   = selected,
        adjacency_para = adjacency_para,
        closing        = closing_text,
        markdown       = markdown,
        provenance     = provenance,
    )
    result.asset_id = _persist(conn, result)
    return result


# ── Fragment scoring ──────────────────────────────────────────────────────────

def _score_fragment(
    text:          str,
    source_line:   int,
    source_type:   str,
    required_set:  set[str],
    preferred_set: set[str],
    domain_set:    set[str],
    ats_set:       set[str],
    skill_map:     dict[str, str],
) -> FragmentScore:
    """Score a CL fragment or resume bullet against the job's term sets."""
    score:             float     = 0.0
    matched_required:  list[str] = []
    matched_preferred: list[str] = []
    matched_ats:       list[str] = []

    terms = set(_extract_vocab_terms(text))
    direct_required: list[str] = []

    for term in terms:
        ev   = _lookup_skill(term, skill_map)
        ev_w = _EVIDENCE_WEIGHT.get(ev, _ABSENT_EVIDENCE_WEIGHT) if ev else _ABSENT_EVIDENCE_WEIGHT

        if term in required_set:
            score += _CAT_WEIGHT["required"] * ev_w
            matched_required.append(term)
            if ev == "direct":
                direct_required.append(term)
        elif term in preferred_set:
            score += _CAT_WEIGHT["preferred"] * ev_w
            matched_preferred.append(term)
        elif term in domain_set:
            score += _CAT_WEIGHT["domain"] * ev_w

        if (term in ats_set
                and term not in required_set
                and term not in preferred_set
                and term not in domain_set):
            score += _CAT_WEIGHT["ats"] * ev_w
            matched_ats.append(term)

    return FragmentScore(
        text               = text,
        source_line        = source_line,
        source_type        = source_type,
        score              = score,
        matched_required   = matched_required,
        matched_preferred  = matched_preferred,
        matched_ats        = matched_ats,
        has_direct_evidence= bool(direct_required),
    )


# ── Opening paragraph ─────────────────────────────────────────────────────────

_OPENING_SKILLS_CLAUSE: dict[str, str] = {
    "ml": (
        "With {yoe_phrase} of experience designing and deploying production ML systems "
        "using {skill_phrase}, I bring both the technical depth and the operational "
        "discipline this role demands."
    ),
    "data_engineering": (
        "With {yoe_phrase} of experience building reliable, observable data pipelines "
        "with {skill_phrase}, I am well-positioned to contribute to your data infrastructure."
    ),
    "devops": (
        "With {yoe_phrase} of experience building and maintaining cloud infrastructure "
        "with {skill_phrase}, I bring the reliability engineering mindset this role requires."
    ),
    "frontend": (
        "With {yoe_phrase} of experience delivering accessible, performant user interfaces "
        "with {skill_phrase}, I am eager to bring this craft to your product."
    ),
    "fullstack": (
        "With {yoe_phrase} of experience shipping end-to-end features using {skill_phrase}, "
        "I am well-suited to contribute across your stack."
    ),
    "backend": (
        "With {yoe_phrase} of experience building scalable backend systems "
        "with {skill_phrase}, I am confident I can contribute meaningfully to your team."
    ),
}


def _build_opening(
    profile:      dict,
    job_title:    str,
    job_company:  str,
    direct_used:  list[str],
    skill_map:    dict[str, str],
    role_type:    str = "backend",
) -> str:
    """
    Build a templated opening paragraph grounded in confirmed profile evidence.
    Never names skills that are unsupported gaps.
    Varies by role_type so different jobs produce different letters.
    """
    yoe_label  = _estimate_yoe_label(profile)
    yoe_phrase = f"{yoe_label} years" if yoe_label else "several years"

    def _display(term: str) -> str:
        return _DISPLAY_TERMS.get(term, term.title() if " " not in term else term)

    if direct_used:
        disp = [_display(s) for s in direct_used[:3]]
        if len(disp) >= 3:
            skill_phrase = f"{disp[0]}, {disp[1]}, and {disp[2]}"
        elif len(disp) == 2:
            skill_phrase = f"{disp[0]} and {disp[1]}"
        else:
            skill_phrase = disp[0]

        tmpl = _OPENING_SKILLS_CLAUSE.get(role_type, _OPENING_SKILLS_CLAUSE["backend"])
        skills_clause = " " + tmpl.format(yoe_phrase=yoe_phrase, skill_phrase=skill_phrase)
    else:
        skills_clause = (
            f" With {yoe_phrase} of software engineering experience, "
            f"I am confident I can contribute meaningfully to your team."
        )

    return (
        f"I am writing to express my interest in {job_title} at {job_company}."
        + skills_clause
    )


# ── Markdown rendering ────────────────────────────────────────────────────────

def _render_markdown(
    profile:        dict,
    salutation:     str,
    opening:        str,
    proof_points:   list[FragmentScore],
    adjacency_para: str | None,
    closing:        str,
) -> str:
    """
    Render the targeted cover letter as a markdown string.

    Structure:
      **Name**
      Location

      {Salutation — verbatim from base CL}

      {Opening — templated}

      {Proof point 1 — verbatim}
      ...

      {Adjacency paragraph — templated, optional}

      Thank you for your time and consideration. ...

      {Sign-off — verbatim from base CL}
    """
    personal = profile.get("personal", {})
    name     = personal.get("name", "Candidate")
    location = personal.get("location", "")

    lines: list[str] = []

    lines.append(f"**{name}**")
    if location and not str(location).lower().startswith("todo"):
        lines.append(location)
    lines.append("")

    if salutation:
        lines.append(salutation)
        lines.append("")

    lines.append(opening)
    lines.append("")

    for pp in proof_points:
        lines.append(pp.text)
        lines.append("")

    if adjacency_para:
        lines.append(adjacency_para)
        lines.append("")

    lines.append(
        "Thank you for your time and consideration. "
        "I look forward to the opportunity to discuss how my background "
        "can contribute to your team."
    )
    lines.append("")

    if closing:
        lines.append(closing)

    return "\n".join(lines)


# ── DB persistence ────────────────────────────────────────────────────────────

def _persist(conn: sqlite3.Connection, result: TargetedCLResult) -> int:
    """Write the generated cover letter to generated_assets and return the new row id."""
    cur = conn.execute(
        """INSERT INTO generated_assets
           (job_id, asset_type, content, metadata_json, base_cl_id, label)
           VALUES (?, 'cover_letter', ?, ?, ?, ?)""",
        (
            result.job_id,
            result.markdown,
            json.dumps(result.provenance.to_dict()),
            result.base_cl_id,
            result.label,
        ),
    )
    conn.commit()
    return cur.lastrowid
