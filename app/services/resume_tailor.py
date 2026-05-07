"""
app/services/resume_tailor.py — Deterministic targeted resume generator.

Design contract:
  - Grounded: every selected bullet comes verbatim from the ingested base resume.
  - Honest: adjacent evidence influences ordering only; unsupported gaps are never
    presented as owned qualifications.
  - Deterministic: same inputs always produce the same output.
  - No LLM: pure ranking, templating, and vocabulary lookup.

Public API:
  generate_targeted_resume(job_id, conn, profile, base_resume, ...)
      -> TailoredResumeResult   (parses + ranks + persists)

Ranking algorithm:
  Each resume bullet is scored by summing the relevance weights of the vocabulary
  terms it contains, multiplied by the candidate's profile evidence weight for
  that term.

  Term category weights:
    required skill   1.5   (must-have match → highest value)
    preferred skill  1.0   (nice-to-have match)
    domain term      0.5   (industry/domain alignment)
    ats keyword only 0.2   (keyword present in JD but not a named required/preferred)

  Profile evidence multipliers (from scorer.py vocabulary):
    direct   1.0   (candidate claims direct experience)
    adjacent 0.5   (candidate claims adjacent / transferable experience)
    familiar 0.2   (candidate is familiar but not a practitioner)
    absent   0.3   (term is in a real bullet but not in profile — still valid, just
                    weighted lower since the profile doesn't confirm it)

  The top max_bullets_per_section bullets per section (by score) are selected;
  they are then presented in source-line order within the section so reading
  order is natural.

Persistence:
  Writes one row to generated_assets (asset_type='resume'):
    content       — full markdown string
    metadata_json — TailoredResumeProvenance serialised to JSON
"""

from __future__ import annotations

import datetime
import json
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Optional

from app.services.base_asset_ingest import ResumeBullet, ResumeResult
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


# ── Tuning constants ──────────────────────────────────────────────────────────

# Relevance weight for each term category
_CAT_WEIGHT: dict[str, float] = {
    "required":  1.5,
    "preferred": 1.0,
    "domain":    0.5,
    "ats":       0.2,
}

# Evidence weight when a bullet term is NOT in the candidate profile
# (the bullet is still real experience; we just weight it conservatively)
_ABSENT_EVIDENCE_WEIGHT = 0.3

# Sections whose bullets we never surface in the targeted output
# (they are handled separately via the profile or the skills block)
_SKIP_SECTIONS = frozenset({
    "header", "contact", "skills", "education",
    "certifications", "languages", "interests", "summary",
})

# Default cap on bullets per section in the targeted output
_DEFAULT_MAX_BULLETS = 8


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class BulletScore:
    """A single resume bullet with its computed relevance score and match breakdown."""
    bullet:             ResumeBullet
    score:              float
    matched_required:   list[str]   # required-skill terms found in bullet
    matched_preferred:  list[str]   # preferred-skill terms found in bullet
    matched_ats:        list[str]   # ATS-only terms found in bullet
    has_direct_evidence: bool        # ≥1 required term w/ direct profile evidence


@dataclass
class TailoredResumeProvenance:
    """
    Lightweight record explaining where the generated content came from.
    Persisted as metadata_json alongside the markdown in generated_assets.
    """
    base_resume_id:                   int
    job_id:                           int
    selected_bullet_source_lines:     list[int]   # 1-based source lines kept
    excluded_bullet_source_lines:     list[int]   # 1-based source lines dropped
    direct_evidence_used:             list[str]   # required terms with direct evidence
    adjacent_evidence_referenced:     list[str]   # req/pref terms with adjacent evidence
    unsupported_gaps_excluded:        list[str]   # required terms with no profile evidence
    jd_required_skills:               list[str]
    jd_preferred_skills:              list[str]
    used_extraction:                  bool
    total_bullets_available:          int
    total_bullets_selected:           int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TailoredResumeResult:
    """Full output from generate_targeted_resume()."""
    job_id:           int
    asset_id:         int                       # 0 until persisted
    base_resume_id:   int
    label:            str
    summary:          str                       # generated targeted summary paragraph
    skills_section:   dict[str, list[str]]      # category → [skill names, priority order]
    scored_bullets:   list[BulletScore]         # ALL bullets ranked by score
    selected_bullets: list[BulletScore]         # bullets that appear in the output
    markdown:         str                       # full draft resume in markdown
    provenance:       TailoredResumeProvenance

    def to_dict(self) -> dict:
        return {
            "job_id":          self.job_id,
            "asset_id":        self.asset_id,
            "base_resume_id":  self.base_resume_id,
            "label":           self.label,
            "summary":         self.summary,
            "skills_section":  self.skills_section,
            "selected_bullet_source_lines": [
                bs.bullet.source_line for bs in self.selected_bullets
            ],
            "markdown":        self.markdown,
            "provenance":      self.provenance.to_dict(),
        }


# ── Public entry point ────────────────────────────────────────────────────────

def generate_targeted_resume(
    job_id:                 int,
    conn:                   sqlite3.Connection,
    profile:                dict,
    base_resume:            ResumeResult,
    extracted:              object | None = None,   # ExtractionResult | None
    assessment:             object | None = None,   # ScoreBreakdown | None
    label:                  str   = "targeted",
    max_bullets_per_section: int  = _DEFAULT_MAX_BULLETS,
    evidence_items:         list | None   = None,   # EvidenceItem list from evidence_bank
) -> TailoredResumeResult:
    """
    Generate a targeted resume for *job_id* and persist it to generated_assets.

    Args:
        job_id:                  DB id of the target job.
        conn:                    Open SQLite connection (caller owns lifecycle).
        profile:                 Candidate profile dict (from profile_loader.load_profile).
        base_resume:             Ingested base resume (from base_asset_ingest.ingest_resume).
        extracted:               Optional ExtractionResult.  When provided, uses
                                 structured required/preferred/domain/ats lists;
                                 otherwise falls back to vocab-scanning the job raw text.
        assessment:              Optional ScoreBreakdown.  Used only to pull a
                                 pre-computed unsupported_gaps list; recomputed if absent.
        label:                   Short version label stored in generated_assets.
        max_bullets_per_section: Maximum bullets per section in the output.  Highest-
                                 scoring bullets are chosen; ties broken by source_line.

    Returns:
        TailoredResumeResult with asset_id populated (DB write has already happened).

    Raises:
        ValueError: if *job_id* is not found in the database.
    """
    # ── 1. Load job text ───────────────────────────────────────────────────────
    row = conn.execute(
        "SELECT raw_text, remote_policy, title FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Job id={job_id} not found in database.")
    job_raw_text = row["raw_text"]
    job_title    = row["title"] or ""

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

    # ── 4. Score every bullet ─────────────────────────────────────────────────
    scored: list[BulletScore] = [
        _score_bullet(b, required_set, preferred_set, domain_set, ats_set, skill_map)
        for b in base_resume.bullet_bank
    ]
    # Sort by score descending, break ties by source_line ascending
    scored.sort(key=lambda bs: (-bs.score, bs.bullet.source_line))

    # ── 5. Select top N bullets per section ───────────────────────────────────
    selected = _select_bullets(scored, max_bullets_per_section)
    selected_lines = {bs.bullet.source_line for bs in selected}

    # ── 6. Build provenance ────────────────────────────────────────────────────
    direct_used = sorted({
        t
        for bs in selected
        for t in bs.matched_required
        if _lookup_skill(t, skill_map) == "direct"
    })
    adjacent_ref = sorted({
        t
        for bs in selected
        for t in (bs.matched_required + bs.matched_preferred)
        if _lookup_skill(t, skill_map) in ("adjacent", "familiar")
    })

    if assessment is not None:
        unsupported_gaps = list(assessment.unsupported_gaps)
    else:
        unsupported_gaps = [
            t for t in required_skills if not _lookup_skill(t, skill_map)
        ]

    provenance = TailoredResumeProvenance(
        base_resume_id                = base_resume.resume_id,
        job_id                        = job_id,
        selected_bullet_source_lines  = sorted(selected_lines),
        excluded_bullet_source_lines  = sorted(
            bs.bullet.source_line
            for bs in scored
            if bs.bullet.source_line not in selected_lines
        ),
        direct_evidence_used          = direct_used,
        adjacent_evidence_referenced  = adjacent_ref,
        unsupported_gaps_excluded     = unsupported_gaps,
        jd_required_skills            = required_skills,
        jd_preferred_skills           = preferred_skills,
        used_extraction               = used_extraction,
        total_bullets_available       = len(base_resume.bullet_bank),
        total_bullets_selected        = len(selected),
    )

    # ── 7. Build ordered skills block ─────────────────────────────────────────
    skills_section = _build_skills_section(profile, required_set, preferred_set)

    # ── 8. Generate summary paragraph ─────────────────────────────────────────
    summary = _generate_summary(
        profile, required_skills, skill_map, direct_used, job_title=job_title
    )

    # ── 9. Render markdown ────────────────────────────────────────────────────
    markdown = _render_markdown(
        profile, summary, skills_section, selected, base_resume
    )

    # ── 9b. Optional evidence bank highlights ─────────────────────────────────
    # Append verbatim text from direct evidence items allowed for resume use.
    # This section is labelled clearly so it is never mistaken for generated copy.
    if evidence_items:
        direct_resume = [
            item for item in evidence_items
            if item.evidence_strength == "direct" and "resume" in item.allowed_uses
        ]
        if direct_resume:
            markdown = _append_evidence_highlights(markdown, direct_resume)

    # ── 10. Persist + return ──────────────────────────────────────────────────
    result = TailoredResumeResult(
        job_id           = job_id,
        asset_id         = 0,
        base_resume_id   = base_resume.resume_id,
        label            = label,
        summary          = summary,
        skills_section   = skills_section,
        scored_bullets   = scored,
        selected_bullets = selected,
        markdown         = markdown,
        provenance       = provenance,
    )
    result.asset_id = _persist(conn, result)
    return result


# ── Bullet scoring ────────────────────────────────────────────────────────────

def _score_bullet(
    bullet:       ResumeBullet,
    required_set: set[str],
    preferred_set: set[str],
    domain_set:   set[str],
    ats_set:      set[str],
    skill_map:    dict[str, str],
) -> BulletScore:
    """
    Compute a relevance score for *bullet* against the job's term sets.

    Score = Σ  category_weight × profile_evidence_weight
    summed over every vocabulary term found in the bullet text.
    """
    score:              float      = 0.0
    matched_required:  list[str]  = []
    matched_preferred: list[str]  = []
    matched_ats:       list[str]  = []

    terms = set(_extract_vocab_terms(bullet.text))

    # Track which required terms have direct evidence
    direct_required: list[str] = []

    for term in terms:
        ev      = _lookup_skill(term, skill_map)
        ev_w    = _EVIDENCE_WEIGHT.get(ev, _ABSENT_EVIDENCE_WEIGHT) if ev else _ABSENT_EVIDENCE_WEIGHT

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
        # ATS bonus for any matching keyword not already counted above
        if (term in ats_set
                and term not in required_set
                and term not in preferred_set
                and term not in domain_set):
            score += _CAT_WEIGHT["ats"] * ev_w
            matched_ats.append(term)

    return BulletScore(
        bullet             = bullet,
        score              = score,
        matched_required   = matched_required,
        matched_preferred  = matched_preferred,
        matched_ats        = matched_ats,
        has_direct_evidence= bool(direct_required),
    )


# ── Bullet selection ──────────────────────────────────────────────────────────

def _select_bullets(
    scored:          list[BulletScore],   # already sorted score DESC
    max_per_section: int,
) -> list[BulletScore]:
    """
    Pick the top *max_per_section* bullets for each section, then re-sort each
    section's bullets by source_line for natural reading order in the output.

    Sections in _SKIP_SECTIONS are excluded from selection entirely.
    """
    by_section: dict[str, list[BulletScore]] = {}
    for bs in scored:
        sec = bs.bullet.section
        if sec in _SKIP_SECTIONS:
            continue
        by_section.setdefault(sec, []).append(bs)

    selected: list[BulletScore] = []
    for section_bullets in by_section.values():
        # scored list is already sorted score DESC → take first N
        top = section_bullets[:max_per_section]
        # Re-sort by source_line for natural document order
        top.sort(key=lambda bs: bs.bullet.source_line)
        selected.extend(top)

    return selected


# ── Skills section reordering ─────────────────────────────────────────────────

def _build_skills_section(
    profile:       dict,
    required_set:  set[str],
    preferred_set: set[str],
) -> dict[str, list[str]]:
    """
    Return an ordered skills dict from the profile:
      - Required-skill matches first (priority 0)
      - Preferred-skill matches second (priority 1)
      - Everything else last (priority 2)
    TODO entries and blank entries are excluded.
    """
    result: dict[str, list[str]] = {}

    for cat, items in profile.get("skills", {}).items():
        if not isinstance(items, list):
            continue

        ranked: list[tuple[int, str]] = []

        for item in items:
            if isinstance(item, dict):
                name = item.get("name", "")
                ev   = item.get("evidence", "direct")
            else:
                name = str(item)
                ev   = "direct"

            if not name or str(name).lower().startswith("todo"):
                continue
            if str(ev).lower().startswith("todo"):
                continue

            norm = _normalize(str(name))
            if norm in required_set:
                priority = 0
            elif norm in preferred_set:
                priority = 1
            else:
                priority = 2

            ranked.append((priority, name))

        if ranked:
            ranked.sort(key=lambda x: x[0])
            result[cat] = [name for _, name in ranked]

    return result


# ── Summary generation ────────────────────────────────────────────────────────

_ROLE_TYPE_FRAMING: dict[str, str] = {
    "ml":               "building and deploying production ML systems",
    "data_engineering": "building reliable, observable data pipelines at scale",
    "devops":           "cloud infrastructure, CI/CD, and reliability engineering",
    "frontend":         "building responsive, accessible user interfaces",
    "fullstack":        "delivering end-to-end product features across the stack",
    "backend":          "designing and scaling backend services and APIs",
}


def _generate_summary(
    profile:            dict,
    required_skills:    list[str],
    skill_map:          dict[str, str],
    direct_evidence:    list[str],
    job_title:          str = "",
) -> str:
    """
    Build a short targeted summary paragraph from profile data + job requirements.
    Uses only confirmed profile data — never fabricates qualifications.
    Varies framing based on detected role type so summaries differ across job types.
    """
    personal  = profile.get("personal", {})
    targets   = profile.get("job_targets", {})

    seniority_raw = targets.get("seniority_self_assessed", "")
    seniority = (
        str(seniority_raw).lower()
        if isinstance(seniority_raw, str) and not str(seniority_raw).lower().startswith("todo")
        else ""
    )

    yoe_label  = _estimate_yoe_label(profile)
    role_type  = _detect_role_type(job_title, required_skills)

    # Top required skills the candidate can actually claim (direct first, then adjacent)
    direct_req   = [s for s in required_skills if _lookup_skill(s, skill_map) == "direct"][:3]
    adjacent_req = [
        s for s in required_skills
        if _lookup_skill(s, skill_map) in ("adjacent", "familiar")
    ][:2]
    # Never fall back to raw required_skills — those may be gaps the candidate can't claim.
    top_skills = direct_req or direct_evidence[:3]

    def _display(term: str) -> str:
        return _DISPLAY_TERMS.get(term, term.title() if " " not in term else term)

    seniority_label = seniority.title() if seniority else "Software"
    yoe_phrase      = f" with {yoe_label} years of experience" if yoe_label else ""
    role_framing    = _ROLE_TYPE_FRAMING.get(role_type, "building production software systems")

    if top_skills:
        disp = [_display(s) for s in top_skills]
        if len(disp) >= 3:
            skill_phrase = f"{disp[0]}, {disp[1]}, and {disp[2]}"
        elif len(disp) == 2:
            skill_phrase = f"{disp[0]} and {disp[1]}"
        else:
            skill_phrase = disp[0]
        opening = (
            f"{seniority_label} software engineer{yoe_phrase} "
            f"specializing in {skill_phrase}."
        )
    else:
        opening = f"{seniority_label} software engineer{yoe_phrase}."

    parts = [opening]

    # Role-type framing: makes summaries meaningfully different across job types
    parts.append(f"Track record of {role_framing}.")

    # Supplementary sentence: extra direct-evidence skills not yet mentioned
    extra = [s for s in direct_evidence if s not in top_skills][:4]
    if not extra:
        extra = [s for s in required_skills if _lookup_skill(s, skill_map) == "direct"
                 and s not in top_skills][:3]
    if extra:
        extra_disp = [_display(s) for s in extra]
        if len(extra_disp) >= 3:
            extra_phrase = f"{extra_disp[0]}, {extra_disp[1]}, and {extra_disp[2]}"
        elif len(extra_disp) == 2:
            extra_phrase = f"{extra_disp[0]} and {extra_disp[1]}"
        else:
            extra_phrase = extra_disp[0]
        parts.append(f"Proven expertise in {extra_phrase}.")

    # Optional adjacent hint (honest framing — "familiar with" not "expert in")
    if adjacent_req and not direct_req:
        adj_phrase = ", ".join(adjacent_req)
        parts.append(f"Transferable experience with {adj_phrase}.")

    return "  ".join(parts)


def _detect_role_type(job_title: str, required_skills: list[str]) -> str:
    """
    Infer the primary role type from job title + required skills.
    Returns one of: ml, data_engineering, devops, frontend, fullstack, backend.
    """
    title = job_title.lower()
    if any(k in title for k in ("machine learning", " ml ", "ml engineer", "ai engineer",
                                 "deep learning", "data scientist", "mlops")):
        return "ml"
    if any(k in title for k in ("data engineer", "data platform", "etl", "analytics engineer",
                                 "data infrastructure")):
        return "data_engineering"
    if any(k in title for k in ("devops", "site reliability", " sre", "platform engineer",
                                 "infrastructure engineer", "cloud engineer")):
        return "devops"
    if any(k in title for k in ("frontend", "front-end", "front end", "ui engineer")):
        return "frontend"
    if any(k in title for k in ("fullstack", "full-stack", "full stack")):
        return "fullstack"

    # Infer from required skills when title is ambiguous
    skill_set = set(required_skills)
    ml_signals = {"pytorch", "tensorflow", "scikit-learn", "scikit learn", "mlflow",
                  "machine learning", "deep learning", "hugging face"}
    de_signals = {"spark", "airflow", "kafka", "dbt", "snowflake", "redshift",
                  "bigquery", "data engineering", "flink"}
    ops_signals = {"kubernetes", "terraform", "prometheus", "grafana", "ci/cd", "devops"}
    if len(skill_set & ml_signals) >= 2:
        return "ml"
    if len(skill_set & de_signals) >= 2:
        return "data_engineering"
    if len(skill_set & ops_signals) >= 2:
        return "devops"
    return "backend"


def _estimate_yoe_label(profile: dict) -> str:
    """
    Estimate years of experience from experience entries.
    Returns a display string like '7+' or '5', or '' if undetermined.
    """
    experience = profile.get("experience", [])
    if not experience:
        return ""

    earliest: int | None = None
    for exp in experience:
        start = exp.get("start_date", "") if isinstance(exp, dict) else ""
        if not start or str(start).lower().startswith("todo"):
            continue
        try:
            year = int(str(start).split("-")[0])
            if earliest is None or year < earliest:
                earliest = year
        except (ValueError, IndexError):
            continue

    if earliest is None:
        return ""

    years = datetime.datetime.now().year - earliest
    if years <= 0:
        return ""
    return f"{years}+"


# ── Markdown rendering ────────────────────────────────────────────────────────

def _render_markdown(
    profile:          dict,
    summary:          str,
    skills_section:   dict[str, list[str]],
    selected_bullets: list[BulletScore],
    base_resume:      ResumeResult,
) -> str:
    """
    Render the targeted resume as a markdown string.

    Structure:
      # Name
      Location

      ## Summary
      {summary}

      ## Skills
      **Category:** skill1, skill2, ...

      ## {Section heading}   (experience, projects, etc.)
      - bullet text
      ...

      ## Education
      - Institution — Degree (Year)

      ## Certifications
      - Cert name (Year)
    """
    lines: list[str] = []

    personal = profile.get("personal", {})
    name     = personal.get("name", "Candidate")
    location = personal.get("location", "")

    # ── Header ─────────────────────────────────────────────────────────────────
    lines.append(f"# {name}")
    if location and not str(location).lower().startswith("todo"):
        lines.append(location)
    lines.append("")

    # ── Summary ───────────────────────────────────────────────────────────────
    lines.append("## Summary")
    lines.append("")
    lines.append(summary)
    lines.append("")

    # ── Skills ────────────────────────────────────────────────────────────────
    if skills_section:
        lines.append("## Skills")
        lines.append("")
        for cat, skill_names in skills_section.items():
            if skill_names:
                cat_label = cat.replace("_", " ").title()
                lines.append(f"**{cat_label}:** {', '.join(skill_names)}")
        lines.append("")

    # ── Experience / Projects (bullets grouped by section) ────────────────────
    # Canonical section priority keeps Experience before Projects on every run.
    _SECTION_PRIORITY = {
        "experience": 0, "projects": 1, "achievements": 2,
        "publications": 3, "volunteer": 4, "other": 99,
    }

    bullets_by_section: dict[str, list[BulletScore]] = {}
    for bs in selected_bullets:
        sec = bs.bullet.section
        if sec not in bullets_by_section:
            bullets_by_section[sec] = []
        bullets_by_section[sec].append(bs)

    # Sort sections by canonical priority, then alphabetically for stable output
    section_order = sorted(
        bullets_by_section.keys(),
        key=lambda s: (_SECTION_PRIORITY.get(s, 50), s),
    )

    # Use the original heading text from the base resume where available
    heading_map: dict[str, str] = {
        s.name: s.heading
        for s in base_resume.sections
        if s.heading and s.name not in _SKIP_SECTIONS
    }

    for sec_name in section_order:
        if sec_name in _SKIP_SECTIONS:
            continue
        heading = heading_map.get(sec_name) or sec_name.title()
        lines.append(f"## {heading}")
        lines.append("")
        for bs in bullets_by_section[sec_name]:
            lines.append(f"- {bs.bullet.text}")
        lines.append("")

    # ── Education (from profile) ───────────────────────────────────────────────
    education = [
        e for e in profile.get("education", [])
        if isinstance(e, dict) and e.get("institution")
        and not str(e.get("institution", "")).lower().startswith("todo")
    ]
    if education:
        lines.append("## Education")
        lines.append("")
        for edu in education:
            institution = edu.get("institution", "")
            degree      = edu.get("degree", "")
            year        = edu.get("year", "")
            entry       = institution
            if degree and not str(degree).lower().startswith("todo"):
                entry += f" — {degree}"
            if year and not str(year).lower().startswith("todo"):
                entry += f" ({year})"
            lines.append(f"- {entry}")
        lines.append("")

    # ── Certifications (from profile) ─────────────────────────────────────────
    certs = [
        c for c in profile.get("certifications", [])
        if isinstance(c, dict) and c.get("name")
        and not str(c.get("name", "")).lower().startswith("todo")
    ]
    if certs:
        lines.append("## Certifications")
        lines.append("")
        for cert in certs:
            cert_name = cert.get("name", "")
            cert_year = cert.get("year", "")
            entry     = cert_name
            if cert_year and not str(cert_year).lower().startswith("todo"):
                entry += f" ({cert_year})"
            lines.append(f"- {entry}")
        lines.append("")

    return "\n".join(lines)


# ── Evidence Bank hook ────────────────────────────────────────────────────────

def _append_evidence_highlights(markdown: str, items: list) -> str:
    """
    Append a "Key Evidence" section to *markdown* containing verbatim raw_text
    from direct evidence bank items allowed for resume use.

    Verbatim content only — nothing is rewritten or inferred beyond what the
    item's raw_text already says.
    """
    lines: list[str] = [markdown.rstrip(), "", "## Key Evidence", ""]
    for item in items:
        lines.append(f"**{item.title}**")
        for line in item.raw_text.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(f"  {stripped}")
        lines.append("")
    return "\n".join(lines)


# ── DB persistence ────────────────────────────────────────────────────────────

def _persist(conn: sqlite3.Connection, result: TailoredResumeResult) -> int:
    """
    Write the generated resume to generated_assets and return the new row id.
    """
    cur = conn.execute(
        """INSERT INTO generated_assets
           (job_id, asset_type, content, metadata_json, base_resume_id, label)
           VALUES (?, 'resume', ?, ?, ?, ?)""",
        (
            result.job_id,
            result.markdown,
            json.dumps(result.provenance.to_dict()),
            result.base_resume_id,
            result.label,
        ),
    )
    conn.commit()
    return cur.lastrowid
