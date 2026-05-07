"""
app/services/profile_synthesis.py — Derive structured skill/domain proposals
from all user materials already stored in the database.

Sources consulted (in decreasing strength order):
  1. Base resume bullets  — vocab-extracted terms with frequency counting
  2. Evidence Bank items  — skill_tags / domain_tags with evidence_strength
  3. Accepted PR obs      — skill_tags / domain_tags with evidence_strength
  4. Candidate assessments— demonstrated_skills / demonstrated_domains
                            (capped at 'adjacent'; assessments alone never
                             produce 'direct')

Level classification:
  direct   — ≥2 resume-bullet hits, OR evidence bank / PR strength='direct'
  adjacent — 1 resume-bullet hit, OR strength='adjacent', OR
              assessment (high or medium confidence)
  familiar — strength='inferred', OR assessment with low confidence,
              OR assessment-only with no harder evidence

Design rules:
  • Deterministic: same DB state → same output every time.
  • Conservative:  never upgrades a term beyond what the data supports.
  • Grounded:      only emits terms whose category can be determined from
                   the shared vocabulary, or which were explicitly tagged
                   as domain entries.
  • Non-destructive: reads only; never writes to the DB.
  • Assessments cannot produce 'direct' on their own — that level requires
    hard evidence from the resume or the Evidence Bank.

Public API:
  synthesize_profile(conn) -> SynthesisResult
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from app.services.scorer import _ALL_VOCAB, _DISPLAY_TERMS, _extract_vocab_terms


# ── Level helpers ─────────────────────────────────────────────────────────────

# Numeric rank: higher = stronger evidence
_LEVEL_RANK: dict[str, int] = {"direct": 3, "adjacent": 2, "familiar": 1}

# Map evidence_strength values (from evidence_bank / PR) to synthesis level
_STRENGTH_TO_LEVEL: dict[str, str] = {
    "direct":   "direct",
    "adjacent": "adjacent",
    "inferred": "familiar",
}

# Map vocab category names → profile section keys
_CAT_TO_SECTION: dict[str, str] = {
    "language":  "languages",
    "framework": "frameworks",
    "database":  "databases",
    "cloud":     "cloud",
    "tool":      "tools",
    "practice":  "practices",
    "domain":    "domains",
}


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class SynthesizedSkill:
    name:    str
    level:   str        # "direct" | "adjacent" | "familiar"
    sources: list[str]  # human-readable source traces, e.g. ["resume (3 bullets)"]


@dataclass
class SynthesisResult:
    languages:    list[SynthesizedSkill]
    frameworks:   list[SynthesizedSkill]
    databases:    list[SynthesizedSkill]
    cloud:        list[SynthesizedSkill]
    tools:        list[SynthesizedSkill]
    practices:    list[SynthesizedSkill]
    domains:      list[SynthesizedSkill]
    sources_used: list[str]   # e.g. ["Base resume (12 bullets)", "Evidence Bank (5 items)"]
    skills_found: int


# ── Internal helpers ──────────────────────────────────────────────────────────

def _display(term: str) -> str:
    """Properly-cased display name for a vocab term."""
    if term in _DISPLAY_TERMS:
        return _DISPLAY_TERMS[term]
    # Reasonable fallback: title-case, but keep CI/CD-style capitalisation intact
    return term.title()


def _max_level(a: str | None, b: str) -> str:
    """Return the stronger of two level strings."""
    if a is None:
        return b
    return a if _LEVEL_RANK.get(a, 0) >= _LEVEL_RANK.get(b, 0) else b


class _Accumulator:
    """
    Tracks the best observed level and source list for every normalised term.
    Separate instances are kept for skill-section terms and domain terms so
    that categorisation is unambiguous at build time.
    """

    def __init__(self) -> None:
        # norm_term → {"level": str|None, "sources": list[str]}
        self._data: dict[str, dict] = {}

    def register(self, term: str, level: str, source: str) -> None:
        term = term.strip().lower()
        if not term:
            return
        if term not in self._data:
            self._data[term] = {"level": None, "sources": []}
        td = self._data[term]
        td["level"] = _max_level(td["level"], level)
        if source not in td["sources"]:
            td["sources"].append(source)

    def items(self):
        return self._data.items()


# ── Main synthesis function ───────────────────────────────────────────────────

def synthesize_profile(conn: sqlite3.Connection) -> SynthesisResult:
    """
    Derive structured skill/domain proposals from all available DB sources.
    Returns a SynthesisResult with each section sorted by level desc, name asc.
    """

    # Two separate accumulators:
    #   skill_acc  — vocab-only terms whose category maps to a skill section
    #   domain_acc — domain-category and free-form domain strings
    skill_acc  = _Accumulator()
    domain_acc = _Accumulator()
    sources_used: list[str] = []

    def _reg_skill(term: str, level: str, source: str) -> None:
        """Register *term* only if it lives in a known skill section."""
        cat = _ALL_VOCAB.get(term)
        if cat is not None and cat in _CAT_TO_SECTION:
            skill_acc.register(term, level, source)

    def _reg_domain_tag(tag: str, level: str, source: str) -> None:
        """
        Route a domain-tagged string to the right accumulator.
        Terms that are actually skill/practice vocabulary (e.g. "ci/cd" in domain_tags)
        are sent to skill_acc; genuine domain-category or unknown strings go to domain_acc.
        """
        tag_lower = tag.strip().lower()
        if not tag_lower:
            return
        cat = _ALL_VOCAB.get(tag_lower)
        if cat == "domain" or cat is None:
            # Genuine domain or unknown — store in domain bucket
            domain_acc.register(tag_lower, level, source)
        elif cat in _CAT_TO_SECTION:
            # Actually a skill/practice term that was placed in domain_tags
            skill_acc.register(tag_lower, level, source)

    # ── 1. Base resume (latest only) ──────────────────────────────────────────
    resume_row = conn.execute(
        "SELECT normalized_json FROM base_resumes ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if resume_row:
        norm = json.loads(resume_row["normalized_json"] or "{}")
        bullets = norm.get("bullet_bank", [])

        # Count occurrences of each vocab term across all bullets
        term_counts: dict[str, int] = {}
        for b in bullets:
            for t in b.get("skills", []):
                term_counts[t] = term_counts.get(t, 0) + 1

        for term, count in term_counts.items():
            # ≥2 bullets = direct evidence; 1 bullet = adjacent
            level  = "direct" if count >= 2 else "adjacent"
            source = f"resume ({count} bullet{'s' if count > 1 else ''})"
            _reg_skill(term, level, source)

        if bullets:
            sources_used.append(f"Base resume ({len(bullets)} bullet{'s' if len(bullets) != 1 else ''})")

    # ── 2. Evidence Bank items ─────────────────────────────────────────────────
    ev_rows = conn.execute(
        "SELECT skill_tags, domain_tags, evidence_strength FROM evidence_items"
    ).fetchall()

    if ev_rows:
        for row in ev_rows:
            strength = row["evidence_strength"] or "adjacent"
            level    = _STRENGTH_TO_LEVEL.get(strength, "adjacent")
            for tag in json.loads(row["skill_tags"] or "[]"):
                _reg_skill(tag.lower(), level, "evidence bank")
            for tag in json.loads(row["domain_tags"] or "[]"):
                _reg_domain_tag(tag, level, "evidence bank")
        n = len(ev_rows)
        sources_used.append(f"Evidence Bank ({n} item{'s' if n != 1 else ''})")

    # ── 3. Accepted Profile Reconstruction observations ───────────────────────
    pr_rows = conn.execute(
        "SELECT skill_tags, domain_tags, evidence_strength "
        "FROM pr_observations WHERE review_state = 'accepted'"
    ).fetchall()

    if pr_rows:
        for row in pr_rows:
            strength = row["evidence_strength"] or "adjacent"
            level    = _STRENGTH_TO_LEVEL.get(strength, "adjacent")
            for tag in json.loads(row["skill_tags"] or "[]"):
                _reg_skill(tag.lower(), level, "profile reconstruction")
            for tag in json.loads(row["domain_tags"] or "[]"):
                _reg_domain_tag(tag, level, "profile reconstruction")
        n = len(pr_rows)
        sources_used.append(f"Accepted observation{'s' if n != 1 else ''} ({n})")

    # ── 4. Candidate assessments (capped at 'adjacent') ───────────────────────
    asmt_rows = conn.execute(
        "SELECT demonstrated_skills, demonstrated_domains, confidence "
        "FROM candidate_assessments"
    ).fetchall()

    if asmt_rows:
        for row in asmt_rows:
            confidence = (row["confidence"] or "medium").lower()
            # Assessments NEVER produce 'direct'; low-confidence → 'familiar'
            level = "familiar" if confidence == "low" else "adjacent"
            for tag in json.loads(row["demonstrated_skills"] or "[]"):
                # Extract recognised vocab terms from each free-form skill string
                for term in _extract_vocab_terms(tag):
                    _reg_skill(term, level, "candidate assessment")
            for tag in json.loads(row["demonstrated_domains"] or "[]"):
                _reg_domain_tag(tag, level, "candidate assessment")
        n = len(asmt_rows)
        sources_used.append(f"Candidate assessment{'s' if n != 1 else ''} ({n})")

    # ── Build output buckets ──────────────────────────────────────────────────
    buckets: dict[str, list[SynthesizedSkill]] = {
        k: [] for k in
        ("languages", "frameworks", "databases", "cloud", "tools", "practices", "domains")
    }

    # Skill-section terms
    for norm_term, td in skill_acc.items():
        cat     = _ALL_VOCAB.get(norm_term)
        section = _CAT_TO_SECTION.get(cat or "", "")
        if section and section != "domains":
            buckets[section].append(SynthesizedSkill(
                name    = _display(norm_term),
                level   = td["level"] or "familiar",
                sources = list(td["sources"]),
            ))
        elif section == "domains":
            # domain-category vocab term reached via skill_acc (e.g. "devops" in resume)
            buckets["domains"].append(SynthesizedSkill(
                name    = _display(norm_term),
                level   = td["level"] or "familiar",
                sources = list(td["sources"]),
            ))

    # Domain terms (from domain_acc)
    for norm_term, td in domain_acc.items():
        display = _display(norm_term) if norm_term in _ALL_VOCAB else norm_term.title()
        buckets["domains"].append(SynthesizedSkill(
            name    = display,
            level   = td["level"] or "familiar",
            sources = list(td["sources"]),
        ))

    # Deduplicate each bucket by name (case-insensitive), keeping highest level
    sort_key = lambda s: (-_LEVEL_RANK.get(s.level, 0), s.name.lower())
    for sec in buckets:
        best: dict[str, SynthesizedSkill] = {}
        for skill in buckets[sec]:
            key = skill.name.lower()
            if key not in best or (
                _LEVEL_RANK.get(skill.level, 0) > _LEVEL_RANK.get(best[key].level, 0)
            ):
                # Merge source lists
                if key in best:
                    for src in skill.sources:
                        if src not in best[key].sources:
                            best[key].sources.append(src)
                    best[key] = SynthesizedSkill(
                        name    = best[key].name,
                        level   = skill.level,
                        sources = best[key].sources,
                    )
                else:
                    best[key] = skill
            elif key in best:
                # Same or lower level — just merge sources
                for src in skill.sources:
                    if src not in best[key].sources:
                        best[key].sources.append(src)
        buckets[sec] = sorted(best.values(), key=sort_key)

    total = sum(len(v) for v in buckets.values())

    return SynthesisResult(
        languages    = buckets["languages"],
        frameworks   = buckets["frameworks"],
        databases    = buckets["databases"],
        cloud        = buckets["cloud"],
        tools        = buckets["tools"],
        practices    = buckets["practices"],
        domains      = buckets["domains"],
        sources_used = sources_used,
        skills_found = total,
    )
