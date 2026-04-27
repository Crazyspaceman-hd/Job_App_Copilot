"""
app/services/extractor.py -- Deterministic, rule-based job-description parser.

Design contract:
  - Deterministic: same raw text always produces the same ExtractionResult.
  - Honest: partial structure produces partial output + notes, not fabricated structure.
  - Modular: every logical step is its own function; nothing is buried in one big routine.
  - No LLM: pure regex, vocabulary lookup, and heuristics.

Output structure (ExtractionResult):
  required_skills        -- vocabulary terms from the requirements section
  preferred_skills       -- vocabulary terms from the preferred / nice-to-have section
  responsibilities       -- raw bullet text from responsibilities section
  years_of_experience    -- {min, max, raw} or None
  education_requirements -- short normalised phrases
  domain_requirements    -- domain-category vocabulary terms
  seniority              -- junior | mid | senior | staff | principal | unknown
  logistics_constraints  -- {remote_policy, no_sponsorship, clearance_required,
                             relocation_required, other}
  ats_keywords           -- all vocabulary terms found in the full text
  extraction_confidence  -- high | medium | low
  extraction_notes       -- list of strings explaining ambiguity or missing structure

Persistence:
  persist_extraction(conn, result)  -> extraction_run_id
  load_latest_extraction(conn, job_id) -> ExtractionResult | None
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Optional

# Import shared vocabulary utilities from scorer.
# Dependency direction: extractor -> scorer (scorer does NOT import extractor).
from app.services.scorer import (
    _ALL_VOCAB,
    _extract_vocab_terms,
    _normalize,
    _SENIORITY_LEVELS,
    _SENIORITY_TITLE_PATTERNS,
    _YOE_PATTERN,
)


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ExtractionResult:
    job_id:                 int
    required_skills:        list[str]   = field(default_factory=list)
    preferred_skills:       list[str]   = field(default_factory=list)
    responsibilities:       list[str]   = field(default_factory=list)
    years_of_experience:    Optional[dict] = None   # {min, max, raw}
    education_requirements: list[str]   = field(default_factory=list)
    domain_requirements:    list[str]   = field(default_factory=list)
    seniority:              str         = "unknown"
    logistics_constraints:  dict        = field(default_factory=dict)
    ats_keywords:           list[str]   = field(default_factory=list)
    extraction_confidence:  str         = "low"
    extraction_notes:       list[str]   = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ExtractionResult":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ── Section header patterns ───────────────────────────────────────────────────
# Each pattern matches a section header on its own line (with optional colon).
# Groups are checked with re.MULTILINE + re.IGNORECASE.

_FLAGS = re.IGNORECASE | re.MULTILINE | re.VERBOSE

_SECTION_PATTERNS: dict[str, re.Pattern] = {
    "must_have": re.compile(r"""
        ^[ \t]*
        (?:
            requirements?           |
            required\s+qualifications?  |
            basic\s+qualifications?     |
            minimum\s+qualifications?   |
            must[- ]have                |
            you\s+must\s+have           |
            what\s+you.{0,4}ll\s+need   |
            what\s+we.{0,4}re?\s+looking\s+for |
            what\s+you\s+bring          |
            required\s+skills?          |
            key\s+requirements?
        )
        [ \t]*:?[ \t]*$
    """, _FLAGS),

    "nice_to_have": re.compile(r"""
        ^[ \t]*
        (?:
            preferred\s+qualifications? |
            preferred                   |
            nice[- ]to[- ]have          |
            bonus                       |
            plus                        |
            desirable                   |
            additional\s+qualifications?|
            what\s+would\s+be\s+great   |
            good\s+to\s+have            |
            preferred\s+skills?
        )
        [ \t]*:?[ \t]*$
    """, _FLAGS),

    "responsibilities": re.compile(r"""
        ^[ \t]*
        (?:
            responsibilities?           |
            what\s+you.{0,4}ll\s+do     |
            what\s+you.{0,4}ll\s+be\s+doing |
            what\s+you.{0,4}re\s+expected\s+to\s+do |
            your\s+role                 |
            in\s+this\s+role            |
            the\s+role                  |
            key\s+responsibilities?     |
            your\s+responsibilities?    |
            day[- ]to[- ]day            |
            duties
        )
        [ \t]*:?[ \t]*$
    """, _FLAGS),

    "about": re.compile(r"""
        ^[ \t]*
        (?:
            about\s+the\s+role          |
            about\s+the\s+job           |
            about\s+the\s+position      |
            job\s+description           |
            overview                    |
            job\s+summary               |
            position\s+summary          |
            about\s+us                  |
            the\s+opportunity
        )
        [ \t]*:?[ \t]*$
    """, _FLAGS),
}

# ── Bullet extraction ─────────────────────────────────────────────────────────

_BULLET_LINE = re.compile(
    r"^[ \t]*(?:[-*+]|[•·]|\d+[.)]) [ \t]*(.+)$",
    re.MULTILINE,
)

# ── Education patterns ────────────────────────────────────────────────────────

_EDUCATION_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bph\.?d\.?(?:\s+in\s+[\w\s,/&-]{3,40})?", re.I),
    re.compile(r"\bm\.?s\.?(?:\s+in\s+[\w\s,/&-]{3,40})?", re.I),
    re.compile(r"\bmaster.?s?\s+(?:degree\s+)?(?:in\s+)?[\w\s,/&-]{3,40}", re.I),
    re.compile(r"\b(?:b\.?s\.?|b\.?a\.?|b\.?eng\.?)\s+(?:degree\s+)?(?:in\s+)?[\w\s,/&-]{3,40}", re.I),
    re.compile(r"\bbachelor.?s?\s+(?:degree\s+)?(?:in\s+)?[\w\s,/&-]{3,40}", re.I),
    re.compile(r"\bmba\b", re.I),
    re.compile(r"\b(?:undergraduate|graduate)\s+degree(?:\s+in\s+[\w\s,/&-]{3,40})?", re.I),
    re.compile(r"\bdegree\s+in\s+[\w\s,/&-]{3,40}", re.I),
    re.compile(r"\bcomputer\s+science\b|\bsoftware\s+engineering\b|\bcomputer\s+engineering\b", re.I),
    re.compile(r"\brelated\s+(?:technical\s+)?field\b", re.I),
    re.compile(r"\bquantitative\s+field\b", re.I),
]

_EDU_NOISE = re.compile(r"\b(and|or|with|a|the|an|of|in|for|is|to|that|will|you|we|our|this|have|has)\b\s*$", re.I)

# ── Logistics patterns ────────────────────────────────────────────────────────

_LOGISTICS_PATTERNS = {
    "no_sponsorship": re.compile(
        r"no\s+(?:visa\s+)?sponsorship"
        r"|must\s+be\s+(?:authorized|eligible|legally\s+authorized)\s+to\s+work"
        r"|must\s+be\s+(?:a\s+)?us\s*(?:citizen|national)"
        r"|cannot\s+(?:provide|offer|sponsor)\s+(?:visa|work\s+visa|sponsorship)"
        r"|work\s+authorization\s+(?:required|needed)"
        r"|not\s+able\s+to\s+(?:provide|offer)\s+(?:visa\s+)?sponsorship",
        re.I,
    ),
    "clearance_required": re.compile(
        r"security\s+clearance|top\s+secret|ts/sci|secret\s+clearance|public\s+trust",
        re.I,
    ),
    "relocation_required": re.compile(
        r"must\s+(?:be\s+(?:willing\s+to\s+)?)?relocat"
        r"|relocation\s+(?:is\s+)?required"
        r"|local\s+candidates?\s+only"
        r"|must\s+be\s+(?:located|based)\s+in",
        re.I,
    ),
}

_REMOTE_PATTERNS_ORDERED = [
    (re.compile(r"\bfully[- ]remote\b",  re.I), "remote"),
    (re.compile(r"\bremote[- ]first\b",  re.I), "remote"),
    (re.compile(r"\b100%\s*remote\b",    re.I), "remote"),
    (re.compile(r"\bhybrid\b",           re.I), "hybrid"),
    (re.compile(r"\bin[- ]office\b",     re.I), "onsite"),
    (re.compile(r"\bon[- ]site\b",       re.I), "onsite"),
    (re.compile(r"\bonsite\b",           re.I), "onsite"),
    (re.compile(r"\bremote\b",           re.I), "remote"),
]


# ── Public API ────────────────────────────────────────────────────────────────

def extract(job_id: int, raw_text: str) -> ExtractionResult:
    """
    Parse *raw_text* into structured extraction data.
    Always returns a complete ExtractionResult; ambiguity goes into extraction_notes.
    """
    sections = _split_sections(raw_text)
    notes: list[str] = []

    # -- Required skills -------------------------------------------------------
    required_skills: list[str] = []
    if sections.get("must_have"):
        required_skills = _skills_from_section(sections["must_have"])
    if not required_skills:
        notes.append(
            "No requirements section found or no recognisable skills extracted from it; "
            "required_skills derived from full-text vocabulary scan (lower precision)."
        )
        required_skills = _extract_vocab_terms(raw_text)

    # -- Preferred skills ------------------------------------------------------
    preferred_skills: list[str] = []
    if sections.get("nice_to_have"):
        preferred_skills = _skills_from_section(sections["nice_to_have"])
    if not preferred_skills and not sections.get("nice_to_have"):
        notes.append("No preferred/nice-to-have section found.")

    # -- Responsibilities -------------------------------------------------------
    responsibilities: list[str] = []
    if sections.get("responsibilities"):
        responsibilities = _extract_bullets(sections["responsibilities"])
    else:
        notes.append("No responsibilities section found.")

    # -- Years of experience ---------------------------------------------------
    yoe = _extract_yoe(raw_text)
    if yoe is None:
        notes.append("Years of experience not explicitly stated.")

    # -- Education -------------------------------------------------------------
    education = _extract_education(raw_text)
    if not education:
        notes.append("Education requirements not explicitly stated.")

    # -- Domain requirements ---------------------------------------------------
    domains = _extract_domains(raw_text)

    # -- Seniority -------------------------------------------------------------
    seniority, sen_note = _infer_seniority_str(raw_text)
    if seniority == "unknown":
        notes.append(sen_note or "Seniority level could not be inferred.")
    elif sen_note:
        notes.append(sen_note)

    # -- Logistics -------------------------------------------------------------
    logistics = _extract_logistics(raw_text)

    # -- ATS keywords ----------------------------------------------------------
    ats_keywords = _extract_vocab_terms(raw_text)

    # -- Confidence ------------------------------------------------------------
    confidence = _assess_confidence(sections, required_skills, raw_text)

    return ExtractionResult(
        job_id                = job_id,
        required_skills       = required_skills,
        preferred_skills      = preferred_skills,
        responsibilities      = responsibilities,
        years_of_experience   = yoe,
        education_requirements= education,
        domain_requirements   = domains,
        seniority             = seniority,
        logistics_constraints = logistics,
        ats_keywords          = ats_keywords,
        extraction_confidence = confidence,
        extraction_notes      = notes,
    )


def persist_extraction(conn: sqlite3.Connection, result: ExtractionResult) -> int:
    """
    Write extraction results to the database.

    Clears any previous extraction for this job first (re-extraction replaces old data).
    Returns the new extraction_run_id.
    """
    # Purge old data for this job
    conn.execute("DELETE FROM extracted_requirements WHERE job_id = ?", (result.job_id,))
    conn.execute("DELETE FROM extraction_runs WHERE job_id = ?", (result.job_id,))

    # Write individual requirement rows
    _insert_req_rows(conn, result)

    # Write run metadata
    yoe = result.years_of_experience or {}
    cur = conn.execute(
        """INSERT INTO extraction_runs
           (job_id, extraction_confidence, extraction_notes_json, seniority,
            min_years_experience, max_years_experience,
            logistics_json, ats_keywords_json, summary_json)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            result.job_id,
            result.extraction_confidence,
            json.dumps(result.extraction_notes),
            result.seniority,
            yoe.get("min"),
            yoe.get("max"),
            json.dumps(result.logistics_constraints),
            json.dumps(result.ats_keywords),
            json.dumps(result.to_dict()),
        ),
    )
    conn.commit()
    return cur.lastrowid


def load_latest_extraction(
    conn: sqlite3.Connection, job_id: int
) -> Optional[ExtractionResult]:
    """
    Return the most recent ExtractionResult for *job_id*, or None if none exists.
    Loads from the summary_json blob for efficiency.
    """
    row = conn.execute(
        "SELECT summary_json FROM extraction_runs WHERE job_id = ? ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if not row or not row["summary_json"]:
        return None
    try:
        return ExtractionResult.from_dict(json.loads(row["summary_json"]))
    except (json.JSONDecodeError, TypeError, KeyError):
        return None


# ── Section splitting ─────────────────────────────────────────────────────────

def _split_sections(text: str) -> dict[str, str]:
    """
    Find all recognised section headers, then assign the text between each pair
    of consecutive headers to the first header's section type.

    Returns dict: section_type -> joined text content.
    Unknown / duplicate sections are merged under their first-matched type.
    """
    # Collect all matches across all patterns
    hits: list[tuple[int, int, str]] = []   # (start, end, section_type)
    for sec_type, pattern in _SECTION_PATTERNS.items():
        for m in pattern.finditer(text):
            hits.append((m.start(), m.end(), sec_type))

    if not hits:
        return {}  # no recognisable structure

    hits.sort(key=lambda x: x[0])  # sort by position in text

    result: dict[str, str] = {}
    for i, (_, hdr_end, sec_type) in enumerate(hits):
        next_hdr_start = hits[i + 1][0] if i + 1 < len(hits) else len(text)
        content = text[hdr_end:next_hdr_start].strip()
        if not content:
            continue
        # Merge repeated sections (e.g. two requirements-style headers)
        if sec_type in result:
            result[sec_type] = result[sec_type] + "\n" + content
        else:
            result[sec_type] = content

    return result


# ── Bullet extraction ─────────────────────────────────────────────────────────

def _extract_bullets(section_text: str) -> list[str]:
    """
    Return a list of bullet content strings from a section.
    Falls back to treating non-empty lines as implicit bullets when no markers are found.
    Filters out very short fragments (< 8 chars) and lines that look like sub-headers.
    """
    bullets = [m.group(1).strip() for m in _BULLET_LINE.finditer(section_text)]

    if not bullets:
        # Fallback: non-empty lines
        bullets = [
            line.strip()
            for line in section_text.splitlines()
            if line.strip() and len(line.strip()) >= 8
        ]

    # Filter lines that look like sub-headers (short, ends with colon)
    return [b for b in bullets if b and not (len(b) < 30 and b.rstrip().endswith(":"))]


def _normalize_bullet(bullet: str) -> str:
    """Strip common trailing qualifiers from a bullet before skill extraction."""
    # e.g. "Strong experience with Python (3+ years)" -> "Python (3+ years)"
    bullet = re.sub(r"^(?:strong|solid|proven|hands[- ]on|extensive|deep)\s+", "", bullet, flags=re.I)
    bullet = re.sub(r"^(?:experience|proficiency|knowledge|familiarity|expertise)\s+(?:with|in|of)\s+", "", bullet, flags=re.I)
    return bullet.strip()


def _skills_from_section(section_text: str) -> list[str]:
    """
    Extract vocabulary terms from a section's bullets.
    Normalises each bullet before vocab-matching to reduce noise.
    Returns a deduplicated ordered list.
    """
    skills: list[str] = []
    seen: set[str] = set()

    for bullet in _extract_bullets(section_text):
        cleaned = _normalize_bullet(bullet)
        for term in _extract_vocab_terms(cleaned):
            if term not in seen:
                skills.append(term)
                seen.add(term)

    return skills


# ── Years of experience ───────────────────────────────────────────────────────

_YOE_RANGE  = re.compile(r"(\d+)\s*[-\u2013to]+\s*(\d+)\s*years?", re.I)
_YOE_PLUS   = re.compile(r"(\d+)\+\s*years?", re.I)
_YOE_ATLEAST= re.compile(r"(?:at\s+least|minimum\s+(?:of\s+)?)\s*(\d+)\s*years?", re.I)
_YOE_SIMPLE = re.compile(r"(\d+)\s*years?\s+(?:of\s+)?(?:professional\s+)?experience", re.I)


def _extract_yoe(text: str) -> Optional[dict]:
    """
    Find the primary years-of-experience requirement.

    Returns {min: int, max: int|None, raw: str} or None.
    When multiple values are present, returns the highest minimum (most conservative).
    """
    candidates: list[tuple[int, Optional[int], str]] = []

    for m in _YOE_RANGE.finditer(text):
        lo, hi = int(m.group(1)), int(m.group(2))
        candidates.append((lo, hi, m.group(0)))

    for m in _YOE_PLUS.finditer(text):
        candidates.append((int(m.group(1)), None, m.group(0)))

    for m in _YOE_ATLEAST.finditer(text):
        candidates.append((int(m.group(1)), None, m.group(0)))

    for m in _YOE_SIMPLE.finditer(text):
        # Only add if not already captured by broader patterns above.
        # Check both min and max values to avoid re-adding the upper bound of
        # a range that was already captured (e.g. "5" from "3-5 years").
        n = int(m.group(1))
        if not any(c[0] == n or c[1] == n for c in candidates):
            candidates.append((n, None, m.group(0)))

    if not candidates:
        return None

    # Use the candidate with the highest minimum (most demanding requirement)
    best = max(candidates, key=lambda c: c[0])
    return {"min": best[0], "max": best[1], "raw": best[2]}


# ── Education ─────────────────────────────────────────────────────────────────

_EDU_MAX_LEN = 80   # cap match length to avoid sprawling captures

def _extract_education(text: str) -> list[str]:
    """
    Return a deduplicated list of education requirement phrases.
    Keeps only meaningful matches; strips trailing stopwords.
    """
    found: list[str] = []
    seen:  set[str]  = set()

    for pat in _EDUCATION_PATTERNS:
        for m in pat.finditer(text):
            phrase = m.group(0)[:_EDU_MAX_LEN].strip()
            phrase = _EDU_NOISE.sub("", phrase).strip()
            norm   = phrase.lower()
            if len(norm) >= 4 and norm not in seen:
                found.append(phrase)
                seen.add(norm)

    return found


# ── Domain requirements ───────────────────────────────────────────────────────

def _extract_domains(text: str) -> list[str]:
    """Return domain-category vocabulary terms found in *text*."""
    return [
        t for t in _extract_vocab_terms(text)
        if _ALL_VOCAB.get(t) == "domain"
    ]


# ── Seniority inference ───────────────────────────────────────────────────────

def _infer_seniority_str(text: str) -> tuple[str, str]:
    """
    Return (seniority_string, note).
    seniority_string is one of: junior | mid | senior | staff | principal | unknown
    note is empty string when confident, or an explanation when uncertain.
    """
    for pattern, level in _SENIORITY_TITLE_PATTERNS:
        if pattern.search(text):
            name = {v: k for k, v in _SENIORITY_LEVELS.items()}.get(level, "mid")
            return name, ""

    # Fallback: infer from years of experience
    yoe = _extract_yoe(text)
    if yoe:
        years = yoe["min"]
        if years >= 10:
            return "staff", f"Seniority inferred from YOE ({yoe['raw']}) — may be principal."
        if years >= 7:
            return "staff", f"Seniority inferred from YOE ({yoe['raw']})."
        if years >= 5:
            return "senior", f"Seniority inferred from YOE ({yoe['raw']})."
        if years >= 3:
            return "mid", f"Seniority inferred from YOE ({yoe['raw']})."
        return "junior", f"Seniority inferred from YOE ({yoe['raw']})."

    return "unknown", "No seniority title or YOE found in JD."


# ── Logistics extraction ──────────────────────────────────────────────────────

def _extract_remote_policy(text: str) -> Optional[str]:
    lower = text.lower()
    for pattern, policy in _REMOTE_PATTERNS_ORDERED:
        if pattern.search(lower):
            return policy
    return None


def _extract_logistics(text: str) -> dict:
    """
    Return a logistics constraints dict:
      remote_policy     : remote | hybrid | onsite | None
      no_sponsorship    : bool
      clearance_required: bool
      relocation_required: bool
      other             : list[str]   (free-text notes)
    """
    other: list[str] = []

    return {
        "remote_policy":      _extract_remote_policy(text),
        "no_sponsorship":     bool(_LOGISTICS_PATTERNS["no_sponsorship"].search(text)),
        "clearance_required": bool(_LOGISTICS_PATTERNS["clearance_required"].search(text)),
        "relocation_required":bool(_LOGISTICS_PATTERNS["relocation_required"].search(text)),
        "other":              other,
    }


# ── Confidence assessment ─────────────────────────────────────────────────────

def _assess_confidence(
    sections:        dict[str, str],
    required_skills: list[str],
    raw_text:        str,
) -> str:
    """
    Coarse confidence rating for the extraction itself.

    high   — clear requirements section with 3+ skills extracted
    medium — section found but few skills, OR no section but vocab scan found some
    low    — no sections found and very few skills
    """
    has_must_have = bool(sections.get("must_have"))
    n_required    = len(required_skills)

    if has_must_have and n_required >= 3:
        return "high"
    if has_must_have or n_required >= 2:
        return "medium"
    return "low"


# ── DB helpers ────────────────────────────────────────────────────────────────

def _insert_req_rows(conn: sqlite3.Connection, result: ExtractionResult) -> None:
    """Insert one extracted_requirements row per requirement item."""
    rows: list[tuple] = []

    for skill in result.required_skills:
        rows.append((result.job_id, "must_have", skill, None))
    for skill in result.preferred_skills:
        rows.append((result.job_id, "nice_to_have", skill, None))
    for resp in result.responsibilities:
        rows.append((result.job_id, "responsibility", resp[:500], None))
    for edu in result.education_requirements:
        rows.append((result.job_id, "education", edu, None))
    for dom in result.domain_requirements:
        rows.append((result.job_id, "domain", dom, None))

    conn.executemany(
        "INSERT INTO extracted_requirements (job_id, category, requirement, source_phrase) "
        "VALUES (?,?,?,?)",
        rows,
    )
