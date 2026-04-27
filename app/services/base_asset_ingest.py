"""
app/services/base_asset_ingest.py — Ingest base resume and cover letter.

Design contract:
  - Deterministic: same raw_text → same output, every time.
  - Non-destructive: content is stored verbatim; nothing is rewritten.
  - Traceable: every bullet/fragment carries a 1-based source_line so future
    generation can cite the exact line in the original document.
  - No LLM: pure heuristics and the shared vocabulary from scorer.py.

Public API:
  parse_resume(raw_text, label)           -> ResumeResult   (no DB)
  parse_cover_letter(raw_text, label)     -> CoverLetterResult (no DB)
  ingest_resume(raw_text, conn, label)    -> ResumeResult   (parses + persists)
  ingest_cover_letter(raw_text, conn, l)  -> CoverLetterResult (parses + persists)
  load_resume(conn, resume_id)            -> ResumeResult | None
  load_cover_letter(conn, cl_id)          -> CoverLetterResult | None
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Optional

from app.services.scorer import _extract_vocab_terms


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class ResumeBullet:
    """One extracted bullet (achievement/description line) from a resume section."""
    section:     str        # canonical section name the bullet came from
    text:        str        # verbatim bullet text (never rewritten)
    skills:      list[str]  # vocab terms extracted from this bullet
    source_line: int        # 1-based line number in original raw_text


@dataclass
class ResumeSection:
    """A detected section of a resume (Experience, Education, Skills, …)."""
    name:       str                 # canonical section name
    heading:    str                 # heading text as it appeared in the document
    start_line: int                 # 1-based line number of the heading
    bullets:    list[ResumeBullet]  # bullets found within this section


@dataclass
class ResumeResult:
    """Full structured output from resume parsing / ingestion."""
    resume_id:   int                 # DB row id (0 until persisted)
    label:       str
    raw_text:    str
    sections:    list[ResumeSection]
    bullet_bank: list[ResumeBullet]  # all bullets across all sections, in order
    skills:      list[str]           # deduplicated vocab terms from full text

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CLFragment:
    """One paragraph extracted from a cover letter."""
    kind:        str   # opening | proof_point | closing
    text:        str   # verbatim paragraph text (never rewritten)
    source_line: int   # 1-based line number where the paragraph starts


@dataclass
class CoverLetterResult:
    """Full structured output from cover letter parsing / ingestion."""
    cl_id:     int
    label:     str
    raw_text:  str
    fragments: list[CLFragment]

    def to_dict(self) -> dict:
        return asdict(self)


# ── Section heading vocabulary ────────────────────────────────────────────────
# Map of canonical section names to all recognised heading variants.
# Matching is exact (case-insensitive, whitespace-normalised) after stripping
# leading `#` markers and trailing colons.

_SECTION_HEADINGS: dict[str, list[str]] = {
    "summary": [
        "summary", "professional summary", "career summary",
        "objective", "career objective", "professional objective",
        "profile", "professional profile",
        "about", "about me",
        "overview",
    ],
    "experience": [
        "experience", "work experience", "professional experience",
        "relevant experience", "employment", "employment history",
        "career history", "work history", "professional background",
        "professional history",
    ],
    "education": [
        "education", "educational background", "academic background",
        "academic history", "degrees", "schooling", "academics",
    ],
    "skills": [
        "skills", "technical skills", "core competencies", "core competency",
        "technologies", "tools", "tools & technologies",
        "tools and technologies", "expertise", "key skills",
        "technical expertise", "programming languages",
        "programming skills", "technical proficiencies",
        "technical qualifications",
    ],
    "projects": [
        "projects", "personal projects", "side projects",
        "key projects", "notable projects", "portfolio",
        "open source", "open-source", "open source projects",
    ],
    "certifications": [
        "certifications", "certification", "licenses", "license",
        "credentials", "accreditations", "accreditation",
        "professional certifications",
    ],
    "achievements": [
        "achievements", "awards", "honors", "accomplishments",
        "recognition", "honors & awards", "honors and awards",
    ],
    "publications": [
        "publications", "research", "papers", "articles",
        "presentations", "published work",
    ],
    "volunteer": [
        "volunteer", "volunteer experience", "community involvement",
        "civic activities", "leadership", "leadership & activities",
        "leadership and activities", "extracurricular activities",
    ],
    "interests": ["interests", "hobbies", "personal interests", "activities"],
    "languages":  ["languages", "spoken languages"],
    "contact":    ["contact", "contact information", "personal information"],
}

# Build flat lookup dict: normalised heading text → canonical name.
_HEADING_LOOKUP: dict[str, str] = {}
for _canon, _variants in _SECTION_HEADINGS.items():
    for _v in _variants:
        _HEADING_LOOKUP[re.sub(r"\s+", " ", _v.strip().lower())] = _canon


# ── Regex primitives ──────────────────────────────────────────────────────────

_BULLET_MARKER   = re.compile(r"^[\t ]*[-*+•·◦○▪▸►»]\s+")
_NUMBERED_MARKER = re.compile(r"^[\t ]*\d+[.)]\s+")
_UNDERLINE_LINE  = re.compile(r"^[-=*_]{3,}\s*$")
_MARKDOWN_HDR    = re.compile(r"^(#{1,3})\s+(.+)$")


# ── Heading classification ────────────────────────────────────────────────────

def _classify_heading_text(raw: str) -> str | None:
    """
    Return canonical section name for *raw* heading text, or None if unrecognised.
    Strips markdown `#` prefix, trailing colon, and surrounding whitespace.
    """
    # Strip markdown prefix
    text = _MARKDOWN_HDR.sub(lambda m: m.group(2), raw).strip()
    # Strip trailing colon / whitespace
    text = text.rstrip(":").strip()
    # Normalise internal whitespace and case
    key  = re.sub(r"\s+", " ", text).lower()
    return _HEADING_LOOKUP.get(key)


def _is_heading(line: str, next_line: str | None) -> tuple[str | None, bool]:
    """
    Determine whether *line* is a section heading.

    Returns (canonical_name | None, consume_next_line).
    consume_next_line=True when the next line is an underline to be skipped.
    """
    stripped = line.strip()
    if not stripped:
        return None, False

    # Markdown heading: # Experience  /  ## Work Experience
    m = _MARKDOWN_HDR.match(stripped)
    if m:
        name = _classify_heading_text(m.group(2).strip())
        return name, False

    # Underline-style:
    #   Experience
    #   ----------
    if (next_line is not None
            and next_line.strip()
            and _UNDERLINE_LINE.match(next_line.strip())
            and not _BULLET_MARKER.match(stripped)
            and not _NUMBERED_MARKER.match(stripped)):
        name = _classify_heading_text(stripped)
        if name:
            return name, True   # recognised heading; skip the underline line

    # Plain heading: short line that matches a known section name exactly
    if (len(stripped) <= 65
            and not _BULLET_MARKER.match(stripped)
            and not _NUMBERED_MARKER.match(stripped)):
        name = _classify_heading_text(stripped)
        if name:
            return name, False

    return None, False


# ── Section span detection ────────────────────────────────────────────────────

def _detect_section_spans(
    lines: list[str],
) -> list[tuple[str, int, int, str]]:
    """
    Find section boundaries in a list of lines.

    Returns a list of (canonical_name, content_start_idx, content_end_idx, heading_text).
    All indices are 0-based into *lines*; content_end_idx is exclusive.
    Content before the first recognised heading is labelled "header".
    """
    # (line_idx, canonical_name, heading_text, consume_next)
    boundaries: list[tuple[int, str, str, bool]] = []

    i = 0
    while i < len(lines):
        nxt  = lines[i + 1] if i + 1 < len(lines) else None
        name, consume_next = _is_heading(lines[i], nxt)
        if name:
            boundaries.append((i, name, lines[i].strip(), consume_next))
        i += 1

    if not boundaries:
        # No recognised sections — treat entire document as one block
        return [("other", 0, len(lines), "")]

    spans: list[tuple[str, int, int, str]] = []

    # Content before the first heading
    if boundaries[0][0] > 0:
        spans.append(("header", 0, boundaries[0][0], ""))

    for k, (line_idx, name, heading_text, consume_next) in enumerate(boundaries):
        # Content starts on the line after the heading (skip underline when present)
        content_start = line_idx + (2 if consume_next else 1)
        content_end   = boundaries[k + 1][0] if k + 1 < len(boundaries) else len(lines)
        spans.append((name, content_start, content_end, heading_text))

    return spans


# ── Bullet extraction ─────────────────────────────────────────────────────────

def _extract_bullet_text(line: str) -> str | None:
    """
    Return stripped text content of a bullet line, or None if not a bullet.
    Handles -, *, +, •, and numbered lists.
    """
    m = _BULLET_MARKER.match(line)
    if m:
        return line[m.end():].strip()
    m = _NUMBERED_MARKER.match(line)
    if m:
        return line[m.end():].strip()
    return None


# ── Resume parsing ────────────────────────────────────────────────────────────

def parse_resume(raw_text: str, label: str = "default") -> ResumeResult:
    """
    Parse *raw_text* into a structured ResumeResult without DB access.
    Same input always produces the same output (deterministic).
    Content is never rewritten — all text fields are verbatim.
    """
    lines = raw_text.splitlines()
    spans = _detect_section_spans(lines)

    sections:    list[ResumeSection] = []
    bullet_bank: list[ResumeBullet]  = []

    for sec_name, start_idx, end_idx, heading_text in spans:
        sec_bullets: list[ResumeBullet] = []

        for j in range(start_idx, end_idx):
            bullet_text = _extract_bullet_text(lines[j])
            if bullet_text and bullet_text.strip():
                skills = _extract_vocab_terms(bullet_text)
                b = ResumeBullet(
                    section     = sec_name,
                    text        = bullet_text,
                    skills      = skills,
                    source_line = j + 1,   # convert to 1-based
                )
                sec_bullets.append(b)
                bullet_bank.append(b)

        # Heading appears on the line before content_start
        # (or content_start - 2 for underline style, but we store the heading line)
        heading_line = max(1, start_idx)   # 1-based; the heading is just before content

        sections.append(ResumeSection(
            name       = sec_name,
            heading    = heading_text,
            start_line = heading_line,
            bullets    = sec_bullets,
        ))

    all_skills = _extract_vocab_terms(raw_text)

    return ResumeResult(
        resume_id   = 0,          # populated after DB write
        label       = label,
        raw_text    = raw_text,
        sections    = sections,
        bullet_bank = bullet_bank,
        skills      = all_skills,
    )


# ── Cover letter parsing ──────────────────────────────────────────────────────

def parse_cover_letter(raw_text: str, label: str = "default") -> CoverLetterResult:
    """
    Parse *raw_text* into a structured CoverLetterResult without DB access.
    Paragraphs are classified by position:
      first → opening, last → closing, all others → proof_point.
    Content is never rewritten.
    """
    lines = raw_text.splitlines()

    # Collect paragraphs (contiguous non-blank lines)
    paragraphs: list[tuple[str, int]] = []   # (joined_text, start_line_1based)
    current:    list[str] = []
    para_start  = 1

    for i, line in enumerate(lines, start=1):
        if line.strip():
            if not current:
                para_start = i
            current.append(line.strip())
        else:
            if current:
                paragraphs.append((" ".join(current), para_start))
                current = []

    if current:
        paragraphs.append((" ".join(current), para_start))

    n = len(paragraphs)
    fragments: list[CLFragment] = []

    for i, (text, start_line) in enumerate(paragraphs):
        if n <= 1:
            kind = "opening"
        elif i == 0:
            kind = "opening"
        elif i == n - 1:
            kind = "closing"
        else:
            kind = "proof_point"

        fragments.append(CLFragment(kind=kind, text=text, source_line=start_line))

    return CoverLetterResult(
        cl_id     = 0,
        label     = label,
        raw_text  = raw_text,
        fragments = fragments,
    )


# ── DB persistence ────────────────────────────────────────────────────────────

def persist_resume(conn: sqlite3.Connection, result: ResumeResult) -> int:
    """
    Write *result* to base_resumes and resume_bullets tables.
    Returns the new resume_id.
    """
    summary = {
        "label":         result.label,
        "section_count": len(result.sections),
        "bullet_count":  len(result.bullet_bank),
        "skill_count":   len(result.skills),
        "sections": [
            {
                "name":         s.name,
                "heading":      s.heading,
                "start_line":   s.start_line,
                "bullet_count": len(s.bullets),
            }
            for s in result.sections
        ],
        "bullet_bank": [asdict(b) for b in result.bullet_bank],
        "skills":       result.skills,
    }

    cur = conn.execute(
        """INSERT INTO base_resumes
           (label, raw_text, normalized_json, section_count, bullet_count, skill_count)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            result.label,
            result.raw_text,
            json.dumps(summary),
            len(result.sections),
            len(result.bullet_bank),
            len(result.skills),
        ),
    )
    resume_id = cur.lastrowid

    if result.bullet_bank:
        conn.executemany(
            """INSERT INTO resume_bullets
               (resume_id, section, text, skills_json, source_line)
               VALUES (?, ?, ?, ?, ?)""",
            [
                (resume_id, b.section, b.text, json.dumps(b.skills), b.source_line)
                for b in result.bullet_bank
            ],
        )

    conn.commit()
    return resume_id


def persist_cover_letter(conn: sqlite3.Connection, result: CoverLetterResult) -> int:
    """
    Write *result* to base_cover_letters and cover_letter_fragments tables.
    Returns the new cl_id.
    """
    summary = {
        "label":          result.label,
        "fragment_count": len(result.fragments),
        "fragments":      [asdict(f) for f in result.fragments],
    }

    cur = conn.execute(
        """INSERT INTO base_cover_letters
           (label, raw_text, normalized_json, fragment_count)
           VALUES (?, ?, ?, ?)""",
        (
            result.label,
            result.raw_text,
            json.dumps(summary),
            len(result.fragments),
        ),
    )
    cl_id = cur.lastrowid

    if result.fragments:
        conn.executemany(
            """INSERT INTO cover_letter_fragments
               (cover_letter_id, kind, text, source_line)
               VALUES (?, ?, ?, ?)""",
            [(cl_id, f.kind, f.text, f.source_line) for f in result.fragments],
        )

    conn.commit()
    return cl_id


def load_resume(
    conn: sqlite3.Connection, resume_id: int
) -> Optional[ResumeResult]:
    """Reload a previously-persisted ResumeResult from the DB."""
    row = conn.execute(
        "SELECT label, raw_text, normalized_json FROM base_resumes WHERE id = ?",
        (resume_id,),
    ).fetchone()
    if not row:
        return None

    data        = json.loads(row["normalized_json"])
    bullet_bank = [ResumeBullet(**b) for b in data.get("bullet_bank", [])]

    # Re-attach bullets to sections by canonical name (first-match wins for dupes)
    bullets_by_section: dict[str, list[ResumeBullet]] = {}
    for b in bullet_bank:
        bullets_by_section.setdefault(b.section, []).append(b)

    seen_sections: set[str] = set()
    sections: list[ResumeSection] = []
    for sec in data.get("sections", []):
        name = sec["name"]
        # For the first occurrence of a name, assign its bullets; subsequent
        # occurrences share from the same pool (edge case: duplicate section names)
        if name not in seen_sections:
            seen_sections.add(name)
            sec_bullets = bullets_by_section.get(name, [])
        else:
            sec_bullets = []
        sections.append(ResumeSection(
            name       = name,
            heading    = sec["heading"],
            start_line = sec["start_line"],
            bullets    = sec_bullets,
        ))

    return ResumeResult(
        resume_id   = resume_id,
        label       = row["label"],
        raw_text    = row["raw_text"],
        sections    = sections,
        bullet_bank = bullet_bank,
        skills      = data.get("skills", []),
    )


def load_cover_letter(
    conn: sqlite3.Connection, cl_id: int
) -> Optional[CoverLetterResult]:
    """Reload a previously-persisted CoverLetterResult from the DB."""
    row = conn.execute(
        "SELECT label, raw_text, normalized_json "
        "FROM base_cover_letters WHERE id = ?",
        (cl_id,),
    ).fetchone()
    if not row:
        return None

    data      = json.loads(row["normalized_json"])
    fragments = [CLFragment(**f) for f in data.get("fragments", [])]

    return CoverLetterResult(
        cl_id     = cl_id,
        label     = row["label"],
        raw_text  = row["raw_text"],
        fragments = fragments,
    )


# ── Public entry points ───────────────────────────────────────────────────────

def load_latest_base_resume(
    conn: sqlite3.Connection,
    resume_id: int | None = None,
) -> Optional[ResumeResult]:
    """
    Return the most recently ingested ResumeResult, or the one with *resume_id*.

    When *resume_id* is None, selects the row with the highest id (most recent).
    Returns None if no base resumes exist.
    """
    if resume_id is not None:
        return load_resume(conn, resume_id)

    row = conn.execute(
        "SELECT id FROM base_resumes ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return load_resume(conn, row["id"])


def load_latest_cover_letter(
    conn: sqlite3.Connection,
    cl_id: int | None = None,
) -> Optional[CoverLetterResult]:
    """
    Return the most recently ingested CoverLetterResult, or the one with *cl_id*.

    When *cl_id* is None, selects the row with the highest id (most recent).
    Returns None if no base cover letters exist.
    """
    if cl_id is not None:
        return load_cover_letter(conn, cl_id)

    row = conn.execute(
        "SELECT id FROM base_cover_letters ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return load_cover_letter(conn, row["id"])


def ingest_resume(
    raw_text: str,
    conn: sqlite3.Connection,
    label: str = "default",
) -> ResumeResult:
    """Parse *raw_text* as a resume, persist it, and return the populated result."""
    result = parse_resume(raw_text, label=label)
    result.resume_id = persist_resume(conn, result)
    return result


def ingest_cover_letter(
    raw_text: str,
    conn: sqlite3.Connection,
    label: str = "default",
) -> CoverLetterResult:
    """Parse *raw_text* as a cover letter, persist it, and return the populated result."""
    result = parse_cover_letter(raw_text, label=label)
    result.cl_id = persist_cover_letter(conn, result)
    return result
