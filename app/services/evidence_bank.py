"""
app/services/evidence_bank.py — Evidence Bank: store and retrieve reusable candidate evidence.

Design principles:
  - Grounded: every item stores verbatim text from real experience. Nothing is invented.
  - Tagged:   lightweight manual tagging (skills, domains, business problems).
  - Honest:   evidence_strength distinguishes direct claims from adjacent/inferred ones.
  - No LLM:  all tagging is manual; the only automation is whitespace normalization.

Public API:
  normalize_tags(tags)                           -> list[str]
  create_item(conn, title, raw_text, ...)        -> EvidenceItem
  get_item(conn, item_id)                        -> EvidenceItem | None
  list_items(conn, source_type, strength)        -> list[EvidenceItem]
  update_item(conn, item_id, title, raw_text, …) -> EvidenceItem
  delete_item(conn, item_id)                     -> bool
  get_usable_items(conn, allowed_use, min_strength) -> list[EvidenceItem]
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from typing import Optional


# ── Valid domain values ───────────────────────────────────────────────────────

SOURCE_TYPES: frozenset[str] = frozenset({
    "resume_bullet",
    "cover_letter_fragment",
    "project_note",
    "rewrite",
    "brag_note",
    "interview_story",
    "other",
})

EVIDENCE_STRENGTHS: frozenset[str] = frozenset({
    "direct",    # candidate has done this directly and can substantiate it
    "adjacent",  # transferable experience; not identical but credible
    "inferred",  # reasonable inference; should never be presented as direct
})

ALLOWED_USE_VALUES: frozenset[str] = frozenset({
    "resume",
    "cover_letter",
    "project_repositioning",
    "interview_prep",
})

# Numeric ordering: higher = stronger
_STRENGTH_RANK: dict[str, int] = {"direct": 2, "adjacent": 1, "inferred": 0}


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class EvidenceItem:
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
    confidence:            Optional[str]
    notes:                 Optional[str]
    profile_id:            Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)


# ── Tag helpers ───────────────────────────────────────────────────────────────

def normalize_tags(tags: list[str]) -> list[str]:
    """
    Normalize a list of tags: strip whitespace, lowercase, deduplicate (preserve
    first-occurrence order), drop empty strings.

    >>> normalize_tags(['Python ', 'python', 'FASTAPI', ''])
    ['python', 'fastapi']
    """
    seen: set[str] = set()
    result: list[str] = []
    for raw in tags:
        norm = raw.strip().lower()
        if norm and norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result


def _normalize_uses(uses: list[str]) -> list[str]:
    """Same as normalize_tags but for allowed_uses (values are lowercase enums)."""
    return normalize_tags(uses)


# ── Validation ────────────────────────────────────────────────────────────────

def _validate(
    source_type:       str,
    evidence_strength: str,
    allowed_uses:      list[str],
) -> None:
    if source_type not in SOURCE_TYPES:
        raise ValueError(
            f"Invalid source_type {source_type!r}. "
            f"Must be one of: {sorted(SOURCE_TYPES)}"
        )
    if evidence_strength not in EVIDENCE_STRENGTHS:
        raise ValueError(
            f"Invalid evidence_strength {evidence_strength!r}. "
            f"Must be one of: {sorted(EVIDENCE_STRENGTHS)}"
        )
    invalid = set(allowed_uses) - ALLOWED_USE_VALUES
    if invalid:
        raise ValueError(
            f"Invalid allowed_uses values: {sorted(invalid)}. "
            f"Must be a subset of: {sorted(ALLOWED_USE_VALUES)}"
        )


# ── Row conversion ────────────────────────────────────────────────────────────

def _row_to_item(row: sqlite3.Row) -> EvidenceItem:
    return EvidenceItem(
        item_id               = row["id"],
        created_at            = row["created_at"],
        updated_at            = row["updated_at"],
        title                 = row["title"],
        raw_text              = row["raw_text"],
        source_type           = row["source_type"],
        skill_tags            = json.loads(row["skill_tags"]            or "[]"),
        domain_tags           = json.loads(row["domain_tags"]           or "[]"),
        business_problem_tags = json.loads(row["business_problem_tags"] or "[]"),
        evidence_strength     = row["evidence_strength"],
        allowed_uses          = json.loads(row["allowed_uses"]          or "[]"),
        confidence            = row["confidence"],
        notes                 = row["notes"],
        profile_id            = row["profile_id"],
    )


# ── CRUD ──────────────────────────────────────────────────────────────────────

def create_item(
    conn:                  sqlite3.Connection,
    title:                 str,
    raw_text:              str,
    source_type:           str         = "other",
    skill_tags:            list[str]  | None = None,
    domain_tags:           list[str]  | None = None,
    business_problem_tags: list[str]  | None = None,
    evidence_strength:     str         = "adjacent",
    allowed_uses:          list[str]  | None = None,
    confidence:            str | None  = None,
    notes:                 str | None  = None,
    profile_id:            int | None  = None,
) -> EvidenceItem:
    """
    Create and persist a new EvidenceItem.

    Tags are normalized (lowercased, deduped, stripped) before storage.
    Raises ValueError for invalid source_type, evidence_strength, or allowed_uses.
    """
    uses = _normalize_uses(allowed_uses or [])
    _validate(source_type, evidence_strength, uses)

    skill_t  = normalize_tags(skill_tags            or [])
    domain_t = normalize_tags(domain_tags           or [])
    biz_t    = normalize_tags(business_problem_tags or [])

    cur = conn.execute(
        """
        INSERT INTO evidence_items
               (title, raw_text, source_type,
                skill_tags, domain_tags, business_problem_tags,
                evidence_strength, allowed_uses,
                confidence, notes, profile_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title, raw_text, source_type,
            json.dumps(skill_t), json.dumps(domain_t), json.dumps(biz_t),
            evidence_strength, json.dumps(uses),
            confidence, notes, profile_id,
        ),
    )
    conn.commit()
    item = get_item(conn, cur.lastrowid)
    assert item is not None
    return item


def get_item(conn: sqlite3.Connection, item_id: int) -> Optional[EvidenceItem]:
    """Return the EvidenceItem with *item_id*, or None if not found."""
    row = conn.execute(
        "SELECT * FROM evidence_items WHERE id = ?", (item_id,)
    ).fetchone()
    return _row_to_item(row) if row else None


def list_items(
    conn:              sqlite3.Connection,
    source_type:       str | None = None,
    evidence_strength: str | None = None,
) -> list[EvidenceItem]:
    """
    Return all evidence items, newest first.

    Optional SQL-level filters:
      source_type       — exact match on source_type column
      evidence_strength — exact match on evidence_strength column
    """
    conditions: list[str] = []
    params:     list[str] = []

    if source_type is not None:
        conditions.append("source_type = ?")
        params.append(source_type)
    if evidence_strength is not None:
        conditions.append("evidence_strength = ?")
        params.append(evidence_strength)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM evidence_items {where} ORDER BY id DESC",
        params,
    ).fetchall()
    return [_row_to_item(r) for r in rows]


def update_item(
    conn:                  sqlite3.Connection,
    item_id:               int,
    title:                 str,
    raw_text:              str,
    source_type:           str        = "other",
    skill_tags:            list[str] | None = None,
    domain_tags:           list[str] | None = None,
    business_problem_tags: list[str] | None = None,
    evidence_strength:     str        = "adjacent",
    allowed_uses:          list[str] | None = None,
    confidence:            str | None = None,
    notes:                 str | None = None,
) -> EvidenceItem:
    """
    Replace all mutable fields of an existing EvidenceItem.
    Raises ValueError if item_id does not exist or if values are invalid.
    """
    uses = _normalize_uses(allowed_uses or [])
    _validate(source_type, evidence_strength, uses)

    row = conn.execute(
        "SELECT id FROM evidence_items WHERE id = ?", (item_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Evidence item id={item_id} not found.")

    skill_t  = normalize_tags(skill_tags            or [])
    domain_t = normalize_tags(domain_tags           or [])
    biz_t    = normalize_tags(business_problem_tags or [])

    conn.execute(
        """
        UPDATE evidence_items SET
            updated_at            = datetime('now'),
            title                 = ?,
            raw_text              = ?,
            source_type           = ?,
            skill_tags            = ?,
            domain_tags           = ?,
            business_problem_tags = ?,
            evidence_strength     = ?,
            allowed_uses          = ?,
            confidence            = ?,
            notes                 = ?
        WHERE id = ?
        """,
        (
            title, raw_text, source_type,
            json.dumps(skill_t), json.dumps(domain_t), json.dumps(biz_t),
            evidence_strength, json.dumps(uses),
            confidence, notes,
            item_id,
        ),
    )
    conn.commit()
    item = get_item(conn, item_id)
    assert item is not None
    return item


def delete_item(conn: sqlite3.Connection, item_id: int) -> bool:
    """
    Delete the EvidenceItem with *item_id*.
    Returns True if a row was deleted, False if it did not exist.
    """
    cur = conn.execute("DELETE FROM evidence_items WHERE id = ?", (item_id,))
    conn.commit()
    return cur.rowcount > 0


# ── Integration retrieval ─────────────────────────────────────────────────────

def get_usable_items(
    conn:         sqlite3.Connection,
    allowed_use:  str | None = None,
    min_strength: str | None = None,
) -> list[EvidenceItem]:
    """
    Return evidence items filtered for a specific downstream use-case.

    Args:
        allowed_use:  If given, only items with this value in their allowed_uses
                      list are returned (e.g. "resume", "cover_letter").
        min_strength: If given, only items whose evidence_strength rank is >= the
                      rank of min_strength are returned.
                      Rank: direct(2) > adjacent(1) > inferred(0).

    This is the primary hook for resume_tailor / cover_letter to pull relevant
    evidence without querying the DB directly.
    """
    items = list_items(conn)

    if allowed_use is not None:
        items = [i for i in items if allowed_use in i.allowed_uses]

    if min_strength is not None:
        min_rank = _STRENGTH_RANK.get(min_strength, 0)
        items = [
            i for i in items
            if _STRENGTH_RANK.get(i.evidence_strength, 0) >= min_rank
        ]

    return items
