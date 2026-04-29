"""
app/services/profile_reconstruction.py — Profile Reconstruction subsystem.

Deterministically extracts structured observations and rewrite-safe claim
candidates from raw, messy professional evidence text.  All classification
is rule-based; no LLM is used.  Human review is required before anything
enters the Evidence Bank.

Design principles:
  - Grounded:       extracts only what is literally present in the input text.
  - Honest:         evidence_strength labels reflect the linguistic signals in
                    the text, not potential or aspiration.
  - Conservative:   inferred observations are never silently upgraded to direct
                    claims.  Ambiguous text defaults to "adjacent".
  - Non-destructive: nothing auto-promotes to the Evidence Bank; every item
                    requires explicit user acceptance.
  - Deterministic:  same input → same output (no randomness, no LLM calls).

Public API:
  create_source(conn, raw_text, ...)              -> RawSource
  get_source(conn, source_id)                     -> RawSource
  list_sources(conn)                              -> list[RawSource]
  delete_source(conn, source_id)                  -> bool
  run_reconstruction(conn, source_id)             -> ReconstructionResult
  get_observation(conn, obs_id)                   -> Observation
  list_observations(conn, source_id)              -> list[Observation]
  update_observation(conn, obs_id, **fields)      -> Observation
  get_claim(conn, claim_id)                       -> ClaimCandidate
  list_claims(conn, source_id)                    -> list[ClaimCandidate]
  update_claim(conn, claim_id, **fields)          -> ClaimCandidate
  promote_claim(conn, claim_id)                   -> EvidenceItem
  generate_draft_summary(conn, source_id)         -> str
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from typing import Optional

from app.services.evidence_bank import (
    ALLOWED_USE_VALUES,
    create_item as create_evidence_item,
)
from app.services.scorer import _ALL_VOCAB, _extract_vocab_terms


# ── Valid domain values ───────────────────────────────────────────────────────

PR_SOURCE_TYPES: frozenset[str] = frozenset({
    "project_note",
    "debugging_story",
    "old_resume",
    "cover_letter",
    "assignment",
    "ai_summary",
    "free_text",
    "other",
})

REVIEW_STATES: frozenset[str] = frozenset({"pending", "accepted", "rejected"})

# Map reconstruction source types to Evidence Bank source types
_EB_SOURCE_TYPE_MAP: dict[str, str] = {
    "old_resume":      "resume_bullet",
    "cover_letter":    "cover_letter_fragment",
    "project_note":    "project_note",
    "debugging_story": "project_note",
    "assignment":      "project_note",
    "ai_summary":      "other",
    "free_text":       "other",
    "other":           "other",
}

_DEFAULT_ALLOWED_USES: list[str] = sorted(ALLOWED_USE_VALUES)

# Minimum character length for a text unit to be treated as an observation
_MIN_UNIT_LEN = 12


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class RawSource:
    id:          int
    created_at:  str
    updated_at:  str
    title:       str
    raw_text:    str
    source_type: str
    label:       Optional[str]


@dataclass
class Observation:
    id:                    int
    created_at:            str
    updated_at:            str
    source_id:             int
    text:                  str
    skill_tags:            list[str]
    domain_tags:           list[str]
    business_problem_tags: list[str]
    evidence_strength:     str
    confidence:            str
    allowed_uses:          list[str]
    review_state:          str
    notes:                 Optional[str]


@dataclass
class ClaimCandidate:
    id:               int
    created_at:       str
    updated_at:       str
    observation_id:   int
    text:             str
    framing:          str           # "direct" | "adjacent" | "inferred"
    evidence_basis:   Optional[str] # excerpt from original text
    review_state:     str
    promoted_item_id: Optional[int] # set after promotion to Evidence Bank


@dataclass
class ReconstructionResult:
    source_id:         int
    observations:      list[Observation]
    claims:            list[ClaimCandidate]
    draft_summary:     str
    observation_count: int
    claim_count:       int


# ── Extraction patterns ───────────────────────────────────────────────────────

# Strong past-tense ownership verbs → evidence that the candidate *did* something
_DIRECT_VERBS = re.compile(
    r"\b("
    r"built|created|designed|led|implemented|shipped|deployed|architected|"
    r"wrote|developed|owned|launched|optimized|reduced|improved|increased|"
    r"drove|managed|delivered|produced|fixed|solved|automated|integrated|"
    r"migrated|refactored|configured|established|engineered|directed|"
    r"spearheaded|founded|authored|generated|executed|maintained|operated|"
    r"administered|oversaw|supervised|trained|scaled|achieved|accomplished|"
    r"secured|won|earned|grew|expanded|redesigned|rewrote|replaced|rebuilt|"
    r"simplified|tested|validated|audited|analyzed|bootstrapped|delegated|"
    r"documented|enabled|enforced|evaluated|identified|introduced|"
    r"investigated|modelled|planned|presented|prioritized|released|"
    r"researched|resolved|standardized|streamlined|structured|transformed|"
    r"upgraded|onboarded|mentored|coached|converted|classified|extracted|"
    r"cleaned|queried|ingested|parsed|rendered|deployed|containerized|"
    r"orchestrated|provisioned|secured|hardened|benchmarked|profiled"
    r")\b",
    re.IGNORECASE,
)

# Collaborative / supporting verbs → candidate was *involved* but not the owner
_ADJACENT_VERBS = re.compile(
    r"\b("
    r"helped|assisted|supported|contributed|collaborated|participated|"
    r"co-built|co-designed|co-developed|co-wrote|paired|facilitated|"
    r"aided|joined|advised|consulted|coordinated"
    r")\b"
    r"|worked\s+(?:with|on|alongside|under)"
    r"|was\s+(?:involved|part\s+of|a\s+member)"
    r"|took\s+part"
    r"|played\s+a\s+role"
    r"|part\s+of\s+(?:a\s+)?team",
    re.IGNORECASE,
)

# Learning / aspiration signals → candidate *wants* to or is *starting* to
_INFERRED_SIGNALS = re.compile(
    r"\b("
    r"learning|exploring|studying|interested\s+in|"
    r"planning\s+to|hoping\s+to|want\s+to|would\s+like|"
    r"beginning\s+to|starting\s+to|getting\s+into|"
    r"teaching\s+myself|new\s+to|excited\s+about|"
    r"curious\s+about|reading\s+about|picking\s+up"
    r")\b"
    r"|familiar\s+with"
    r"|exposure\s+to"
    r"|self.stud",
    re.IGNORECASE,
)

# Metric / outcome signals → raises confidence
_METRIC_SIGNAL = re.compile(
    r"\d+\s*"
    r"(%|x\b|X\b|\$|k\b|K\b|ms\b|hrs?\b|days?\b|weeks?\b|months?\b|"
    r"years?\b|users?\b|customers?\b|requests?\b|tickets?\b|bugs?\b|"
    r"services?\b|endpoints?\b|nodes?\b|replicas?\b|engineers?\b)",
    re.IGNORECASE,
)

# Any 2+ digit number (weaker confidence signal)
_NUMBER_SIGNAL = re.compile(r"\b\d{2,}\b")

# Hedging language → lowers confidence
_HEDGING = re.compile(
    r"\b("
    r"maybe|might|could\s+have|not\s+sure|I\s+think|probably|"
    r"sometimes|occasionally|somewhat|sort\s+of|kind\s+of|"
    r"not\s+really|a\s+little|slightly|vaguely"
    r")\b",
    re.IGNORECASE,
)

# Lines to skip entirely
_SKIP_LINE = re.compile(
    r"^("
    r"\s*$"                                 # blank
    r"|\s*[-–—]{3,}\s*$"                   # horizontal rule
    r"|\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"  # date 01/01/2020
    r"|[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+" # email
    r"|\+?[\d\s\-().]{7,20}$"             # phone number
    r"|https?://"                           # URL
    r"|[A-Z ]{4,}:?\s*$"                   # all-caps heading-only lines
    r")"
)

# Bullet markers to strip
_BULLET_MARKER = re.compile(r"^[\t ]*[-*+•·◦○▪▸►»–—>‣⁃●◉]\s+")


# ── Text splitting ────────────────────────────────────────────────────────────

def _split_into_units(text: str) -> list[str]:
    """
    Split raw evidence text into candidate observation units.

    1. Split on newlines.
    2. Strip bullet markers.
    3. For long lines (>200 chars), further split on sentence boundaries.
    4. If very few units result, fall back to sentence-boundary splitting of
       the whole text.
    5. Filter skippable and too-short units.
    """
    raw_lines = text.splitlines()
    units: list[str] = []

    for line in raw_lines:
        s = line.strip()
        if not s or len(s) < _MIN_UNIT_LEN or _SKIP_LINE.match(s):
            continue

        # Strip bullet markers
        s = _BULLET_MARKER.sub("", s).strip()
        if len(s) < _MIN_UNIT_LEN:
            continue

        if len(s) > 200:
            # Split long lines on sentence boundaries
            sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z\d])", s)
            for sent in sentences:
                sent = sent.strip()
                if len(sent) >= _MIN_UNIT_LEN and not _SKIP_LINE.match(sent):
                    units.append(sent)
        else:
            units.append(s)

    # Fallback: if nearly nothing was extracted, try global sentence splitting
    if len(units) < 2:
        sentences = re.split(r"(?<=[.!?])\s+", text)
        seen = set(units)
        for sent in sentences:
            sent = sent.strip()
            if len(sent) >= _MIN_UNIT_LEN and not _SKIP_LINE.match(sent) and sent not in seen:
                units.append(sent)
                seen.add(sent)

    return units


# ── Classification helpers ────────────────────────────────────────────────────

def _detect_strength(text: str) -> str:
    """
    Classify evidence strength as 'direct', 'adjacent', or 'inferred'.

    Priority order (most conservative wins): inferred > adjacent > direct.
    Ambiguous text (no matching verb) defaults to 'adjacent'.
    """
    if _INFERRED_SIGNALS.search(text):
        return "inferred"
    if _ADJACENT_VERBS.search(text):
        return "adjacent"
    if _DIRECT_VERBS.search(text):
        return "direct"
    return "adjacent"  # conservative default


def _detect_confidence(text: str, strength: str) -> str:
    """
    Classify confidence as 'high', 'medium', or 'low'.

    high:   specific metric or outcome present
    medium: clear action or context, no metric
    low:    hedging language, inferred strength, or text is very short
    """
    if strength == "inferred" or _HEDGING.search(text):
        return "low"
    if _METRIC_SIGNAL.search(text):
        return "high"
    if _NUMBER_SIGNAL.search(text) or _DIRECT_VERBS.search(text):
        return "medium"
    if len(text) < 40:
        return "low"
    return "medium"


def _extract_tags(text: str) -> tuple[list[str], list[str]]:
    """
    Return (skill_tags, domain_tags) extracted from *text* using the vocab.

    skill_tags:  language/framework/database/cloud/tool vocab terms
    domain_tags: domain/practice vocab terms
    """
    terms = _extract_vocab_terms(text)
    skill_cats = {"language", "framework", "database", "cloud", "tool"}
    domain_cats = {"domain", "practice"}
    skills = [t for t in terms if _ALL_VOCAB.get(t) in skill_cats]
    domains = [t for t in terms if _ALL_VOCAB.get(t) in domain_cats]
    return skills, domains


def _make_claim(obs_text: str, strength: str) -> tuple[str, str]:
    """
    Generate (claim_text, framing) from an observation unit.

    The claim text is a cleaned, framing-adjusted version of the observation.
    Framing matches strength: 'direct' | 'adjacent' | 'inferred'.
    """
    cleaned = obs_text.strip()
    # Ensure sentence starts with a capital
    if cleaned and cleaned[0].islower():
        cleaned = cleaned[0].upper() + cleaned[1:]
    # Remove trailing period so we can add one consistently
    if cleaned.endswith("."):
        cleaned = cleaned[:-1]

    if strength == "direct":
        return f"{cleaned}.", "direct"

    if strength == "adjacent":
        # Only prefix if text doesn't already open with a collaborative verb
        if not re.match(
            r"\b(Helped|Assisted|Supported|Contributed|Collaborated|"
            r"Worked\s+(?:with|on)|Participated|Facilitated|Co-)",
            cleaned, re.IGNORECASE,
        ):
            return f"Contributed to: {cleaned}.", "adjacent"
        return f"{cleaned}.", "adjacent"

    # inferred
    if not re.match(
        r"\b(Learning|Exploring|Developing|Building|Growing|Gaining)\b",
        cleaned, re.IGNORECASE,
    ):
        return f"Developing familiarity with: {cleaned}.", "inferred"
    return f"{cleaned}.", "inferred"


# ── Row converters ────────────────────────────────────────────────────────────

def _row_to_source(row: sqlite3.Row) -> RawSource:
    return RawSource(
        id=row["id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        title=row["title"],
        raw_text=row["raw_text"],
        source_type=row["source_type"],
        label=row["label"],
    )


def _row_to_observation(row: sqlite3.Row) -> Observation:
    return Observation(
        id=row["id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        source_id=row["source_id"],
        text=row["text"],
        skill_tags=json.loads(row["skill_tags"] or "[]"),
        domain_tags=json.loads(row["domain_tags"] or "[]"),
        business_problem_tags=json.loads(row["business_problem_tags"] or "[]"),
        evidence_strength=row["evidence_strength"],
        confidence=row["confidence"],
        allowed_uses=json.loads(row["allowed_uses"] or "[]"),
        review_state=row["review_state"],
        notes=row["notes"],
    )


def _row_to_claim(row: sqlite3.Row) -> ClaimCandidate:
    return ClaimCandidate(
        id=row["id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        observation_id=row["observation_id"],
        text=row["text"],
        framing=row["framing"],
        evidence_basis=row["evidence_basis"],
        review_state=row["review_state"],
        promoted_item_id=row["promoted_item_id"],
    )


# ── Source CRUD ───────────────────────────────────────────────────────────────

def create_source(
    conn: sqlite3.Connection,
    raw_text: str,
    source_type: str = "free_text",
    title: str = "",
    label: Optional[str] = None,
) -> RawSource:
    """Create and persist a new raw evidence source."""
    if source_type not in PR_SOURCE_TYPES:
        raise ValueError(
            f"source_type {source_type!r} must be one of {sorted(PR_SOURCE_TYPES)}"
        )
    cur = conn.execute(
        "INSERT INTO pr_sources (title, raw_text, source_type, label) VALUES (?, ?, ?, ?)",
        (title.strip(), raw_text, source_type, label),
    )
    conn.commit()
    return get_source(conn, cur.lastrowid)


def get_source(conn: sqlite3.Connection, source_id: int) -> RawSource:
    row = conn.execute(
        "SELECT * FROM pr_sources WHERE id = ?", (source_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Source {source_id} not found")
    return _row_to_source(row)


def list_sources(conn: sqlite3.Connection) -> list[RawSource]:
    rows = conn.execute(
        "SELECT * FROM pr_sources ORDER BY id DESC"
    ).fetchall()
    return [_row_to_source(r) for r in rows]


def delete_source(conn: sqlite3.Connection, source_id: int) -> bool:
    """Delete source and cascade-delete all observations and claims."""
    cur = conn.execute("DELETE FROM pr_sources WHERE id = ?", (source_id,))
    conn.commit()
    return cur.rowcount > 0


# ── Reconstruction run ────────────────────────────────────────────────────────

def run_reconstruction(
    conn: sqlite3.Connection,
    source_id: int,
) -> ReconstructionResult:
    """
    Extract observations and claim candidates from a source's raw_text.

    Existing observations (and their claims) for this source are replaced
    on each run — the tables use ON DELETE CASCADE.

    Returns a ReconstructionResult with all extracted data.
    """
    source = get_source(conn, source_id)

    # Replace previous results for this source
    conn.execute(
        "DELETE FROM pr_observations WHERE source_id = ?", (source_id,)
    )
    conn.commit()

    units = _split_into_units(source.raw_text)

    observations: list[Observation] = []
    claims: list[ClaimCandidate] = []

    for unit_text in units:
        strength   = _detect_strength(unit_text)
        confidence = _detect_confidence(unit_text, strength)
        skills, domains = _extract_tags(unit_text)
        claim_text, framing = _make_claim(unit_text, strength)

        cur = conn.execute(
            """
            INSERT INTO pr_observations
                (source_id, text, skill_tags, domain_tags, business_problem_tags,
                 evidence_strength, confidence, allowed_uses, review_state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (
                source_id, unit_text,
                json.dumps(skills), json.dumps(domains), json.dumps([]),
                strength, confidence,
                json.dumps(_DEFAULT_ALLOWED_USES),
            ),
        )
        conn.commit()
        obs = get_observation(conn, cur.lastrowid)
        observations.append(obs)

        cur = conn.execute(
            """
            INSERT INTO pr_claims
                (observation_id, text, framing, evidence_basis, review_state)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (obs.id, claim_text, framing, unit_text[:300]),
        )
        conn.commit()
        claims.append(get_claim(conn, cur.lastrowid))

    draft_summary = _build_draft_summary(observations)

    return ReconstructionResult(
        source_id=source_id,
        observations=observations,
        claims=claims,
        draft_summary=draft_summary,
        observation_count=len(observations),
        claim_count=len(claims),
    )


# ── Observation CRUD ──────────────────────────────────────────────────────────

def get_observation(conn: sqlite3.Connection, obs_id: int) -> Observation:
    row = conn.execute(
        "SELECT * FROM pr_observations WHERE id = ?", (obs_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Observation {obs_id} not found")
    return _row_to_observation(row)


def list_observations(
    conn: sqlite3.Connection,
    source_id: int,
) -> list[Observation]:
    rows = conn.execute(
        "SELECT * FROM pr_observations WHERE source_id = ? ORDER BY id ASC",
        (source_id,),
    ).fetchall()
    return [_row_to_observation(r) for r in rows]


def update_observation(
    conn: sqlite3.Connection,
    obs_id: int,
    *,
    text:                  Optional[str]       = None,
    skill_tags:            Optional[list[str]] = None,
    domain_tags:           Optional[list[str]] = None,
    business_problem_tags: Optional[list[str]] = None,
    evidence_strength:     Optional[str]       = None,
    confidence:            Optional[str]       = None,
    allowed_uses:          Optional[list[str]] = None,
    review_state:          Optional[str]       = None,
    notes:                 Optional[str]       = None,
) -> Observation:
    """Partial-update an observation; only provided fields are changed."""
    existing = get_observation(conn, obs_id)

    new_state = review_state if review_state is not None else existing.review_state
    if new_state not in REVIEW_STATES:
        raise ValueError(f"review_state must be one of {sorted(REVIEW_STATES)}")

    conn.execute(
        """
        UPDATE pr_observations SET
            updated_at            = datetime('now'),
            text                  = ?,
            skill_tags            = ?,
            domain_tags           = ?,
            business_problem_tags = ?,
            evidence_strength     = ?,
            confidence            = ?,
            allowed_uses          = ?,
            review_state          = ?,
            notes                 = ?
        WHERE id = ?
        """,
        (
            text                  if text                  is not None else existing.text,
            json.dumps(skill_tags            if skill_tags            is not None else existing.skill_tags),
            json.dumps(domain_tags           if domain_tags           is not None else existing.domain_tags),
            json.dumps(business_problem_tags if business_problem_tags is not None else existing.business_problem_tags),
            evidence_strength     if evidence_strength     is not None else existing.evidence_strength,
            confidence            if confidence            is not None else existing.confidence,
            json.dumps(allowed_uses if allowed_uses        is not None else existing.allowed_uses),
            new_state,
            notes                 if notes                 is not None else existing.notes,
            obs_id,
        ),
    )
    conn.commit()
    return get_observation(conn, obs_id)


# ── Claim CRUD ────────────────────────────────────────────────────────────────

def get_claim(conn: sqlite3.Connection, claim_id: int) -> ClaimCandidate:
    row = conn.execute(
        "SELECT * FROM pr_claims WHERE id = ?", (claim_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Claim {claim_id} not found")
    return _row_to_claim(row)


def list_claims(
    conn: sqlite3.Connection,
    source_id: int,
) -> list[ClaimCandidate]:
    """Return all claim candidates linked (via observations) to *source_id*."""
    rows = conn.execute(
        """
        SELECT c.* FROM pr_claims c
        JOIN pr_observations o ON c.observation_id = o.id
        WHERE o.source_id = ?
        ORDER BY c.id ASC
        """,
        (source_id,),
    ).fetchall()
    return [_row_to_claim(r) for r in rows]


def update_claim(
    conn: sqlite3.Connection,
    claim_id: int,
    *,
    text:         Optional[str] = None,
    framing:      Optional[str] = None,
    review_state: Optional[str] = None,
) -> ClaimCandidate:
    existing = get_claim(conn, claim_id)

    new_state = review_state if review_state is not None else existing.review_state
    if new_state not in REVIEW_STATES:
        raise ValueError(f"review_state must be one of {sorted(REVIEW_STATES)}")

    conn.execute(
        """
        UPDATE pr_claims
        SET updated_at = datetime('now'), text = ?, framing = ?, review_state = ?
        WHERE id = ?
        """,
        (
            text    if text    is not None else existing.text,
            framing if framing is not None else existing.framing,
            new_state,
            claim_id,
        ),
    )
    conn.commit()
    return get_claim(conn, claim_id)


# ── Evidence Bank promotion ───────────────────────────────────────────────────

def promote_claim(conn: sqlite3.Connection, claim_id: int):
    """
    Promote an accepted claim candidate into the Evidence Bank.

    - Creates an EvidenceItem using the claim's text and the parent
      observation's classifications.
    - Marks the claim row with the resulting item_id.

    Returns the created EvidenceItem.
    Raises ValueError if the claim is not in 'accepted' state.
    """
    claim = get_claim(conn, claim_id)
    if claim.review_state != "accepted":
        raise ValueError(
            f"Claim {claim_id} must be 'accepted' before promotion "
            f"(current state: {claim.review_state!r})."
        )

    if claim.promoted_item_id is not None:
        # Already promoted — return the existing item
        from app.services.evidence_bank import get_item
        existing = get_item(conn, claim.promoted_item_id)
        if existing:
            return existing

    obs    = get_observation(conn, claim.observation_id)
    source = get_source(conn, obs.source_id)

    eb_source_type = _EB_SOURCE_TYPE_MAP.get(source.source_type, "other")
    title          = source.title.strip() or claim.text[:60].rstrip(".")

    item = create_evidence_item(
        conn,
        title                 = title,
        raw_text              = claim.text,
        source_type           = eb_source_type,
        skill_tags            = obs.skill_tags,
        domain_tags           = obs.domain_tags,
        business_problem_tags = obs.business_problem_tags,
        evidence_strength     = obs.evidence_strength,
        allowed_uses          = obs.allowed_uses,
        confidence            = obs.confidence,
        notes=(
            f"Reconstructed from: {source.title or source.source_type}"
            f" (source id {source.id})"
        ),
    )

    conn.execute(
        "UPDATE pr_claims SET promoted_item_id = ?, updated_at = datetime('now') WHERE id = ?",
        (item.item_id, claim_id),
    )
    conn.commit()
    return item


# ── Draft summary ─────────────────────────────────────────────────────────────

def _build_draft_summary(observations: list[Observation]) -> str:
    """
    Build a plain-text draft profile summary from extracted observations.

    Uses only evidence present in the observations.  Does not invent claims.
    Conservative framing throughout.
    """
    if not observations:
        return (
            "No observations could be extracted.  "
            "Try pasting more detailed evidence — specific actions, "
            "outcomes, or work descriptions work best."
        )

    direct   = [o for o in observations if o.evidence_strength == "direct"]
    adjacent = [o for o in observations if o.evidence_strength == "adjacent"]
    inferred = [o for o in observations if o.evidence_strength == "inferred"]

    all_skills: list[str] = []
    all_domains: list[str] = []
    for obs in observations:
        all_skills.extend(obs.skill_tags)
        all_domains.extend(obs.domain_tags)

    top_skills  = [t for t, _ in Counter(all_skills).most_common(6)]
    top_domains = [t for t, _ in Counter(all_domains).most_common(3)]

    total = len(observations)
    parts: list[str] = [
        f"Extracted {total} observation{'s' if total != 1 else ''}: "
        f"{len(direct)} direct, {len(adjacent)} adjacent, {len(inferred)} inferred."
    ]

    if top_skills:
        skill_phrase = ", ".join(t.title() for t in top_skills[:5])
        parts.append(f"Technical signals present: {skill_phrase}.")

    if top_domains:
        domain_phrase = ", ".join(t.replace("_", " ").title() for t in top_domains)
        parts.append(f"Domain context: {domain_phrase}.")

    high_conf = [o for o in observations if o.confidence == "high"]
    if high_conf:
        parts.append(
            f"{len(high_conf)} observation{'s' if len(high_conf) != 1 else ''} "
            f"contain specific metrics or outcomes — "
            f"these are your strongest claim candidates."
        )

    if not direct:
        parts.append(
            "No direct-ownership signals detected.  "
            "Consider adding specifics: what did you personally build, own, or deliver?"
        )
    elif len(direct) >= 3:
        parts.append(
            f"{len(direct)} direct-ownership observations suggest "
            f"a substantive track record worth promoting."
        )

    return "  ".join(parts)


def generate_draft_summary(conn: sqlite3.Connection, source_id: int) -> str:
    """Re-generate the draft summary from current observations for a source."""
    return _build_draft_summary(list_observations(conn, source_id))
