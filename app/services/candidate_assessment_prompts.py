"""
app/services/candidate_assessment_prompts.py — Stock prompt registry for Candidate Assessments.

Design goals:
  - Deterministic: same prompt_type + prompt_version always returns the same text.
  - Conservative: master instruction enforces truthfulness over flattery.
  - Structural: fixed output schema keeps responses comparable across models/sessions.
  - No LLM orchestration: this module only produces prompt text; callers paste it.

Public API:
  get_prompt(prompt_type, version=CURRENT_VERSION) -> PromptRecord
  list_prompts()                                   -> list[PromptRecord]
  CURRENT_VERSION                                  -> str
  PROMPT_TYPES                                     -> frozenset[str]
"""

from __future__ import annotations

from dataclasses import dataclass

CURRENT_VERSION: str = "1.0"

PROMPT_TYPES = frozenset({
    "working_assessment",
    "skill_observation",
    "project_delivery_assessment",
    "growth_assessment",
})

# ── Master instruction ─────────────────────────────────────────────────────────
# Prepended to every stock prompt. Enforces the shared truthfulness contract.

_MASTER_INSTRUCTION = """\
ASSESSMENT RULES — follow these exactly, without exception:

1. TRUTHFULNESS OVER FLATTERY
   Report only what you directly observed or can reliably infer from evidence
   provided. Do not add positive spin, softening language, or unsupported praise.
   If evidence is thin, say so explicitly.

2. EVIDENCE TIERS — always label claims with the appropriate tier:
   • DIRECT   — you observed or tested this yourself, or it is documented fact.
   • ADJACENT — strongly implied by the evidence but not directly tested.
   • INFERRED — a reasonable inference; plausible but unconfirmed.
   Never present ADJACENT or INFERRED claims as DIRECT.

3. ROLE-FIT LANGUAGE — be conservative.
   Do not claim fit for roles the evidence does not specifically support.
   Distinguish between "could grow into this" and "ready now."

4. NO INFLATION
   Do not upgrade seniority levels, expand scope of impact, or generalise
   specific wins into broad competencies without direct supporting evidence.

5. STRUCTURED OUTPUT
   Always respond using the exact JSON schema shown in the prompt.
   Do not add extra fields. Do not omit required fields.
   Use null for any field where the evidence is genuinely absent.

6. SCOPE BOUNDARY
   Assess only what was described in the supplied context.
   Do not hallucinate projects, roles, skills, or outcomes not mentioned.
"""

# ── Output schema (shared across all prompt types) ─────────────────────────────

_OUTPUT_SCHEMA = """\
Respond with exactly this JSON object and nothing else — no markdown fences,
no commentary before or after:

{
  "assessment_kind":      "<one of: working_assessment | skill_observation | project_delivery_assessment | growth_assessment>",
  "confidence":           "<high | medium | low>",
  "raw_text":             "<2-5 sentence plain-English summary of the overall assessment>",
  "strengths":            ["<tag>", ...],
  "growth_areas":         ["<tag>", ...],
  "demonstrated_skills":  ["<skill tag>", ...],
  "demonstrated_domains": ["<domain tag>", ...],
  "work_style":           "<one concise phrase, or null>",
  "role_fit":             "<one concise phrase describing the role archetype this person fits now, or null>"
}

Rules for tag lists:
  - Each tag is lowercase, 1-4 words, no punctuation.
  - strengths and growth_areas describe behaviours and traits, not job titles.
  - demonstrated_skills are technical or methodological capabilities.
  - demonstrated_domains are industry/problem-space areas.
  - All lists may be empty ([]) but must not be omitted.
"""

# ── Per-type context prompts ───────────────────────────────────────────────────

_CONTEXT_PROMPTS: dict[str, str] = {

    "working_assessment": """\
TASK: Working Assessment

You are producing a holistic professional assessment of this candidate based
on the context provided below. Cover:
  - How they think and approach problems
  - How they communicate and collaborate
  - What they reliably deliver and where they struggle
  - What kind of work environment and role archetype fits them

Prioritise patterns across multiple data points over single anecdotes.
If the context is a single session or narrow slice, acknowledge that limit.

CONTEXT:
""",

    "skill_observation": """\
TASK: Skill Observation

You are producing a focused assessment of specific technical or methodological
skills demonstrated in the context below. For each skill observed:
  - State whether the evidence is DIRECT, ADJACENT, or INFERRED.
  - Note depth of demonstrated knowledge (surface familiarity vs. applied
    production use vs. expert-level understanding).
  - Do not list skills merely mentioned in passing without evidence of use.

CONTEXT:
""",

    "project_delivery_assessment": """\
TASK: Project Delivery Assessment

You are assessing how this candidate plans, executes, and delivers project work
based on the context below. Cover:
  - Problem scoping and breakdown
  - Technical decision quality (with evidence tier)
  - Execution and follow-through
  - Communication of progress and outcomes
  - What would have been done differently at a higher seniority level

If the context describes only part of a project, scope your assessment
accordingly and note what is missing.

CONTEXT:
""",

    "growth_assessment": """\
TASK: Growth Assessment

You are assessing this candidate's trajectory, learning patterns, and
development priorities based on the context below. Cover:
  - Observable growth since an earlier baseline (if available)
  - Current ceiling vs. next realistic level
  - Specific, actionable growth areas (not generic advice)
  - Risks to growth (bad habits, blind spots, context dependency)

Be direct. Growth assessments are useless if they only confirm strengths.

CONTEXT:
""",
}

# ── Data structures ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PromptRecord:
    prompt_type:    str
    version:        str
    title:          str
    description:    str
    full_text:      str   # master instruction + context prompt + output schema


_TITLES: dict[str, str] = {
    "working_assessment":            "Working Assessment",
    "skill_observation":             "Skill Observation",
    "project_delivery_assessment":   "Project Delivery Assessment",
    "growth_assessment":             "Growth Assessment",
}

_DESCRIPTIONS: dict[str, str] = {
    "working_assessment": (
        "Holistic assessment covering thinking style, collaboration, delivery patterns, "
        "and role-fit. Best used after a broad AI conversation or deep review session."
    ),
    "skill_observation": (
        "Focused technical assessment of specific skills observed in the session. "
        "Labels each skill as direct, adjacent, or inferred evidence."
    ),
    "project_delivery_assessment": (
        "Evaluates how the candidate scopes, executes, and communicates project work. "
        "Useful after reviewing a specific project or delivery arc."
    ),
    "growth_assessment": (
        "Assesses trajectory, learning patterns, and specific next-level gaps. "
        "Most useful when a prior baseline assessment exists for comparison."
    ),
}


def _build_full_text(prompt_type: str) -> str:
    context_prompt = _CONTEXT_PROMPTS[prompt_type]
    return f"{_MASTER_INSTRUCTION}\n{'-' * 72}\n\n{context_prompt}\n{'-' * 72}\n\n{_OUTPUT_SCHEMA}"


def _make_record(prompt_type: str, version: str) -> PromptRecord:
    return PromptRecord(
        prompt_type  = prompt_type,
        version      = version,
        title        = _TITLES[prompt_type],
        description  = _DESCRIPTIONS[prompt_type],
        full_text    = _build_full_text(prompt_type),
    )


# Build the registry once at import time — deterministic per version.
_REGISTRY: dict[tuple[str, str], PromptRecord] = {
    (pt, CURRENT_VERSION): _make_record(pt, CURRENT_VERSION)
    for pt in PROMPT_TYPES
}


# ── Public API ─────────────────────────────────────────────────────────────────

def get_prompt(prompt_type: str, version: str = CURRENT_VERSION) -> PromptRecord:
    """Return the PromptRecord for *prompt_type* at *version*."""
    if prompt_type not in PROMPT_TYPES:
        raise ValueError(f"Unknown prompt_type {prompt_type!r}. "
                         f"Valid: {sorted(PROMPT_TYPES)}")
    key = (prompt_type, version)
    if key not in _REGISTRY:
        raise ValueError(f"No prompt registered for type={prompt_type!r} version={version!r}")
    return _REGISTRY[key]


def list_prompts(version: str = CURRENT_VERSION) -> list[PromptRecord]:
    """Return all registered prompts for *version*, sorted by type name."""
    return sorted(
        [r for (pt, v), r in _REGISTRY.items() if v == version],
        key=lambda r: r.prompt_type,
    )
