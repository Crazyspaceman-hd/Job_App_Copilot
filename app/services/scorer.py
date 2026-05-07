"""
app/services/scorer.py — Rule-based fit assessment: candidate profile vs. job description.

Design principles:
  - Transparent: every score is traceable to specific evidence (or its absence).
  - Honest: missing evidence is recorded as a gap, not silently ignored.
  - Skeptical: adjacent evidence scores at half weight; familiar at 0.2 weight.
  - Deterministic: same inputs always produce the same output.
  - No LLM: pure pattern matching + vocabulary lookup.

Rubric weights (must sum to 1.0):
  must_have      0.40
  nice_to_have   0.20
  domain         0.15
  seniority      0.15
  logistics      0.10

ATS score is computed but does NOT feed overall_score — it is informational only.

Verdict thresholds (before blocker override):
  overall >= 0.70 → "Strong fit"
  overall >= 0.50 → "Reach but viable"
  overall >= 0.30 → "Long shot"
  overall <  0.30 → "Skip"

A confirmed hard blocker forces verdict to "Skip" regardless of score.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Optional


# ── Score weights ─────────────────────────────────────────────────────────────

WEIGHTS = {
    "must_have":     0.40,
    "nice_to_have":  0.20,
    "domain":        0.15,
    "seniority":     0.15,
    "logistics":     0.10,
}

# Evidence → numeric hit weight
_EVIDENCE_WEIGHT = {
    "direct":   1.0,
    "adjacent": 0.5,
    "familiar": 0.2,
}

# ── Technology vocabulary ─────────────────────────────────────────────────────
# Used to extract recognisable skill terms from raw JD text.
# Multi-word terms must be checked before single-word tokenisation.

_MULTI_WORD_VOCAB: dict[str, str] = {
    "machine learning":           "domain",
    "deep learning":              "domain",
    "natural language processing":"domain",
    "computer vision":            "domain",
    "reinforcement learning":     "domain",
    "data engineering":           "domain",
    "data science":               "domain",
    "data analytics":             "domain",
    "distributed systems":        "practice",
    "system design":              "practice",
    "api design":                 "practice",
    "event driven":               "practice",
    "event-driven":               "practice",
    "test driven":                "practice",
    "test-driven":                "practice",
    "full stack":                 "practice",
    "full-stack":                 "practice",
    "ci/cd":                      "practice",
    "github actions":             "tool",
    "gitlab ci":                  "tool",
    "google cloud":               "cloud",
    "aws lambda":                 "cloud",
    "amazon web services":        "cloud",
    "google bigquery":            "database",
    "apache spark":               "framework",
    "apache kafka":               "tool",
    "apache airflow":             "tool",
    "scikit learn":               "framework",
    "scikit-learn":               "framework",
    "hugging face":               "framework",
    "c++":                        "language",
    "node.js":                    "framework",
    "next.js":                    "framework",
}

_SINGLE_WORD_VOCAB: dict[str, str] = {
    # Languages
    "python":       "language",
    "javascript":   "language",
    "typescript":   "language",
    "golang":       "language",
    "go":           "language",
    "java":         "language",
    "scala":        "language",
    "kotlin":       "language",
    "rust":         "language",
    "ruby":         "language",
    "swift":        "language",
    "sql":          "language",
    "bash":         "language",
    "r":            "language",
    # Frameworks / Libraries
    "fastapi":      "framework",
    "django":       "framework",
    "flask":        "framework",
    "react":        "framework",
    "vue":          "framework",
    "angular":      "framework",
    "spring":       "framework",
    "spark":        "framework",
    "ray":          "framework",
    "pytorch":      "framework",
    "tensorflow":   "framework",
    "keras":        "framework",
    "transformers": "framework",
    "dask":         "framework",
    "pandas":       "framework",
    "numpy":        "framework",
    "celery":       "framework",
    # Databases
    "postgresql":   "database",
    "postgres":     "database",
    "mysql":        "database",
    "sqlite":       "database",
    "mongodb":      "database",
    "redis":        "database",
    "elasticsearch":"database",
    "cassandra":    "database",
    "snowflake":    "database",
    "bigquery":     "database",
    "redshift":     "database",
    "dynamodb":     "database",
    "hbase":        "database",
    "neo4j":        "database",
    "pinecone":     "database",
    # Cloud
    "aws":          "cloud",
    "gcp":          "cloud",
    "azure":        "cloud",
    "s3":           "cloud",
    "lambda":       "cloud",
    "ec2":          "cloud",
    "sqs":          "cloud",
    "sns":          "cloud",
    "rds":          "cloud",
    "eks":          "cloud",
    "ecs":          "cloud",
    "gcs":          "cloud",
    "cloudformation":"cloud",
    "cdk":          "cloud",
    "pulumi":       "cloud",
    # Tools / Infrastructure
    "docker":       "tool",
    "kubernetes":   "tool",
    "k8s":          "tool",
    "terraform":    "tool",
    "airflow":      "tool",
    "dbt":          "tool",
    "kafka":        "tool",
    "rabbitmq":     "tool",
    "git":          "tool",
    "jenkins":      "tool",
    "prometheus":   "tool",
    "grafana":      "tool",
    "datadog":      "tool",
    "splunk":       "tool",
    "flink":        "tool",
    "mlflow":       "tool",
    "kubeflow":     "tool",
    # Practices / Domains
    "microservices":"practice",
    "rest":         "practice",
    "graphql":      "practice",
    "grpc":         "practice",
    "agile":        "practice",
    "scrum":        "practice",
    "devops":       "domain",
    "mlops":        "domain",
    "fintech":      "domain",
    "payments":     "domain",
    "ecommerce":    "domain",
    "healthcare":   "domain",
    "security":     "domain",
}

# Ordered for matching: multi-word first (longest first to avoid partial matches)
_MULTI_WORD_KEYS  = sorted(_MULTI_WORD_VOCAB.keys(), key=len, reverse=True)
_ALL_VOCAB        = {**_MULTI_WORD_VOCAB, **_SINGLE_WORD_VOCAB}

# Proper display casing for tech terms where .title() would be wrong
# (e.g. "FastAPI" not "Fastapi", "PostgreSQL" not "Postgresql")
_DISPLAY_TERMS: dict[str, str] = {
    "fastapi":          "FastAPI",
    "postgresql":       "PostgreSQL",
    "postgres":         "PostgreSQL",
    "graphql":          "GraphQL",
    "grpc":             "gRPC",
    "dynamodb":         "DynamoDB",
    "javascript":       "JavaScript",
    "typescript":       "TypeScript",
    "github":           "GitHub",
    "github actions":   "GitHub Actions",
    "gitlab":           "GitLab",
    "gitlab ci":        "GitLab CI",
    "mongodb":          "MongoDB",
    "bigquery":         "BigQuery",
    "google bigquery":  "Google BigQuery",
    "pytorch":          "PyTorch",
    "mlflow":           "MLflow",
    "kubeflow":         "KubeFlow",
    "aws":              "AWS",
    "aws lambda":       "AWS Lambda",
    "amazon web services": "Amazon Web Services",
    "gcp":              "GCP",
    "google cloud":     "Google Cloud",
    "sql":              "SQL",
    "nosql":            "NoSQL",
    "elasticsearch":    "Elasticsearch",
    "kubernetes":       "Kubernetes",
    "k8s":              "Kubernetes",
    "redis":            "Redis",
    "terraform":        "Terraform",
    "docker":           "Docker",
    "kafka":            "Kafka",
    "apache kafka":     "Apache Kafka",
    "airflow":          "Airflow",
    "apache airflow":   "Apache Airflow",
    "spark":            "Spark",
    "apache spark":     "Apache Spark",
    "scikit-learn":     "scikit-learn",
    "scikit learn":     "scikit-learn",
    "dbt":              "dbt",
    "neo4j":            "Neo4j",
    "pinecone":         "Pinecone",
    "snowflake":        "Snowflake",
    "redshift":         "Redshift",
    "rabbitmq":         "RabbitMQ",
    "datadog":          "Datadog",
    "prometheus":       "Prometheus",
    "grafana":          "Grafana",
    "mlops":            "MLOps",
    "devops":           "DevOps",
    "ci/cd":            "CI/CD",
}


# ── Seniority levels ──────────────────────────────────────────────────────────

def _fmt_term(term: str) -> str:
    """Return a human-readable display name for a normalised vocab term."""
    return _DISPLAY_TERMS.get(term, term.title() if " " not in term else term.title())


_SENIORITY_LEVELS = {
    "junior":    1,
    "mid":       2,
    "senior":    3,
    "staff":     4,
    "principal": 5,
}

_SENIORITY_TITLE_PATTERNS: list[tuple[re.Pattern, int]] = [
    # Checked highest-to-lowest so that e.g. "Staff Engineer mentoring junior devs"
    # matches "staff" rather than the incidental "junior".
    (re.compile(r"\b(principal|distinguished|fellow)\b",  re.I), 5),
    (re.compile(r"\b(staff|lead|architect)\b",            re.I), 4),
    (re.compile(r"\bsenior\b",                            re.I), 3),
    (re.compile(r"\b(junior|entry[- ]level|associate)\b", re.I), 1),
    # "mid" is the default when no modifier is found; used as fallback
]

_YOE_PATTERN = re.compile(r"(\d+)\+?\s*(?:to|-)\s*(\d+)\s*years?|(\d+)\+\s*years?", re.I)


# ── Logistics blocker patterns ────────────────────────────────────────────────

_NO_SPONSORSHIP = re.compile(
    r"no\s+(visa\s+)?sponsorship"
    r"|must\s+be\s+(?:authorized|eligible|legally\s+authorized)\s+to\s+work"
    r"|must\s+be\s+(?:a\s+)?us\s*(citizen|national)"
    r"|cannot\s+(?:provide|offer|sponsor)\s+(?:visa|work\s+visa|sponsorship)"
    r"|work\s+authorization\s+(?:required|needed)",
    re.I,
)
_CLEARANCE_REQUIRED = re.compile(
    r"security\s+clearance|top\s+secret|ts/sci|secret\s+clearance|public\s+trust",
    re.I,
)
_RELOCATION_REQUIRED = re.compile(
    r"must\s+(?:be\s+(?:willing\s+to\s+)?)?relocat"
    r"|relocation\s+(?:is\s+)?required"
    r"|local\s+candidates?\s+only"
    r"|must\s+be\s+(?:located|based)\s+in",
    re.I,
)
_ONSITE_REQUIRED = re.compile(
    r"\b(?:fully\s+)?on[- ]?site\b|\bin[- ]office\b|\bonsite\b",
    re.I,
)


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ScoreBreakdown:
    must_have_score:     float
    nice_to_have_score:  float
    domain_score:        float
    seniority_score:     float
    logistics_score:     float
    ats_score:           float          # informational only
    overall_score:       float
    verdict:             str            # Strong fit | Reach but viable | Long shot | Skip
    confidence:          str            # low | medium | high
    strengths:           list[str]      = field(default_factory=list)
    gaps:                list[str]      = field(default_factory=list)
    red_flags:           list[str]      = field(default_factory=list)
    rationale:           str            = ""
    # Machine-readable evidence buckets (must-have requirements only)
    direct_evidence:     list[str]      = field(default_factory=list)
    adjacent_evidence:   list[str]      = field(default_factory=list)
    unsupported_gaps:    list[str]      = field(default_factory=list)

    @property
    def hard_blockers(self) -> list[str]:
        """Explicit machine-readable hard-blocker list (same data as red_flags)."""
        return self.red_flags

    def to_json(self) -> str:
        d = asdict(self)
        d["hard_blockers"] = list(self.red_flags)  # property → manual inclusion
        return json.dumps(d, indent=2)

    @property
    def gap_summary(self) -> str:
        if not self.gaps:
            return "No gaps identified."
        return "; ".join(self.gaps[:8])  # cap at 8 for DB storage

    def evidence_dict(self) -> dict:
        """Return the four machine-readable evidence buckets as a plain dict."""
        return {
            "direct_evidence":   self.direct_evidence,
            "adjacent_evidence": self.adjacent_evidence,
            "unsupported_gaps":  self.unsupported_gaps,
            "hard_blockers":     self.hard_blockers,
        }


# ── Main entry point ──────────────────────────────────────────────────────────

def assess(
    job_raw_text:      str,
    job_remote_policy: Optional[str],
    profile:           dict,
    project_skills:    set[str] | None = None,
    profile_complete:  float = 0.0,
    extracted:         object | None   = None,   # ExtractionResult | None
) -> ScoreBreakdown:
    """
    Run a full rule-based fit assessment.

    Args:
        job_raw_text:      Raw job description text.
        job_remote_policy: Remote policy detected during ingestion.
        profile:           Candidate profile dict.
        project_skills:    Optional set of skill names from project inventory.
        profile_complete:  Completeness score 0–1 from profile_loader.completeness().
        extracted:         Optional ExtractionResult from extractor.extract().
                           When present the scorer uses structured data instead of
                           re-running raw-text heuristics.  When absent it falls
                           back to the original heuristic pipeline.

    Returns:
        ScoreBreakdown with all fields populated.
    """
    project_skills = project_skills or set()
    jd_lower       = job_raw_text.lower()
    using_extracted = extracted is not None

    # 1. Determine required/preferred skill lists and supporting data
    if using_extracted:
        jd_required   = extracted.required_skills
        jd_preferred  = extracted.preferred_skills
        jd_all_skills = extracted.ats_keywords
        ext_domains   = extracted.domain_requirements
        ext_seniority = extracted.seniority        # string or "unknown"
        ext_logistics = extracted.logistics_constraints
    else:
        sections      = _parse_jd_sections(job_raw_text)
        jd_required   = _extract_vocab_terms(sections["must_have"])
        jd_preferred  = _extract_vocab_terms(sections["nice_to_have"])
        jd_all_skills = _extract_vocab_terms(job_raw_text)
        ext_domains   = None
        ext_seniority = None
        ext_logistics = None

    # 2. Build candidate evidence map  {normalised_skill: evidence_level}
    skill_map = _build_skill_map(profile, project_skills)

    # 3. Score each dimension
    must_have_score, mh_direct, mh_adjacent, mh_gaps = _score_skill_match(
        jd_required, skill_map, fallback_to_full=True, jd_full_skills=jd_all_skills
    )
    nice_to_have_score, nth_direct, nth_adjacent, nth_gaps = _score_skill_match(
        jd_preferred, skill_map, fallback_to_full=False, jd_full_skills=jd_all_skills
    )
    domain_score,    dom_strengths, dom_gaps = _score_domain(
        jd_lower, profile, extracted_domains=ext_domains
    )
    seniority_score, sen_strengths, sen_gaps = _score_seniority(
        job_raw_text, profile, extracted_seniority=ext_seniority
    )
    logistics_score, red_flags = _score_logistics(
        jd_lower, job_remote_policy, profile, extracted_logistics=ext_logistics
    )
    ats_score = _score_ats(jd_all_skills, skill_map)

    # 4. Rebuild display-friendly strengths (direct first; adjacent labeled clearly)
    mh_strengths  = [_fmt_term(s) for s in mh_direct]
    nth_strengths = [_fmt_term(s) for s in nth_direct] + [f"{_fmt_term(s)} (adjacent)" for s in nth_adjacent]

    # 5. Weighted overall
    overall = (
        WEIGHTS["must_have"]    * must_have_score
        + WEIGHTS["nice_to_have"] * nice_to_have_score
        + WEIGHTS["domain"]       * domain_score
        + WEIGHTS["seniority"]    * seniority_score
        + WEIGHTS["logistics"]    * logistics_score
    )
    overall = round(min(max(overall, 0.0), 1.0), 4)

    # 6. Verdict — hard blockers override score-based verdict
    verdict = _determine_verdict(overall, bool(red_flags))

    # 7. Confidence — based on profile completeness
    confidence = _determine_confidence(profile_complete, must_have_score, jd_required)

    # 8. Aggregate narrative lists with strategic gap classification
    strengths = mh_strengths + nth_strengths + dom_strengths + sen_strengths

    # Gaps: hard-missing required → under-signaled required → preferred → domain → seniority
    gaps = (
        [f"{_fmt_term(t)}: required — no profile evidence" for t in mh_gaps]
        + [f"{_fmt_term(t)}: required — adjacent evidence only (build a project to strengthen)" for t in mh_adjacent]
        + [f"{_fmt_term(t)}: preferred — not in profile" for t in nth_gaps]
        + dom_gaps
        + sen_gaps
    )

    rationale = _build_rationale(
        must_have_score, jd_required, jd_preferred,
        skill_map, verdict, confidence, red_flags,
        using_extracted=using_extracted,
        extraction_confidence=getattr(extracted, "extraction_confidence", None),
    )

    return ScoreBreakdown(
        must_have_score    = round(must_have_score, 4),
        nice_to_have_score = round(nice_to_have_score, 4),
        domain_score       = round(domain_score, 4),
        seniority_score    = round(seniority_score, 4),
        logistics_score    = round(logistics_score, 4),
        ats_score          = round(ats_score, 4),
        overall_score      = overall,
        verdict            = verdict,
        confidence         = confidence,
        strengths          = strengths,
        gaps               = gaps,
        red_flags          = red_flags,
        rationale          = rationale,
        # Machine-readable evidence buckets (must-have requirements)
        direct_evidence    = mh_direct,
        adjacent_evidence  = mh_adjacent,
        unsupported_gaps   = mh_gaps,
    )


# ── JD section parsing ────────────────────────────────────────────────────────

_MUST_HEADER   = re.compile(
    r"(?:^|\n)\s*(?:requirements?|qualifications?|must[- ]have|required|"
    r"you must|what you.?ll need|minimum qualifications?|basic qualifications?)"
    r"\s*:?\s*\n",
    re.I,
)
_NICE_HEADER   = re.compile(
    r"(?:^|\n)\s*(?:preferred|nice[- ]to[- ]have|bonus|plus|desirable|"
    r"additional qualifications?|what would be great|good to have)"
    r"\s*:?\s*\n",
    re.I,
)
_SECTION_BREAK = re.compile(r"\n\s*\n")


def _parse_jd_sections(text: str) -> dict[str, str]:
    """
    Attempt to split the JD into must-have and nice-to-have sections.
    Falls back to the full text if headers are not found.
    """
    must_start  = _find_section_start(_MUST_HEADER,  text)
    nice_start  = _find_section_start(_NICE_HEADER,  text)

    must_text   = _extract_section(text, must_start,  nice_start)
    nice_text   = _extract_section(text, nice_start,  None)

    return {
        "must_have":    must_text  or "",
        "nice_to_have": nice_text  or "",
        "full":         text,
    }


def _find_section_start(pattern: re.Pattern, text: str) -> int | None:
    m = pattern.search(text)
    return m.end() if m else None


def _extract_section(text: str, start: int | None, end: int | None) -> str:
    if start is None:
        return ""
    chunk = text[start:end] if end else text[start:]
    # Stop at the next recognisable header (a short non-bullet line followed by colon/newline)
    header_stop = re.search(r"\n[A-Z][A-Za-z &/-]{2,40}:\s*\n", chunk)
    if header_stop:
        chunk = chunk[:header_stop.start()]
    return chunk.strip()


# ── Vocabulary extraction ─────────────────────────────────────────────────────

def _normalize(s: str) -> str:
    """Lowercase, collapse whitespace; keep alphanumeric + space + / + . + -

    Sentence-ending periods (i.e. not followed by a word char) are stripped so
    that "postgresql." → "postgresql" while "node.js" is preserved unchanged.
    """
    s = s.lower()
    s = re.sub(r"[^\w\s/.\-+#]", " ", s)
    # Strip periods that are NOT immediately followed by a word char (sentence
    # punctuation). This preserves "node.js", "next.js", "b.s." (before letter)
    # while cleaning "postgresql." and "sql." at end-of-sentence or end-of-line.
    s = re.sub(r"\.(?!\w)", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _extract_vocab_terms(text: str) -> list[str]:
    """Return all TECH_VOCAB terms found in *text*, multi-word first."""
    norm  = _normalize(text)
    found = []
    seen  = set()

    # Multi-word pass
    for term in _MULTI_WORD_KEYS:
        if term in norm and term not in seen:
            found.append(term)
            seen.add(term)
            # blank it out so single-word pass doesn't double-count constituents
            norm = norm.replace(term, " ")

    # Single-word pass
    tokens = norm.split()
    for tok in tokens:
        if tok in _SINGLE_WORD_VOCAB and tok not in seen:
            found.append(tok)
            seen.add(tok)

    return found


# ── Candidate skill map ───────────────────────────────────────────────────────

def _build_skill_map(profile: dict, project_skills: set[str]) -> dict[str, str]:
    """
    Return {normalised_skill_name: evidence_level} for all candidate skills.

    Profile skills take precedence over project skills.
    Project skills that aren't already in the profile are added as 'adjacent'.
    """
    skill_map: dict[str, str] = {}

    for category_items in profile.get("skills", {}).values():
        if not isinstance(category_items, list):
            continue
        for item in category_items:
            if isinstance(item, dict):
                name = _normalize(item.get("name", ""))
                ev   = item.get("evidence", "direct")
            else:
                name = _normalize(str(item))
                ev   = "direct"
            # Case-insensitive TODO check: raw evidence strings may be "TODO: ..."
            if name and not name.startswith("todo") and not ev.lower().startswith("todo"):
                skill_map[name] = ev

    # Project skills fill gaps as 'adjacent'
    for s in project_skills:
        norm = _normalize(s)
        if norm and not norm.startswith("todo") and norm not in skill_map:
            skill_map[norm] = "adjacent"

    return skill_map


def _lookup_skill(term: str, skill_map: dict[str, str]) -> str | None:
    """
    Return evidence level for *term* or None if not found.
    Tries exact match, then substring match (e.g. 'postgres' matches 'postgresql').
    """
    if term in skill_map:
        return skill_map[term]
    # Substring fallback: term is a substring of a skill the candidate listed
    for key, ev in skill_map.items():
        if term in key or key in term:
            return ev
    return None


# ── Dimension scorers ─────────────────────────────────────────────────────────

def _score_skill_match(
    jd_skills:        list[str],
    skill_map:        dict[str, str],
    fallback_to_full: bool,
    jd_full_skills:   list[str],
) -> tuple[float, list[str], list[str], list[str]]:
    """
    Score how well the candidate covers *jd_skills*.

    Returns (score 0–1, direct_hits, adjacent_hits, gaps).

    direct_hits   — required skills covered with "direct" evidence
    adjacent_hits — required skills covered with "adjacent" or "familiar" evidence
    gaps          — required skills with no evidence at all

    Falls back to jd_full_skills when jd_skills is empty and fallback_to_full=True,
    but dampens the score by 0.8 since we can't confirm which were truly required.
    """
    direct:   list[str] = []
    adjacent: list[str] = []
    gaps:     list[str] = []

    target = jd_skills
    using_fallback = False
    if not target:
        if fallback_to_full and jd_full_skills:
            target         = jd_full_skills
            using_fallback = True
        else:
            return 0.5, [], [], []  # no skills identified — neutral, not penalised

    if not target:
        return 0.5, [], [], []

    total_weight = 0.0
    max_weight   = float(len(target))

    for term in target:
        ev = _lookup_skill(term, skill_map)
        if ev and ev in _EVIDENCE_WEIGHT:
            w = _EVIDENCE_WEIGHT[ev]
            total_weight += w
            if ev == "direct":
                direct.append(term)
            else:  # adjacent, familiar
                adjacent.append(term)
        else:
            gaps.append(term)

    score = total_weight / max_weight
    if using_fallback:
        score = score * 0.8

    return score, direct, adjacent, gaps


def _score_domain(
    jd_lower:         str,
    profile:          dict,
    extracted_domains: list[str] | None = None,
) -> tuple[float, list[str], list[str]]:
    """
    Score domain/industry alignment.

    When *extracted_domains* is provided, uses that list directly instead of
    re-running vocabulary scanning on raw text.
    """
    strengths: list[str] = []
    gaps:      list[str] = []

    # Candidate domain tokens
    candidate_domains: dict[str, str] = {}
    for d in profile.get("domains", []):
        if isinstance(d, dict):
            name = _normalize(d.get("name", ""))
            ev   = d.get("evidence", "direct")
        else:
            name = _normalize(str(d))
            ev   = "direct"
        if name and not name.startswith("todo") and not ev.startswith("todo"):
            candidate_domains[name] = ev

    # JD domain terms: prefer extracted, fall back to vocab scan
    if extracted_domains is not None:
        jd_domains = extracted_domains
    else:
        jd_domains = [t for t in _extract_vocab_terms(jd_lower) if _ALL_VOCAB.get(t) == "domain"]

    if not jd_domains:
        return 0.5, [], []  # JD doesn't signal a specific domain — neutral

    total = 0.0
    for term in jd_domains:
        ev = candidate_domains.get(term) or _lookup_skill(term, candidate_domains)
        if ev and ev in _EVIDENCE_WEIGHT:
            total += _EVIDENCE_WEIGHT[ev]
            label = "direct" if ev == "direct" else "adjacent"
            strengths.append(f"{_fmt_term(term)}: domain knowledge ({label})")
        else:
            gaps.append(f"{_fmt_term(term)}: required domain knowledge — not in profile")

    return total / len(jd_domains), strengths, gaps


def _infer_job_seniority(text: str) -> int:
    """
    Infer the seniority level (1–5) expected by the job.
    Uses title patterns first, then years-of-experience heuristics.
    """
    for pattern, level in _SENIORITY_TITLE_PATTERNS:
        if pattern.search(text):
            return level

    # Fallback: look for YOE requirement
    for m in _YOE_PATTERN.finditer(text):
        lo = int(m.group(1) or m.group(3) or 0)
        hi = int(m.group(2) or lo)
        years = (lo + hi) / 2
        if years >= 8:
            return 4   # staff
        if years >= 5:
            return 3   # senior
        if years >= 3:
            return 2   # mid
        return 1       # junior

    return 2  # default: mid


def _score_seniority(
    text:                str,
    profile:             dict,
    extracted_seniority: str | None = None,
) -> tuple[float, list[str], list[str]]:
    """
    Compare the job's required seniority against the candidate's self-assessment.

    When *extracted_seniority* is provided (and not 'unknown'), uses that instead
    of running _infer_job_seniority() on the raw text.

    Returns (score 0–1, strengths list, gaps list).
    """
    strengths: list[str] = []
    gaps:      list[str] = []

    raw = (profile.get("job_targets") or {}).get("seniority_self_assessed", "")
    if not raw or str(raw).lower().startswith("todo"):
        gaps.append("seniority: self-assessment not set — cannot score seniority")
        return 0.5, strengths, gaps  # unknown → neutral

    candidate_level = _SENIORITY_LEVELS.get(str(raw).lower())
    if not candidate_level:
        gaps.append(f"seniority: unrecognised value '{raw}' — treating as neutral")
        return 0.5, strengths, gaps

    # Prefer extracted seniority; fall back to heuristic inference
    if extracted_seniority and extracted_seniority != "unknown":
        job_level = _SENIORITY_LEVELS.get(extracted_seniority.lower(), 2)
    else:
        job_level = _infer_job_seniority(text)

    delta     = abs(candidate_level - job_level)

    if delta == 0:
        score = 1.0
        strengths.append(f"seniority: exact match ({raw})")
    elif delta == 1:
        if candidate_level < job_level:
            score = 0.60
            gaps.append(
                f"seniority: candidate is {raw}, job targets ~{_level_name(job_level)} — reach"
            )
        else:
            score = 0.80
            strengths.append(
                f"seniority: candidate is {raw}, job targets ~{_level_name(job_level)} — slight over-qualification (usually fine)"
            )
    elif delta == 2:
        if candidate_level < job_level:
            score = 0.25
            gaps.append(
                f"seniority: candidate is {raw}, job targets ~{_level_name(job_level)} — significant gap"
            )
        else:
            score = 0.50
            gaps.append(
                f"seniority: candidate is {raw}, job targets ~{_level_name(job_level)} — likely overqualified"
            )
    else:
        score = 0.10
        gaps.append(
            f"seniority: {delta}-level mismatch between candidate ({raw}) and job"
        )

    return score, strengths, gaps


def _level_name(level: int) -> str:
    return {v: k for k, v in _SENIORITY_LEVELS.items()}.get(level, str(level))


def _score_logistics(
    jd_lower:            str,
    job_remote_policy:   Optional[str],
    profile:             dict,
    extracted_logistics: dict | None = None,
) -> tuple[float, list[str]]:
    """
    Score logistics alignment and detect hard blockers.

    When *extracted_logistics* is provided, uses pre-extracted flags instead of
    re-running regex scans on raw text.  The effective remote_policy comes from
    extracted_logistics["remote_policy"] if set, otherwise falls back to
    job_remote_policy (ingestion-time value).

    Returns (score 0–1, red_flags list).  Red flags are hard blockers.
    """
    red_flags: list[str] = []
    deductions           = 0.0
    targets              = profile.get("job_targets") or {}

    # Resolve logistics flags from extracted data or raw patterns
    if extracted_logistics is not None:
        effective_remote    = extracted_logistics.get("remote_policy") or job_remote_policy
        no_sponsorship_flag = extracted_logistics.get("no_sponsorship", False)
        clearance_flag      = extracted_logistics.get("clearance_required", False)
        relocation_flag     = extracted_logistics.get("relocation_required", False)
    else:
        effective_remote    = job_remote_policy
        no_sponsorship_flag = bool(_NO_SPONSORSHIP.search(jd_lower))
        clearance_flag      = bool(_CLEARANCE_REQUIRED.search(jd_lower))
        relocation_flag     = bool(_RELOCATION_REQUIRED.search(jd_lower))

    # ── Remote policy ──────────────────────────────────────────────────────────
    desired = str(targets.get("desired_remote_policy", "")).lower()
    if not desired.startswith("todo") and desired and effective_remote:
        if desired == "any":
            pass
        elif desired == "remote" and effective_remote == "onsite":
            if not targets.get("willing_to_relocate"):
                red_flags.append("BLOCKER: job is onsite, candidate requires remote")
            else:
                deductions += 0.4
        elif desired == "onsite" and effective_remote == "remote":
            deductions += 0.1
        elif desired == "hybrid" and effective_remote == "remote":
            pass
        elif desired == "hybrid" and effective_remote == "onsite":
            deductions += 0.3

    # ── Visa / work authorisation ──────────────────────────────────────────────
    work_auth = str(targets.get("work_authorization", "")).lower()
    if not work_auth.startswith("todo") and no_sponsorship_flag:
        if work_auth == "need_sponsorship":
            red_flags.append("BLOCKER: job offers no visa sponsorship, candidate needs it")
        elif work_auth == "h1b_transfer":
            red_flags.append("BLOCKER: job likely won't support H-1B transfer — verify")

    # ── Security clearance ────────────────────────────────────────────────────
    if clearance_flag:
        certs = [c.get("name", "").lower() for c in profile.get("certifications", [])
                 if isinstance(c, dict)]
        has_clearance = any("clearance" in c or "ts/sci" in c or "secret" in c for c in certs)
        if not has_clearance:
            red_flags.append("BLOCKER: security clearance required — not found in profile")

    # ── Relocation ────────────────────────────────────────────────────────────
    if relocation_flag:
        willing = targets.get("willing_to_relocate", False)
        if not willing:
            red_flags.append("BLOCKER: relocation required, candidate not willing to relocate")

    if red_flags:
        return 0.0, red_flags

    score = max(0.0, 1.0 - deductions)
    return score, []


def _score_ats(jd_skills: list[str], skill_map: dict[str, str]) -> float:
    """
    Lightweight ATS keyword overlap: fraction of JD tech terms found in profile.
    Informational only — does not feed overall_score.
    """
    if not jd_skills:
        return 0.0
    hits = sum(1 for t in jd_skills if _lookup_skill(t, skill_map))
    return hits / len(jd_skills)


# ── Verdict and confidence ────────────────────────────────────────────────────

def _determine_verdict(overall: float, has_blocker: bool) -> str:
    if has_blocker:
        return "Skip"
    if overall >= 0.70:
        return "Strong fit"
    if overall >= 0.50:
        return "Reach but viable"
    if overall >= 0.30:
        return "Long shot"
    return "Skip"


def _determine_confidence(
    profile_complete: float,
    must_have_score:  float,
    jd_required:      list[str],
) -> str:
    """
    Confidence is about how much we trust the assessment, not how good the fit is.

    Factors:
    - Profile completeness (are the key scoring fields filled in?)
    - Whether we could identify any required skills in the JD

    A profile with no filled fields is always low-confidence regardless of JD clarity.
    """
    if profile_complete <= 0.0:
        return "low"
    if profile_complete >= 0.75 and jd_required:
        return "high"
    if profile_complete >= 0.40 or jd_required:
        return "medium"
    return "low"


# ── Rationale builder ─────────────────────────────────────────────────────────

def _build_rationale(
    must_have_score:       float,
    jd_required:           list[str],
    jd_preferred:          list[str],
    skill_map:             dict[str, str],
    verdict:               str,
    confidence:            str,
    red_flags:             list[str],
    using_extracted:       bool = False,
    extraction_confidence: str | None = None,
) -> str:
    parts: list[str] = []

    if using_extracted:
        parts.append(
            f"[Structured extraction used (confidence: {extraction_confidence or 'unknown'})]"
        )
    else:
        parts.append("[Raw-text heuristics — run extract-requirements to improve accuracy]")

    # Required skill coverage with direct/adjacent breakdown
    if jd_required:
        direct_hits   = [t for t in jd_required if _lookup_skill(t, skill_map) == "direct"]
        adjacent_hits = [t for t in jd_required if _lookup_skill(t, skill_map) in ("adjacent", "familiar")]
        missing       = [t for t in jd_required if not _lookup_skill(t, skill_map)]

        parts.append(
            f"{len(direct_hits)}/{len(jd_required)} required skills: "
            f"{len(direct_hits)} direct, {len(adjacent_hits)} adjacent-only, "
            f"{len(missing)} missing. Must-have score: {must_have_score:.0%}."
        )
        if missing:
            missing_disp = ", ".join(_fmt_term(t) for t in missing[:5])
            suffix = f" (+{len(missing)-5} more)" if len(missing) > 5 else ""
            parts.append(f"Missing required: {missing_disp}{suffix}.")
        if adjacent_hits:
            adj_disp = ", ".join(_fmt_term(t) for t in adjacent_hits[:4])
            parts.append(f"Adjacent-only (under-signaled): {adj_disp}.")
    else:
        parts.append(
            "Could not identify discrete required skills from JD — "
            "must-have score derived from full-text matching (lower confidence)."
        )

    if jd_preferred:
        hits_p = sum(1 for t in jd_preferred if _lookup_skill(t, skill_map))
        parts.append(f"{hits_p}/{len(jd_preferred)} preferred skills found in profile.")

    if red_flags:
        parts.append("Hard blockers: " + "; ".join(red_flags))

    if confidence == "low":
        parts.append(
            "LOW confidence — profile largely unfilled. "
            "Add skills, domains, seniority, and work-authorisation for accurate assessment."
        )
    elif confidence == "medium":
        parts.append(
            "MEDIUM confidence — some profile fields missing. "
            "Fill remaining TODO fields to sharpen the assessment."
        )

    parts.append(f"Verdict: {verdict}.")
    return "  ".join(parts)


# ── DB persistence ─────────────────────────────────────────────────────────────

def persist_assessment(
    conn:       sqlite3.Connection,
    job_id:     int,
    profile_id: int,
    result:     ScoreBreakdown,
) -> int:
    """
    Write *result* to fit_assessments and return the new row id.

    Stores both the full scores_json blob and a compact evidence_json blob
    containing only the four machine-readable evidence buckets so they can
    be queried without parsing the entire breakdown.
    """
    cur = conn.execute(
        """INSERT INTO fit_assessments
           (job_id, candidate_profile_id, overall_score, verdict, confidence,
            rationale, gap_summary, scores_json, evidence_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            job_id,
            profile_id,
            result.overall_score,
            result.verdict,
            result.confidence,
            result.rationale,
            result.gap_summary,
            result.to_json(),
            json.dumps(result.evidence_dict()),
        ),
    )
    conn.commit()
    return cur.lastrowid
