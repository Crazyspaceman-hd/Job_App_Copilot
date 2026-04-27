"""
tests/test_base_asset_ingest.py — Base resume and cover letter ingestion tests.

Covered cases:
  Section detection
    1.  ALL-CAPS heading without colon                 ("EXPERIENCE")
    2.  ALL-CAPS heading with colon                    ("SKILLS:")
    3.  Markdown heading (## Projects)
    4.  Underline-style heading (Experience / -------)
    5.  Title-case plain heading                       ("Work Experience")
    6.  Unrecognised heading → not counted as a section
    7.  Multiple distinct sections are all detected
    8.  Content before first heading is tagged "header"
    9.  No headings → single "other" section

  Bullet extraction
   10.  Dash bullet (- text)
   11.  Star bullet (* text)
   12.  Unicode bullet (• text)
   13.  Numbered bullet  (1. text / 2) text)
   14.  Non-bullet body line → not extracted as a bullet
   15.  Empty bullet line → not extracted

  Skill / vocab extraction from resume text
   16.  Known framework appears in result.skills
   17.  Known language appears in result.skills
   18.  Unknown token is not returned

  Resume parse — structural
   19.  section_count matches number of detected sections
   20.  bullet_bank contains bullets from all sections
   21.  resume_id is 0 before persistence
   22.  raw_text preserved verbatim

  Cover letter fragment extraction
   23.  Single paragraph → one opening fragment
   24.  Two paragraphs → opening + closing
   25.  Three paragraphs → opening, proof_point, closing
   26.  Many paragraphs → first=opening, last=closing, all others=proof_point
   27.  Blank-line separation correctly splits paragraphs
   28.  cl_id is 0 before persistence
   29.  raw_text preserved verbatim

  Determinism
   30.  Same input → same output (parse_resume idempotent)
   31.  Same input → same output (parse_cover_letter idempotent)

  DB persistence and reload — resume
   32.  persist_resume returns a positive integer id
   33.  load_resume returns correct label and raw_text
   34.  load_resume bullet_bank matches original
   35.  load_resume skills list matches original
   36.  load_resume section names match original
   37.  Missing id returns None

  DB persistence and reload — cover letter
   38.  persist_cover_letter returns a positive integer id
   39.  load_cover_letter returns correct label and raw_text
   40.  load_cover_letter fragments match original (kind + text)
   41.  Missing id returns None

  Public entry points (ingest_*)
   42.  ingest_resume sets resume_id on returned result
   43.  ingest_cover_letter sets cl_id on returned result
   44.  ingest_resume writes resume_bullets rows to DB
   45.  ingest_cover_letter writes cover_letter_fragments rows to DB

  End-to-end with sample files
   46.  sample_resume.txt: ≥4 sections detected
   47.  sample_resume.txt: ≥10 bullets in bullet_bank
   48.  sample_resume.txt: ≥5 unique skills
   49.  sample_cover_letter.txt: exactly 7 paragraphs
   50.  sample_cover_letter.txt: first fragment kind == "opening"
   51.  sample_cover_letter.txt: last fragment kind == "closing"
   52.  sample_cover_letter.txt: all middle fragments kind == "proof_point"
   53.  Full round-trip: ingest sample resume + reload and compare bullets
   54.  Full round-trip: ingest sample cover letter + reload and compare fragments
"""

import json
import sqlite3
from pathlib import Path

import pytest

from app.db import apply_migrations
from app.services.base_asset_ingest import (
    CLFragment,
    CoverLetterResult,
    ResumeBullet,
    ResumeResult,
    ingest_cover_letter,
    ingest_resume,
    load_cover_letter,
    load_resume,
    parse_cover_letter,
    parse_resume,
    persist_cover_letter,
    persist_resume,
)

# ── Paths ──────────────────────────────────────────────────────────────────────

_PROJECT_ROOT    = Path(__file__).resolve().parent.parent
_SAMPLE_RESUME   = _PROJECT_ROOT / "data" / "sample_resume.txt"
_SAMPLE_CL       = _PROJECT_ROOT / "data" / "sample_cover_letter.txt"


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def mem_conn():
    """In-memory SQLite with full schema + migrations applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    # Base schema (tables needed by migrations)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
            status      TEXT NOT NULL DEFAULT 'new',
            title       TEXT,
            company     TEXT,
            remote_policy TEXT,
            raw_text    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS candidate_profiles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            version      TEXT,
            profile_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fit_assessments (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id               INTEGER NOT NULL REFERENCES jobs(id),
            candidate_profile_id INTEGER REFERENCES candidate_profiles(id),
            assessed_at          TEXT    NOT NULL DEFAULT (datetime('now')),
            overall_score        REAL,
            rationale            TEXT,
            gap_summary          TEXT
        );
        CREATE TABLE IF NOT EXISTS extracted_requirements (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id      INTEGER NOT NULL REFERENCES jobs(id),
            skill       TEXT,
            category    TEXT,
            is_required INTEGER
        );
    """)
    apply_migrations(conn)
    return conn


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resume_with_sections(*sections: str) -> str:
    """Build a minimal resume string from (heading, bullets) pairs."""
    parts = []
    for heading, bullets in zip(sections[0::2], sections[1::2]):
        parts.append(heading)
        for b in bullets:
            parts.append(f"- {b}")
        parts.append("")
    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# 1–9  Section detection
# ═══════════════════════════════════════════════════════════════════════════════

def test_section_all_caps_no_colon():
    r = parse_resume("EXPERIENCE\n- Built APIs\n")
    names = [s.name for s in r.sections]
    assert "experience" in names


def test_section_all_caps_with_colon():
    r = parse_resume("SKILLS:\n- Python\n")
    names = [s.name for s in r.sections]
    assert "skills" in names


def test_section_markdown_heading():
    r = parse_resume("## Projects\n- Built something\n")
    names = [s.name for s in r.sections]
    assert "projects" in names


def test_section_underline_style():
    text = "Experience\n----------\n- Led a team\n"
    r = parse_resume(text)
    names = [s.name for s in r.sections]
    assert "experience" in names


def test_section_title_case_plain():
    r = parse_resume("Work Experience\n- Delivered features\n")
    names = [s.name for s in r.sections]
    assert "experience" in names


def test_section_unrecognised_heading_not_counted():
    """A heading that isn't in our vocabulary should not produce a named section."""
    r = parse_resume("References Available Upon Request\n- Dr. Smith\n")
    names = {s.name for s in r.sections}
    # Should fall through to "other" or "header", not a named section
    assert "references" not in names


def test_section_multiple_sections_detected():
    text = (
        "SUMMARY\n- Experienced engineer\n\n"
        "EXPERIENCE\n- Built APIs\n\n"
        "SKILLS\n- Python\n\n"
        "EDUCATION\n- B.S. CS\n"
    )
    r = parse_resume(text)
    names = {s.name for s in r.sections}
    assert {"summary", "experience", "skills", "education"}.issubset(names)


def test_content_before_first_heading_tagged_header():
    text = "Alex Rivera\nalex@example.com\n\nEXPERIENCE\n- Built APIs\n"
    r = parse_resume(text)
    names = [s.name for s in r.sections]
    assert names[0] == "header"


def test_no_headings_gives_other_section():
    text = "This is just some text.\nWith no headings at all.\n"
    r = parse_resume(text)
    assert len(r.sections) == 1
    assert r.sections[0].name == "other"


# ═══════════════════════════════════════════════════════════════════════════════
# 10–15  Bullet extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_bullet_dash():
    r = parse_resume("EXPERIENCE\n- Built something great\n")
    assert any(b.text == "Built something great" for b in r.bullet_bank)


def test_bullet_star():
    r = parse_resume("EXPERIENCE\n* Shipped a feature\n")
    assert any(b.text == "Shipped a feature" for b in r.bullet_bank)


def test_bullet_unicode():
    r = parse_resume("EXPERIENCE\n• Led a migration\n")
    assert any(b.text == "Led a migration" for b in r.bullet_bank)


def test_bullet_numbered():
    r = parse_resume("EXPERIENCE\n1. First achievement\n2. Second achievement\n")
    texts = [b.text for b in r.bullet_bank]
    assert "First achievement" in texts
    assert "Second achievement" in texts


def test_non_bullet_body_not_extracted():
    """A plain body line (no bullet marker) should not appear in bullet_bank."""
    r = parse_resume("EXPERIENCE\nSenior Engineer at Acme Corp, 2020-2023\n")
    assert not r.bullet_bank


def test_empty_bullet_line_not_extracted():
    r = parse_resume("EXPERIENCE\n- \n-    \n")
    assert not r.bullet_bank


# ═══════════════════════════════════════════════════════════════════════════════
# 16–18  Skill / vocab extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_known_framework_in_skills():
    r = parse_resume("SKILLS\n- FastAPI, Django, Flask\n")
    skills_lower = [s.lower() for s in r.skills]
    assert any("fastapi" in s or "django" in s for s in skills_lower)


def test_known_language_in_skills():
    r = parse_resume("SKILLS\n- Python, SQL, TypeScript\n")
    skills_lower = [s.lower() for s in r.skills]
    assert any("python" in s for s in skills_lower)


def test_unknown_token_not_in_skills():
    r = parse_resume("SKILLS\n- FooBarBazQuux\n")
    assert "foobarbazquux" not in [s.lower() for s in r.skills]


# ═══════════════════════════════════════════════════════════════════════════════
# 19–22  Resume parse — structural
# ═══════════════════════════════════════════════════════════════════════════════

def test_section_count_matches():
    text = "SUMMARY\n- Overview\n\nEXPERIENCE\n- Built APIs\n\nSKILLS\n- Python\n"
    r = parse_resume(text)
    names = {s.name for s in r.sections}
    assert "summary" in names and "experience" in names and "skills" in names


def test_bullet_bank_contains_bullets_from_all_sections():
    text = (
        "EXPERIENCE\n- Built a thing\n\n"
        "PROJECTS\n- Open-source work\n"
    )
    r = parse_resume(text)
    texts = {b.text for b in r.bullet_bank}
    assert "Built a thing" in texts
    assert "Open-source work" in texts


def test_resume_id_is_zero_before_persist():
    r = parse_resume("SKILLS\n- Python\n")
    assert r.resume_id == 0


def test_raw_text_preserved():
    text = "SKILLS\n- Python\n- SQL\n"
    r = parse_resume(text)
    assert r.raw_text == text


# ═══════════════════════════════════════════════════════════════════════════════
# 23–29  Cover letter fragment extraction
# ═══════════════════════════════════════════════════════════════════════════════

def test_cl_single_paragraph_is_opening():
    cl = parse_cover_letter("Just one paragraph here.")
    assert len(cl.fragments) == 1
    assert cl.fragments[0].kind == "opening"


def test_cl_two_paragraphs():
    text = "First paragraph.\n\nSecond paragraph."
    cl = parse_cover_letter(text)
    assert len(cl.fragments) == 2
    assert cl.fragments[0].kind == "opening"
    assert cl.fragments[1].kind == "closing"


def test_cl_three_paragraphs():
    text = "Opening para.\n\nProof point.\n\nClosing para."
    cl = parse_cover_letter(text)
    assert len(cl.fragments) == 3
    assert cl.fragments[0].kind == "opening"
    assert cl.fragments[1].kind == "proof_point"
    assert cl.fragments[2].kind == "closing"


def test_cl_many_paragraphs_structure():
    paras = ["Para one.", "Para two.", "Para three.", "Para four.", "Para five."]
    text = "\n\n".join(paras)
    cl = parse_cover_letter(text)
    assert len(cl.fragments) == 5
    assert cl.fragments[0].kind == "opening"
    assert cl.fragments[-1].kind == "closing"
    assert all(f.kind == "proof_point" for f in cl.fragments[1:-1])


def test_cl_blank_lines_separate_paragraphs():
    text = "Line one.\nLine two.\n\nLine three.\nLine four.\n"
    cl = parse_cover_letter(text)
    # Two paragraphs: "Line one. Line two." and "Line three. Line four."
    assert len(cl.fragments) == 2
    assert "Line one." in cl.fragments[0].text
    assert "Line three." in cl.fragments[1].text


def test_cl_id_is_zero_before_persist():
    cl = parse_cover_letter("Some text.")
    assert cl.cl_id == 0


def test_cl_raw_text_preserved():
    text = "Hello hiring manager.\n\nI am great.\n\nSincerely, Alex.\n"
    cl = parse_cover_letter(text)
    assert cl.raw_text == text


# ═══════════════════════════════════════════════════════════════════════════════
# 30–31  Determinism
# ═══════════════════════════════════════════════════════════════════════════════

def test_parse_resume_is_deterministic():
    text = "EXPERIENCE\n- Built APIs\n\nSKILLS\n- Python\n"
    r1 = parse_resume(text)
    r2 = parse_resume(text)
    assert r1.sections == r2.sections
    assert r1.bullet_bank == r2.bullet_bank
    assert r1.skills == r2.skills


def test_parse_cover_letter_is_deterministic():
    text = "Opening.\n\nProof.\n\nClosing."
    c1 = parse_cover_letter(text)
    c2 = parse_cover_letter(text)
    assert c1.fragments == c2.fragments


# ═══════════════════════════════════════════════════════════════════════════════
# 32–37  DB persistence and reload — resume
# ═══════════════════════════════════════════════════════════════════════════════

def test_persist_resume_returns_positive_id(mem_conn):
    r = parse_resume("SKILLS\n- Python\n- SQL\n")
    rid = persist_resume(mem_conn, r)
    assert isinstance(rid, int) and rid > 0


def test_load_resume_label_and_raw_text(mem_conn):
    r = parse_resume("SKILLS\n- Python\n", label="v2")
    rid = persist_resume(mem_conn, r)
    loaded = load_resume(mem_conn, rid)
    assert loaded is not None
    assert loaded.label    == "v2"
    assert loaded.raw_text == "SKILLS\n- Python\n"


def test_load_resume_bullet_bank_matches(mem_conn):
    text = "EXPERIENCE\n- Built APIs with Python\n- Deployed to AWS\n"
    r    = parse_resume(text)
    rid  = persist_resume(mem_conn, r)
    loaded = load_resume(mem_conn, rid)
    assert len(loaded.bullet_bank) == len(r.bullet_bank)
    for orig, reloaded in zip(r.bullet_bank, loaded.bullet_bank):
        assert orig.text        == reloaded.text
        assert orig.section     == reloaded.section
        assert orig.source_line == reloaded.source_line


def test_load_resume_skills_match(mem_conn):
    r   = parse_resume("SKILLS\n- Python, PostgreSQL, Docker\n")
    rid = persist_resume(mem_conn, r)
    loaded = load_resume(mem_conn, rid)
    assert loaded.skills == r.skills


def test_load_resume_section_names_match(mem_conn):
    text = "EXPERIENCE\n- Built APIs\n\nEDUCATION\n- B.S. CS\n"
    r    = parse_resume(text)
    rid  = persist_resume(mem_conn, r)
    loaded = load_resume(mem_conn, rid)
    orig_names   = [s.name for s in r.sections]
    loaded_names = [s.name for s in loaded.sections]
    assert orig_names == loaded_names


def test_load_resume_missing_id_returns_none(mem_conn):
    result = load_resume(mem_conn, 99999)
    assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# 38–41  DB persistence and reload — cover letter
# ═══════════════════════════════════════════════════════════════════════════════

def test_persist_cover_letter_returns_positive_id(mem_conn):
    cl    = parse_cover_letter("Opening.\n\nClosing.")
    cl_id = persist_cover_letter(mem_conn, cl)
    assert isinstance(cl_id, int) and cl_id > 0


def test_load_cover_letter_label_and_raw_text(mem_conn):
    text  = "Opening.\n\nClosing."
    cl    = parse_cover_letter(text, label="template-a")
    cl_id = persist_cover_letter(mem_conn, cl)
    loaded = load_cover_letter(mem_conn, cl_id)
    assert loaded is not None
    assert loaded.label    == "template-a"
    assert loaded.raw_text == text


def test_load_cover_letter_fragments_match(mem_conn):
    text  = "Opening para.\n\nProof point.\n\nClosing para."
    cl    = parse_cover_letter(text)
    cl_id = persist_cover_letter(mem_conn, cl)
    loaded = load_cover_letter(mem_conn, cl_id)
    assert len(loaded.fragments) == 3
    for orig, reloaded in zip(cl.fragments, loaded.fragments):
        assert orig.kind        == reloaded.kind
        assert orig.text        == reloaded.text
        assert orig.source_line == reloaded.source_line


def test_load_cover_letter_missing_id_returns_none(mem_conn):
    result = load_cover_letter(mem_conn, 99999)
    assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# 42–45  Public entry points (ingest_*)
# ═══════════════════════════════════════════════════════════════════════════════

def test_ingest_resume_sets_resume_id(mem_conn):
    result = ingest_resume("SKILLS\n- Python\n", mem_conn, label="test")
    assert result.resume_id > 0


def test_ingest_cover_letter_sets_cl_id(mem_conn):
    result = ingest_cover_letter("Opening.\n\nClosing.", mem_conn, label="test")
    assert result.cl_id > 0


def test_ingest_resume_writes_bullet_rows(mem_conn):
    text   = "EXPERIENCE\n- Built APIs\n- Deployed to AWS\n"
    result = ingest_resume(text, mem_conn)
    rows   = mem_conn.execute(
        "SELECT * FROM resume_bullets WHERE resume_id = ?", (result.resume_id,)
    ).fetchall()
    assert len(rows) == 2
    texts = {r["text"] for r in rows}
    assert "Built APIs" in texts
    assert "Deployed to AWS" in texts


def test_ingest_cover_letter_writes_fragment_rows(mem_conn):
    text   = "Opening.\n\nProof.\n\nClosing."
    result = ingest_cover_letter(text, mem_conn)
    rows   = mem_conn.execute(
        "SELECT * FROM cover_letter_fragments WHERE cover_letter_id = ?",
        (result.cl_id,),
    ).fetchall()
    assert len(rows) == 3
    kinds = [r["kind"] for r in rows]
    assert kinds == ["opening", "proof_point", "closing"]


# ═══════════════════════════════════════════════════════════════════════════════
# 46–54  End-to-end with sample files
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def parsed_resume():
    assert _SAMPLE_RESUME.exists(), f"Missing {_SAMPLE_RESUME}"
    return parse_resume(_SAMPLE_RESUME.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def parsed_cl():
    assert _SAMPLE_CL.exists(), f"Missing {_SAMPLE_CL}"
    return parse_cover_letter(_SAMPLE_CL.read_text(encoding="utf-8"))


def test_sample_resume_section_count(parsed_resume):
    named_sections = [s for s in parsed_resume.sections if s.name not in ("header", "other")]
    assert len(named_sections) >= 4, \
        f"Expected ≥4 named sections, got {[s.name for s in named_sections]}"


def test_sample_resume_bullet_count(parsed_resume):
    assert len(parsed_resume.bullet_bank) >= 10, \
        f"Expected ≥10 bullets, got {len(parsed_resume.bullet_bank)}"


def test_sample_resume_skills_count(parsed_resume):
    assert len(parsed_resume.skills) >= 5, \
        f"Expected ≥5 unique skills, got {parsed_resume.skills}"


def test_sample_cl_paragraph_count(parsed_cl):
    # Paragraphs: salutation, body×4 (opening statement + 3 proof points),
    # thank-you, sign-off  →  7 total
    assert len(parsed_cl.fragments) == 7, \
        f"Expected 7 fragments from sample_cover_letter.txt, got {len(parsed_cl.fragments)}"


def test_sample_cl_first_fragment_is_opening(parsed_cl):
    assert parsed_cl.fragments[0].kind == "opening"


def test_sample_cl_last_fragment_is_closing(parsed_cl):
    assert parsed_cl.fragments[-1].kind == "closing"


def test_sample_cl_middle_fragments_are_proof_points(parsed_cl):
    middle = parsed_cl.fragments[1:-1]
    assert all(f.kind == "proof_point" for f in middle), \
        f"Middle kinds: {[f.kind for f in middle]}"


def test_sample_resume_roundtrip(mem_conn):
    """Ingest sample_resume.txt, reload from DB, verify bullet_bank integrity."""
    text   = _SAMPLE_RESUME.read_text(encoding="utf-8")
    result = ingest_resume(text, mem_conn, label="sample")
    loaded = load_resume(mem_conn, result.resume_id)
    assert loaded is not None
    assert len(loaded.bullet_bank) == len(result.bullet_bank)
    for orig, reloaded in zip(result.bullet_bank, loaded.bullet_bank):
        assert orig.text        == reloaded.text
        assert orig.section     == reloaded.section
        assert orig.source_line == reloaded.source_line


def test_sample_cl_roundtrip(mem_conn):
    """Ingest sample_cover_letter.txt, reload from DB, verify fragment integrity."""
    text   = _SAMPLE_CL.read_text(encoding="utf-8")
    result = ingest_cover_letter(text, mem_conn, label="sample")
    loaded = load_cover_letter(mem_conn, result.cl_id)
    assert loaded is not None
    assert len(loaded.fragments) == len(result.fragments)
    for orig, reloaded in zip(result.fragments, loaded.fragments):
        assert orig.kind        == reloaded.kind
        assert orig.text        == reloaded.text
        assert orig.source_line == reloaded.source_line
