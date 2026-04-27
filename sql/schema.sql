-- Job Application Copilot — SQLite schema
-- Run once via: python -m app.db  (or db.init_db())

PRAGMA foreign_keys = ON;

-- ── Core job listing ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    source_url      TEXT,                       -- NULL for manually pasted JDs
    company         TEXT,
    title           TEXT,
    location        TEXT,
    remote_policy   TEXT,                       -- remote / hybrid / onsite
    raw_text        TEXT    NOT NULL,           -- original pasted / scraped text
    status          TEXT    NOT NULL DEFAULT 'new'
                            CHECK(status IN ('new','reviewing','applied','rejected','offer','archived'))
);

-- ── Requirements extracted from the JD (one row per bullet / skill) ───────────
CREATE TABLE IF NOT EXISTS extracted_requirements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    category        TEXT    NOT NULL,           -- must_have | nice_to_have | responsibility | education | domain
    requirement     TEXT    NOT NULL,
    source_phrase   TEXT,                       -- optional: original text fragment that produced this row
    extracted_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ── Extraction run metadata (one row per extraction; individual rows above) ───
CREATE TABLE IF NOT EXISTS extraction_runs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id                INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    extracted_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    extraction_confidence TEXT,                 -- high | medium | low
    extraction_notes_json TEXT,                 -- JSON array of note strings
    seniority             TEXT,                 -- junior|mid|senior|staff|principal|unknown
    min_years_experience  INTEGER,
    max_years_experience  INTEGER,
    logistics_json        TEXT,                 -- JSON {remote_policy, no_sponsorship, ...}
    ats_keywords_json     TEXT,                 -- JSON array
    summary_json          TEXT                  -- full ExtractionResult as JSON (for fast reloading)
);

-- ── Candidate snapshot used for scoring ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS candidate_profiles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    version         TEXT    NOT NULL DEFAULT '1.0',
    profile_json    TEXT    NOT NULL            -- serialised JSON blob from data/candidate_profile.json
);

-- ── Fit assessment: candidate vs. one job ────────────────────────────────────
CREATE TABLE IF NOT EXISTS fit_assessments (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id               INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    candidate_profile_id INTEGER NOT NULL REFERENCES candidate_profiles(id),
    assessed_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    overall_score        REAL,                  -- 0.0 – 1.0
    verdict              TEXT,                  -- Strong fit | Reach but viable | Long shot | Skip
    confidence           TEXT,                  -- low | medium | high
    rationale            TEXT,
    gap_summary          TEXT,
    scores_json          TEXT                   -- JSON blob: full per-dimension breakdown
);

-- ── Generated assets: resume variants, cover letters, etc. ───────────────────
CREATE TABLE IF NOT EXISTS generated_assets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    asset_type      TEXT    NOT NULL            -- 'resume' | 'cover_letter' | 'email'
                            CHECK(asset_type IN ('resume','cover_letter','email','other')),
    file_path       TEXT,                       -- relative path if saved to disk
    content         TEXT,                       -- inline content (for short assets)
    generated_at    TEXT    NOT NULL DEFAULT (datetime('now'))
    -- TODO(LLM): populated by services/generator.py once LLM integration lands
);

-- ── Project / portfolio recommendations tied to a job ────────────────────────
CREATE TABLE IF NOT EXISTS project_recommendations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    recommended_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    project_title   TEXT    NOT NULL,
    rationale       TEXT,
    priority        INTEGER NOT NULL DEFAULT 5  -- 1 (highest) – 10 (lowest)
    -- TODO(LLM): populated by services/recommender.py once LLM integration lands
);

-- ── Application tracking ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS applications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    applied_at      TEXT,
    platform        TEXT,                       -- LinkedIn / company site / email / etc.
    resume_asset_id INTEGER REFERENCES generated_assets(id),
    cover_letter_asset_id INTEGER REFERENCES generated_assets(id),
    notes           TEXT,
    last_updated    TEXT    NOT NULL DEFAULT (datetime('now'))
);
