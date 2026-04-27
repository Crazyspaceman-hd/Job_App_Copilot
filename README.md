# Job Application Copilot

A local-first Python tool for managing job applications end-to-end:
ingest job descriptions → extract requirements → score fit → generate tailored
resumes and cover letters → recommend portfolio projects → track decisions.

Everything runs on your machine. No cloud, no auth, no LLM required.

---

## Project layout

```
Job-App-Copilot/
├── app/
│   ├── main.py                     # CLI entry point
│   ├── api.py                      # FastAPI JSON API (for the UI)
│   ├── db.py                       # SQLite connection + schema migrations
│   └── services/
│       ├── intake.py               # Ingest & normalise job descriptions
│       ├── profile_loader.py       # Load & validate candidate profile
│       ├── project_loader.py       # Load project inventory
│       ├── extractor.py            # Extract structured requirements from JD
│       ├── scorer.py               # Rule-based fit assessment
│       ├── base_asset_ingest.py    # Ingest base resume and cover letter
│       ├── resume_tailor.py        # Generate targeted resume
│       ├── cover_letter.py         # Generate targeted cover letter
│       ├── project_recommender.py  # Portfolio project recommendations
│       └── tracker.py             # Application decision tracker
├── data/
│   ├── candidate_profile.json      # Your profile (fill this in)
│   ├── project_inventory.json      # Your projects (fill this in)
│   └── copilot.db                  # SQLite DB (git-ignored)
├── ui/                             # React + Vite frontend
│   └── src/
│       ├── App.tsx
│       ├── components/
│       └── lib/api.ts
├── sql/
│   └── schema.sql                  # Base schema
├── tests/                          # 428 tests
└── requirements.txt
```

---

## Quick start

```bash
# 1. Create a virtual environment (optional but recommended)
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Initialise the database
python -m app.db

# 4. Fill in your profile
#    Edit data/candidate_profile.json and data/project_inventory.json

# 5. Ingest a job description
python -m app.main ingest --file path/to/jd.txt

# 6. Run the full workflow (see below)
```

---

## CLI workflow

All commands accept `--help` for full options.

| Step | Command | Notes |
|------|---------|-------|
| Ingest JD | `ingest --file jd.txt` | also accepts stdin |
| List jobs | `list` | shows status and fit verdict |
| Extract requirements | `extract-requirements --job-id 1` | structured skill/requirement parse |
| Score fit | `assess-fit --job-id 1` | rule-based; uses `candidate_profile.json` |
| Ingest base resume | `ingest-resume --file resume.txt` | one-time; reused for all jobs |
| Ingest base cover letter | `ingest-cover-letter --file cl.txt` | one-time |
| Generate resume | `generate-resume --job-id 1` | ranked, verbatim bullets |
| Generate cover letter | `generate-cover-letter --job-id 1` | verbatim proof points |
| Recommend projects | `recommend-project --job-id 1` | new concept + reposition existing |
| Set decision | `set-application-status --job-id 1 --status apply` | apply / hold / skip |
| Review package | `show-package --job-id 1` | single-pane-of-glass summary |

```bash
# Complete example for job id=1
python -m app.main ingest --file jd.txt
python -m app.main extract-requirements --job-id 1
python -m app.main assess-fit --job-id 1
python -m app.main ingest-resume --file data/my_resume.txt
python -m app.main ingest-cover-letter --file data/my_cl.txt
python -m app.main generate-resume --job-id 1 --output out/resume.md
python -m app.main generate-cover-letter --job-id 1 --output out/cl.md
python -m app.main recommend-project --job-id 1 --output out/recs.md
python -m app.main set-application-status --job-id 1 --status apply --platform LinkedIn
python -m app.main show-package --job-id 1
```

---

## React UI

A local review UI is available at `ui/`. It connects to the FastAPI backend.

```bash
# Terminal 1 — start the API server
uvicorn app.api:app --port 8080 --reload
# API runs at http://localhost:8080

# Terminal 2 — start the React dev server
cd ui
npm install     # first time only
npm run dev
# UI runs at http://localhost:5173
```

### UI layout

```
┌─────────────────────────────────────────────────────────┐
│  Job Application Copilot                        Refresh  │
├──────────────┬──────────────────────────────────────────┤
│  Jobs (N)    │  Senior Engineer @ Acme  [reviewing]      │
│              │  Remote · Source ↗                        │
│  ▶ Acme      ├──────────────────────────────────────────┤
│    SWE       │  Fit | Resume | Cover letter | Projects   │
│              ├──────────────────────────┬───────────────┤
│  Widgets Co  │  [tab content]           │ Application   │
│    Data Eng  │                          │ decision      │
│              │  Evidence buckets:       │               │
│  ...         │  ✓ Direct  (python, …)   │ ○ Apply       │
│              │  ~ Adjacent (kafka, …)   │ ○ Hold        │
│              │  ✗ Gaps    (k8s, …)      │ ○ Skip        │
│              │                          │               │
│              │                          │ Notes: …      │
│              │                          │ Follow-up: …  │
│              │                          │ [Save]        │
└──────────────┴──────────────────────────┴───────────────┘
```

The UI exposes all six workflow steps as tabs, lets you set apply/hold/skip
directly, and shows evidence buckets colour-coded by type.

---

## Candidate profile

Edit `data/candidate_profile.json`. Key fields the scorer uses:

| Field | Purpose |
|-------|---------|
| `skills.*` | list of skills with `evidence: direct\|adjacent\|familiar` |
| `domains` | industry/problem-space familiarity |
| `job_targets.seniority_self_assessed` | for seniority match scoring |
| `job_targets.desired_remote_policy` | for logistics scoring |
| `job_targets.work_authorization` | for visa/sponsorship blocker detection |

Edit `data/project_inventory.json` to list side-projects. Skills listed there
count as `adjacent` evidence in scoring and feed the project recommender.

---

## Scoring rubric

| Dimension | Weight | What it measures |
|-----------|--------|-----------------|
| Must-have coverage | 40% | Required-skill match |
| Nice-to-have coverage | 20% | Preferred-skill match |
| Domain alignment | 15% | Industry/problem-space fit |
| Seniority match | 15% | Level match |
| Logistics | 10% | Remote policy, visa, relocation |

**Verdicts:** Strong fit (≥70%) · Reach but viable (≥50%) · Long shot (≥30%) · Skip (<30%)

Hard blockers (sponsorship conflict, missing clearance, relocation mismatch)
force verdict to **Skip** regardless of score.

Evidence weights: `direct` 1.0 · `adjacent` 0.5 · `familiar` 0.2

---

## Generated asset design

All generation is rule-based and deterministic — no LLM.

- **Resume**: selects verbatim bullets from the ingested base resume, ranked by
  relevance to the JD's required/preferred skills. Top bullets per section are
  included in source-line order.
- **Cover letter**: selects verbatim proof-point fragments from the ingested
  base cover letter, supplemented by resume bullets for uncovered required
  skills. Builds a templated opening from direct-evidence skills only.
- **Project recommendations**: produces one new-project concept (targeting the
  primary skill gap) and one reposition-existing recommendation (if a credible
  candidate project scores ≥ 0.5). All measurable outcomes are framed as
  simulated/demo — no fabricated experience.

---

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/jobs` | List all jobs (with latest verdict) |
| GET | `/api/jobs/{id}/package` | Full application package for one job |
| POST | `/api/jobs/{id}/decision` | Set apply / hold / skip status |
| POST | `/api/jobs/{id}/generate-resume` | Generate targeted resume |
| POST | `/api/jobs/{id}/generate-cover-letter` | Generate targeted cover letter |
| POST | `/api/jobs/{id}/recommend-project` | Generate project recommendations |

Interactive API docs: `http://localhost:8080/docs`

---

## Running tests

```bash
pytest                  # all 428 tests
pytest tests/test_api.py -v    # API endpoint tests only
pytest tests/test_tracker.py   # tracker tests only
```

---

## Schema overview

| Table | Purpose |
|-------|---------|
| `jobs` | One row per ingested job description |
| `extracted_requirements` | Parsed skills/requirements per job |
| `extraction_runs` | Metadata for each extraction pass |
| `candidate_profiles` | Snapshot of candidate_profile.json at assessment time |
| `fit_assessments` | Scored assessments with evidence buckets |
| `base_resumes` | Ingested base resumes (bullets stored separately) |
| `resume_bullets` | Individual bullets from base resumes |
| `base_cover_letters` | Ingested base cover letters |
| `cover_letter_fragments` | Individual fragments (salutation/proof_point/closing) |
| `generated_assets` | Generated resume and cover letter markdown |
| `project_recommendations` | Portfolio project recommendations |
| `applications` | Application decisions (apply/hold/skip) with notes |
