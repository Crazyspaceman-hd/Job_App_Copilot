"""
app/main.py -- CLI entry point for the Job Application Copilot.

Usage examples:
    python -m app.main ingest --file path/to/jd.txt
    python -m app.main ingest                               # paste / pipe stdin
    python -m app.main list
    python -m app.main extract-requirements --job-id 1
    python -m app.main assess-fit --job-id 1
    python -m app.main assess-fit --job-id 1 --profile path/to/profile.json
"""

import argparse
import json
import sys
import textwrap
from pathlib import Path

from app.db import get_conn, init_db
from app.services.intake import ingest


# -- ingest -------------------------------------------------------------------

def cmd_ingest(args: argparse.Namespace) -> None:
    if args.file:
        with open(args.file, encoding="utf-8") as fh:
            raw_text = fh.read()
    else:
        if sys.stdin.isatty():
            print("Paste the job description below. "
                  "Press Ctrl-D (Unix) or Ctrl-Z Enter (Windows) when done.\n")
        raw_text = sys.stdin.read()

    if not raw_text.strip():
        print("[error] No text received. Aborting.", file=sys.stderr)
        sys.exit(1)

    init_db()
    with get_conn() as conn:
        job_id = ingest(raw_text, conn, source_url=args.url)

    print(f"[ok] Job ingested -- id={job_id}")
    print(f"     First 120 chars: {raw_text.strip()[:120]!r}")


# -- list ---------------------------------------------------------------------

def cmd_list(args: argparse.Namespace) -> None:
    init_db()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, ingested_at, status, title, company, remote_policy "
            "FROM jobs ORDER BY id DESC LIMIT 50"
        ).fetchall()

    if not rows:
        print("No jobs ingested yet. Run:  python -m app.main ingest --file <path>")
        return

    print(f"{'ID':>4}  {'Ingested':>19}  {'Status':>10}  {'Title':<40}  "
          f"{'Company':<20}  Remote")
    print("-" * 110)
    for r in rows:
        print(
            f"{r['id']:>4}  {r['ingested_at']:>19}  {r['status']:>10}  "
            f"{(r['title'] or '')[:40]:<40}  "
            f"{(r['company'] or '')[:20]:<20}  {r['remote_policy'] or ''}"
        )


# -- extract-requirements -----------------------------------------------------

def cmd_extract_requirements(args: argparse.Namespace) -> None:
    from app.services.extractor import extract, persist_extraction

    init_db()

    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, title, raw_text FROM jobs WHERE id = ?",
            (args.job_id,),
        ).fetchone()

    if not row:
        print(f"[error] Job id={args.job_id} not found. Run 'list' to see available jobs.",
              file=sys.stderr)
        sys.exit(1)

    result = extract(row["id"], row["raw_text"])

    with get_conn() as conn:
        run_id = persist_extraction(conn, result)

    _print_extraction(row, result, run_id)


def _print_extraction(job_row, result, run_id: int) -> None:
    W   = 68
    SEP = "=" * W

    title = (job_row["title"] or "Untitled")[:55]

    print()
    print(SEP)
    print(f"  EXTRACTION SUMMARY -- Job #{job_row['id']}")
    print(f"  {title}")
    print(SEP)

    conf_label = result.extraction_confidence.upper()
    print(f"\n  Extraction confidence: {conf_label}")

    _print_list("REQUIRED SKILLS", result.required_skills, empty="(none identified)")
    _print_list("PREFERRED SKILLS", result.preferred_skills, empty="(none identified)")

    yoe = result.years_of_experience
    if yoe:
        mx = f"-{yoe['max']}" if yoe.get("max") else "+"
        print(f"\n  YEARS OF EXPERIENCE:  {yoe['min']}{mx}  (raw: {yoe['raw']!r})")
    else:
        print("\n  YEARS OF EXPERIENCE:  not stated")

    print(f"\n  SENIORITY:  {result.seniority}")

    lc = result.logistics_constraints
    print("\n  LOGISTICS")
    print(f"    remote_policy      : {lc.get('remote_policy') or 'not stated'}")
    print(f"    no_sponsorship     : {lc.get('no_sponsorship', False)}")
    print(f"    clearance_required : {lc.get('clearance_required', False)}")
    print(f"    relocation_required: {lc.get('relocation_required', False)}")

    _print_list("EDUCATION REQUIREMENTS", result.education_requirements,
                empty="(none identified)")
    _print_list("DOMAIN REQUIREMENTS", result.domain_requirements, empty="(none identified)")

    if result.responsibilities:
        print(f"\n  RESPONSIBILITIES ({len(result.responsibilities)})")
        for r in result.responsibilities[:5]:
            print(f"    - {r[:80]}")
        if len(result.responsibilities) > 5:
            print(f"    ... and {len(result.responsibilities) - 5} more")

    kw_preview = ", ".join(result.ats_keywords[:12])
    if len(result.ats_keywords) > 12:
        kw_preview += f", ... ({len(result.ats_keywords)} total)"
    print(f"\n  ATS KEYWORDS:  {kw_preview or '(none)'}")

    if result.extraction_notes:
        print("\n  NOTES")
        for note in result.extraction_notes:
            for line in textwrap.wrap(note, width=W - 6):
                print(f"    * {line}")

    req_rows = (len(result.required_skills) + len(result.preferred_skills)
                + len(result.responsibilities) + len(result.education_requirements)
                + len(result.domain_requirements))
    print()
    print(SEP)
    print(f"  Wrote {req_rows} rows to extracted_requirements.")
    print(f"  Extraction run id={run_id} saved to extraction_runs.")
    print(SEP)
    print()


def _print_list(header: str, items: list[str], empty: str = "(none)") -> None:
    if items:
        print(f"\n  {header} ({len(items)})")
        for item in items[:10]:
            print(f"    - {item}")
        if len(items) > 10:
            print(f"    ... and {len(items) - 10} more")
    else:
        print(f"\n  {header}")
        print(f"    {empty}")


# -- assess-fit ---------------------------------------------------------------

def cmd_assess_fit(args: argparse.Namespace) -> None:
    from app.services.profile_loader import load_profile, completeness
    from app.services.project_loader import load_projects, extract_project_skills
    from app.services.scorer import assess, persist_assessment
    from app.services.extractor import load_latest_extraction

    init_db()

    # Load job from DB
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, title, company, remote_policy, raw_text FROM jobs WHERE id = ?",
            (args.job_id,),
        ).fetchone()

    if not row:
        print(f"[error] Job id={args.job_id} not found. Run 'list' to see available jobs.",
              file=sys.stderr)
        sys.exit(1)

    # Load structured extraction if available
    with get_conn() as conn:
        extracted = load_latest_extraction(conn, row["id"])

    if extracted:
        print(f"[info] Using extracted requirements "
              f"(confidence: {extracted.extraction_confidence})")
    else:
        print("[info] No extraction found for this job. "
              "Run 'extract-requirements' first for better accuracy.")

    # Load profile and projects
    profile_path = Path(args.profile) if args.profile else None
    try:
        profile = load_profile(profile_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)

    projects      = load_projects()
    proj_skills   = extract_project_skills(projects)
    prof_complete = completeness(profile)

    # Run scorer (passes extracted if available)
    result = assess(
        job_raw_text      = row["raw_text"],
        job_remote_policy = row["remote_policy"],
        profile           = profile,
        project_skills    = proj_skills,
        profile_complete  = prof_complete,
        extracted         = extracted,
    )

    # Persist profile snapshot + assessment (with evidence buckets)
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO candidate_profiles (version, profile_json) VALUES (?, ?)",
            (profile.get("version", "1.0"), json.dumps(profile)),
        )
        profile_id    = cur.lastrowid
        assessment_id = persist_assessment(conn, row["id"], profile_id, result)

    _print_assessment(row, result, assessment_id)


def _print_assessment(job_row, result, assessment_id: int) -> None:
    W   = 68
    SEP = "=" * W

    def bar(score: float, width: int = 10) -> str:
        filled = round(score * width)
        return "#" * filled + "." * (width - filled)

    title   = (job_row["title"] or "Untitled")[:50]
    company = (job_row["company"] or "Unknown company")[:30]

    print()
    print(SEP)
    print(f"  FIT ASSESSMENT -- Job #{job_row['id']}")
    print(f"  {title}")
    print(f"  {company}")
    print(SEP)

    print(f"\n  VERDICT:    {result.verdict.upper()}")
    print(f"  Score:      {result.overall_score:.0%}   "
          f"Confidence: {result.confidence.upper()}")

    print(f"\n  SCORE BREAKDOWN  {'-'*44}")
    _score_line("Must-have coverage  ", result.must_have_score,    "40%", bar)
    _score_line("Nice-to-have        ", result.nice_to_have_score, "20%", bar)
    _score_line("Domain alignment    ", result.domain_score,       "15%", bar)
    _score_line("Seniority match     ", result.seniority_score,    "15%", bar)
    _score_line("Logistics           ", result.logistics_score,    "10%", bar)
    print()
    _score_line("ATS keyword overlap ", result.ats_score,          "n/a", bar,
                note=" (informational)")

    if result.strengths:
        print("\n  STRENGTHS")
        for s in result.strengths[:8]:
            print(f"    + {s}")

    if result.gaps:
        print("\n  GAPS")
        for g in result.gaps[:8]:
            print(f"    - {g}")
        if len(result.gaps) > 8:
            print(f"    ... and {len(result.gaps) - 8} more")

    # Machine-readable evidence bucket summary
    print("\n  EVIDENCE BUCKETS  (required skills)")
    if result.direct_evidence:
        print(f"    Direct   ({len(result.direct_evidence)}): "
              + ", ".join(result.direct_evidence[:10])
              + (" ..." if len(result.direct_evidence) > 10 else ""))
    else:
        print("    Direct   (0): —")
    if result.adjacent_evidence:
        print(f"    Adjacent ({len(result.adjacent_evidence)}): "
              + ", ".join(result.adjacent_evidence[:10])
              + (" ..." if len(result.adjacent_evidence) > 10 else ""))
    else:
        print("    Adjacent (0): —")
    if result.unsupported_gaps:
        print(f"    Gaps     ({len(result.unsupported_gaps)}): "
              + ", ".join(result.unsupported_gaps[:10])
              + (" ..." if len(result.unsupported_gaps) > 10 else ""))
    else:
        print("    Gaps     (0): —")

    print("\n  RED FLAGS / HARD BLOCKERS")
    if result.hard_blockers:
        for r in result.hard_blockers:
            print(f"    ! {r}")
    else:
        print("    (none detected)")

    print("\n  RATIONALE")
    for line in textwrap.wrap(result.rationale, width=W - 4):
        print(f"    {line}")

    print()
    print(SEP)
    print(f"  Assessment id={assessment_id} saved to fit_assessments.")
    print(SEP)
    print()


def _score_line(label: str, score: float, weight: str, bar_fn, note: str = "") -> None:
    print(f"  {label}  {score:.2f}  [{bar_fn(score)}]  (weight {weight}){note}")


# -- ingest-resume ------------------------------------------------------------

def cmd_ingest_resume(args: argparse.Namespace) -> None:
    from app.services.base_asset_ingest import ingest_resume

    with open(args.file, encoding="utf-8") as fh:
        raw_text = fh.read()

    if not raw_text.strip():
        print("[error] File is empty. Aborting.", file=sys.stderr)
        sys.exit(1)

    init_db()
    with get_conn() as conn:
        result = ingest_resume(raw_text, conn, label=args.label)

    W   = 68
    SEP = "=" * W
    print()
    print(SEP)
    print(f"  RESUME INGESTED  (id={result.resume_id})")
    print(f"  Label: {result.label}")
    print(SEP)
    print(f"\n  Sections detected ({len(result.sections)}):")
    for sec in result.sections:
        bullet_note = f"  [{len(sec.bullets)} bullet(s)]"
        heading_display = f'"{sec.heading}"' if sec.heading else f'[{sec.name}]'
        print(f"    {sec.name:<20} {heading_display:<40}{bullet_note}")
    print(f"\n  Total bullets : {len(result.bullet_bank)}")
    print(f"  Unique skills : {len(result.skills)}")
    if result.skills:
        skill_preview = ", ".join(result.skills[:15])
        if len(result.skills) > 15:
            skill_preview += f", ... ({len(result.skills)} total)"
        print(f"  Skills        : {skill_preview}")
    print()
    print(SEP)
    print()


# -- ingest-cover-letter ------------------------------------------------------

def cmd_ingest_cover_letter(args: argparse.Namespace) -> None:
    from app.services.base_asset_ingest import ingest_cover_letter

    with open(args.file, encoding="utf-8") as fh:
        raw_text = fh.read()

    if not raw_text.strip():
        print("[error] File is empty. Aborting.", file=sys.stderr)
        sys.exit(1)

    init_db()
    with get_conn() as conn:
        result = ingest_cover_letter(raw_text, conn, label=args.label)

    W   = 68
    SEP = "=" * W
    print()
    print(SEP)
    print(f"  COVER LETTER INGESTED  (id={result.cl_id})")
    print(f"  Label: {result.label}")
    print(SEP)
    print(f"\n  Fragments detected ({len(result.fragments)}):")
    for frag in result.fragments:
        preview = frag.text[:70] + ("…" if len(frag.text) > 70 else "")
        print(f"    [{frag.kind:<11}] line {frag.source_line:>3}  {preview!r}")
    print()
    print(SEP)
    print()


# -- generate-resume ----------------------------------------------------------

def cmd_generate_resume(args: argparse.Namespace) -> None:
    from app.services.resume_tailor import generate_targeted_resume
    from app.services.base_asset_ingest import load_latest_base_resume
    from app.services.profile_loader import load_profile, completeness
    from app.services.extractor import load_latest_extraction

    init_db()

    # Load job
    with get_conn() as conn:
        job_row = conn.execute(
            "SELECT id, title, company, remote_policy, raw_text FROM jobs WHERE id = ?",
            (args.job_id,),
        ).fetchone()

    if not job_row:
        print(f"[error] Job id={args.job_id} not found. Run 'list' to see available jobs.",
              file=sys.stderr)
        sys.exit(1)

    # Load extraction (optional)
    with get_conn() as conn:
        extracted = load_latest_extraction(conn, args.job_id)

    if extracted:
        print(f"[info] Using structured extraction "
              f"(confidence: {extracted.extraction_confidence})")
    else:
        print("[info] No extraction found — run 'extract-requirements' first for "
              "better bullet ranking accuracy.")

    # Load profile
    profile_path = Path(args.profile) if args.profile else None
    try:
        profile = load_profile(profile_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)

    # Load base resume
    resume_id = args.resume_id  # None → latest
    with get_conn() as conn:
        base_resume = load_latest_base_resume(conn, resume_id=resume_id)

    if not base_resume:
        print("[error] No base resume found. Run 'ingest-resume' first.", file=sys.stderr)
        sys.exit(1)

    print(f"[info] Using base resume id={base_resume.resume_id} "
          f"(label={base_resume.label!r}, "
          f"{len(base_resume.bullet_bank)} bullets)")

    # Generate
    with get_conn() as conn:
        result = generate_targeted_resume(
            job_id    = args.job_id,
            conn      = conn,
            profile   = profile,
            base_resume = base_resume,
            extracted = extracted,
            label     = args.label,
        )

    _print_tailored_resume(job_row, result)

    # Optionally write markdown to file
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(result.markdown, encoding="utf-8")
        print(f"[ok] Markdown written to {out}")


def _print_tailored_resume(job_row, result) -> None:
    W   = 68
    SEP = "=" * W

    title   = (job_row["title"]   or "Untitled")[:50]
    company = (job_row["company"] or "Unknown")[:30]

    print()
    print(SEP)
    print(f"  TARGETED RESUME — Job #{job_row['id']}")
    print(f"  {title}")
    print(f"  {company}")
    print(SEP)

    prov = result.provenance
    print(f"\n  Base resume id : {prov.base_resume_id}")
    print(f"  Asset id       : {result.asset_id}  (saved to generated_assets)")
    print(f"  Used extraction: {prov.used_extraction}")
    print(f"  Bullets        : {prov.total_bullets_selected} selected "
          f"/ {prov.total_bullets_available} available")

    if prov.direct_evidence_used:
        print(f"\n  Direct evidence  ({len(prov.direct_evidence_used)}): "
              + ", ".join(prov.direct_evidence_used[:10])
              + (" ..." if len(prov.direct_evidence_used) > 10 else ""))
    if prov.adjacent_evidence_referenced:
        print(f"  Adjacent refs    ({len(prov.adjacent_evidence_referenced)}): "
              + ", ".join(prov.adjacent_evidence_referenced[:10])
              + (" ..." if len(prov.adjacent_evidence_referenced) > 10 else ""))
    if prov.unsupported_gaps_excluded:
        print(f"  Gaps excluded    ({len(prov.unsupported_gaps_excluded)}): "
              + ", ".join(prov.unsupported_gaps_excluded[:10])
              + (" ..." if len(prov.unsupported_gaps_excluded) > 10 else ""))

    print(f"\n  GENERATED SUMMARY")
    for line in textwrap.wrap(result.summary, width=W - 4):
        print(f"    {line}")

    print(f"\n  SKILLS (relevance order, first category shown)")
    if result.skills_section:
        first_cat = next(iter(result.skills_section))
        names = result.skills_section[first_cat]
        print(f"    {first_cat}: {', '.join(names[:10])}"
              + (" ..." if len(names) > 10 else ""))

    print(f"\n  TOP BULLETS (by relevance score)")
    for bs in result.scored_bullets[:5]:
        preview = bs.bullet.text[:75] + ("…" if len(bs.bullet.text) > 75 else "")
        print(f"    [{bs.score:5.2f}]  {preview}")

    print()
    print(SEP)
    print(f"  Full markdown preview (first 30 lines):")
    print(SEP)
    for line in result.markdown.splitlines()[:30]:
        print(f"  {line}")
    lines_total = len(result.markdown.splitlines())
    if lines_total > 30:
        print(f"  ... ({lines_total - 30} more lines)")
    print(SEP)
    print()


# -- generate-cover-letter ----------------------------------------------------

def cmd_generate_cover_letter(args: argparse.Namespace) -> None:
    from app.services.cover_letter import generate_targeted_cover_letter
    from app.services.base_asset_ingest import (
        load_latest_cover_letter,
        load_latest_base_resume,
    )
    from app.services.profile_loader import load_profile
    from app.services.extractor import load_latest_extraction

    init_db()

    # Load job
    with get_conn() as conn:
        job_row = conn.execute(
            "SELECT id, title, company, remote_policy, raw_text FROM jobs WHERE id = ?",
            (args.job_id,),
        ).fetchone()

    if not job_row:
        print(f"[error] Job id={args.job_id} not found. Run 'list' to see available jobs.",
              file=sys.stderr)
        sys.exit(1)

    # Load extraction (optional)
    with get_conn() as conn:
        extracted = load_latest_extraction(conn, args.job_id)

    if extracted:
        print(f"[info] Using structured extraction "
              f"(confidence: {extracted.extraction_confidence})")
    else:
        print("[info] No extraction found — run 'extract-requirements' first for "
              "better proof point ranking accuracy.")

    # Load profile
    profile_path = Path(args.profile) if args.profile else None
    try:
        profile = load_profile(profile_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)

    # Load base cover letter
    cl_id = args.cl_id  # None → latest
    with get_conn() as conn:
        base_cl = load_latest_cover_letter(conn, cl_id=cl_id)

    if not base_cl:
        print("[error] No base cover letter found. Run 'ingest-cover-letter' first.",
              file=sys.stderr)
        sys.exit(1)

    print(f"[info] Using base cover letter id={base_cl.cl_id} "
          f"(label={base_cl.label!r}, {len(base_cl.fragments)} fragments)")

    # Load base resume (optional supplement)
    with get_conn() as conn:
        base_resume = load_latest_base_resume(conn, resume_id=args.resume_id)

    if base_resume:
        print(f"[info] Using base resume id={base_resume.resume_id} "
              f"(label={base_resume.label!r}) for supplemental proof points")
    else:
        print("[info] No base resume — proof points from cover letter only")

    # Generate
    with get_conn() as conn:
        result = generate_targeted_cover_letter(
            job_id      = args.job_id,
            conn        = conn,
            profile     = profile,
            base_cl     = base_cl,
            extracted   = extracted,
            base_resume = base_resume,
            label       = args.label,
        )

    _print_targeted_cl(job_row, result)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(result.markdown, encoding="utf-8")
        print(f"[ok] Markdown written to {out}")


def _print_targeted_cl(job_row, result) -> None:
    W   = 68
    SEP = "=" * W

    title   = (job_row["title"]   or "Untitled")[:50]
    company = (job_row["company"] or "Unknown")[:30]

    print()
    print(SEP)
    print(f"  TARGETED COVER LETTER — Job #{job_row['id']}")
    print(f"  {title}")
    print(f"  {company}")
    print(SEP)

    prov = result.provenance
    print(f"\n  Base CL id     : {prov.base_cl_id}")
    if prov.base_resume_id is not None:
        print(f"  Base resume id : {prov.base_resume_id}")
    print(f"  Asset id       : {result.asset_id}  (saved to generated_assets)")
    print(f"  Used extraction: {prov.used_extraction}")
    print(f"  Proof points   : {len(result.proof_points)}"
          f"  (adjacency para: {prov.included_adjacency_para})")

    if prov.direct_evidence_used:
        print(f"\n  Direct evidence  ({len(prov.direct_evidence_used)}): "
              + ", ".join(prov.direct_evidence_used[:10])
              + (" ..." if len(prov.direct_evidence_used) > 10 else ""))
    if prov.adjacent_evidence_referenced:
        print(f"  Adjacent refs    ({len(prov.adjacent_evidence_referenced)}): "
              + ", ".join(prov.adjacent_evidence_referenced[:10])
              + (" ..." if len(prov.adjacent_evidence_referenced) > 10 else ""))
    if prov.unsupported_gaps_excluded:
        print(f"  Gaps excluded    ({len(prov.unsupported_gaps_excluded)}): "
              + ", ".join(prov.unsupported_gaps_excluded[:10])
              + (" ..." if len(prov.unsupported_gaps_excluded) > 10 else ""))

    print(f"\n  OPENING")
    for line in textwrap.wrap(result.opening, width=W - 4):
        print(f"    {line}")

    print(f"\n  PROOF POINTS ({len(result.proof_points)})")
    for i, pp in enumerate(result.proof_points, 1):
        src = f"[{pp.source_type} line {pp.source_line}  score={pp.score:.2f}]"
        preview = pp.text[:75] + ("…" if len(pp.text) > 75 else "")
        print(f"    {i}. {src}")
        print(f"       {preview!r}")

    print()
    print(SEP)
    print(f"  Full markdown preview (first 30 lines):")
    print(SEP)
    for line in result.markdown.splitlines()[:30]:
        print(f"  {line}")
    lines_total = len(result.markdown.splitlines())
    if lines_total > 30:
        print(f"  ... ({lines_total - 30} more lines)")
    print(SEP)
    print()


# -- recommend-project --------------------------------------------------------

def cmd_recommend_project(args: argparse.Namespace) -> None:
    from app.services.project_recommender import recommend_project
    from app.services.project_loader import load_projects
    from app.services.profile_loader import load_profile
    from app.services.extractor import load_latest_extraction

    init_db()

    # Load job
    with get_conn() as conn:
        job_row = conn.execute(
            "SELECT id, title, company, remote_policy, raw_text FROM jobs WHERE id = ?",
            (args.job_id,),
        ).fetchone()

    if not job_row:
        print(f"[error] Job id={args.job_id} not found. Run 'list' to see available jobs.",
              file=sys.stderr)
        sys.exit(1)

    # Load extraction (optional)
    with get_conn() as conn:
        extracted = load_latest_extraction(conn, args.job_id)

    if extracted:
        print(f"[info] Using structured extraction "
              f"(confidence: {extracted.extraction_confidence})")
    else:
        print("[info] No extraction found — run 'extract-requirements' first for "
              "better recommendation accuracy.")

    # Load profile
    profile_path = Path(args.profile) if args.profile else None
    try:
        profile = load_profile(profile_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)

    # Load project inventory
    inv_path = Path(args.inventory) if args.inventory else None
    try:
        projects = load_projects(inv_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[warning] Could not load project inventory: {exc}", file=sys.stderr)
        projects = []

    if projects:
        print(f"[info] Loaded {len(projects)} project(s) from inventory")
    else:
        print("[info] No project inventory — reposition recommendation may be skipped")

    # Generate recommendations
    with get_conn() as conn:
        result = recommend_project(
            job_id    = args.job_id,
            conn      = conn,
            profile   = profile,
            extracted = extracted,
            projects  = projects,
            label     = args.label,
        )

    _print_recommendations(job_row, result)

    # Optionally write markdown to file
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        md  = _recommendations_to_markdown(job_row, result)
        out.write_text(md, encoding="utf-8")
        print(f"[ok] Recommendations written to {out}")


def _print_recommendations(job_row, result) -> None:
    W   = 72
    SEP = "=" * W

    title   = (job_row["title"]   or "Untitled")[:55]
    company = (job_row["company"] or "Unknown")[:35]

    print()
    print(SEP)
    print(f"  PROJECT RECOMMENDATIONS — Job #{job_row['id']}")
    print(f"  {title}")
    print(f"  {company}")
    print(SEP)

    prov = result.provenance
    print(f"\n  Primary gap targeted : {prov.primary_gap or '(none)'}")
    print(f"  Used extraction      : {prov.used_extraction}")
    print(f"  Projects evaluated   : {prov.projects_considered}")
    if prov.gaps_considered:
        print(f"  Gaps considered      : "
              + ", ".join(prov.gaps_considered[:8])
              + (" ..." if len(prov.gaps_considered) > 8 else ""))
    if prov.adjacent_considered:
        print(f"  Adjacent signals     : "
              + ", ".join(prov.adjacent_considered[:6]))

    _print_rec_block("NEW PROJECT RECOMMENDATION", result.new_project, W)

    if result.reposition_existing:
        _print_rec_block("REPOSITION EXISTING PROJECT", result.reposition_existing, W)
    else:
        print(f"\n  REPOSITION EXISTING PROJECT")
        print(f"    (no credible reposition candidate found — "
              f"add projects to data/project_inventory.json)")

    print()
    print(SEP)
    print(f"  new_project   -> asset id={result.new_project.asset_id} "
          f"(saved to project_recommendations)")
    if result.reposition_existing:
        print(f"  reposition    -> asset id={result.reposition_existing.asset_id}")
    print(SEP)
    print()


def _print_rec_block(header: str, rec, W: int) -> None:
    SEP2 = "-" * W
    print(f"\n  {header}")
    print(f"  {SEP2}")
    print(f"  Title  : {rec.title}")
    print(f"  Type   : {rec.recommendation_type}")
    print(f"  Target : {rec.target_gap_or_signal}")
    print()
    print(f"  WHY THIS MATCHES")
    for line in textwrap.wrap(rec.why_this_matches, width=W - 4):
        print(f"    {line}")
    print()
    print(f"  BUSINESS PROBLEM")
    for line in textwrap.wrap(rec.business_problem, width=W - 4):
        print(f"    {line}")
    print()
    print(f"  STACK:  {', '.join(rec.stack)}")
    print()
    print(f"  SCOPED VERSION")
    for line in textwrap.wrap(rec.scoped_version, width=W - 4):
        print(f"    {line}")
    print()
    print(f"  MEASURABLE OUTCOMES")
    for outcome in rec.measurable_outcomes:
        for line in textwrap.wrap(f"• {outcome}", width=W - 4):
            print(f"    {line}")
    print()
    print(f"  RESUME VALUE")
    for line in textwrap.wrap(rec.resume_value, width=W - 4):
        print(f"    {line}")


def _recommendations_to_markdown(job_row, result) -> str:
    title   = job_row["title"]   or "Untitled"
    company = job_row["company"] or "Unknown"
    prov    = result.provenance

    lines = [
        f"# Project Recommendations — {title}",
        f"**Company:** {company}  |  **Job id:** {job_row['id']}",
        "",
        f"**Primary gap targeted:** {prov.primary_gap}",
        f"**Gaps considered:** {', '.join(prov.gaps_considered) or '(none)'}",
        "",
        "---",
        "",
    ]

    def _rec_section(heading: str, rec) -> list[str]:
        s = [
            f"## {heading}",
            "",
            f"**Title:** {rec.title}",
            f"**Type:** `{rec.recommendation_type}`",
            f"**Targets:** {rec.target_gap_or_signal}",
            "",
            f"### Why This Matches",
            rec.why_this_matches,
            "",
            f"### Business Problem",
            rec.business_problem,
            "",
            f"### Stack",
            ", ".join(f"`{s}`" for s in rec.stack),
            "",
            f"### Scoped Version",
            rec.scoped_version,
            "",
            f"### Measurable Outcomes",
        ]
        for o in rec.measurable_outcomes:
            s.append(f"- {o}")
        s += ["", f"### Resume Value", rec.resume_value, "", f"### Implementation Notes",
              rec.implementation_notes, ""]
        return s

    lines += _rec_section("New Project Recommendation", result.new_project)

    if result.reposition_existing:
        lines += ["---", ""]
        lines += _rec_section("Reposition Existing Project", result.reposition_existing)
    else:
        lines += [
            "---",
            "",
            "## Reposition Existing Project",
            "",
            "_No credible reposition candidate found. "
            "Add projects to `data/project_inventory.json` to enable this recommendation._",
            "",
        ]

    return "\n".join(lines)


# -- set-application-status ---------------------------------------------------

def cmd_set_application_status(args: argparse.Namespace) -> None:
    from app.services.tracker import save_application_decision, VALID_STATUSES

    init_db()

    with get_conn() as conn:
        job_row = conn.execute(
            "SELECT id, title, company FROM jobs WHERE id = ?",
            (args.job_id,),
        ).fetchone()

    if not job_row:
        print(f"[error] Job id={args.job_id} not found. Run 'list' to see available jobs.",
              file=sys.stderr)
        sys.exit(1)

    if args.status not in VALID_STATUSES:
        print(f"[error] Invalid status {args.status!r}. Must be one of: "
              f"{sorted(VALID_STATUSES)}", file=sys.stderr)
        sys.exit(1)

    with get_conn() as conn:
        app_id = save_application_decision(
            job_id         = args.job_id,
            conn           = conn,
            status         = args.status,
            notes          = args.notes,
            follow_up_date = args.follow_up,
            platform       = args.platform,
        )

    title   = job_row["title"]   or "Untitled"
    company = job_row["company"] or "Unknown"
    print(f"[ok] Application decision saved (id={app_id})")
    print(f"     Job #{args.job_id}: {title} @ {company}")
    print(f"     Status: {args.status}")
    if args.notes:
        print(f"     Notes: {args.notes}")
    if args.follow_up:
        print(f"     Follow-up: {args.follow_up}")
    if args.platform:
        print(f"     Platform: {args.platform}")


# -- show-package -------------------------------------------------------------

def cmd_show_package(args: argparse.Namespace) -> None:
    from app.services.tracker import load_application_package

    init_db()

    with get_conn() as conn:
        try:
            pkg = load_application_package(args.job_id, conn)
        except ValueError as exc:
            print(f"[error] {exc}", file=sys.stderr)
            sys.exit(1)

    _print_package(pkg)


def _print_package(pkg) -> None:
    W   = 72
    SEP = "=" * W
    SEP2 = "-" * W

    title   = (pkg.job_title   or "Untitled")[:55]
    company = (pkg.job_company or "Unknown")[:35]

    print()
    print(SEP)
    print(f"  APPLICATION PACKAGE — Job #{pkg.job_id}")
    print(f"  {title}")
    print(f"  {company}")
    print(SEP)

    # Job basics
    print(f"\n  JOB BASICS")
    print(f"    Remote policy : {pkg.job_remote_policy or 'not stated'}")
    print(f"    DB status     : {pkg.job_status}")

    # Fit assessment
    print(f"\n  FIT ASSESSMENT")
    if pkg.assessment_id:
        print(f"    Assessment id : {pkg.assessment_id}")
        print(f"    Assessed at   : {pkg.assessed_at}")
        print(f"    Verdict       : {(pkg.verdict or '').upper()}")
        print(f"    Score         : {pkg.overall_score:.0%}" if pkg.overall_score is not None
              else "    Score         : n/a")
        print(f"    Confidence    : {pkg.confidence or 'n/a'}")
        if pkg.direct_evidence:
            print(f"    Direct  ({len(pkg.direct_evidence)}): "
                  + ", ".join(pkg.direct_evidence[:8])
                  + (" ..." if len(pkg.direct_evidence) > 8 else ""))
        if pkg.adjacent_evidence:
            print(f"    Adjacent({len(pkg.adjacent_evidence)}): "
                  + ", ".join(pkg.adjacent_evidence[:8])
                  + (" ..." if len(pkg.adjacent_evidence) > 8 else ""))
        if pkg.unsupported_gaps:
            print(f"    Gaps    ({len(pkg.unsupported_gaps)}): "
                  + ", ".join(pkg.unsupported_gaps[:8])
                  + (" ..." if len(pkg.unsupported_gaps) > 8 else ""))
    else:
        print(f"    (no assessment yet — run 'assess-fit --job-id {pkg.job_id}')")

    # Generated resume
    print(f"\n  GENERATED RESUME")
    if pkg.resume:
        r = pkg.resume
        print(f"    Asset id      : {r.asset_id}")
        print(f"    Label         : {r.label or 'n/a'}")
        print(f"    Generated at  : {r.generated_at}")
        preview = (r.content_preview or "")[:100].replace("\n", " ")
        print(f"    Preview       : {preview!r}")
    else:
        print(f"    (none — run 'generate-resume --job-id {pkg.job_id}')")

    # Generated cover letter
    print(f"\n  GENERATED COVER LETTER")
    if pkg.cover_letter:
        cl = pkg.cover_letter
        print(f"    Asset id      : {cl.asset_id}")
        print(f"    Label         : {cl.label or 'n/a'}")
        print(f"    Generated at  : {cl.generated_at}")
        preview = (cl.content_preview or "")[:100].replace("\n", " ")
        print(f"    Preview       : {preview!r}")
    else:
        print(f"    (none — run 'generate-cover-letter --job-id {pkg.job_id}')")

    # Project recommendations
    print(f"\n  PROJECT RECOMMENDATIONS ({len(pkg.recommendations)})")
    if pkg.recommendations:
        for rec in pkg.recommendations[:5]:
            tag = rec.recommendation_type or "?"
            print(f"    [{tag:<20}] id={rec.rec_id}  {rec.title}")
            if rec.target_gap_or_signal:
                print(f"      Target: {rec.target_gap_or_signal}")
        if len(pkg.recommendations) > 5:
            print(f"    ... and {len(pkg.recommendations) - 5} more")
    else:
        print(f"    (none — run 'recommend-project --job-id {pkg.job_id}')")

    # Application decision
    print(f"\n  APPLICATION STATUS")
    app = pkg.application
    if app.application_id:
        print(f"    Decision id   : {app.application_id}")
        print(f"    Status        : {app.status}")
        print(f"    Last updated  : {app.last_updated}")
        if app.platform:
            print(f"    Platform      : {app.platform}")
        if app.follow_up_date:
            print(f"    Follow-up     : {app.follow_up_date}")
        if app.notes:
            print(f"    Notes         :")
            for line in textwrap.wrap(app.notes, width=W - 18):
                print(f"      {line}")
    else:
        print(f"    (no decision yet — run "
              f"'set-application-status --job-id {pkg.job_id} --status apply|hold|skip')")

    print()
    print(SEP)
    print()


# -- Argument parser ----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="copilot",
        description="Job Application Copilot -- CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python -m app.main ingest --file jd.txt
              python -m app.main list
              python -m app.main extract-requirements --job-id 1
              python -m app.main assess-fit --job-id 1
              python -m app.main assess-fit --job-id 1 --profile data/myprofile.json
              python -m app.main ingest-resume --file data/sample_resume.txt --label v1
              python -m app.main ingest-cover-letter --file data/sample_cover_letter.txt
              python -m app.main generate-resume --job-id 1
              python -m app.main generate-resume --job-id 1 --output out/resume.md
              python -m app.main generate-cover-letter --job-id 1
              python -m app.main generate-cover-letter --job-id 1 --output out/cl.md
              python -m app.main recommend-project --job-id 1
              python -m app.main recommend-project --job-id 1 --output out/recs.md
              python -m app.main set-application-status --job-id 1 --status apply
              python -m app.main set-application-status --job-id 1 --status hold --notes "waiting for referral"
              python -m app.main show-package --job-id 1
        """),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ingest
    p_ingest = sub.add_parser("ingest", help="Ingest a job description")
    p_ingest.add_argument("--file", "-f", metavar="PATH",
                          help="Path to a .txt file containing the JD")
    p_ingest.add_argument("--url", "-u", metavar="URL",
                          help="Source URL for reference (optional)")
    p_ingest.set_defaults(func=cmd_ingest)

    # list
    p_list = sub.add_parser("list", help="List ingested jobs")
    p_list.set_defaults(func=cmd_list)

    # extract-requirements
    p_extract = sub.add_parser("extract-requirements",
                                help="Extract structured requirements from a job")
    p_extract.add_argument("--job-id", "-j", type=int, required=True,
                           metavar="ID", help="Job id (from 'list')")
    p_extract.set_defaults(func=cmd_extract_requirements)

    # assess-fit
    p_assess = sub.add_parser("assess-fit", help="Run a fit assessment for a job")
    p_assess.add_argument("--job-id", "-j", type=int, required=True,
                          metavar="ID", help="Job id (from 'list')")
    p_assess.add_argument("--profile", "-p", metavar="PATH",
                          help="Path to candidate profile JSON "
                               "(default: data/candidate_profile.json)")
    p_assess.set_defaults(func=cmd_assess_fit)

    # ingest-resume
    p_resume = sub.add_parser("ingest-resume",
                               help="Ingest a base resume for reuse in tailoring")
    p_resume.add_argument("--file", "-f", metavar="PATH", required=True,
                          help="Path to a plain-text or markdown resume file")
    p_resume.add_argument("--label", "-l", metavar="LABEL", default="default",
                          help="Short name for this resume version (default: 'default')")
    p_resume.set_defaults(func=cmd_ingest_resume)

    # ingest-cover-letter
    p_cl = sub.add_parser("ingest-cover-letter",
                           help="Ingest a base cover letter for reuse in tailoring")
    p_cl.add_argument("--file", "-f", metavar="PATH", required=True,
                      help="Path to a plain-text or markdown cover letter file")
    p_cl.add_argument("--label", "-l", metavar="LABEL", default="default",
                      help="Short name for this cover letter version (default: 'default')")
    p_cl.set_defaults(func=cmd_ingest_cover_letter)

    # generate-resume
    p_gen = sub.add_parser("generate-resume",
                            help="Generate a targeted resume draft for a given job")
    p_gen.add_argument("--job-id", "-j", type=int, required=True,
                       metavar="ID", help="Job id (from 'list')")
    p_gen.add_argument("--profile", "-p", metavar="PATH",
                       help="Path to candidate profile JSON "
                            "(default: data/candidate_profile.json)")
    p_gen.add_argument("--resume-id", "-r", type=int, default=None, metavar="ID",
                       help="Base resume id to use (default: most recently ingested)")
    p_gen.add_argument("--label", "-l", metavar="LABEL", default="targeted",
                       help="Version label for the generated asset (default: 'targeted')")
    p_gen.add_argument("--output", "-o", metavar="PATH",
                       help="Also write the markdown to this file path (optional)")
    p_gen.set_defaults(func=cmd_generate_resume)

    # generate-cover-letter
    p_cl_gen = sub.add_parser("generate-cover-letter",
                               help="Generate a targeted cover letter draft for a given job")
    p_cl_gen.add_argument("--job-id", "-j", type=int, required=True,
                          metavar="ID", help="Job id (from 'list')")
    p_cl_gen.add_argument("--profile", "-p", metavar="PATH",
                          help="Path to candidate profile JSON "
                               "(default: data/candidate_profile.json)")
    p_cl_gen.add_argument("--cl-id", type=int, default=None, metavar="ID",
                          help="Base cover letter id to use (default: most recently ingested)")
    p_cl_gen.add_argument("--resume-id", "-r", type=int, default=None, metavar="ID",
                          help="Base resume id for supplemental proof points (optional)")
    p_cl_gen.add_argument("--label", "-l", metavar="LABEL", default="targeted",
                          help="Version label for the generated asset (default: 'targeted')")
    p_cl_gen.add_argument("--output", "-o", metavar="PATH",
                          help="Also write the markdown to this file path (optional)")
    p_cl_gen.set_defaults(func=cmd_generate_cover_letter)

    # recommend-project
    p_rec = sub.add_parser("recommend-project",
                            help="Generate portfolio project recommendations for a given job")
    p_rec.add_argument("--job-id", "-j", type=int, required=True,
                       metavar="ID", help="Job id (from 'list')")
    p_rec.add_argument("--profile", "-p", metavar="PATH",
                       help="Path to candidate profile JSON "
                            "(default: data/candidate_profile.json)")
    p_rec.add_argument("--inventory", "-i", metavar="PATH",
                       help="Path to project_inventory.json "
                            "(default: data/project_inventory.json)")
    p_rec.add_argument("--label", "-l", metavar="LABEL", default="targeted",
                       help="Version label for the saved recommendations (default: 'targeted')")
    p_rec.add_argument("--output", "-o", metavar="PATH",
                       help="Also write a markdown summary to this file path (optional)")
    p_rec.set_defaults(func=cmd_recommend_project)

    # set-application-status
    p_status = sub.add_parser("set-application-status",
                               help="Record an apply / hold / skip decision for a job")
    p_status.add_argument("--job-id", "-j", type=int, required=True,
                          metavar="ID", help="Job id (from 'list')")
    p_status.add_argument("--status", "-s", required=True,
                          choices=["apply", "hold", "skip"],
                          help="Decision: apply | hold | skip")
    p_status.add_argument("--notes", "-n", metavar="TEXT",
                          help="Free-text note (optional)")
    p_status.add_argument("--follow-up", metavar="DATE",
                          help="Follow-up date ISO format YYYY-MM-DD (optional)")
    p_status.add_argument("--platform", metavar="NAME",
                          help="Where the application was submitted (optional)")
    p_status.set_defaults(func=cmd_set_application_status)

    # show-package
    p_pkg = sub.add_parser("show-package",
                            help="Show a single-pane-of-glass summary for one job")
    p_pkg.add_argument("--job-id", "-j", type=int, required=True,
                       metavar="ID", help="Job id (from 'list')")
    p_pkg.set_defaults(func=cmd_show_package)

    return parser


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
