import { useEffect, useRef, useState } from 'react'
import type { CandidateProfile, IngestStatus, SkillLevel, SynthesisResult } from '../lib/api'
import { api } from '../lib/api'
import { CandidateAssessmentSection } from './CandidateAssessment'

// ── Constants ─────────────────────────────────────────────────────────────────

const SENIORITY_OPTIONS = ['', 'junior', 'mid', 'senior', 'staff', 'principal', 'lead']
const REMOTE_OPTIONS    = ['', 'remote', 'hybrid', 'onsite', 'flexible']
const AUTH_OPTIONS      = [
  '',
  'US Citizen',
  'Permanent Resident',
  'No sponsorship needed',
  'H1-B required',
  'OPT/STEM OPT',
  'EAD',
  'Other',
]

const SKILL_SECTIONS = [
  { key: 'languages',  label: 'Languages',  placeholder: 'Python' },
  { key: 'frameworks', label: 'Frameworks', placeholder: 'FastAPI' },
  { key: 'databases',  label: 'Databases',  placeholder: 'PostgreSQL' },
  { key: 'cloud',      label: 'Cloud',      placeholder: 'AWS' },
  { key: 'tools',      label: 'Tools',      placeholder: 'Docker' },
  { key: 'practices',  label: 'Practices',  placeholder: 'CI/CD' },
] as const

const LEVEL_LABELS: Record<SkillLevel, string> = {
  direct:   'direct',
  adjacent: 'adjacent',
  familiar: 'familiar',
}

// ── Chip data type ─────────────────────────────────────────────────────────────

export interface SkillChip {
  name:  string
  level: SkillLevel
}

// ── Helper: strip TODO placeholders ───────────────────────────────────────────

function stripTodo(s: string): string {
  return s.startsWith('TODO:') ? '' : s
}

// ── Profile ↔ form conversions ────────────────────────────────────────────────

function skillsToChips(arr: { name: string; evidence: string }[]): SkillChip[] {
  return arr
    .filter(s => s.name && !s.name.startsWith('TODO:'))
    .map(s => ({
      name:  s.name,
      level: (['direct', 'adjacent', 'familiar'].includes(s.evidence)
                ? s.evidence
                : 'familiar') as SkillLevel,
    }))
}

function chipsToSkills(chips: SkillChip[]) {
  return chips.map(c => ({ name: c.name, years: 0, evidence: c.level }))
}

function chipsToDomainsProfile(chips: SkillChip[]) {
  return chips.map(c => ({ name: c.name, evidence: c.level }))
}

interface SkillFormState {
  languages:  SkillChip[]
  frameworks: SkillChip[]
  databases:  SkillChip[]
  cloud:      SkillChip[]
  tools:      SkillChip[]
  practices:  SkillChip[]
  domains:    SkillChip[]
}

interface PersonalFormState {
  name:          string
  location:      string
  linkedin:      string
  github:        string
  titles:        string
  seniority:     string
  remote:        string
  authorization: string
  relocate:      boolean
}

const EMPTY_SKILLS: SkillFormState = {
  languages: [], frameworks: [], databases: [],
  cloud: [], tools: [], practices: [], domains: [],
}

const EMPTY_PERSONAL: PersonalFormState = {
  name: '', location: '', linkedin: '', github: '',
  titles: '', seniority: '', remote: '', authorization: '', relocate: false,
}

function profileToForm(p: CandidateProfile): { personal: PersonalFormState; skills: SkillFormState } {
  const jt = p.job_targets
  return {
    personal: {
      name:          stripTodo(p.personal.name),
      location:      stripTodo(p.personal.location),
      linkedin:      stripTodo(p.personal.linkedin),
      github:        stripTodo(p.personal.github),
      titles:        (jt.titles ?? []).join('\n'),
      seniority:     jt.seniority_self_assessed ?? '',
      remote:        jt.desired_remote_policy ?? '',
      authorization: jt.work_authorization ?? '',
      relocate:      jt.willing_to_relocate ?? false,
    },
    skills: {
      languages:  skillsToChips(p.skills.languages ?? []),
      frameworks: skillsToChips(p.skills.frameworks ?? []),
      databases:  skillsToChips(p.skills.databases ?? []),
      cloud:      skillsToChips(p.skills.cloud ?? []),
      tools:      skillsToChips(p.skills.tools ?? []),
      practices:  skillsToChips(p.skills.practices ?? []),
      domains:    skillsToChips(p.domains ?? []),
    },
  }
}

function buildProfile(
  personal: PersonalFormState,
  skills: SkillFormState,
  original: CandidateProfile | null,
): CandidateProfile {
  const base: CandidateProfile = original ?? {
    version:     '1.0',
    personal:    { name: '', location: '', linkedin: '', github: '' },
    job_targets: {
      titles: [], seniority_self_assessed: '', desired_remote_policy: '',
      willing_to_relocate: false, work_authorization: '',
    },
    skills:  { languages: [], frameworks: [], databases: [], cloud: [], tools: [], practices: [] },
    domains: [],
  }

  return {
    ...base,
    personal: {
      name:     personal.name     || 'TODO: your name',
      location: personal.location || 'TODO: your location',
      linkedin: personal.linkedin || 'TODO: your LinkedIn',
      github:   personal.github   || 'TODO: your GitHub',
    },
    job_targets: {
      titles:                  personal.titles.split('\n').map(t => t.trim()).filter(Boolean),
      seniority_self_assessed: personal.seniority,
      desired_remote_policy:   personal.remote,
      willing_to_relocate:     personal.relocate,
      work_authorization:      personal.authorization,
    },
    skills: {
      languages:  chipsToSkills(skills.languages),
      frameworks: chipsToSkills(skills.frameworks),
      databases:  chipsToSkills(skills.databases),
      cloud:      chipsToSkills(skills.cloud),
      tools:      chipsToSkills(skills.tools),
      practices:  chipsToSkills(skills.practices),
    },
    domains: chipsToDomainsProfile(skills.domains),
  }
}

function synthesisToSkills(result: SynthesisResult): SkillFormState {
  const toChips = (items: { name: string; level: string }[]): SkillChip[] =>
    items.map(s => ({ name: s.name, level: s.level as SkillLevel }))
  return {
    languages:  toChips(result.languages),
    frameworks: toChips(result.frameworks),
    databases:  toChips(result.databases),
    cloud:      toChips(result.cloud),
    tools:      toChips(result.tools),
    practices:  toChips(result.practices),
    domains:    toChips(result.domains),
  }
}

function hasAnySkills(skills: SkillFormState): boolean {
  return Object.values(skills).some(arr => arr.length > 0)
}

// ── Chip editor component ─────────────────────────────────────────────────────

function SkillEditor({
  label, placeholder, chips, onChange,
}: {
  label:       string
  placeholder: string
  chips:       SkillChip[]
  onChange:    (chips: SkillChip[]) => void
}) {
  const [newName, setNewName] = useState('')

  function addChip() {
    const name = newName.trim()
    if (!name) return
    if (chips.some(c => c.name.toLowerCase() === name.toLowerCase())) {
      setNewName('')
      return
    }
    onChange([...chips, { name, level: 'adjacent' }])
    setNewName('')
  }

  function removeChip(i: number) {
    onChange(chips.filter((_, j) => j !== i))
  }

  function updateLevel(i: number, level: SkillLevel) {
    onChange(chips.map((c, j) => j === i ? { ...c, level } : c))
  }

  return (
    <div className="skill-editor">
      <label className="form-label">{label}</label>
      <div className="skill-chips">
        {chips.length === 0 && (
          <span className="skill-chips-empty">None — add below or rebuild from materials.</span>
        )}
        {chips.map((c, i) => (
          <span key={i} className={`skill-chip skill-chip--${c.level}`}>
            <span className="skill-chip-name">{c.name}</span>
            <select
              className="skill-chip-select"
              value={c.level}
              onChange={e => updateLevel(i, e.target.value as SkillLevel)}
            >
              {(Object.keys(LEVEL_LABELS) as SkillLevel[]).map(l => (
                <option key={l} value={l}>{LEVEL_LABELS[l]}</option>
              ))}
            </select>
            <button
              type="button"
              className="skill-chip-remove"
              onClick={() => removeChip(i)}
              title="Remove"
            >×</button>
          </span>
        ))}
      </div>
      <div className="skill-add-row">
        <input
          className="form-input skill-add-input"
          placeholder={placeholder}
          value={newName}
          onChange={e => setNewName(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); addChip() } }}
        />
        <button type="button" className="btn btn--sm" onClick={addChip}>Add</button>
      </div>
    </div>
  )
}

// ── Synthesis banner ──────────────────────────────────────────────────────────

function SynthesisBanner({
  synthesis,
  synthesizing,
  onRebuild,
}: {
  synthesis:   SynthesisResult | null
  synthesizing: boolean
  onRebuild:   () => void
}) {
  return (
    <div className="synthesis-banner">
      <div className="synthesis-banner-text">
        {synthesis ? (
          <>
            <span className="synthesis-status">
              {synthesis.skills_found} terms proposed from:{' '}
              {synthesis.sources_used.length > 0
                ? synthesis.sources_used.join(', ')
                : 'no materials found'}
            </span>
          </>
        ) : (
          <span className="synthesis-status synthesis-status--empty">
            Skills &amp; domains auto-populated from your ingested materials.
            {' '}Rebuild any time after adding evidence.
          </span>
        )}
      </div>
      <button
        type="button"
        className="btn btn--sm btn--ghost"
        disabled={synthesizing}
        onClick={onRebuild}
      >
        {synthesizing ? 'Rebuilding…' : synthesis ? 'Rebuild from my materials' : 'Build from my materials'}
      </button>
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export function ProfileSetup() {
  const [personal,  setPersonal]  = useState<PersonalFormState>(EMPTY_PERSONAL)
  const [skills,    setSkills]    = useState<SkillFormState>(EMPTY_SKILLS)
  const [original,  setOriginal]  = useState<CandidateProfile | null>(null)
  const [status,    setStatus]    = useState<IngestStatus | null>(null)

  const [saving,       setSaving]       = useState(false)
  const [saveMsg,      setSaveMsg]      = useState<string | null>(null)
  const [saveErr,      setSaveErr]      = useState<string | null>(null)
  const [completeness, setCompleteness] = useState<number | null>(null)

  const [synthesis,    setSynthesis]    = useState<SynthesisResult | null>(null)
  const [synthesizing, setSynthesizing] = useState(false)
  const [synthErr,     setSynthErr]     = useState<string | null>(null)
  // Prevent double auto-synthesis on mount
  const didAutoSynth = useRef(false)

  const [resumeText,      setResumeText]      = useState('')
  const [clText,          setClText]          = useState('')
  const [ingestingResume, setIngestingResume] = useState(false)
  const [ingestingCl,     setIngestingCl]     = useState(false)
  const [resumeMsg,       setResumeMsg]       = useState<string | null>(null)
  const [clMsg,           setClMsg]           = useState<string | null>(null)
  const [resumeErr,       setResumeErr]       = useState<string | null>(null)
  const [clErr,           setClErr]           = useState<string | null>(null)

  // ── Load profile + ingest status on mount ─────────────────────────────────
  useEffect(() => {
    Promise.all([api.getProfile(), api.getIngestStatus()]).then(([p, s]) => {
      setOriginal(p)
      const { personal: per, skills: sk } = profileToForm(p)
      setPersonal(per)
      setCompleteness(p._completeness ?? null)
      setStatus(s)

      // If the profile has no skills yet, auto-synthesize to pre-populate
      if (!hasAnySkills(sk) && !didAutoSynth.current) {
        didAutoSynth.current = true
        runSynthesis(sk)
      } else {
        setSkills(sk)
      }
    }).catch(() => {})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function runSynthesis(currentSkills?: SkillFormState) {
    setSynthesizing(true)
    setSynthErr(null)
    try {
      const result = await api.synthesizeProfile()
      setSynthesis(result)
      if (result.skills_found > 0) {
        setSkills(synthesisToSkills(result))
      } else if (currentSkills) {
        setSkills(currentSkills)
      }
    } catch (e: unknown) {
      setSynthErr(e instanceof Error ? e.message : String(e))
      if (currentSkills) setSkills(currentSkills)
    } finally {
      setSynthesizing(false)
    }
  }

  // ── Personal field helpers ─────────────────────────────────────────────────
  function setP(key: keyof PersonalFormState) {
    return (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
      setPersonal(f => ({
        ...f,
        [key]: (e.target as HTMLInputElement).type === 'checkbox'
          ? (e.target as HTMLInputElement).checked
          : e.target.value,
      }))
    }
  }

  // ── Skill chip helpers ─────────────────────────────────────────────────────
  function setSection(key: keyof SkillFormState) {
    return (chips: SkillChip[]) => setSkills(s => ({ ...s, [key]: chips }))
  }

  // ── Save ───────────────────────────────────────────────────────────────────
  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setSaveErr(null)
    setSaveMsg(null)
    try {
      const profile = buildProfile(personal, skills, original)
      const res     = await api.saveProfile(profile)
      setCompleteness(res.completeness)
      setSaveMsg(`Saved. Profile completeness: ${Math.round(res.completeness * 100)}%`)
    } catch (e: unknown) {
      setSaveErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  // ── Ingest handlers ────────────────────────────────────────────────────────
  async function handleIngestResume() {
    if (!resumeText.trim()) return
    setIngestingResume(true)
    setResumeErr(null)
    setResumeMsg(null)
    try {
      const res = await api.ingestResume(resumeText.trim())
      setResumeMsg(`Ingested ${res.bullet_count} bullets from resume (id ${res.resume_id}).`)
      setResumeText('')
      const s = await api.getIngestStatus()
      setStatus(s)
    } catch (e: unknown) {
      setResumeErr(e instanceof Error ? e.message : String(e))
    } finally {
      setIngestingResume(false)
    }
  }

  async function handleIngestCl() {
    if (!clText.trim()) return
    setIngestingCl(true)
    setClErr(null)
    setClMsg(null)
    try {
      const res = await api.ingestCoverLetter(clText.trim())
      setClMsg(`Ingested ${res.fragment_count} fragments from cover letter (id ${res.cl_id}).`)
      setClText('')
      const s = await api.getIngestStatus()
      setStatus(s)
    } catch (e: unknown) {
      setClErr(e instanceof Error ? e.message : String(e))
    } finally {
      setIngestingCl(false)
    }
  }

  const pct = completeness != null ? Math.round(completeness * 100) : null

  return (
    <div className="intake-page">
      <div className="intake-page-header">
        <h2 className="intake-title">Profile setup</h2>
        <p className="intake-subtitle">
          Fill in personal details and job targets, then review the skills &amp; domains
          proposed from your ingested materials.
          {pct != null && (
            <span
              className="completeness-pill"
              data-level={pct >= 70 ? 'good' : pct >= 40 ? 'ok' : 'low'}
            >
              {pct}% complete
            </span>
          )}
        </p>
      </div>

      <form className="intake-form" onSubmit={handleSave}>

        {/* ── Personal ─────────────────────────────────────────────────────── */}
        <div className="form-section">
          <div className="form-section-label">Personal</div>
          <div className="form-grid-2">
            <div>
              <label className="form-label" htmlFor="ps-name">Full name</label>
              <input id="ps-name" className="form-input" placeholder="Jane Smith"
                value={personal.name} onChange={setP('name')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-location">Location</label>
              <input id="ps-location" className="form-input" placeholder="New York, NY"
                value={personal.location} onChange={setP('location')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-linkedin">LinkedIn URL</label>
              <input id="ps-linkedin" className="form-input" placeholder="https://linkedin.com/in/…"
                value={personal.linkedin} onChange={setP('linkedin')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-github">GitHub URL</label>
              <input id="ps-github" className="form-input" placeholder="https://github.com/…"
                value={personal.github} onChange={setP('github')} />
            </div>
          </div>
        </div>

        {/* ── Job targets ──────────────────────────────────────────────────── */}
        <div className="form-section">
          <div className="form-section-label">Job targets</div>
          <div className="form-grid-2">
            <div className="form-grid-2-span">
              <label className="form-label" htmlFor="ps-titles">Target job titles (one per line)</label>
              <textarea id="ps-titles" className="form-textarea" rows={3}
                placeholder="Senior Software Engineer&#10;Staff Engineer&#10;Principal Engineer"
                value={personal.titles} onChange={setP('titles')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-seniority">Seniority</label>
              <select id="ps-seniority" className="form-select"
                value={personal.seniority} onChange={setP('seniority')}>
                {SENIORITY_OPTIONS.map(o => <option key={o} value={o}>{o || '(not set)'}</option>)}
              </select>
            </div>
            <div>
              <label className="form-label" htmlFor="ps-remote">Desired remote policy</label>
              <select id="ps-remote" className="form-select"
                value={personal.remote} onChange={setP('remote')}>
                {REMOTE_OPTIONS.map(o => <option key={o} value={o}>{o || '(not set)'}</option>)}
              </select>
            </div>
            <div>
              <label className="form-label" htmlFor="ps-auth">Work authorization</label>
              <select id="ps-auth" className="form-select"
                value={personal.authorization} onChange={setP('authorization')}>
                {AUTH_OPTIONS.map(o => <option key={o} value={o}>{o || '(not set)'}</option>)}
              </select>
            </div>
            <div className="form-checkbox-row">
              <input id="ps-relocate" type="checkbox" checked={personal.relocate}
                onChange={setP('relocate')} />
              <label htmlFor="ps-relocate" className="form-label form-label--inline">
                Willing to relocate
              </label>
            </div>
          </div>
        </div>

        {/* ── Skills & Domains ─────────────────────────────────────────────── */}
        <div className="form-section">
          <div className="form-section-label">
            Skills &amp; domains
            <span className="form-section-hint">
              Proposed from your materials — adjust levels, add, or remove.
            </span>
          </div>

          <SynthesisBanner
            synthesis={synthesis}
            synthesizing={synthesizing}
            onRebuild={() => runSynthesis()}
          />

          {synthErr && <div className="form-error" style={{ marginBottom: '0.75rem' }}>{synthErr}</div>}

          <div className="form-grid-2">
            {SKILL_SECTIONS.map(({ key, label, placeholder }) => (
              <SkillEditor
                key={key}
                label={label}
                placeholder={placeholder}
                chips={skills[key]}
                onChange={setSection(key)}
              />
            ))}
          </div>

          {/* Domains spans full width */}
          <div style={{ marginTop: '0.75rem' }}>
            <SkillEditor
              label="Domains"
              placeholder="FinTech"
              chips={skills.domains}
              onChange={setSection('domains')}
            />
          </div>
        </div>

        {saveErr && <div className="form-error">{saveErr}</div>}
        {saveMsg && <div className="form-success">{saveMsg}</div>}

        <div className="form-actions">
          <button className="btn btn--primary btn--lg" type="submit" disabled={saving}>
            {saving ? 'Saving…' : 'Save profile'}
          </button>
        </div>

      </form>

      {/* ── Base resume ───────────────────────────────────────────────────── */}
      <div className="intake-form intake-section-divider">
        <div className="form-section">
          <div className="form-section-label">
            Base resume
            {status && (
              <span className={`ingest-status-pill ${status.has_resume ? 'ingest-status-pill--ok' : 'ingest-status-pill--missing'}`}>
                {status.has_resume
                  ? `${status.resume_bullets} bullets ingested`
                  : 'Not ingested yet'}
              </span>
            )}
          </div>
          <textarea
            className="form-textarea form-textarea--tall"
            rows={12}
            placeholder="Paste your full resume text here. Include all sections — experience, projects, skills…"
            value={resumeText}
            onChange={e => setResumeText(e.target.value)}
          />
          <span className="form-hint">
            Paste plain text. After ingesting, use "Rebuild from my materials" above to refresh skills.
          </span>
        </div>
        {resumeErr && <div className="form-error">{resumeErr}</div>}
        {resumeMsg && <div className="form-success">{resumeMsg}</div>}
        <div className="form-actions">
          <button
            className="btn btn--primary btn--lg"
            type="button"
            disabled={ingestingResume || !resumeText.trim()}
            onClick={handleIngestResume}
          >
            {ingestingResume ? 'Ingesting…' : 'Ingest resume'}
          </button>
        </div>
      </div>

      {/* ── Base cover letter ─────────────────────────────────────────────── */}
      <div className="intake-form intake-section-divider">
        <div className="form-section">
          <div className="form-section-label">
            Base cover letter
            {status && (
              <span className={`ingest-status-pill ${status.has_cover_letter ? 'ingest-status-pill--ok' : 'ingest-status-pill--missing'}`}>
                {status.has_cover_letter
                  ? `${status.cl_fragments} fragments ingested`
                  : 'Not ingested yet'}
              </span>
            )}
          </div>
          <textarea
            className="form-textarea form-textarea--tall"
            rows={12}
            placeholder="Paste your base cover letter text here. Include your opening, proof points, and closing…"
            value={clText}
            onChange={e => setClText(e.target.value)}
          />
          <span className="form-hint">
            Paste plain text. Proof-point fragments are selected and reordered per job.
          </span>
        </div>
        {clErr && <div className="form-error">{clErr}</div>}
        {clMsg && <div className="form-success">{clMsg}</div>}
        <div className="form-actions">
          <button
            className="btn btn--primary btn--lg"
            type="button"
            disabled={ingestingCl || !clText.trim()}
            onClick={handleIngestCl}
          >
            {ingestingCl ? 'Ingesting…' : 'Ingest cover letter'}
          </button>
        </div>
      </div>

      <CandidateAssessmentSection />
    </div>
  )
}
