import { useEffect, useState } from 'react'
import type { CandidateProfile, IngestStatus } from '../lib/api'
import { api } from '../lib/api'
import { CandidateAssessmentSection } from './CandidateAssessment'

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

function stripTodo(s: string): string {
  return s.startsWith('TODO:') ? '' : s
}

function skillsToText(arr: { name: string; evidence: string }[]): string {
  return arr.map(s => `${s.name} ${s.evidence}`).join('\n')
}

function textToSkills(text: string): { name: string; years: number; evidence: string }[] {
  return text
    .split('\n')
    .map(l => l.trim())
    .filter(Boolean)
    .map(l => {
      const parts = l.split(/\s+/)
      const evidence = ['direct', 'adjacent', 'familiar'].includes(parts[parts.length - 1])
        ? parts.pop()!
        : 'familiar'
      return { name: parts.join(' '), years: 0, evidence }
    })
}

function domainsToText(arr: { name: string; evidence: string }[]): string {
  return arr.map(d => `${d.name} ${d.evidence}`).join('\n')
}

function textToDomains(text: string): { name: string; evidence: string }[] {
  return text
    .split('\n')
    .map(l => l.trim())
    .filter(Boolean)
    .map(l => {
      const parts = l.split(/\s+/)
      const evidence = ['direct', 'adjacent', 'familiar'].includes(parts[parts.length - 1])
        ? parts.pop()!
        : 'adjacent'
      return { name: parts.join(' '), evidence }
    })
}

interface FormState {
  name: string
  location: string
  linkedin: string
  github: string
  titles: string
  seniority: string
  remote: string
  authorization: string
  relocate: boolean
  languages: string
  frameworks: string
  databases: string
  cloud: string
  tools: string
  practices: string
  domains: string
}

function profileToForm(p: CandidateProfile): FormState {
  const jt = p.job_targets
  return {
    name:          stripTodo(p.personal.name),
    location:      stripTodo(p.personal.location),
    linkedin:      stripTodo(p.personal.linkedin),
    github:        stripTodo(p.personal.github),
    titles:        (jt.titles ?? []).join('\n'),
    seniority:     jt.seniority_self_assessed ?? '',
    remote:        jt.desired_remote_policy ?? '',
    authorization: jt.work_authorization ?? '',
    relocate:      jt.willing_to_relocate ?? false,
    languages:     skillsToText(p.skills.languages ?? []),
    frameworks:    skillsToText(p.skills.frameworks ?? []),
    databases:     skillsToText(p.skills.databases ?? []),
    cloud:         skillsToText(p.skills.cloud ?? []),
    tools:         skillsToText(p.skills.tools ?? []),
    practices:     skillsToText(p.skills.practices ?? []),
    domains:       domainsToText(p.domains ?? []),
  }
}

function formToProfile(form: FormState, original: CandidateProfile | null): CandidateProfile {
  const base: CandidateProfile = original ?? {
    version: '1.0',
    personal: { name: '', location: '', linkedin: '', github: '' },
    job_targets: {
      titles: [],
      seniority_self_assessed: '',
      desired_remote_policy: '',
      willing_to_relocate: false,
      work_authorization: '',
    },
    skills: { languages: [], frameworks: [], databases: [], cloud: [], tools: [], practices: [] },
    domains: [],
  }

  return {
    ...base,
    personal: {
      name:     form.name     || 'TODO: your name',
      location: form.location || 'TODO: your location',
      linkedin: form.linkedin || 'TODO: your LinkedIn',
      github:   form.github   || 'TODO: your GitHub',
    },
    job_targets: {
      titles:                  form.titles.split('\n').map(t => t.trim()).filter(Boolean),
      seniority_self_assessed: form.seniority,
      desired_remote_policy:   form.remote,
      willing_to_relocate:     form.relocate,
      work_authorization:      form.authorization,
    },
    skills: {
      languages:  textToSkills(form.languages),
      frameworks: textToSkills(form.frameworks),
      databases:  textToSkills(form.databases),
      cloud:      textToSkills(form.cloud),
      tools:      textToSkills(form.tools),
      practices:  textToSkills(form.practices),
    },
    domains: textToDomains(form.domains),
  }
}

const EMPTY_FORM: FormState = {
  name: '', location: '', linkedin: '', github: '',
  titles: '', seniority: '', remote: '', authorization: '', relocate: false,
  languages: '', frameworks: '', databases: '', cloud: '', tools: '', practices: '',
  domains: '',
}

export function ProfileSetup() {
  const [form,   setForm]   = useState<FormState>(EMPTY_FORM)
  const [original, setOriginal] = useState<CandidateProfile | null>(null)
  const [status, setStatus] = useState<IngestStatus | null>(null)

  const [saving,       setSaving]       = useState(false)
  const [saveMsg,      setSaveMsg]      = useState<string | null>(null)
  const [saveErr,      setSaveErr]      = useState<string | null>(null)
  const [completeness, setCompleteness] = useState<number | null>(null)

  const [resumeText,     setResumeText]     = useState('')
  const [clText,         setClText]         = useState('')
  const [ingestingResume, setIngestingResume] = useState(false)
  const [ingestingCl,     setIngestingCl]     = useState(false)
  const [resumeMsg,      setResumeMsg]      = useState<string | null>(null)
  const [clMsg,          setClMsg]          = useState<string | null>(null)
  const [resumeErr,      setResumeErr]      = useState<string | null>(null)
  const [clErr,          setClErr]          = useState<string | null>(null)

  useEffect(() => {
    Promise.all([api.getProfile(), api.getIngestStatus()]).then(([p, s]) => {
      setOriginal(p)
      setForm(profileToForm(p))
      setCompleteness(p._completeness ?? null)
      setStatus(s)
    }).catch(() => {})
  }, [])

  function set(key: keyof FormState) {
    return (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
      setForm(f => ({ ...f, [key]: (e.target as HTMLInputElement).type === 'checkbox'
        ? (e.target as HTMLInputElement).checked
        : e.target.value }))
    }
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setSaveErr(null)
    setSaveMsg(null)
    try {
      const profile = formToProfile(form, original)
      const res = await api.saveProfile(profile)
      setCompleteness(res.completeness)
      setSaveMsg(`Saved. Profile completeness: ${Math.round(res.completeness * 100)}%`)
    } catch (e: unknown) {
      setSaveErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

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
          Fill in your profile, then paste your base resume and cover letter. These
          are used to score fit and generate tailored documents for every job.
          {pct != null && (
            <span className="completeness-pill" data-level={pct >= 70 ? 'good' : pct >= 40 ? 'ok' : 'low'}>
              {pct}% complete
            </span>
          )}
        </p>
      </div>

      {/* ── Profile form ──────────────────────────────────────────────────── */}
      <form className="intake-form" onSubmit={handleSave}>

        <div className="form-section">
          <div className="form-section-label">Personal</div>
          <div className="form-grid-2">
            <div>
              <label className="form-label" htmlFor="ps-name">Full name</label>
              <input id="ps-name" className="form-input" placeholder="Jane Smith"
                value={form.name} onChange={set('name')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-location">Location</label>
              <input id="ps-location" className="form-input" placeholder="New York, NY"
                value={form.location} onChange={set('location')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-linkedin">LinkedIn URL</label>
              <input id="ps-linkedin" className="form-input" placeholder="https://linkedin.com/in/…"
                value={form.linkedin} onChange={set('linkedin')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-github">GitHub URL</label>
              <input id="ps-github" className="form-input" placeholder="https://github.com/…"
                value={form.github} onChange={set('github')} />
            </div>
          </div>
        </div>

        <div className="form-section">
          <div className="form-section-label">Job targets</div>
          <div className="form-grid-2">
            <div className="form-grid-2-span">
              <label className="form-label" htmlFor="ps-titles">Target job titles (one per line)</label>
              <textarea id="ps-titles" className="form-textarea" rows={3}
                placeholder="Senior Software Engineer&#10;Staff Engineer&#10;Principal Engineer"
                value={form.titles} onChange={set('titles')} />
            </div>
            <div>
              <label className="form-label" htmlFor="ps-seniority">Seniority</label>
              <select id="ps-seniority" className="form-select" value={form.seniority} onChange={set('seniority')}>
                {SENIORITY_OPTIONS.map(o => <option key={o} value={o}>{o || '(not set)'}</option>)}
              </select>
            </div>
            <div>
              <label className="form-label" htmlFor="ps-remote">Desired remote policy</label>
              <select id="ps-remote" className="form-select" value={form.remote} onChange={set('remote')}>
                {REMOTE_OPTIONS.map(o => <option key={o} value={o}>{o || '(not set)'}</option>)}
              </select>
            </div>
            <div>
              <label className="form-label" htmlFor="ps-auth">Work authorization</label>
              <select id="ps-auth" className="form-select" value={form.authorization} onChange={set('authorization')}>
                {AUTH_OPTIONS.map(o => <option key={o} value={o}>{o || '(not set)'}</option>)}
              </select>
            </div>
            <div className="form-checkbox-row">
              <input id="ps-relocate" type="checkbox" checked={form.relocate}
                onChange={set('relocate')} />
              <label htmlFor="ps-relocate" className="form-label form-label--inline">
                Willing to relocate
              </label>
            </div>
          </div>
        </div>

        <div className="form-section">
          <div className="form-section-label">
            Skills
            <span className="form-section-hint">One skill per line: <code>SkillName direct|adjacent|familiar</code></span>
          </div>
          <div className="form-grid-2">
            {([
              ['ps-langs',     'languages',  'Languages',  'Python direct\nGo adjacent'],
              ['ps-fw',        'frameworks', 'Frameworks', 'FastAPI direct\nDjango adjacent'],
              ['ps-db',        'databases',  'Databases',  'PostgreSQL direct\nRedis adjacent'],
              ['ps-cloud',     'cloud',      'Cloud',      'AWS direct\nGCP adjacent'],
              ['ps-tools',     'tools',      'Tools',      'Docker direct\nKubernetes adjacent'],
              ['ps-practices', 'practices',  'Practices',  'CI/CD direct\nAgile familiar'],
            ] as const).map(([id, key, label, ph]) => (
              <div key={id}>
                <label className="form-label" htmlFor={id}>{label}</label>
                <textarea id={id} className="form-textarea" rows={4}
                  placeholder={ph}
                  value={form[key]} onChange={set(key)} />
              </div>
            ))}
          </div>
        </div>

        <div className="form-section">
          <div className="form-section-label">
            Domains
            <span className="form-section-hint">One domain per line: <code>Domain direct|adjacent|familiar</code></span>
          </div>
          <textarea className="form-textarea" rows={3}
            placeholder="FinTech direct&#10;Healthcare adjacent&#10;E-commerce familiar"
            value={form.domains} onChange={set('domains')} />
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
            Paste plain text. Bullets are extracted and ranked against each job description.
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
