import { useState } from 'react'
import type { CreateJobResult, CreatePackageResult } from '../lib/api'
import { api } from '../lib/api'

interface Props {
  onCreated: (result: CreateJobResult) => void
}

const REMOTE_OPTIONS  = ['', 'remote', 'hybrid', 'onsite']
const PLATFORM_OPTIONS = ['', 'LinkedIn', 'Indeed', 'Company site', 'Glassdoor', 'Wellfound', 'Other']

const STEP_LABELS: Record<string, string> = {
  extract:      'Requirements extracted',
  assess:       'Fit assessed',
  resume:       'Resume generated',
  cover_letter: 'Cover letter generated',
  project:      'Project recommendations',
}

const MISSING_LABELS: Record<string, string> = {
  profile:           'candidate profile',
  base_resume:       'ingested resume',
  base_cover_letter: 'ingested cover letter',
  projects:          'projects file',
}

// ── Step summary shown after package is created ──────────────────────────────

function PackageSteps({ result, onView }: { result: CreatePackageResult; onView: () => void }) {
  const totalOk = Object.values(result.steps).filter(Boolean).length

  return (
    <div className="intake-page">
      <div className="intake-page-header">
        <h2 className="intake-title">Package ready</h2>
        <p className="intake-subtitle">
          {totalOk} of {Object.keys(result.steps).length} steps completed.
          {result.verdict && <> Verdict: <strong>{result.verdict}</strong>.</>}
        </p>
      </div>

      <div className="pkg-steps">
        {Object.entries(STEP_LABELS).map(([key, label]) => {
          const ok  = result.steps[key]
          const err = result.errors[key]
          const icon = ok ? '✓' : (err ? '✗' : '—')
          const cls  = ok ? 'pkg-step--ok' : (err ? 'pkg-step--err' : 'pkg-step--skip')
          return (
            <div key={key} className={`pkg-step ${cls}`}>
              <span className="pkg-step-icon">{icon}</span>
              <span className="pkg-step-label">{label}</span>
              {err && <span className="pkg-step-msg">{err}</span>}
            </div>
          )
        })}
      </div>

      {result.missing.length > 0 && (
        <p className="pkg-missing">
          Missing:{' '}
          {result.missing.map(k => MISSING_LABELS[k] ?? k).join(', ')}.
          {' '}Set up your profile and ingest base documents to enable all steps.
        </p>
      )}

      <div className="form-actions" style={{ marginTop: '1.5rem' }}>
        <button className="btn btn--primary btn--lg" onClick={onView}>
          View Package →
        </button>
      </div>
    </div>
  )
}

// ── Main form ─────────────────────────────────────────────────────────────────

export function NewJobForm({ onCreated }: Props) {
  const [rawText,      setRawText]      = useState('')
  const [company,      setCompany]      = useState('')
  const [title,        setTitle]        = useState('')
  const [location,     setLocation]     = useState('')
  const [sourceUrl,    setSourceUrl]    = useState('')
  const [remotePolicy, setRemotePolicy] = useState('')
  const [platform,     setPlatform]     = useState('')
  const [showDetails,  setShowDetails]  = useState(false)

  const [submitting,    setSubmitting]    = useState(false)
  const [packageResult, setPackageResult] = useState<CreatePackageResult | null>(null)
  const [error,         setError]         = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!rawText.trim()) { setError('Job description text is required.'); return }

    setSubmitting(true)
    setError(null)
    setPackageResult(null)
    try {
      const result = await api.createJobPackage({
        raw_text:      rawText.trim(),
        company:       company.trim()   || undefined,
        title:         title.trim()     || undefined,
        location:      location.trim()  || undefined,
        source_url:    sourceUrl.trim() || undefined,
        remote_policy: remotePolicy     || undefined,
        platform:      platform         || undefined,
      })
      setPackageResult(result)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }

  function handleViewPackage() {
    if (!packageResult) return
    onCreated({
      ok:        packageResult.ok,
      job_id:    packageResult.job_id,
      extracted: packageResult.steps['extract'] ?? false,
      assessed:  packageResult.steps['assess']  ?? false,
      verdict:   packageResult.verdict,
    })
  }

  if (packageResult) {
    return <PackageSteps result={packageResult} onView={handleViewPackage} />
  }

  return (
    <div className="intake-page">
      <div className="intake-page-header">
        <h2 className="intake-title">New job</h2>
        <p className="intake-subtitle">
          Paste the job description — extraction, fit assessment, resume, cover letter,
          and project recommendations run automatically.
        </p>
      </div>

      <form className="intake-form" onSubmit={handleSubmit}>

        <div className="form-section">
          <label className="form-label form-label--required" htmlFor="nj-rawtext">
            Job description
          </label>
          <textarea
            id="nj-rawtext"
            className="form-textarea form-textarea--tall"
            rows={16}
            placeholder="Paste the full job posting text here…"
            value={rawText}
            onChange={e => setRawText(e.target.value)}
            required
          />
          <span className="form-hint">
            Include the full text — requirements, responsibilities, about the company.
          </span>
        </div>

        <div className="form-section">
          <button
            type="button"
            className="details-toggle"
            onClick={() => setShowDetails(v => !v)}
          >
            {showDetails ? '▴ Hide details' : '▾ Add details (optional)'}
          </button>

          {showDetails && (
            <div className="form-grid-2" style={{ marginTop: '0.75rem' }}>
              <div>
                <label className="form-label" htmlFor="nj-company">Company</label>
                <input id="nj-company" className="form-input" placeholder="Acme Corp"
                  value={company} onChange={e => setCompany(e.target.value)} />
              </div>
              <div>
                <label className="form-label" htmlFor="nj-title">Job title</label>
                <input id="nj-title" className="form-input" placeholder="Senior Python Engineer"
                  value={title} onChange={e => setTitle(e.target.value)} />
              </div>
              <div>
                <label className="form-label" htmlFor="nj-location">Location</label>
                <input id="nj-location" className="form-input" placeholder="New York, NY"
                  value={location} onChange={e => setLocation(e.target.value)} />
              </div>
              <div>
                <label className="form-label" htmlFor="nj-remote">Remote policy</label>
                <select id="nj-remote" className="form-select"
                  value={remotePolicy} onChange={e => setRemotePolicy(e.target.value)}>
                  {REMOTE_OPTIONS.map(o => (
                    <option key={o} value={o}>{o || '(detect from text)'}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="form-label" htmlFor="nj-platform">Platform</label>
                <select id="nj-platform" className="form-select"
                  value={platform} onChange={e => setPlatform(e.target.value)}>
                  {PLATFORM_OPTIONS.map(o => (
                    <option key={o} value={o}>{o || '(not specified)'}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="form-label" htmlFor="nj-url">Source URL</label>
                <input id="nj-url" className="form-input" type="url"
                  placeholder="https://linkedin.com/jobs/…"
                  value={sourceUrl} onChange={e => setSourceUrl(e.target.value)} />
              </div>
            </div>
          )}
        </div>

        {error && <div className="form-error">{error}</div>}

        <div className="form-actions">
          <button
            className="btn btn--primary btn--lg"
            type="submit"
            disabled={submitting || !rawText.trim()}
          >
            {submitting ? 'Creating package…' : 'Create Package'}
          </button>
          {submitting && (
            <span className="form-hint">
              Running extraction, fit assessment, and generating documents…
            </span>
          )}
        </div>

      </form>
    </div>
  )
}
