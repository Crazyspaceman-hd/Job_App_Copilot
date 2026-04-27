import { useState } from 'react'
import type { CreateJobResult } from '../lib/api'
import { api } from '../lib/api'

interface Props {
  onCreated: (result: CreateJobResult) => void
}

const REMOTE_OPTIONS = ['', 'remote', 'hybrid', 'onsite']
const PLATFORM_OPTIONS = ['', 'LinkedIn', 'Indeed', 'Company site', 'Glassdoor', 'Wellfound', 'Other']

export function NewJobForm({ onCreated }: Props) {
  const [rawText,      setRawText]      = useState('')
  const [company,      setCompany]      = useState('')
  const [title,        setTitle]        = useState('')
  const [location,     setLocation]     = useState('')
  const [sourceUrl,    setSourceUrl]    = useState('')
  const [remotePolicy, setRemotePolicy] = useState('')
  const [platform,     setPlatform]     = useState('')

  const [submitting, setSubmitting] = useState(false)
  const [error,      setError]      = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!rawText.trim()) { setError('Job description text is required.'); return }

    setSubmitting(true)
    setError(null)
    try {
      const result = await api.createJob({
        raw_text:      rawText.trim(),
        company:       company.trim()      || undefined,
        title:         title.trim()        || undefined,
        location:      location.trim()     || undefined,
        source_url:    sourceUrl.trim()    || undefined,
        remote_policy: remotePolicy        || undefined,
        platform:      platform            || undefined,
      })
      onCreated(result)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <div className="intake-page">
      <div className="intake-page-header">
        <h2 className="intake-title">Add new job</h2>
        <p className="intake-subtitle">
          Paste the job description and fill in what you know. Requirements are
          extracted and fit is scored automatically.
        </p>
      </div>

      <form className="intake-form" onSubmit={handleSubmit}>

        {/* ── Job description — most important, first ─────────────────── */}
        <div className="form-section">
          <label className="form-label form-label--required" htmlFor="nj-rawtext">
            Job description text
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
            Include the full text — requirements, responsibilities, about the company. The more text, the better the extraction.
          </span>
        </div>

        {/* ── Metadata row ────────────────────────────────────────────── */}
        <div className="form-section">
          <div className="form-section-label">Optional metadata</div>
          <div className="form-grid-2">
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
              <input id="nj-location" className="form-input" placeholder="New York, NY / Remote"
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
              <label className="form-label" htmlFor="nj-platform">Platform / source</label>
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
        </div>

        {error && <div className="form-error">{error}</div>}

        <div className="form-actions">
          <button className="btn btn--primary btn--lg" type="submit" disabled={submitting || !rawText.trim()}>
            {submitting ? 'Saving & analysing…' : 'Add job'}
          </button>
          {submitting && (
            <span className="form-hint">
              Running extraction and fit assessment…
            </span>
          )}
        </div>

      </form>
    </div>
  )
}
