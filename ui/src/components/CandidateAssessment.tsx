import { useCallback, useEffect, useState } from 'react'
import type { CandidateAssessment, CandidateAssessmentPayload } from '../lib/api'
import {
  ASSESSMENT_ALLOWED_USE_LABELS,
  ASSESSMENT_CONFIDENCE_LABELS,
  ASSESSMENT_KIND_LABELS,
  ASSESSMENT_SOURCE_LABELS,
  api,
} from '../lib/api'

// ── helpers ───────────────────────────────────────────────────────────────────

const EMPTY_FORM: CandidateAssessmentPayload = {
  source_type:          'manual',
  source_label:         '',
  assessment_kind:      'working_assessment',
  raw_text:             '',
  strengths:            [],
  growth_areas:         [],
  demonstrated_skills:  [],
  demonstrated_domains: [],
  work_style:           '',
  role_fit:             '',
  confidence:           '',
  allowed_uses:         [],
}

function tagsToText(tags: string[]): string { return tags.join(', ') }
function textToTags(text: string): string[] {
  return text.split(',').map(t => t.trim()).filter(Boolean)
}

// ── AssessmentForm ─────────────────────────────────────────────────────────────

interface FormProps {
  initial?: CandidateAssessment | null
  onSave: (payload: CandidateAssessmentPayload) => Promise<void>
  onCancel: () => void
}

function AssessmentForm({ initial, onSave, onCancel }: FormProps) {
  const [form, setForm] = useState<CandidateAssessmentPayload>(() =>
    initial
      ? {
          source_type:          initial.source_type,
          source_label:         initial.source_label ?? '',
          assessment_kind:      initial.assessment_kind,
          raw_text:             initial.raw_text,
          strengths:            initial.strengths,
          growth_areas:         initial.growth_areas,
          demonstrated_skills:  initial.demonstrated_skills,
          demonstrated_domains: initial.demonstrated_domains,
          work_style:           initial.work_style ?? '',
          role_fit:             initial.role_fit ?? '',
          confidence:           initial.confidence ?? '',
          allowed_uses:         initial.allowed_uses,
        }
      : EMPTY_FORM
  )
  const [saving, setSaving] = useState(false)
  const [err, setErr]       = useState<string | null>(null)

  function set(key: keyof CandidateAssessmentPayload, value: unknown) {
    setForm(f => ({ ...f, [key]: value }))
  }

  function toggleUse(use: string) {
    set(
      'allowed_uses',
      form.allowed_uses?.includes(use)
        ? form.allowed_uses.filter(u => u !== use)
        : [...(form.allowed_uses ?? []), use]
    )
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setErr(null)
    try {
      await onSave(form)
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <form className="ca-form" onSubmit={handleSubmit}>
      <div className="ca-form-grid">
        <label className="form-label">
          Source
          <select
            className="form-select"
            value={form.source_type}
            onChange={e => set('source_type', e.target.value)}
          >
            {Object.entries(ASSESSMENT_SOURCE_LABELS).map(([v, l]) => (
              <option key={v} value={v}>{l}</option>
            ))}
          </select>
        </label>

        <label className="form-label">
          Source label (optional)
          <input
            className="form-input"
            type="text"
            placeholder="e.g. Claude session 2026-04-24"
            value={form.source_label ?? ''}
            onChange={e => set('source_label', e.target.value)}
          />
        </label>

        <label className="form-label">
          Assessment kind
          <select
            className="form-select"
            value={form.assessment_kind}
            onChange={e => set('assessment_kind', e.target.value)}
          >
            {Object.entries(ASSESSMENT_KIND_LABELS).map(([v, l]) => (
              <option key={v} value={v}>{l}</option>
            ))}
          </select>
        </label>

        <label className="form-label">
          Confidence
          <select
            className="form-select"
            value={form.confidence ?? ''}
            onChange={e => set('confidence', e.target.value || null)}
          >
            <option value="">— none —</option>
            {Object.entries(ASSESSMENT_CONFIDENCE_LABELS).map(([v, l]) => (
              <option key={v} value={v}>{l}</option>
            ))}
          </select>
        </label>
      </div>

      <label className="form-label">
        Raw text / full assessment
        <textarea
          className="form-textarea ca-textarea--tall"
          value={form.raw_text}
          onChange={e => set('raw_text', e.target.value)}
          placeholder="Paste the full assessment text here…"
          rows={6}
        />
      </label>

      <div className="ca-form-grid">
        <label className="form-label">
          Strengths (comma-separated)
          <input
            className="form-input"
            type="text"
            value={tagsToText(form.strengths ?? [])}
            onChange={e => set('strengths', textToTags(e.target.value))}
            placeholder="e.g. systems thinking, shipping, mentoring"
          />
        </label>

        <label className="form-label">
          Growth areas (comma-separated)
          <input
            className="form-input"
            type="text"
            value={tagsToText(form.growth_areas ?? [])}
            onChange={e => set('growth_areas', textToTags(e.target.value))}
            placeholder="e.g. public speaking, estimation"
          />
        </label>

        <label className="form-label">
          Demonstrated skills (comma-separated)
          <input
            className="form-input"
            type="text"
            value={tagsToText(form.demonstrated_skills ?? [])}
            onChange={e => set('demonstrated_skills', textToTags(e.target.value))}
            placeholder="e.g. python, sql, system design"
          />
        </label>

        <label className="form-label">
          Demonstrated domains (comma-separated)
          <input
            className="form-input"
            type="text"
            value={tagsToText(form.demonstrated_domains ?? [])}
            onChange={e => set('demonstrated_domains', textToTags(e.target.value))}
            placeholder="e.g. data engineering, fintech"
          />
        </label>

        <label className="form-label">
          Work style
          <input
            className="form-input"
            type="text"
            value={form.work_style ?? ''}
            onChange={e => set('work_style', e.target.value || null)}
            placeholder="e.g. async-first, deep focus"
          />
        </label>

        <label className="form-label">
          Role fit
          <input
            className="form-input"
            type="text"
            value={form.role_fit ?? ''}
            onChange={e => set('role_fit', e.target.value || null)}
            placeholder="e.g. IC engineer over manager"
          />
        </label>
      </div>

      <div className="ca-uses-row">
        <span className="form-label-text">Allowed uses</span>
        {Object.entries(ASSESSMENT_ALLOWED_USE_LABELS).map(([v, l]) => (
          <label key={v} className="form-checkbox-row">
            <input
              type="checkbox"
              checked={form.allowed_uses?.includes(v) ?? false}
              onChange={() => toggleUse(v)}
            />
            {l}
          </label>
        ))}
      </div>

      {err && <div className="form-error">{err}</div>}

      <div className="form-actions">
        <button type="submit" className="btn btn--primary" disabled={saving}>
          {saving ? 'Saving…' : initial ? 'Save changes' : 'Add assessment'}
        </button>
        <button type="button" className="btn btn--ghost" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </form>
  )
}

// ── AssessmentCard ─────────────────────────────────────────────────────────────

interface CardProps {
  assessment: CandidateAssessment
  onEdit:          () => void
  onDelete:        () => void
  onSetPreferred:  () => void
}

function AssessmentCard({ assessment: a, onEdit, onDelete, onSetPreferred }: CardProps) {
  const [expanded,     setExpanded]     = useState(false)
  const [confirmDelete, setConfirmDelete] = useState(false)

  return (
    <div className={`ca-card ${a.is_preferred ? 'ca-card--preferred' : ''}`}>
      <div className="ca-card-header">
        <div className="ca-card-title-row">
          <button
            className={`ca-star-btn ${a.is_preferred ? 'ca-star-btn--active' : ''}`}
            onClick={onSetPreferred}
            title={a.is_preferred ? 'Preferred assessment' : 'Set as preferred'}
          >
            {a.is_preferred ? '★' : '☆'}
          </button>
          <span className="ca-kind-badge ca-kind-badge--{a.assessment_kind}">
            {ASSESSMENT_KIND_LABELS[a.assessment_kind] ?? a.assessment_kind}
          </span>
          <span className="ca-source-badge">
            {ASSESSMENT_SOURCE_LABELS[a.source_type] ?? a.source_type}
            {a.source_label && <span className="ca-source-label"> · {a.source_label}</span>}
          </span>
          {a.confidence && (
            <span className="ca-confidence-badge">
              {ASSESSMENT_CONFIDENCE_LABELS[a.confidence] ?? a.confidence}
            </span>
          )}
        </div>
        <div className="ca-card-actions">
          <button className="btn btn--ghost btn--sm" onClick={() => setExpanded(e => !e)}>
            {expanded ? 'Collapse' : 'Expand'}
          </button>
          <button className="btn btn--ghost btn--sm" onClick={onEdit}>Edit</button>
          {confirmDelete ? (
            <>
              <button className="btn btn--danger btn--sm" onClick={onDelete}>Confirm delete</button>
              <button className="btn btn--ghost btn--sm" onClick={() => setConfirmDelete(false)}>Cancel</button>
            </>
          ) : (
            <button className="btn btn--ghost-danger btn--sm" onClick={() => setConfirmDelete(true)}>
              Delete
            </button>
          )}
        </div>
      </div>

      {a.allowed_uses.length > 0 && (
        <div className="ca-uses-pills">
          {a.allowed_uses.map(u => (
            <span key={u} className="ca-use-pill">
              {ASSESSMENT_ALLOWED_USE_LABELS[u] ?? u}
            </span>
          ))}
        </div>
      )}

      {(a.strengths.length > 0 || a.growth_areas.length > 0 || a.demonstrated_skills.length > 0) && (
        <div className="ca-tag-rows">
          {a.strengths.length > 0 && (
            <div className="ca-tag-row">
              <span className="ca-tag-label">Strengths:</span>
              {a.strengths.map(t => <span key={t} className="ca-tag ca-tag--strength">{t}</span>)}
            </div>
          )}
          {a.growth_areas.length > 0 && (
            <div className="ca-tag-row">
              <span className="ca-tag-label">Growth:</span>
              {a.growth_areas.map(t => <span key={t} className="ca-tag ca-tag--growth">{t}</span>)}
            </div>
          )}
          {a.demonstrated_skills.length > 0 && (
            <div className="ca-tag-row">
              <span className="ca-tag-label">Skills:</span>
              {a.demonstrated_skills.map(t => <span key={t} className="ca-tag">{t}</span>)}
            </div>
          )}
          {a.demonstrated_domains.length > 0 && (
            <div className="ca-tag-row">
              <span className="ca-tag-label">Domains:</span>
              {a.demonstrated_domains.map(t => <span key={t} className="ca-tag">{t}</span>)}
            </div>
          )}
        </div>
      )}

      {expanded && (
        <div className="ca-card-body">
          {a.raw_text && <pre className="ca-raw-text">{a.raw_text}</pre>}
          {(a.work_style || a.role_fit) && (
            <div className="ca-meta-details">
              {a.work_style && <div><strong>Work style:</strong> {a.work_style}</div>}
              {a.role_fit   && <div><strong>Role fit:</strong> {a.role_fit}</div>}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── CandidateAssessmentSection ─────────────────────────────────────────────────

export function CandidateAssessmentSection() {
  const [assessments, setAssessments] = useState<CandidateAssessment[]>([])
  const [loading,     setLoading]     = useState(true)
  const [err,         setErr]         = useState<string | null>(null)

  const [showAdd,   setShowAdd]   = useState(false)
  const [editingId, setEditingId] = useState<number | null>(null)

  const [filterKind, setFilterKind] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setErr(null)
    try {
      const items = await api.listAssessments()
      setAssessments(items)
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  async function handleAdd(payload: CandidateAssessmentPayload) {
    await api.createAssessment(payload)
    setShowAdd(false)
    await load()
  }

  async function handleUpdate(id: number, payload: CandidateAssessmentPayload) {
    await api.updateAssessment(id, payload)
    setEditingId(null)
    await load()
  }

  async function handleDelete(id: number) {
    await api.deleteAssessment(id)
    await load()
  }

  async function handleSetPreferred(id: number) {
    await api.setPreferredAssessment(id)
    await load()
  }

  const visible = filterKind
    ? assessments.filter(a => a.assessment_kind === filterKind)
    : assessments

  return (
    <section className="ca-section">
      <div className="ca-section-header">
        <div>
          <h2 className="ca-section-title">Candidate Assessments</h2>
          <p className="ca-section-subtitle">
            Structured, AI-derived or manual assessments of your strengths, work style, and fit.
          </p>
        </div>
        {!showAdd && (
          <button className="btn btn--primary" onClick={() => setShowAdd(true)}>
            + Add assessment
          </button>
        )}
      </div>

      {showAdd && (
        <div className="ca-add-panel">
          <AssessmentForm
            onSave={handleAdd}
            onCancel={() => setShowAdd(false)}
          />
        </div>
      )}

      <div className="ca-filter-bar">
        <select
          className="form-select ca-filter-select"
          value={filterKind}
          onChange={e => setFilterKind(e.target.value)}
        >
          <option value="">All kinds</option>
          {Object.entries(ASSESSMENT_KIND_LABELS).map(([v, l]) => (
            <option key={v} value={v}>{l}</option>
          ))}
        </select>
        <span className="ca-count">{visible.length} assessment{visible.length !== 1 ? 's' : ''}</span>
      </div>

      {loading && <div className="ca-loading">Loading…</div>}
      {err     && <div className="form-error">{err}</div>}

      {!loading && !err && visible.length === 0 && (
        <div className="ca-empty">
          No assessments yet. Add one to capture AI or manual insights about your work.
        </div>
      )}

      <div className="ca-card-list">
        {visible.map(a =>
          editingId === a.id ? (
            <div key={a.id} className="ca-edit-panel">
              <AssessmentForm
                initial={a}
                onSave={payload => handleUpdate(a.id, payload)}
                onCancel={() => setEditingId(null)}
              />
            </div>
          ) : (
            <AssessmentCard
              key={a.id}
              assessment={a}
              onEdit={() => setEditingId(a.id)}
              onDelete={() => handleDelete(a.id)}
              onSetPreferred={() => handleSetPreferred(a.id)}
            />
          )
        )}
      </div>
    </section>
  )
}
