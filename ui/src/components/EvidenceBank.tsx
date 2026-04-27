import { useEffect, useState } from 'react'
import type { EvidenceItem, EvidenceItemPayload } from '../lib/api'
import {
  ALLOWED_USE_LABELS,
  SOURCE_TYPE_LABELS,
  STRENGTH_LABELS,
  api,
} from '../lib/api'

// ── Constants ─────────────────────────────────────────────────────────────────

const SOURCE_TYPES   = Object.keys(SOURCE_TYPE_LABELS)
const STRENGTHS      = Object.keys(STRENGTH_LABELS)
const ALLOWED_USES   = Object.keys(ALLOWED_USE_LABELS)
const CONFIDENCE_OPTS = ['', 'high', 'medium', 'low']

// ── Form state ────────────────────────────────────────────────────────────────

interface FormState {
  title:                 string
  raw_text:              string
  source_type:           string
  evidence_strength:     string
  skill_tags:            string   // comma-separated
  domain_tags:           string
  business_problem_tags: string
  allowed_uses:          string[] // checkboxes
  confidence:            string
  notes:                 string
}

const EMPTY_FORM: FormState = {
  title:                 '',
  raw_text:              '',
  source_type:           'other',
  evidence_strength:     'adjacent',
  skill_tags:            '',
  domain_tags:           '',
  business_problem_tags: '',
  allowed_uses:          [],
  confidence:            '',
  notes:                 '',
}

function formToPayload(f: FormState): EvidenceItemPayload {
  const splitTags = (s: string) =>
    s.split(',').map(t => t.trim()).filter(Boolean)

  return {
    title:                 f.title.trim(),
    raw_text:              f.raw_text.trim(),
    source_type:           f.source_type,
    evidence_strength:     f.evidence_strength,
    skill_tags:            splitTags(f.skill_tags),
    domain_tags:           splitTags(f.domain_tags),
    business_problem_tags: splitTags(f.business_problem_tags),
    allowed_uses:          f.allowed_uses,
    confidence:            f.confidence || null,
    notes:                 f.notes.trim() || null,
  }
}

function itemToForm(item: EvidenceItem): FormState {
  return {
    title:                 item.title,
    raw_text:              item.raw_text,
    source_type:           item.source_type,
    evidence_strength:     item.evidence_strength,
    skill_tags:            item.skill_tags.join(', '),
    domain_tags:           item.domain_tags.join(', '),
    business_problem_tags: item.business_problem_tags.join(', '),
    allowed_uses:          item.allowed_uses,
    confidence:            item.confidence ?? '',
    notes:                 item.notes ?? '',
  }
}

// ── Small sub-components ──────────────────────────────────────────────────────

function StrengthBadge({ strength }: { strength: string }) {
  return (
    <span className={`eb-strength-badge eb-strength--${strength}`}>
      {STRENGTH_LABELS[strength] ?? strength}
    </span>
  )
}

function SourceBadge({ sourceType }: { sourceType: string }) {
  return (
    <span className="eb-source-badge">
      {SOURCE_TYPE_LABELS[sourceType] ?? sourceType}
    </span>
  )
}

function TagList({ tags, cls }: { tags: string[]; cls?: string }) {
  if (!tags.length) return null
  return (
    <span className={`eb-tag-list ${cls ?? ''}`}>
      {tags.map(t => <span key={t} className="eb-tag">{t}</span>)}
    </span>
  )
}

// ── Item form (used for both add and edit) ────────────────────────────────────

interface ItemFormProps {
  initial:    FormState
  onSave:     (f: FormState) => Promise<void>
  onCancel:   () => void
  saving:     boolean
  error:      string | null
  submitLabel: string
}

function ItemForm({ initial, onSave, onCancel, saving, error, submitLabel }: ItemFormProps) {
  const [f, setF] = useState<FormState>(initial)

  function set(key: keyof FormState) {
    return (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) =>
      setF(prev => ({ ...prev, [key]: e.target.value }))
  }

  function toggleUse(use: string) {
    setF(prev => ({
      ...prev,
      allowed_uses: prev.allowed_uses.includes(use)
        ? prev.allowed_uses.filter(u => u !== use)
        : [...prev.allowed_uses, use],
    }))
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    await onSave(f)
  }

  return (
    <form className="eb-form" onSubmit={handleSubmit}>
      <div className="eb-form-grid">
        <div className="eb-form-field eb-form-span">
          <label className="form-label form-label--required">Title / label</label>
          <input className="form-input" placeholder="Short label for this evidence"
            value={f.title} onChange={set('title')} required />
        </div>

        <div className="eb-form-field eb-form-span">
          <label className="form-label form-label--required">Verbatim text</label>
          <textarea className="form-textarea" rows={5}
            placeholder="Paste the exact text — a bullet, a story, a metric — exactly as you'd say it."
            value={f.raw_text} onChange={set('raw_text')} required />
          <span className="form-hint">Never edit for polish here. This is a source-of-truth store.</span>
        </div>

        <div className="eb-form-field">
          <label className="form-label">Source type</label>
          <select className="form-select" value={f.source_type} onChange={set('source_type')}>
            {SOURCE_TYPES.map(t => (
              <option key={t} value={t}>{SOURCE_TYPE_LABELS[t]}</option>
            ))}
          </select>
        </div>

        <div className="eb-form-field">
          <label className="form-label">Evidence strength</label>
          <select className="form-select" value={f.evidence_strength} onChange={set('evidence_strength')}>
            {STRENGTHS.map(s => (
              <option key={s} value={s}>{STRENGTH_LABELS[s]}</option>
            ))}
          </select>
          <span className="form-hint">
            Direct = you did this. Adjacent = transferable. Inferred = reasonable but unverified.
          </span>
        </div>

        <div className="eb-form-field">
          <label className="form-label">Skill tags</label>
          <input className="form-input" placeholder="python, fastapi, postgresql"
            value={f.skill_tags} onChange={set('skill_tags')} />
          <span className="form-hint">Comma-separated. Normalized to lowercase on save.</span>
        </div>

        <div className="eb-form-field">
          <label className="form-label">Domain tags</label>
          <input className="form-input" placeholder="fintech, data engineering"
            value={f.domain_tags} onChange={set('domain_tags')} />
        </div>

        <div className="eb-form-field eb-form-span">
          <label className="form-label">Business problem tags</label>
          <input className="form-input" placeholder="latency, cost reduction, reliability"
            value={f.business_problem_tags} onChange={set('business_problem_tags')} />
        </div>

        <div className="eb-form-field eb-form-span">
          <label className="form-label">Allowed uses</label>
          <div className="eb-use-checkboxes">
            {ALLOWED_USES.map(use => (
              <label key={use} className="eb-use-checkbox-label">
                <input
                  type="checkbox"
                  checked={f.allowed_uses.includes(use)}
                  onChange={() => toggleUse(use)}
                />
                {' '}{ALLOWED_USE_LABELS[use]}
              </label>
            ))}
          </div>
        </div>

        <div className="eb-form-field">
          <label className="form-label">Confidence</label>
          <select className="form-select" value={f.confidence} onChange={set('confidence')}>
            {CONFIDENCE_OPTS.map(o => (
              <option key={o} value={o}>{o || '(not set)'}</option>
            ))}
          </select>
        </div>

        <div className="eb-form-field">
          <label className="form-label">Notes</label>
          <textarea className="form-textarea" rows={2}
            placeholder="Any context: when, where, who can verify"
            value={f.notes} onChange={set('notes')} />
        </div>
      </div>

      {error && <div className="form-error" style={{ marginTop: '0.5rem' }}>{error}</div>}

      <div className="eb-form-actions">
        <button className="btn btn--primary" type="submit" disabled={saving || !f.title.trim() || !f.raw_text.trim()}>
          {saving ? 'Saving…' : submitLabel}
        </button>
        <button className="btn btn--secondary" type="button" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </form>
  )
}

// ── Single item card ──────────────────────────────────────────────────────────

interface CardProps {
  item:       EvidenceItem
  onUpdated:  (item: EvidenceItem) => void
  onDeleted:  (id: number) => void
}

function EvidenceCard({ item, onUpdated, onDeleted }: CardProps) {
  const [expanded, setExpanded] = useState(false)
  const [editing,  setEditing]  = useState(false)
  const [saving,   setSaving]   = useState(false)
  const [error,    setError]    = useState<string | null>(null)
  const [confirming, setConfirming] = useState(false)

  async function handleSave(f: FormState) {
    setSaving(true)
    setError(null)
    try {
      const updated = await api.updateEvidence(item.item_id, formToPayload(f))
      onUpdated(updated)
      setEditing(false)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete() {
    if (!confirming) { setConfirming(true); return }
    try {
      await api.deleteEvidence(item.item_id)
      onDeleted(item.item_id)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
      setConfirming(false)
    }
  }

  if (editing) {
    return (
      <div className="eb-card eb-card--editing">
        <ItemForm
          initial     = {itemToForm(item)}
          onSave      = {handleSave}
          onCancel    = {() => { setEditing(false); setError(null) }}
          saving      = {saving}
          error       = {error}
          submitLabel = "Save changes"
        />
      </div>
    )
  }

  return (
    <div className={`eb-card ${expanded ? 'eb-card--expanded' : ''}`}>
      <div className="eb-card-header" onClick={() => setExpanded(e => !e)} role="button" tabIndex={0}
        onKeyDown={e => e.key === 'Enter' && setExpanded(x => !x)}>
        <div className="eb-card-title-row">
          <span className="eb-card-title">{item.title}</span>
          <span className="eb-card-badges">
            <SourceBadge sourceType={item.source_type} />
            <StrengthBadge strength={item.evidence_strength} />
            {item.confidence && (
              <span className="eb-confidence-badge">{item.confidence}</span>
            )}
          </span>
        </div>

        <div className="eb-card-meta">
          {item.allowed_uses.length > 0 && (
            <span className="eb-uses">
              {item.allowed_uses.map(u => (
                <span key={u} className="eb-use-pill">{ALLOWED_USE_LABELS[u] ?? u}</span>
              ))}
            </span>
          )}
          <TagList tags={item.skill_tags}            cls="eb-tags--skill" />
          <TagList tags={item.domain_tags}           cls="eb-tags--domain" />
          <TagList tags={item.business_problem_tags} cls="eb-tags--biz" />
        </div>
      </div>

      {expanded && (
        <div className="eb-card-body">
          <pre className="eb-raw-text">{item.raw_text}</pre>
          {item.notes && (
            <p className="eb-notes"><strong>Notes:</strong> {item.notes}</p>
          )}
          {error && <div className="form-error">{error}</div>}
          <div className="eb-card-actions">
            <button className="btn btn--secondary" onClick={() => setEditing(true)}>Edit</button>
            <button
              className={`btn ${confirming ? 'btn--danger' : 'btn--ghost-danger'}`}
              onClick={handleDelete}
              onBlur={() => setConfirming(false)}
            >
              {confirming ? 'Confirm delete' : 'Delete'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Main view ─────────────────────────────────────────────────────────────────

export function EvidenceBank() {
  const [items,     setItems]     = useState<EvidenceItem[]>([])
  const [loading,   setLoading]   = useState(true)
  const [error,     setError]     = useState<string | null>(null)
  const [showAdd,   setShowAdd]   = useState(false)
  const [addSaving, setAddSaving] = useState(false)
  const [addError,  setAddError]  = useState<string | null>(null)

  const [filterSource,   setFilterSource]   = useState('')
  const [filterStrength, setFilterStrength] = useState('')

  useEffect(() => {
    setLoading(true)
    api.listEvidence()
      .then(data => { setItems(data); setLoading(false) })
      .catch(e => { setError(e instanceof Error ? e.message : String(e)); setLoading(false) })
  }, [])

  async function handleAdd(f: FormState) {
    setAddSaving(true)
    setAddError(null)
    try {
      const created = await api.createEvidence(formToPayload(f))
      setItems(prev => [created, ...prev])
      setShowAdd(false)
    } catch (e: unknown) {
      setAddError(e instanceof Error ? e.message : String(e))
    } finally {
      setAddSaving(false)
    }
  }

  function handleUpdated(updated: EvidenceItem) {
    setItems(prev => prev.map(i => i.item_id === updated.item_id ? updated : i))
  }

  function handleDeleted(id: number) {
    setItems(prev => prev.filter(i => i.item_id !== id))
  }

  const filtered = items.filter(item => {
    if (filterSource   && item.source_type       !== filterSource)   return false
    if (filterStrength && item.evidence_strength !== filterStrength) return false
    return true
  })

  return (
    <div className="intake-page">
      <div className="intake-page-header">
        <div className="eb-page-header-row">
          <div>
            <h2 className="intake-title">Evidence Bank</h2>
            <p className="intake-subtitle">
              Store verbatim phrasing, metrics, and stories from real experience.
              Nothing here is invented — these items become source material for
              resumes, cover letters, and interview prep.
            </p>
          </div>
          <button
            className="btn btn--primary btn--lg"
            onClick={() => { setShowAdd(s => !s); setAddError(null) }}
          >
            {showAdd ? 'Cancel' : '+ Add item'}
          </button>
        </div>
      </div>

      {/* ── Add form ────────────────────────────────────────────────────── */}
      {showAdd && (
        <div className="eb-add-panel">
          <div className="eb-add-panel-title">New evidence item</div>
          <ItemForm
            initial     = {EMPTY_FORM}
            onSave      = {handleAdd}
            onCancel    = {() => { setShowAdd(false); setAddError(null) }}
            saving      = {addSaving}
            error       = {addError}
            submitLabel = "Add item"
          />
        </div>
      )}

      {/* ── Filter bar ──────────────────────────────────────────────────── */}
      <div className="eb-filter-bar">
        <select className="form-select eb-filter-select" value={filterSource}
          onChange={e => setFilterSource(e.target.value)}>
          <option value="">All source types</option>
          {SOURCE_TYPES.map(t => (
            <option key={t} value={t}>{SOURCE_TYPE_LABELS[t]}</option>
          ))}
        </select>
        <select className="form-select eb-filter-select" value={filterStrength}
          onChange={e => setFilterStrength(e.target.value)}>
          <option value="">All strengths</option>
          {STRENGTHS.map(s => (
            <option key={s} value={s}>{STRENGTH_LABELS[s]}</option>
          ))}
        </select>
        <span className="eb-count">
          {filtered.length} of {items.length} item{items.length !== 1 ? 's' : ''}
        </span>
      </div>

      {/* ── Content ─────────────────────────────────────────────────────── */}
      {loading && <p className="muted">Loading evidence bank…</p>}
      {error   && <p className="err">{error}</p>}

      {!loading && !error && items.length === 0 && (
        <div className="panel-empty">
          <p>No evidence items yet.</p>
          <p className="panel-empty-cmd">
            Click "+ Add item" to save a bullet, metric, story, or rewrite that
            captures strong phrasing you want to preserve.
          </p>
        </div>
      )}

      {!loading && !error && items.length > 0 && filtered.length === 0 && (
        <p className="muted">No items match the current filters.</p>
      )}

      <div className="eb-list">
        {filtered.map(item => (
          <EvidenceCard
            key       = {item.item_id}
            item      = {item}
            onUpdated = {handleUpdated}
            onDeleted = {handleDeleted}
          />
        ))}
      </div>
    </div>
  )
}
