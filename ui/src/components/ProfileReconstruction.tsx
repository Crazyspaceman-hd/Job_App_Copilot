/**
 * ProfileReconstruction.tsx
 *
 * Paste messy work evidence → extract observations → review → promote to Evidence Bank.
 *
 * Flow:
 *   1. Input panel: paste raw text, choose source type, title, Extract
 *   2. Review panel: observations (accept/reject/edit) + claims (accept/reject/edit)
 *   3. Draft summary panel: generated from accepted observations
 *   4. Saved sources list: reload and re-review any previous session
 */

import { useEffect, useState } from 'react'
import type { PRClaim, PRObservation, PRRunResult, PRSource } from '../lib/api'
import { PR_SOURCE_TYPE_LABELS, api } from '../lib/api'

// ── Constants ─────────────────────────────────────────────────────────────────

const SOURCE_TYPES = Object.keys(PR_SOURCE_TYPE_LABELS)

const STRENGTH_LABELS: Record<string, string> = {
  direct:   'Direct',
  adjacent: 'Adjacent',
  inferred: 'Inferred',
}

const CONFIDENCE_LABELS: Record<string, string> = {
  high:   'High',
  medium: 'Medium',
  low:    'Low',
}

const ALLOWED_USES_ALL = [
  'resume', 'cover_letter', 'interview_prep', 'project_repositioning',
]

// ── Strength badge ────────────────────────────────────────────────────────────

function StrengthBadge({ strength }: { strength: string }) {
  const cls = `pr-strength-badge pr-strength-badge--${strength}`
  return <span className={cls}>{STRENGTH_LABELS[strength] ?? strength}</span>
}

function ConfidenceBadge({ confidence }: { confidence: string }) {
  const cls = `pr-confidence-badge pr-confidence-badge--${confidence}`
  return <span className={cls}>{CONFIDENCE_LABELS[confidence] ?? confidence}</span>
}

function ReviewBadge({ state }: { state: string }) {
  const cls = `pr-review-badge pr-review-badge--${state}`
  return <span className={cls}>{state}</span>
}

// ── Observation card ──────────────────────────────────────────────────────────

interface ObsCardProps {
  obs:      PRObservation
  claim:    PRClaim | undefined
  onUpdate: (id: number, patch: Partial<PRObservation>) => void
  onClaimUpdate: (id: number, patch: Partial<PRClaim>) => void
  onPromote: (claimId: number) => void
  promoting: number | null
}

function ObsCard({ obs, claim, onUpdate, onClaimUpdate, onPromote, promoting }: ObsCardProps) {
  const [editing, setEditing] = useState(false)
  const [editText, setEditText] = useState(obs.text)
  const [editStrength, setEditStrength] = useState(obs.evidence_strength)
  const [editConf, setEditConf] = useState(obs.confidence)
  const [editSkills, setEditSkills] = useState(obs.skill_tags.join(', '))
  const [editDomains, setEditDomains] = useState(obs.domain_tags.join(', '))
  const [editClaimText, setEditClaimText] = useState(claim?.text ?? '')
  const [claimEditing, setClaimEditing] = useState(false)

  function saveObsEdit() {
    onUpdate(obs.id, {
      text:              editText.trim(),
      evidence_strength: editStrength,
      confidence:        editConf,
      skill_tags:        editSkills.split(',').map(t => t.trim()).filter(Boolean),
      domain_tags:       editDomains.split(',').map(t => t.trim()).filter(Boolean),
    })
    setEditing(false)
  }

  function saveClaimEdit() {
    if (claim) {
      onClaimUpdate(claim.id, { text: editClaimText.trim() })
      setClaimEditing(false)
    }
  }

  const isAccepted = obs.review_state === 'accepted'
  const isRejected = obs.review_state === 'rejected'
  const isPending  = obs.review_state === 'pending'
  const claimAccepted = claim?.review_state === 'accepted'
  const promoted   = claim?.promoted_item_id != null

  return (
    <div className={`pr-obs-card pr-obs-card--${obs.review_state}`}>
      <div className="pr-obs-header">
        <div className="pr-obs-badges">
          <StrengthBadge strength={obs.evidence_strength} />
          <ConfidenceBadge confidence={obs.confidence} />
          <ReviewBadge state={obs.review_state} />
        </div>
        <div className="pr-obs-actions">
          {!isAccepted && !isRejected && (
            <button className="btn btn--xs btn--success"
              onClick={() => onUpdate(obs.id, { review_state: 'accepted' })}>
              Accept
            </button>
          )}
          {!isRejected && (
            <button className="btn btn--xs btn--danger"
              onClick={() => onUpdate(obs.id, { review_state: 'rejected' })}>
              Reject
            </button>
          )}
          {isRejected && (
            <button className="btn btn--xs"
              onClick={() => onUpdate(obs.id, { review_state: 'pending' })}>
              Restore
            </button>
          )}
          <button className="btn btn--xs btn--ghost" onClick={() => setEditing(e => !e)}>
            {editing ? 'Cancel edit' : 'Edit'}
          </button>
        </div>
      </div>

      {editing ? (
        <div className="pr-obs-edit">
          <label className="pr-label">Observation text</label>
          <textarea className="pr-textarea"
            value={editText} rows={3}
            onChange={e => setEditText(e.target.value)} />
          <div className="pr-edit-row">
            <label className="pr-label">Strength</label>
            <select className="pr-select" value={editStrength}
              onChange={e => setEditStrength(e.target.value)}>
              <option value="direct">Direct</option>
              <option value="adjacent">Adjacent</option>
              <option value="inferred">Inferred</option>
            </select>
            <label className="pr-label">Confidence</label>
            <select className="pr-select" value={editConf}
              onChange={e => setEditConf(e.target.value)}>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
          </div>
          <label className="pr-label">Skill tags (comma-separated)</label>
          <input className="pr-input" value={editSkills}
            onChange={e => setEditSkills(e.target.value)} />
          <label className="pr-label">Domain tags (comma-separated)</label>
          <input className="pr-input" value={editDomains}
            onChange={e => setEditDomains(e.target.value)} />
          <button className="btn btn--sm btn--primary" onClick={saveObsEdit}>
            Save observation
          </button>
        </div>
      ) : (
        <p className="pr-obs-text">{obs.text}</p>
      )}

      {obs.skill_tags.length > 0 && (
        <div className="pr-tag-row">
          {obs.skill_tags.map(t => (
            <span key={t} className="pr-tag pr-tag--skill">{t}</span>
          ))}
          {obs.domain_tags.map(t => (
            <span key={t} className="pr-tag pr-tag--domain">{t}</span>
          ))}
        </div>
      )}

      {/* Claim sub-card */}
      {claim && !isRejected && (
        <div className="pr-claim-sub">
          <div className="pr-claim-header">
            <span className="pr-claim-label">Claim candidate</span>
            <span className={`pr-framing-badge pr-framing-badge--${claim.framing}`}>
              {claim.framing}
            </span>
            <ReviewBadge state={claim.review_state} />
            <div className="pr-claim-actions">
              {claim.review_state !== 'accepted' && (
                <button className="btn btn--xs btn--success"
                  onClick={() => onClaimUpdate(claim.id, { review_state: 'accepted' })}>
                  Accept claim
                </button>
              )}
              {claim.review_state !== 'rejected' && claim.review_state !== 'pending' && (
                <button className="btn btn--xs btn--danger"
                  onClick={() => onClaimUpdate(claim.id, { review_state: 'rejected' })}>
                  Reject
                </button>
              )}
              {claim.review_state === 'rejected' && (
                <button className="btn btn--xs"
                  onClick={() => onClaimUpdate(claim.id, { review_state: 'pending' })}>
                  Restore
                </button>
              )}
              {claimAccepted && !promoted && (
                <button
                  className="btn btn--xs btn--primary"
                  disabled={promoting === claim.id}
                  onClick={() => onPromote(claim.id)}>
                  {promoting === claim.id ? 'Promoting…' : '→ Evidence Bank'}
                </button>
              )}
              {promoted && (
                <span className="pr-promoted-badge">✓ In Evidence Bank</span>
              )}
              <button className="btn btn--xs btn--ghost"
                onClick={() => setClaimEditing(ce => !ce)}>
                {claimEditing ? 'Cancel' : 'Edit'}
              </button>
            </div>
          </div>
          {claimEditing ? (
            <div className="pr-claim-edit">
              <textarea className="pr-textarea" rows={2}
                value={editClaimText}
                onChange={e => setEditClaimText(e.target.value)} />
              <button className="btn btn--sm btn--primary" onClick={saveClaimEdit}>
                Save claim
              </button>
            </div>
          ) : (
            <p className="pr-claim-text">{claim.text}</p>
          )}
        </div>
      )}
    </div>
  )
}

// ── Input form ────────────────────────────────────────────────────────────────

interface InputPanelProps {
  onExtracted: (run: PRRunResult) => void
}

function InputPanel({ onExtracted }: InputPanelProps) {
  const [rawText,    setRawText]    = useState('')
  const [sourceType, setSourceType] = useState('free_text')
  const [title,      setTitle]      = useState('')
  const [loading,    setLoading]    = useState(false)
  const [error,      setError]      = useState<string | null>(null)

  async function handleExtract() {
    if (!rawText.trim()) { setError('Paste some evidence text first.'); return }
    setLoading(true)
    setError(null)
    try {
      const src = await api.createPRSource({ raw_text: rawText, source_type: sourceType, title })
      const run = await api.runPRReconstruction(src.id)
      onExtracted(run)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="pr-input-panel">
      <h2 className="pr-section-title">Paste evidence</h2>
      <p className="pr-hint">
        Paste raw notes, an old resume, a debugging story, project descriptions — anything that
        captures real professional experience.  The extractor will identify observable claims
        without inventing or inflating them.
      </p>

      <div className="pr-input-row">
        <div className="pr-field">
          <label className="pr-label">Source type</label>
          <select className="pr-select" value={sourceType}
            onChange={e => setSourceType(e.target.value)}>
            {SOURCE_TYPES.map(t => (
              <option key={t} value={t}>{PR_SOURCE_TYPE_LABELS[t]}</option>
            ))}
          </select>
        </div>
        <div className="pr-field pr-field--grow">
          <label className="pr-label">Label / title (optional)</label>
          <input className="pr-input" placeholder="e.g. Backend work at Acme 2023"
            value={title} onChange={e => setTitle(e.target.value)} />
        </div>
      </div>

      <textarea
        className="pr-textarea pr-textarea--main"
        placeholder="Paste your raw evidence here…"
        rows={12}
        value={rawText}
        onChange={e => setRawText(e.target.value)}
      />

      {error && <div className="pr-error">{error}</div>}

      <button className="btn btn--primary" disabled={loading} onClick={handleExtract}>
        {loading ? 'Extracting…' : 'Extract observations'}
      </button>
    </div>
  )
}

// ── Review panel ──────────────────────────────────────────────────────────────

interface ReviewPanelProps {
  run:      PRRunResult
  onReset:  () => void
}

function ReviewPanel({ run: initialRun, onReset }: ReviewPanelProps) {
  const [observations, setObservations] = useState<PRObservation[]>(initialRun.observations)
  const [claims,       setClaims]       = useState<PRClaim[]>(initialRun.claims)
  const [summary,      setSummary]      = useState(initialRun.draft_summary)
  const [promoting,    setPromoting]    = useState<number | null>(null)
  const [promoteMsg,   setPromoteMsg]   = useState<string | null>(null)
  const [filter,       setFilter]       = useState<'all' | 'pending' | 'accepted' | 'rejected'>('all')

  const claimByObsId = Object.fromEntries(claims.map(c => [c.observation_id, c]))

  function updateObs(id: number, updated: PRObservation) {
    setObservations(prev => prev.map(o => o.id === id ? updated : o))
  }
  function updateClaim(id: number, updated: PRClaim) {
    setClaims(prev => prev.map(c => c.id === id ? updated : c))
  }

  async function handleObsUpdate(obsId: number, patch: Partial<PRObservation>) {
    try {
      const updated = await api.updatePRObservation(obsId, patch)
      updateObs(obsId, updated)
    } catch (e: unknown) {
      console.error('Observation update failed', e)
    }
  }

  async function handleClaimUpdate(claimId: number, patch: Partial<PRClaim>) {
    try {
      const updated = await api.updatePRClaim(claimId, patch)
      updateClaim(claimId, updated)
    } catch (e: unknown) {
      console.error('Claim update failed', e)
    }
  }

  async function handlePromote(claimId: number) {
    setPromoting(claimId)
    setPromoteMsg(null)
    try {
      const result = await api.promotePRClaim(claimId)
      setClaims(prev => prev.map(c =>
        c.id === claimId ? { ...c, promoted_item_id: result.evidence_item_id } : c
      ))
      setPromoteMsg(`"${result.title}" added to Evidence Bank.`)
    } catch (e: unknown) {
      setPromoteMsg(e instanceof Error ? e.message : String(e))
    } finally {
      setPromoting(null)
    }
  }

  async function acceptAll() {
    const pending = observations.filter(o => o.review_state === 'pending')
    for (const obs of pending) {
      await handleObsUpdate(obs.id, { review_state: 'accepted' })
    }
    const pendingClaims = claims.filter(c => c.review_state === 'pending')
    for (const c of pendingClaims) {
      await handleClaimUpdate(c.id, { review_state: 'accepted' })
    }
  }

  const visibleObs = filter === 'all'
    ? observations
    : observations.filter(o => o.review_state === filter)

  const accepted  = observations.filter(o => o.review_state === 'accepted').length
  const rejected  = observations.filter(o => o.review_state === 'rejected').length
  const pending   = observations.filter(o => o.review_state === 'pending').length
  const promoted  = claims.filter(c => c.promoted_item_id != null).length

  return (
    <div className="pr-review-panel">
      {/* Stats bar */}
      <div className="pr-stats-bar">
        <span className="pr-stat">{observations.length} observations</span>
        <span className="pr-stat pr-stat--accepted">{accepted} accepted</span>
        <span className="pr-stat pr-stat--rejected">{rejected} rejected</span>
        <span className="pr-stat">{pending} pending</span>
        <span className="pr-stat pr-stat--promoted">{promoted} promoted</span>
        <div className="pr-stats-actions">
          <button className="btn btn--sm" onClick={acceptAll}>Accept all pending</button>
          <button className="btn btn--sm btn--ghost" onClick={onReset}>New evidence</button>
        </div>
      </div>

      {promoteMsg && (
        <div className="pr-promote-msg">{promoteMsg}</div>
      )}

      <div className="pr-filter-row">
        {(['all', 'pending', 'accepted', 'rejected'] as const).map(f => (
          <button key={f}
            className={`pr-filter-btn ${filter === f ? 'pr-filter-btn--active' : ''}`}
            onClick={() => setFilter(f)}>
            {f === 'all' ? 'All' : f.charAt(0).toUpperCase() + f.slice(1)}
          </button>
        ))}
      </div>

      <div className="pr-obs-list">
        {visibleObs.length === 0 && (
          <p className="pr-empty">No observations match this filter.</p>
        )}
        {visibleObs.map(obs => (
          <ObsCard
            key={obs.id}
            obs={obs}
            claim={claimByObsId[obs.id]}
            onUpdate={handleObsUpdate}
            onClaimUpdate={handleClaimUpdate}
            onPromote={handlePromote}
            promoting={promoting}
          />
        ))}
      </div>

      {/* Draft summary */}
      <div className="pr-summary-box">
        <h3 className="pr-summary-title">Draft profile summary</h3>
        <p className="pr-summary-text">{summary}</p>
        <button className="btn btn--sm btn--ghost"
          onClick={() => {
            api.getPRSummary(initialRun.source_id)
              .then(r => setSummary(r.summary))
              .catch(() => {})
          }}>
          Refresh summary
        </button>
      </div>
    </div>
  )
}

// ── Saved sources list ────────────────────────────────────────────────────────

interface SavedSourcesProps {
  onLoad: (run: PRRunResult) => void
}

function SavedSources({ onLoad }: SavedSourcesProps) {
  const [sources,  setSources]  = useState<PRSource[]>([])
  const [loading,  setLoading]  = useState(true)
  const [expanded, setExpanded] = useState(false)
  const [running,  setRunning]  = useState<number | null>(null)

  useEffect(() => {
    api.listPRSources()
      .then(setSources)
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  async function handleReload(source: PRSource) {
    setRunning(source.id)
    try {
      const run = await api.runPRReconstruction(source.id)
      onLoad(run)
    } catch (e: unknown) {
      console.error(e)
    } finally {
      setRunning(null)
    }
  }

  async function handleDelete(id: number) {
    if (!confirm('Delete this source and all its observations?')) return
    try {
      await api.deletePRSource(id)
      setSources(prev => prev.filter(s => s.id !== id))
    } catch (e: unknown) {
      console.error(e)
    }
  }

  if (loading || sources.length === 0) return null

  return (
    <div className="pr-saved-sources">
      <button className="pr-sources-toggle"
        onClick={() => setExpanded(e => !e)}>
        {expanded ? '▾' : '▸'} Previous sources ({sources.length})
      </button>
      {expanded && (
        <div className="pr-sources-list">
          {sources.map(s => (
            <div key={s.id} className="pr-source-row">
              <div className="pr-source-meta">
                <span className="pr-source-title">{s.title || '(untitled)'}</span>
                <span className="pr-source-type">{PR_SOURCE_TYPE_LABELS[s.source_type] ?? s.source_type}</span>
                <span className="pr-source-date">{s.created_at.slice(0, 10)}</span>
              </div>
              <div className="pr-source-actions">
                <button className="btn btn--xs"
                  disabled={running === s.id}
                  onClick={() => handleReload(s)}>
                  {running === s.id ? 'Loading…' : 'Review'}
                </button>
                <button className="btn btn--xs btn--danger"
                  onClick={() => handleDelete(s.id)}>
                  Delete
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export function ProfileReconstruction() {
  const [runResult, setRunResult] = useState<PRRunResult | null>(null)

  function handleExtracted(run: PRRunResult) {
    setRunResult(run)
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  return (
    <div className="pr-root">
      <div className="pr-header">
        <h1 className="pr-title">Profile Reconstruction</h1>
        <p className="pr-subtitle">
          Turn raw notes, old resumes, or work stories into structured evidence —
          without inventing or inflating your experience.
        </p>
      </div>

      {runResult ? (
        <ReviewPanel
          run={runResult}
          onReset={() => setRunResult(null)}
        />
      ) : (
        <InputPanel onExtracted={handleExtracted} />
      )}

      <SavedSources onLoad={run => setRunResult(run)} />
    </div>
  )
}
