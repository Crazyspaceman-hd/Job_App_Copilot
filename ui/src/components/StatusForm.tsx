import { useState } from 'react'
import type { ApplicationRecord, DecisionPayload } from '../lib/api'

interface Props {
  current: ApplicationRecord
  onSave: (payload: DecisionPayload) => Promise<void>
  saving: boolean
  error: string | null
}

const STATUS_LABELS = ['apply', 'hold', 'skip'] as const

export function StatusForm({ current, onSave, saving, error }: Props) {
  const [status, setStatus]       = useState<string>(current.status ?? '')
  const [notes, setNotes]         = useState(current.notes ?? '')
  const [followUp, setFollowUp]   = useState(current.follow_up_date ?? '')
  const [platform, setPlatform]   = useState(current.platform ?? '')
  const [saved, setSaved]         = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!status) return
    await onSave({
      status: status as 'apply' | 'hold' | 'skip',
      notes: notes || undefined,
      follow_up_date: followUp || undefined,
      platform: platform || undefined,
    })
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  return (
    <form className="status-form" onSubmit={handleSubmit}>
      <div className="status-form-row">
        <label className="form-label">Status</label>
        <div className="status-btn-group">
          {STATUS_LABELS.map(s => (
            <button
              type="button"
              key={s}
              className={`status-btn status-btn--${s} ${status === s ? 'status-btn--active' : ''}`}
              onClick={() => setStatus(s)}
            >
              {s.charAt(0).toUpperCase() + s.slice(1)}
            </button>
          ))}
        </div>
      </div>

      <div className="status-form-row">
        <label className="form-label" htmlFor="sf-platform">Platform</label>
        <input
          id="sf-platform"
          className="form-input"
          placeholder="LinkedIn, company site, email…"
          value={platform}
          onChange={e => setPlatform(e.target.value)}
        />
      </div>

      <div className="status-form-row">
        <label className="form-label" htmlFor="sf-followup">Follow-up date</label>
        <input
          id="sf-followup"
          className="form-input"
          type="date"
          value={followUp}
          onChange={e => setFollowUp(e.target.value)}
        />
      </div>

      <div className="status-form-row">
        <label className="form-label" htmlFor="sf-notes">Notes</label>
        <textarea
          id="sf-notes"
          className="form-textarea"
          rows={3}
          placeholder="Reason, next step, anything relevant…"
          value={notes}
          onChange={e => setNotes(e.target.value)}
        />
      </div>

      {error && <div className="form-error">{error}</div>}

      <div className="status-form-footer">
        {current.last_updated && (
          <span className="muted">
            Last saved: {current.last_updated.slice(0, 16).replace('T', ' ')}
          </span>
        )}
        <button className="btn btn--primary" type="submit" disabled={!status || saving}>
          {saving ? 'Saving…' : saved ? 'Saved!' : 'Save decision'}
        </button>
      </div>
    </form>
  )
}
