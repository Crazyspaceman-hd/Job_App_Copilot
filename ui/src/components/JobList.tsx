import type { JobSummary } from '../lib/api'

interface Props {
  jobs: JobSummary[]
  selectedId: number | null
  onSelect: (id: number) => void
  loading: boolean
  error: string | null
  onNewJob: () => void
  onProfile: () => void
}

const VERDICT_LABEL: Record<string, string> = {
  strong_fit:       'Strong fit',
  reach_but_viable: 'Reach',
  long_shot:        'Long shot',
  skip:             'Skip',
}

const VERDICT_COLOR: Record<string, string> = {
  strong_fit:       '#2a7a2a',
  reach_but_viable: '#7a6a00',
  long_shot:        '#b05000',
  skip:             '#8a0000',
}

const STATUS_BADGE: Record<string, string> = {
  new:       '#555',
  reviewing: '#5a5a00',
  applied:   '#1a5a1a',
  rejected:  '#8a0000',
  offer:     '#004a8a',
  archived:  '#666',
}

function verdictKey(raw: string | null): string {
  if (!raw) return ''
  return raw.toLowerCase().replace(/\s+/g, '_')
}

export function JobList({ jobs, selectedId, onSelect, loading, error, onNewJob, onProfile }: Props) {
  if (loading) return <aside className="job-list"><p className="muted">Loading jobs…</p></aside>
  if (error)   return <aside className="job-list"><p className="err">{error}</p></aside>
  if (!jobs.length)
    return (
      <aside className="job-list">
        <div className="job-list-empty">
          <p>No jobs yet.</p>
          <button className="btn btn--primary" style={{ marginTop: '0.6rem', width: '100%' }} onClick={onNewJob}>
            + Add your first job
          </button>
          <button className="btn btn--secondary" style={{ marginTop: '0.4rem', width: '100%' }} onClick={onProfile}>
            Set up profile first
          </button>
        </div>
      </aside>
    )

  return (
    <aside className="job-list">
      <div className="job-list-header">Jobs ({jobs.length})</div>
      <ul className="job-list-ul">
        {jobs.map(j => {
          const vk = verdictKey(j.verdict)
          const isSelected = j.id === selectedId
          return (
            <li
              key={j.id}
              className={`job-item ${isSelected ? 'job-item--selected' : ''}`}
              onClick={() => onSelect(j.id)}
            >
              <div className="job-item-title">{j.title || '(no title)'}</div>
              <div className="job-item-company">{j.company || '—'}</div>
              <div className="job-item-meta">
                <span
                  className="badge"
                  style={{ background: STATUS_BADGE[j.status] ?? '#555' }}
                >
                  {j.status}
                </span>
                {vk && (
                  <span style={{ color: VERDICT_COLOR[vk] ?? '#555', fontSize: '0.75rem' }}>
                    {VERDICT_LABEL[vk] ?? j.verdict}
                    {j.overall_score != null && ` ${Math.round(j.overall_score * 100)}%`}
                  </span>
                )}
              </div>
            </li>
          )
        })}
      </ul>
    </aside>
  )
}
