import type { RecOut } from '../lib/api'

interface Props {
  recs: RecOut[]
  jobId: number
  onGenerate: () => Promise<void>
  generating: boolean
}

const TYPE_LABEL: Record<string, string> = {
  new_project:          'New project',
  reposition_existing:  'Reposition existing',
}

export function ProjectsPanel({ recs, jobId, onGenerate, generating }: Props) {
  if (!recs.length) {
    return (
      <div className="panel-empty">
        No project recommendations yet.
        <div style={{ marginTop: '0.75rem' }}>
          <button className="btn btn--primary" onClick={onGenerate} disabled={generating}>
            {generating ? 'Generating…' : 'Recommend projects'}
          </button>
        </div>
        <div className="panel-empty-cmd">
          Or run: <code>python -m app.main recommend-project --job-id {jobId}</code>
        </div>
      </div>
    )
  }

  return (
    <div className="projects-panel">
      <div className="projects-actions">
        <button className="btn btn--secondary" onClick={onGenerate} disabled={generating}>
          {generating ? 'Generating…' : 'Refresh recommendations'}
        </button>
      </div>

      {recs.map(rec => (
        <div key={rec.rec_id} className="rec-card">
          <div className="rec-card-header">
            <span className="rec-type-badge">
              {TYPE_LABEL[rec.recommendation_type ?? ''] ?? rec.recommendation_type ?? 'recommendation'}
            </span>
            <span className="rec-title">{rec.title}</span>
          </div>
          {rec.target_gap_or_signal && (
            <div className="rec-target">
              Targets gap: <strong>{rec.target_gap_or_signal}</strong>
            </div>
          )}
          {rec.business_problem && (
            <div className="rec-problem">{rec.business_problem}</div>
          )}
        </div>
      ))}
    </div>
  )
}
