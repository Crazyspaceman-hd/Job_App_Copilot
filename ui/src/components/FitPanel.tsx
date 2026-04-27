import type { Package } from '../lib/api'

interface Props { pkg: Package }

function ScoreBar({ value }: { value: number }) {
  const pct = Math.round(value * 100)
  return (
    <div className="score-bar-wrap">
      <div className="score-bar-fill" style={{ width: `${pct}%` }} />
      <span className="score-bar-label">{pct}%</span>
    </div>
  )
}

function TagList({ items, colorClass }: { items: string[]; colorClass: string }) {
  if (!items.length) return <span className="muted">(none)</span>
  return (
    <div className="tag-list">
      {items.map(t => (
        <span key={t} className={`tag tag--${colorClass}`}>{t}</span>
      ))}
    </div>
  )
}

const VERDICT_DISPLAY: Record<string, string> = {
  strong_fit:       'Strong fit',
  reach_but_viable: 'Reach but viable',
  long_shot:        'Long shot',
  skip:             'Skip',
}

export function FitPanel({ pkg }: Props) {
  if (!pkg.assessment_id) {
    return (
      <div className="panel-empty">
        No fit assessment yet.<br />
        Run: <code>python -m app.main assess-fit --job-id {pkg.job_id}</code>
      </div>
    )
  }

  const vk = (pkg.verdict ?? '').toLowerCase().replace(/\s+/g, '_')

  return (
    <div className="fit-panel">
      <div className="fit-header">
        <span className={`verdict-badge verdict--${vk}`}>
          {VERDICT_DISPLAY[vk] ?? pkg.verdict}
        </span>
        <span className="fit-meta">
          confidence: <strong>{pkg.confidence}</strong>
          {pkg.assessed_at && <> &nbsp;·&nbsp; assessed {pkg.assessed_at.slice(0, 10)}</>}
        </span>
      </div>

      {pkg.overall_score != null && (
        <div className="score-row">
          <span className="score-row-label">Overall score</span>
          <ScoreBar value={pkg.overall_score} />
        </div>
      )}

      <div className="evidence-section">
        <div className="evidence-block">
          <div className="evidence-heading evidence-heading--direct">
            Direct evidence ({pkg.direct_evidence.length})
          </div>
          <TagList items={pkg.direct_evidence} colorClass="direct" />
        </div>
        <div className="evidence-block">
          <div className="evidence-heading evidence-heading--adjacent">
            Adjacent evidence ({pkg.adjacent_evidence.length})
          </div>
          <TagList items={pkg.adjacent_evidence} colorClass="adjacent" />
        </div>
        <div className="evidence-block">
          <div className="evidence-heading evidence-heading--gap">
            Unsupported gaps ({pkg.unsupported_gaps.length})
          </div>
          <TagList items={pkg.unsupported_gaps} colorClass="gap" />
        </div>
      </div>
    </div>
  )
}
