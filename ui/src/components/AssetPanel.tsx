import type { AssetRef } from '../lib/api'

interface Props {
  asset: AssetRef | null
  kind: 'resume' | 'cover_letter'
  jobId: number
  onGenerate: () => Promise<void>
  generating: boolean
}

const GENERATE_CMD: Record<string, string> = {
  resume:       'generate-resume',
  cover_letter: 'generate-cover-letter',
}

export function AssetPanel({ asset, kind, jobId, onGenerate, generating }: Props) {
  const label = kind === 'resume' ? 'Resume' : 'Cover letter'

  return (
    <div className="asset-panel">
      {asset ? (
        <>
          <div className="asset-meta">
            <span>Asset id: <strong>{asset.asset_id}</strong></span>
            {asset.label && <span>Label: <strong>{asset.label}</strong></span>}
            <span>Generated: <strong>{asset.generated_at.slice(0, 16).replace('T', ' ')}</strong></span>
          </div>

          <button
            className="btn btn--secondary"
            onClick={onGenerate}
            disabled={generating}
          >
            {generating ? 'Generating…' : `Regenerate ${label}`}
          </button>

          <div className="asset-content">
            <pre className="asset-pre">{asset.content ?? asset.content_preview}</pre>
          </div>
        </>
      ) : (
        <div className="panel-empty">
          No {label.toLowerCase()} generated yet.
          <div style={{ marginTop: '0.75rem' }}>
            <button className="btn btn--primary" onClick={onGenerate} disabled={generating}>
              {generating ? 'Generating…' : `Generate ${label}`}
            </button>
          </div>
          <div className="panel-empty-cmd">
            Or run: <code>python -m app.main {GENERATE_CMD[kind]} --job-id {jobId}</code>
          </div>
        </div>
      )}
    </div>
  )
}
