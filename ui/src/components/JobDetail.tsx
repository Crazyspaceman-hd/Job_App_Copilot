import { useState } from 'react'
import type { Package, DecisionPayload } from '../lib/api'
import { api } from '../lib/api'
import { FitPanel } from './FitPanel'
import { AssetPanel } from './AssetPanel'
import { ProjectsPanel } from './ProjectsPanel'
import { StatusForm } from './StatusForm'

type Tab = 'fit' | 'resume' | 'cover_letter' | 'projects'

const TABS: { id: Tab; label: string }[] = [
  { id: 'fit',          label: 'Fit' },
  { id: 'resume',       label: 'Resume' },
  { id: 'cover_letter', label: 'Cover letter' },
  { id: 'projects',     label: 'Projects' },
]

const REMOTE_LABEL: Record<string, string> = {
  remote: 'Remote',
  hybrid: 'Hybrid',
  onsite: 'On-site',
}

const STATUS_BADGE_COLOR: Record<string, string> = {
  new:       '#555',
  reviewing: '#7a6a00',
  applied:   '#1a5a1a',
  rejected:  '#8a0000',
  offer:     '#004a8a',
  archived:  '#666',
}

interface Props {
  pkg: Package
  onRefresh: () => void
}

export function JobDetail({ pkg, onRefresh }: Props) {
  const [tab, setTab]             = useState<Tab>('fit')
  const [genErr, setGenErr]       = useState<string | null>(null)
  const [genLoading, setGenLoading] = useState<Tab | null>(null)
  const [decErr, setDecErr]       = useState<string | null>(null)
  const [decSaving, setDecSaving] = useState(false)

  async function generate(kind: 'resume' | 'cover_letter' | 'projects') {
    setGenErr(null)
    setGenLoading(kind === 'projects' ? 'projects' : kind)
    try {
      if (kind === 'resume')        await api.generateResume(pkg.job_id)
      if (kind === 'cover_letter')  await api.generateCoverLetter(pkg.job_id)
      if (kind === 'projects')      await api.recommendProject(pkg.job_id)
      onRefresh()
    } catch (e: unknown) {
      setGenErr(e instanceof Error ? e.message : String(e))
    } finally {
      setGenLoading(null)
    }
  }

  async function saveDecision(payload: DecisionPayload) {
    setDecErr(null)
    setDecSaving(true)
    try {
      await api.setDecision(pkg.job_id, payload)
      onRefresh()
    } catch (e: unknown) {
      setDecErr(e instanceof Error ? e.message : String(e))
    } finally {
      setDecSaving(false)
    }
  }

  const appStatus = pkg.application.status
  const statusColor = STATUS_BADGE_COLOR[pkg.job_status] ?? '#555'

  return (
    <div className="job-detail">
      {/* ── Job header ──────────────────────────────────────────────────── */}
      <div className="job-header">
        <div className="job-header-left">
          <h1 className="job-title">{pkg.job_title ?? '(no title)'}</h1>
          <div className="job-subtitle">
            <span>{pkg.job_company ?? '—'}</span>
            {pkg.job_location && <><span className="sep">·</span><span>{pkg.job_location}</span></>}
            {pkg.job_remote_policy && (
              <><span className="sep">·</span>
              <span>{REMOTE_LABEL[pkg.job_remote_policy] ?? pkg.job_remote_policy}</span></>
            )}
          </div>
          {pkg.job_source_url && (
            <a className="job-source-link" href={pkg.job_source_url} target="_blank" rel="noreferrer">
              Source ↗
            </a>
          )}
        </div>
        <div className="job-header-right">
          <span className="badge badge--lg" style={{ background: statusColor }}>
            {pkg.job_status}
          </span>
          {appStatus && (
            <span className={`decision-badge decision-badge--${appStatus}`}>
              {appStatus.charAt(0).toUpperCase() + appStatus.slice(1)}
            </span>
          )}
        </div>
      </div>

      {genErr && <div className="form-error" style={{ margin: '0 0 0.75rem' }}>{genErr}</div>}

      {/* ── Tabs ────────────────────────────────────────────────────────── */}
      <div className="tabs">
        {TABS.map(t => (
          <button
            key={t.id}
            className={`tab-btn ${tab === t.id ? 'tab-btn--active' : ''}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="tab-body">
        {tab === 'fit' && <FitPanel pkg={pkg} />}

        {tab === 'resume' && (
          <AssetPanel
            asset={pkg.resume}
            kind="resume"
            jobId={pkg.job_id}
            onGenerate={() => generate('resume')}
            generating={genLoading === 'resume'}
          />
        )}

        {tab === 'cover_letter' && (
          <AssetPanel
            asset={pkg.cover_letter}
            kind="cover_letter"
            jobId={pkg.job_id}
            onGenerate={() => generate('cover_letter')}
            generating={genLoading === 'cover_letter'}
          />
        )}

        {tab === 'projects' && (
          <ProjectsPanel
            recs={pkg.recommendations}
            jobId={pkg.job_id}
            onGenerate={() => generate('projects')}
            generating={genLoading === 'projects'}
          />
        )}
      </div>

      {/* ── Status sidebar ──────────────────────────────────────────────── */}
      <div className="status-sidebar">
        <div className="status-sidebar-heading">Application decision</div>
        <StatusForm
          current={pkg.application}
          onSave={saveDecision}
          saving={decSaving}
          error={decErr}
        />
      </div>
    </div>
  )
}
