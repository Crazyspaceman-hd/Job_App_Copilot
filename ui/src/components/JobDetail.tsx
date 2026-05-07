import { useState } from 'react'
import type { Package, DecisionPayload, CreatePackageResult } from '../lib/api'
import { api } from '../lib/api'
import { FitPanel } from './FitPanel'
import { AssetPanel } from './AssetPanel'
import { ProjectsPanel } from './ProjectsPanel'
import { StatusForm } from './StatusForm'

type Tab = 'fit' | 'resume' | 'cover_letter' | 'projects'

const STEP_LABELS: Record<string, string> = {
  extract:      'Requirements extracted',
  assess:       'Fit assessed',
  resume:       'Resume generated',
  cover_letter: 'Cover letter generated',
  project:      'Project recommendations',
}

const MISSING_LABELS: Record<string, string> = {
  profile:           'candidate profile',
  base_resume:       'ingested resume',
  base_cover_letter: 'ingested cover letter',
  projects:          'projects file',
}

function RerunResults({ result, onDismiss }: { result: CreatePackageResult; onDismiss: () => void }) {
  const totalOk = Object.values(result.steps).filter(Boolean).length
  return (
    <div className="rerun-results">
      <div className="rerun-results-header">
        <span className="rerun-results-summary">
          Re-run complete — {totalOk}/{Object.keys(result.steps).length} steps succeeded.
          {result.verdict && <> Verdict: <strong>{result.verdict}</strong>.</>}
        </span>
        <button className="btn btn--ghost btn--sm" onClick={onDismiss}>Dismiss</button>
      </div>
      <div className="pkg-steps">
        {Object.entries(STEP_LABELS).map(([key, label]) => {
          const ok  = result.steps[key]
          const err = result.errors[key]
          const icon = ok ? '✓' : (err ? '✗' : '—')
          const cls  = ok ? 'pkg-step--ok' : (err ? 'pkg-step--err' : 'pkg-step--skip')
          return (
            <div key={key} className={`pkg-step ${cls}`}>
              <span className="pkg-step-icon">{icon}</span>
              <span className="pkg-step-label">{label}</span>
              {err && <span className="pkg-step-msg">{err}</span>}
            </div>
          )
        })}
      </div>
      {result.missing.length > 0 && (
        <p className="pkg-missing">
          Missing: {result.missing.map(k => MISSING_LABELS[k] ?? k).join(', ')}.
        </p>
      )}
    </div>
  )
}

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
  const [rerunning, setRerunning]   = useState(false)
  const [rerunResult, setRerunResult] = useState<CreatePackageResult | null>(null)
  const [rerunErr, setRerunErr]     = useState<string | null>(null)

  async function rerun() {
    setRerunning(true)
    setRerunErr(null)
    setRerunResult(null)
    try {
      const result = await api.rerunPackage(pkg.job_id)
      setRerunResult(result)
      onRefresh()
    } catch (e: unknown) {
      setRerunErr(e instanceof Error ? e.message : String(e))
    } finally {
      setRerunning(false)
    }
  }

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
          <button
            className="btn btn--ghost btn--sm"
            onClick={rerun}
            disabled={rerunning}
            title="Re-run extraction, fit assessment, and document generation"
          >
            {rerunning ? 'Re-running…' : '↺ Re-run'}
          </button>
        </div>
      </div>

      {rerunErr && <div className="form-error" style={{ margin: '0 0 0.75rem' }}>{rerunErr}</div>}
      {rerunResult && (
        <RerunResults result={rerunResult} onDismiss={() => setRerunResult(null)} />
      )}
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
