import { useCallback, useEffect, useState } from 'react'
import type { CreateJobResult, JobSummary, Package } from './lib/api'
import { api } from './lib/api'
import { JobList } from './components/JobList'
import { JobDetail } from './components/JobDetail'
import { NewJobForm } from './components/NewJobForm'
import { ProfileSetup } from './components/ProfileSetup'
import { EvidenceBank } from './components/EvidenceBank'
import './styles.css'

type View = 'jobs' | 'new-job' | 'profile' | 'evidence'

export default function App() {
  const [view, setView] = useState<View>('jobs')

  const [jobs, setJobs]               = useState<JobSummary[]>([])
  const [jobsErr, setJobsErr]         = useState<string | null>(null)
  const [jobsLoading, setJobsLoading] = useState(true)

  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [pkg, setPkg]               = useState<Package | null>(null)
  const [pkgErr, setPkgErr]         = useState<string | null>(null)
  const [pkgLoading, setPkgLoading] = useState(false)

  const loadJobs = useCallback(async () => {
    setJobsLoading(true)
    setJobsErr(null)
    try {
      const data = await api.listJobs()
      setJobs(data)
      if (!selectedId && data.length > 0) setSelectedId(data[0].id)
    } catch (e: unknown) {
      setJobsErr(e instanceof Error ? e.message : String(e))
    } finally {
      setJobsLoading(false)
    }
  }, [selectedId])

  const loadPackage = useCallback(async (id: number) => {
    setPkgLoading(true)
    setPkgErr(null)
    try {
      const data = await api.getPackage(id)
      setPkg(data)
    } catch (e: unknown) {
      setPkgErr(e instanceof Error ? e.message : String(e))
      setPkg(null)
    } finally {
      setPkgLoading(false)
    }
  }, [])

  useEffect(() => { loadJobs() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (selectedId != null) loadPackage(selectedId)
  }, [selectedId, loadPackage])

  function handleSelect(id: number) {
    setSelectedId(id)
    setView('jobs')
  }

  function handleRefresh() {
    if (selectedId != null) {
      loadPackage(selectedId)
      loadJobs()
    }
  }

  function handleJobCreated(result: CreateJobResult) {
    loadJobs().then(() => {
      setSelectedId(result.job_id)
      setView('jobs')
    })
  }

  return (
    <div className="app-layout">
      <header className="app-header">
        <span className="app-header-title">Job Application Copilot</span>
        <nav className="app-nav">
          <button
            className={`app-nav-btn ${view === 'jobs' ? 'app-nav-btn--active' : ''}`}
            onClick={() => setView('jobs')}
          >
            Jobs {jobs.length > 0 && `(${jobs.length})`}
          </button>
          <button
            className={`app-nav-btn app-nav-btn--new ${view === 'new-job' ? 'app-nav-btn--active' : ''}`}
            onClick={() => setView('new-job')}
          >
            + New Job
          </button>
          <button
            className={`app-nav-btn ${view === 'profile' ? 'app-nav-btn--active' : ''}`}
            onClick={() => setView('profile')}
          >
            Profile Setup
          </button>
          <button
            className={`app-nav-btn ${view === 'evidence' ? 'app-nav-btn--active' : ''}`}
            onClick={() => setView('evidence')}
          >
            Evidence Bank
          </button>
        </nav>
        {view === 'jobs' && selectedId != null && (
          <button className="btn btn--ghost" onClick={handleRefresh} title="Refresh">
            Refresh
          </button>
        )}
      </header>

      {view === 'new-job' && (
        <main className="main-area main-area--full">
          <NewJobForm onCreated={handleJobCreated} />
        </main>
      )}

      {view === 'profile' && (
        <main className="main-area main-area--full">
          <ProfileSetup />
        </main>
      )}

      {view === 'evidence' && (
        <main className="main-area main-area--full">
          <EvidenceBank />
        </main>
      )}

      {view === 'jobs' && (
        <div className="app-body">
          <JobList
            jobs={jobs}
            selectedId={selectedId}
            onSelect={handleSelect}
            loading={jobsLoading}
            error={jobsErr}
            onNewJob={() => setView('new-job')}
            onProfile={() => setView('profile')}
          />

          <main className="main-area">
            {pkgLoading && <div className="main-loading">Loading…</div>}
            {pkgErr    && <div className="main-error">{pkgErr}</div>}
            {!pkgLoading && !pkgErr && pkg && (
              <JobDetail pkg={pkg} onRefresh={handleRefresh} />
            )}
            {!pkgLoading && !pkgErr && !pkg && !jobsLoading && (
              <div className="main-empty">
                {jobs.length === 0
                  ? 'No jobs yet. Click "+ New Job" to get started.'
                  : 'Select a job from the left panel.'}
              </div>
            )}
          </main>
        </div>
      )}
    </div>
  )
}
