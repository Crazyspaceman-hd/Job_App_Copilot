// src/lib/api.ts — typed wrappers around the FastAPI backend

export interface JobSummary {
  id: number
  title: string | null
  company: string | null
  location: string | null
  remote_policy: string | null
  status: string
  ingested_at: string
  source_url: string | null
  verdict: string | null
  overall_score: number | null
  confidence: string | null
}

export interface AssetRef {
  asset_id: number
  asset_type: string
  label: string | null
  generated_at: string
  content_preview: string
  content: string | null
}

export interface RecOut {
  rec_id: number
  recommendation_type: string | null
  title: string
  target_gap_or_signal: string | null
  business_problem: string | null
}

export interface ApplicationRecord {
  application_id: number | null
  status: string | null
  notes: string | null
  follow_up_date: string | null
  platform: string | null
  last_updated: string | null
}

export interface Package {
  job_id: number
  job_title: string | null
  job_company: string | null
  job_remote_policy: string | null
  job_status: string
  job_location: string | null
  job_source_url: string | null
  assessment_id: number | null
  assessed_at: string | null
  verdict: string | null
  overall_score: number | null
  confidence: string | null
  direct_evidence: string[]
  adjacent_evidence: string[]
  unsupported_gaps: string[]
  resume: AssetRef | null
  cover_letter: AssetRef | null
  recommendations: RecOut[]
  application: ApplicationRecord
}

export interface DecisionPayload {
  status: 'apply' | 'hold' | 'skip'
  notes?: string
  follow_up_date?: string
  platform?: string
}

// Profile types (mirrors candidate_profile.json structure)
export interface SkillEntry { name: string; years: number; evidence: string }
export interface DomainEntry { name: string; evidence: string }

export interface CandidateProfile {
  version: string
  _completeness?: number
  personal: { name: string; location: string; linkedin: string; github: string }
  job_targets: {
    titles: string[]
    seniority_self_assessed: string
    desired_remote_policy: string
    willing_to_relocate: boolean
    work_authorization: string
  }
  skills: {
    languages:  SkillEntry[]
    frameworks: SkillEntry[]
    databases:  SkillEntry[]
    cloud:      SkillEntry[]
    tools:      SkillEntry[]
    practices:  SkillEntry[]
  }
  domains: DomainEntry[]
  // remaining keys not used in the form but preserved on save
  [key: string]: unknown
}

export interface IngestStatus {
  has_resume:       boolean
  resume_id:        number | null
  resume_label:     string | null
  resume_bullets:   number
  resume_count:     number
  has_cover_letter: boolean
  cl_id:            number | null
  cl_label:         string | null
  cl_fragments:     number
  cl_count:         number
}

export interface CreateJobPayload {
  raw_text:      string
  company?:      string
  title?:        string
  location?:     string
  source_url?:   string
  remote_policy?: string
  platform?:     string
}

export interface CreateJobResult {
  ok:        boolean
  job_id:    number
  extracted: boolean
  assessed:  boolean
  verdict:   string | null
}

export interface CreatePackageResult {
  ok:      boolean
  job_id:  number
  verdict: string | null
  steps:   Record<string, boolean>
  errors:  Record<string, string>
  missing: string[]
}

// Evidence Bank types
export interface EvidenceItem {
  item_id:               number
  created_at:            string
  updated_at:            string
  title:                 string
  raw_text:              string
  source_type:           string
  skill_tags:            string[]
  domain_tags:           string[]
  business_problem_tags: string[]
  evidence_strength:     string
  allowed_uses:          string[]
  confidence:            string | null
  notes:                 string | null
  profile_id:            number | null
}

export interface EvidenceItemPayload {
  title:                 string
  raw_text:              string
  source_type?:          string
  skill_tags?:           string[]
  domain_tags?:          string[]
  business_problem_tags?: string[]
  evidence_strength?:    string
  allowed_uses?:         string[]
  confidence?:           string | null
  notes?:                string | null
}

export const SOURCE_TYPE_LABELS: Record<string, string> = {
  resume_bullet:          'Resume bullet',
  cover_letter_fragment:  'Cover letter fragment',
  project_note:           'Project note',
  rewrite:                'Rewrite',
  brag_note:              'Brag note',
  interview_story:        'Interview story',
  other:                  'Other',
}

export const STRENGTH_LABELS: Record<string, string> = {
  direct:   'Direct',
  adjacent: 'Adjacent',
  inferred: 'Inferred',
}

export const ALLOWED_USE_LABELS: Record<string, string> = {
  resume:                 'Resume',
  cover_letter:           'Cover letter',
  project_repositioning:  'Project repositioning',
  interview_prep:         'Interview prep',
}

// Candidate Assessment types
export interface CandidateAssessment {
  id:                   number
  created_at:           string
  updated_at:           string
  source_type:          string
  source_label:         string | null
  assessment_kind:      string
  raw_text:             string
  strengths:            string[]
  growth_areas:         string[]
  demonstrated_skills:  string[]
  demonstrated_domains: string[]
  work_style:           string | null
  role_fit:             string | null
  confidence:           string | null
  allowed_uses:         string[]
  is_preferred:         boolean
  profile_id:           number | null
  prompt_type:          string | null
  prompt_version:       string | null
  source_model:         string | null
}

export interface CandidateAssessmentPayload {
  source_type?:          string
  source_label?:         string | null
  assessment_kind?:      string
  raw_text?:             string
  strengths?:            string[]
  growth_areas?:         string[]
  demonstrated_skills?:  string[]
  demonstrated_domains?: string[]
  work_style?:           string | null
  role_fit?:             string | null
  confidence?:           string | null
  allowed_uses?:         string[]
  profile_id?:           number | null
  prompt_type?:          string | null
  prompt_version?:       string | null
  source_model?:         string | null
}

export interface AssessmentPrompt {
  prompt_type:  string
  version:      string
  title:        string
  description:  string
  full_text:    string
}

export const ASSESSMENT_SOURCE_LABELS: Record<string, string> = {
  chatgpt: 'ChatGPT',
  claude:  'Claude',
  gemini:  'Gemini',
  manual:  'Manual',
  other:   'Other',
}

export const ASSESSMENT_KIND_LABELS: Record<string, string> = {
  working_assessment:            'Working Assessment',
  skill_observation:             'Skill Observation',
  project_delivery_assessment:   'Project Delivery',
  growth_assessment:             'Growth Assessment',
}

export const ASSESSMENT_CONFIDENCE_LABELS: Record<string, string> = {
  high:   'High',
  medium: 'Medium',
  low:    'Low',
}

export const ASSESSMENT_ALLOWED_USE_LABELS: Record<string, string> = {
  resume:       'Resume',
  cover_letter: 'Cover letter',
  interview:    'Interview',
  internal:     'Internal',
}

// ── Profile Reconstruction types ───────────────────────────────────────────

export interface PRSource {
  id:          number
  created_at:  string
  updated_at:  string
  title:       string
  raw_text:    string
  source_type: string
  label:       string | null
}

export interface PRObservation {
  id:                    number
  created_at:            string
  updated_at:            string
  source_id:             number
  text:                  string
  skill_tags:            string[]
  domain_tags:           string[]
  business_problem_tags: string[]
  evidence_strength:     string
  confidence:            string
  allowed_uses:          string[]
  review_state:          string
  notes:                 string | null
}

export interface PRClaim {
  id:               number
  created_at:       string
  updated_at:       string
  observation_id:   number
  text:             string
  framing:          string
  evidence_basis:   string | null
  review_state:     string
  promoted_item_id: number | null
}

export interface PRRunResult {
  source_id:         number
  observations:      PRObservation[]
  claims:            PRClaim[]
  draft_summary:     string
  observation_count: number
  claim_count:       number
}

export const PR_SOURCE_TYPE_LABELS: Record<string, string> = {
  project_note:    'Project note',
  debugging_story: 'Debugging story',
  old_resume:      'Old resume',
  cover_letter:    'Cover letter',
  assignment:      'Assignment',
  ai_summary:      'AI summary',
  free_text:       'Free text',
  other:           'Other',
}

const BASE = '/api'

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`${BASE}${path}`)
  if (!r.ok) {
    const text = await r.text()
    throw new Error(`GET ${path} failed ${r.status}: ${text}`)
  }
  return r.json() as Promise<T>
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) {
    const text = await r.text()
    throw new Error(`POST ${path} failed ${r.status}: ${text}`)
  }
  return r.json() as Promise<T>
}

async function put<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) {
    const text = await r.text()
    throw new Error(`PUT ${path} failed ${r.status}: ${text}`)
  }
  return r.json() as Promise<T>
}

async function del<T>(path: string): Promise<T> {
  const r = await fetch(`${BASE}${path}`, { method: 'DELETE' })
  if (!r.ok) {
    const text = await r.text()
    throw new Error(`DELETE ${path} failed ${r.status}: ${text}`)
  }
  return r.json() as Promise<T>
}

async function patch_<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) {
    const text = await r.text()
    throw new Error(`PATCH ${path} failed ${r.status}: ${text}`)
  }
  return r.json() as Promise<T>
}

export const api = {
  // ── Jobs ──────────────────────────────────────────────────────────────────
  listJobs: () => get<JobSummary[]>('/jobs'),

  getPackage: (jobId: number) => get<Package>(`/jobs/${jobId}/package`),

  createJob: (payload: CreateJobPayload) =>
    post<CreateJobResult>('/jobs', payload),

  createJobPackage: (payload: CreateJobPayload) =>
    post<CreatePackageResult>('/jobs/create-package', payload),

  setDecision: (jobId: number, payload: DecisionPayload) =>
    post<{ ok: boolean; application_id: number }>(`/jobs/${jobId}/decision`, payload),

  generateResume: (jobId: number, label = 'targeted') =>
    post<{ ok: boolean; asset_id: number }>(`/jobs/${jobId}/generate-resume`, { label }),

  generateCoverLetter: (jobId: number, label = 'targeted') =>
    post<{ ok: boolean; asset_id: number }>(`/jobs/${jobId}/generate-cover-letter`, { label }),

  recommendProject: (jobId: number, label = 'targeted') =>
    post<{ ok: boolean; new_project_id: number; reposition_id: number | null }>(
      `/jobs/${jobId}/recommend-project`, { label }
    ),

  // ── Profile ────────────────────────────────────────────────────────────────
  getProfile: () => get<CandidateProfile>('/profile'),

  saveProfile: (profile: Omit<CandidateProfile, '_completeness'>) =>
    post<{ ok: boolean; completeness: number }>('/profile', profile),

  // ── Ingestion ──────────────────────────────────────────────────────────────
  getIngestStatus: () => get<IngestStatus>('/ingest/status'),

  ingestResume: (text: string, label = 'default') =>
    post<{ ok: boolean; resume_id: number; label: string; bullet_count: number; section_count: number }>(
      '/ingest/resume', { text, label }
    ),

  ingestCoverLetter: (text: string, label = 'default') =>
    post<{ ok: boolean; cl_id: number; label: string; fragment_count: number }>(
      '/ingest/cover-letter', { text, label }
    ),

  // ── Evidence Bank ────────────────────────────────────────────────────────────
  listEvidence: (filters?: { source_type?: string; evidence_strength?: string }) => {
    const params = new URLSearchParams()
    if (filters?.source_type)       params.set('source_type', filters.source_type)
    if (filters?.evidence_strength) params.set('evidence_strength', filters.evidence_strength)
    const qs = params.toString()
    return get<EvidenceItem[]>(`/evidence${qs ? '?' + qs : ''}`)
  },

  createEvidence: (payload: EvidenceItemPayload) =>
    post<EvidenceItem>('/evidence', payload),

  updateEvidence: (id: number, payload: EvidenceItemPayload) =>
    put<EvidenceItem>(`/evidence/${id}`, payload),

  deleteEvidence: (id: number) =>
    del<{ ok: boolean; item_id: number }>(`/evidence/${id}`),

  // ── Candidate Assessments ──────────────────────────────────────────────────
  listAssessments: (filters?: { source_type?: string; assessment_kind?: string }) => {
    const params = new URLSearchParams()
    if (filters?.source_type)      params.set('source_type', filters.source_type)
    if (filters?.assessment_kind)  params.set('assessment_kind', filters.assessment_kind)
    const qs = params.toString()
    return get<CandidateAssessment[]>(`/assessments${qs ? '?' + qs : ''}`)
  },

  getPreferredAssessment: () =>
    get<CandidateAssessment | null>('/assessments/preferred'),

  createAssessment: (payload: CandidateAssessmentPayload) =>
    post<CandidateAssessment>('/assessments', payload),

  updateAssessment: (id: number, payload: CandidateAssessmentPayload) =>
    put<CandidateAssessment>(`/assessments/${id}`, payload),

  deleteAssessment: (id: number) =>
    del<{ ok: boolean; id: number }>(`/assessments/${id}`),

  setPreferredAssessment: (id: number) =>
    post<CandidateAssessment>(`/assessments/${id}/set-preferred`, {}),

  // ── Assessment Prompts ─────────────────────────────────────────────────────
  listAssessmentPrompts: () =>
    get<AssessmentPrompt[]>('/assessment-prompts'),

  getAssessmentPrompt: (promptType: string) =>
    get<AssessmentPrompt>(`/assessment-prompts/${promptType}`),

  // ── Profile Reconstruction ─────────────────────────────────────────────────
  createPRSource: (payload: { raw_text: string; source_type: string; title?: string; label?: string | null }) =>
    post<PRSource>('/reconstruction/sources', payload),

  listPRSources: () =>
    get<PRSource[]>('/reconstruction/sources'),

  deletePRSource: (id: number) =>
    del<{ ok: boolean; source_id: number }>(`/reconstruction/sources/${id}`),

  runPRReconstruction: (sourceId: number) =>
    post<PRRunResult>(`/reconstruction/sources/${sourceId}/run`, {}),

  listPRObservations: (sourceId: number) =>
    get<PRObservation[]>(`/reconstruction/sources/${sourceId}/observations`),

  updatePRObservation: (obsId: number, patch: Partial<PRObservation>) =>
    patch_<PRObservation>(`/reconstruction/observations/${obsId}`, patch),

  listPRClaims: (sourceId: number) =>
    get<PRClaim[]>(`/reconstruction/sources/${sourceId}/claims`),

  updatePRClaim: (claimId: number, patch: Partial<PRClaim>) =>
    patch_<PRClaim>(`/reconstruction/claims/${claimId}`, patch),

  promotePRClaim: (claimId: number) =>
    post<{ ok: boolean; claim_id: number; evidence_item_id: number; title: string }>(
      `/reconstruction/claims/${claimId}/promote`, {}
    ),

  getPRSummary: (sourceId: number) =>
    get<{ source_id: number; summary: string }>(`/reconstruction/sources/${sourceId}/summary`),
}
