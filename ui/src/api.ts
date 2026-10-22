import { z } from 'zod';
import type { Intent } from '@blueprintjs/core';
import { newRequestId, report, requestHeader } from './report';
import type { RequestId } from './report';

const scanId = z.number().int().positive().brand<'ScanId'>();
const feedId = z.number().int().positive().brand<'FeedId'>();
const count = z.number().int().nonnegative();
const nullableText = z.string().nullable();

const rulesSchema = z.object({
  any_terms: z.array(z.string()),
  all_terms: z.array(z.string()),
  exclude_terms: z.array(z.string()),
});

const feedSchema = z.object({
  id: feedId,
  scan_id: scanId,
  url: z.string(),
  last_checked: nullableText,
  error: nullableText,
  item_count: count,
  next_check_at: nullableText,
  failures: count,
});

const setupRoundSchema = z.object({
  id: z.number().int(),
  run_id: z.string(),
  round: count,
  status: z.enum(['running', 'changed', 'stable', 'needs_attention', 'failed', 'stale', 'cancelled']),
  started_at: z.string(),
  finished_at: nullableText,
  reviewed_count: count,
  total_count: count,
  calls_used: count,
  rationale: z.string(),
  error: nullableText,
  actions: z.array(z.unknown()),
  probes: z.array(z.object({ url: z.string(), query: nullableText, item_count: count, error: nullableText })),
});

const setupReviewSchema = z.object({
  run_id: z.string(),
  phase: z.enum(['collecting', 'reviewing', 'ready', 'needs_attention', 'stopped']),
  round: count,
  max_rounds: count,
  calls_used: count,
  call_limit: count,
  lifetime_calls_used: count,
  next_review_at: nullableText,
  reviewed_count: count,
  total_count: count,
  archive_through_id: z.number().int().nullable(),
  reason: z.string(),
  history: z.array(setupRoundSchema),
});

const scanBase = z.object({
  id: scanId,
  topic: z.string(),
  objective: z.string(),
  rules: rulesSchema,
  status: z.enum(['active', 'paused']),
  revision: z.number().int(),
  last_polled: nullableText,
  created_at: z.string(),
  feeds: z.array(feedSchema),
  kept: count,
  discarded: count,
  completed_at: nullableText,
  last_error: nullableText,
  setup_review: setupReviewSchema.nullable(),
});

const scanSchema = z.discriminatedUnion('mode', [
  scanBase.extend({ mode: z.literal('live'), start_date: z.null(), end_date: z.null() }),
  scanBase.extend({ mode: z.literal('historical'), start_date: z.string(), end_date: z.string() }),
]);

const recordSchema = z.object({
  id: z.number().int(),
  stable_key: z.string(),
  title: z.string(),
  url: z.string(),
  source: z.string(),
  published_at: z.string(),
  summary: z.string(),
  feed_url: z.string(),
  fetched_at: z.string(),
  scan_id: scanId,
  scan_topic: z.string(),
  kept: z.boolean(),
  reasons: z.array(z.string()),
  matched_terms: z.array(z.string()),
  rule_revision: z.number().int(),
  decided_at: z.string(),
});

const activitySchema = z.object({
  id: z.number().int(),
  at: z.string(),
  type: z.string(),
  scan_id: scanId.nullable(),
  message: z.string(),
  details: z.unknown(),
});

const decisionPair = z.object({ scan_id: scanId, article_id: z.number().int() });

/** Deterministic gap and rule-consistency check computed before the model review. Lists are complete, never sampled. */
const decisionReportSchema = z.object({
  gap_definition: z.string(),
  missing_count: count,
  inconsistent_count: count,
  missing_decisions: z.array(decisionPair),
  inconsistent_decisions: z.array(decisionPair.extend({
    expected: z.object({ kept: z.boolean(), reasons: z.array(z.string()), matched_terms: z.array(z.string()) }),
  })),
});

const auditBase = z.object({
  id: z.number().int(),
  started_at: z.string(),
  finished_at: nullableText,
  status: z.string(),
  coverage_count: count,
  decision_count: count,
  fresh_count: count,
  rationale: z.string(),
  actions: z.array(z.unknown()),
  error: nullableText,
});

/** Coverage audits, including rows migrated from the single audit, store an empty report. */
const auditSchema = z.discriminatedUnion('kind', [
  auditBase.extend({ kind: z.literal('coverage'), report: z.object({}) }),
  auditBase.extend({ kind: z.literal('decision'), report: decisionReportSchema }),
]);

const settingsSchema = z.object({
  audit_enabled: z.boolean(),
  audit_interval_minutes: z.number().int(),
  audit_objective: z.string(),
  decision_audit_enabled: z.boolean(),
  decision_audit_interval_minutes: z.number().int(),
});

const jobSchema = z.object({
  id: z.string(),
  kind: z.enum(['poll', 'replay', 'setup', 'setup_review', 'audit', 'decision_audit']),
  scan_id: scanId.nullable(),
  status: z.enum(['queued', 'running', 'completed', 'failed', 'cancelled']),
  message: z.string(),
  created_at: z.string(),
  finished_at: nullableText,
});

export const stateSchema = z.object({
  scans: z.array(scanSchema),
  records: z.array(recordSchema),
  activity: z.array(activitySchema),
  audits: z.array(auditSchema),
  settings: settingsSchema,
  jobs: z.array(jobSchema),
  collection_interval_seconds: z.number().int().positive(),
  codex_available: z.boolean(),
  article_count: count,
  stats: z.object({ active: count, kept: count, discarded: count, feeds: count }),
});

export const discoverySchema = z.object({
  feeds: z.array(z.object({ name: z.string(), url: z.string() })),
  headlines: z.array(z.object({ title: z.string(), url: z.string(), source: z.string() })),
  error: nullableText,
});

export const createdSchema = z.object({ scan_id: scanId });

export type ScanId = z.infer<typeof scanId>;
export type FeedId = z.infer<typeof feedId>;
export type Rules = z.infer<typeof rulesSchema>;
export type Feed = z.infer<typeof feedSchema>;
export type Scan = z.infer<typeof scanSchema>;
export type ArticleRecord = z.infer<typeof recordSchema>;
export type Job = z.infer<typeof jobSchema>;
export type Audit = z.infer<typeof auditSchema>;
export type AuditKind = Audit['kind'];
export type SetupReview = z.infer<typeof setupReviewSchema>;
export type SetupRound = z.infer<typeof setupRoundSchema>;
export type DecisionReport = z.infer<typeof decisionReportSchema>;
export type Settings = z.infer<typeof settingsSchema>;
export type Discovery = z.infer<typeof discoverySchema>;
export type State = z.infer<typeof stateSchema>;
export type Mode = Scan['mode'];

/** Window sent with plan, discovery, and manual creation. Dates are inclusive UTC days. */
export type ScanWindow =
  | { mode: 'live' }
  | { mode: 'historical'; start_date: string; end_date: string };

export type Mutation =
  | { path: '/api/scans'; body: { topic: string; objective: string; rules: Rules; feed_urls: string[] } & WindowFields }
  | { path: '/api/plan'; body: { topic: string; objective: string; feed_urls: string[] } & WindowFields }
  | { path: '/api/scans/delete'; body: { scan_id: ScanId } }
  | { path: '/api/scans/update'; body: ScanUpdate }
  | { path: '/api/poll' | '/api/replay'; body: { scan_id: ScanId } }
  | { path: '/api/scans/review'; body: { scan_id: ScanId; action: ReviewAction } }
  | { path: '/api/settings'; body: Settings }
  | { path: '/api/audit' | '/api/decision-audit'; body: Record<string, never> };

type ReviewAction = 'start' | 'stop';

type WindowFields = { mode: Mode; start_date: string | null; end_date: string | null };

type ScanUpdate =
  | { scan_id: ScanId; rules: Rules; objective: string; expected_revision: number }
  | { scan_id: ScanId; status: Scan['status'] }
  | { scan_id: ScanId; feed_url: string }
  | { scan_id: ScanId; remove_feed_id: FeedId };

export type Mutate = (mutation: Mutation, notice: string) => Promise<unknown>;

export function windowFields(window: ScanWindow): WindowFields {
  return window.mode === 'live'
    ? { mode: 'live', start_date: null, end_date: null }
    : window;
}

const errorBody = z.object({ error: z.string() });

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text();
  let body: unknown = null;
  try {
    body = JSON.parse(text);
  } catch {
    if (response.ok) throw new Error('The server returned a response that is not JSON.');
  }
  if (!response.ok) {
    const parsed = errorBody.safeParse(body);
    throw new Error(parsed.success ? parsed.data.error : `Request failed with HTTP ${response.status}.`);
  }
  return body;
}

async function request(path: string, init: RequestInit): Promise<{ body: unknown; requestId: RequestId }> {
  const requestId = newRequestId();
  const correlation = { request_id: requestId, path, issue_path: null, stack: null };
  const headers = new Headers(init.headers);
  headers.set(requestHeader, requestId);
  let response: Response;
  try {
    response = await fetch(path, { ...init, headers });
  } catch (error) {
    report({ ...correlation, kind: 'api', message: `${errorMessage(error)} ${String(error)}`, status: null }, 'console');
    throw error;
  }
  if (response.headers.get(requestHeader) !== requestId) {
    report({ ...correlation, kind: 'api', message: 'The response did not echo its request ID.', status: response.status });
  }
  try {
    return { body: await readJson(response), requestId };
  } catch (error) {
    report({ ...correlation, kind: 'api', message: errorMessage(error), status: response.status });
    throw error;
  }
}

/** Throws the original ZodError so errorMessage keeps naming the mismatched field. */
export function parseResponse<S extends z.ZodType>(schema: S, body: unknown, from: { path: string; requestId: RequestId | null }): z.output<S> {
  const parsed = schema.safeParse(body);
  if (parsed.success) return parsed.data;
  const [issue] = parsed.error.issues;
  report({
    kind: 'schema',
    message: parsed.error.issues.slice(0, 5).map(each => `${issuePath(each.path) || 'response'}: ${each.message}`).join('; '),
    request_id: from.requestId,
    path: from.path,
    status: null,
    issue_path: issue ? issuePath(issue.path) : null,
    stack: null,
  });
  throw parsed.error;
}

function issuePath(path: PropertyKey[]): string {
  return path.map(String).join('.');
}

export async function get<S extends z.ZodType>(path: string, schema: S): Promise<z.output<S>> {
  const { body, requestId } = await request(path, { cache: 'no-store' });
  return parseResponse(schema, body, { path, requestId });
}

export async function post({ path, body }: Mutation): Promise<unknown> {
  return (await request(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Wirewatch': '1' },
    body: JSON.stringify(body),
  })).body;
}

export function errorMessage(error: unknown): string {
  if (error instanceof z.ZodError) {
    const issue = error.issues[0];
    const where = issue ? issue.path.join('.') : 'response';
    return `The server response did not match this UI (${where}). Check that the backend and UI versions match.`;
  }
  if (error instanceof TypeError) return 'Could not reach the Wirewatch server.';
  return error instanceof Error ? error.message : 'Request failed.';
}

export function isPending(job: Job): boolean {
  return job.status === 'queued' || job.status === 'running';
}

const jobUse: Record<Job['kind'], { model: boolean; blocksScanRun: boolean }> = {
  poll: { model: false, blocksScanRun: true },
  replay: { model: false, blocksScanRun: true },
  setup: { model: true, blocksScanRun: true },
  setup_review: { model: true, blocksScanRun: false },
  audit: { model: true, blocksScanRun: true },
  decision_audit: { model: true, blocksScanRun: true },
};

export function modelBusy(jobs: Job[]): boolean {
  return jobs.some(job => jobUse[job.kind].model && isPending(job));
}

export function scanWorking(scan: Scan, jobs: Job[]): boolean {
  return jobs.some(job => job.scan_id === scan.id && jobUse[job.kind].blocksScanRun && isPending(job));
}

export type ScanStatus = 'working' | 'failed' | 'complete' | 'ready' | 'listening' | 'paused';

export const scanStatusView: Record<ScanStatus, { label: string; intent: Intent }> = {
  working: { label: 'Working', intent: 'primary' },
  failed: { label: 'Failed', intent: 'danger' },
  complete: { label: 'Complete', intent: 'none' },
  ready: { label: 'Not run', intent: 'none' },
  listening: { label: 'Listening', intent: 'success' },
  paused: { label: 'Paused', intent: 'warning' },
};

/** A running job outranks a stored error or completion so reruns show progress. */
export function scanStatus(scan: Scan, jobs: Job[]): ScanStatus {
  if (scanWorking(scan, jobs)) return 'working';
  if (scan.last_error) return 'failed';
  if (scan.mode === 'historical') return scan.completed_at ? 'complete' : 'ready';
  return scan.status === 'active' ? 'listening' : 'paused';
}

export function formatDate(value: string | null): string {
  if (!value) return 'Never';
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? value : parsed.toLocaleString();
}

export function formatTime(value: string): string {
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? value : parsed.toLocaleTimeString();
}

export function safeHref(value: string): string | undefined {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' || url.protocol === 'http:' ? url.href : undefined;
  } catch {
    return undefined;
  }
}

export function splitTerms(text: string): string[] {
  return text.split(',').map(term => term.trim()).filter(Boolean);
}

export function validMinutes(value: number): boolean {
  return Number.isInteger(value) && value >= 1 && value <= 10080;
}

export const historicalCoverage = 'Coverage may be incomplete. The dated news search and saved articles are filtered to the window, and custom feeds often list only recent items.';

export const reviewPhaseView: Record<SetupReview['phase'], { label: string; intent: Intent; active: boolean }> = {
  collecting: { label: 'Collecting', intent: 'primary', active: true },
  reviewing: { label: 'Reviewing', intent: 'primary', active: true },
  ready: { label: 'Ready', intent: 'success', active: false },
  needs_attention: { label: 'Needs attention', intent: 'warning', active: false },
  stopped: { label: 'Stopped', intent: 'none', active: false },
};

export const roundStatusView: Record<SetupRound['status'], { label: string; intent: Intent }> = {
  running: { label: 'Running', intent: 'primary' },
  changed: { label: 'Changed setup', intent: 'none' },
  stable: { label: 'Stable', intent: 'success' },
  needs_attention: { label: 'Needs attention', intent: 'warning' },
  failed: { label: 'Failed', intent: 'danger' },
  stale: { label: 'Stale', intent: 'none' },
  cancelled: { label: 'Cancelled', intent: 'none' },
};

type ReviewControl = { action: ReviewAction; label: string; icon: 'play' | 'stop' | 'repeat'; notice: string };

export function reviewControl(review: SetupReview | null): ReviewControl {
  if (!review) {
    return { action: 'start', label: 'Start setup review', icon: 'play', notice: 'Setup review started. It collects first, then Codex reviews.' };
  }
  return reviewPhaseView[review.phase].active
    ? { action: 'stop', label: 'Stop setup review', icon: 'stop', notice: 'Setup review stopped. Collection continues.' }
    : { action: 'start', label: 'Restart setup review', icon: 'repeat', notice: 'Setup review restarted with a new model run budget.' };
}

export function modelRuns(used: number, limit?: number): string {
  const runs = `${used}${limit === undefined ? '' : ` of ${limit}`} model run`;
  return (limit ?? used) === 1 ? runs : `${runs}s`;
}

export const readyCaveat = 'Coverage incomplete. News feeds and searches are capped, and Ready is not proof that every relevant article was found.';
