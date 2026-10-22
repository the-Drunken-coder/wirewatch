import { z } from 'zod';

export const requestHeader = 'X-Wirewatch-Request';

const requestIdSchema = z.string().regex(/^[0-9a-f]{16}$/).brand<'RequestId'>();
export type RequestId = z.infer<typeof requestIdSchema>;

export function newRequestId(): RequestId {
  const bytes = crypto.getRandomValues(new Uint8Array(8));
  return requestIdSchema.parse(Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join(''));
}

/** Fields of POST /api/client-errors except count, which the reporter owns. */
export type ClientError = {
  kind: 'api' | 'schema' | 'runtime' | 'rejection' | 'render';
  message: string;
  request_id: RequestId | null;
  path: string | null;
  status: number | null;
  issue_path: string | null;
  stack: string | null;
};

type Report = ClientError & { count: number };

const endpoint = '/api/client-errors';
const windowMs = 60_000;
const maxFingerprints = 64;
const maxSends = 10;
const maxCount = 1000;
const maxBytes = 8192;

const secrets: [RegExp, string][] = [
  [/\b(bearer|basic)\s+[\w.~+/=-]+/gi, '$1 [redacted]'],
  [/(\w+:\/\/)[^\s/@]+@/g, '$1[redacted]@'],
  [/(\w+:\/\/[^\s?#'"()]*)[?#][^\s'"()]*/g, '$1?[redacted]'],
  [/\b([\w-]*(?:token|secret|password|passwd|api[_-]?key|authorization|cookie|session(?:[_-]?id)?))(["']?\s*[:=]\s*["']?)[^\s"'&,;]+/gi, '$1$2[redacted]'],
];

function redact(text: string): string {
  return secrets.reduce((value, [pattern, replacement]) => value.replace(pattern, replacement), text);
}

function clip(text: string, max: number): string {
  if (text.length <= max) return text;
  const kept = text.slice(0, Math.max(0, max - 1));
  return `${/[\uD800-\uDBFF]$/.test(kept) ? kept.slice(0, -1) : kept}…`;
}

function bounded(text: string | null, max: number): string | null {
  return text === null ? null : clip(redact(text), max);
}

function route(path: string): string {
  return clip(path.split(/[?#]/, 1)[0] ?? '', 256);
}

const seen = new Map<string, { lastAdmittedAtMs: number; pendingReportCount: number }>();
const sent: number[] = [];

function sendAllowed(now: number): boolean {
  while (sent[0] !== undefined && now - sent[0] >= windowMs) sent.shift();
  if (sent.length >= maxSends) return false;
  sent.push(now);
  return true;
}

function admitReport(fingerprint: string, limitServerSends: boolean): number | null {
  const now = Date.now();
  const entry = seen.get(fingerprint);
  const count = Math.min((entry?.pendingReportCount ?? 0) + 1, maxCount);
  const admitted = !(entry && now - entry.lastAdmittedAtMs < windowMs) && (!limitServerSends || sendAllowed(now));
  seen.delete(fingerprint);
  seen.set(fingerprint, admitted
    ? { lastAdmittedAtMs: now, pendingReportCount: 0 }
    : { lastAdmittedAtMs: entry?.lastAdmittedAtMs ?? -Infinity, pendingReportCount: count });
  if (seen.size > maxFingerprints) {
    const oldest = seen.keys().next();
    if (!oldest.done) seen.delete(oldest.value);
  }
  return admitted ? count : null;
}

function overBudget(body: string): number {
  return new TextEncoder().encode(body).length - maxBytes;
}

function serialize(report: Report): string {
  let current = report;
  let body = JSON.stringify(current);
  for (let excess = overBudget(body); excess > 0 && (current.stack || current.message.length > 1); excess = overBudget(body)) {
    const { stack, message } = current;
    current = stack
      ? { ...current, stack: stack.length > excess ? clip(stack, stack.length - excess) : null }
      : { ...current, message: clip(message, Math.max(1, message.length - excess)) };
    body = JSON.stringify(current);
  }
  return body;
}

async function sendDiagnosticViaRawFetch(report: Report): Promise<void> {
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json', 'X-Wirewatch': '1', [requestHeader]: newRequestId() },
      body: serialize(report),
    });
    if (!response.ok) console.warn(`Wirewatch could not record a browser error (HTTP ${response.status}).`);
  } catch (failure) {
    console.warn('Wirewatch could not record a browser error.', failure);
  }
}

/** Without a network response there is nothing to report to, so `console` delivery only logs locally. */
export function report(error: ClientError, delivery: 'server' | 'console' = 'server'): void {
  const clean: ClientError = {
    ...error,
    message: bounded(error.message, 2000) || '(no message)',
    path: error.path === null ? null : route(error.path),
    issue_path: bounded(error.issue_path, 300),
    stack: bounded(error.stack, 4000),
  };
  const local = delivery === 'console' || !navigator.onLine;
  const fingerprint = JSON.stringify([local, clean.kind, clean.path, clean.status, clean.issue_path, clean.message]);
  const count = admitReport(fingerprint, !local);
  if (count === null) return;
  if (local) console.warn('Wirewatch browser error (not sent while offline).', { ...clean, count });
  else void sendDiagnosticViaRawFetch({ ...clean, count });
}

function describe(value: unknown): { message: string; stack: string | null } {
  if (value instanceof Error) return { message: `${value.name}: ${value.message}`, stack: value.stack ?? null };
  if (typeof value === 'string') return { message: value, stack: null };
  return { message: `Non-error ${value === null ? 'null' : typeof value} thrown`, stack: null };
}

function reportThrown(kind: 'runtime' | 'rejection' | 'render', value: unknown, context: string | null): void {
  const { message, stack } = describe(value);
  report({
    kind, message, request_id: null, path: location.pathname, status: null, issue_path: null,
    stack: [stack, context].filter(Boolean).join('\n') || null,
  });
}

export function installGlobalReporting(): void {
  window.addEventListener('error', event => {
    const thrown: unknown = event.error;
    reportThrown('runtime', thrown ?? event.message, event.filename ? `at ${event.filename}:${event.lineno}:${event.colno}` : null);
  });
  window.addEventListener('unhandledrejection', event => {
    const reason: unknown = event.reason;
    reportThrown('rejection', reason, null);
  });
}

/** React root callback. Replacing React's default handler keeps the console output it would have printed. */
export function reportRender(error: unknown, info: { componentStack?: string | null }): void {
  console.error(error);
  reportThrown('render', error, info.componentStack ? `Component stack:${info.componentStack}` : null);
}
