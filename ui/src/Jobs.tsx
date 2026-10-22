import { useState } from 'react';
import { Button, HTMLTable } from '@blueprintjs/core';
import { formatDate, isPending } from './api';
import type { Job, Scan } from './api';

const kindLabel: Record<Job['kind'], string> = {
  poll: 'Scan run',
  replay: 'Replay',
  setup: 'Codex setup',
  setup_review: 'Setup review',
  audit: 'Coverage audit',
  decision_audit: 'Decision audit',
};

const statusClass: Record<Job['status'], string> = {
  queued: '',
  running: 'text-primary',
  completed: 'text-success',
  failed: 'text-danger',
  cancelled: 'text-muted',
};

const recentFailureMs = 24 * 60 * 60 * 1000;

/**
 * Jobs created while the page is open stay listed until dismissed. Jobs persist
 * across restarts, so ones that finished before the page loaded show only when
 * they failed within the last day.
 */
export function Jobs({ jobs, scans }: { jobs: Job[]; scans: Scan[] }) {
  const [before] = useState<ReadonlySet<string>>(() => new Set(jobs.map(job => job.id)));
  const [loadedAt] = useState(() => Date.now());
  const [dismissed, setDismissed] = useState<ReadonlySet<string>>(() => new Set());
  const recentFailure = (job: Job) => job.status === 'failed'
    && loadedAt - Date.parse(job.finished_at ?? job.created_at) < recentFailureMs;
  const visible = jobs
    .filter(job => !dismissed.has(job.id))
    .filter(job => isPending(job) || !before.has(job.id) || recentFailure(job))
    .toReversed();
  if (!visible.length) return null;

  const dismiss = (ids: string[]) => setDismissed(current => new Set([...current, ...ids]));
  const finished = visible.filter(job => !isPending(job)).map(job => job.id);
  const topic = (job: Job) => scans.find(scan => scan.id === job.scan_id)?.topic
    ?? (job.scan_id === null ? '' : `Deleted scan ${job.scan_id}`);

  return (
    <section className="jobs" aria-label="Jobs" aria-live="polite">
      <div className="jobs-head">
        <h2>Jobs</h2>
        {finished.length > 0 && <Button size="small" variant="minimal" onClick={() => dismiss(finished)}>Dismiss finished</Button>}
      </div>
      <div className="table-scroll">
        <HTMLTable compact className="table jobs-table">
          <thead>
            <tr><th>Job</th><th>Scan</th><th>Status</th><th>Progress</th><th className="minor">Created</th><th><span className="sr-only">Dismiss</span></th></tr>
          </thead>
          <tbody>
            {visible.map(job => (
              <tr key={job.id}>
                <td className="nowrap">{kindLabel[job.kind]}</td>
                <td>{topic(job)}</td>
                <td className={`nowrap ${statusClass[job.status]}`}>{job.status}</td>
                <td>{job.message}</td>
                <td className="minor nowrap muted">{formatDate(job.created_at)}</td>
                <td className="cell-action">
                  {!isPending(job) && (
                    <Button
                      size="small" variant="minimal" icon="cross"
                      aria-label={`Dismiss ${kindLabel[job.kind]} job`} onClick={() => dismiss([job.id])}
                    />
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
      </div>
    </section>
  );
}
