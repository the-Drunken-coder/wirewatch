import { Fragment, useState } from 'react';
import type { FormEvent } from 'react';
import { Button, Callout, FormGroup, HTMLSelect, HTMLTable, Switch, TextArea } from '@blueprintjs/core';
import { errorMessage, formatDate, modelBusy, validMinutes } from './api';
import type { AuditKind, DecisionReport, Mutate, Settings, State } from './api';
import { Empty, ExpandButton, Json, MinutesField, useDraft, useExpanded } from './ui';

type AuditView = {
  title: string;
  description: string;
  path: '/api/audit' | '/api/decision-audit';
  enabled: 'audit_enabled' | 'decision_audit_enabled';
  interval: 'audit_interval_minutes' | 'decision_audit_interval_minutes';
};

const auditViews: Record<AuditKind, AuditView> = {
  decision: {
    title: 'Decision audit',
    description: 'Checks every stored keep and discard decision for gaps and rule consistency, then Codex reviews them for relevance errors. '
      + 'It may update rules and replay existing scans. It does not fetch feeds or create scans. Scheduled runs skip Codex when nothing changed.',
    path: '/api/decision-audit',
    enabled: 'decision_audit_enabled',
    interval: 'decision_audit_interval_minutes',
  },
  coverage: {
    title: 'Coverage audit',
    description: 'Codex compares broader headlines with collected articles to find missing topics, sources, or articles. '
      + 'It may update rules, subscribe feeds, replay, and create related scans. Feed failures and overdue checks show on each scan without Codex.',
    path: '/api/audit',
    enabled: 'audit_enabled',
    interval: 'audit_interval_minutes',
  },
};

const auditKinds: AuditKind[] = ['decision', 'coverage'];
type KindFilter = AuditKind | 'all';

const kindOptions: { value: KindFilter; label: string }[] = [
  { value: 'all', label: 'All audits' },
  { value: 'decision', label: 'Decision' },
  { value: 'coverage', label: 'Coverage' },
];

function DecisionChecks({ report }: { report: DecisionReport }) {
  return (
    <>
      <dl className="facts">
        <dt>Missing decisions</dt>
        <dd className={report.missing_count ? 'text-danger' : ''}>{report.missing_count}</dd>
        <dt>Inconsistent decisions</dt>
        <dd className={report.inconsistent_count ? 'text-danger' : ''}>{report.inconsistent_count}</dd>
        <dt>Gap rule</dt>
        <dd>{report.gap_definition}</dd>
      </dl>
      {report.missing_count > 0 && (
        <details>
          <summary>{report.missing_count} missing scan and article pairs</summary>
          <Json value={report.missing_decisions} />
        </details>
      )}
      {report.inconsistent_count > 0 && (
        <details>
          <summary>{report.inconsistent_count} inconsistent pairs with the expected rule result</summary>
          <Json value={report.inconsistent_decisions} />
        </details>
      )}
    </>
  );
}

function isKindFilter(value: string): value is KindFilter {
  return kindOptions.some(option => option.value === value);
}

export function Audits({ state, busy, mutate }: { state: State; busy: boolean; mutate: Mutate }) {
  const { draft, setDraft, dirty, stale, reload, markSaved } = useDraft(state.settings);
  const [error, setError] = useState('');
  const [filter, setFilter] = useState<KindFilter>('all');
  const expanded = useExpanded();
  const codexBusy = modelBusy(state.jobs);
  const blocked = !state.codex_available ? 'Codex CLI is unavailable on this server.'
    : codexBusy ? 'Codex is busy with a setup, review, or audit.'
    : !state.scans.length ? 'Create a scan before auditing.'
    : '';
  const audits = state.audits.filter(audit => filter === 'all' || audit.kind === filter);

  async function save(event: FormEvent) {
    event.preventDefault();
    setError('');
    if (!validMinutes(draft.audit_interval_minutes) || !validMinutes(draft.decision_audit_interval_minutes)) {
      return setError('Use whole intervals from 1 to 10080 minutes.');
    }
    if (!draft.audit_objective.trim()) return setError('Enter an audit objective.');
    const submitted: Settings = { ...draft, audit_objective: draft.audit_objective.trim() };
    try {
      await mutate({ path: '/api/settings', body: submitted }, 'Audit settings saved.');
      markSaved(submitted);
    } catch (failure) {
      setError(errorMessage(failure));
    }
  }

  async function runNow(view: AuditView) {
    setError('');
    try {
      await mutate({ path: view.path, body: {} }, `${view.title} queued. Progress shows under Jobs.`);
    } catch (failure) {
      setError(errorMessage(failure));
    }
  }

  return (
    <>
      <h2>Audits</h2>
      <p className="muted">
        Both audits use Codex and apply to live and historical scans. Routine collection and feed health never use Codex.
        {blocked && ` ${blocked}`}
      </p>
      {error && <Callout intent="danger" role="alert">{error}</Callout>}
      {stale && (
        <Callout intent="warning" role="status">
          Audit settings changed since you started editing. Your edits are kept.
          <Button size="small" onClick={() => { reload(); setError(''); }}>Reload saved</Button>
        </Callout>
      )}
      <form className="settings" onSubmit={event => void save(event)}>
        {auditKinds.map(kind => {
          const view = auditViews[kind];
          const saved = state.settings;
          return (
            <section key={kind} aria-label={view.title}>
              <div className="heading">
                <div>
                  <h3>{view.title}</h3>
                  <p className="muted">
                    {view.description}
                    {' '}{saved[view.enabled] ? `Runs every ${saved[view.interval]} minutes.` : 'Recurring runs are off.'}
                  </p>
                </div>
                <Button icon="search-template" disabled={busy || Boolean(blocked)} onClick={() => void runNow(view)}>Run now</Button>
              </div>
              <div className="fields">
                <Switch
                  label="Recurring" checked={draft[view.enabled]}
                  onChange={event => setDraft({ ...draft, [view.enabled]: event.currentTarget.checked })}
                />
                <MinutesField
                  id={`${kind}-interval`} label="Interval in minutes" value={draft[view.interval]}
                  onChange={minutes => setDraft({ ...draft, [view.interval]: minutes })}
                />
              </div>
            </section>
          );
        })}
        <FormGroup label="Audit objective, shared by both audits" labelFor="audit-objective">
          <TextArea
            id="audit-objective" required maxLength={2000} rows={2} fill value={draft.audit_objective}
            onChange={event => setDraft({ ...draft, audit_objective: event.target.value })}
          />
        </FormGroup>
        <div className="toolbar">
          <Button type="submit" intent="primary" disabled={busy || !dirty}>Save settings</Button>
          <Button variant="minimal" disabled={busy || !dirty} onClick={() => { reload(); setError(''); }}>Discard edits</Button>
        </div>
      </form>

      <div className="heading">
        <h3>History</h3>
        <HTMLSelect
          aria-label="Filter by audit kind" options={kindOptions} value={filter}
          onChange={event => { if (isKindFilter(event.target.value)) setFilter(event.target.value); }}
        />
      </div>
      {audits.length > 0 ? (
        <div className="table-scroll">
          <HTMLTable compact className="table">
            <thead>
              <tr>
                <th><span className="sr-only">Expand</span></th><th>Audit</th><th>Kind</th><th>Status</th><th>Started</th><th className="minor">Finished</th>
                <th className="num">Coverage</th><th className="num">Decisions</th><th className="num">Fresh</th><th className="num">Actions</th>
              </tr>
            </thead>
            <tbody>
              {audits.map(audit => {
                const key = String(audit.id);
                const open = expanded.isOpen(key);
                return (
                  <Fragment key={audit.id}>
                    <tr>
                      <td className="cell-action"><ExpandButton open={open} controls={`audit-${key}`} label={`audit ${audit.id}`} onClick={() => expanded.toggle(key)} /></td>
                      <td>{audit.id}</td>
                      <td className="nowrap">{auditViews[audit.kind].title}</td>
                      <td className={audit.error || audit.status === 'failed' ? 'text-danger' : audit.status === 'running' ? 'text-primary' : ''}>{audit.status}</td>
                      <td className="nowrap muted">{formatDate(audit.started_at)}</td>
                      <td className="minor nowrap muted">{audit.finished_at ? formatDate(audit.finished_at) : 'Not finished'}</td>
                      <td className="num">{audit.coverage_count}</td>
                      <td className="num">{audit.decision_count}</td>
                      <td className="num">{audit.fresh_count}</td>
                      <td className="num">{audit.actions.length}</td>
                    </tr>
                    {open && (
                      <tr className="expansion" id={`audit-${key}`}>
                        <td />
                        <td colSpan={9}>
                          {audit.error && <Callout intent="danger">{audit.error}</Callout>}
                          {audit.kind === 'decision' && <DecisionChecks report={audit.report} />}
                          <p className="summary">{audit.rationale || 'No rationale recorded.'}</p>
                          {audit.actions.length > 0 ? <Json value={audit.actions} /> : <p className="muted">No tool actions.</p>}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </HTMLTable>
        </div>
      ) : (
        <Empty>{state.audits.length ? 'No audits of this kind.' : 'No audits yet.'}</Empty>
      )}
    </>
  );
}
