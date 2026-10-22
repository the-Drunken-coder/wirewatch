import { useState } from 'react';
import type { FormEvent } from 'react';
import { Alert, Button, Callout, FormGroup, HTMLTable, InputGroup, Tab, Tabs, TextArea } from '@blueprintjs/core';
import { discoverySchema, errorMessage, formatDate, get, historicalCoverage, reviewControl, reviewPhaseView, scanStatus, scanStatusView, scanWorking, splitTerms } from './api';
import type { Feed, Job, Mutate, Mutation, Scan } from './api';
import { SetupReviewPanel } from './SetupReview';
import { Empty, RuleFields, SourceLink, useDraft } from './ui';
import type { RuleText } from './ui';

type SavedFields = { revision: number; objective: string; rules: RuleText };
type LiveScan = Extract<Scan, { mode: 'live' }>;

/** A feed with no scheduled time, or one in the past, is due on the collector's next pass. */
function dueAt(feed: Feed, now: number): number | null {
  const at = feed.next_check_at ? Date.parse(feed.next_check_at) : NaN;
  return Number.isNaN(at) || at <= now ? null : at;
}

function feedNextCheck(scan: LiveScan, feed: Feed, now: number): string {
  if (scan.status === 'paused') return 'Paused';
  const at = dueAt(feed, now);
  if (at === null) return 'Due now';
  return `${feed.failures ? 'Backoff until ' : ''}${new Date(at).toLocaleTimeString()}`;
}

function scanNextCheck(scan: LiveScan, now: number): string {
  if (scan.status === 'paused') return 'Paused. Resume to check feeds.';
  if (!scan.feeds.length) return 'No feeds to check.';
  const times = scan.feeds.map(feed => dueAt(feed, now));
  const next = times.includes(null) ? 'Due now' : new Date(Math.min(...times.filter(at => at !== null))).toLocaleTimeString();
  const backingOff = scan.feeds.filter(feed => feed.failures > 0).length;
  return backingOff ? `${next}. ${backingOff} failing feed${backingOff === 1 ? '' : 's'} backing off.` : next;
}

function savedFields(scan: Scan): SavedFields {
  return {
    revision: scan.revision,
    objective: scan.objective,
    rules: {
      any: scan.rules.any_terms.join(', '),
      all: scan.rules.all_terms.join(', '),
      exclude: scan.rules.exclude_terms.join(', '),
    },
  };
}

export function ScanDetail({ scan, jobs, interval, codexAvailable, busy, mutate, onRecords, onDeleted }: {
  scan: Scan; jobs: Job[]; interval: number; codexAvailable: boolean; busy: boolean; mutate: Mutate;
  onRecords: () => void; onDeleted: () => void;
}) {
  const { draft, setDraft, dirty, stale, baseline, reload, markSaved } = useDraft(savedFields(scan));
  const [feedUrl, setFeedUrl] = useState('');
  const [error, setError] = useState('');
  const [confirmDelete, setConfirmDelete] = useState(false);
  const working = scanWorking(scan, jobs);
  const status = scanStatusView[scanStatus(scan, jobs)];
  const now = Date.now();
  const subscribedNotice = scan.mode === 'live' ? 'Feed subscribed. It is checked on the next pass.' : 'Feed subscribed. Run again to collect it.';

  async function act(mutation: Mutation, notice: string): Promise<boolean> {
    setError('');
    try {
      await mutate(mutation, notice);
      return true;
    } catch (failure) {
      setError(errorMessage(failure));
      return false;
    }
  }

  async function save(event: FormEvent) {
    event.preventDefault();
    if (!draft.objective.trim()) return setError('Enter an objective.');
    const submitted = draft;
    const ok = await act({
      path: '/api/scans/update',
      body: {
        scan_id: scan.id,
        expected_revision: baseline.revision,
        objective: submitted.objective.trim(),
        rules: {
          any_terms: splitTerms(submitted.rules.any),
          all_terms: splitTerms(submitted.rules.all),
          exclude_terms: splitTerms(submitted.rules.exclude),
        },
      },
    }, 'Saved. Changed rules replay saved articles.');
    if (ok) markSaved(submitted);
  }

  async function subscribe() {
    const ok = await act({ path: '/api/scans/update', body: { scan_id: scan.id, feed_url: feedUrl.trim() } }, subscribedNotice);
    if (ok) setFeedUrl('');
  }

  // Discovery lists the topic's Google News search first, dated to the window for historical scans.
  async function addTopicSearch() {
    setError('');
    try {
      const query = new URLSearchParams(scan.mode === 'live'
        ? { topic: scan.topic, mode: 'live' }
        : { topic: scan.topic, mode: 'historical', start_date: scan.start_date, end_date: scan.end_date });
      const [feed] = (await get(`/api/discover?${query}`, discoverySchema)).feeds;
      if (!feed) return setError('Discovery returned no topic search feed.');
      await act({ path: '/api/scans/update', body: { scan_id: scan.id, feed_url: feed.url } }, subscribedNotice);
    } catch (failure) {
      setError(errorMessage(failure));
    }
  }

  const runLabel = scan.mode === 'live' ? 'Check now' : scan.completed_at || scan.last_error ? 'Run again' : 'Run now';
  const canRun = !busy && !working && (scan.mode === 'historical' || scan.status === 'active');
  const review = scan.setup_review;
  const control = reviewControl(review);
  const reviewBlocked = control.action === 'start' && (!codexAvailable || scan.status === 'paused');
  const reviewOpen = review !== null && (reviewPhaseView[review.phase].active || review.phase === 'ready');

  return (
    <section className="detail" aria-label={`${scan.topic} details`}>
      <div className="heading">
        <div>
          <h3>{scan.topic}</h3>
          <p className="muted">
            Scan {scan.id} · Revision {scan.revision} · <span className={`text-${status.intent}`}>{status.label}</span>
            {review && <> · Setup review <span className={`text-${reviewPhaseView[review.phase].intent}`}>{reviewPhaseView[review.phase].label}</span></>}
          </p>
        </div>
        <Button variant="minimal" intent="primary" endIcon="arrow-right" onClick={onRecords}>View records</Button>
      </div>

      <div className="toolbar">
        <Button intent="primary" icon="play" disabled={!canRun} onClick={() => void act({ path: '/api/poll', body: { scan_id: scan.id } }, 'Run queued.')}>
          {runLabel}
        </Button>
        {scan.mode === 'live' && (
          <Button
            icon={scan.status === 'active' ? 'pause' : 'play'} disabled={busy}
            onClick={() => void act(
              { path: '/api/scans/update', body: { scan_id: scan.id, status: scan.status === 'active' ? 'paused' : 'active' } },
              scan.status === 'active' ? 'Scan paused.' : 'Scan resumed.',
            )}
          >
            {scan.status === 'active' ? 'Pause' : 'Resume'}
          </Button>
        )}
        <Button icon="refresh" disabled={busy || working} onClick={() => void act({ path: '/api/replay', body: { scan_id: scan.id } }, 'Replay queued.')}>
          Replay saved articles
        </Button>
        <Button
          icon={control.icon} disabled={busy || reviewBlocked}
          onClick={() => void act({ path: '/api/scans/review', body: { scan_id: scan.id, action: control.action } }, control.notice)}
        >
          {control.label}
        </Button>
        {scan.mode === 'live' && scan.status === 'paused' && <span className="muted">Resume to check feeds.</span>}
        {reviewBlocked && <span className="muted">{!codexAvailable
          ? 'Codex CLI is unavailable, so setup review cannot start.'
          : 'Resume this scan to start setup review.'}</span>}
        <Button className="push-right" variant="minimal" intent="danger" icon="trash" disabled={busy} onClick={() => setConfirmDelete(true)}>
          Delete scan
        </Button>
      </div>

      {scan.last_error && <Callout intent="danger" title="Last run failed">{scan.last_error}</Callout>}
      {scan.mode === 'historical' && <Callout intent="warning" icon="history">{historicalCoverage}</Callout>}
      {error && <Callout intent="danger" role="alert">{error}</Callout>}
      {stale && (
        <Callout intent="warning" role="status">
          This scan changed since you started editing. Your edits are kept. Reload saved settings before saving.
          <Button size="small" onClick={() => { reload(); setError(''); }}>Reload saved</Button>
        </Callout>
      )}

      <form onSubmit={event => void save(event)}>
        <Tabs id={`scan-${scan.id}`} className="detail-tabs">
          <Tab id="setup" title="Setup" panel={
            <div className="tab-body">
              <dl className="facts">
                {scan.mode === 'live' ? <>
                  <dt>Mode</dt><dd>Live. Checks feeds about every {interval} s while active. Failing feeds back off.</dd>
                  <dt>Next check</dt><dd>{scanNextCheck(scan, now)}</dd>
                  <dt>Last check</dt><dd>{formatDate(scan.last_polled)}</dd>
                </> : <>
                  <dt>Mode</dt><dd>Historical. Runs once over the window, then completes.</dd>
                  <dt>Window</dt><dd>{scan.start_date} to {scan.end_date}, inclusive UTC. Fixed for this scan.</dd>
                  <dt>Completed</dt><dd>{scan.completed_at ? formatDate(scan.completed_at) : 'Not yet'}</dd>
                  <dt>Last run</dt><dd>{formatDate(scan.last_polled)}</dd>
                </>}
                <dt>Created</dt><dd>{formatDate(scan.created_at)}</dd>
              </dl>
              <FormGroup label="Objective" labelFor={`objective-${scan.id}`}>
                <TextArea
                  id={`objective-${scan.id}`} required maxLength={2000} rows={2} fill
                  value={draft.objective} onChange={event => setDraft({ ...draft, objective: event.target.value })}
                />
              </FormGroup>
            </div>
          } />
          <Tab id="rules" title="Rules" panel={
            <div className="tab-body">
              <RuleFields prefix={`rules-${scan.id}`} value={draft.rules} onChange={rules => setDraft({ ...draft, rules })} />
            </div>
          } />
          <Tab id="feeds" title={`Feeds (${scan.feeds.length})`} panel={
            <div className="tab-body">
              {scan.feeds.length > 0 ? (
                <div className="table-scroll">
                  <HTMLTable compact className="table">
                    <thead><tr><th>Feed</th><th>Health</th><th className="minor">Last checked</th>{scan.mode === 'live' && <th className="minor">Next check</th>}<th className="num">Items</th><th><span className="sr-only">Actions</span></th></tr></thead>
                    <tbody>
                      {scan.feeds.map(feed => (
                        <tr key={feed.id}>
                          <td className="break"><SourceLink url={feed.url} /></td>
                          <td className={feed.error ? 'text-danger' : feed.last_checked ? 'text-success' : 'muted'}>
                            {feed.error ?? (feed.last_checked ? 'Healthy' : 'Not fetched yet')}
                          </td>
                          <td className="minor nowrap muted">{formatDate(feed.last_checked)}</td>
                          {scan.mode === 'live' && (
                            <td className={`minor nowrap ${feed.failures ? 'text-warning' : 'muted'}`}>{feedNextCheck(scan, feed, now)}</td>
                          )}
                          <td className="num">{feed.item_count}</td>
                          <td className="cell-action">
                            <Button
                              size="small" variant="minimal" disabled={busy}
                              onClick={() => void act({ path: '/api/scans/update', body: { scan_id: scan.id, remove_feed_id: feed.id } }, 'Feed removed. Its saved articles stay in the archive.')}
                            >
                              Remove
                            </Button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </HTMLTable>
                </div>
              ) : (
                <Empty>
                  No feed subscriptions.
                  <Button size="small" variant="minimal" intent="primary" disabled={busy} onClick={() => void addTopicSearch()}>Add topic search</Button>
                </Empty>
              )}
              <FormGroup
                label="Add RSS or Atom feed" labelFor={`feed-${scan.id}`} className="add-feed"
                helperText={scan.mode === 'historical' ? 'Only items published in the window are kept. Many feeds list only recent items.' : undefined}
              >
                <InputGroup
                  id={`feed-${scan.id}`} type="url" value={feedUrl} placeholder="https://example.com/feed.xml"
                  onChange={event => setFeedUrl(event.target.value)}
                  onKeyDown={event => {
                    if (event.key === 'Enter') {
                      event.preventDefault();
                      if (feedUrl.trim() && !busy) void subscribe();
                    }
                  }}
                  rightElement={<Button variant="minimal" disabled={busy || !feedUrl.trim()} onClick={() => void subscribe()}>Subscribe</Button>}
                />
              </FormGroup>
            </div>
          } />
          <Tab id="review" title="Setup review" panel={
            <div className="tab-body"><SetupReviewPanel review={review} /></div>
          } />
        </Tabs>
        <div className="toolbar save-bar">
          <Button type="submit" intent="primary" disabled={busy || !dirty}>Save changes</Button>
          <Button variant="minimal" disabled={busy || !dirty} onClick={() => { reload(); setError(''); }}>Discard edits</Button>
          <span className="muted">
            {dirty ? 'Unsaved changes. Saving new rules replays saved articles.' : 'No unsaved changes.'}
            {dirty && reviewOpen && ' Saving also ends the current setup review. Restart it to review the new setup.'}
          </span>
        </div>
      </form>

      <Alert
        isOpen={confirmDelete} intent="danger" icon="trash" loading={busy}
        confirmButtonText="Delete scan" cancelButtonText="Cancel"
        canEscapeKeyCancel={!busy} canOutsideClickCancel={!busy}
        onCancel={() => setConfirmDelete(false)}
        onConfirm={() => void act({ path: '/api/scans/delete', body: { scan_id: scan.id } }, `Deleted ${scan.topic}.`).then(ok => {
          setConfirmDelete(false);
          if (ok) onDeleted();
        })}
      >
        <p><strong>Delete “{scan.topic}”?</strong></p>
        <p>This removes the scan, its feed subscriptions, and its keep and discard records. The shared article archive and activity log stay.</p>
      </Alert>
    </section>
  );
}
