import { Fragment } from 'react';
import { Callout, HTMLTable } from '@blueprintjs/core';
import { formatDate, modelRuns, readyCaveat, reviewPhaseView, roundStatusView } from './api';
import type { SetupReview, SetupRound } from './api';
import { Empty, ExpandButton, Json, SourceLink, useExpanded } from './ui';

function readySummary(review: SetupReview, newestFirstRounds: SetupRound[]): string {
  const through = review.archive_through_id === null ? '' : ` through article #${review.archive_through_id}`;
  const finished = newestFirstRounds.find(round => round.run_id === review.run_id && round.finished_at)?.finished_at;
  return `Stable on reviewed evidence${through}${finished ? ` as of ${formatDate(finished)}` : ''}.`;
}

export function SetupReviewPanel({ review }: { review: SetupReview | null }) {
  const expanded = useExpanded();
  if (!review) {
    return (
      <Empty>
        No setup review yet. Starting one collects for a short window, then Codex reviews every decision and feed coverage
        and refines rules and feeds over a capped number of rounds and model runs. Viewing a scan never runs Codex.
      </Empty>
    );
  }
  const phase = reviewPhaseView[review.phase];
  const rounds = review.history.toSorted((a, b) => b.id - a.id);

  return (
    <>
      <dl className="facts">
        <dt>Stage</dt>
        <dd>
          <span className={`text-${phase.intent}`}>{phase.label}</span>
          {review.phase === 'ready' && <> {readySummary(review, rounds)} {readyCaveat}</>}
        </dd>
        <dt>Round</dt><dd>{review.round} of {review.max_rounds}</dd>
        <dt>Model runs</dt><dd>{modelRuns(review.calls_used, review.call_limit)} this run · {modelRuns(review.lifetime_calls_used)} for this scan</dd>
        <dt>Next review</dt><dd>{review.next_review_at ? formatDate(review.next_review_at) : 'Not scheduled'}</dd>
        <dt>Reviewed</dt>
        <dd>
          {review.reviewed_count} of {review.total_count} decisions
          {review.archive_through_id !== null && ` through article #${review.archive_through_id}`}
        </dd>
        <dt>Reason</dt><dd>{review.reason || 'None recorded.'}</dd>
      </dl>
      {rounds.length > 0 ? (
        <div className="table-scroll">
          <HTMLTable compact className="table">
            <thead>
              <tr>
                <th><span className="sr-only">Expand</span></th><th>Round</th><th>Run</th><th>Result</th><th>Started</th><th className="minor">Finished</th>
                <th className="num">Reviewed</th><th className="num">Model runs</th><th className="num">Actions</th><th className="num">Probes</th>
              </tr>
            </thead>
            <tbody>
              {rounds.map(round => {
                const key = String(round.id);
                const open = expanded.isOpen(key);
                const status = roundStatusView[round.status];
                return (
                  <Fragment key={round.id}>
                    <tr>
                      <td className="cell-action"><ExpandButton open={open} controls={`review-round-${key}`} label={`round ${round.round}`} onClick={() => expanded.toggle(key)} /></td>
                      <td>{round.round}</td>
                      <td className="nowrap muted">{round.run_id === review.run_id ? 'Current' : 'Earlier'}</td>
                      <td className={`nowrap text-${status.intent}`}>{status.label}</td>
                      <td className="nowrap muted">{formatDate(round.started_at)}</td>
                      <td className="minor nowrap muted">{round.finished_at ? formatDate(round.finished_at) : 'Not finished'}</td>
                      <td className="num">{round.reviewed_count} of {round.total_count}</td>
                      <td className="num">{round.calls_used}</td>
                      <td className="num">{round.actions.length}</td>
                      <td className="num">{round.probes.length}</td>
                    </tr>
                    {open && (
                      <tr className="expansion" id={`review-round-${key}`}>
                        <td />
                        <td colSpan={9}>
                          {round.error && <Callout intent="danger">{round.error}</Callout>}
                          <p className="summary">{round.rationale || 'No rationale recorded.'}</p>
                          {round.actions.length > 0 ? <Json value={round.actions} /> : <p className="muted">No setup changes.</p>}
                          {round.probes.length > 0 ? <Probes probes={round.probes} /> : <p className="muted">No probes.</p>}
                          <p className="muted">Run {round.run_id}</p>
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
        <Empty>No rounds yet.</Empty>
      )}
    </>
  );
}

function Probes({ probes }: { probes: SetupRound['probes'] }) {
  return (
    <HTMLTable compact className="table">
      <thead><tr><th>Probe feed</th><th>Query</th><th className="num">Items</th><th>Result</th></tr></thead>
      <tbody>
        {probes.map((probe, index) => (
          <tr key={`${probe.url}-${index}`}>
            <td className="break"><SourceLink url={probe.url} /></td>
            <td className="break">{probe.query ?? <span className="muted">Feed URL</span>}</td>
            <td className="num">{probe.item_count}</td>
            <td className={probe.error ? 'text-danger' : 'text-success'}>{probe.error ?? 'Fetched'}</td>
          </tr>
        ))}
      </tbody>
    </HTMLTable>
  );
}
