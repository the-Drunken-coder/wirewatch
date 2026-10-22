import { useState } from 'react';
import { Button, HTMLSelect, HTMLTable, InputGroup } from '@blueprintjs/core';
import { formatDate, formatTime, modelRuns, reviewPhaseView, scanStatus, scanStatusView } from './api';
import type { Mode, Mutate, Scan, ScanId, SetupReview, State } from './api';
import { CreateScan } from './CreateScan';
import { ScanDetail } from './ScanDetail';
import { Empty } from './ui';

type ModeFilter = Mode | 'all';

const modeOptions: { value: ModeFilter; label: string }[] = [
  { value: 'all', label: 'All modes' },
  { value: 'live', label: 'Live' },
  { value: 'historical', label: 'Historical' },
];

function isModeFilter(value: string): value is ModeFilter {
  return modeOptions.some(option => option.value === value);
}

export function Scans({ state, busy, mutate, selected, onSelect, onRecords, onDeleted }: {
  state: State; busy: boolean; mutate: Mutate;
  selected: ScanId | null; onSelect: (id: ScanId | null) => void;
  onRecords: (id: ScanId) => void; onDeleted: (id: ScanId) => void;
}) {
  const [search, setSearch] = useState('');
  const [mode, setMode] = useState<ModeFilter>('all');
  const [creating, setCreating] = useState(false);
  const needle = search.trim().toLocaleLowerCase();
  const scans = state.scans.filter(scan =>
    (mode === 'all' || scan.mode === mode)
    && `${scan.topic} ${scan.objective}`.toLocaleLowerCase().includes(needle));
  const scan = state.scans.find(scan => scan.id === selected);
  const { stats } = state;

  return (
    <>
      <div className="heading">
        <h2>Scans</h2>
        <Button intent="primary" icon="plus" onClick={() => setCreating(true)}>New scan</Button>
      </div>
      <div className="toolbar">
        <InputGroup
          aria-label="Find a scan" leftIcon="search" placeholder="Find a scan" type="search"
          value={search} onChange={event => setSearch(event.target.value)}
        />
        <HTMLSelect
          aria-label="Filter by mode" options={modeOptions} value={mode}
          onChange={event => { if (isModeFilter(event.target.value)) setMode(event.target.value); }}
        />
        <span className="muted">
          {scans.length} of {state.scans.length} scans · {stats.active} active · {stats.feeds} feeds · {stats.kept} kept · {stats.discarded} discarded
        </span>
      </div>
      {scans.length > 0 ? (
        <div className="table-scroll">
          <HTMLTable compact interactive className="table scan-table">
            <thead>
              <tr>
                <th>Topic</th><th>Mode and period</th><th>Status</th><th>Setup review</th><th>Feeds</th>
                <th className="num">Kept</th><th className="num">Discarded</th><th className="minor">Last check</th>
              </tr>
            </thead>
            <tbody>
              {scans.map(item => (
                <ScanRow
                  key={item.id} scan={item} state={state} selected={item.id === selected}
                  onToggle={() => onSelect(item.id === selected ? null : item.id)}
                />
              ))}
            </tbody>
          </HTMLTable>
        </div>
      ) : (
        <Empty>
          {state.scans.length ? 'No scans match these filters.' : 'No scans yet. Create one to start collecting articles.'}
          {state.scans.length > 0 && (
            <Button size="small" variant="minimal" intent="primary" onClick={() => { setSearch(''); setMode('all'); }}>Clear filters</Button>
          )}
        </Empty>
      )}
      {scan && (
        <ScanDetail
          key={scan.id} scan={scan} jobs={state.jobs} interval={state.collection_interval_seconds}
          codexAvailable={state.codex_available} busy={busy} mutate={mutate}
          onRecords={() => onRecords(scan.id)}
          onDeleted={() => {
            onDeleted(scan.id);
            if (scans.length === 1 && state.scans.length > 1) {
              setSearch('');
              setMode('all');
            }
          }}
        />
      )}
      {creating && (
        <CreateScan
          state={state} busy={busy} mutate={mutate}
          onClose={() => setCreating(false)}
          onCreated={id => {
            setCreating(false);
            if (id !== null) {
              setSearch('');
              setMode('all');
              onSelect(id);
            }
          }}
        />
      )}
    </>
  );
}

function ScanRow({ scan, state, selected, onToggle }: {
  scan: Scan; state: State; selected: boolean; onToggle: () => void;
}) {
  const status = scanStatusView[scanStatus(scan, state.jobs)];
  const failing = scan.feeds.filter(feed => feed.error).length;
  const checked = scan.feeds.filter(feed => feed.last_checked).length;
  return (
    <tr className={selected ? 'selected' : undefined} onClick={onToggle}>
      <td>
        <button
          type="button" className="row-link" aria-expanded={selected}
          onClick={event => { event.stopPropagation(); onToggle(); }}
        >
          {scan.topic}
        </button>
      </td>
      <td className="nowrap">
        {scan.mode === 'live' ? 'Live' : 'Historical'}
        <div className="sub">
          {scan.mode === 'historical' ? `${scan.start_date} to ${scan.end_date} UTC`
            : scan.status === 'active' ? `Continuous, ${state.collection_interval_seconds} s checks` : 'Checks paused'}
        </div>
      </td>
      <td className={`nowrap text-${status.intent}`}>{status.label}</td>
      <ReviewCell review={scan.setup_review} />
      <td className="nowrap">
        {scan.feeds.length}
        <div className={`sub ${failing ? 'text-danger' : ''}`}>
          {failing ? `${failing} failing` : scan.feeds.length ? `${checked} checked` : 'None'}
        </div>
      </td>
      <td className="num">{scan.kept}</td>
      <td className="num">{scan.discarded}</td>
      <td className="minor nowrap muted">{formatDate(scan.last_polled)}</td>
    </tr>
  );
}

function ReviewCell({ review }: { review: SetupReview | null }) {
  if (!review) {
    return <td className="nowrap muted">Not started<div className="sub">No model runs</div></td>;
  }
  const phase = reviewPhaseView[review.phase];
  const next = review.phase === 'collecting' && review.next_review_at
    ? ` · review ${formatTime(review.next_review_at)}` : '';
  return (
    <td className="nowrap">
      <span className={`text-${phase.intent}`}>{phase.label}</span>
      <div className="sub">Round {review.round} of {review.max_rounds} · {modelRuns(review.calls_used, review.call_limit)}{next}</div>
    </td>
  );
}
