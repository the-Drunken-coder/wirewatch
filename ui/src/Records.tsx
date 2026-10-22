import { Fragment, useState } from 'react';
import { AnchorButton, HTMLSelect, HTMLTable, InputGroup } from '@blueprintjs/core';
import { formatDate } from './api';
import type { ArticleRecord, ScanId, State } from './api';
import { Empty, ExpandButton, Pagination, SourceLink, pageSlice, useExpanded } from './ui';

type Decision = 'kept' | 'discarded' | 'all';

const decisions: { value: Decision; label: string }[] = [
  { value: 'kept', label: 'Kept' },
  { value: 'discarded', label: 'Discarded' },
  { value: 'all', label: 'All decisions' },
];

const PAGE = 50;

function isDecision(value: string): value is Decision {
  return decisions.some(option => option.value === value);
}

/** Mirrors the server's export filter so the JSON export matches the table. */
function matches(record: ArticleRecord, scan: ScanId | null, decision: Decision, query: string): boolean {
  return (scan === null || record.scan_id === scan)
    && (decision === 'all' || record.kept === (decision === 'kept'))
    && (!query || `${record.title} ${record.summary} ${record.source}`.toLocaleLowerCase().includes(query.toLocaleLowerCase()));
}

export function Records({ state, scanFilter, onScanFilter }: {
  state: State; scanFilter: ScanId | null; onScanFilter: (scan: ScanId | null) => void;
}) {
  const [decision, setDecision] = useState<Decision>('kept');
  const [query, setQuery] = useState('');
  const [page, setPage] = useState(0);
  const expanded = useExpanded();
  const scan = state.scans.some(item => item.id === scanFilter) ? scanFilter : null;
  const filtered = state.records.filter(record => matches(record, scan, decision, query));
  const shown = pageSlice(filtered, page, PAGE);
  const exportQuery = new URLSearchParams({ scan: scan === null ? '' : String(scan), decision, q: query });

  return (
    <>
      <div className="heading">
        <h2>Records</h2>
        <AnchorButton icon="download" href={`/api/export?${exportQuery}`} download="wirewatch-records.json" disabled={!filtered.length}>
          Export JSON
        </AnchorButton>
      </div>
      <div className="toolbar">
        <InputGroup
          aria-label="Search records" leftIcon="search" placeholder="Search title, excerpt, source" type="search"
          value={query} onChange={event => { setQuery(event.target.value); setPage(0); }}
        />
        <HTMLSelect
          aria-label="Filter by scan" value={scan === null ? '' : String(scan)}
          onChange={event => {
            onScanFilter(state.scans.find(item => String(item.id) === event.target.value)?.id ?? null);
            setPage(0);
          }}
          options={[{ value: '', label: 'All scans' }, ...state.scans.map(item => ({ value: String(item.id), label: item.topic }))]}
        />
        <HTMLSelect
          aria-label="Filter by decision" value={decision} options={decisions}
          onChange={event => { if (isDecision(event.target.value)) setDecision(event.target.value); setPage(0); }}
        />
        <span className="muted">{filtered.length} decisions · {state.article_count} archived articles</span>
      </div>
      {shown.items.length > 0 ? (
        <div className="table-scroll">
          <HTMLTable compact className="table records-table">
            <thead>
              <tr>
                <th><span className="sr-only">Expand</span></th><th>Title</th><th>Decision</th><th>Scan</th>
                <th className="minor">Source</th><th className="minor">Published</th><th className="num">Rev</th>
              </tr>
            </thead>
            <tbody>
              {shown.items.map(record => {
                const key = `${record.scan_id}-${record.id}`;
                const open = expanded.isOpen(key);
                return (
                  <Fragment key={key}>
                    <tr>
                      <td className="cell-action"><ExpandButton open={open} controls={`record-${key}`} label={record.title} onClick={() => expanded.toggle(key)} /></td>
                      <td><SourceLink url={record.url}>{record.title}</SourceLink></td>
                      <td className={record.kept ? 'text-success' : 'muted'}>{record.kept ? 'Kept' : 'Discarded'}</td>
                      <td>{record.scan_topic}</td>
                      <td className="minor">{record.source}</td>
                      <td className="minor nowrap muted">{record.published_at ? formatDate(record.published_at) : 'Unknown'}</td>
                      <td className="num">{record.rule_revision}</td>
                    </tr>
                    {open && (
                      <tr className="expansion" id={`record-${key}`}>
                        <td />
                        <td colSpan={6}><RecordDetail record={record} /></td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </HTMLTable>
        </div>
      ) : (
        <Empty>{state.records.length ? 'No records match these filters.' : 'No records yet. Records appear after a scan runs.'}</Empty>
      )}
      <Pagination page={shown.page} total={filtered.length} size={PAGE} onChange={setPage} />
    </>
  );
}

function RecordDetail({ record }: { record: ArticleRecord }) {
  return (
    <>
      {record.reasons.length > 0 && (
        <ul className="reasons">{record.reasons.map((reason, index) => <li key={index}>{reason}</li>)}</ul>
      )}
      {record.summary && <p className="summary">{record.summary}</p>}
      <dl className="facts">
        <dt>Matched terms</dt><dd>{record.matched_terms.join(', ') || 'None'}</dd>
        <dt>Rule revision</dt><dd>{record.rule_revision}</dd>
        <dt>Decided</dt><dd>{formatDate(record.decided_at)}</dd>
        <dt>Published</dt><dd>{record.published_at || 'Not supplied by the feed'}</dd>
        <dt>Fetched</dt><dd>{formatDate(record.fetched_at)}</dd>
        <dt>Source</dt><dd>{record.source}</dd>
        <dt>Source URL</dt><dd className="break"><SourceLink url={record.url} /></dd>
        <dt>Feed</dt><dd className="break"><SourceLink url={record.feed_url} /></dd>
        <dt>Article</dt><dd>{record.id} · {record.stable_key}</dd>
      </dl>
    </>
  );
}
