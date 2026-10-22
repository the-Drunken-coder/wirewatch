import { Fragment, useState } from 'react';
import { HTMLSelect, HTMLTable } from '@blueprintjs/core';
import { formatDate } from './api';
import type { State } from './api';
import { Empty, ExpandButton, Json, Pagination, pageSlice, useExpanded } from './ui';

const PAGE = 100;

export function Activity({ state }: { state: State }) {
  const [type, setType] = useState('');
  const [page, setPage] = useState(0);
  const expanded = useExpanded();
  const types = [...new Set(state.activity.map(event => event.type))].sort();
  const events = state.activity.filter(event => !type || event.type === type).toSorted((a, b) => b.id - a.id);
  const shown = pageSlice(events, page, PAGE);
  const topic = (id: number | null) => id === null ? '' : state.scans.find(scan => scan.id === id)?.topic ?? `Scan ${id}`;

  return (
    <>
      <div className="heading"><h2>Activity</h2></div>
      <div className="toolbar">
        <HTMLSelect
          aria-label="Filter by event type" value={type}
          onChange={event => { setType(event.target.value); setPage(0); }}
          options={[{ value: '', label: 'All event types' }, ...types.map(value => ({ value, label: value }))]}
        />
        <span className="muted">{events.length} events, newest first</span>
      </div>
      {shown.items.length > 0 ? (
        <div className="table-scroll">
          <HTMLTable compact className="table">
            <thead>
              <tr><th><span className="sr-only">Expand</span></th><th>Time</th><th>Type</th><th className="minor">Scan</th><th>Message</th></tr>
            </thead>
            <tbody>
              {shown.items.map(event => {
                const key = String(event.id);
                const hasDetails = event.details !== null && event.details !== undefined;
                const open = hasDetails && expanded.isOpen(key);
                return (
                  <Fragment key={event.id}>
                    <tr>
                      <td className="cell-action">
                        {hasDetails && <ExpandButton open={open} controls={`event-${key}`} label={`${event.type} data`} onClick={() => expanded.toggle(key)} />}
                      </td>
                      <td className="nowrap muted">{formatDate(event.at)}</td>
                      <td className="nowrap">{event.type}</td>
                      <td className="minor">{topic(event.scan_id)}</td>
                      <td>{event.message}</td>
                    </tr>
                    {open && (
                      <tr className="expansion" id={`event-${key}`}>
                        <td />
                        <td colSpan={4}><Json value={event.details} /></td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </HTMLTable>
        </div>
      ) : (
        <Empty>{state.activity.length ? 'No events of this type.' : 'No activity yet.'}</Empty>
      )}
      <Pagination page={shown.page} total={events.length} size={PAGE} onChange={setPage} />
    </>
  );
}
