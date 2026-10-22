import { useCallback, useEffect, useRef, useState } from 'react';
import { Button, Callout, NonIdealState, Spinner, Tab, Tabs } from '@blueprintjs/core';
import { errorMessage, get, post, stateSchema } from './api';
import type { Mutate, ScanId, State } from './api';
import { Jobs } from './Jobs';
import { Scans } from './Scans';
import { Records } from './Records';
import { Activity } from './Activity';
import { Audits } from './Audits';

const pages = ['scans', 'records', 'activity', 'audits'] as const;
type Page = (typeof pages)[number];

function pageFromHash(): Page {
  return pages.find(page => `#${page}` === location.hash) ?? 'scans';
}

function isPage(value: unknown): value is Page {
  return pages.some(page => page === value);
}

/** Polls /api/state. Responses that arrive out of order are dropped. */
function useServerState() {
  const [state, setState] = useState<State | null>(null);
  const [error, setError] = useState('');
  const sequence = useRef(0);
  const applied = useRef(0);

  const refresh = useCallback(async () => {
    const id = ++sequence.current;
    try {
      const next = stateSchema.parse(await get('/api/state'));
      if (id < applied.current) return;
      applied.current = id;
      setState(next);
      setError('');
    } catch (failure) {
      if (id >= applied.current) setError(errorMessage(failure));
    }
  }, []);

  useEffect(() => {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = async () => {
      await refresh();
      if (!stopped) timer = setTimeout(() => void tick(), 2000);
    };
    void tick();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [refresh]);

  return { state, error, refresh };
}

export function App() {
  const { state, error, refresh } = useServerState();
  const [page, setPage] = useState<Page>(pageFromHash);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState('');
  const [selected, setSelected] = useState<ScanId | null>(null);
  const [recordScan, setRecordScan] = useState<ScanId | null>(null);
  const lock = useRef(false);

  useEffect(() => {
    const onHash = () => setPage(pageFromHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  const navigate = (next: Page) => {
    setPage(next);
    history.replaceState(null, '', `#${next}`);
  };

  const mutate: Mutate = async (mutation, message) => {
    if (lock.current) throw new Error('Wait for the current change to finish.');
    lock.current = true;
    setBusy(true);
    try {
      const result = await post(mutation);
      setNotice(message);
      await refresh();
      return result;
    } finally {
      lock.current = false;
      setBusy(false);
    }
  };

  return (
    <>
      <header className="brand">
        <h1>Wirewatch</h1>
        <span>
          {error ? 'Connection error' : state
            ? `Codex CLI ${state.codex_available ? 'available' : 'unavailable'} · ${state.article_count} archived articles`
            : 'Connecting'}
        </span>
      </header>
      {error && (
        <Callout intent="danger" role="alert" className="notice">
          {state ? `Refresh failed. Showing the last loaded state. ${error}` : error}
          <Button size="small" onClick={() => void refresh()}>Retry</Button>
        </Callout>
      )}
      {notice && (
        <Callout intent="success" role="status" className="notice">
          {notice}
          <Button size="small" variant="minimal" icon="cross" aria-label="Dismiss notice" onClick={() => setNotice('')} />
        </Callout>
      )}
      {state && <Jobs jobs={state.jobs} scans={state.scans} />}
      {state ? (
        <Tabs
          id="pages" className="pages" selectedTabId={page}
          onChange={id => { if (isPage(id)) navigate(id); }}
        >
          <Tab id="scans" title="Scans" panel={
            <Scans
              state={state} busy={busy} mutate={mutate}
              selected={selected} onSelect={setSelected}
              onRecords={id => { setRecordScan(id); navigate('records'); }}
              onDeleted={id => {
                setSelected(null);
                if (recordScan === id) setRecordScan(null);
              }}
            />
          } />
          <Tab id="records" title="Records" panel={
            <Records state={state} scanFilter={recordScan} onScanFilter={setRecordScan} />
          } />
          <Tab id="activity" title="Activity" panel={<Activity state={state} />} />
          <Tab id="audits" title="Audits" panel={<Audits state={state} busy={busy} mutate={mutate} />} />
        </Tabs>
      ) : (
        <NonIdealState
          className="loading"
          icon={error ? 'error' : <Spinner size={24} />}
          title={error ? 'State could not be loaded' : 'Loading'}
          description={error ? 'Retry once the server is running.' : undefined}
        />
      )}
    </>
  );
}
