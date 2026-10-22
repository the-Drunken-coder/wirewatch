import { useState } from 'react';
import type { FormEvent } from 'react';
import { Button, Callout, Checkbox, Classes, Dialog, FormGroup, HTMLSelect, InputGroup, SegmentedControl, TextArea } from '@blueprintjs/core';
import { createdSchema, discoverySchema, errorMessage, get, historicalCoverage, modelBusy, parseResponse, splitTerms, windowFields } from './api';
import type { Discovery, Mutate, ScanId, ScanWindow, State } from './api';
import { RuleFields, SourceLink } from './ui';
import type { RuleText } from './ui';

type Setup = 'codex' | 'manual';
type Preview = { key: string; result: Discovery; selected: ReadonlySet<string> };

function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

function windowError(period: ScanWindow): string {
  if (period.mode === 'live') return '';
  if (!period.start_date || !period.end_date) return 'Choose a start and end date.';
  if (period.start_date > period.end_date) return 'The end date must be on or after the start date.';
  return '';
}

export function CreateScan({ state, busy, mutate, onClose, onCreated }: {
  state: State; busy: boolean; mutate: Mutate; onClose: () => void; onCreated: (id: ScanId | null) => void;
}) {
  const [topic, setTopic] = useState('');
  const [objective, setObjective] = useState('');
  const [period, setPeriod] = useState<ScanWindow>({ mode: 'live' });
  const [setup, setSetup] = useState<Setup>(state.codex_available ? 'codex' : 'manual');
  const [rules, setRules] = useState<RuleText>({ any: '', all: '', exclude: '' });
  const [customFeeds, setCustomFeeds] = useState('');
  const [preview, setPreview] = useState<Preview | null>(null);
  const [discovering, setDiscovering] = useState(false);
  const [error, setError] = useState('');
  const codexBusy = modelBusy(state.jobs);
  const previewKey = JSON.stringify([topic.trim(), period]);
  const current = preview?.key === previewKey ? preview : null;
  const today = todayUtc();

  function changeMode(mode: string) {
    setPeriod(mode === 'historical' ? { mode: 'historical', start_date: '', end_date: '' } : { mode: 'live' });
  }

  async function discover() {
    setError('');
    if (!topic.trim()) return setError('Enter a topic to discover feeds.');
    const invalid = windowError(period);
    if (invalid) return setError(invalid);
    const query = new URLSearchParams({ topic: topic.trim(), mode: period.mode });
    if (period.mode === 'historical') {
      query.set('start_date', period.start_date);
      query.set('end_date', period.end_date);
    }
    setDiscovering(true);
    try {
      const result = await get(`/api/discover?${query}`, discoverySchema);
      setPreview({ key: previewKey, result, selected: new Set(result.feeds.slice(0, 1).map(feed => feed.url)) });
    } catch (failure) {
      setError(errorMessage(failure));
    } finally {
      setDiscovering(false);
    }
  }

  function toggleFeed(url: string, checked: boolean) {
    if (!current) return;
    const selected = new Set(current.selected);
    if (checked) selected.add(url);
    else selected.delete(url);
    setPreview({ ...current, selected });
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError('');
    const invalid = windowError(period);
    if (invalid) return setError(invalid);
    // An empty list makes the server subscribe the topic's Google News search, dated for historical scans.
    const feedUrls = [...new Set([...(current?.selected ?? []), ...customFeeds.split('\n').map(url => url.trim()).filter(Boolean)])];
    const fields = { topic: topic.trim(), objective: objective.trim(), feed_urls: feedUrls, ...windowFields(period) };
    try {
      if (setup === 'codex') {
        await mutate({ path: '/api/plan', body: fields }, 'Codex setup queued. Progress shows under Jobs.');
        onCreated(null);
        return;
      }
      const created = parseResponse(createdSchema, await mutate({
        path: '/api/scans',
        body: {
          ...fields,
          rules: { any_terms: splitTerms(rules.any), all_terms: splitTerms(rules.all), exclude_terms: splitTerms(rules.exclude) },
        },
      }, period.mode === 'live' ? 'Scan created. Continuous checks started.' : 'Scan created. Historical run queued.'), { path: '/api/scans', requestId: null });
      onCreated(created.scan_id);
    } catch (failure) {
      setError(errorMessage(failure));
    }
  }

  const codexBlocked = setup === 'codex' && (!state.codex_available || codexBusy);

  return (
    <Dialog
      isOpen title="New scan" className="create-dialog"
      onClose={onClose} canEscapeKeyClose={!busy} canOutsideClickClose={false}
    >
      <form onSubmit={event => void submit(event)}>
        <div className={Classes.DIALOG_BODY}>
          <FormGroup label="Topic" labelFor="new-topic">
            <InputGroup id="new-topic" autoFocus required maxLength={300} value={topic} onChange={event => setTopic(event.target.value)} />
          </FormGroup>
          <FormGroup label="Objective" labelFor="new-objective">
            <TextArea id="new-objective" required maxLength={2000} rows={2} fill value={objective} onChange={event => setObjective(event.target.value)} />
          </FormGroup>
          <div className="fields">
            <FormGroup label="Mode" labelFor="new-mode">
              <HTMLSelect
                id="new-mode" value={period.mode} onChange={event => changeMode(event.target.value)}
                options={[{ value: 'live', label: 'Live, continuous' }, { value: 'historical', label: 'Historical, date window' }]}
              />
            </FormGroup>
            {period.mode === 'historical' && <>
              <FormGroup label="Start date (UTC)" labelFor="new-start">
                <InputGroup
                  id="new-start" type="date" required value={period.start_date} max={period.end_date || today}
                  onChange={event => setPeriod({ ...period, start_date: event.target.value })}
                />
              </FormGroup>
              <FormGroup label="End date (UTC)" labelFor="new-end">
                <InputGroup
                  id="new-end" type="date" required value={period.end_date} min={period.start_date || undefined} max={today}
                  onChange={event => setPeriod({ ...period, end_date: event.target.value })}
                />
              </FormGroup>
            </>}
          </div>
          <p className="muted">
            {period.mode === 'live'
              ? `Checks feeds about every ${state.collection_interval_seconds} s until paused.`
              : `Runs once over articles published on or between both dates, then completes. ${historicalCoverage}`}
          </p>

          <FormGroup label="Setup" className="setup-choice">
            <SegmentedControl
              role="radiogroup" value={setup}
              onValueChange={value => setSetup(value === 'manual' ? 'manual' : 'codex')}
              options={[
                { value: 'codex', label: 'Codex plan', disabled: !state.codex_available },
                { value: 'manual', label: 'Manual rules' },
              ]}
            />
          </FormGroup>

          {setup === 'codex' ? (
            <p className="muted">
              Codex picks rules and feeds from the topic, objective, and {period.mode === 'live' ? 'mode' : 'date window'}.
              It then runs a setup review automatically. Each round collects, reviews the decisions and feed coverage, and refines
              rules and feeds. Rounds and model runs are capped, and usage shows on the scan.
              {!state.codex_available && ' Codex CLI is unavailable on this server.'}
              {state.codex_available && codexBusy && ' Codex is busy with another setup, review, or audit. Try again when it finishes.'}
            </p>
          ) : (
            <div className="manual">
              <RuleFields prefix="new" value={rules} onChange={setRules} />
              <p className="help">Uses no model. Start a setup review on the scan later if you want Codex to refine it.</p>
            </div>
          )}

          <div className="toolbar">
            <Button icon="search" loading={discovering} disabled={busy || !topic.trim()} onClick={() => void discover()}>Discover feeds</Button>
            {!current && <span className="muted">Optional. Suggests feeds and preview headlines for the topic{period.mode === 'historical' ? ' and window' : ''}.</span>}
          </div>
          {current && (
            <div className="discovery">
              {current.result.error && <Callout intent="warning">Preview failed: {current.result.error}</Callout>}
              {current.result.feeds.length > 0
                ? current.result.feeds.map(feed => (
                  <Checkbox
                    key={feed.url} checked={current.selected.has(feed.url)}
                    onChange={event => toggleFeed(feed.url, event.currentTarget.checked)}
                    labelElement={<>{feed.name} <span className="muted break">{feed.url}</span></>}
                  />
                ))
                : <p className="muted">No feeds found for this topic.</p>}
              {current.result.headlines.length > 0 && (
                <details>
                  <summary>{current.result.headlines.length} preview headlines</summary>
                  <ul className="headlines">
                    {current.result.headlines.map((headline, index) => (
                      <li key={`${headline.url}-${index}`}>
                        <SourceLink url={headline.url}>{headline.title}</SourceLink> <span className="muted">{headline.source}</span>
                      </li>
                    ))}
                  </ul>
                </details>
              )}
            </div>
          )}
          <FormGroup
            label="Custom RSS or Atom feed URLs, one per line" labelFor="new-feeds"
            helperText={setup === 'codex'
              ? 'Optional. Codex sets up rules for these feeds. With none selected, it uses the topic search.'
              : `Optional. With none selected, the scan uses the Google News search for the topic${period.mode === 'historical' ? ', dated to the window' : ''}.`}
          >
            <TextArea id="new-feeds" rows={2} fill value={customFeeds} onChange={event => setCustomFeeds(event.target.value)} />
          </FormGroup>
          {error && <Callout intent="danger" role="alert">{error}</Callout>}
        </div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button onClick={onClose} disabled={busy}>Cancel</Button>
            <Button type="submit" intent="primary" loading={busy} disabled={discovering || codexBlocked}>
              {setup === 'codex' ? 'Create with Codex' : 'Create with rules'}
            </Button>
          </div>
        </div>
      </form>
    </Dialog>
  );
}
