import { useState } from 'react';
import type { ReactNode } from 'react';
import { Button, FormGroup, InputGroup, NumericInput } from '@blueprintjs/core';
import { safeHref } from './api';

export function SourceLink({ url, children }: { url: string; children?: ReactNode }) {
  const href = safeHref(url);
  const label = children ?? url;
  return href
    ? <a href={href} target="_blank" rel="noopener noreferrer">{label}</a>
    : <span>{label}</span>;
}

export function Json({ value }: { value: unknown }) {
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>;
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="empty">{children}</p>;
}

export function MinutesField({ id, label, value, onChange }: {
  id: string; label: string; value: number; onChange: (value: number) => void;
}) {
  return (
    <FormGroup label={label} labelFor={id} helperText="Whole minutes, 1 to 10080.">
      <NumericInput
        id={id} min={1} max={10080} stepSize={1} minorStepSize={null} value={Number.isNaN(value) ? '' : value}
        onValueChange={onChange} inputMode="numeric" clampValueOnBlur
      />
    </FormGroup>
  );
}

export type RuleText = { any: string; all: string; exclude: string };

const ruleFields: [keyof RuleText, string][] = [
  ['any', 'Match any'],
  ['all', 'Require all'],
  ['exclude', 'Exclude'],
];

export function RuleFields({ prefix, value, onChange }: {
  prefix: string; value: RuleText; onChange: (value: RuleText) => void;
}) {
  return (
    <>
      <div className="fields">
        {ruleFields.map(([key, label]) => (
          <FormGroup key={key} label={label} labelFor={`${prefix}-${key}`}>
            <InputGroup
              id={`${prefix}-${key}`} value={value[key]}
              onChange={event => onChange({ ...value, [key]: event.target.value })}
            />
          </FormGroup>
        ))}
      </div>
      <p className="help">Comma separated. Case-insensitive match on title and excerpt. Empty match lists keep everything not excluded.</p>
    </>
  );
}

export function Pagination({ page, total, size, onChange }: {
  page: number; total: number; size: number; onChange: (page: number) => void;
}) {
  if (total <= size) return null;
  return (
    <nav className="pagination" aria-label="Pagination">
      <Button size="small" icon="chevron-left" disabled={page === 0} onClick={() => onChange(page - 1)}>Previous</Button>
      <span>{page * size + 1} to {Math.min((page + 1) * size, total)} of {total}</span>
      <Button size="small" endIcon="chevron-right" disabled={(page + 1) * size >= total} onClick={() => onChange(page + 1)}>Next</Button>
    </nav>
  );
}

/** Clamp a page index when filters shrink the list underneath it. */
export function pageSlice<T>(items: T[], page: number, size: number): { page: number; items: T[] } {
  const last = Math.max(0, Math.ceil(items.length / size) - 1);
  const current = Math.min(page, last);
  return { page: current, items: items.slice(current * size, (current + 1) * size) };
}

/** Expand state for table rows keyed by a stable string. */
export function useExpanded() {
  const [open, setOpen] = useState<ReadonlySet<string>>(() => new Set());
  return {
    isOpen: (key: string) => open.has(key),
    toggle: (key: string) => setOpen(current => {
      const next = new Set(current);
      if (!next.delete(key)) next.add(key);
      return next;
    }),
  };
}

export function ExpandButton({ open, controls, label, onClick }: {
  open: boolean; controls: string; label: string; onClick: () => void;
}) {
  return (
    <Button
      variant="minimal" size="small" icon={open ? 'chevron-down' : 'chevron-right'}
      aria-expanded={open} aria-controls={controls} aria-label={`${open ? 'Collapse' : 'Expand'} ${label}`}
      onClick={onClick}
    />
  );
}

/**
 * Edit buffer that survives background refreshes. While the draft is unchanged it
 * follows the saved value. Once edited, it holds until saved or reloaded, and
 * `stale` reports that the saved value moved underneath it.
 */
export function useDraft<T>(saved: T) {
  const [baseline, setBaseline] = useState(saved);
  const [draft, setDraft] = useState(saved);
  const dirty = !same(draft, baseline);
  const stale = !same(saved, baseline);
  if (stale && !dirty) {
    setBaseline(saved);
    setDraft(saved);
  }
  return {
    draft, setDraft, dirty, stale, baseline,
    reload: () => { setBaseline(saved); setDraft(saved); },
    markSaved: (value: T) => setBaseline(value),
  };
}

function same(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
