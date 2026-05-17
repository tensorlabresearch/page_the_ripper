import { useEffect, useId, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api } from '../api';

const ALL_TAGS_KEY = ['tags', 'all'] as const;

export function useAllTags() {
  return useQuery({
    queryKey: ALL_TAGS_KEY,
    queryFn: api.listAllTags,
    staleTime: 60_000,
  });
}

export function TagChip({
  tag,
  onClick,
  onRemove,
  selected,
}: {
  tag: string;
  onClick?: (tag: string) => void;
  onRemove?: (tag: string) => void;
  selected?: boolean;
}) {
  const base =
    'inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium transition-colors';
  const tone = selected
    ? 'bg-brand-500 text-slate-950'
    : 'bg-slate-800 text-slate-200 hover:bg-slate-700';
  return (
    <span className={`${base} ${tone}`}>
      {onClick ? (
        <button
          type="button"
          className="font-medium"
          onClick={(e) => {
            e.stopPropagation();
            onClick(tag);
          }}
          title={selected ? 'Remove from filter' : `Filter by ${tag}`}
        >
          {tag}
        </button>
      ) : (
        <span>{tag}</span>
      )}
      {onRemove && (
        <button
          type="button"
          className="leading-none opacity-70 hover:opacity-100"
          onClick={(e) => {
            e.stopPropagation();
            onRemove(tag);
          }}
          title={`Remove tag ${tag}`}
          aria-label={`Remove tag ${tag}`}
        >
          ×
        </button>
      )}
    </span>
  );
}

export function TagEditor({
  tags,
  onChange,
  onTagClick,
  pending,
}: {
  tags: string[];
  onChange: (next: string[]) => void;
  onTagClick?: (tag: string) => void;
  pending: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState('');
  const allTagsQ = useAllTags();
  const inputRef = useRef<HTMLInputElement | null>(null);
  const listId = useId();

  useEffect(() => {
    if (editing) inputRef.current?.focus();
  }, [editing]);

  const commit = (raw: string) => {
    const tag = raw.trim().toLowerCase();
    if (!tag) return;
    if (tags.includes(tag)) {
      setDraft('');
      return;
    }
    onChange([...tags, tag]);
    setDraft('');
  };

  return (
    <div className="flex flex-wrap items-center gap-1">
      {tags.map((t) => (
        <TagChip
          key={t}
          tag={t}
          onClick={onTagClick}
          onRemove={pending ? undefined : (tag) => onChange(tags.filter((x) => x !== tag))}
        />
      ))}
      {editing ? (
        <span className="inline-flex items-center gap-1">
          <input
            ref={inputRef}
            list={listId}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                commit(draft);
              } else if (e.key === 'Escape') {
                setEditing(false);
                setDraft('');
              }
            }}
            onBlur={() => {
              if (draft) commit(draft);
              setEditing(false);
            }}
            placeholder="tag"
            className="w-24 rounded-md border border-slate-700 bg-slate-950 px-2 py-0.5 text-xs text-slate-100 focus:border-brand-500 focus:outline-none"
            disabled={pending}
          />
          <datalist id={listId}>
            {(allTagsQ.data ?? [])
              .filter((t) => !tags.includes(t))
              .map((t) => (
                <option key={t} value={t} />
              ))}
          </datalist>
        </span>
      ) : (
        <button
          type="button"
          className="rounded-full border border-dashed border-slate-700 px-1.5 py-0.5 text-xs text-slate-400 hover:border-brand-500 hover:text-brand-400"
          onClick={() => setEditing(true)}
          disabled={pending}
          title="Add tag"
          aria-label="Add tag"
        >
          + tag
        </button>
      )}
    </div>
  );
}

export function useTagsMutation(jobId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (tags: string[]) => api.setJobTags(jobId, tags),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['jobs'] });
      qc.invalidateQueries({ queryKey: ALL_TAGS_KEY });
    },
  });
}

export function TagFilterBar({
  activeTags,
  onChange,
}: {
  activeTags: string[];
  onChange: (next: string[]) => void;
}) {
  const allTagsQ = useAllTags();
  const [draft, setDraft] = useState('');
  const listId = useId();

  const add = (raw: string) => {
    const tag = raw.trim().toLowerCase();
    if (!tag || activeTags.includes(tag)) return;
    onChange([...activeTags, tag]);
    setDraft('');
  };

  const known = allTagsQ.data ?? [];

  return (
    <div className="flex flex-wrap items-center gap-2 rounded-lg border border-slate-800 bg-slate-900/40 px-3 py-2">
      <span className="text-xs uppercase tracking-wide text-slate-500">filter</span>
      {activeTags.length === 0 && (
        <span className="text-xs text-slate-500">no filter — showing all jobs</span>
      )}
      {activeTags.map((t) => (
        <TagChip
          key={t}
          tag={t}
          selected
          onRemove={(tag) => onChange(activeTags.filter((x) => x !== tag))}
        />
      ))}
      <span className="ml-auto inline-flex items-center gap-1">
        <input
          list={listId}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault();
              add(draft);
            }
          }}
          placeholder={known.length ? 'add tag filter…' : 'no tags yet'}
          className="w-40 rounded-md border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100 focus:border-brand-500 focus:outline-none"
        />
        <datalist id={listId}>
          {known
            .filter((t) => !activeTags.includes(t))
            .map((t) => (
              <option key={t} value={t} />
            ))}
        </datalist>
        {activeTags.length > 0 && (
          <button
            type="button"
            className="text-xs text-slate-400 hover:text-rose-300"
            onClick={() => onChange([])}
            title="Clear all filters"
          >
            clear
          </button>
        )}
      </span>
    </div>
  );
}
