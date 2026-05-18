import { useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { api, type ScanJob } from '../api';
import ErrorCard from '../components/ErrorCard';
import Spinner from '../components/Spinner';
import { EyeIcon } from '../components/Icon';
import { TagChip, TagFilterBar } from '../components/Tags';
import { formatTimestamp } from '../lib/format';

const PAGE_SIZE = 100;

function JobLabel({
  job,
  onTagClick,
}: {
  job: ScanJob;
  onTagClick: (tag: string) => void;
}) {
  const tags = job.tags ?? [];
  return (
    <div className="min-w-0 space-y-0.5">
      <div className="truncate font-mono text-xs text-slate-300">{job.id.slice(0, 12)}</div>
      <div className="truncate text-xs text-slate-500">
        {job.scanner} · {job.number_of_pages ?? '?'} pages · {formatTimestamp(job.created_at)}
      </div>
      {tags.length > 0 && (
        <div className="flex flex-wrap gap-1 pt-0.5">
          {tags.map((t) => (
            <TagChip key={t} tag={t} onClick={onTagClick} />
          ))}
        </div>
      )}
    </div>
  );
}

export default function CompositePage() {
  const qc = useQueryClient();
  const navigate = useNavigate();

  const [activeTags, setActiveTags] = useState<string[]>([]);

  const jobsQ = useQuery({
    queryKey: ['jobs', 'composite-picker', activeTags],
    queryFn: () => api.listJobs(1, PAGE_SIZE, activeTags),
  });

  const [order, setOrder] = useState<string[]>([]);
  // Default to NOT re-running OCR. ocrmypdf reprocesses every page and can
  // multiply the output file size; the source PDFs already have their own
  // OCR text layers that concatenate fine without a re-pass.
  const [reocr, setReocr] = useState(false);

  const addTagFilter = (tag: string) => {
    if (activeTags.includes(tag)) return;
    setActiveTags([...activeTags, tag]);
  };

  const completedJobs = useMemo<ScanJob[]>(
    () => (jobsQ.data?.items ?? []).filter((j) => j.status === 'completed'),
    [jobsQ.data],
  );

  const jobById = useMemo(() => {
    const m = new Map<string, ScanJob>();
    for (const j of completedJobs) m.set(j.id, j);
    return m;
  }, [completedJobs]);

  const orderedJobs = useMemo(
    () => order.map((id) => jobById.get(id)).filter((j): j is ScanJob => Boolean(j)),
    [order, jobById],
  );

  const toggle = (id: string) => {
    setOrder((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  };

  const move = (id: string, delta: number) => {
    setOrder((prev) => {
      const idx = prev.indexOf(id);
      if (idx < 0) return prev;
      const next = prev.slice();
      const target = idx + delta;
      if (target < 0 || target >= next.length) return prev;
      [next[idx], next[target]] = [next[target], next[idx]];
      return next;
    });
  };

  const submit = useMutation({
    mutationFn: () => api.createComposite({ sources: order, reocr }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['jobs'] });
      navigate('/jobs');
    },
  });

  return (
    <section className="space-y-4">
      <header>
        <h1 className="text-lg font-semibold text-slate-100">Composite PDF</h1>
        <p className="text-sm text-slate-400">
          Pick two or more completed scans, set the order, and merge them into a new PDF.
        </p>
      </header>

      <TagFilterBar activeTags={activeTags} onChange={setActiveTags} />

      {jobsQ.isLoading && <Spinner />}
      {jobsQ.error && <ErrorCard error={jobsQ.error} />}

      {jobsQ.data && (
        <div className="grid gap-4 lg:grid-cols-2">
          <div className="card space-y-2">
            <div className="flex items-center justify-between">
              <div className="font-medium text-slate-100">Available scans</div>
              <div className="text-xs text-slate-500">{completedJobs.length} completed</div>
            </div>
            <div className="max-h-[60vh] space-y-1 overflow-y-auto">
              {completedJobs.length === 0 && (
                <div className="text-sm text-slate-500">
                  {activeTags.length > 0
                    ? `No completed scans match all of: ${activeTags.join(', ')}`
                    : 'No completed scans available.'}
                </div>
              )}
              {completedJobs.map((j) => {
                const checked = order.includes(j.id);
                return (
                  <label
                    key={j.id}
                    className={`flex cursor-pointer items-center gap-3 rounded-md border px-2 py-1.5 ${
                      checked
                        ? 'border-brand-500/40 bg-brand-500/10'
                        : 'border-slate-800 hover:bg-slate-800/40'
                    }`}
                  >
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border-slate-700 bg-slate-950 text-brand-500 focus:ring-brand-500"
                      checked={checked}
                      onChange={() => toggle(j.id)}
                    />
                    <JobLabel job={j} onTagClick={addTagFilter} />
                  </label>
                );
              })}
            </div>
          </div>

          <div className="card space-y-3">
            <div className="flex items-center justify-between">
              <div className="font-medium text-slate-100">Output order</div>
              <div className="text-xs text-slate-500">{orderedJobs.length} selected</div>
            </div>

            <ol className="space-y-1">
              {orderedJobs.length === 0 && (
                <li className="text-sm text-slate-500">Pick scans on the left to add them here.</li>
              )}
              {orderedJobs.map((j, i) => (
                <li
                  key={j.id}
                  className="flex items-center gap-2 rounded-md border border-slate-800 bg-slate-950 px-2 py-1.5"
                >
                  <span className="w-5 text-center font-mono text-xs text-slate-500">{i + 1}.</span>
                  <JobLabel job={j} onTagClick={addTagFilter} />
                  <div className="ml-auto flex gap-1">
                    <a
                      className="btn-ghost"
                      href={api.viewUrl(j.id)}
                      target="_blank"
                      rel="noreferrer"
                      title="Preview PDF in a new tab"
                      aria-label="Preview PDF"
                    >
                      <EyeIcon />
                    </a>
                    <button
                      type="button"
                      className="btn-ghost"
                      disabled={i === 0}
                      onClick={() => move(j.id, -1)}
                      title="Move up"
                    >
                      ↑
                    </button>
                    <button
                      type="button"
                      className="btn-ghost"
                      disabled={i === orderedJobs.length - 1}
                      onClick={() => move(j.id, 1)}
                      title="Move down"
                    >
                      ↓
                    </button>
                    <button
                      type="button"
                      className="btn-danger"
                      onClick={() => toggle(j.id)}
                      title="Remove"
                    >
                      ✕
                    </button>
                  </div>
                </li>
              ))}
            </ol>

            <label className="flex items-center gap-2 text-sm text-slate-200">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-slate-700 bg-slate-950 text-brand-500 focus:ring-brand-500"
                checked={reocr}
                onChange={(e) => setReocr(e.target.checked)}
              />
              Re-run OCR on the merged PDF
            </label>

            <button
              type="button"
              className="btn-primary w-full justify-center"
              disabled={order.length < 1 || submit.isPending}
              onClick={() => submit.mutate()}
            >
              {submit.isPending
                ? 'Submitting…'
                : `Merge ${order.length} ${order.length === 1 ? 'scan' : 'scans'} into one PDF`}
            </button>

            {submit.error && <ErrorCard error={submit.error} />}
          </div>
        </div>
      )}
    </section>
  );
}
