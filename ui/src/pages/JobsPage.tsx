import { useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { api, type ScanJob } from '../api';
import StatusBadge from '../components/StatusBadge';
import ErrorCard from '../components/ErrorCard';
import Spinner from '../components/Spinner';
import { CropIcon, DownloadIcon, EyeIcon, RecoverIcon, ResumeIcon, RetryIcon, TrashIcon } from '../components/Icon';
import { TagEditor, TagFilterBar, useTagsMutation } from '../components/Tags';
import { duration, formatTimestamp } from '../lib/format';

const PAGE_SIZE = 25;

function isLive(status: string) {
  const s = status.toLowerCase();
  return s === 'pending' || s === 'queued' || s === 'running' || s === 'scanning' || s === 'processing' || s === 'ocr';
}

function JobRow({
  job,
  onDelete,
  onRecover,
  recovering,
  onResume,
  resuming,
  onRetry,
  retrying,
  onTagClick,
}: {
  job: ScanJob;
  onDelete: (id: string) => void;
  onRecover: (id: string) => void;
  recovering: boolean;
  onResume: (id: string) => void;
  resuming: boolean;
  onRetry: (id: string) => void;
  retrying: boolean;
  onTagClick: (tag: string) => void;
}) {
  const tagsMut = useTagsMutation(job.id);
  const tags = job.tags ?? [];
  const live = isLive(job.status);
  const progress =
    job.ocr_batch_count && job.ocr_batches_completed != null
      ? `${job.ocr_batches_completed}/${job.ocr_batch_count} OCR batches`
      : job.stage_detail ?? job.stage ?? '';

  return (
    <tr className="border-t border-slate-800 align-top">
      <td className="px-3 py-2 font-mono text-xs text-slate-300">
        <div>{job.id.slice(0, 8)}</div>
        <div className="text-slate-500">{formatTimestamp(job.created_at)}</div>
        {job.created_via && (
          <div className="text-[10px] text-slate-500">↪ {job.created_via}</div>
        )}
      </td>
      <td className="px-3 py-2 text-sm text-slate-200">{job.scanner}</td>
      <td className="px-3 py-2">
        <TagEditor
          tags={tags}
          pending={tagsMut.isPending}
          onChange={(next) => tagsMut.mutate(next)}
          onTagClick={onTagClick}
        />
      </td>
      <td className="px-3 py-2">
        <div className="flex flex-col gap-1">
          <StatusBadge status={job.status} />
          {live && <Spinner label={progress || job.stage || 'working'} />}
          {!live && progress && (
            <div className="text-xs text-slate-500">{progress}</div>
          )}
          {job.error && <div className="text-xs text-rose-300">{job.error}</div>}
        </div>
      </td>
      <td className="px-3 py-2 text-sm text-slate-300">
        {job.number_of_pages ?? '—'}
      </td>
      <td className="px-3 py-2 text-sm text-slate-300">
        {duration(job.duration_seconds)}
      </td>
      <td className="px-3 py-2">
        <div className="flex flex-wrap items-center gap-1">
          {job.status === 'completed' && (
            <>
              <a
                className="icon-btn-primary"
                href={api.viewUrl(job.id)}
                target="_blank"
                rel="noreferrer"
                title="Preview PDF in a new tab"
                aria-label="Preview PDF"
              >
                <EyeIcon />
              </a>
              <a
                className="icon-btn"
                href={api.downloadUrl(job.id)}
                title="Download PDF"
                aria-label="Download PDF"
              >
                <DownloadIcon />
              </a>
              <Link
                className="icon-btn"
                to={`/jobs/${job.id}/crop`}
                title="Crop this scan"
                aria-label="Crop"
              >
                <CropIcon />
              </Link>
            </>
          )}
          {job.status === 'failed' &&
            (job.scanner === '__crop__' || job.scanner === '__composite__') && (
              <button
                className="icon-btn"
                onClick={() => onRetry(job.id)}
                disabled={retrying}
                title={`Retry this ${
                  job.scanner === '__crop__' ? 'crop' : 'composite'
                } with the same parameters (new job)`}
                aria-label={`Retry ${job.scanner === '__crop__' ? 'crop' : 'composite'}`}
              >
                <RetryIcon />
              </button>
            )}
          {job.status === 'failed' && job.recovery_available && (
            <>
              <button
                className="icon-btn"
                onClick={() => onRecover(job.id)}
                disabled={recovering}
                title="Recover this scan from its leftover raw pages"
                aria-label="Recover scan"
              >
                <RecoverIcon />
              </button>
              <button
                className="icon-btn"
                onClick={() => onResume(job.id)}
                disabled={resuming}
                title="Resume: recover partial, then scan remaining pages and auto-merge"
                aria-label="Resume scan"
              >
                <ResumeIcon />
              </button>
            </>
          )}
          <button
            className="icon-btn-danger"
            onClick={() => onDelete(job.id)}
            title="Delete job"
            aria-label="Delete job"
          >
            <TrashIcon />
          </button>
        </div>
      </td>
    </tr>
  );
}

export default function JobsPage() {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [activeTags, setActiveTags] = useState<string[]>([]);

  const setActiveTagsAndReset = (tags: string[]) => {
    setActiveTags(tags);
    setPage(1);
  };

  const jobsQ = useQuery({
    queryKey: ['jobs', page, activeTags],
    queryFn: () => api.listJobs(page, PAGE_SIZE, activeTags),
    refetchInterval: (query) => {
      const items = query.state.data?.items ?? [];
      return items.some((j) => isLive(j.status)) ? 1500 : 8000;
    },
  });

  const del = useMutation({
    mutationFn: (id: string) => api.deleteScan(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['jobs'] }),
  });

  const recover = useMutation({
    mutationFn: (id: string) => api.recoverScan(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['jobs'] }),
  });

  const [resumeTarget, setResumeTarget] = useState<string | null>(null);
  const resume = useMutation({
    mutationFn: (id: string) => api.resumeScan(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['jobs'] });
      setResumeTarget(null);
    },
  });

  const retry = useMutation({
    mutationFn: (id: string) => api.retryScan(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['jobs'] }),
  });

  const pageCount = useMemo(() => {
    if (!jobsQ.data) return 1;
    return Math.max(1, Math.ceil(jobsQ.data.total / jobsQ.data.page_size));
  }, [jobsQ.data]);

  return (
    <section className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-slate-100">Jobs</h1>
          <p className="text-sm text-slate-400">
            {jobsQ.data ? `${jobsQ.data.total} total` : 'Loading…'}
          </p>
        </div>
        <button className="btn-ghost" onClick={() => jobsQ.refetch()} disabled={jobsQ.isFetching}>
          {jobsQ.isFetching ? 'Refreshing…' : 'Refresh'}
        </button>
      </header>

      {jobsQ.error && <ErrorCard error={jobsQ.error} />}
      {del.error && <ErrorCard error={del.error} />}

      <TagFilterBar activeTags={activeTags} onChange={setActiveTagsAndReset} />

      <div className="card overflow-x-auto p-0">
        <table className="w-full text-left text-sm">
          <thead className="bg-slate-900 text-xs uppercase tracking-wide text-slate-400">
            <tr>
              <th className="px-3 py-2 font-medium">Job</th>
              <th className="px-3 py-2 font-medium">Scanner</th>
              <th className="px-3 py-2 font-medium">Tags</th>
              <th className="px-3 py-2 font-medium">Status</th>
              <th className="px-3 py-2 font-medium">Pages</th>
              <th className="px-3 py-2 font-medium">Duration</th>
              <th className="px-3 py-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {jobsQ.isLoading && (
              <tr>
                <td colSpan={7} className="px-3 py-6">
                  <Spinner />
                </td>
              </tr>
            )}
            {jobsQ.data?.items.length === 0 && (
              <tr>
                <td colSpan={7} className="px-3 py-6 text-center text-slate-500">
                  {activeTags.length > 0
                    ? `No jobs match all of: ${activeTags.join(', ')}`
                    : 'No jobs yet.'}
                </td>
              </tr>
            )}
            {jobsQ.data?.items.map((j) => (
              <JobRow
                key={j.id}
                job={j}
                onDelete={(id) => del.mutate(id)}
                onRecover={(id) => recover.mutate(id)}
                recovering={recover.isPending && recover.variables === j.id}
                onResume={(id) => setResumeTarget(id)}
                resuming={resume.isPending && resume.variables === j.id}
                onRetry={(id) => retry.mutate(id)}
                retrying={retry.isPending && retry.variables === j.id}
                onTagClick={(tag) => {
                  if (activeTags.includes(tag)) return;
                  setActiveTagsAndReset([...activeTags, tag]);
                }}
              />
            ))}
          </tbody>
        </table>
      </div>

      {jobsQ.data && pageCount > 1 && (
        <div className="flex items-center justify-end gap-2 text-sm text-slate-300">
          <button
            className="btn-ghost"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
          >
            ← Prev
          </button>
          <span>
            page {page} / {pageCount}
          </span>
          <button
            className="btn-ghost"
            onClick={() => setPage((p) => Math.min(pageCount, p + 1))}
            disabled={page >= pageCount}
          >
            Next →
          </button>
        </div>
      )}

      {resumeTarget && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 p-4"
          onClick={() => !resume.isPending && setResumeTarget(null)}
        >
          <div
            className="max-w-md space-y-4 rounded-lg border border-slate-800 bg-slate-900 p-6 shadow-xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div>
              <h2 className="text-lg font-semibold text-slate-100">Resume scan</h2>
              <p className="mt-1 text-sm text-slate-400">
                We'll stitch the pages already captured into a partial PDF, then
                start a fresh scan with the same settings. Once the new scan
                finishes, the two will be auto-merged into a single combined PDF.
              </p>
              <p className="mt-2 text-sm text-amber-300">
                Load the remaining pages into the ADF (or place the next sheet on the
                flatbed) before continuing.
              </p>
            </div>
            {resume.error && <ErrorCard error={resume.error} />}
            <div className="flex justify-end gap-2">
              <button
                className="btn-ghost"
                onClick={() => setResumeTarget(null)}
                disabled={resume.isPending}
              >
                Cancel
              </button>
              <button
                className="btn-primary"
                onClick={() => resume.mutate(resumeTarget)}
                disabled={resume.isPending}
              >
                {resume.isPending ? 'Resuming…' : 'Continue'}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
