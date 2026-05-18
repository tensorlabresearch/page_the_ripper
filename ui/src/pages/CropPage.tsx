import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useNavigate, useParams, Link } from 'react-router-dom';
import ReactCrop, { type PercentCrop } from 'react-image-crop';
import 'react-image-crop/dist/ReactCrop.css';
import { api, type CropRequest } from '../api';
import ErrorCard from '../components/ErrorCard';
import Spinner from '../components/Spinner';
import { rotateImageToDataUrl } from '../lib/rotateImage';

type LockMode = 'free' | 'page';

export default function CropPage() {
  const { jobId = '' } = useParams<{ jobId: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const pagesQ = useQuery({
    queryKey: ['pages', jobId],
    queryFn: () => api.listPages(jobId),
    enabled: Boolean(jobId),
  });

  const [pageIndex, setPageIndex] = useState(0);
  const [reocr, setReocr] = useState(true);
  const [lock, setLock] = useState<LockMode>('free');
  const [crop, setCrop] = useState<PercentCrop | undefined>();
  const [imgDims, setImgDims] = useState<{ w: number; h: number } | null>(null);
  const [rotation, setRotation] = useState(0);
  // DPI controls the rasterization quality the crop worker uses on each page.
  // Default 92 mirrors the New Scan default; lower than 92 looks too soft.
  const [dpi, setDpi] = useState<number>(92);

  const DPI_PRESETS: { label: string; value: number; hint: string }[] = [
    { label: 'Text', value: 92, hint: 'Small files; best for typed pages' },
    { label: 'Cover', value: 100, hint: 'Mixed text + image' },
    { label: 'HQ', value: 300, hint: 'High quality for OCR or fine detail' },
  ];
  const [displaySrc, setDisplaySrc] = useState<string>('');
  const [rotating, setRotating] = useState(false);

  // Cap the natural preview image at 400px wide so it fits on the smallest
  // mobile viewport even if some ancestor's sizing cascade ends up resolving
  // to the image's intrinsic width. The cropper itself is still usable at
  // this size; ReactCrop reports the crop in percent so resolution doesn't
  // matter for the backend.
  const previewSrc = useMemo(
    () => (jobId ? api.pagePreviewUrl(jobId, pageIndex, 400) : ''),
    [jobId, pageIndex],
  );

  // Re-render the displayed image whenever the source page or rotation changes.
  useEffect(() => {
    if (!previewSrc) {
      setDisplaySrc('');
      return;
    }
    let cancelled = false;
    setRotating(true);
    rotateImageToDataUrl(previewSrc, rotation)
      .then((url) => {
        if (!cancelled) setDisplaySrc(url);
      })
      .catch(() => {
        if (!cancelled) setDisplaySrc(previewSrc);
      })
      .finally(() => {
        if (!cancelled) setRotating(false);
      });
    return () => {
      cancelled = true;
    };
  }, [previewSrc, rotation]);

  const firstPage = pagesQ.data?.pages[0];
  // Aspect lock follows the *displayed* image's own aspect, which already
  // reflects any rotation. In ReactCrop's percent coordinate system, locking
  // to the image's own aspect collapses to crop_w_pct == crop_h_pct.
  const aspect = lock === 'page' && imgDims ? imgDims.w / imgDims.h : undefined;

  // Reset the crop region when the image loads or the aspect lock changes.
  useEffect(() => {
    if (!imgDims) return;
    const margin = 5; // percent
    const baseWidth = 100 - margin * 2;
    setCrop({
      unit: '%',
      x: margin,
      y: margin,
      width: baseWidth,
      height: aspect ? Math.min(100 - margin, baseWidth) : baseWidth,
    });
  }, [imgDims, aspect, firstPage]);

  const normalizedBox = useMemo<[number, number, number, number] | null>(() => {
    if (!crop) return null;
    const x0 = clamp01(crop.x / 100);
    const y0 = clamp01(crop.y / 100);
    const x1 = clamp01((crop.x + crop.width) / 100);
    const y1 = clamp01((crop.y + crop.height) / 100);
    if (x1 - x0 < 0.02 || y1 - y0 < 0.02) return null;
    return [x0, y0, x1, y1];
  }, [crop]);

  const submit = useMutation({
    mutationFn: (req: CropRequest) => api.createCrop(jobId, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['jobs'] });
      navigate('/jobs');
    },
  });

  if (!jobId) {
    return <ErrorCard error="Missing job ID" />;
  }

  return (
    <section className="space-y-4">
      <header className="flex flex-wrap items-baseline justify-between gap-2">
        <div>
          <h1 className="text-lg font-semibold text-slate-100">Crop scan</h1>
          <p className="text-sm text-slate-400">
            Drag the handles on the rectangle to resize. The same crop is applied to every page.
          </p>
        </div>
        <Link to="/jobs" className="text-xs text-slate-400 hover:text-brand-400">
          ← Back to jobs
        </Link>
      </header>

      {pagesQ.isLoading && <Spinner />}
      {pagesQ.error && <ErrorCard error={pagesQ.error} />}

      {pagesQ.data && (
        <div className="space-y-4 lg:pr-[340px]">
          <div className="card overflow-hidden p-0" style={{ maxWidth: '100%' }}>
            <div
              className="flex min-h-[280px] items-center justify-center bg-slate-950 p-3 sm:min-h-[420px]"
              style={{ maxWidth: '100%', overflow: 'hidden' }}
            >
              {displaySrc ? (
                <div
                  className="mx-auto"
                  style={{ width: 'min(100% , calc(100vw - 4rem))', maxWidth: '640px' }}
                >
                  <ReactCrop
                    crop={crop}
                    onChange={(_pixel, percent) => setCrop(percent)}
                    aspect={aspect}
                    keepSelection
                    ruleOfThirds
                    style={{ display: 'block', width: '100%', maxWidth: '100%' }}
                  >
                    <img
                      src={displaySrc}
                      alt={`page ${pageIndex + 1}`}
                      style={{
                        display: 'block',
                        width: '100%',
                        height: 'auto',
                        maxWidth: '100%',
                      }}
                      className="select-none"
                      onLoad={(e) => {
                        const t = e.currentTarget;
                        setImgDims({ w: t.naturalWidth, h: t.naturalHeight });
                      }}
                    />
                  </ReactCrop>
                </div>
              ) : (
                <Spinner label="loading page…" />
              )}
            </div>
            <div className="flex items-center justify-between gap-2 border-t border-slate-800 px-3 py-2 text-xs text-slate-400">
              <div>
                page {pageIndex + 1} of {pagesQ.data.page_count}
                {firstPage && (
                  <>
                    {' · '}
                    {firstPage.width_pt.toFixed(0)}×{firstPage.height_pt.toFixed(0)} pt
                  </>
                )}
                {imgDims && (
                  <>
                    {' · '}
                    {imgDims.w}×{imgDims.h} px
                  </>
                )}
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  className="btn-ghost"
                  disabled={pageIndex === 0}
                  onClick={() => setPageIndex((i) => Math.max(0, i - 1))}
                >
                  ← Prev
                </button>
                <button
                  type="button"
                  className="btn-ghost"
                  disabled={pageIndex >= pagesQ.data.page_count - 1}
                  onClick={() => setPageIndex((i) => Math.min(pagesQ.data.page_count - 1, i + 1))}
                >
                  Next →
                </button>
              </div>
            </div>
          </div>

          <aside
            className="card space-y-3 text-sm"
            style={{
              position: 'fixed',
              top: '80px',
              right: '16px',
              width: '300px',
              maxHeight: 'calc(100vh - 100px)',
              overflowY: 'auto',
              zIndex: 20,
              boxShadow: '0 10px 25px rgba(0,0,0,0.5)',
            }}
          >
            <div>
              <div className="font-medium text-slate-100">Settings</div>
              <p className="mt-0.5 text-xs text-slate-500">
                Source job <span className="font-mono">{jobId.slice(0, 12)}</span>
              </p>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 sm:gap-4">
            <div className="space-y-1">
              <div className="text-xs text-slate-400">Aspect</div>
              <div className="flex gap-1">
                <button
                  type="button"
                  className={lock === 'free' ? 'btn-primary flex-1 justify-center' : 'btn-ghost flex-1 justify-center'}
                  onClick={() => setLock('free')}
                >
                  Free
                </button>
                <button
                  type="button"
                  className={lock === 'page' ? 'btn-primary flex-1 justify-center' : 'btn-ghost flex-1 justify-center'}
                  onClick={() => setLock('page')}
                  disabled={!imgDims}
                  title="Lock crop to the displayed image's aspect"
                >
                  Page
                </button>
              </div>
            </div>

            <div className="space-y-1">
              <div className="flex items-center justify-between text-xs text-slate-400">
                <span>Rotation</span>
                <span className="font-mono text-slate-300">
                  {Math.round(((rotation % 360) + 360) % 360)}°{rotating ? ' …' : ''}
                </span>
              </div>
              <div className="grid grid-cols-4 gap-1">
                {[0, 90, 180, 270].map((deg) => (
                  <button
                    key={deg}
                    type="button"
                    className={
                      ((rotation % 360) + 360) % 360 === deg
                        ? 'btn-primary justify-center'
                        : 'btn-ghost justify-center'
                    }
                    onClick={() => setRotation(deg)}
                    title={deg === 0 ? 'No rotation' : `Rotate ${deg}° clockwise`}
                  >
                    {deg}°
                  </button>
                ))}
              </div>
              <input
                type="range"
                min={0}
                max={359}
                step={1}
                value={((rotation % 360) + 360) % 360}
                onChange={(e) => setRotation(Number(e.target.value))}
                className="w-full accent-brand-500"
                aria-label="Rotation in degrees"
              />
            </div>
            </div>

            <div className="space-y-1">
              <div className="flex items-center justify-between text-xs text-slate-400">
                <span>DPI</span>
                <span className="font-mono text-slate-300">{dpi}</span>
              </div>
              <div className="grid grid-cols-3 gap-1">
                {DPI_PRESETS.map((p) => (
                  <button
                    key={p.value}
                    type="button"
                    className={
                      dpi === p.value
                        ? 'btn-primary justify-center text-xs'
                        : 'btn-ghost justify-center text-xs'
                    }
                    onClick={() => setDpi(p.value)}
                    title={p.hint}
                  >
                    {p.label} · {p.value}
                  </button>
                ))}
              </div>
              <input
                type="number"
                className="input mt-1 w-full"
                min={50}
                max={1200}
                step={25}
                value={dpi}
                onChange={(e) => {
                  const n = Number(e.target.value);
                  if (Number.isFinite(n) && n > 0) setDpi(Math.max(50, Math.min(1200, n)));
                }}
                aria-label="DPI"
              />
            </div>

            <button
              type="button"
              className="btn-ghost w-full justify-center"
              onClick={() =>
                setCrop({ unit: '%', x: 0, y: 0, width: 100, height: 100 })
              }
            >
              Reset to full page
            </button>

            <label className="flex items-center gap-2 text-slate-200">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-slate-700 bg-slate-950 text-brand-500 focus:ring-brand-500"
                checked={reocr}
                onChange={(e) => setReocr(e.target.checked)}
              />
              Re-run OCR on the cropped PDF
            </label>

            <div className="rounded-md border border-slate-800 bg-slate-950 p-2 text-xs">
              <div className="text-slate-500">Crop box (normalized)</div>
              <pre className="mt-1 font-mono text-slate-300">
                {normalizedBox
                  ? normalizedBox.map((n) => n.toFixed(3)).join(', ')
                  : 'drag inside the preview'}
              </pre>
            </div>

            <button
              type="button"
              className="btn-primary w-full justify-center"
              disabled={!normalizedBox || submit.isPending}
              onClick={() => {
                if (!normalizedBox) return;
                submit.mutate({ box: normalizedBox, reocr, rotation, dpi });
              }}
            >
              {submit.isPending ? 'Submitting…' : 'Create cropped scan'}
            </button>

            {submit.error && <ErrorCard error={submit.error} />}
          </aside>
        </div>
      )}
    </section>
  );
}

function clamp01(v: number): number {
  if (Number.isNaN(v)) return 0;
  return Math.max(0, Math.min(1, v));
}
