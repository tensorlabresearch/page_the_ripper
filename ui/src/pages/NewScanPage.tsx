import { useEffect, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { api, type ScanRequest } from '../api';
import ErrorCard from '../components/ErrorCard';
import Spinner from '../components/Spinner';
import { useScannersQuery } from '../hooks/useScanners';

export default function NewScanPage() {
  const navigate = useNavigate();
  const qc = useQueryClient();

  const { query: scannersQ, refresh: refreshScanners } = useScannersQuery();

  const [scanner, setScanner] = useState('');
  // Default to 92 DPI — typed text still reads cleanly at this density and
  // file sizes stay reasonable. 72 was tested and looked too soft.
  const [dpi, setDpi] = useState<string>('92');
  const [color, setColor] = useState(false);

  const DPI_PRESETS: { label: string; value: number; hint: string }[] = [
    { label: 'Text', value: 92, hint: 'Best for typed documents (small files, still legible)' },
    { label: 'Cover', value: 100, hint: 'For book covers / mixed text + image' },
    { label: 'HQ', value: 300, hint: 'High quality for OCR or fine detail (largest files)' },
  ];

  useEffect(() => {
    if (!scanner && scannersQ.data && scannersQ.data.length > 0) {
      setScanner(scannersQ.data[0].id);
    }
  }, [scannersQ.data, scanner]);

  const submit = useMutation({
    mutationFn: (req: ScanRequest) => api.createScan(req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['jobs'] });
      navigate('/jobs');
    },
  });

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!scanner) return;
    const req: ScanRequest = { scanner };
    if (dpi) req.dpi = Number(dpi);
    if (color) req.color = true;
    submit.mutate(req);
  };

  return (
    <section className="grid gap-6 lg:grid-cols-[1fr_auto]">
      <form onSubmit={onSubmit} className="card max-w-lg space-y-4">
        <div>
          <h1 className="text-lg font-semibold text-slate-100">New scan</h1>
          <p className="text-sm text-slate-400">Pick a scanner and start a job.</p>
        </div>

        <label className="field">
          <span>Scanner</span>
          {scannersQ.isLoading ? (
            <Spinner />
          ) : scannersQ.error ? (
            <ErrorCard error={scannersQ.error} />
          ) : (
            <select
              className="input"
              value={scanner}
              onChange={(e) => setScanner(e.target.value)}
              required
            >
              {scannersQ.data?.length === 0 && <option value="">No scanners configured</option>}
              {scannersQ.data?.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.label} ({s.backend} · {s.backend_status})
                </option>
              ))}
            </select>
          )}
        </label>

        <div className="field">
          <span>DPI</span>
          <div className="grid grid-cols-3 gap-1">
            {DPI_PRESETS.map((p) => {
              const active = Number(dpi) === p.value;
              return (
                <button
                  key={p.value}
                  type="button"
                  className={
                    active
                      ? 'btn-primary justify-center text-xs'
                      : 'btn-ghost justify-center text-xs'
                  }
                  onClick={() => setDpi(String(p.value))}
                  title={p.hint}
                >
                  {p.label} · {p.value}
                </button>
              );
            })}
          </div>
          <input
            type="number"
            className="input mt-1"
            min={50}
            max={1200}
            step={25}
            placeholder="custom"
            value={dpi}
            onChange={(e) => setDpi(e.target.value)}
          />
        </div>

        <label className="flex items-center gap-2 text-sm text-slate-300">
          <input
            type="checkbox"
            className="h-4 w-4 rounded border-slate-700 bg-slate-950 text-brand-500 focus:ring-brand-500"
            checked={color}
            onChange={(e) => setColor(e.target.checked)}
          />
          Scan in color (RGB24)
        </label>

        <div className="flex items-center gap-2">
          <button
            type="submit"
            className="btn-primary"
            disabled={!scanner || submit.isPending}
          >
            {submit.isPending ? 'Starting…' : 'Start scan'}
          </button>
          <button
            type="button"
            className="btn-ghost"
            onClick={() => refreshScanners.mutate()}
            disabled={refreshScanners.isPending}
            title="Re-probe USB and network scanners (slow)"
          >
            {refreshScanners.isPending ? 'Re-probing…' : 'Refresh scanners'}
          </button>
        </div>

        {submit.error && <ErrorCard error={submit.error} />}
      </form>

      <aside className="card max-w-sm space-y-2 text-sm text-slate-300">
        <div className="font-medium text-slate-100">Tips</div>
        <ul className="list-disc space-y-1 pl-4 text-slate-400">
          <li>Default DPI comes from the scanner's section in <code>scanner.cfg</code>.</li>
          <li>Color jobs are larger; grayscale is the default for OCR.</li>
          <li>Jobs run in a background queue — you can submit multiple at once.</li>
        </ul>
      </aside>
    </section>
  );
}
