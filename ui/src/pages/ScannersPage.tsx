import StatusBadge, { statusTone } from '../components/StatusBadge';
import ErrorCard from '../components/ErrorCard';
import Spinner from '../components/Spinner';
import { useScannersQuery } from '../hooks/useScanners';

export default function ScannersPage() {
  const { query, refresh } = useScannersQuery();
  const { data, error, isLoading } = query;

  return (
    <section className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-slate-100">Scanners</h1>
          <p className="text-sm text-slate-400">
            Devices configured in <code className="text-brand-400">/etc/page-the-ripper/scanner.cfg</code>
          </p>
        </div>
        <button
          className="btn-ghost"
          onClick={() => refresh.mutate()}
          disabled={refresh.isPending}
          title="Re-probe USB and network scanners (slow)"
        >
          {refresh.isPending ? 'Re-probing…' : 'Refresh'}
        </button>
      </header>

      {isLoading && <Spinner />}
      {error && <ErrorCard error={error} />}

      {data && (
        <div className="grid gap-3 sm:grid-cols-2">
          {data.length === 0 && (
            <div className="card text-sm text-slate-400">No scanners configured.</div>
          )}
          {data.map((s) => (
            <div key={s.id} className="card space-y-2">
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="font-medium text-slate-100">{s.label}</div>
                  <div className="text-xs text-slate-500">id: {s.id}</div>
                </div>
                <StatusBadge status={s.backend_status} tone={statusTone(s.backend_status)} />
              </div>
              <dl className="grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1 text-xs">
                <dt className="text-slate-500">Backend</dt>
                <dd className="text-slate-200">{s.backend}</dd>
                <dt className="text-slate-500">Device</dt>
                <dd className="font-mono text-slate-300">{s.configured_device ?? '—'}</dd>
                <dt className="text-slate-500">In use</dt>
                <dd className="text-slate-300">{s.in_use ? 'yes' : 'no'}</dd>
              </dl>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
