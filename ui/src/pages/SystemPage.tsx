import { useQuery } from '@tanstack/react-query';
import { api } from '../api';
import StatusBadge, { statusTone } from '../components/StatusBadge';
import ErrorCard from '../components/ErrorCard';
import Spinner from '../components/Spinner';
import { bytes, duration } from '../lib/format';

export default function SystemPage() {
  const { data, error, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['system'],
    queryFn: api.getSystem,
    refetchInterval: 15_000,
  });

  return (
    <section className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-slate-100">System</h1>
          <p className="text-sm text-slate-400">Host health and component versions.</p>
        </div>
        <button className="btn-ghost" onClick={() => refetch()} disabled={isFetching}>
          {isFetching ? 'Refreshing…' : 'Refresh'}
        </button>
      </header>

      {isLoading && <Spinner />}
      {error && <ErrorCard error={error} />}

      {data && (
        <div className="grid gap-3 md:grid-cols-2">
          <div className="card space-y-2">
            <div className="flex items-center justify-between">
              <div className="font-medium text-slate-100">Service</div>
              <StatusBadge status={data.status} tone={statusTone(data.status)} />
            </div>
            <dl className="grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1 text-xs">
              {Object.entries(data.system).map(([k, v]) => (
                <div key={k} className="contents">
                  <dt className="text-slate-500">{k}</dt>
                  <dd className="font-mono text-slate-300">{String(v)}</dd>
                </div>
              ))}
            </dl>
          </div>

          <div className="card space-y-2">
            <div className="font-medium text-slate-100">Resources</div>
            <dl className="grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1 text-xs">
              <dt className="text-slate-500">Uptime</dt>
              <dd className="text-slate-300">{duration(data.resources.uptime_seconds)}</dd>
              <dt className="text-slate-500">CPU count</dt>
              <dd className="text-slate-300">{data.resources.cpu_count}</dd>
              <dt className="text-slate-500">Load 1/5/15</dt>
              <dd className="font-mono text-slate-300">
                {data.resources.cpu_load.load_1.toFixed(2)} /{' '}
                {data.resources.cpu_load.load_5.toFixed(2)} /{' '}
                {data.resources.cpu_load.load_15.toFixed(2)}
              </dd>
              <dt className="text-slate-500">Memory</dt>
              <dd className="text-slate-300">
                {bytes(
                  data.resources.memory.total_bytes - data.resources.memory.available_bytes,
                )}{' '}
                used / {bytes(data.resources.memory.total_bytes)}
              </dd>
            </dl>
          </div>

          {(data.resources.disks?.length ?? 0) > 0 && (
            <div className="card md:col-span-2 space-y-2">
              <div className="font-medium text-slate-100">Disk usage</div>
              <div className="grid gap-2 sm:grid-cols-2">
                {data.resources.disks!.map((d) => {
                  const tone =
                    d.percent_used >= 95
                      ? 'bg-rose-500'
                      : d.percent_used >= 85
                      ? 'bg-amber-500'
                      : 'bg-brand-500';
                  return (
                    <div
                      key={d.path}
                      className="rounded-md border border-slate-800 bg-slate-950 px-3 py-2 text-xs"
                    >
                      <div className="flex items-baseline justify-between gap-2">
                        <div className="text-sm text-slate-200">
                          {d.label ?? 'disk'}{' '}
                          <span className="font-mono text-[10px] text-slate-500">{d.path}</span>
                        </div>
                        <div className="font-mono text-slate-300">
                          {d.percent_used.toFixed(1)}%
                        </div>
                      </div>
                      <div className="mt-1 h-2 w-full overflow-hidden rounded-full bg-slate-800">
                        <div
                          className={`h-full ${tone} transition-all`}
                          style={{ width: `${Math.min(100, d.percent_used)}%` }}
                        />
                      </div>
                      <div className="mt-1 text-slate-400">
                        {bytes(d.used_bytes)} used · {bytes(d.free_bytes)} free ·{' '}
                        {bytes(d.total_bytes)} total
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          <div className="card md:col-span-2 space-y-2">
            <div className="font-medium text-slate-100">Components</div>
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
              {Object.entries(data.components).map(([name, c]) => (
                <div
                  key={name}
                  className="rounded-md border border-slate-800 bg-slate-950 px-3 py-2"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-sm text-slate-200">{name}</div>
                    <StatusBadge status={c.status} tone={statusTone(c.status)} />
                  </div>
                  <div className="mt-0.5 text-xs text-slate-500">
                    {c.version ? `v${c.version}` : c.error ?? ''}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
