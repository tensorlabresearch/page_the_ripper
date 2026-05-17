export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

export function duration(seconds: number | null | undefined): string {
  if (seconds == null) return '—';
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)} ms`;
  if (seconds < 60) return `${seconds.toFixed(1)} s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds - m * 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m - h * 60}m`;
}

export function formatTimestamp(iso: string): string {
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  // Server timestamps from main.py are naive UTC (datetime.utcnow().isoformat()
  // emits no timezone suffix), so the browser would parse them as local time.
  // Force a UTC interpretation, then render in the user's local timezone with
  // a complete, unambiguous, sortable format.
  const utcDate = iso.endsWith('Z') || /[+-]\d\d:?\d\d$/.test(iso) ? new Date(iso) : new Date(iso + 'Z');
  return utcDate.toLocaleString(undefined, {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

