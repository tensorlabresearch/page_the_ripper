type Tone = 'ok' | 'warn' | 'error' | 'info' | 'muted';

const TONE: Record<Tone, string> = {
  ok: 'bg-emerald-500/15 text-emerald-300',
  warn: 'bg-amber-500/15 text-amber-300',
  error: 'bg-rose-500/15 text-rose-300',
  info: 'bg-brand-500/15 text-brand-300',
  muted: 'bg-slate-500/15 text-slate-300',
};

export function statusTone(status: string): Tone {
  const s = status.toLowerCase();
  if (s === 'completed' || s === 'ok' || s === 'ready' || s === 'idle') return 'ok';
  if (s === 'failed' || s === 'error' || s === 'busy') return 'error';
  if (s === 'pending' || s === 'queued' || s === 'preparing') return 'muted';
  if (s === 'running' || s === 'scanning' || s === 'processing' || s === 'ocr') return 'info';
  return 'warn';
}

export default function StatusBadge({ status, tone }: { status: string; tone?: Tone }) {
  const t = tone ?? statusTone(status);
  return <span className={`badge ${TONE[t]}`}>{status}</span>;
}
