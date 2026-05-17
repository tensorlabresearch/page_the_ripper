export default function ErrorCard({ error }: { error: unknown }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div className="card border-rose-900/60 bg-rose-950/30">
      <div className="mb-1 text-sm font-medium text-rose-300">Request failed</div>
      <pre className="overflow-x-auto whitespace-pre-wrap text-xs text-rose-200">{msg}</pre>
    </div>
  );
}
