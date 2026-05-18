export type ScannerInfo = {
  id: string;
  label: string;
  backend: string;
  backend_status: string;
  configured_device: string | null;
  in_use: boolean;
};

export type ScannerDetails = ScannerInfo & {
  backend_details?: Record<string, unknown> | null;
};

export type ScanJob = {
  id: string;
  scanner: string;
  status: string;
  result_path: string | null;
  error: string | null;
  created_at: string;
  updated_at: string;
  stage: string | null;
  stage_detail: string | null;
  number_of_pages: number | null;
  ocr_batch_count: number | null;
  ocr_batches_completed: number | null;
  duration_seconds: number | null;
  recovery_available?: boolean;
  created_via?: string | null;
  result_size_bytes?: number | null;
  result_dpi?: number | null;
  tags?: string[];
};

export type ScanJobPage = {
  page: number;
  page_size: number;
  total: number;
  items: ScanJob[];
};

export type ScanCreateResponse = {
  job_id: string;
  status: string;
  stage: string;
  duration_seconds: number;
};

export type SystemStatus = {
  status: string;
  system: Record<string, unknown>;
  resources: {
    uptime_seconds: number;
    cpu_count: number;
    cpu_load: { load_1: number; load_5: number; load_15: number };
    memory: { total_bytes: number; available_bytes: number };
    disks?: Array<{
      label?: string;
      path: string;
      total_bytes: number;
      used_bytes: number;
      free_bytes: number;
      percent_used: number;
    }>;
  };
  components: Record<string, { status: string; version?: string; error?: string }>;
};

export type ScanRequest = {
  scanner: string;
  dpi?: number;
  color?: boolean;
};

export type PdfPageInfo = {
  index: number;
  width_pt: number;
  height_pt: number;
};

export type PdfPagesResponse = {
  page_count: number;
  pages: PdfPageInfo[];
};

export type CropRequest = {
  box: [number, number, number, number];
  reocr?: boolean;
  dpi?: number;
  rotation?: number;
};

export type CompositeRequest = {
  sources: string[];
  reocr?: boolean;
};

async function jsonFetch<T>(input: string, init?: RequestInit): Promise<T> {
  const res = await fetch(input, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${text ? `: ${text}` : ''}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

export const api = {
  listScanners: (refresh = false) =>
    jsonFetch<ScannerInfo[]>(`/api/scanners${refresh ? '?refresh=true' : ''}`),
  getScanner: (id: string, refresh = false) =>
    jsonFetch<ScannerDetails>(
      `/api/scanners/${encodeURIComponent(id)}${refresh ? '?refresh=true' : ''}`,
    ),
  getSystem: () => jsonFetch<SystemStatus>('/api/system'),
  listJobs: (page: number, pageSize: number, tags: string[] = []) => {
    const params = new URLSearchParams();
    params.set('page', String(page));
    params.set('page_size', String(pageSize));
    for (const t of tags) params.append('tags', t);
    return jsonFetch<ScanJobPage>(`/api/scans?${params.toString()}`);
  },
  listAllTags: () => jsonFetch<string[]>('/api/tags'),
  setJobTags: (jobId: string, tags: string[]) =>
    jsonFetch<{ tags: string[] }>(`/api/scans/${encodeURIComponent(jobId)}/tags`, {
      method: 'PUT',
      body: JSON.stringify({ tags }),
    }),
  getJob: (jobId: string) => jsonFetch<ScanJob>(`/api/scans/${encodeURIComponent(jobId)}`),
  createScan: (req: ScanRequest) =>
    jsonFetch<ScanCreateResponse>('/api/scans', {
      method: 'POST',
      body: JSON.stringify(req),
    }),
  deleteScan: (jobId: string) =>
    jsonFetch<void>(`/api/scans/${encodeURIComponent(jobId)}`, { method: 'DELETE' }),
  downloadUrl: (jobId: string) => `/api/scans/download/${encodeURIComponent(jobId)}`,
  viewUrl: (jobId: string) => `/api/scans/${encodeURIComponent(jobId)}/view`,
  listPages: (jobId: string) =>
    jsonFetch<PdfPagesResponse>(`/api/scans/${encodeURIComponent(jobId)}/pages`),
  pagePreviewUrl: (jobId: string, pageIndex: number, maxWidth = 1000) =>
    `/api/scans/${encodeURIComponent(jobId)}/pages/${pageIndex}/preview.jpg?max_width=${maxWidth}`,
  createCrop: (jobId: string, req: CropRequest) =>
    jsonFetch<ScanCreateResponse>(`/api/scans/${encodeURIComponent(jobId)}/crop`, {
      method: 'POST',
      body: JSON.stringify(req),
    }),
  createComposite: (req: CompositeRequest) =>
    jsonFetch<ScanCreateResponse>('/api/scans/composite', {
      method: 'POST',
      body: JSON.stringify(req),
    }),
  recoverScan: (jobId: string, reocr = false) =>
    jsonFetch<ScanCreateResponse>(`/api/scans/${encodeURIComponent(jobId)}/recover`, {
      method: 'POST',
      body: JSON.stringify({ reocr }),
    }),
  resumeScan: (jobId: string) =>
    jsonFetch<{ recovered_id: string; new_scan_id: string }>(
      `/api/scans/${encodeURIComponent(jobId)}/resume`,
      { method: 'POST', body: JSON.stringify({}) },
    ),
  retryScan: (jobId: string) =>
    jsonFetch<ScanCreateResponse>(`/api/scans/${encodeURIComponent(jobId)}/retry`, {
      method: 'POST',
      body: JSON.stringify({}),
    }),
};
