export interface Controls {
  writer: { enabled: boolean; batches_per_second: number; sales_per_batch: number };
  export: { rows_per_second: number };
}

export interface Limits {
  batches_per_second: { min: number; max: number };
  sales_per_batch: { min: number; max: number };
  rows_per_second: { min: number; max: number };
}

export interface WriterStatus {
  state: string;
  pid?: number;
  batches_committed?: number;
  last_batch_id?: number;
  last_commit_epoch?: number;
  last_error?: string | null;
  observed_rate?: number;
  status_age_seconds?: number;
  uptime_seconds?: number;
}

export interface BusinessSummary {
  latest_batch_id: number;
  latest_committed_at: string | null;
  counts: { products: number; stock: number; sales: number };
  sales_units: number;
}

export interface CheckItem {
  id: string;
  title: string;
  ok: boolean;
  detail: string;
}

export interface CheckBundle {
  all_ok: boolean;
  items: CheckItem[];
  details: Record<string, unknown>;
}

export interface TableFile {
  name: string;
  file: string;
  rows: number;
  bytes: number;
  sha256: string;
  schema: { name: string; type: string; nullable: boolean }[];
}

export interface Preview {
  columns: string[];
  types: string[];
  rows: Record<string, unknown>[];
  shown: number;
  total: number;
}

export interface ExportTask {
  export_id: string;
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  stage: string;
  progress: number;
  rows_total: number;
  rows_done: number;
  business_batch_id: number | null;
  snapshot_committed_at: string | null;
  requested_at: string;
  started_at: string | null;
  finished_at: string | null;
  cancel_requested: boolean;
  reused_from: string | null;
  manifest_sha256: string | null;
  bundle_sha256: string | null;
  error: string | null;
  files: TableFile[] | null;
  checks: CheckBundle | null;
  previews: Record<string, Preview> | null;
  speed: number;
  manifest: Record<string, unknown> | null;
}

export interface VerificationReport {
  verification_id: string;
  created_at: string;
  source: string;
  export_id: string | null;
  ok: boolean;
  checks: CheckItem[];
  files: { name: string; file: string; bytes: number; sha256: string }[];
  details: Record<string, unknown>;
  upload_name?: string;
}

const BASE = "/api";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${BASE}${path}`, init);
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (typeof body?.detail === "string") detail = body.detail;
    } catch {
      /* non-JSON error body */
    }
    throw new Error(detail);
  }
  return (await response.json()) as T;
}

export const api = {
  overview: () => request<{ controls: Controls; writer_status: WriterStatus; business: BusinessSummary; capacity: number; max_pending: number }>("/overview"),
  controls: () =>
    request<{ controls: Controls; limits: Limits; writer_status: WriterStatus; capacity: number; max_pending: number }>("/controls"),
  setControls: (payload: Partial<{ writer_enabled: boolean; batches_per_second: number; sales_per_batch: number; rows_per_second: number }>) =>
    request<{ controls: Controls }>("/controls", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  listExports: (limit = 20) =>
    request<{ tasks: ExportTask[]; writer_status: WriterStatus; overview: BusinessSummary }>(`/exports?limit=${limit}`),
  createExport: () => request<{ export_id: string }>("/exports", { method: "POST" }),
  cancelExport: (id: string) => request<{ cancel_requested: boolean }>(`/exports/${id}/cancel`, { method: "POST" }),
  preview: (id: string) => request<{ previews: Record<string, Preview> }>(`/exports/${id}/preview`),
  verifyStored: (id: string) => {
    const body = new FormData();
    body.append("export_id", id);
    return request<VerificationReport>("/verifications", { method: "POST", body });
  },
  verifyUpload: (file: File) => {
    const body = new FormData();
    body.append("upload", file);
    return request<VerificationReport>("/verifications", { method: "POST", body });
  },
  verifications: () => request<{ reports: VerificationReport[] }>("/verifications?limit=10"),
};

export function downloadUrl(exportId: string, file?: string): string {
  return file ? `${BASE}/exports/${exportId}/files/${file}` : `${BASE}/exports/${exportId}/download`;
}

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString("zh-CN");
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  return date.toLocaleTimeString("zh-CN", { hour12: false });
}
