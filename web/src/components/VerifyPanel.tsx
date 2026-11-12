import { useRef } from "react";
import type { VerificationReport } from "../api";
import { formatTime } from "../api";
import { CheckList } from "./CheckList";

interface Props {
  reports: VerificationReport[];
  storedExports: { export_id: string; business_batch_id: number | null; status: string }[];
  selectedExport: string | null;
  busy: boolean;
  uploadBusy: boolean;
  onVerifyStored: (id: string) => void;
  onUpload: (file: File) => void;
  onSelectExport: (id: string) => void;
}

export function VerifyPanel({ reports, storedExports, selectedExport, busy, uploadBusy, onVerifyStored, onUpload, onSelectExport }: Props) {
  const fileRef = useRef<HTMLInputElement>(null);
  const published = storedExports.filter((task) => task.status === "succeeded");
  const latest = reports[0] ?? null;

  return (
    <section className="card">
      <header className="card-head">
        <h2>复核批次</h2>
        <span className="card-tag">散列 + 勾稽</span>
      </header>
      <p className="card-sub">
        重新读取文件并核对 manifest 散列、每个 Parquet 文件的 sha256、行数与结构，以及“库存 = 初始 − 累计销量”“明细合计 = 累计销量”。
      </p>

      <div className="verify-controls">
        <label className="field">
          <span>已发布批次</span>
          <select
            value={selectedExport ?? ""}
            onChange={(event) => onSelectExport(event.target.value)}
            disabled={busy || published.length === 0}
          >
            <option value="">选择批次…</option>
            {published.map((task) => (
              <option key={task.export_id} value={task.export_id}>
                {task.export_id} · 批次 #{task.business_batch_id}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className="btn btn-primary"
          disabled={!selectedExport || busy}
          onClick={() => selectedExport && onVerifyStored(selectedExport)}
        >
          {busy ? "核对中…" : "重新核对"}
        </button>
        <span className="or">或</span>
        <input
          ref={fileRef}
          type="file"
          accept=".zip,application/zip"
          className="visually-hidden"
          aria-label="上传导出压缩包"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) onUpload(file);
            event.target.value = "";
          }}
        />
        <button type="button" className="btn btn-quiet" disabled={uploadBusy} onClick={() => fileRef.current?.click()}>
          {uploadBusy ? "校验中…" : "上传批次 zip"}
        </button>
      </div>

      {latest && (
        <>
          <div className={`verify-result ${latest.ok ? "good" : "bad"}`}>
            <span className="verify-mark" aria-hidden>
              {latest.ok ? "✓" : "✕"}
            </span>
            <div>
              <strong>{latest.ok ? "复核通过" : "复核未通过"}</strong>
              <p>
                {latest.source === "upload" ? `上传文件 ${latest.upload_name ?? ""}` : `批次 ${latest.export_id}`} · {formatTime(latest.created_at)}
              </p>
            </div>
          </div>
          <CheckList
            bundle={{ all_ok: latest.ok, items: latest.checks, details: latest.details }}
            title="本次复核明细"
          />
        </>
      )}

      {reports.length > 1 && (
        <div className="history">
          <h3>最近复核</h3>
          <ul>
            {reports.slice(1, 6).map((report) => (
              <li key={report.verification_id}>
                <span className={`check-icon ${report.ok ? "ok" : "bad"}`} aria-hidden>
                  {report.ok ? "✓" : "✕"}
                </span>
                <span className="history-id">{report.export_id ?? report.upload_name ?? "上传批次"}</span>
                <span className="history-meta">
                  {report.checks.filter((item) => item.ok).length}/{report.checks.length} 项 · {formatTime(report.created_at)}
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}
