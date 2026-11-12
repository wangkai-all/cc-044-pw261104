import type { ExportTask, Preview } from "../api";
import { downloadUrl, formatBytes, formatCount, formatTime } from "../api";
import { Meter } from "./Meter";

const STAGE_LABELS: Record<string, string> = {
  queued: "排队中",
  snapshot: "建立只读快照",
  reading: "读取并写出",
  verifying: "核对三张表",
  publishing: "发布文件",
  published: "已发布",
  failed: "失败",
  cancelled: "已取消",
  interrupted: "进程中断",
};

export function stageLabel(stage: string): string {
  if (STAGE_LABELS[stage]) return STAGE_LABELS[stage];
  if (stage.startsWith("reading:")) {
    const table = stage.split(":")[1];
    const name = table === "products" ? "商品表" : table === "stock" ? "库存表" : table === "sales" ? "销售表" : table;
    return `读取${name}`;
  }
  return stage;
}

export function statusLabel(status: ExportTask["status"]): string {
  return { queued: "排队", running: "运行中", succeeded: "已发布", failed: "失败", cancelled: "已取消" }[status];
}

interface Props {
  task: ExportTask;
  busy: boolean;
  onCancel: (id: string) => void;
  onVerify: (id: string) => void;
  verifying: boolean;
  selected: boolean;
  onSelect: (id: string) => void;
}

export function ExportCard({ task, busy, onCancel, onVerify, verifying, selected, onSelect }: Props) {
  const pct = task.rows_total > 0 ? Math.min(1, task.rows_done / task.rows_total) : task.progress;
  const live = task.status === "running" || task.status === "queued";
  const tone = task.status === "succeeded" ? "done" : task.status === "failed" || task.status === "cancelled" ? "error" : "running";

  return (
    <article className={`export-card ${selected ? "selected" : ""}`} aria-current={selected}>
      <header className="export-head">
        <button
          type="button"
          className="export-id"
          onClick={() => onSelect(task.export_id)}
          aria-label={`查看批次 ${task.export_id} 的预览与清单`}
        >
          {task.export_id}
        </button>
        <span className={`badge badge-${task.status}`}>{statusLabel(task.status)}</span>
      </header>

      <Meter
        value={pct}
        tone={tone}
        label={live ? stageLabel(task.stage) : task.status === "succeeded" ? "已发布" : stageLabel(task.stage)}
        visibleLabel={`${(pct * 100).toFixed(0)}%`}
        caption={
          live
            ? `${formatCount(task.rows_done)} / ${formatCount(task.rows_total)} 行 · ${task.speed > 0 ? `${formatCount(Math.round(task.speed))} 行/秒` : "测速中"}`
            : task.status === "succeeded"
              ? `${formatCount(task.rows_total)} 行 · 批次 #${formatCount(task.business_batch_id)}`
              : task.error ?? ""
        }
      />

      <dl className="export-facts">
        <div>
          <dt>快照批次</dt>
          <dd>{task.business_batch_id ? `#${formatCount(task.business_batch_id)}` : "—"}</dd>
        </div>
        <div>
          <dt>批次提交时间</dt>
          <dd>{formatTime(task.snapshot_committed_at)}</dd>
        </div>
        <div>
          <dt>发起时间</dt>
          <dd>{formatTime(task.requested_at)}</dd>
        </div>
      </dl>

      {task.files && (
        <div className="files-row">
          {task.files.map((file) => (
            <span key={file.name} className={`file-chip file-${file.name}`}>
              <i className="file-key" aria-hidden />
              {file.file}
              <small>
                {formatCount(file.rows)} 行 · {formatBytes(file.bytes)}
              </small>
            </span>
          ))}
        </div>
      )}

      {task.error && task.status !== "succeeded" && <p className="error-note">{task.error}</p>}
      {task.reused_from && <p className="reuse-note">同批次内容已存在，复用批次 {task.reused_from} 的文件。</p>}

      <footer className="export-actions">
        {live && (
          <button type="button" className="btn btn-quiet" disabled={task.cancel_requested} onClick={() => onCancel(task.export_id)}>
            {task.cancel_requested ? "正在取消…" : "取消"}
          </button>
        )}
        {task.status === "succeeded" && (
          <>
            <a className="btn btn-primary" href={downloadUrl(task.export_id)} download={`${task.export_id}.zip`}>
              下载整批（zip）
            </a>
            <button type="button" className="btn btn-quiet" disabled={verifying} onClick={() => onVerify(task.export_id)}>
              {verifying ? "核对中…" : "重新核对"}
            </button>
          </>
        )}
        {busy && live && <span className="pending-note">正在处理…</span>}
      </footer>
    </article>
  );
}

export function PreviewTables({ previews }: { previews: Record<string, Preview> | null }) {
  if (!previews) return null;
  const tables = ["products", "stock", "sales"].filter((name) => previews[name]);
  return (
    <section className="card">
      <header className="card-head">
        <h2>数据预览</h2>
        <span className="card-tag">每表前 24 行</span>
      </header>
      {tables.map((name) => {
        const preview = previews[name];
        return (
          <div key={name} className="preview-block">
            <h3>
              <i className={`file-key file-${name}`} aria-hidden />
              {name}.parquet
              <small>
                {preview.shown} / {preview.total} 行 · {preview.columns.length} 列
              </small>
            </h3>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    {preview.columns.map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.rows.map((row, index) => (
                    <tr key={index}>
                      {preview.columns.map((column) => (
                        <td key={column}>{formatCell(row[column])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
    </section>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return value.toLocaleString("zh-CN");
  return String(value);
}
