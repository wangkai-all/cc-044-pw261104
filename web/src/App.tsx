import { useEffect, useMemo, useRef, useState } from "react";
import { api, formatBytes, formatCount, formatTime, type BusinessSummary, type Controls, type ExportTask, type Limits, type VerificationReport, type WriterStatus } from "./api";
import { CheckList } from "./components/CheckList";
import { ControlsPanel } from "./components/ControlsPanel";
import { ExportCard, PreviewTables } from "./components/ExportCard";
import { Meter } from "./components/Meter";
import { StatTile } from "./components/StatTile";
import { VerifyPanel } from "./components/VerifyPanel";
import { WriterStatusBadge } from "./components/WriterStatusBadge";

const POLL_MS = 1000;

export function App() {
  const [controls, setControls] = useState<Controls | null>(null);
  const [limits, setLimits] = useState<Limits | null>(null);
  const [writer, setWriter] = useState<WriterStatus | null>(null);
  const [business, setBusiness] = useState<BusinessSummary | null>(null);
  const [tasks, setTasks] = useState<ExportTask[]>([]);
  const [previews, setPreviews] = useState<Record<string, Record<string, import("./api").Preview>>>({});
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [reports, setReports] = useState<VerificationReport[]>([]);
  const [controlBusy, setControlBusy] = useState(false);
  const [creating, setCreating] = useState(false);
  const [verifying, setVerifying] = useState<string | null>(null);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [notice, setNotice] = useState<{ kind: "ok" | "error"; text: string } | null>(null);
  const [loading, setLoading] = useState(true);
  const noticeTimer = useRef<number | null>(null);
  const selectedRef = useRef<string | null>(null);

  selectedRef.current = selectedId;

  function flash(kind: "ok" | "error", text: string) {
    setNotice({ kind, text });
    if (noticeTimer.current) window.clearTimeout(noticeTimer.current);
    noticeTimer.current = window.setTimeout(() => setNotice(null), 4200);
  }

  async function refresh() {
    try {
      const [overview, exportList] = await Promise.all([api.controls(), api.listExports(12)]);
      setControls(overview.controls);
      setLimits(overview.limits);
      setWriter(exportList.writer_status ?? overview.writer_status);
      setBusiness(exportList.overview);
      setTasks(exportList.tasks);
      const current = selectedRef.current;
      const nextDefault =
        current && exportList.tasks.some((task) => task.export_id === current)
          ? current
          : exportList.tasks.find((task) => task.status === "succeeded")?.export_id ?? exportList.tasks[0]?.export_id ?? null;
      if (nextDefault !== current) setSelectedId(nextDefault);
    } catch (error) {
      flash("error", error instanceof Error ? error.message : "刷新失败");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), POLL_MS);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    void api.verifications().then((data) => setReports(data.reports)).catch(() => undefined);
  }, []);

  const selectedTask = useMemo(() => tasks.find((task) => task.export_id === selectedId) ?? null, [tasks, selectedId]);

  useEffect(() => {
    if (!selectedTask || selectedTask.status !== "succeeded") return;
    if (previews[selectedTask.export_id]) return;
    let cancelled = false;
    void api
      .preview(selectedTask.export_id)
      .then((data) => {
        if (!cancelled) setPreviews((prev) => ({ ...prev, [selectedTask.export_id]: data.previews }));
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [selectedTask, previews]);

  async function patchControls(patch: Parameters<typeof api.setControls>[0]) {
    setControlBusy(true);
    try {
      const saved = await api.setControls(patch);
      setControls(saved.controls);
      setNotice(null);
    } catch (error) {
      flash("error", error instanceof Error ? error.message : "控制更新失败");
    } finally {
      setControlBusy(false);
    }
  }

  async function createExport() {
    setCreating(true);
    try {
      const { export_id } = await api.createExport();
      setSelectedId(export_id);
      await refresh();
      flash("ok", `已发起导出 ${export_id}`);
    } catch (error) {
      flash("error", error instanceof Error ? error.message : "发起导出失败");
    } finally {
      setCreating(false);
    }
  }

  async function cancelExport(id: string) {
    try {
      await api.cancelExport(id);
      await refresh();
    } catch (error) {
      flash("error", error instanceof Error ? error.message : "取消失败");
    }
  }

  async function verifyStored(id: string) {
    setVerifying(id);
    try {
      const report = await api.verifyStored(id);
      setReports((prev) => [report, ...prev.filter((item) => item.verification_id !== report.verification_id)]);
      flash(report.ok ? "ok" : "error", report.ok ? "复核通过" : "复核未通过，详见明细");
    } catch (error) {
      flash("error", error instanceof Error ? error.message : "复核失败");
    } finally {
      setVerifying(null);
    }
  }

  async function verifyUpload(file: File) {
    setUploadBusy(true);
    try {
      const report = await api.verifyUpload(file);
      setReports((prev) => [report, ...prev]);
      flash(report.ok ? "ok" : "error", report.ok ? "上传批次复核通过" : "上传批次存在问题");
    } catch (error) {
      flash("error", error instanceof Error ? error.message : "上传复核失败");
    } finally {
      setUploadBusy(false);
    }
  }

  const activeTasks = tasks.filter((task) => task.status === "running" || task.status === "queued");
  const latestDone = tasks.find((task) => task.status === "succeeded") ?? null;
  const latestFailed = tasks.find((task) => task.status === "failed") ?? null;

  return (
    <div className="app">
      <header className="app-head">
        <div>
          <h1>Pointwise Delivery</h1>
          <p className="app-sub">商品、库存、销售三表同快照导出 · 校验通过后整批发布</p>
        </div>
        <div className="head-actions">
          <WriterStatusBadge status={writer} />
          <button type="button" className="btn btn-primary" disabled={creating} onClick={createExport}>
            {creating ? "发起中…" : "发起导出"}
          </button>
        </div>
      </header>

      {notice && <div className={`notice notice-${notice.kind}`}>{notice.text}</div>}

      <section className="kpi-row" aria-label="实时概况">
        <StatTile
          label="业务写入批次"
          value={business ? `#${formatCount(business.latest_batch_id)}` : "—"}
          detail={
            writer && writer.observed_rate
              ? `观测 ${writer.observed_rate.toFixed(1)} 批/秒 · ${formatTime(business?.latest_committed_at)} 提交`
              : `最近提交 ${formatTime(business?.latest_committed_at)}`
          }
        />
        <StatTile label="商品行" value={business ? formatCount(business.counts.products) : "—"} unit="行" accent="products" detail="products.parquet" />
        <StatTile label="库存行" value={business ? formatCount(business.counts.stock) : "—"} unit="行" accent="stock" detail="stock.parquet" />
        <StatTile
          label="销售行"
          value={business ? formatCount(business.counts.sales) : "—"}
          unit="行"
          accent="sales"
          detail={business ? `累计 ${formatCount(business.sales_units)} 件` : undefined}
        />
        <StatTile
          label="最近成功导出"
          value={latestDone ? `#${formatCount(latestDone.business_batch_id)}` : "—"}
          detail={latestDone ? `${formatCount(latestDone.rows_total)} 行 · ${latestDone.export_id.slice(-6)}` : "尚未发布"}
        />
      </section>

      {activeTasks.length > 0 && (
        <section className="card live-card">
          <header className="card-head">
            <h2>导出进行中</h2>
            <span className="card-tag">{activeTasks.length} 个任务</span>
          </header>
          {activeTasks.map((task) => (
            <Meter
              key={task.export_id}
              value={task.rows_total > 0 ? task.rows_done / task.rows_total : task.progress}
              label={`${task.export_id} · ${task.stage.startsWith("reading:") ? `读取 ${task.stage.split(":")[1]}` : task.stage}`}
              caption={`${formatCount(task.rows_done)} / ${formatCount(task.rows_total)} 行 · 快照批次 #${formatCount(task.business_batch_id)}`}
            />
          ))}
        </section>
      )}

      <div className="layout">
        <ControlsPanel controls={controls ?? FALLBACK_CONTROLS} limits={limits} busy={controlBusy} onChange={patchControls} />

        <section className="card">
          <header className="card-head">
            <h2>导出批次</h2>
            <span className="card-tag">最新 12 个</span>
          </header>
          {loading && tasks.length === 0 ? (
            <p className="empty-note">正在加载…</p>
          ) : tasks.length === 0 ? (
            <p className="empty-note">还没有导出。点击右上角“发起导出”。</p>
          ) : (
            <div className="export-list">
              {tasks.map((task) => (
                <ExportCard
                  key={task.export_id}
                  task={task}
                  busy={verifying === task.export_id}
                  selected={task.export_id === selectedId}
                  onSelect={setSelectedId}
                  onCancel={cancelExport}
                  onVerify={verifyStored}
                  verifying={verifying === task.export_id}
                />
              ))}
            </div>
          )}
          {latestFailed && latestFailed.error && latestFailed.status === "failed" && (
            <p className="error-note">最近一次失败：{latestFailed.error}</p>
          )}
        </section>
      </div>

      {selectedTask && selectedTask.status === "succeeded" && (
        <>
          <CheckList bundle={selectedTask.checks} title={`勾稽结果 · ${selectedTask.export_id}`} />
          <PreviewTables previews={previews[selectedTask.export_id] ?? selectedTask.previews} />
          {selectedTask.files && <ManifestPanel task={selectedTask} />}
        </>
      )}

      <VerifyPanel
        reports={reports}
        storedExports={tasks}
        selectedExport={selectedId}
        busy={Boolean(verifying)}
        uploadBusy={uploadBusy}
        onVerifyStored={verifyStored}
        onUpload={verifyUpload}
        onSelectExport={setSelectedId}
      />

      <footer className="app-foot">
        <span>只读事务 · WAL 快照 · 失败或取消不留半份文件</span>
        <span>{controls ? `导出速度 ${controls.export.rows_per_second === 0 ? "不限" : `${controls.export.rows_per_second} 行/秒`}` : ""}</span>
      </footer>
    </div>
  );
}

function ManifestPanel({ task }: { task: ExportTask }) {
  return (
    <section className="card">
      <header className="card-head">
        <h2>清单与散列</h2>
        <span className="card-tag">manifest.json</span>
      </header>
      <p className="card-sub">
        清单记录结构、行数、文件散列与快照批次。下载整批或单个文件时返回已发布的原文件，不重新生成。
      </p>
      <div className="table-scroll">
        <table className="manifest-table">
          <thead>
            <tr>
              <th>文件</th>
              <th>行数</th>
              <th>大小</th>
              <th>sha256</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {(task.files ?? []).map((file) => (
              <tr key={file.file}>
                <td>
                  <i className={`file-key file-${file.name}`} aria-hidden />
                  {file.file}
                </td>
                <td className="num">{formatCount(file.rows)}</td>
                <td className="num">{formatBytes(file.bytes)}</td>
                <td className="hash">{file.sha256}</td>
                <td>
                  <a href={`/api/exports/${task.export_id}/files/${file.file}`} download={file.file}>
                    下载
                  </a>
                </td>
              </tr>
            ))}
            <tr>
              <td>
                <i className="file-key" aria-hidden />
                manifest.json
              </td>
              <td className="num">—</td>
              <td className="num">—</td>
              <td className="hash">{task.manifest_sha256 ?? "散列见 manifest.sha256"}</td>
              <td>
                <a href={`/api/exports/${task.export_id}/files/manifest.json`} download="manifest.json">
                  下载
                </a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  );
}

const FALLBACK_CONTROLS: Controls = {
  writer: { enabled: false, batches_per_second: 3, sales_per_batch: 2 },
  export: { rows_per_second: 0 },
};
