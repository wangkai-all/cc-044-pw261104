import type { WriterStatus } from "../api";

export function WriterStatusBadge({ status }: { status: WriterStatus | null }) {
  if (!status) {
    return (
      <span className="status-badge status-unknown">
        <span className="status-dot" aria-hidden />写入进程未知
      </span>
    );
  }
  const alive = status.state === "running" || status.state === "paused";
  const state = status.state === "running" ? "持续写入中" : status.state === "paused" ? "写入已暂停" : status.state === "starting" ? "正在启动" : "写入进程无响应";
  const tone = !alive ? "bad" : status.state === "paused" ? "warn" : "good";
  return (
    <span className={`status-badge status-${tone}`}>
      <span className="status-dot" aria-hidden />
      <span>{state}</span>
      {status.pid ? <span className="status-meta">PID {status.pid}</span> : null}
    </span>
  );
}
