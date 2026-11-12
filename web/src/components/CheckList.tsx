import type { CheckBundle, CheckItem } from "../api";

interface Props {
  bundle: CheckBundle | null;
  title?: string;
  emptyNote?: string;
}

function StatusIcon({ ok }: { ok: boolean }) {
  return (
    <span className={`check-icon ${ok ? "ok" : "bad"}`} aria-hidden>
      {ok ? "✓" : "✕"}
    </span>
  );
}

export function CheckList({ bundle, title = "三表勾稽", emptyNote = "尚无核对结果" }: Props) {
  const items: CheckItem[] = bundle?.items ?? [];
  const okCount = items.filter((item) => item.ok).length;
  return (
    <section className="card">
      <header className="card-head">
        <h2>{title}</h2>
        {bundle && (
          <span className={`badge ${bundle.all_ok ? "badge-good" : "badge-bad"}`}>
            <StatusIcon ok={bundle.all_ok} />
            {bundle.all_ok ? "全部通过" : "存在问题"}
          </span>
        )}
      </header>
      {items.length === 0 ? (
        <p className="empty-note">{emptyNote}</p>
      ) : (
        <>
          <p className="card-sub">
            {okCount} / {items.length} 项通过。每一项都从已写出的 Parquet 文件读取，不依赖导出时内存中的值。
          </p>
          <ul className="check-list">
            {items.map((item) => (
              <li key={item.id} className={item.ok ? "" : "failed"}>
                <div className="check-title">
                  <StatusIcon ok={item.ok} />
                  <span>{item.title}</span>
                </div>
                <div className="check-detail">{item.detail}</div>
              </li>
            ))}
          </ul>
        </>
      )}
    </section>
  );
}
