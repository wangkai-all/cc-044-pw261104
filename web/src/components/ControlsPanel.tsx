import { useId } from "react";
import type { Controls, Limits } from "../api";

interface Props {
  controls: Controls;
  limits: Limits | null;
  busy: boolean;
  onChange: (patch: Partial<{ writer_enabled: boolean; batches_per_second: number; sales_per_batch: number; rows_per_second: number }>) => void;
}

function Slider({
  label,
  hint,
  value,
  min,
  max,
  step,
  format,
  onCommit,
}: {
  label: string;
  hint: string;
  value: number;
  min: number;
  max: number;
  step: number;
  format: (v: number) => string;
  onCommit: (v: number) => void;
}) {
  const id = useId();
  return (
    <div className="slider-row">
      <label htmlFor={id} className="slider-label">
        {label}
        <span className="slider-hint">{hint}</span>
      </label>
      <span className="slider-value">{format(value)}</span>
      <input
        id={id}
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(event) => onCommit(Number(event.target.value))}
      />
    </div>
  );
}

export function ControlsPanel({ controls, limits, busy, onChange }: Props) {
  const writerMin = limits?.batches_per_second.min ?? 0.2;
  const writerMax = limits?.batches_per_second.max ?? 30;
  const salesMin = limits?.sales_per_batch.min ?? 1;
  const salesMax = limits?.sales_per_batch.max ?? 8;
  const exportMax = limits?.rows_per_second.max ?? 5000;

  return (
    <section className="card">
      <header className="card-head">
        <h2>现场控制</h2>
        <span className="card-tag">实时生效</span>
      </header>
      <button
        type="button"
        className={`writer-toggle ${controls.writer.enabled ? "on" : "off"}`}
        disabled={busy}
        aria-pressed={controls.writer.enabled}
        onClick={() => onChange({ writer_enabled: !controls.writer.enabled })}
      >
        <span className="writer-dot" aria-hidden />
        {controls.writer.enabled ? "持续写入进行中" : "写入已暂停"}
        <span className="writer-action">{busy ? "…" : controls.writer.enabled ? "暂停" : "继续"}</span>
      </button>
      <Slider
        label="写入批次频率"
        hint="每秒提交的事务数"
        value={controls.writer.batches_per_second}
        min={writerMin}
        max={writerMax}
        step={0.2}
        format={(v) => `${v.toFixed(1)} 批/秒`}
        onCommit={(v) => onChange({ batches_per_second: v })}
      />
      <Slider
        label="每批销售行数"
        hint="每个事务登记的销售条数"
        value={controls.writer.sales_per_batch}
        min={salesMin}
        max={salesMax}
        step={1}
        format={(v) => `${v} 行`}
        onCommit={(v) => onChange({ sales_per_batch: v })}
      />
      <Slider
        label="导出读取速度"
        hint="0 表示不限速"
        value={controls.export.rows_per_second}
        min={0}
        max={exportMax}
        step={20}
        format={(v) => (v === 0 ? "不限速" : `${v} 行/秒`)}
        onCommit={(v) => onChange({ rows_per_second: v })}
      />
    </section>
  );
}
