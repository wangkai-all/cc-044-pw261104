interface MeterProps {
  /** 0..1, clamped. */
  value: number;
  label: string;
  caption?: string;
  tone?: "running" | "done" | "error";
  visibleLabel?: string;
}

/**
 * Meter: the fill carries state, the track is the same ramp one step lighter.
 * The visible percentage is always printed beside the track, never inside the
 * fill alone, so the value is readable without colour.
 */
export function Meter({ value, label, caption, tone = "running", visibleLabel }: MeterProps) {
  const pct = Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
  return (
    <div className={`meter meter-${tone}`}>
      <div className="meter-head">
        <span className="meter-label">{label}</span>
        <span className="meter-value">{visibleLabel ?? `${(pct * 100).toFixed(0)}%`}</span>
      </div>
      <div
        className="meter-track"
        role="progressbar"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={Math.round(pct * 100)}
        aria-valuetext={`${(pct * 100).toFixed(0)}%`}
      >
        <div className="meter-fill" style={{ width: `${pct * 100}%` }} />
      </div>
      {caption && <span className="meter-caption">{caption}</span>}
    </div>
  );
}
