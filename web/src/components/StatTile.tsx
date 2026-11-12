import type { ReactNode } from "react";

interface StatTileProps {
  label: string;
  value: ReactNode;
  unit?: string;
  detail?: ReactNode;
  accent?: "products" | "stock" | "sales" | "neutral";
}

/** Stat tile contract: label (sentence case), value, optional detail. */
export function StatTile({ label, value, unit, detail, accent = "neutral" }: StatTileProps) {
  const color =
    accent === "products"
      ? "var(--series-products)"
      : accent === "stock"
        ? "var(--series-stock)"
        : accent === "sales"
          ? "var(--series-sales)"
          : "var(--baseline)";
  return (
    <div className="stat-tile">
      <span className="stat-label">
        {accent !== "neutral" && <i className="stat-key" style={{ background: color }} aria-hidden />}
        {label}
      </span>
      <span className="stat-value">
        {value}
        {unit && <small className="stat-unit">{unit}</small>}
      </span>
      {detail && <span className="stat-detail">{detail}</span>}
    </div>
  );
}
