import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ExportTask, VerificationReport } from "./api";
import { App } from "./App";

const baseTask: ExportTask = {
  export_id: "exp-20261113-053409-1af6ea",
  status: "running",
  stage: "reading:sales",
  progress: 0.25,
  rows_total: 24174,
  rows_done: 6000,
  business_batch_id: 12061,
  snapshot_committed_at: "2026-11-12T21:34:09.254Z",
  requested_at: "2026-11-12T21:34:09.378Z",
  started_at: "2026-11-12T21:34:09.384Z",
  finished_at: null,
  cancel_requested: false,
  reused_from: null,
  manifest_sha256: null,
  bundle_sha256: null,
  error: null,
  files: null,
  checks: null,
  previews: null,
  speed: 400,
  manifest: null,
};

const succeededTask: ExportTask = {
  ...baseTask,
  export_id: "exp-20261113-053500-abcdef",
  status: "succeeded",
  stage: "published",
  progress: 1,
  rows_done: 24174,
  finished_at: "2026-11-12T21:34:30.000Z",
  files: [
    { name: "products", file: "products.parquet", rows: 18, bytes: 2975, sha256: "a".repeat(64), schema: [] },
    { name: "stock", file: "stock.parquet", rows: 18, bytes: 824, sha256: "b".repeat(64), schema: [] },
    { name: "sales", file: "sales.parquet", rows: 24138, bytes: 366783, sha256: "c".repeat(64), schema: [] },
  ],
  checks: {
    all_ok: true,
    items: [
      { id: "stock_balance", title: "库存 = 初始库存 - 累计销量", ok: true, detail: "全部 18 个商品相符" },
      { id: "sales_per_product", title: "每个商品销售明细合计 = 累计销量", ok: true, detail: "全部 18 个商品相符" },
    ],
    details: {},
  },
  previews: {
    products: { columns: ["product_id", "initial_stock", "cumulative_sold"], types: [], rows: [{ product_id: "prod-001", initial_stock: 3200, cumulative_sold: 800 }], shown: 1, total: 18 },
    stock: { columns: ["product_id", "on_hand"], types: [], rows: [{ product_id: "prod-001", on_hand: 2400 }], shown: 1, total: 18 },
    sales: { columns: ["sale_id", "product_id", "quantity"], types: [], rows: [{ sale_id: 1, product_id: "prod-001", quantity: 2 }], shown: 1, total: 24138 },
  },
};

const failedTask: ExportTask = {
  ...baseTask,
  export_id: "exp-20261113-053600-123456",
  status: "failed",
  stage: "failed",
  error: "导出失败：勾稽校验未通过：库存 8 ≠ 初始 10 - 累计销售 3",
  business_batch_id: 12062,
};

const report: VerificationReport = {
  verification_id: "v-1",
  created_at: "2026-11-12T21:35:00.000Z",
  source: "stored",
  export_id: succeededTask.export_id,
  ok: false,
  checks: [
    { id: "stock_balance", title: "库存 = 初始库存 - 累计销量", ok: false, detail: "prod-001: 库存 8 ≠ 初始 10 - 累计销售 3" },
  ],
  files: [],
  details: {},
};

async function taskCard(exportId: string): Promise<HTMLElement> {
  const matches = await screen.findAllByText(exportId);
  const node = matches.find((element) => element.closest("article"));
  if (!node) throw new Error(`no card for ${exportId}`);
  return node.closest("article")!;
}

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    }),
  );
}

let tasks: ExportTask[];
let controls: { writer: { enabled: boolean; batches_per_second: number; sales_per_batch: number }; export: { rows_per_second: number } };
let verifications: VerificationReport[];

beforeEach(() => {
  tasks = [baseTask, succeededTask, failedTask];
  controls = { writer: { enabled: true, batches_per_second: 3, sales_per_batch: 2 }, export: { rows_per_second: 400 } };
  verifications = [];
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const method = init?.method ?? "GET";
      if (url.startsWith("/api/controls") && method === "PUT") {
        const patch = JSON.parse(String(init?.body ?? "{}"));
        if (patch.writer_enabled !== undefined) controls.writer.enabled = patch.writer_enabled;
        if (patch.rows_per_second !== undefined) controls.export.rows_per_second = patch.rows_per_second;
        return jsonResponse({ controls });
      }
      if (url.startsWith("/api/controls")) {
        return jsonResponse({
          controls,
          limits: { batches_per_second: { min: 0.2, max: 30 }, sales_per_batch: { min: 1, max: 8 }, rows_per_second: { min: 0, max: 5000 } },
          writer_status: { state: "running", pid: 1234, observed_rate: 3, last_batch_id: 12061 },
          capacity: 0,
          max_pending: 2,
        });
      }
      if (url.startsWith("/api/exports?limit")) {
        return jsonResponse({
          tasks,
          writer_status: { state: "running", pid: 1234, observed_rate: 3, last_batch_id: 12061 },
          overview: { latest_batch_id: 12061, latest_committed_at: "2026-11-12T21:34:09.254Z", counts: { products: 18, stock: 18, sales: 24138 }, sales_units: 36000 },
        });
      }
      if (url.startsWith("/api/exports/") && url.endsWith("/preview")) {
        const id = url.split("/")[3];
        const task = tasks.find((item) => item.export_id === id);
        return jsonResponse({ export_id: id, previews: task?.previews ?? {} });
      }
      if (url === "/api/exports" && method === "POST") {
        const created: ExportTask = { ...baseTask, export_id: "exp-20261113-060000-newone" };
        tasks = [created, ...tasks];
        return jsonResponse({ export_id: created.export_id }, 202);
      }
      if (url.startsWith("/api/exports/") && url.endsWith("/cancel")) {
        const id = url.split("/")[3];
        const task = tasks.find((item) => item.export_id === id);
        if (task) task.status = "cancelled";
        return jsonResponse({ export_id: id, cancel_requested: true });
      }
      if (url.startsWith("/api/verifications") && method === "POST") {
        verifications = [report, ...verifications];
        return jsonResponse(report);
      }
      if (url.startsWith("/api/verifications")) {
        return jsonResponse({ reports: verifications });
      }
      return jsonResponse({ detail: "not found" }, 404);
    }),
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("dashboard", () => {
  it("shows live export progress with rows and snapshot batch", async () => {
    render(<App />);
    expect((await screen.findAllByText(/6,000 \/ 24,174 行/)).length).toBeGreaterThan(0);
    const liveCard = await taskCard(baseTask.export_id);
    expect(within(liveCard).getByText("运行中")).toBeInTheDocument();
    expect(within(liveCard).getByRole("progressbar")).toHaveAttribute("aria-valuenow", "25");
    expect(within(liveCard).getByText(/快照批次/)).toBeInTheDocument();
    expect(within(liveCard).getByText("#12,061")).toBeInTheDocument();
  });

  it("presents a published batch with files, reconciliation and previews", async () => {
    render(<App />);
    const card = await taskCard(succeededTask.export_id);
    expect(within(card).getByText("已发布", { selector: ".badge" })).toBeInTheDocument();
    expect(within(card).getByText("下载整批（zip）")).toHaveAttribute("href", `/api/exports/${succeededTask.export_id}/download`);
    expect(within(card).getByText("重新核对")).toBeInTheDocument();
    expect(await screen.findByText("勾稽结果 · exp-20261113-053500-abcdef")).toBeInTheDocument();
    expect(await screen.findByText("库存 = 初始库存 - 累计销量")).toBeInTheDocument();
    expect(await screen.findByText("sales.parquet", { selector: "h3" })).toBeInTheDocument();
    expect(await screen.findByRole("cell", { name: "2,400" })).toBeInTheDocument();
  });

  it("shows failure detail and refuses download for failed or cancelled batches", async () => {
    render(<App />);
    const card = await taskCard(failedTask.export_id);
    expect(within(card).getByText("失败", { selector: ".badge" })).toBeInTheDocument();
    expect(within(card).getAllByText(/勾稽校验未通过/).length).toBeGreaterThan(0);
    expect(within(card).queryByText("下载整批（zip）")).toBeNull();
  });

  it("sends control changes to the server and reflects them", async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(await screen.findByRole("button", { name: /持续写入进行中/ }));
    expect(controls.writer.enabled).toBe(false);
    expect(await screen.findByRole("button", { name: /写入已暂停/ })).toBeInTheDocument();
  });

  it("starts a new export and selects it", async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(await screen.findByRole("button", { name: "发起导出" }));
    expect(await screen.findByText(/已发起导出 exp-20261113-060000-newone/)).toBeInTheDocument();
  });

  it("runs a verification against the selected published batch", async () => {
    const user = userEvent.setup();
    render(<App />);
    const card = await taskCard(succeededTask.export_id);
    await user.click(within(card).getByText("重新核对"));
    expect(await screen.findByText("复核未通过")).toBeInTheDocument();
    expect(await screen.findByText(/prod-001: 库存 8/)).toBeInTheDocument();
  });

  it("warns when the writer process stops reporting", async () => {
    vi.mocked(fetch).mockImplementation((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/controls")) {
        return jsonResponse({
          controls,
          limits: { batches_per_second: { min: 0.2, max: 30 }, sales_per_batch: { min: 1, max: 8 }, rows_per_second: { min: 0, max: 5000 } },
          writer_status: { state: "stale", last_batch_id: 12061, observed_rate: 0 },
          capacity: 0,
          max_pending: 2,
        });
      }
      if (url.startsWith("/api/exports?limit")) {
        return jsonResponse({
          tasks,
          writer_status: { state: "stale", last_batch_id: 12061, observed_rate: 0 },
          overview: { latest_batch_id: 12061, latest_committed_at: null, counts: { products: 18, stock: 18, sales: 24138 }, sales_units: 0 },
        });
      }
      if (url.startsWith("/api/verifications")) return jsonResponse({ reports: [] });
      return jsonResponse({}, 404);
    });
    render(<App />);
    expect(await screen.findByText("写入进程无响应")).toBeInTheDocument();
  });
});
