"""Parquet artifacts: manifests, hashing, previews, and reconciliation.

Both the exporter and the verifier use this module, so a published file is
checked by the same logic that produced its manifest. All numbers compared here
are read back from the Parquet files themselves, not from memory.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA_VERSION = 1

TABLE_SPECS = {
    "products": {
        "file": "products.parquet",
        "columns": [
            ("product_id", "string"),
            ("sku", "string"),
            ("name", "string"),
            ("category", "string"),
            ("unit_price_cents", "int64"),
            ("initial_stock", "int64"),
            ("cumulative_sold", "int64"),
            ("created_at", "string"),
        ],
        "sort": "product_id ASC",
    },
    "stock": {
        "file": "stock.parquet",
        "columns": [
            ("product_id", "string"),
            ("on_hand", "int64"),
        ],
        "sort": "product_id ASC",
    },
    "sales": {
        "file": "sales.parquet",
        "columns": [
            ("sale_id", "int64"),
            ("product_id", "string"),
            ("quantity", "int64"),
            ("unit_price_cents", "int64"),
            ("sold_at_us", "int64"),
            ("batch_id", "int64"),
        ],
        "sort": "sale_id ASC",
    },
}

ARROW_TYPES = {
    "string": pa.string(),
    "int64": pa.int64(),
}


def table_schema(name: str) -> pa.Schema:
    return pa.schema([pa.field(col, ARROW_TYPES[kind], nullable=False) for col, kind in TABLE_SPECS[name]["columns"]])


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def write_atomic(path: Path, data: bytes | str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as handle:
        handle.write(data.encode("utf-8") if isinstance(data, str) else data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def arrow_type_name(dtype: pa.DataType) -> str:
    return str(dtype)


def schema_entries(schema: pa.Schema) -> list[dict]:
    return [
        {"name": field.name, "type": arrow_type_name(field.type), "nullable": bool(field.nullable)}
        for field in schema
    ]


def file_record(path: Path) -> dict:
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def build_manifest(
    *,
    export_id: str,
    created_at: str,
    business: dict,
    files: list[dict],
    checks: dict,
    table_rows: dict[str, int],
    preview_rows: int,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "pointwise-export",
        "export_id": export_id,
        "created_at": created_at,
        "business": business,
        "tables": files,
        "row_counts": table_rows,
        "checks": checks,
        "preview_rows": preview_rows,
        "reader": {"engine": "sqlite", "journal_mode": "wal", "snapshot": "read-transaction-end-mark"},
    }


def read_manifest(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest 不是 JSON 对象")
    return payload


def preview_table(path: Path, limit: int) -> dict:
    table = pq.read_table(path)
    total = table.num_rows
    head = table.slice(0, max(0, limit))
    return {
        "columns": [f.name for f in table.schema],
        "types": [arrow_type_name(f.type) for f in table.schema],
        "rows": head.to_pylist(),
        "shown": head.num_rows,
        "total": total,
    }


def preview_bundle(directory: Path, manifest: dict, limit: int) -> dict:
    previews = {}
    for entry in manifest.get("tables", []):
        name = entry.get("name")
        file_name = entry.get("file")
        if not name or not file_name:
            continue
        path = directory / file_name
        previews[name] = preview_table(path, limit)
    return previews


@dataclass
class SalesAggregate:
    per_product: dict[str, int]
    rows: int
    units: int
    max_batch: int | None
    min_quantity: int | None
    non_positive_rows: int
    product_ids: set[str]


@dataclass
class SnapshotData:
    products: list[dict]
    stock: list[dict]
    sales: SalesAggregate


def read_products_rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def read_stock_rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def aggregate_sales(path: Path) -> SalesAggregate:
    """Aggregate the sales file in batches so memory stays flat.

    The aggregation is what the reconciliation compares against the products
    table, so it streams instead of materialising every sale row.
    """
    per_product: dict[str, int] = {}
    total_rows = 0
    total_units = 0
    max_batch: int | None = None
    min_qty: int | None = None
    non_positive = 0
    product_ids: set[str] = set()
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=8192, columns=["product_id", "quantity", "batch_id"]):
        pids = batch.column(0).to_pylist()
        qtys = batch.column(1).to_pylist()
        batch_ids = batch.column(2).to_pylist()
        for pid, qty, batch_id in zip(pids, qtys, batch_ids):
            qty = int(qty)
            batch_id = int(batch_id)
            per_product[pid] = per_product.get(pid, 0) + qty
            total_units += qty
            product_ids.add(pid)
            if qty <= 0:
                non_positive += 1
            if max_batch is None or batch_id > max_batch:
                max_batch = batch_id
            if min_qty is None or qty < min_qty:
                min_qty = qty
        total_rows += batch.num_rows
    return SalesAggregate(per_product, total_rows, total_units, max_batch, min_qty, non_positive, product_ids)


def load_snapshot_data(
    directory: Path,
    table_names: Iterable[str] = ("products", "stock", "sales"),
) -> SnapshotData:
    names = set(table_names)
    products = read_products_rows(directory / TABLE_SPECS["products"]["file"]) if "products" in names else []
    stock = read_stock_rows(directory / TABLE_SPECS["stock"]["file"]) if "stock" in names else []
    sales = (
        aggregate_sales(directory / TABLE_SPECS["sales"]["file"])
        if "sales" in names
        else SalesAggregate({}, 0, 0, None, None, 0, set())
    )
    return SnapshotData(products, stock, sales)


def reconcile(
    data: SnapshotData,
    *,
    business_batch_id: int | None,
    expected_counts: dict[str, int] | None = None,
) -> dict:
    """Check the three exported tables against one another.

    Returns {"all_ok": bool, "items": [...], "details": {...}} where each item is
    a named check with a human-readable detail string.
    """
    items: list[dict] = []

    def add(check_id: str, title: str, ok: bool, detail: str) -> None:
        items.append({"id": check_id, "title": title, "ok": bool(ok), "detail": detail})

    products_by_id = {row["product_id"]: row for row in data.products}
    stock_by_id = {row["product_id"]: row for row in data.stock}

    duplicate_products = len(products_by_id) != len(data.products)
    duplicate_stock = len(stock_by_id) != len(data.stock)
    add(
        "primary_keys",
        "商品与库存主键唯一",
        not (duplicate_products or duplicate_stock),
        f"商品 {len(data.products)} 行 / 唯一 {len(products_by_id)}；库存 {len(data.stock)} 行 / 唯一 {len(stock_by_id)}",
    )

    product_ids = set(products_by_id)
    stock_ids = set(stock_by_id)
    missing_stock = sorted(product_ids - stock_ids)
    extra_stock = sorted(stock_ids - product_ids)
    add(
        "coverage",
        "每个商品都有对应库存行",
        not missing_stock and not extra_stock,
        f"缺库存 {len(missing_stock)} 个，多余库存 {len(extra_stock)} 个"
        + (f"：{', '.join(missing_stock[:5])}" if missing_stock else ""),
    )

    balance_mismatches: list[str] = []
    negative: list[str] = []
    for product_id, product in products_by_id.items():
        stock_row = stock_by_id.get(product_id)
        if stock_row is None:
            continue
        on_hand = int(stock_row["on_hand"])
        expected = int(product["initial_stock"]) - int(product["cumulative_sold"])
        if on_hand != expected:
            balance_mismatches.append(f"{product_id}: 库存 {on_hand} ≠ 初始 {product['initial_stock']} - 累计销售 {product['cumulative_sold']}")
        if on_hand < 0:
            negative.append(f"{product_id}: {on_hand}")
    add(
        "stock_balance",
        "库存 = 初始库存 - 累计销量",
        not balance_mismatches,
        "全部 %d 个商品相符" % len(products_by_id) if not balance_mismatches else "；".join(balance_mismatches[:4]),
    )
    add("non_negative", "库存不为负", not negative, "全部商品库存 ≥ 0" if not negative else "；".join(negative[:4]))

    sales_mismatches: list[str] = []
    for product_id, product in products_by_id.items():
        sold = int(data.sales.per_product.get(product_id, 0))
        cumulative = int(product["cumulative_sold"])
        if sold != cumulative:
            sales_mismatches.append(f"{product_id}: 明细合计 {sold} ≠ 累计销量 {cumulative}")
    add(
        "sales_per_product",
        "每个商品销售明细合计 = 累计销量",
        not sales_mismatches,
        "全部 %d 个商品相符" % len(products_by_id) if not sales_mismatches else "；".join(sales_mismatches[:4]),
    )

    total_cumulative = sum(int(p["cumulative_sold"]) for p in data.products)
    add(
        "sales_total",
        "销售明细总量 = 商品累计销量之和",
        data.sales.units == total_cumulative,
        f"明细 {data.sales.units} 件，累计销量 {total_cumulative} 件",
    )

    orphan_sales = sorted(data.sales.product_ids - product_ids)
    add(
        "sales_references",
        "销售明细均指向已导出商品",
        not orphan_sales,
        "无孤立销售行" if not orphan_sales else f"孤立商品 {', '.join(orphan_sales[:5])}",
    )

    if business_batch_id is None:
        add("snapshot_batch", "销售明细不晚于快照批次", data.sales.max_batch is None, f"快照批次：{business_batch_id}")
    else:
        within = data.sales.max_batch is None or data.sales.max_batch <= business_batch_id
        add(
            "snapshot_batch",
            "销售明细不晚于快照批次",
            within,
            f"明细最大批次 {data.sales.max_batch}，快照批次 {business_batch_id}",
        )

    add(
        "quantities_positive",
        "销售数量为正整数",
        data.sales.rows == 0 or data.sales.non_positive_rows == 0,
        f"最小销售数量：{data.sales.min_quantity}，非正数行：{data.sales.non_positive_rows}",
    )

    if expected_counts is not None:
        actual = {
            "products": len(data.products),
            "stock": len(data.stock),
            "sales": data.sales.rows,
        }
        mismatched = [
            f"{name}: 清单 {expected_counts.get(name)} 行，实际 {actual[name]} 行"
            for name in ("products", "stock", "sales")
            if expected_counts.get(name) != actual[name]
        ]
        add("row_counts", "行数与清单一致", not mismatched, "三张表行数均与清单一致" if not mismatched else "；".join(mismatched))

    details = {
        "products": len(data.products),
        "stock_rows": len(data.stock),
        "sales_rows": data.sales.rows,
        "sales_units": data.sales.units,
        "cumulative_sold": total_cumulative,
        "sales_max_batch": data.sales.max_batch,
        "business_batch_id": business_batch_id,
    }
    return {"all_ok": all(item["ok"] for item in items), "items": items, "details": details}


def build_bundle(directory: Path, bundle_path: Path) -> None:
    """Zip the published batch with fixed metadata so the bytes are stable."""
    entries = ["manifest.json", "manifest.sha256", "preview.json"]
    manifest = read_manifest(directory / "manifest.json")
    for entry in manifest.get("tables", []):
        entries.append(entry["file"])
    tmp = bundle_path.with_name(bundle_path.name + ".tmp")
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name in entries:
            info = zipfile.ZipInfo(name, date_time=(2026, 11, 13, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            source = directory / name
            archive.writestr(info, source.read_bytes())
    os.replace(tmp, bundle_path)


def cleanup_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def iter_table_files(manifest: dict) -> Iterator[dict]:
    for entry in manifest.get("tables", []):
        if entry.get("file"):
            yield entry
