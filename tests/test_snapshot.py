"""Focused checks on the pieces most likely to regress.

These bypass FastAPI: they exercise the read transaction, the throttled writer,
and the artifact helpers directly.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.artifacts import TABLE_SPECS, load_snapshot_data, reconcile, table_schema
from app.config import Settings
from app.db import snapshot_reader


def _commit_sale(conn: sqlite3.Connection, product_id: str, qty: int, price: int) -> int:
    conn.execute("BEGIN IMMEDIATE")
    batch_id = conn.execute(
        "INSERT INTO write_batches(committed_at, sale_count, units) VALUES ('2026-01-01T00:00:00.000Z', 1, ?)"
        " RETURNING batch_id",
        (qty,),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO sales(product_id, quantity, unit_price_cents, sold_at_us, batch_id) VALUES (?,?,?,?,?)",
        (product_id, qty, price, int(time.time() * 1_000_000), batch_id),
    )
    conn.execute("UPDATE stock SET on_hand = on_hand - ? WHERE product_id = ?", (qty, product_id))
    conn.execute("UPDATE products SET cumulative_sold = cumulative_sold + ? WHERE product_id = ?", (qty, product_id))
    conn.execute("COMMIT")
    return batch_id


def test_read_transaction_sees_one_snapshot_while_writer_commits(settings: Settings) -> None:
    probe = sqlite3.connect(settings.business_db)
    product = probe.execute("SELECT product_id, unit_price_cents FROM products LIMIT 1").fetchone()
    probe.close()
    reads: list[tuple[int, int, int]] = []
    committed_during: list[int] = []
    stop = threading.Event()

    def keep_writing() -> None:
        writer = sqlite3.connect(settings.business_db, isolation_level=None, timeout=10)
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            while not stop.is_set():
                batch_id = _commit_sale(writer, product[0], 1, product[1])
                committed_during.append(batch_id)
                time.sleep(0.01)
        finally:
            writer.close()

    thread = threading.Thread(target=keep_writing, daemon=True)
    thread.start()
    try:
        with snapshot_reader(settings.business_db) as reader:
            # One transaction, deliberately slow reads over all three tables.
            for _ in range(3):
                batch = reader.execute("SELECT MAX(batch_id) FROM write_batches").fetchone()[0]
                sold = reader.execute("SELECT SUM(cumulative_sold) FROM products").fetchone()[0]
                units = reader.execute("SELECT SUM(quantity) FROM sales").fetchone()[0]
                reads.append((batch, sold, units))
                time.sleep(0.12)
    finally:
        stop.set()
        thread.join(timeout=5)

    assert len({row[0] for row in reads}) == 1, reads
    assert len({row[1] for row in reads}) == 1, reads
    assert len({row[2] for row in reads}) == 1, reads
    assert len(committed_during) >= 2, "writer must have committed while the snapshot was open"
    assert reads[0][1] == reads[0][2], reads


def test_reconcile_flags_stock_mismatch(tmp_path: Path) -> None:
    products = [
        {
            "product_id": "p1",
            "sku": "s1",
            "name": "n",
            "category": "c",
            "unit_price_cents": 100,
            "initial_stock": 10,
            "cumulative_sold": 3,
            "created_at": "t",
        }
    ]
    write_table(tmp_path / "products.parquet", "products", products)
    write_table(tmp_path / "stock.parquet", "stock", [{"product_id": "p1", "on_hand": 8}])
    write_table(
        tmp_path / "sales.parquet",
        "sales",
        [{"sale_id": 1, "product_id": "p1", "quantity": 3, "unit_price_cents": 100, "sold_at_us": 1, "batch_id": 1}],
    )

    data = load_snapshot_data(tmp_path)
    result = reconcile(data, business_batch_id=1)
    assert result["all_ok"] is False
    stock_check = next(item for item in result["items"] if item["id"] == "stock_balance")
    assert stock_check["ok"] is False
    sales_check = next(item for item in result["items"] if item["id"] == "sales_per_product")
    assert sales_check["ok"] is True


def write_table(path: Path, name: str, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows, schema=table_schema(name)), path)


def test_reconcile_flags_missing_product_in_stock(tmp_path: Path) -> None:
    products = [
        {
            "product_id": "p1",
            "sku": "s1",
            "name": "n",
            "category": "c",
            "unit_price_cents": 100,
            "initial_stock": 10,
            "cumulative_sold": 0,
            "created_at": "t",
        },
        {
            "product_id": "p2",
            "sku": "s2",
            "name": "n",
            "category": "c",
            "unit_price_cents": 100,
            "initial_stock": 10,
            "cumulative_sold": 0,
            "created_at": "t",
        },
    ]
    write_table(tmp_path / "products.parquet", "products", products)
    write_table(tmp_path / "stock.parquet", "stock", [{"product_id": "p1", "on_hand": 10}])
    write_table(tmp_path / "sales.parquet", "sales", [])
    result = reconcile(load_snapshot_data(tmp_path), business_batch_id=0)
    coverage = next(item for item in result["items"] if item["id"] == "coverage")
    assert coverage["ok"] is False
    assert result["all_ok"] is False


def test_parquet_schema_is_stable() -> None:
    assert [field.name for field in table_schema("sales")] == [column for column, _ in TABLE_SPECS["sales"]["columns"]]
    assert table_schema("sales").field("sale_id").type == pa.int64()
    assert table_schema("products").field("product_id").type == pa.string()
