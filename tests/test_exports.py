from __future__ import annotations

import io
import json
import sqlite3
import time
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
from conftest import read_business, wait_for


def _export(client) -> dict:
    response = client.post("/api/exports")
    assert response.status_code == 202, response.text
    return wait_for(client, response.json()["export_id"])


def test_export_publishes_three_parquet_files_with_manifest(client) -> None:
    task = _export(client)
    assert task["status"] == "succeeded", task
    assert task["files"] is not None and len(task["files"]) == 3
    assert {entry["name"] for entry in task["files"]} == {"products", "stock", "sales"}
    manifest = task["manifest"]
    assert manifest["row_counts"] == {entry["name"]: entry["rows"] for entry in task["files"]}
    assert manifest["business"]["batch_id"] == task["business_batch_id"]
    assert manifest["reader"]["journal_mode"] == "wal"


def test_published_files_are_reconcilable_and_have_downloadable_bytes(client, settings) -> None:
    task = _export(client)
    directory = settings.exports_dir / task["export_id"]
    products = pq.read_table(directory / "products.parquet").to_pylist()
    stock = {row["product_id"]: row["on_hand"] for row in pq.read_table(directory / "stock.parquet").to_pylist()}
    sales = pq.read_table(directory / "sales.parquet").to_pylist()

    for product in products:
        assert stock[product["product_id"]] == product["initial_stock"] - product["cumulative_sold"]
    sold = {}
    for row in sales:
        sold[row["product_id"]] = sold.get(row["product_id"], 0) + row["quantity"]
    assert all(sold.get(product["product_id"], 0) == product["cumulative_sold"] for product in products)
    assert sum(sold.values()) == sum(product["cumulative_sold"] for product in products)

    manifest_sha = (directory / "manifest.sha256").read_text().split()[0]
    response = client.get(f"/api/exports/{task['export_id']}/files/manifest.json")
    assert response.status_code == 200
    import hashlib

    assert hashlib.sha256(response.content).hexdigest() == manifest_sha


def test_repeat_download_returns_identical_bytes(client) -> None:
    task = _export(client)
    first = client.get(f"/api/exports/{task['export_id']}/download")
    second = client.get(f"/api/exports/{task['export_id']}/download")
    assert first.status_code == second.status_code == 200
    assert first.content == second.content
    with zipfile.ZipFile(io.BytesIO(first.content)) as archive:
        names = set(archive.namelist())
    assert {"manifest.json", "manifest.sha256", "products.parquet", "stock.parquet", "sales.parquet"} <= names


def test_download_refused_before_publish(client) -> None:
    assert client.get("/api/exports/nope/download").status_code == 404


def test_cancel_leaves_no_files(client) -> None:
    client.put("/api/controls", json={"rows_per_second": 5.0})
    created = client.post("/api/exports").json()["export_id"]
    time.sleep(1.0)
    response = client.post(f"/api/exports/{created}/cancel")
    assert response.status_code == 200
    task = wait_for(client, created)
    assert task["status"] == "cancelled"
    assert client.get(f"/api/exports/{created}/download").status_code == 409
    data_dir = Path(client.get("/api/health").json()["data_dir"])
    assert not (data_dir / "exports" / created).exists()
    assert not (data_dir / "staging" / created).exists()


def test_concurrent_writer_never_tears_snapshot(client, writer_process, settings) -> None:
    """The export's three tables must all match one committed batch.

    The writer keeps committing while the export reads, and the export is
    deliberately slowed so several commits land mid-read. The manifest records
    the batch visible at the transaction's first read; the three files must
    reconcile against each other, and sales must not include a batch newer than
    the recorded snapshot.
    """
    row = read_business(settings).execute(
        "SELECT COUNT(*) FROM sales"
    ).fetchone()
    assert row[0] > 0
    client.put("/api/controls", json={"rows_per_second": 30.0})

    tasks = []
    for _ in range(3):
        export_id = client.post("/api/exports").json()["export_id"]
        tasks.append(export_id)
    finished = [wait_for(client, export_id, timeout=90) for export_id in tasks]
    all_succeeded = [task for task in finished if task["status"] == "succeeded"]
    assert all_succeeded, finished

    for task in all_succeeded:
        checks = task["checks"]
        assert checks["all_ok"], checks
        directory = settings.exports_dir / task["export_id"]
        products = pq.read_table(directory / "products.parquet").to_pylist()
        stock = {row["product_id"]: row["on_hand"] for row in pq.read_table(directory / "stock.parquet").to_pylist()}
        sales = pq.read_table(directory / "sales.parquet").to_pylist()
        assert max(row["batch_id"] for row in sales) <= task["business_batch_id"]

        sold = {}
        for row in sales:
            sold[row["product_id"]] = sold.get(row["product_id"], 0) + row["quantity"]
        for product in products:
            assert stock[product["product_id"]] == product["initial_stock"] - product["cumulative_sold"]
            assert sold.get(product["product_id"], 0) == product["cumulative_sold"]

    status = json.loads(settings.writer_status_file.read_text())
    baseline = min(task["business_batch_id"] for task in all_succeeded)
    # Writers kept committing while the slowed exports read: the DB moved past
    # the oldest snapshot, yet each export stayed internally consistent.
    assert status["last_batch_id"] > baseline

    # At rest in the live database, every batch's sales match the product
    # counters, so the writer's three-way commit is atomic too.
    conn = read_business(settings)
    product_total = conn.execute("SELECT SUM(cumulative_sold) FROM products").fetchone()[0]
    sales_total = conn.execute("SELECT SUM(quantity) FROM sales").fetchone()[0]
    assert product_total == sales_total
    orphan_batches = conn.execute(
        "SELECT COUNT(*) FROM sales s LEFT JOIN write_batches b ON b.batch_id = s.batch_id WHERE b.batch_id IS NULL"
    ).fetchone()[0]
    assert orphan_batches == 0
    zero_stock = conn.execute("SELECT COUNT(*) FROM products WHERE initial_stock - cumulative_sold < 0").fetchone()[0]
    assert zero_stock == 0


def test_verification_of_stored_export_passes(client) -> None:
    task = _export(client)
    response = client.post("/api/verifications", data={"export_id": task["export_id"]})
    assert response.status_code == 200
    report = response.json()
    assert report["ok"] is True
    ids = {item["id"] for item in report["checks"]}
    assert {"manifest_sha", "sha_products", "sha_stock", "sha_sales", "stock_balance", "sales_per_product"} <= ids


def test_verification_detects_tampered_file(client, settings) -> None:
    task = _export(client)
    path = settings.exports_dir / task["export_id"] / "stock.parquet"
    original = path.read_bytes()
    # Append a valid Parquet footer? Simpler: rewrite with an extra row.
    table = pq.read_table(path)
    import pyarrow as pa

    altered = pa.concat_tables([table, table.slice(0, 1)])
    pq.write_table(altered, path)
    try:
        report = client.post("/api/verifications", data={"export_id": task["export_id"]}).json()
    finally:
        path.write_bytes(original)
    assert report["ok"] is False
    sha_check = next(item for item in report["checks"] if item["id"] == "sha_stock")
    assert sha_check["ok"] is False


def test_verification_detects_broken_reconciliation(client, settings) -> None:
    """A self-consistent tampered bundle still fails the three-table check.

    The altered products file gets a matching hash and size in the manifest,
    and manifest.sha256 is recomputed too - so every hash check passes and the
    reconciliation must catch the broken stock/sales identity on its own.
    """
    import pyarrow as pa

    from app import artifacts

    task = _export(client)
    source = settings.exports_dir / task["export_id"]
    work = settings.staging_dir / "tamper-test"
    work.mkdir(parents=True, exist_ok=True)
    for name in ("manifest.json", "products.parquet", "stock.parquet", "sales.parquet"):
        (work / name).write_bytes((source / name).read_bytes())

    products = pq.read_table(work / "products.parquet")
    changed = products.to_pylist()
    changed[0]["cumulative_sold"] += 1
    altered = pa.Table.from_pylist(changed, schema=products.schema)
    pq.write_table(altered, work / "products.parquet")

    manifest = json.loads((work / "manifest.json").read_text())
    for entry in manifest["tables"]:
        if entry["name"] == "products":
            entry["bytes"] = (work / "products.parquet").stat().st_size
            entry["sha256"] = artifacts.sha256_file(work / "products.parquet")
    manifest_path = work / "manifest.json"
    manifest_path.write_text(artifacts.canonical_json(manifest), encoding="utf-8")
    (work / "manifest.sha256").write_text(f"{artifacts.sha256_file(manifest_path)}  manifest.json\n", encoding="utf-8")

    tampered = settings.staging_dir / "tampered-bundle.zip"
    with zipfile.ZipFile(tampered, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in ("manifest.json", "manifest.sha256", "products.parquet", "stock.parquet", "sales.parquet"):
            archive.write(work / name, name)

    with tampered.open("rb") as handle:
        report = client.post("/api/verifications", files={"upload": ("tampered.zip", handle, "application/zip")}).json()

    assert report["ok"] is False
    checks = {item["id"]: item for item in report["checks"]}
    assert checks["sha_products"]["ok"] is True
    assert checks["manifest_sha"]["ok"] is True
    assert checks["stock_balance"]["ok"] is False
    assert checks["sales_per_product"]["ok"] is False
    assert report["upload_name"] == "tampered.zip"


def test_verification_rejects_bad_upload(client) -> None:
    response = client.post("/api/verifications", files={"upload": ("x.zip", b"not a zip", "application/zip")})
    assert response.status_code == 400
    assert "zip" in response.json()["detail"]


def test_verification_accepts_downloaded_bundle(client, tmp_path: Path) -> None:
    task = _export(client)
    bundle = client.get(f"/api/exports/{task['export_id']}/download")
    assert bundle.status_code == 200
    upload_path = tmp_path / "downloaded.zip"
    upload_path.write_bytes(bundle.content)
    with upload_path.open("rb") as handle:
        report = client.post("/api/verifications", files={"upload": ("downloaded.zip", handle, "application/zip")}).json()
    assert report["ok"] is True
    assert report["source"] == "upload"


def test_controls_are_clamped(client) -> None:
    response = client.put("/api/controls", json={"rows_per_second": 99999, "batches_per_second": 1000, "sales_per_batch": 0})
    assert response.status_code == 200
    controls = response.json()["controls"]
    assert controls["export"]["rows_per_second"] == 5000
    assert controls["writer"]["batches_per_second"] == 30
    assert controls["writer"]["sales_per_batch"] == 1


def test_queue_limit(client) -> None:
    client.put("/api/controls", json={"rows_per_second": 20.0})
    accepted = 0
    for _ in range(6):
        code = client.post("/api/exports").status_code
        if code == 202:
            accepted += 1
        else:
            assert code == 429, code
    assert accepted >= 1


def test_snapshot_isolation_against_concurrent_writer(client, settings) -> None:
    """Same-batch exports share a snapshot batch, and check against the DB."""
    conn = sqlite3.connect(settings.business_db, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO write_batches(committed_at, sale_count, units) VALUES ('2026-01-01T00:00:00.000Z', 1, 1)"
    )
    batch_id = conn.execute("SELECT batch_id FROM write_batches ORDER BY batch_id DESC LIMIT 1").fetchone()[0]
    product = conn.execute("SELECT product_id, unit_price_cents, initial_stock, cumulative_sold FROM products LIMIT 1").fetchone()
    conn.execute(
        "INSERT INTO sales(product_id, quantity, unit_price_cents, sold_at_us, batch_id) VALUES (?,?,?,?,?)",
        (product[0], 1, product[1], int(time.time() * 1_000_000), batch_id),
    )
    conn.execute("UPDATE stock SET on_hand = on_hand - 1 WHERE product_id = ?", (product[0],))
    conn.execute("UPDATE products SET cumulative_sold = cumulative_sold + 1 WHERE product_id = ?", (product[0],))
    conn.commit()
    conn.close()

    task = _export(client)
    assert task["checks"]["all_ok"] is True
    assert task["business_batch_id"] >= batch_id
