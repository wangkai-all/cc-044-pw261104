from __future__ import annotations

import json
import os
import random
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .clock import iso_ms, now_iso

CATALOG = [
    ("P-1001", "手冲壶", "器具", 18900),
    ("P-1002", "滤纸 100 枚", "耗材", 3200),
    ("P-1003", "电子秤", "器具", 25900),
    ("P-1004", "磨豆机 C40", "器具", 98000),
    ("P-1005", "耶加雪菲 250g", "咖啡豆", 12800),
    ("P-1006", "哥伦比亚 250g", "咖啡豆", 10800),
    ("P-1007", "曼特宁 250g", "咖啡豆", 11800),
    ("P-1008", "冷萃瓶 1L", "器具", 16800),
    ("P-1009", "陶瓷马克杯", "周边", 7600),
    ("P-1010", "玻璃分享壶", "器具", 8800),
    ("P-1011", "法压壶 600ml", "器具", 13800),
    ("P-1012", "挂耳装 10 包", "咖啡豆", 6800),
    ("P-1013", "奶泡壶", "器具", 8900),
    ("P-1014", "咖啡豆密封罐", "周边", 5400),
    ("P-1015", "便携咖啡杯", "周边", 13900),
    ("P-1016", "爱乐压", "器具", 32800),
    ("P-1017", "肯尼亚 250g", "咖啡豆", 13800),
    ("P-1018", "清洁片 30 片", "耗材", 2600),
]

# Page cache is per connection. A deliberately small cache keeps an export from
# becoming trivially fast; the demo also throttles explicitly.
READER_CACHE_KIB = 4096


def business_connect(path: Path, *, read_only: bool = False, timeout: float = 10.0) -> sqlite3.Connection:
    if read_only:
        conn = sqlite3.connect(
            f"file:{path.as_posix()}?mode=ro",
            uri=True,
            timeout=timeout,
            isolation_level=None,
        )
        conn.execute("PRAGMA query_only=ON")
    else:
        conn = sqlite3.connect(path, timeout=timeout, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
    conn.row_factory = sqlite3.Row
    return conn


def tasks_connect(path: Path, timeout: float = 10.0) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=timeout, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS business_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS write_batches (
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    committed_at TEXT NOT NULL,
    sale_count INTEGER NOT NULL CHECK (sale_count >= 0),
    units INTEGER NOT NULL CHECK (units >= 0)
);

CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    initial_stock INTEGER NOT NULL CHECK (initial_stock >= 0),
    cumulative_sold INTEGER NOT NULL DEFAULT 0 CHECK (cumulative_sold >= 0),
    created_at TEXT NOT NULL,
    CHECK (cumulative_sold <= initial_stock)
);

CREATE TABLE IF NOT EXISTS stock (
    product_id TEXT PRIMARY KEY REFERENCES products(product_id),
    on_hand INTEGER NOT NULL CHECK (on_hand >= 0)
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    sold_at_us INTEGER NOT NULL,
    batch_id INTEGER NOT NULL REFERENCES write_batches(batch_id)
);

CREATE INDEX IF NOT EXISTS sales_batch_idx ON sales(batch_id);
CREATE INDEX IF NOT EXISTS sales_product_idx ON sales(product_id, quantity);

"""


TASKS_SCHEMA = """
CREATE TABLE IF NOT EXISTS export_tasks (
    export_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
    stage TEXT NOT NULL,
    progress REAL NOT NULL DEFAULT 0,
    rows_total INTEGER NOT NULL DEFAULT 0,
    rows_done INTEGER NOT NULL DEFAULT 0,
    business_batch_id INTEGER,
    snapshot_committed_at TEXT,
    requested_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    reused_from TEXT,
    error TEXT,
    files_json TEXT,
    checks_json TEXT,
    preview_json TEXT,
    speed REAL NOT NULL DEFAULT 0,
    manifest_sha256 TEXT,
    bundle_sha256 TEXT
);

CREATE TABLE IF NOT EXISTS verifications (
    verification_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    source TEXT NOT NULL,
    export_id TEXT,
    ok INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


def _now_iso() -> str:
    return now_iso()


def _iso_us(epoch_us: int) -> str:
    return iso_ms(epoch_us / 1_000_000)


_rfc3339_now = _now_iso


HISTORY_BATCHES = 12000


def seed_business(db_path: Path, history_batches: int = HISTORY_BATCHES) -> None:
    """Create and seed the demo business database once.

    Seeding is idempotent and guarded by BEGIN IMMEDIATE so the API process and
    the standalone writer can race to start without double-seeding.
    """
    conn = business_connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()

    seed_data(db_path, history_batches)


def seed_data(db_path: Path, history_batches: int = HISTORY_BATCHES) -> None:
    conn = business_connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        has_products = conn.execute("SELECT 1 FROM products LIMIT 1").fetchone()
        if has_products:
            conn.execute("COMMIT")
            return

        rng = random.Random(20261113)
        now = int(time.time() * 1_000_000)
        start = now - (history_batches + 1) * 3_000_000
        products = []
        stock = []
        for i, (sku, name, category, price) in enumerate(CATALOG):
            product_id = f"prod-{i + 1:03d}"
            initial = rng.randint(1800, 3400)
            products.append(
                (
                    product_id,
                    sku,
                    name,
                    category,
                    price,
                    initial,
                    0,
                    _iso_us(start),
                )
            )
            stock.append((product_id, initial))
        conn.executemany(
            "INSERT INTO products(product_id, sku, name, category, unit_price_cents,"
            " initial_stock, cumulative_sold, created_at) VALUES (?,?,?,?,?,?,?,?)",
            products,
        )
        conn.executemany("INSERT INTO stock(product_id, on_hand) VALUES (?,?)", stock)
        price_by_id = {p[0]: p[4] for p in products}

        # History respects per-product headroom (initial stock minus a 40-unit
        # reserve), so seed volume is independent of how much history is asked
        # for and the demo can start with tens of thousands of sales rows.
        headroom = {p[0]: p[5] - 40 for p in products}
        sales: list[tuple] = []
        batches = []
        for batch_no in range(1, history_batches + 1):
            committed_us = start + batch_no * 3_000_000
            choices = [pid for pid, left in headroom.items() if left >= 2]
            if not choices:
                break
            count = min(rng.randint(1, 3), len(choices))
            units = 0
            batch_sales = []
            for _ in range(count):
                product_id = rng.choice(choices)
                qty = rng.randint(1, 2)
                headroom[product_id] -= qty
                if headroom[product_id] < 2:
                    choices = [pid for pid in choices if pid != product_id or headroom[pid] >= 2]
                batch_sales.append(
                    (product_id, qty, price_by_id[product_id], committed_us + rng.randint(0, 2_000_000), batch_no)
                )
                units += qty
            sales.extend(batch_sales)
            batches.append((batch_no, _iso_us(committed_us), count, units))

        if sales:
            conn.executemany(
                "INSERT INTO write_batches(batch_id, committed_at, sale_count, units) VALUES (?,?,?,?)",
                batches,
            )
            conn.executemany(
                "INSERT INTO sales(product_id, quantity, unit_price_cents, sold_at_us, batch_id)"
                " VALUES (?,?,?,?,?)",
                sales,
            )
            per_product: dict[str, int] = {}
            for product_id, qty, *_ in sales:
                per_product[product_id] = per_product.get(product_id, 0) + qty
            conn.executemany(
                "UPDATE products SET cumulative_sold = cumulative_sold + ? WHERE product_id = ?",
                [(units, pid) for pid, units in per_product.items()],
            )
            conn.executemany(
                "UPDATE stock SET on_hand = on_hand - ? WHERE product_id = ?",
                [(units, pid) for pid, units in per_product.items()],
            )

        conn.execute(
            "INSERT OR REPLACE INTO business_meta(key, value) VALUES ('latest_batch_id', ?)",
            (str(max(0, history_batches)),),
        )
        conn.execute(
            "INSERT OR REPLACE INTO business_meta(key, value) VALUES ('latest_committed_at', ?)",
            (batches[-1][1] if batches else _rfc3339_now(),),
        )
        conn.execute("INSERT OR REPLACE INTO business_meta(key, value) VALUES ('seeded_at', ?)", (_rfc3339_now(),))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def ensure_tasks_schema(db_path: Path) -> None:
    conn = tasks_connect(db_path)
    try:
        conn.executescript(TASKS_SCHEMA)
        _ensure_column(conn, "export_tasks", "speed", "speed REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "export_tasks", "manifest_sha256", "manifest_sha256 TEXT")
        _ensure_column(conn, "export_tasks", "bundle_sha256", "bundle_sha256 TEXT")
        # An export that was running when the process died never published.
        conn.execute(
            "UPDATE export_tasks SET status='failed', finished_at=?,"
            " error=COALESCE(error, '进程中断，导出未完成'), stage='interrupted'"
            " WHERE status IN ('queued','running')",
            (_rfc3339_now(),),
        )
    finally:
        conn.close()


@contextmanager
def snapshot_reader(db_path: Path, on_first_read=None) -> Iterator[sqlite3.Connection]:
    """Open a read-only connection and hold one DEFERRED read transaction.

    The first SELECT fixes the WAL end mark. Every read after it, however slow,
    sees that same snapshot; this is what makes the three tables mutually
    consistent while the writer keeps committing.
    """
    conn = business_connect(db_path, read_only=True)
    try:
        conn.execute("BEGIN DEFERRED")
        if on_first_read is not None:
            on_first_read(conn)
        yield conn
    finally:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        conn.close()


def clamp_controls(raw: dict, base: dict) -> dict:
    """Merge raw writer/export values onto base, clamping to the allowed range."""
    from .config import CONTROL_LIMITS

    result = json.loads(json.dumps(base))
    sections = {
        "writer": ("enabled", "batches_per_second", "sales_per_batch"),
        "export": ("rows_per_second",),
    }
    for section, keys in sections.items():
        incoming = raw.get(section) if isinstance(raw, dict) else None
        if not isinstance(incoming, dict):
            continue
        for key in keys:
            if key not in incoming:
                continue
            value = incoming[key]
            if key == "enabled":
                if isinstance(value, bool):
                    result[section][key] = value
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            low, high = CONTROL_LIMITS[key]
            value = min(high, max(low, float(value)))
            result[section][key] = int(round(value)) if key == "sales_per_batch" else round(value, 2)
    return result


def read_controls(path: Path) -> dict:
    from .config import DEFAULT_CONTROLS

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raw = {}
    return clamp_controls(raw, DEFAULT_CONTROLS)


def write_controls(path: Path, controls: dict) -> dict:
    from .config import DEFAULT_CONTROLS

    merged = clamp_controls(controls, DEFAULT_CONTROLS)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return merged
