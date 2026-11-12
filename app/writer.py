"""Standalone demo writer.

Run this as a *separate process* from the API: `python -m app.writer`. It owns
all business writes and never opens the task database. Every sale, stock
decrement and product cumulative-sold increment for one business event is
committed in a single IMMEDIATE transaction together with its batch row.
"""

from __future__ import annotations

import json
import os
import random
import signal
import sqlite3
import sys
import time
from pathlib import Path

from .config import Settings
from .db import _iso_us, business_connect, read_controls, seed_business

STOP = False


def _stop(*_args) -> None:
    global STOP
    STOP = True


def _atomic_write_json(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _pick_products(conn: sqlite3.Connection, limit: int, rng: random.Random) -> list[sqlite3.Row]:
    rows = conn.execute(
        "SELECT p.product_id, p.unit_price_cents FROM products p"
        " JOIN stock s ON s.product_id = p.product_id"
        " WHERE s.on_hand > 0 AND p.initial_stock - p.cumulative_sold >= 6"
        " ORDER BY p.product_id LIMIT ?",
        (limit,),
    ).fetchall()
    return list(rows)


def run_batch(conn: sqlite3.Connection, rng: random.Random, sales_per_batch: int) -> dict:
    chosen = _pick_products(conn, sales_per_batch, rng)
    if not chosen:
        return {"sale_count": 0, "units": 0, "batch_id": None}

    now_us = time.time_ns() // 1_000
    committed_at = _iso_us(now_us)
    conn.execute("BEGIN IMMEDIATE")
    try:
        # RETURNING gives the batch id before the sale rows are staged. The
        # whole transaction - batch row, stock, product counter, sales - is one
        # commit, so exporters never observe a torn batch.
        batch_id = conn.execute(
            "INSERT INTO write_batches(committed_at, sale_count, units) VALUES (?, 0, 0)"
            " RETURNING batch_id",
            (committed_at,),
        ).fetchone()[0]
        sale_rows = []
        total_units = 0
        for row in chosen:
            product_id = row["product_id"]
            stock_row = conn.execute("SELECT on_hand FROM stock WHERE product_id=?", (product_id,)).fetchone()
            if stock_row is None or stock_row["on_hand"] <= 0:
                continue
            qty = 1 if rng.random() < 0.72 else 2
            qty = min(qty, stock_row["on_hand"])
            if qty <= 0:
                continue
            sale_rows.append((product_id, qty, row["unit_price_cents"], now_us, batch_id))
            total_units += qty
        if not sale_rows:
            conn.execute("ROLLBACK")
            return {"sale_count": 0, "units": 0, "batch_id": None}

        conn.executemany(
            "INSERT INTO sales(product_id, quantity, unit_price_cents, sold_at_us, batch_id)"
            " VALUES (?,?,?,?,?)",
            sale_rows,
        )
        for product_id, qty, *_ in sale_rows:
            conn.execute(
                "UPDATE stock SET on_hand = on_hand - ? WHERE product_id = ?",
                (qty, product_id),
            )
            conn.execute(
                "UPDATE products SET cumulative_sold = cumulative_sold + ? WHERE product_id = ?",
                (qty, product_id),
            )
        conn.execute(
            "UPDATE write_batches SET sale_count=?, units=? WHERE batch_id=?",
            (len(sale_rows), total_units, batch_id),
        )
        conn.execute(
            "INSERT OR REPLACE INTO business_meta(key, value) VALUES ('latest_batch_id', ?)",
            (str(batch_id),),
        )
        conn.execute(
            "INSERT OR REPLACE INTO business_meta(key, value) VALUES ('latest_committed_at', ?)",
            (committed_at,),
        )
        conn.execute("COMMIT")
        return {"sale_count": len(sale_rows), "units": total_units, "batch_id": batch_id}
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def main() -> int:
    settings = Settings.from_env()
    settings.ensure_dirs()
    seed_business(settings.business_db, settings.seed_history_batches)
    read_controls(settings.controls_file)

    rng = random.Random(7_110_131)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    conn = business_connect(settings.business_db)
    batches_committed = 0
    recent_commits: list[float] = []
    last_batch_id = 0
    last_error: str | None = None
    last_commit_at = 0.0
    started = time.monotonic()

    # Status file lets the UI distinguish "writer alive but paused" from "dead".
    while not STOP:
        controls = read_controls(settings.controls_file)
        writer = controls["writer"]
        rate = float(writer["batches_per_second"])
        interval = 1.0 / rate if rate > 0 else 1.0
        cycle_started = time.monotonic()

        if not writer.get("enabled", True):
            outcome = None
        else:
            try:
                outcome = run_batch(conn, rng, int(writer["sales_per_batch"]))
                if outcome["batch_id"] is not None:
                    last_batch_id = outcome["batch_id"]
                    batches_committed += 1
                    last_commit_at = time.time()
                    recent_commits.append(last_commit_at)
                    recent_commits = recent_commits[-20:]
                    last_error = None
            except sqlite3.OperationalError as exc:
                last_error = str(exc)
                time.sleep(0.2)
                outcome = None
            except sqlite3.IntegrityError as exc:
                # Guard rail: the demo data must never drive stock negative.
                last_error = str(exc)
                time.sleep(0.2)
                outcome = None

        try:
            status = {
                "pid": os.getpid(),
                "state": ("paused" if not writer.get("enabled", True) else ("error" if last_error else "running")),
                "batches_committed": batches_committed,
                "last_batch_id": last_batch_id,
                "last_commit_epoch": last_commit_at,
                "last_error": last_error,
                "recent_commits": recent_commits,
                "uptime_seconds": round(time.monotonic() - started, 1),
                "updated_epoch": time.time(),
            }
            _atomic_write_json(settings.writer_status_file, status)
        except OSError:
            pass

        spent = time.monotonic() - cycle_started
        remaining = interval - spent
        while remaining > 0 and not STOP:
            step = min(0.15, remaining)
            time.sleep(step)
            remaining -= step

    _atomic_write_json(
        settings.writer_status_file,
        {
            "pid": os.getpid(),
            "state": "stopped",
            "batches_committed": batches_committed,
            "last_batch_id": last_batch_id,
            "last_commit_epoch": last_commit_at,
            "last_error": last_error,
            "recent_commits": recent_commits,
            "updated_epoch": time.time(),
        },
    )
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
