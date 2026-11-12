"""Export worker: one read-only transaction, three Parquet files, verify, publish.

Publication is all-or-nothing. Everything is written under a private staging
directory; only after the reconciliation and manifest hash computation pass do
the artifacts get atomically renamed into `exports/<export_id>/`. A failure or
cancel removes the staging directory and leaves only the task record, so a
half-written batch is never downloadable.
"""

from __future__ import annotations

import json
import os
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import artifacts
from .artifacts import (
    TABLE_SPECS,
    build_bundle,
    build_manifest,
    file_record,
    fsync_dir,
    reconcile,
    table_schema,
    write_atomic,
)
from .config import Settings
from .db import snapshot_reader
from .tasks import TaskStore, now_iso

READ_BATCH_ROWS = 4000


class ExportCancelled(Exception):
    pass


@dataclass
class ExportState:
    export_id: str
    business_batch_id: int = 0
    snapshot_committed_at: str | None = None
    rows_total: int = 0
    rows_done: int = 0
    tables: dict = field(default_factory=dict)
    throttle: dict = field(default_factory=dict)
    started_at: float = field(default_factory=time.monotonic)


def _export_rate(controls_path: Path) -> float:
    try:
        raw = json.loads(controls_path.read_text(encoding="utf-8"))
        return max(0.0, float(raw.get("export", {}).get("rows_per_second", 0) or 0))
    except (OSError, ValueError, TypeError, AttributeError):
        return 0.0


class ExportWorker:
    def __init__(self, settings: Settings, store: TaskStore):
        self.settings = settings
        self.store = store
        self._queue: "queue.Queue[str | None]" = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._active: set[str] = set()

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="export-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._queue.put(None)

    def enqueue(self, export_id: str) -> None:
        self._queue.put(export_id)

    def has_capacity(self) -> bool:
        with self._lock:
            return len(self._active) < self.settings.max_pending_exports

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    # -- worker loop -------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                export_id = self._queue.get(timeout=0.3)
            except queue.Empty:
                export_id = self._claim_queued()
            if export_id is None:
                if self._stop.is_set():
                    break
                time.sleep(0.2)
                continue
            with self._lock:
                self._active.add(export_id)
            try:
                task = self.store.get(export_id)
                if task is not None and task["status"] == "queued":
                    self._execute(task)
            except Exception as exc:  # never kill the worker thread
                try:
                    self.store.fail(export_id, "failed", f"导出线程异常：{exc}")
                except Exception:
                    pass
            finally:
                with self._lock:
                    self._active.discard(export_id)

    def _claim_queued(self, exclude: set[str] | None = None) -> str | None:
        with self._lock:
            active = set(self._active)
        for task in self.store.list(limit=100):
            if task["status"] == "queued" and task["export_id"] not in active:
                return task["export_id"]
        return None

    # -- one export --------------------------------------------------------
    def _execute(self, task: dict) -> None:
        export_id = task["export_id"]
        staging = self.settings.staging_dir / export_id
        artifacts.cleanup_dir(staging)
        staging.mkdir(parents=True, exist_ok=True)
        state = ExportState(export_id=export_id)
        if task.get("cancel_requested"):
            artifacts.cleanup_dir(staging)
            self.store.fail(export_id, "cancelled", "已取消；未发布任何文件")
            return
        try:
            with snapshot_reader(
                self.settings.business_db,
                on_first_read=lambda conn: self._snapshot_metadata(conn, state),
            ) as conn:
                self._check_cancel(export_id)

                # Every business change bumps the batch id, so a snapshot
                # anchored at the same batch id has identical content. If a
                # batch was already published, this task reuses its original
                # files instead of producing a second copy.
                original = self.store.find_succeeded_for_batch(state.business_batch_id, export_id)
                if original is not None:
                    self._reuse(export_id, original, state)
                    return

                if not self.store.mark_running(
                    export_id,
                    state.business_batch_id,
                    state.snapshot_committed_at or "",
                    state.rows_total,
                ):
                    raise ExportCancelled()
                self._write_tables(conn, staging, state)

            self._check_cancel(export_id)
            self._finalize(staging, state)
        except ExportCancelled:
            artifacts.cleanup_dir(staging)
            self.store.fail(export_id, "cancelled", "已取消；未发布任何文件")
        except Exception as exc:
            artifacts.cleanup_dir(staging)
            self.store.fail(export_id, "failed", f"导出失败：{exc}")

    def _reuse(self, export_id: str, original: str, state: ExportState) -> None:
        """Adopt an already-published batch; no new files are written."""
        source = self.store.get(original)
        if source is None or source["status"] != "succeeded" or not source["files"]:
            raise RuntimeError("可复用的批次记录不完整")
        artifacts.cleanup_dir(self.settings.staging_dir / export_id)
        self.store.succeed(
            export_id,
            business_batch_id=state.business_batch_id,
            snapshot_committed_at=state.snapshot_committed_at or "",
            manifest={
                "tables": source["files"],
                "business": {
                    "batch_id": state.business_batch_id,
                    "committed_at": state.snapshot_committed_at,
                    "snapshot": "read-transaction",
                },
            },
            checks=source["checks"],
            preview=source["previews"],
            rows_total=source["rows_total"],
            manifest_sha256=source.get("manifest_sha256"),
            bundle_sha256=source.get("bundle_sha256"),
            reused_from=original,
        )

    def _snapshot_metadata(self, conn: sqlite3.Connection, state: ExportState) -> None:
        """First read in the transaction: fixes the WAL end mark and the batch.

        The batch row is read before anything else, so the recorded id is the
        commit the snapshot is anchored to - not a later commit that landed
        while the export was busy.
        """
        latest = conn.execute("SELECT batch_id, committed_at FROM write_batches ORDER BY batch_id DESC LIMIT 1").fetchone()
        state.business_batch_id = int(latest["batch_id"]) if latest else 0
        state.snapshot_committed_at = latest["committed_at"] if latest else None
        total = 0
        for name in TABLE_SPECS:
            total += conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        state.rows_total = total

    def _check_cancel(self, export_id: str) -> None:
        if self.store.is_cancel_requested(export_id):
            raise ExportCancelled()

    def _rate(self, state: ExportState) -> float:
        """Current export speed, re-read about once a second.

        A live slider therefore takes effect within a second or two instead of
        only on the next export.
        """
        throttle = state.throttle
        now = time.monotonic()
        if not throttle:
            throttle.update(rate=_export_rate(self.settings.controls_file), refreshed=now)
        elif now - throttle["refreshed"] > 1.0:
            throttle["rate"] = _export_rate(self.settings.controls_file)
            throttle["refreshed"] = now
        return float(throttle["rate"])

    def _pace(self, rows: int, rate: float, started: float) -> None:
        """Sleep for the reading time those rows should have taken."""
        if rate <= 0:
            return
        target = rows / rate
        spent = time.monotonic() - started
        if target > spent:
            time.sleep(target - spent)

    def _write_tables(self, conn: sqlite3.Connection, staging: Path, state: ExportState) -> None:
        for name, spec in TABLE_SPECS.items():
            self._check_cancel(state.export_id)
            target = staging / spec["file"]
            schema = table_schema(name)
            columns = [column for column, _ in spec["columns"]]
            writer = pq.ParquetWriter(target, schema, compression="zstd", use_dictionary=False)
            written = 0
            try:
                cursor = conn.execute(f"SELECT {', '.join(columns)} FROM {name} ORDER BY {spec['sort']}")
                while True:
                    self._check_cancel(state.export_id)
                    rows = cursor.fetchmany(READ_BATCH_ROWS)
                    if not rows:
                        break
                    rate = self._rate(state)
                    # When throttled, write the fetched rows in slices worth
                    # roughly 0.4s each so progress moves smoothly on screen.
                    slice_rows = len(rows) if rate <= 0 else max(1, min(len(rows), int(rate * 0.4)))
                    for offset in range(0, len(rows), slice_rows):
                        slice_started = time.monotonic()
                        self._check_cancel(state.export_id)
                        chunk = rows[offset : offset + slice_rows]
                        columns_data = list(zip(*chunk))
                        table = pa.Table.from_arrays(
                            [pa.array(values, type=field.type) for field, values in zip(schema, columns_data)],
                            schema=schema,
                        )
                        writer.write_table(table, row_group_size=len(chunk))
                        written += len(chunk)
                        state.rows_done += len(chunk)
                        elapsed = time.monotonic() - state.started_at
                        speed = state.rows_done / elapsed if elapsed > 0 else 0.0
                        self.store.progress(
                            state.export_id,
                            stage=f"reading:{name}",
                            rows_done=state.rows_done,
                            rows_total=state.rows_total,
                            speed=speed,
                        )
                        self._pace(len(chunk), rate, slice_started)
            finally:
                writer.close()
            state.tables[name] = written
            fsync_dir(staging)

    # -- verify + publish --------------------------------------------------
    def _finalize(self, staging: Path, state: ExportState) -> None:
        self.store.progress(state.export_id, stage="verifying")
        self._check_cancel(state.export_id)

        manifest_tables = []
        # Re-derive metadata from the files on disk, never from what we think
        # we wrote.
        for name, spec in TABLE_SPECS.items():
            path = staging / spec["file"]
            record = file_record(path)
            record["name"] = name
            record["rows"] = pq.read_metadata(path).num_rows
            record["schema"] = artifacts.schema_entries(pq.read_schema(path))
            manifest_tables.append(record)

        counts = {entry["name"]: entry["rows"] for entry in manifest_tables}
        state.tables = counts
        if sum(counts.values()) != state.rows_done:
            raise RuntimeError(f"行数统计不一致：写出 {state.rows_done} 行，文件合计 {sum(counts.values())} 行")

        data = artifacts.load_snapshot_data(staging)
        checks = reconcile(data, business_batch_id=state.business_batch_id, expected_counts=counts)
        if not checks["all_ok"]:
            failed = "；".join(item["detail"] for item in checks["items"] if not item["ok"])
            raise RuntimeError(f"勾稽校验未通过：{failed}")

        self._check_cancel(state.export_id)
        manifest = build_manifest(
            export_id=state.export_id,
            created_at=now_iso(),
            business={
                "batch_id": state.business_batch_id,
                "committed_at": state.snapshot_committed_at,
                "snapshot": "read-transaction",
            },
            files=manifest_tables,
            checks=checks,
            table_rows=counts,
            preview_rows=self.settings.preview_rows,
        )
        manifest_path = staging / "manifest.json"
        write_atomic(manifest_path, artifacts.canonical_json(manifest))
        manifest_sha = artifacts.sha256_file(manifest_path)
        write_atomic(staging / "manifest.sha256", f"{manifest_sha}  manifest.json\n")
        write_atomic(
            staging / "preview.json",
            artifacts.canonical_json(artifacts.preview_bundle(staging, manifest, self.settings.preview_rows)),
        )

        bundle_path = staging / "bundle.zip"
        build_bundle(staging, bundle_path)
        bundle_sha = artifacts.sha256_file(bundle_path)
        write_atomic(staging / "bundle.zip.sha256", f"{bundle_sha}  bundle.zip\n")
        fsync_dir(staging)

        self.store.progress(state.export_id, stage="publishing")
        target_dir = self.settings.exports_dir / state.export_id
        if target_dir.exists():
            # Left behind by a previous crashed attempt for this id; never a
            # successfully published one because ids are unique.
            artifacts.cleanup_dir(target_dir)
        os.replace(staging, target_dir)
        fsync_dir(self.settings.exports_dir)

        preview = json.loads((target_dir / "preview.json").read_text(encoding="utf-8"))
        if self.store.is_cancel_requested(state.export_id):
            # Published bytes are complete; the cancel arrived after the point
            # of no return. Report success rather than hide a valid batch.
            pass
        self.store.succeed(
            state.export_id,
            business_batch_id=state.business_batch_id,
            snapshot_committed_at=state.snapshot_committed_at or "",
            manifest=manifest,
            checks=checks,
            preview=preview,
            rows_total=state.rows_done,
            manifest_sha256=manifest_sha,
            bundle_sha256=bundle_sha,
        )
