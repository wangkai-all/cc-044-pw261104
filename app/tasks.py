from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .db import tasks_connect


from .clock import now_iso  # noqa: F401  (re-exported for callers)


class TaskStore:
    """Small SQLite-backed task store. One connection per operation."""

    def __init__(self, path: Path):
        self.path = path

    def _conn(self) -> sqlite3.Connection:
        return tasks_connect(self.path)

    def create_export(self, export_id: str, requested_at: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO export_tasks(export_id, status, stage, requested_at)"
                " VALUES (?, 'queued', 'queued', ?)",
                (export_id, requested_at),
            )

    def mark_running(
        self, export_id: str, business_batch_id: int, snapshot_committed_at: str, rows_total: int
    ) -> bool:
        """Move queued -> running. Returns False if the task was cancelled first."""
        with self._conn() as conn:
            rowcount = conn.execute(
                "UPDATE export_tasks SET status='running', stage='snapshot', started_at=?,"
                " business_batch_id=?, snapshot_committed_at=?, rows_total=?, rows_done=0, progress=0"
                " WHERE export_id=? AND status='queued' AND cancel_requested=0",
                (now_iso(), business_batch_id, snapshot_committed_at, rows_total, export_id),
            ).rowcount
        return rowcount > 0

    def progress(
        self,
        export_id: str,
        *,
        stage: str | None = None,
        rows_done: int | None = None,
        rows_total: int | None = None,
        speed: float | None = None,
    ) -> None:
        fields = []
        params: list[Any] = []
        if stage is not None:
            fields.append("stage=?")
            params.append(stage)
        if rows_done is not None:
            fields.append("rows_done=?")
            params.append(rows_done)
        if rows_total is not None:
            fields.append("rows_total=?")
            params.append(rows_total)
        if speed is not None:
            fields.append("speed=?")
            params.append(round(speed, 2))
        fields.append(
            "progress=CASE WHEN rows_total > 0 THEN MIN(1.0, CAST(rows_done AS REAL) / rows_total) ELSE progress END"
        )
        if not fields:
            return
        params.append(export_id)
        with self._conn() as conn:
            conn.execute(f"UPDATE export_tasks SET {', '.join(fields)} WHERE export_id=?", params)

    def request_cancel(self, export_id: str) -> bool:
        with self._conn() as conn:
            rows = conn.execute(
                "UPDATE export_tasks SET cancel_requested=1 WHERE export_id=? AND status IN ('queued','running')",
                (export_id,),
            ).rowcount
        return rows > 0

    def is_cancel_requested(self, export_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute("SELECT cancel_requested FROM export_tasks WHERE export_id=?", (export_id,)).fetchone()
        return bool(row and row["cancel_requested"])

    def get(self, export_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM export_tasks WHERE export_id=?", (export_id,)).fetchone()
        return _task_row(row) if row else None

    def list(self, limit: int = 30) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM export_tasks ORDER BY requested_at DESC, export_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_task_row(row) for row in rows]

    def find_succeeded_for_batch(self, business_batch_id: int, exclude: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT export_id FROM export_tasks WHERE status='succeeded' AND business_batch_id=?"
                " AND export_id<>? ORDER BY finished_at LIMIT 1",
                (business_batch_id, exclude),
            ).fetchone()
        return str(row["export_id"]) if row else None

    def succeed(
        self,
        export_id: str,
        *,
        business_batch_id: int,
        snapshot_committed_at: str,
        manifest: dict,
        checks: dict,
        preview: dict,
        rows_total: int,
        manifest_sha256: str | None = None,
        bundle_sha256: str | None = None,
        reused_from: str | None = None,
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE export_tasks SET status='succeeded', stage='published', progress=1,"
                " rows_done=?, rows_total=?, business_batch_id=?, snapshot_committed_at=?,"
                " finished_at=?, files_json=?, checks_json=?, preview_json=?, reused_from=?, error=NULL,"
                " manifest_sha256=?, bundle_sha256=?"
                " WHERE export_id=?",
                (
                    rows_total,
                    rows_total,
                    business_batch_id,
                    snapshot_committed_at,
                    now_iso(),
                    json.dumps(manifest["tables"], ensure_ascii=False),
                    json.dumps(checks, ensure_ascii=False),
                    json.dumps(preview, ensure_ascii=False),
                    reused_from,
                    manifest_sha256,
                    bundle_sha256,
                    export_id,
                ),
            )

    def fail(self, export_id: str, status: str, error: str, *, checks: dict | None = None) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE export_tasks SET status=?, stage=?, finished_at=?, error=?,"
                " checks_json=COALESCE(?, checks_json)"
                " WHERE export_id=?",
                (
                    status,
                    "cancelled" if status == "cancelled" else "failed",
                    now_iso(),
                    error,
                    json.dumps(checks, ensure_ascii=False) if checks else None,
                    export_id,
                ),
            )

    def add_verification(self, record: dict) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO verifications(verification_id, created_at, source, export_id, ok, report_json)"
                " VALUES (?,?,?,?,?,?)",
                (
                    record["verification_id"],
                    record["created_at"],
                    record["source"],
                    record.get("export_id"),
                    1 if record["ok"] else 0,
                    json.dumps(record, ensure_ascii=False),
                ),
            )

    def list_verifications(self, limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT report_json FROM verifications ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [json.loads(row["report_json"]) for row in rows]


_JSON_FIELD_MAP = {
    "files_json": "files",
    "checks_json": "checks",
    "preview_json": "previews",
}


MANIFEST_KEYS = ("schema_version", "business", "row_counts", "reader", "created_at")


def _manifest_from_row(row: dict) -> dict | None:
    """The full manifest is a published artifact; the task row keeps its parts."""
    if not row.get("files"):
        return None
    return {
        "schema_version": 1,
        "kind": "pointwise-export",
        "export_id": row["export_id"],
        "created_at": row.get("finished_at") or row.get("requested_at"),
        "business": {
            "batch_id": row.get("business_batch_id"),
            "committed_at": row.get("snapshot_committed_at"),
            "snapshot": "read-transaction",
        },
        "tables": row["files"],
        "row_counts": {entry["name"]: entry["rows"] for entry in row["files"]},
        "checks": row.get("checks"),
        "reader": {"engine": "sqlite", "journal_mode": "wal", "snapshot": "read-transaction-end-mark"},
    }


def _task_row(row: sqlite3.Row) -> dict:
    task = dict(row)
    for raw_key, public_key in _JSON_FIELD_MAP.items():
        raw = task.pop(raw_key, None)
        task[public_key] = json.loads(raw) if raw else None
    task["cancel_requested"] = bool(task["cancel_requested"])
    task["manifest"] = _manifest_from_row(task)
    return task
