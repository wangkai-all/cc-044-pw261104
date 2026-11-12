from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import artifacts
from .artifacts import TABLE_SPECS
from .config import CONTROL_LIMITS, ROOT_DIR, Settings
from .db import business_connect, ensure_tasks_schema, read_controls, seed_business, write_controls
from .exporter import ExportWorker
from .tasks import TaskStore, now_iso
from .verify import VerificationError, verify_export, verify_upload


settings = Settings.from_env()
store = TaskStore(settings.tasks_db)
worker = ExportWorker(settings, store)


def _start_writer_process() -> subprocess.Popen | None:
    if not settings.start_writer:
        return None
    log = settings.writer_log_file.open("ab")
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.writer"],
        cwd=str(ROOT_DIR),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return proc


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.ensure_dirs()
    if not settings.controls_file.exists():
        write_controls(settings.controls_file, {})
    seed_business(settings.business_db, settings.seed_history_batches)
    ensure_tasks_schema(settings.tasks_db)
    worker.start()
    app.state.writer = _start_writer_process()
    try:
        yield
    finally:
        worker.stop()
        proc = app.state.writer
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


app = FastAPI(title="Pointwise Delivery", lifespan=lifespan)


# ---------------------------------------------------------------- schemas
class ControlUpdate(BaseModel):
    """Values are clamped server-side; the limits also drive the UI sliders."""

    writer_enabled: bool | None = None
    batches_per_second: float | None = None
    sales_per_batch: int | None = None
    rows_per_second: float | None = None


# ---------------------------------------------------------------- helpers
def _task_or_404(export_id: str) -> dict:
    task = store.get(export_id)
    if task is None:
        raise HTTPException(status_code=404, detail="导出任务不存在")
    return task


def _published_dir(task: dict) -> Path:
    """Directory holding the original published bytes for this task.

    A task that reused an earlier same-batch export has no directory of its
    own; its files are the original's, byte for byte.
    """
    owner = task.get("reused_from") or task["export_id"]
    return settings.exports_dir / owner


def _require_published(export_id: str) -> dict:
    task = _task_or_404(export_id)
    if task["status"] != "succeeded":
        raise HTTPException(status_code=409, detail="该导出尚未发布，没有可下载的文件")
    directory = _published_dir(task)
    if not (directory / "manifest.json").is_file():
        raise HTTPException(status_code=410, detail="该导出已发布但文件缺失")
    return task


# ---------------------------------------------------------------- control plane
@app.get("/api/controls")
def get_controls():
    controls = read_controls(settings.controls_file)
    return {
        "controls": controls,
        "limits": {key: {"min": low, "max": high} for key, (low, high) in CONTROL_LIMITS.items()},
        "writer_status": _read_writer_status(),
        "capacity": worker.active_count(),
        "max_pending": settings.max_pending_exports,
    }


@app.put("/api/controls")
def put_controls(payload: ControlUpdate):
    current = read_controls(settings.controls_file)
    incoming: dict = {"writer": {}, "export": {}}
    if payload.writer_enabled is not None:
        incoming["writer"]["enabled"] = payload.writer_enabled
    if payload.batches_per_second is not None:
        incoming["writer"]["batches_per_second"] = payload.batches_per_second
    if payload.sales_per_batch is not None:
        incoming["writer"]["sales_per_batch"] = payload.sales_per_batch
    if payload.rows_per_second is not None:
        incoming["export"]["rows_per_second"] = payload.rows_per_second
    for section, values in incoming.items():
        current[section].update(values)
    saved = write_controls(settings.controls_file, current)
    return {"controls": saved, "writer_status": _read_writer_status()}


def _read_writer_status() -> dict:
    try:
        raw = json.loads(settings.writer_status_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"state": "starting", "last_batch_id": 0, "batches_committed": 0, "observed_rate": 0}
    age = time.time() - float(raw.get("updated_epoch", 0) or 0)
    if age > 5:
        raw["state"] = "stale"
    commits = raw.pop("recent_commits", None) or []
    if len(commits) >= 2 and commits[-1] > commits[0]:
        raw["observed_rate"] = round((len(commits) - 1) / (commits[-1] - commits[0]), 2)
    else:
        raw["observed_rate"] = 0
    raw["status_age_seconds"] = round(age, 1)
    return raw


@app.get("/api/overview")
def overview():
    controls = read_controls(settings.controls_file)
    writer_status = _read_writer_status()
    business = _business_summary()
    return {
        "controls": controls,
        "writer_status": writer_status,
        "business": business,
        "capacity": worker.active_count(),
        "max_pending": settings.max_pending_exports,
    }


def _business_summary() -> dict:
    conn = business_connect(settings.business_db, read_only=True)
    try:
        latest = conn.execute("SELECT batch_id, committed_at FROM write_batches ORDER BY batch_id DESC LIMIT 1").fetchone()
        counts = {
            "products": conn.execute("SELECT COUNT(*) FROM products").fetchone()[0],
            "stock": conn.execute("SELECT COUNT(*) FROM stock").fetchone()[0],
            "sales": conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0],
        }
        units = conn.execute("SELECT COALESCE(SUM(quantity),0) FROM sales").fetchone()[0]
        return {
            "latest_batch_id": int(latest["batch_id"]) if latest else 0,
            "latest_committed_at": latest["committed_at"] if latest else None,
            "counts": counts,
            "sales_units": int(units),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- exports
@app.post("/api/exports", status_code=202)
def create_export():
    if not worker.has_capacity():
        raise HTTPException(status_code=429, detail="导出队列已满，请稍后再试")
    export_id = f"exp-{time.strftime('%Y%m%d-%H%M%S')}-{os.urandom(3).hex()}"
    store.create_export(export_id, now_iso())
    worker.enqueue(export_id)
    return {"export_id": export_id}


@app.get("/api/exports")
def list_exports(limit: int = 20):
    return {"tasks": store.list(limit=min(limit, 100)), "writer_status": _read_writer_status(), "overview": _business_summary()}


@app.get("/api/exports/{export_id}")
def get_export(export_id: str):
    return _task_or_404(export_id)


@app.post("/api/exports/{export_id}/cancel")
def cancel_export(export_id: str):
    task = _task_or_404(export_id)
    if task["status"] not in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="该任务已结束，无法取消")
    store.request_cancel(export_id)
    return {"export_id": export_id, "cancel_requested": True}


@app.get("/api/exports/{export_id}/download")
def download_export(export_id: str):
    """Serve the original published bytes; never regenerate on download."""
    task = _require_published(export_id)
    path = _published_dir(task) / "bundle.zip"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="整批文件包不存在")
    return FileResponse(
        path,
        media_type="application/zip",
        filename=f"{export_id}.zip",
        content_disposition_type="attachment",
    )


@app.get("/api/exports/{export_id}/files/{filename}")
def download_file(export_id: str, filename: str):
    task = _require_published(export_id)
    allowed = {"manifest.json", "manifest.sha256", "bundle.zip", "bundle.zip.sha256", "preview.json"} | {
        spec["file"] for spec in TABLE_SPECS.values()
    }
    if filename not in allowed:
        raise HTTPException(status_code=404, detail="文件不在该批次清单内")
    path = _published_dir(task) / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")
    return FileResponse(path, filename=filename)


@app.get("/api/exports/{export_id}/preview")
def preview_export(export_id: str):
    task = _require_published(export_id)
    if task.get("previews"):
        return {"export_id": export_id, "previews": task["previews"]}
    directory = _published_dir(task)
    manifest = artifacts.read_manifest(directory / "manifest.json")
    return {"export_id": export_id, "previews": artifacts.preview_bundle(directory, manifest, settings.preview_rows)}


# ---------------------------------------------------------------- verification
@app.post("/api/verifications")
def verify(
    export_id: str | None = Form(default=None),
    upload: UploadFile | None = File(default=None),
):
    if export_id and upload is not None:
        raise HTTPException(status_code=400, detail="请选择已发布批次或上传文件，二者只选其一")
    if export_id:
        task = _task_or_404(export_id)
        if task["status"] != "succeeded":
            raise HTTPException(status_code=409, detail="该批次尚未成功发布")
        try:
            report = verify_export(settings, store, task["reused_from"] or export_id)
        except VerificationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return report
    if upload is None:
        raise HTTPException(status_code=400, detail="缺少 export_id 或上传文件")
    # Stream to a private temp file; never trust the client filename.
    tmp = settings.staging_dir / f"upload-{os.urandom(8).hex()}.zip"
    total = 0
    try:
        with tmp.open("wb") as handle:
            while chunk := upload.file.read(1 << 20):
                total += len(chunk)
                if total > settings.max_upload_bytes:
                    raise HTTPException(status_code=413, detail="上传文件过大")
                handle.write(chunk)
        report = verify_upload(settings, store, tmp, upload_name=upload.filename or "上传文件")
    except VerificationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        tmp.unlink(missing_ok=True)
        upload.file.close()
    return report


@app.get("/api/verifications")
def list_verifications(limit: int = 10):
    return {"reports": store.list_verifications(limit=min(limit, 50))}


@app.get("/api/health")
def health():
    return {"ok": True, "data_dir": str(settings.data_dir)}


# ---------------------------------------------------------------- static
if settings.web_dist.is_dir():
    app.mount("/", StaticFiles(directory=str(settings.web_dist), html=True), name="web")
