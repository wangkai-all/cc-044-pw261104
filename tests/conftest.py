from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.db import ensure_tasks_schema, seed_business  # noqa: E402
from app.tasks import TaskStore  # noqa: E402


@pytest.fixture()
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("POINTWISE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("POINTWISE_START_WRITER", "0")
    monkeypatch.setenv("POINTWISE_SEED_BATCHES", "60")
    instance = Settings(data_dir=tmp_path, start_writer=False, seed_history_batches=60, max_pending_exports=2)
    instance.ensure_dirs()
    seed_business(instance.business_db, instance.seed_history_batches)
    ensure_tasks_schema(instance.tasks_db)
    return instance


@pytest.fixture()
def store(settings: Settings) -> TaskStore:
    return TaskStore(settings.tasks_db)


@pytest.fixture()
def client(settings: Settings, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("POINTWISE_DATA_DIR", str(settings.data_dir))
    from fastapi.testclient import TestClient

    from app import api as api_module

    api_module.settings = settings
    api_module.store = TaskStore(settings.tasks_db)
    api_module.worker = api_module.ExportWorker(settings, api_module.store)
    with TestClient(api_module.app) as test_client:
        yield test_client


@pytest.fixture()
def writer_process(settings: Settings):
    env = os.environ.copy()
    env["POINTWISE_DATA_DIR"] = str(settings.data_dir)
    env["POINTWISE_SEED_BATCHES"] = "60"
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.writer"],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        status_file = settings.writer_status_file
        if status_file.exists():
            try:
                status = json.loads(status_file.read_text())
                if status.get("state") == "running":
                    break
            except ValueError:
                pass
        if proc.poll() is not None:
            raise RuntimeError(proc.stderr.read().decode())
        time.sleep(0.1)
    yield proc
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def wait_for(client, export_id: str, timeout: float = 60.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = client.get(f"/api/exports/{export_id}").json()
        if task["status"] in {"succeeded", "failed", "cancelled"}:
            return task
        time.sleep(0.1)
    raise AssertionError(f"export {export_id} did not finish in {timeout}s")


def read_business(settings: Settings) -> sqlite3.Connection:
    conn = sqlite3.connect(settings.business_db)
    conn.row_factory = sqlite3.Row
    return conn
