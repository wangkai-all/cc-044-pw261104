from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

DEFAULT_CONTROLS = {
    "writer": {
        "enabled": True,
        "batches_per_second": 3.0,
        "sales_per_batch": 2,
    },
    "export": {
        # 0 means "no throttle". Otherwise rows read from SQLite per second.
        "rows_per_second": 0.0,
    },
}

CONTROL_LIMITS = {
    "batches_per_second": (0.2, 30.0),
    "sales_per_batch": (1, 8),
    "rows_per_second": (0.0, 5000.0),
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


@dataclass(frozen=True)
class Settings:
    """Filesystem layout and demo knobs.

    The business database is written by a separate writer process. The task
    database, controls, status heartbeat and export artifacts live beside it.
    """

    data_dir: Path
    start_writer: bool = True
    seed_history_batches: int = 12000
    max_pending_exports: int = 2
    preview_rows: int = 24
    max_upload_bytes: int = 256 * 1024 * 1024
    writer_poll_seconds: float = 0.5
    _source_root: Path = field(default=ROOT_DIR, repr=False)

    @property
    def business_db(self) -> Path:
        return self.data_dir / "business.db"

    @property
    def tasks_db(self) -> Path:
        return self.data_dir / "tasks.db"

    @property
    def exports_dir(self) -> Path:
        return self.data_dir / "exports"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def controls_file(self) -> Path:
        return self.data_dir / "controls.json"

    @property
    def writer_status_file(self) -> Path:
        return self.data_dir / "writer-status.json"

    @property
    def writer_log_file(self) -> Path:
        return self.data_dir / "writer.log"

    @property
    def web_dist(self) -> Path:
        return self._source_root / "web" / "dist"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(os.environ.get("POINTWISE_DATA_DIR", str(ROOT_DIR / ".data")))
        return cls(
            data_dir=data_dir,
            start_writer=_env_bool("POINTWISE_START_WRITER", True),
            seed_history_batches=int(os.environ.get("POINTWISE_SEED_BATCHES", "12000")),
        )
