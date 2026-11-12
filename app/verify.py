"""Verification for a published export or an uploaded one.

An upload arrives as a zip (or a set of files) with a manifest. The verifier
copies the files into an isolated work directory under the staging area, then
checks the manifest hash, every file hash, every declared row count, and the
same three-table reconciliation the exporter ran.
"""

from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path

from . import artifacts
from .clock import now_iso
from .config import Settings
from .artifacts import TABLE_SPECS, read_manifest, reconcile, sha256_file  # noqa: F401
from .tasks import TaskStore

MAX_MANIFEST_BYTES = 8 * 1024 * 1024


class VerificationError(Exception):
    pass


def _safe_zip_extract(archive: zipfile.ZipFile, target: Path) -> None:
    for info in archive.infolist():
        name = info.filename
        if name.startswith("/") or ".." in Path(name).parts or info.is_dir():
            raise VerificationError(f"压缩包内路径不安全：{name}")
        if info.file_size > 512 * 1024 * 1024:
            raise VerificationError(f"压缩包内文件过大：{name}")
        target.mkdir(parents=True, exist_ok=True)
        with archive.open(info) as source, (target / name).open("wb") as dest:
            shutil.copyfileobj(source, dest)


def verify_directory(
    directory: Path,
    *,
    source: str,
    export_id: str | None = None,
    expected_manifest_sha: str | None = None,
) -> dict:
    report: dict = {
        "verification_id": None,
        "created_at": now_iso(),
        "source": source,
        "export_id": export_id,
        "ok": False,
        "manifest_sha256": None,
        "manifest_sha256_expected": expected_manifest_sha,
        "checks": [],
        "files": [],
        "details": {},
    }

    def add(check_id: str, title: str, ok: bool, detail: str) -> None:
        report["checks"].append({"id": check_id, "title": title, "ok": bool(ok), "detail": detail})

    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        add("manifest_present", "清单存在", False, "未找到 manifest.json")
        return report

    if manifest_path.stat().st_size > MAX_MANIFEST_BYTES:
        add("manifest_size", "清单大小合理", False, "manifest.json 过大")
        return report

    try:
        manifest = read_manifest(manifest_path)
    except (OSError, ValueError) as exc:
        add("manifest_parse", "清单可解析", False, str(exc))
        return report
    add("manifest_parse", "清单可解析", True, "manifest.json 解析成功")

    actual_sha = sha256_file(manifest_path)
    report["manifest_sha256"] = actual_sha
    sha_file = directory / "manifest.sha256"
    sha_declared = None
    if sha_file.is_file():
        raw = sha_file.read_text(encoding="utf-8").strip()
        sha_declared = raw.split()[0] if raw else None
    sha_ok = sha_declared == actual_sha
    add(
        "manifest_sha",
        "清单散列与 manifest.sha256 一致",
        sha_ok,
        f"实际 {actual_sha[:16]}…，记录 {(sha_declared[:16] + '…') if sha_declared else '无'}",
    )
    if expected_manifest_sha:
        add(
            "manifest_sha_expected",
            "清单散列与任务记录一致",
            expected_manifest_sha == actual_sha,
            f"任务记录 {expected_manifest_sha[:16]}…，实际 {actual_sha[:16]}…",
        )

    tables = manifest.get("tables") if isinstance(manifest, dict) else None
    if not isinstance(tables, list) or len(tables) != 3:
        add("manifest_tables", "清单声明三张表", False, "清单中 tables 缺失或数量不是 3")
        return report
    add("manifest_tables", "清单声明三张表", True, "商品、库存、销售三张表均在清单中")

    declared = {entry.get("name"): entry for entry in tables if isinstance(entry, dict)}
    missing = [name for name in TABLE_SPECS if name not in declared]
    unknown = [name for name in declared if name not in TABLE_SPECS]
    add(
        "table_set",
        "表集合与约定一致",
        not missing and not unknown,
        "商品 / 库存 / 销售" if not missing and not unknown else f"缺 {missing}，多 {unknown}",
    )
    if missing:
        return report

    file_ok = True
    for name, spec in TABLE_SPECS.items():
        entry = declared[name]
        file_name = entry.get("file")
        if not isinstance(file_name, str) or not file_name or Path(file_name).name != file_name:
            add(f"file_{name}", f"{name} 文件引用非法", False, str(file_name))
            file_ok = False
            continue
        path = directory / file_name
        if not path.is_file():
            add(f"file_{name}", f"{name} 文件存在", False, f"缺少 {file_name}")
            file_ok = False
            continue
        actual = sha256_file(path)
        size = path.stat().st_size
        expected_sha = entry.get("sha256")
        expected_bytes = entry.get("bytes")
        sha_ok = isinstance(expected_sha, str) and actual == expected_sha
        size_ok = isinstance(expected_bytes, int) and size == expected_bytes
        add(
            f"sha_{name}",
            f"{name} 文件散列与清单一致",
            sha_ok,
            f"实际 {actual[:16]}…，清单 {(expected_sha or '缺失')[:16]}…",
        )
        add(
            f"size_{name}",
            f"{name} 文件大小与清单一致",
            size_ok,
            f"实际 {size} 字节，清单 {expected_bytes}",
        )
        report["files"].append({"name": name, "file": file_name, "bytes": size, "sha256": actual, "sha256_expected": expected_sha})
        file_ok = file_ok and sha_ok and size_ok

    if not file_ok:
        return report

    # Schema and row count, read from the parquet metadata.
    row_counts: dict[str, int] = {}
    expected_counts = manifest.get("row_counts") if isinstance(manifest.get("row_counts"), dict) else {}
    schema_ok = True
    for name, spec in TABLE_SPECS.items():
        entry = declared[name]
        path = directory / entry["file"]
        try:
            pa_schema = artifacts.pq.read_schema(path)
            meta = artifacts.pq.read_metadata(path)
        except Exception as exc:
            add(f"read_{name}", f"{name} 可读取", False, f"Parquet 读取失败：{exc}")
            schema_ok = False
            continue
        expected_columns = [column for column, _ in spec["columns"]]
        actual_columns = [field.name for field in pa_schema]
        add(
            f"schema_{name}",
            f"{name} 列结构与约定一致",
            actual_columns == expected_columns,
            f"实际列 {actual_columns}" if actual_columns != expected_columns else f"{len(actual_columns)} 列",
        )
        schema_ok = schema_ok and actual_columns == expected_columns
        row_counts[name] = meta.num_rows
        declared_rows = entry.get("rows")
        add(
            f"rows_{name}",
            f"{name} 行数与清单一致",
            isinstance(declared_rows, int) and meta.num_rows == declared_rows,
            f"实际 {meta.num_rows} 行，清单 {declared_rows}",
        )
        if isinstance(expected_counts.get(name), int) and expected_counts.get(name) != meta.num_rows:
            add(f"rows_manifest_{name}", f"{name} 行数与 row_counts 一致", False, f"row_counts {expected_counts.get(name)}，实际 {meta.num_rows}")

    if not schema_ok:
        return report

    data = artifacts.load_snapshot_data(directory)
    business = manifest.get("business") if isinstance(manifest.get("business"), dict) else {}
    batch_id = business.get("batch_id")
    rec = reconcile(
        data,
        business_batch_id=batch_id if isinstance(batch_id, int) else None,
        expected_counts={name: row_counts[name] for name in TABLE_SPECS},
    )
    for item in rec["items"]:
        report["checks"].append(item)
    report["details"] = rec["details"]
    report["ok"] = rec["all_ok"] and all(check["ok"] for check in report["checks"])
    return report


def verify_export(settings: Settings, store: TaskStore, export_id: str) -> dict:
    manifest_path = settings.exports_dir / export_id / "manifest.json"
    if not manifest_path.is_file():
        raise VerificationError("该批次没有已发布的文件")
    task = store.get(export_id)
    expected_sha = sha256_file(manifest_path)
    report = verify_directory(
        settings.exports_dir / export_id,
        source="stored",
        export_id=export_id,
        # Compare against the hash recorded when the batch was published, not
        # against a hash of the manifest just read from disk (which would make
        # an edited manifest vouch for itself).
        expected_manifest_sha=(task or {}).get("manifest_sha256") or expected_sha,
    )
    report["verification_id"] = f"v-{os.urandom(6).hex()}"
    store.add_verification(report)
    return report


def verify_upload(
    settings: Settings,
    store: TaskStore,
    upload_path: Path,
    *,
    upload_name: str | None = None,
) -> dict:
    work = settings.staging_dir / f"verify-{os.urandom(6).hex()}"
    work.mkdir(parents=True, exist_ok=False)
    try:
        if zipfile.is_zipfile(upload_path):
            with zipfile.ZipFile(upload_path) as archive:
                if len(archive.infolist()) > 64:
                    raise VerificationError("压缩包内文件数量异常")
                _safe_zip_extract(archive, work)
        else:
            raise VerificationError("上传文件必须是包含 manifest.json 的 zip 包")
        report = verify_directory(work, source="upload")
        report["verification_id"] = f"v-{os.urandom(6).hex()}"
        report["upload_name"] = upload_name or upload_path.name
        store.add_verification(report)
        return report
    finally:
        shutil.rmtree(work, ignore_errors=True)
