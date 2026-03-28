from __future__ import annotations

from pathlib import Path


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def runtime_dir(run_dir: Path) -> Path:
    return run_dir / "runtime"


def receipts_dir(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "receipts"


def manifest_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "run_manifest.json"


def cursor_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "round_cursor.json"


def ledger_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "audit_ledger.jsonl"


def registry_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "skill_registry.json"


def receipt_path(run_dir: Path, receipt_id: str) -> Path:
    return receipts_dir(run_dir) / f"{receipt_id}.json"


def ensure_runtime_dirs(run_dir: Path) -> None:
    runtime_dir(run_dir).mkdir(parents=True, exist_ok=True)
    receipts_dir(run_dir).mkdir(parents=True, exist_ok=True)