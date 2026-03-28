from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import cursor_path, ledger_path, manifest_path, registry_path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def init_run_manifest(run_dir: Path, run_id: str) -> dict[str, Any]:
    existing = load_json_if_exists(manifest_path(run_dir)) or {}
    created_at = existing.get("created_at_utc") or utc_now_iso()
    payload = {
        "schema_version": "runtime-manifest-v1",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "created_at_utc": created_at,
        "updated_at_utc": utc_now_iso(),
        "invocation_count": int(existing.get("invocation_count") or 0),
        "last_round_id": existing.get("last_round_id") or "",
        "last_skill_name": existing.get("last_skill_name") or "",
        "last_receipt_id": existing.get("last_receipt_id") or "",
        "last_event_id": existing.get("last_event_id") or "",
        "registry_path": str(registry_path(run_dir)),
        "ledger_path": str(ledger_path(run_dir)),
    }
    write_json(manifest_path(run_dir), payload)
    return payload


def init_round_cursor(run_dir: Path, run_id: str) -> dict[str, Any]:
    existing = load_json_if_exists(cursor_path(run_dir)) or {}
    payload = {
        "schema_version": "runtime-cursor-v1",
        "run_id": run_id,
        "current_round_id": existing.get("current_round_id") or "",
        "last_skill_name": existing.get("last_skill_name") or "",
        "last_receipt_id": existing.get("last_receipt_id") or "",
        "last_event_id": existing.get("last_event_id") or "",
        "updated_at_utc": utc_now_iso(),
    }
    write_json(cursor_path(run_dir), payload)
    return payload


def update_after_run(run_dir: Path, *, run_id: str, round_id: str, skill_name: str, receipt_id: str, event_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_json_if_exists(manifest_path(run_dir)) or init_run_manifest(run_dir, run_id)
    cursor = load_json_if_exists(cursor_path(run_dir)) or init_round_cursor(run_dir, run_id)
    manifest["updated_at_utc"] = utc_now_iso()
    manifest["invocation_count"] = int(manifest.get("invocation_count") or 0) + 1
    manifest["last_round_id"] = round_id
    manifest["last_skill_name"] = skill_name
    manifest["last_receipt_id"] = receipt_id
    manifest["last_event_id"] = event_id
    cursor["current_round_id"] = round_id
    cursor["last_skill_name"] = skill_name
    cursor["last_receipt_id"] = receipt_id
    cursor["last_event_id"] = event_id
    cursor["updated_at_utc"] = utc_now_iso()
    write_json(manifest_path(run_dir), manifest)
    write_json(cursor_path(run_dir), cursor)
    return manifest, cursor