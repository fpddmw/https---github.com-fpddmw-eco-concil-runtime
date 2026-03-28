from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ledger import append_ledger_event, write_receipt
from .manifest import update_after_run
from .registry import resolve_skill_script, workspace_root


class SkillExecutionError(RuntimeError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def stable_hash(*parts: Any) -> str:
    import hashlib

    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def run_skill(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    skill_args: list[str],
    workspace: Path | None = None,
) -> dict[str, Any]:
    root = workspace or workspace_root()
    script_path = resolve_skill_script(skill_name, root)
    command = [sys.executable, str(script_path), "--run-dir", str(run_dir), "--run-id", run_id, "--round-id", round_id, *skill_args]
    started_at = utc_now_iso()
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    finished_at = utc_now_iso()
    event_id = "runtimeevt-" + stable_hash(run_id, round_id, skill_name, started_at, finished_at)[:12]

    if completed.returncode != 0:
        event = {
            "schema_version": "runtime-event-v1",
            "event_id": event_id,
            "event_type": "skill-execution",
            "run_id": run_id,
            "round_id": round_id,
            "skill_name": skill_name,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "exit_code": completed.returncode,
            "status": "failed",
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        append_ledger_event(run_dir, event)
        raise SkillExecutionError(f"Skill execution failed for {skill_name}: {completed.stderr or completed.stdout}")

    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        event = {
            "schema_version": "runtime-event-v1",
            "event_id": event_id,
            "event_type": "skill-execution",
            "run_id": run_id,
            "round_id": round_id,
            "skill_name": skill_name,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "exit_code": completed.returncode,
            "status": "failed",
            "stdout": completed.stdout,
            "stderr": f"Invalid JSON skill output: {exc}",
        }
        append_ledger_event(run_dir, event)
        raise SkillExecutionError(f"Invalid JSON skill output for {skill_name}") from exc
    if not isinstance(payload, dict):
        raise SkillExecutionError(f"Skill {skill_name} returned a non-object payload")

    receipt_id = maybe_text(payload.get("receipt_id")) or ("runtime-receipt-" + stable_hash(run_id, round_id, skill_name, event_id)[:20])
    receipt_file = write_receipt(run_dir, receipt_id, payload)
    event = {
        "schema_version": "runtime-event-v1",
        "event_id": event_id,
        "event_type": "skill-execution",
        "run_id": run_id,
        "round_id": round_id,
        "skill_name": skill_name,
        "started_at_utc": started_at,
        "completed_at_utc": finished_at,
        "exit_code": completed.returncode,
        "status": maybe_text(payload.get("status")) or "completed",
        "receipt_id": receipt_id,
        "batch_id": maybe_text(payload.get("batch_id")),
        "artifact_refs": payload.get("artifact_refs", []),
        "canonical_ids": payload.get("canonical_ids", []),
        "summary": payload.get("summary", {}),
        "receipt_path": str(receipt_file),
    }
    append_ledger_event(run_dir, event)
    manifest, cursor = update_after_run(run_dir, run_id=run_id, round_id=round_id, skill_name=skill_name, receipt_id=receipt_id, event_id=event_id)
    return {
        "status": "completed",
        "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "event_id": event_id, "receipt_id": receipt_id},
        "event": event,
        "manifest": manifest,
        "cursor": cursor,
        "skill_payload": payload,
    }