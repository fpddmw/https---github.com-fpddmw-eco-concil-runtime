from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .governance import CONTRACT_MODES, postflight_skill_execution, preflight_skill_execution
from .ledger import append_ledger_event, write_receipt
from .manifest import update_after_run
from .registry import resolve_skill_entry, workspace_root


class SkillExecutionError(RuntimeError):
    def __init__(self, message: str, payload: dict[str, Any] | None = None):
        super().__init__(message)
        self.payload = payload or {}


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


def json_hash(payload: Any) -> str:
    return stable_hash(json.dumps(payload, ensure_ascii=True, sort_keys=True))


def new_runtime_event_id(prefix: str, *parts: Any) -> str:
    import uuid

    return prefix + "-" + stable_hash(uuid.uuid4().hex, *parts)[:20]


def run_skill(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    skill_args: list[str],
    contract_mode: str = "warn",
    workspace: Path | None = None,
) -> dict[str, Any]:
    if contract_mode not in CONTRACT_MODES:
        raise ValueError(f"Unsupported contract_mode: {contract_mode}")
    root = workspace or workspace_root()
    preflight = preflight_skill_execution(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        skill_args=skill_args,
        contract_mode=contract_mode,
        workspace=root,
    )
    skill_entry = resolve_skill_entry(skill_name, root)
    script_path = Path(maybe_text(skill_entry.get("script_path")))
    skill_options = preflight.get("skill_options", {}) if isinstance(preflight.get("skill_options"), dict) else {}
    command = [sys.executable, str(script_path), "--run-dir", str(run_dir), "--run-id", run_id, "--round-id", round_id, *skill_args]
    command_snapshot = {
        "argv": command,
        "cwd": str(root),
        "python_executable": sys.executable,
        "workspace_root": str(root),
        "script_path": str(script_path),
    }
    execution_input_hash = json_hash(
        {
            "run_id": run_id,
            "round_id": round_id,
            "skill_name": skill_name,
            "skill_args": skill_args,
            "skill_options": skill_options,
            "command_snapshot": command_snapshot,
            "declared_contract": skill_entry.get("declared_contract", {}),
            "preflight": preflight,
            "contract_mode": contract_mode,
        }
    )
    started_at = utc_now_iso()
    if bool(preflight.get("block_execution")):
        finished_at = utc_now_iso()
        event_id = new_runtime_event_id("runtimeevt", run_id, round_id, skill_name, execution_input_hash, started_at, finished_at, "preflight")
        event = {
            "schema_version": "runtime-event-v3",
            "event_id": event_id,
            "event_type": "skill-preflight",
            "run_id": run_id,
            "round_id": round_id,
            "skill_name": skill_name,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "status": "blocked",
            "contract_mode": contract_mode,
            "skill_args": skill_args,
            "skill_options": skill_options,
            "command_snapshot": command_snapshot,
            "execution_input_hash": execution_input_hash,
            "skill_registry_entry": skill_entry,
            "preflight": preflight,
        }
        append_ledger_event(run_dir, event)
        failure_payload = {
            "status": "failed",
            "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
            "message": f"Contract preflight blocked execution for {skill_name}.",
            "preflight": preflight,
        }
        raise SkillExecutionError(failure_payload["message"], failure_payload)

    completed = subprocess.run(command, capture_output=True, text=True, check=False, cwd=str(root))
    finished_at = utc_now_iso()
    event_id = new_runtime_event_id("runtimeevt", run_id, round_id, skill_name, execution_input_hash, started_at, finished_at)

    base_event = {
        "schema_version": "runtime-event-v3",
        "event_id": event_id,
        "event_type": "skill-execution",
        "run_id": run_id,
        "round_id": round_id,
        "skill_name": skill_name,
        "started_at_utc": started_at,
        "completed_at_utc": finished_at,
        "skill_args": skill_args,
        "skill_options": skill_options,
        "command_snapshot": command_snapshot,
        "contract_mode": contract_mode,
        "execution_input_hash": execution_input_hash,
        "skill_registry_entry": skill_entry,
        "declared_reads": preflight.get("declared_reads", []),
        "declared_writes": preflight.get("declared_writes", []),
        "resolved_read_paths": preflight.get("resolved_read_paths", []),
        "resolved_write_paths": preflight.get("resolved_write_paths", []),
        "preflight": preflight,
        "stdout_hash": stable_hash(completed.stdout),
        "stderr_hash": stable_hash(completed.stderr),
    }

    if completed.returncode != 0:
        event = {
            **base_event,
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
            **base_event,
            "exit_code": completed.returncode,
            "status": "failed",
            "stdout": completed.stdout,
            "stderr": f"Invalid JSON skill output: {exc}",
        }
        append_ledger_event(run_dir, event)
        raise SkillExecutionError(f"Invalid JSON skill output for {skill_name}") from exc
    if not isinstance(payload, dict):
        raise SkillExecutionError(f"Skill {skill_name} returned a non-object payload")

    postflight = postflight_skill_execution(
        run_dir,
        skill_name=skill_name,
        payload=payload,
        preflight=preflight,
        contract_mode=contract_mode,
    )
    receipt_id = maybe_text(payload.get("receipt_id")) or ("runtime-receipt-" + stable_hash(run_id, round_id, skill_name, event_id)[:20])
    receipt_file = write_receipt(run_dir, receipt_id, payload)
    if bool(postflight.get("block_execution")):
        event = {
            **base_event,
            "exit_code": completed.returncode,
            "status": "failed",
            "receipt_id": receipt_id,
            "batch_id": maybe_text(payload.get("batch_id")),
            "artifact_refs": payload.get("artifact_refs", []),
            "canonical_ids": payload.get("canonical_ids", []),
            "summary": payload.get("summary", {}),
            "payload_hash": json_hash(payload),
            "receipt_path": str(receipt_file),
            "postflight": postflight,
        }
        append_ledger_event(run_dir, event)
        failure_payload = {
            "status": "failed",
            "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
            "message": f"Contract enforcement blocked completion for {skill_name}.",
            "preflight": preflight,
            "postflight": postflight,
            "receipt_id": receipt_id,
            "receipt_path": str(receipt_file),
        }
        raise SkillExecutionError(failure_payload["message"], failure_payload)

    event = {
        **base_event,
        "exit_code": completed.returncode,
        "status": maybe_text(payload.get("status")) or "completed",
        "receipt_id": receipt_id,
        "batch_id": maybe_text(payload.get("batch_id")),
        "artifact_refs": payload.get("artifact_refs", []),
        "canonical_ids": payload.get("canonical_ids", []),
        "summary": payload.get("summary", {}),
        "payload_hash": json_hash(payload),
        "receipt_path": str(receipt_file),
        "postflight": postflight,
    }
    append_ledger_event(run_dir, event)
    manifest, cursor = update_after_run(run_dir, run_id=run_id, round_id=round_id, skill_name=skill_name, receipt_id=receipt_id, event_id=event_id)
    return {
        "status": "completed",
        "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "event_id": event_id, "receipt_id": receipt_id, "contract_mode": contract_mode},
        "event": event,
        "manifest": manifest,
        "cursor": cursor,
        "skill_payload": payload,
        "governance": {"preflight": preflight, "postflight": postflight},
    }