from __future__ import annotations

import json
import uuid
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ledger import append_ledger_event, write_receipt
from .manifest import update_after_run
from .registry import resolve_skill_entry, workspace_root


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


def json_hash(payload: Any) -> str:
    return stable_hash(json.dumps(payload, ensure_ascii=True, sort_keys=True))


def new_runtime_event_id(prefix: str, *parts: Any) -> str:
    return prefix + "-" + stable_hash(uuid.uuid4().hex, *parts)[:20]


def parse_skill_options(skill_args: list[str]) -> dict[str, Any]:
    options: dict[str, Any] = {}
    index = 0
    while index < len(skill_args):
        token = skill_args[index]
        if not token.startswith("--"):
            index += 1
            continue
        key = token[2:].replace("-", "_")
        value: Any = True
        if index + 1 < len(skill_args) and not skill_args[index + 1].startswith("--"):
            value = skill_args[index + 1]
            index += 1
        existing = options.get(key)
        if existing is None:
            options[key] = value
        elif isinstance(existing, list):
            existing.append(value)
        else:
            options[key] = [existing, value]
        index += 1
    return options


def resolve_contract_paths(patterns: list[Any], run_dir: Path, substitutions: dict[str, Any]) -> list[str]:
    results: list[str] = []
    for item in patterns:
        pattern = maybe_text(item)
        if not pattern:
            continue
        resolved = pattern
        if resolved == "run_dir":
            resolved = str(run_dir.resolve())
        elif resolved.startswith("run_dir/"):
            resolved = str((run_dir / resolved.removeprefix("run_dir/")).resolve())
        for key, value in substitutions.items():
            resolved = resolved.replace(f"<{key}>", maybe_text(value))
        results.append(resolved)
    return results


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
    skill_entry = resolve_skill_entry(skill_name, root)
    script_path = Path(maybe_text(skill_entry.get("script_path")))
    skill_options = parse_skill_options(skill_args)
    declared_contract = skill_entry.get("declared_contract", {}) if isinstance(skill_entry.get("declared_contract"), dict) else {}
    declared_reads = declared_contract.get("reads", []) if isinstance(declared_contract.get("reads"), list) else []
    declared_writes = declared_contract.get("writes", []) if isinstance(declared_contract.get("writes"), list) else []
    substitutions = {"run_id": run_id, "round_id": round_id, "skill_name": skill_name, **skill_options}
    resolved_read_paths = resolve_contract_paths(declared_reads, run_dir, substitutions)
    resolved_write_paths = resolve_contract_paths(declared_writes, run_dir, substitutions)
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
            "declared_contract": declared_contract,
        }
    )
    started_at = utc_now_iso()
    completed = subprocess.run(command, capture_output=True, text=True, check=False, cwd=str(root))
    finished_at = utc_now_iso()
    event_id = new_runtime_event_id("runtimeevt", run_id, round_id, skill_name, execution_input_hash, started_at, finished_at)

    base_event = {
        "schema_version": "runtime-event-v2",
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
        "execution_input_hash": execution_input_hash,
        "skill_registry_entry": skill_entry,
        "declared_reads": declared_reads,
        "declared_writes": declared_writes,
        "resolved_read_paths": resolved_read_paths,
        "resolved_write_paths": resolved_write_paths,
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

    receipt_id = maybe_text(payload.get("receipt_id")) or ("runtime-receipt-" + stable_hash(run_id, round_id, skill_name, event_id)[:20])
    receipt_file = write_receipt(run_dir, receipt_id, payload)
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