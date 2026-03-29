from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .governance import CONTRACT_MODES, postflight_skill_execution, preflight_skill_execution
from .ledger import append_ledger_event, write_receipt
from .locking import exclusive_runtime_lock
from .manifest import update_after_run
from .operations import admission_error_code, evaluate_execution_admission, materialize_dead_letter, refresh_runtime_surfaces
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


def backoff_delay_seconds(retry_backoff_ms: int, attempt_number: int) -> float:
    if retry_backoff_ms <= 0:
        return 0.0
    return max(0.0, (retry_backoff_ms * max(1, attempt_number)) / 1000.0)


def retryable_return_code(return_code: int) -> bool:
    return return_code != 0


def structured_failure(
    *,
    error_code: str,
    message: str,
    retryable: bool,
    attempts: list[dict[str, Any]],
    execution_policy: dict[str, Any],
    recovery_hints: list[str],
) -> dict[str, Any]:
    return {
        "error_code": error_code,
        "message": message,
        "retryable": retryable,
        "attempt_count": len(attempts),
        "last_attempt": attempts[-1] if attempts else {},
        "execution_policy": execution_policy,
        "recovery_hints": recovery_hints,
    }


DEAD_LETTER_ID_PATTERN = re.compile(r"(deadletter-[0-9a-f]{20})")


def skill_command_hint(
    command_name: str,
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    skill_name: str,
    contract_mode: str,
    skill_args: list[str],
) -> str:
    command = [
        command_name,
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--skill-name",
        skill_name,
        "--contract-mode",
        contract_mode,
    ]
    if skill_args:
        command.extend(["--", *skill_args])
    return shlex.join(command)


def extract_dead_letter_id(*texts: str) -> str:
    for text in texts:
        match = DEAD_LETTER_ID_PATTERN.search(maybe_text(text))
        if match:
            return match.group(1)
    return ""


def refresh_runtime_surfaces_safely(run_dir: Path, *, round_id: str) -> dict[str, Any]:
    try:
        return refresh_runtime_surfaces(run_dir, round_id=round_id)
    except Exception:  # noqa: BLE001
        return {}


def run_skill(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    skill_args: list[str],
    contract_mode: str = "warn",
    workspace: Path | None = None,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
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
        timeout_seconds=timeout_seconds,
        retry_budget=retry_budget,
        retry_backoff_ms=retry_backoff_ms,
        allow_side_effects=allow_side_effects,
    )
    skill_entry = resolve_skill_entry(skill_name, root)
    script_path = Path(maybe_text(skill_entry.get("script_path")))
    skill_options = preflight.get("skill_options", {}) if isinstance(preflight.get("skill_options"), dict) else {}
    execution_policy = preflight.get("execution_policy", {}) if isinstance(preflight.get("execution_policy"), dict) else {}
    timeout_seconds = float(execution_policy.get("timeout_seconds") or 0.0)
    retry_budget = int(execution_policy.get("retry_budget") or 0)
    retry_backoff_ms = int(execution_policy.get("retry_backoff_ms") or 0)
    declared_side_effects = preflight.get("declared_side_effects", []) if isinstance(preflight.get("declared_side_effects"), list) else []
    allowed_side_effects = preflight.get("allowed_side_effects", []) if isinstance(preflight.get("allowed_side_effects"), list) else []
    command = [sys.executable, str(script_path), "--run-dir", str(run_dir), "--run-id", run_id, "--round-id", round_id, *skill_args]
    command_snapshot = {
        "argv": command,
        "cwd": str(root),
        "python_executable": sys.executable,
        "workspace_root": str(root),
        "script_path": str(script_path),
    }
    run_command_hint = skill_command_hint(
        "run-skill",
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        contract_mode=contract_mode,
        skill_args=skill_args,
    )
    preflight_command_hint = skill_command_hint(
        "preflight-skill",
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        contract_mode=contract_mode,
        skill_args=skill_args,
    )
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
            "execution_policy": execution_policy,
            "declared_side_effects": declared_side_effects,
            "allowed_side_effects": allowed_side_effects,
        }
    )
    runtime_admission = evaluate_execution_admission(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        actor_kind="skill",
        actor_name=skill_name,
        declared_side_effects=declared_side_effects,
        requested_side_effect_approvals=allowed_side_effects,
        execution_policy=execution_policy,
        resolved_read_paths=preflight.get("resolved_read_paths", []),
        resolved_write_paths=preflight.get("resolved_write_paths", []),
        cwd_path=str(root),
        workspace=root,
    )
    started_at = utc_now_iso()
    with exclusive_runtime_lock(run_dir) as lock_path:
        if bool(preflight.get("block_execution")):
            finished_at = utc_now_iso()
            event_id = new_runtime_event_id("runtimeevt", run_id, round_id, skill_name, execution_input_hash, started_at, finished_at, "preflight")
            failure = structured_failure(
                error_code="contract-preflight-blocked",
                message=f"Contract preflight blocked execution for {skill_name}.",
                retryable=False,
                attempts=[],
                execution_policy=execution_policy,
                recovery_hints=["Resolve the reported governance issues or relax contract mode before retrying."],
            )
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
                "declared_side_effects": declared_side_effects,
                "allowed_side_effects": allowed_side_effects,
                "execution_policy": execution_policy,
                "runtime_admission": runtime_admission,
                "lock_path": str(lock_path),
                "preflight": preflight,
                "failure": failure,
                "attempts": [],
                "attempt_count": 0,
            }
            dead_letter = materialize_dead_letter(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                source_type="skill-preflight",
                source_name=skill_name,
                message=failure["message"],
                failure=failure,
                summary={"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
                related_paths={
                    "policy_path": runtime_admission.get("policy_path", ""),
                    "script_path": str(script_path),
                    "workspace_root": str(root),
                    "lock_path": str(lock_path),
                },
                command_hint=preflight_command_hint,
            )
            event["dead_letter_id"] = dead_letter["dead_letter_id"]
            append_ledger_event(run_dir, event)
            operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
            failure_payload = {
                "status": "failed",
                "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
                "message": failure["message"],
                "failure": failure,
                "preflight": preflight,
                "runtime_admission": runtime_admission,
                "dead_letter": dead_letter,
                "operator_surface": operator_surface,
            }
            raise SkillExecutionError(failure_payload["message"], failure_payload)

        if bool(runtime_admission.get("block_execution")):
            finished_at = utc_now_iso()
            error_code = admission_error_code(runtime_admission)
            failure = structured_failure(
                error_code=error_code,
                message=f"Runtime admission blocked execution for {skill_name}.",
                retryable=False,
                attempts=[],
                execution_policy=execution_policy,
                recovery_hints=[
                    maybe_text(issue.get("message"))
                    for issue in runtime_admission.get("issues", [])
                    if isinstance(issue, dict) and maybe_text(issue.get("message"))
                ]
                or ["Adjust the admission policy or requested approvals before retrying."],
            )
            event = {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, skill_name, execution_input_hash, started_at, finished_at, "admission"),
                "event_type": "skill-admission",
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
                "declared_side_effects": declared_side_effects,
                "allowed_side_effects": allowed_side_effects,
                "execution_policy": execution_policy,
                "lock_path": str(lock_path),
                "preflight": preflight,
                "runtime_admission": runtime_admission,
                "failure": failure,
                "attempts": [],
                "attempt_count": 0,
            }
            dead_letter = materialize_dead_letter(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                source_type="skill-admission",
                source_name=skill_name,
                message=failure["message"],
                failure={**failure, "runtime_admission": runtime_admission},
                summary={"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
                related_paths={
                    "policy_path": runtime_admission.get("policy_path", ""),
                    "script_path": str(script_path),
                    "workspace_root": str(root),
                    "lock_path": str(lock_path),
                },
                command_hint=preflight_command_hint,
            )
            event["dead_letter_id"] = dead_letter["dead_letter_id"]
            append_ledger_event(run_dir, event)
            operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
            failure_payload = {
                "status": "failed",
                "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
                "message": failure["message"],
                "failure": failure,
                "preflight": preflight,
                "runtime_admission": runtime_admission,
                "dead_letter": dead_letter,
                "operator_surface": operator_surface,
            }
            raise SkillExecutionError(failure_payload["message"], failure_payload)

        attempts: list[dict[str, Any]] = []
        completed: subprocess.CompletedProcess[str] | None = None
        payload: dict[str, Any] | None = None
        final_stdout = ""
        final_stderr = ""
        final_error_code = ""
        final_error_message = ""
        final_retryable = False
        recovery_hints: list[str] = []

        for attempt_number in range(1, retry_budget + 2):
            attempt_started_at = utc_now_iso()
            try:
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=False,
                    cwd=str(root),
                    timeout=timeout_seconds or None,
                )
            except subprocess.TimeoutExpired as exc:
                final_stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout.decode("utf-8", errors="replace") if exc.stdout else "")
                final_stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr.decode("utf-8", errors="replace") if exc.stderr else "")
                final_error_code = "skill-timeout"
                final_error_message = f"Skill execution timed out for {skill_name} after {timeout_seconds:.3f}s."
                final_retryable = attempt_number <= retry_budget
                attempt_record = {
                    "attempt_number": attempt_number,
                    "started_at_utc": attempt_started_at,
                    "completed_at_utc": utc_now_iso(),
                    "outcome": "timeout",
                    "retryable": final_retryable,
                    "timeout_seconds": timeout_seconds,
                    "stdout_hash": stable_hash(final_stdout),
                    "stderr_hash": stable_hash(final_stderr),
                }
                if final_retryable:
                    attempt_record["backoff_ms"] = int(backoff_delay_seconds(retry_backoff_ms, attempt_number) * 1000)
                attempts.append(attempt_record)
                recovery_hints = ["Increase timeout_seconds for slower skills or inspect the skill for blocking I/O."]
                if final_retryable:
                    time.sleep(backoff_delay_seconds(retry_backoff_ms, attempt_number))
                    continue
                break

            final_stdout = completed.stdout
            final_stderr = completed.stderr
            if completed.returncode != 0:
                final_error_code = "skill-exit-nonzero"
                final_error_message = f"Skill execution failed for {skill_name}: {completed.stderr or completed.stdout}"
                final_retryable = attempt_number <= retry_budget and retryable_return_code(completed.returncode)
                attempt_record = {
                    "attempt_number": attempt_number,
                    "started_at_utc": attempt_started_at,
                    "completed_at_utc": utc_now_iso(),
                    "outcome": "exit-nonzero",
                    "retryable": final_retryable,
                    "exit_code": completed.returncode,
                    "stdout_hash": stable_hash(completed.stdout),
                    "stderr_hash": stable_hash(completed.stderr),
                }
                if final_retryable:
                    attempt_record["backoff_ms"] = int(backoff_delay_seconds(retry_backoff_ms, attempt_number) * 1000)
                attempts.append(attempt_record)
                recovery_hints = ["Inspect stderr/stdout for transient dependency failures or raise retry_budget for flaky upstream steps."]
                if final_retryable:
                    time.sleep(backoff_delay_seconds(retry_backoff_ms, attempt_number))
                    continue
                break

            try:
                loaded_payload = json.loads(completed.stdout)
            except json.JSONDecodeError as exc:
                final_error_code = "invalid-json-output"
                final_error_message = f"Invalid JSON skill output for {skill_name}"
                final_retryable = False
                attempts.append(
                    {
                        "attempt_number": attempt_number,
                        "started_at_utc": attempt_started_at,
                        "completed_at_utc": utc_now_iso(),
                        "outcome": "invalid-json",
                        "retryable": False,
                        "exit_code": completed.returncode,
                        "stdout_hash": stable_hash(completed.stdout),
                        "stderr_hash": stable_hash(f"Invalid JSON skill output: {exc}"),
                    }
                )
                recovery_hints = ["Fix the skill so it emits a JSON object payload." ]
                break

            if not isinstance(loaded_payload, dict):
                final_error_code = "non-object-payload"
                final_error_message = f"Skill {skill_name} returned a non-object payload"
                final_retryable = False
                attempts.append(
                    {
                        "attempt_number": attempt_number,
                        "started_at_utc": attempt_started_at,
                        "completed_at_utc": utc_now_iso(),
                        "outcome": "non-object-payload",
                        "retryable": False,
                        "exit_code": completed.returncode,
                        "stdout_hash": stable_hash(completed.stdout),
                        "stderr_hash": stable_hash(completed.stderr),
                    }
                )
                recovery_hints = ["Fix the skill so it emits a JSON object payload." ]
                break

            payload = loaded_payload
            attempts.append(
                {
                    "attempt_number": attempt_number,
                    "started_at_utc": attempt_started_at,
                    "completed_at_utc": utc_now_iso(),
                    "outcome": "completed",
                    "retryable": False,
                    "exit_code": completed.returncode,
                    "stdout_hash": stable_hash(completed.stdout),
                    "stderr_hash": stable_hash(completed.stderr),
                }
            )
            break

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
            "execution_policy": execution_policy,
            "declared_side_effects": declared_side_effects,
            "allowed_side_effects": allowed_side_effects,
            "skill_registry_entry": skill_entry,
            "declared_reads": preflight.get("declared_reads", []),
            "declared_writes": preflight.get("declared_writes", []),
            "resolved_read_paths": preflight.get("resolved_read_paths", []),
            "resolved_write_paths": preflight.get("resolved_write_paths", []),
            "preflight": preflight,
            "runtime_admission": runtime_admission,
            "stdout_hash": stable_hash(final_stdout),
            "stderr_hash": stable_hash(final_stderr),
            "lock_path": str(lock_path),
            "attempts": attempts,
            "attempt_count": len(attempts),
            "recovered_after_retry": len(attempts) > 1 and payload is not None,
        }

        if payload is None or completed is None:
            failure = structured_failure(
                error_code=final_error_code or "skill-execution-failed",
                message=final_error_message or f"Skill execution failed for {skill_name}.",
                retryable=final_retryable,
                attempts=attempts,
                execution_policy=execution_policy,
                recovery_hints=recovery_hints or ["Inspect the runtime ledger for the final failed attempt."],
            )
            event = {
                **base_event,
                "exit_code": completed.returncode if completed is not None else None,
                "status": "failed",
                "stdout": final_stdout,
                "stderr": final_stderr,
                "failure": failure,
            }
            existing_dead_letter_id = extract_dead_letter_id(final_error_message, final_stdout, final_stderr)
            dead_letter = {}
            if existing_dead_letter_id:
                event["dead_letter_id"] = existing_dead_letter_id
                dead_letter = {"dead_letter_id": existing_dead_letter_id, "status": "reused"}
            else:
                dead_letter = materialize_dead_letter(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    source_type="skill-execution",
                    source_name=skill_name,
                    message=failure["message"],
                    failure=failure,
                    summary={"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
                    related_paths={
                        "policy_path": runtime_admission.get("policy_path", ""),
                        "script_path": str(script_path),
                        "workspace_root": str(root),
                        "lock_path": str(lock_path),
                    },
                    command_hint=run_command_hint,
                )
                event["dead_letter_id"] = dead_letter["dead_letter_id"]
            append_ledger_event(run_dir, event)
            operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
            raise SkillExecutionError(
                failure["message"],
                {
                    "status": "failed",
                    "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
                    "message": failure["message"],
                    "failure": failure,
                    "preflight": preflight,
                    "runtime_admission": runtime_admission,
                    "dead_letter": dead_letter,
                    "operator_surface": operator_surface,
                },
            )

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
        failure = structured_failure(
            error_code="contract-postflight-blocked",
            message=f"Contract enforcement blocked completion for {skill_name}.",
            retryable=False,
            attempts=attempts,
            execution_policy=execution_policy,
            recovery_hints=["Align emitted artifact refs and summary paths with the declared write contract."],
        )
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
            "failure": failure,
        }
        dead_letter = materialize_dead_letter(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            source_type="skill-postflight",
            source_name=skill_name,
            message=failure["message"],
            failure=failure,
            summary={"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
            related_paths={
                "policy_path": runtime_admission.get("policy_path", ""),
                "receipt_path": str(receipt_file),
                "script_path": str(script_path),
                "workspace_root": str(root),
            },
            command_hint=run_command_hint,
        )
        event["dead_letter_id"] = dead_letter["dead_letter_id"]
        append_ledger_event(run_dir, event)
        operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
        failure_payload = {
            "status": "failed",
            "summary": {"skill_name": skill_name, "run_id": run_id, "round_id": round_id, "contract_mode": contract_mode},
            "message": failure["message"],
            "failure": failure,
            "preflight": preflight,
            "postflight": postflight,
            "runtime_admission": runtime_admission,
            "receipt_id": receipt_id,
            "receipt_path": str(receipt_file),
            "dead_letter": dead_letter,
            "operator_surface": operator_surface,
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
    operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
    manifest, cursor = update_after_run(run_dir, run_id=run_id, round_id=round_id, skill_name=skill_name, receipt_id=receipt_id, event_id=event_id)
    return {
        "status": "completed",
        "summary": {
            "skill_name": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "event_id": event_id,
            "receipt_id": receipt_id,
            "contract_mode": contract_mode,
            "attempt_count": len(attempts),
            "recovered_after_retry": len(attempts) > 1,
            "timeout_seconds": timeout_seconds,
            "retry_budget": retry_budget,
        },
        "event": event,
        "manifest": manifest,
        "cursor": cursor,
        "skill_payload": payload,
        "governance": {"preflight": preflight, "postflight": postflight, "runtime_admission": runtime_admission},
        "operator_surface": operator_surface,
    }
