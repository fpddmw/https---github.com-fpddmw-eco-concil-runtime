from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.ledger import append_ledger_event, write_receipt
from eco_council_runtime.kernel.core.locking import exclusive_runtime_lock
from eco_council_runtime.kernel.core.manifest import update_after_run
from eco_council_runtime.kernel.core.registry import resolve_skill_entry, workspace_root
from eco_council_runtime.kernel.execution.executor_command_hints import skill_command_hint
from eco_council_runtime.kernel.execution.executor_common import (
    SkillExecutionError,
    backoff_delay_seconds,
    json_hash,
    maybe_text,
    new_runtime_event_id,
    retryable_return_code,
    stable_hash,
    utc_now_iso,
)
from eco_council_runtime.kernel.execution.executor_failures import (
    extract_dead_letter_id,
    refresh_runtime_surfaces_safely,
    structured_failure,
)
from eco_council_runtime.kernel.governance.runtime_governance import (
    CONTRACT_MODES,
    postflight_skill_execution,
    preflight_skill_execution,
)
from eco_council_runtime.kernel.governance.skill_approvals import mark_skill_approval_consumed
from eco_council_runtime.kernel.operator.operations import (
    admission_error_code,
    evaluate_execution_admission,
    materialize_dead_letter,
)

def command_uses_runtime_builtins(skill_entry: dict[str, Any]) -> bool:
    declared_inputs = skill_entry.get("declared_inputs", {}) if isinstance(skill_entry.get("declared_inputs"), dict) else {}
    required_inputs = declared_inputs.get("required", []) if isinstance(declared_inputs.get("required"), list) else []
    normalized_required = {maybe_text(item).replace("-", "_") for item in required_inputs}
    return "run_dir" in normalized_required


def build_skill_subprocess_command(
    *,
    script_path: Path,
    run_dir: Path,
    run_id: str,
    round_id: str,
    skill_entry: dict[str, Any],
    skill_args: list[str],
) -> list[str]:
    command = [sys.executable, str(script_path)]
    if command_uses_runtime_builtins(skill_entry):
        command.extend(["--run-dir", str(run_dir), "--run-id", run_id, "--round-id", round_id])
    command.extend(skill_args)
    return command


def _argument_present(argv: list[str], option: str) -> bool:
    normalized = maybe_text(option)
    return any(maybe_text(item) == normalized for item in argv)


def _safe_path_fragment(value: str) -> str:
    fragment = "".join(char if char.isalnum() else "-" for char in maybe_text(value))
    return fragment.strip("-") or "artifact"


def maybe_inject_runtime_capture_output(
    *,
    run_dir: Path,
    round_id: str,
    skill_name: str,
    skill_args: list[str],
) -> tuple[list[str], dict[str, str]]:
    """Add the catalog-declared raw artifact output path for direct fetch runs.

    This is an operational capture boundary only. It does not select sources or
    decide evidence uptake; it makes direct fetch receipts normalizeable later.
    """
    argv = [maybe_text(item) for item in skill_args if maybe_text(item)]
    if not argv or argv[0] != "fetch":
        return argv, {}
    try:
        from eco_council_runtime.kernel.source_queue.source_queue_contract import (  # noqa: PLC0415
            source_config,
            source_runtime_output_arg,
            source_runtime_output_mode,
        )
    except Exception:  # noqa: BLE001
        return argv, {}

    try:
        config = source_config(skill_name)
        output_arg = source_runtime_output_arg(skill_name)
        output_mode = source_runtime_output_mode(skill_name)
    except Exception:  # noqa: BLE001
        return argv, {}
    if not output_arg or output_mode not in {"file", "dir"}:
        return argv, {}
    if _argument_present(argv, output_arg):
        return argv, {}

    suffix = maybe_text(config.get("default_suffix")) or ".json"
    hash_part = stable_hash(skill_name, round_id, json.dumps(argv, ensure_ascii=True, sort_keys=True))[:12]
    base = (
        run_dir
        / "raw"
        / round_id
        / "direct-fetch"
        / f"{_safe_path_fragment(skill_name)}-{hash_part}"
    ).resolve()
    output_path = base.with_suffix(suffix) if output_mode == "file" else base
    output_path.parent.mkdir(parents=True, exist_ok=True)
    argv = [*argv, output_arg, str(output_path)]
    return argv, {
        "injected_output_arg": output_arg,
        "injected_output_mode": output_mode,
        "injected_output_path": str(output_path),
        "injection_semantics": "runtime raw-artifact capture for later normalization; not source selection",
    }


def skill_config_env_path(script_path: Path) -> Path | None:
    if script_path.parent.name != "scripts":
        return None
    candidate = script_path.parent.parent / "assets" / "config.env"
    if candidate.exists():
        return candidate

    skill_dir = script_path.parent.parent
    if skill_dir.name.startswith("fetch-regulationsgov-"):
        for sibling_name in ("fetch-regulationsgov-comments", "fetch-regulationsgov-comment-detail"):
            sibling_candidate = skill_dir.parent / sibling_name / "assets" / "config.env"
            if sibling_candidate.exists():
                return sibling_candidate
    return None


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def build_skill_subprocess_env(
    *,
    script_path: Path,
    actor_role: str,
    resolved_actor_role: str,
    skill_name: str,
    run_id: str,
    round_id: str,
) -> tuple[dict[str, str], list[str]]:
    loaded_env_files: list[str] = []
    file_env: dict[str, str] = {}
    config_path = skill_config_env_path(script_path)
    if config_path is not None:
        file_env.update(parse_env_file(config_path))
        loaded_env_files.append(str(config_path.resolve()))
    env = {
        **file_env,
        **os.environ,
        "OPENCLAW_ACTOR_ROLE": actor_role,
        "OPENCLAW_RESOLVED_ACTOR_ROLE": resolved_actor_role,
        "OPENCLAW_SKILL_NAME": skill_name,
        "OPENCLAW_RUN_ID": run_id,
        "OPENCLAW_ROUND_ID": round_id,
    }
    return env, loaded_env_files


def run_skill(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    actor_role: str,
    skill_args: list[str],
    contract_mode: str = "warn",
    workspace: Path | None = None,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
    skill_approval_request_id: str = "",
) -> dict[str, Any]:
    if contract_mode not in CONTRACT_MODES:
        raise ValueError(f"Unsupported contract_mode: {contract_mode}")
    root = workspace or workspace_root()
    skill_args, runtime_output_capture = maybe_inject_runtime_capture_output(
        run_dir=run_dir,
        round_id=round_id,
        skill_name=skill_name,
        skill_args=skill_args,
    )
    preflight = preflight_skill_execution(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        actor_role=actor_role,
        skill_args=skill_args,
        contract_mode=contract_mode,
        workspace=root,
        timeout_seconds=timeout_seconds,
        retry_budget=retry_budget,
        retry_backoff_ms=retry_backoff_ms,
        allow_side_effects=allow_side_effects,
        skill_approval_request_id=skill_approval_request_id,
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
    command = build_skill_subprocess_command(
        script_path=script_path,
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_entry=skill_entry,
        skill_args=skill_args,
    )
    command_snapshot = {
        "argv": command,
        "cwd": str(root),
        "python_executable": sys.executable,
        "workspace_root": str(root),
        "script_path": str(script_path),
        "actor_role": actor_role,
        "skill_approval_request_id": maybe_text(skill_approval_request_id),
    }
    if runtime_output_capture:
        command_snapshot["runtime_output_capture"] = runtime_output_capture
    env, loaded_env_files = build_skill_subprocess_env(
        script_path=script_path,
        actor_role=actor_role,
        resolved_actor_role=maybe_text(preflight.get("resolved_actor_role")),
        skill_name=skill_name,
        run_id=run_id,
        round_id=round_id,
    )
    if loaded_env_files:
        command_snapshot["loaded_env_files"] = loaded_env_files
    run_command_hint = skill_command_hint(
        "run-skill",
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        actor_role=actor_role,
        contract_mode=contract_mode,
        skill_args=skill_args,
    )
    preflight_command_hint = skill_command_hint(
        "preflight-skill",
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        actor_role=actor_role,
        contract_mode=contract_mode,
        skill_args=skill_args,
    )
    execution_input_hash = json_hash(
        {
            "run_id": run_id,
            "round_id": round_id,
            "skill_name": skill_name,
            "actor_role": actor_role,
            "skill_args": skill_args,
            "skill_options": skill_options,
            "command_snapshot": command_snapshot,
            "declared_contract": skill_entry.get("declared_contract", {}),
            "preflight": preflight,
            "contract_mode": contract_mode,
            "execution_policy": execution_policy,
            "declared_side_effects": declared_side_effects,
            "allowed_side_effects": allowed_side_effects,
            "skill_approval_request_id": maybe_text(skill_approval_request_id),
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
    with exclusive_runtime_lock(
        run_dir,
        metadata={
            "run_id": run_id,
            "round_id": round_id,
            "skill_name": skill_name,
            "actor_role": actor_role,
            "contract_mode": contract_mode,
            "execution_input_hash": execution_input_hash,
            "started_at_utc": started_at,
            "skill_approval_request_id": maybe_text(skill_approval_request_id),
        },
    ) as lock_path:
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
                "actor_role": actor_role,
                "resolved_actor_role": preflight.get("resolved_actor_role", ""),
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
                "summary": {
                    "skill_name": skill_name,
                    "run_id": run_id,
                    "round_id": round_id,
                    "contract_mode": contract_mode,
                    "actor_role": actor_role,
                },
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
                "actor_role": actor_role,
                "resolved_actor_role": preflight.get("resolved_actor_role", ""),
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
                "summary": {
                    "skill_name": skill_name,
                    "run_id": run_id,
                    "round_id": round_id,
                    "contract_mode": contract_mode,
                    "actor_role": actor_role,
                },
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
                    env=env,
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
            "actor_role": actor_role,
            "resolved_actor_role": preflight.get("resolved_actor_role", ""),
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
            "skill_approval": preflight.get("skill_approval", {}),
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
                    "summary": {
                        "skill_name": skill_name,
                        "run_id": run_id,
                        "round_id": round_id,
                        "contract_mode": contract_mode,
                        "actor_role": actor_role,
                    },
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
    receipt_write = write_receipt(
        run_dir,
        receipt_id,
        payload,
        runtime_context={
            "run_id": run_id,
            "round_id": round_id,
            "skill_name": skill_name,
            "event_id": event_id,
            "event_type": "skill-execution",
            "actor_role": actor_role,
            "resolved_actor_role": preflight.get("resolved_actor_role", ""),
            "contract_mode": contract_mode,
            "execution_input_hash": execution_input_hash,
            "payload_hash": json_hash(payload),
            "lock_path": base_event.get("lock_path", ""),
            "command_snapshot": command_snapshot,
            "skill_args": skill_args,
            "skill_options": skill_options,
            "attempts": attempts,
            "attempt_count": len(attempts),
            "recovered_after_retry": len(attempts) > 1 and payload is not None,
            "preflight": preflight,
            "postflight": postflight,
            "runtime_admission": runtime_admission,
            "skill_approval": preflight.get("skill_approval", {}),
        },
    )
    receipt_file = Path(maybe_text(receipt_write.get("receipt_path")))
    if maybe_text(receipt_write.get("write_status")) == "conflict":
        failure = structured_failure(
            error_code=maybe_text(receipt_write.get("error_code"))
            or "receipt-payload-hash-conflict",
            message=(
                f"Runtime receipt conflict for {skill_name}: receipt `{receipt_id}` "
                "already exists with a different payload hash."
            ),
            retryable=False,
            attempts=attempts,
            execution_policy=execution_policy,
            recovery_hints=[
                "Inspect the existing receipt and emitted payload before retrying.",
                "Use a new receipt_id only when this is an intentional new execution result.",
            ],
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
            "receipt_write": receipt_write,
            "postflight": postflight,
            "failure": failure,
        }
        dead_letter = materialize_dead_letter(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            source_type="receipt-conflict",
            source_name=skill_name,
            message=failure["message"],
            failure={**failure, "receipt_write": receipt_write},
            summary={
                "skill_name": skill_name,
                "run_id": run_id,
                "round_id": round_id,
                "contract_mode": contract_mode,
                "receipt_id": receipt_id,
            },
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
            "summary": {
                "skill_name": skill_name,
                "run_id": run_id,
                "round_id": round_id,
                "contract_mode": contract_mode,
                "actor_role": actor_role,
                "receipt_id": receipt_id,
            },
            "message": failure["message"],
            "failure": failure,
            "preflight": preflight,
            "postflight": postflight,
            "runtime_admission": runtime_admission,
            "receipt_id": receipt_id,
            "receipt_path": str(receipt_file),
            "receipt_write": receipt_write,
            "dead_letter": dead_letter,
            "operator_surface": operator_surface,
        }
        raise SkillExecutionError(failure_payload["message"], failure_payload)
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
            "receipt_write": receipt_write,
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
            "summary": {
                "skill_name": skill_name,
                "run_id": run_id,
                "round_id": round_id,
                "contract_mode": contract_mode,
                "actor_role": actor_role,
            },
            "message": failure["message"],
            "failure": failure,
            "preflight": preflight,
            "postflight": postflight,
            "runtime_admission": runtime_admission,
            "receipt_id": receipt_id,
            "receipt_path": str(receipt_file),
            "receipt_write": receipt_write,
            "dead_letter": dead_letter,
            "operator_surface": operator_surface,
        }
        raise SkillExecutionError(failure_payload["message"], failure_payload)

    skill_approval = (
        preflight.get("skill_approval", {})
        if isinstance(preflight.get("skill_approval"), dict)
        else {}
    )
    skill_approval_consumption: dict[str, Any] = {}
    if bool(skill_approval.get("required")) and maybe_text(skill_approval.get("status")) == "approved":
        request_id = maybe_text(skill_approval.get("request_id"))
        if request_id:
            try:
                consumed = mark_skill_approval_consumed(
                    run_dir,
                    request_id=request_id,
                    consumed_by_role=preflight.get("resolved_actor_role") or actor_role,
                    execution_receipt_id=receipt_id,
                    execution_event_id=event_id,
                    execution_status=maybe_text(payload.get("status")) or "completed",
                )
            except ValueError as exc:
                failure = structured_failure(
                    error_code="skill-approval-consumption-failed",
                    message=str(exc),
                    retryable=False,
                    attempts=attempts,
                    execution_policy=execution_policy,
                    recovery_hints=[
                        "Inspect skill approval request state and create a new approved request before retrying.",
                    ],
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
                    "receipt_write": receipt_write,
                    "postflight": postflight,
                    "failure": failure,
                }
                dead_letter = materialize_dead_letter(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    source_type="skill-approval-consumption",
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
                    "summary": {
                        "skill_name": skill_name,
                        "run_id": run_id,
                        "round_id": round_id,
                        "contract_mode": contract_mode,
                        "actor_role": actor_role,
                    },
                    "message": failure["message"],
                    "failure": failure,
                    "preflight": preflight,
                    "postflight": postflight,
                    "runtime_admission": runtime_admission,
                    "receipt_id": receipt_id,
                    "receipt_path": str(receipt_file),
                    "receipt_write": receipt_write,
                    "dead_letter": dead_letter,
                    "operator_surface": operator_surface,
                }
                raise SkillExecutionError(failure_payload["message"], failure_payload)
            skill_approval_consumption = consumed

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
        "receipt_write": receipt_write,
        "postflight": postflight,
        "skill_approval_consumption": skill_approval_consumption,
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
            "actor_role": actor_role,
            "resolved_actor_role": preflight.get("resolved_actor_role", ""),
            "attempt_count": len(attempts),
            "recovered_after_retry": len(attempts) > 1,
            "timeout_seconds": timeout_seconds,
            "retry_budget": retry_budget,
            "skill_approval_request_id": maybe_text(skill_approval.get("request_id"))
            if isinstance(skill_approval, dict)
            else "",
            "receipt_write_status": maybe_text(receipt_write.get("write_status")),
        },
        "event": event,
        "manifest": manifest,
        "cursor": cursor,
        "skill_payload": payload,
        "governance": {
            "preflight": preflight,
            "postflight": postflight,
            "runtime_admission": runtime_admission,
            "skill_approval_consumption": skill_approval_consumption,
        },
        "operator_surface": operator_surface,
    }


__all__ = (
    "SkillExecutionError",
    "backoff_delay_seconds",
    "extract_dead_letter_id",
    "json_hash",
    "maybe_text",
    "new_runtime_event_id",
    "refresh_runtime_surfaces_safely",
    "retryable_return_code",
    "run_skill",
    "skill_command_hint",
    "stable_hash",
    "structured_failure",
    "utc_now_iso",
)
