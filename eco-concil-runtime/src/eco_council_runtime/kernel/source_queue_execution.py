from __future__ import annotations

import json
import shlex
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from .executor import backoff_delay_seconds
from .ledger import append_ledger_event
from .operations import admission_error_code, evaluate_execution_admission, materialize_dead_letter, refresh_runtime_surfaces
from .source_queue_contract import maybe_text, normalize_artifact_capture, stable_hash, unique_texts, utc_now_iso

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]


class DetachedFetchExecutionError(RuntimeError):
    def __init__(self, message: str, payload: dict[str, Any] | None = None):
        super().__init__(message)
        self.payload = payload or {}


def new_detached_fetch_event_id(*parts: Any) -> str:
    return "detfetch-" + stable_hash(*parts)[:20]


def detached_fetch_command_hint(argv: list[str], cwd: Path) -> str:
    return f"(cd {cwd} && {shlex.join(argv)})"


def refresh_runtime_surfaces_safely(run_dir: Path, *, round_id: str) -> dict[str, Any]:
    try:
        return refresh_runtime_surfaces(run_dir, round_id=round_id)
    except Exception:  # noqa: BLE001
        return {}


def structured_detached_fetch_failure(
    *,
    error_code: str,
    message: str,
    retryable: bool,
    attempts: list[dict[str, Any]],
    execution_policy: dict[str, Any],
    recovery_hints: list[str],
    runtime_admission: dict[str, Any] | None = None,
    dead_letter_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error_code": error_code,
        "message": message,
        "retryable": retryable,
        "attempt_count": len(attempts),
        "last_attempt": attempts[-1] if attempts else {},
        "execution_policy": execution_policy,
        "recovery_hints": recovery_hints,
    }
    if runtime_admission:
        payload["runtime_admission"] = runtime_admission
    if dead_letter_id:
        payload["dead_letter_id"] = dead_letter_id
    return payload


def append_detached_fetch_event(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    step_id: str,
    source_skill: str,
    started_at_utc: str,
    completed_at_utc: str,
    status: str,
    command_snapshot: dict[str, Any],
    artifact_path: Path,
    artifact_capture: str,
    execution_policy: dict[str, Any],
    declared_side_effects: list[str],
    requested_side_effect_approvals: list[str],
    runtime_admission: dict[str, Any],
    attempts: list[dict[str, Any]],
    stdout_text: str,
    stderr_text: str,
    failure: dict[str, Any] | None = None,
    dead_letter_id: str = "",
) -> dict[str, Any]:
    event = {
        "schema_version": "runtime-event-v3",
        "event_id": new_detached_fetch_event_id(run_id, round_id, step_id, started_at_utc, completed_at_utc, status),
        "event_type": "detached-fetch-execution",
        "run_id": run_id,
        "round_id": round_id,
        "skill_name": source_skill or step_id,
        "step_id": step_id,
        "source_skill": source_skill,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "status": status,
        "command_snapshot": command_snapshot,
        "artifact_path": str(artifact_path),
        "artifact_capture": artifact_capture,
        "execution_policy": execution_policy,
        "declared_side_effects": declared_side_effects,
        "requested_side_effect_approvals": requested_side_effect_approvals,
        "allow_side_effects": declared_side_effects,
        "runtime_admission": runtime_admission,
        "attempts": attempts,
        "attempt_count": len(attempts),
        "recovered_after_retry": len(attempts) > 1 and status == "completed",
        "stdout_hash": stable_hash(stdout_text),
        "stderr_hash": stable_hash(stderr_text),
    }
    if failure is not None:
        event["failure"] = failure
    if dead_letter_id:
        event["dead_letter_id"] = dead_letter_id
    append_ledger_event(run_dir, event)
    return event


def render_fetch_argv(step: dict[str, Any], *, run_dir: Path, run_id: str, round_id: str) -> list[str]:
    artifact_path = maybe_text(step.get("artifact_path"))
    artifact_dir = maybe_text(step.get("artifact_dir")) or str(Path(artifact_path).expanduser().resolve().parent)
    artifact_file = Path(artifact_path).expanduser().resolve() if artifact_path else run_dir
    substitutions = {
        "artifact_path": artifact_path,
        "artifact_dir": artifact_dir,
        "artifact_basename": artifact_file.name,
        "artifact_stem": artifact_file.stem,
        "run_dir": str(run_dir),
        "run_id": run_id,
        "round_id": round_id,
        "source_skill": maybe_text(step.get("source_skill")),
    }
    argv = step.get("fetch_argv") if isinstance(step.get("fetch_argv"), list) else []
    return [maybe_text(arg).format(**substitutions) for arg in argv if maybe_text(arg)]


def fetch_execution_policy(step: dict[str, Any]) -> dict[str, Any]:
    payload = step.get("fetch_execution_policy") if isinstance(step.get("fetch_execution_policy"), dict) else {}
    timeout_seconds = payload.get("timeout_seconds")
    retry_budget = payload.get("retry_budget")
    retry_backoff_ms = payload.get("retry_backoff_ms")
    try:
        resolved_timeout = max(0.0, float(timeout_seconds if timeout_seconds is not None else 300.0))
    except (TypeError, ValueError):
        resolved_timeout = 300.0
    try:
        resolved_retry_budget = max(0, int(retry_budget if retry_budget is not None else 0))
    except (TypeError, ValueError):
        resolved_retry_budget = 0
    try:
        resolved_retry_backoff_ms = max(0, int(retry_backoff_ms if retry_backoff_ms is not None else 250))
    except (TypeError, ValueError):
        resolved_retry_backoff_ms = 250
    return {
        "timeout_seconds": resolved_timeout,
        "retry_budget": resolved_retry_budget,
        "retry_backoff_ms": resolved_retry_backoff_ms,
    }


def declared_side_effects(step: dict[str, Any]) -> list[str]:
    values = step.get("declared_side_effects") if isinstance(step.get("declared_side_effects"), list) else []
    if not values and isinstance(step.get("allow_side_effects"), list):
        values = step.get("allow_side_effects")
    return unique_texts(values)


def requested_side_effect_approvals(
    step: dict[str, Any],
    *,
    declared_values: list[str] | None = None,
    step_id: str = "",
) -> list[str]:
    values = (
        step.get("requested_side_effect_approvals")
        if isinstance(step.get("requested_side_effect_approvals"), list)
        else []
    )
    if not values and isinstance(step.get("allow_side_effects"), list):
        values = step.get("allow_side_effects")
    approvals = unique_texts(values)
    declared = declared_values if declared_values is not None else declared_side_effects(step)
    undeclared = [value for value in approvals if value not in declared]
    if undeclared:
        normalized_step_id = step_id or maybe_text(step.get("step_id")) or "unknown-step"
        raise RuntimeError(
            "Detached fetch requested_side_effect_approvals must be a subset of declared_side_effects "
            f"for {normalized_step_id}: {', '.join(undeclared)}"
        )
    return approvals


def fetch_cwd(step: dict[str, Any]) -> Path:
    value = maybe_text(step.get("fetch_cwd")) or str(WORKSPACE_ROOT)
    return Path(value).expanduser().resolve()


def resolved_artifact_path(step: dict[str, Any]) -> Path:
    return Path(maybe_text(step.get("artifact_path"))).expanduser().resolve()


def validate_detached_fetch_step(step: dict[str, Any], *, run_dir: Path, run_id: str, round_id: str) -> dict[str, Any]:
    step_id = maybe_text(step.get("step_id")) or "unknown-step"
    artifact_path = resolved_artifact_path(step)
    argv = render_fetch_argv(step, run_dir=run_dir, run_id=run_id, round_id=round_id)
    if not argv:
        raise RuntimeError(f"Detached fetch step has no fetch_argv: {step_id}")
    cwd = fetch_cwd(step)
    if not cwd.exists():
        raise RuntimeError(f"Detached fetch cwd does not exist for {step_id}: {cwd}")
    capture_mode = normalize_artifact_capture(step.get("artifact_capture"))
    policy = fetch_execution_policy(step)
    declared_effects = declared_side_effects(step)
    requested_approvals = requested_side_effect_approvals(step, declared_values=declared_effects, step_id=step_id)
    return {
        "step_id": step_id,
        "argv": argv,
        "cwd": cwd,
        "artifact_path": artifact_path,
        "artifact_capture": capture_mode,
        "execution_policy": policy,
        "declared_side_effects": declared_effects,
        "requested_side_effect_approvals": requested_approvals,
        "allow_side_effects": declared_effects,
    }


def materialize_captured_artifact(
    *,
    artifact_path: Path,
    capture_mode: str,
    stdout_text: str,
) -> None:
    if capture_mode == "stdout-json":
        try:
            payload = json.loads(stdout_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Detached fetch stdout was not valid JSON.") from exc
        artifact_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return
    if capture_mode == "stdout-text":
        artifact_path.write_text(stdout_text, encoding="utf-8")
        return
    if capture_mode == "direct-file":
        if not artifact_path.exists():
            raise RuntimeError(f"Detached fetch expected a direct-file artifact at {artifact_path}")
        return
    raise RuntimeError(f"Unsupported detached fetch artifact_capture: {capture_mode}")


def execute_detached_fetch_step(
    step: dict[str, Any],
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
) -> tuple[Path, dict[str, Any]]:
    resolved = validate_detached_fetch_step(step, run_dir=run_dir, run_id=run_id, round_id=round_id)
    step_id = resolved["step_id"]
    source_skill = maybe_text(step.get("source_skill"))
    artifact_path = resolved["artifact_path"]
    policy = resolved["execution_policy"]
    timeout_seconds = float(policy.get("timeout_seconds") or 0.0)
    retry_budget = int(policy.get("retry_budget") or 0)
    retry_backoff_ms = int(policy.get("retry_backoff_ms") or 0)
    command_snapshot = {
        "argv": resolved["argv"],
        "cwd": str(resolved["cwd"]),
    }
    runtime_admission = evaluate_execution_admission(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        actor_kind="detached-fetch",
        actor_name=step_id,
        declared_side_effects=resolved["declared_side_effects"],
        requested_side_effect_approvals=resolved["requested_side_effect_approvals"],
        execution_policy=policy,
        resolved_read_paths=[],
        resolved_write_paths=[str(artifact_path)],
        cwd_path=str(resolved["cwd"]),
        workspace=WORKSPACE_ROOT,
    )
    started_at_utc = utc_now_iso()
    if bool(runtime_admission.get("block_execution")):
        failure = structured_detached_fetch_failure(
            error_code=admission_error_code(runtime_admission),
            message=f"Detached fetch blocked by runtime admission policy for {step_id}.",
            retryable=False,
            attempts=[],
            execution_policy=policy,
            recovery_hints=[
                maybe_text(issue.get("message"))
                for issue in runtime_admission.get("issues", [])
                if isinstance(issue, dict) and maybe_text(issue.get("message"))
            ]
            or ["Adjust the admission policy or detached fetch approvals before retrying."],
            runtime_admission=runtime_admission,
        )
        dead_letter = materialize_dead_letter(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            source_type="detached-fetch-admission",
            source_name=step_id,
            message=failure["message"],
            failure=failure,
            summary={"step_id": step_id, "source_skill": source_skill, "run_id": run_id, "round_id": round_id},
            related_paths={
                "policy_path": runtime_admission.get("policy_path", ""),
                "artifact_path": str(artifact_path),
            },
            command_hint=detached_fetch_command_hint(resolved["argv"], resolved["cwd"]),
        )
        failure["dead_letter_id"] = dead_letter["dead_letter_id"]
        event = append_detached_fetch_event(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            step_id=step_id,
            source_skill=source_skill,
            started_at_utc=started_at_utc,
            completed_at_utc=utc_now_iso(),
            status="blocked",
            command_snapshot=command_snapshot,
            artifact_path=artifact_path,
            artifact_capture=resolved["artifact_capture"],
            execution_policy=policy,
            declared_side_effects=resolved["declared_side_effects"],
            requested_side_effect_approvals=resolved["requested_side_effect_approvals"],
            runtime_admission=runtime_admission,
            attempts=[],
            stdout_text="",
            stderr_text="",
            failure=failure,
            dead_letter_id=dead_letter["dead_letter_id"],
        )
        operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
        raise DetachedFetchExecutionError(
            failure["message"],
            {
                "status": "failed",
                "summary": {"step_id": step_id, "source_skill": source_skill, "run_id": run_id, "round_id": round_id},
                "message": failure["message"],
                "failure": failure,
                "runtime_admission": runtime_admission,
                "dead_letter": dead_letter,
                "event": event,
                "operator_surface": operator_surface,
            },
        )

    artifact_path.parent.mkdir(parents=True, exist_ok=True)

    attempts: list[dict[str, Any]] = []
    final_stdout = ""
    final_stderr = ""
    final_error_code = ""
    final_error_message = ""
    final_retryable = False
    final_exit_code: int | None = None
    recovery_hints: list[str] = []
    for attempt_number in range(1, retry_budget + 2):
        attempt_started_at = utc_now_iso()
        try:
            completed = subprocess.run(
                resolved["argv"],
                cwd=str(resolved["cwd"]),
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds or None,
            )
        except subprocess.TimeoutExpired as exc:
            final_stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout.decode("utf-8", errors="replace") if exc.stdout else "")
            final_stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr.decode("utf-8", errors="replace") if exc.stderr else "")
            final_error_code = "detached-fetch-timeout"
            final_error_message = f"Detached fetch timed out for {step_id} after {timeout_seconds:.3f}s."
            final_retryable = attempt_number <= retry_budget
            attempts.append(
                {
                    "attempt_number": attempt_number,
                    "started_at_utc": attempt_started_at,
                    "completed_at_utc": utc_now_iso(),
                    "outcome": "timeout",
                    "retryable": final_retryable,
                    "timeout_seconds": timeout_seconds,
                    "stdout_hash": stable_hash(final_stdout),
                    "stderr_hash": stable_hash(final_stderr),
                }
            )
            recovery_hints = ["Increase timeout_seconds or inspect the detached fetch command for a blocking dependency."]
            if final_retryable:
                time.sleep(backoff_delay_seconds(retry_backoff_ms, attempt_number))
                continue
            break

        final_stdout = completed.stdout
        final_stderr = completed.stderr
        final_exit_code = completed.returncode
        if completed.returncode != 0:
            final_error_code = "detached-fetch-exit-nonzero"
            detail = completed.stderr.strip() or completed.stdout.strip() or f"exit={completed.returncode}"
            final_error_message = f"Detached fetch command failed for {step_id}: {detail}"
            final_retryable = attempt_number <= retry_budget
            attempts.append(
                {
                    "attempt_number": attempt_number,
                    "started_at_utc": attempt_started_at,
                    "completed_at_utc": utc_now_iso(),
                    "outcome": "exit-nonzero",
                    "retryable": final_retryable,
                    "exit_code": completed.returncode,
                    "stdout_hash": stable_hash(completed.stdout),
                    "stderr_hash": stable_hash(completed.stderr),
                }
            )
            recovery_hints = ["Inspect the detached fetch stderr/stdout, then retry only if the upstream dependency is healthy."]
            if final_retryable:
                time.sleep(backoff_delay_seconds(retry_backoff_ms, attempt_number))
                continue
            break

        try:
            materialize_captured_artifact(
                artifact_path=artifact_path,
                capture_mode=resolved["artifact_capture"],
                stdout_text=completed.stdout,
            )
        except RuntimeError as exc:
            if resolved["artifact_capture"] == "stdout-json":
                final_error_code = "detached-fetch-invalid-json-output"
            elif resolved["artifact_capture"] == "direct-file":
                final_error_code = "detached-fetch-direct-file-missing"
            else:
                final_error_code = "detached-fetch-artifact-capture-failed"
            final_error_message = f"Detached fetch artifact capture failed for {step_id}: {exc}"
            final_retryable = False
            attempts.append(
                {
                    "attempt_number": attempt_number,
                    "started_at_utc": attempt_started_at,
                    "completed_at_utc": utc_now_iso(),
                    "outcome": "artifact-capture-failed",
                    "retryable": False,
                    "exit_code": completed.returncode,
                    "stdout_hash": stable_hash(completed.stdout),
                    "stderr_hash": stable_hash(str(exc)),
                }
            )
            recovery_hints = ["Fix the detached fetch artifact capture mode or emitted payload before retrying."]
            break

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
        event = append_detached_fetch_event(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            step_id=step_id,
            source_skill=source_skill,
            started_at_utc=started_at_utc,
            completed_at_utc=utc_now_iso(),
            status="completed",
            command_snapshot=command_snapshot,
            artifact_path=artifact_path,
            artifact_capture=resolved["artifact_capture"],
            execution_policy=policy,
            declared_side_effects=resolved["declared_side_effects"],
            requested_side_effect_approvals=resolved["requested_side_effect_approvals"],
            runtime_admission=runtime_admission,
            attempts=attempts,
            stdout_text=completed.stdout,
            stderr_text=completed.stderr,
        )
        operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
        return artifact_path, {
            "command_snapshot": command_snapshot,
            "artifact_capture": resolved["artifact_capture"],
            "execution_policy": policy,
            "declared_side_effects": resolved["declared_side_effects"],
            "requested_side_effect_approvals": resolved["requested_side_effect_approvals"],
            "allow_side_effects": resolved["allow_side_effects"],
            "runtime_admission": runtime_admission,
            "attempts": attempts,
            "attempt_count": len(attempts),
            "recovered_after_retry": len(attempts) > 1,
            "stdout_hash": stable_hash(completed.stdout),
            "stderr_hash": stable_hash(completed.stderr),
            "event_id": maybe_text(event.get("event_id")),
            "operator_surface": operator_surface,
        }

    failure = structured_detached_fetch_failure(
        error_code=final_error_code or "detached-fetch-failed",
        message=final_error_message or f"Detached fetch failed for {step_id}.",
        retryable=final_retryable,
        attempts=attempts,
        execution_policy=policy,
        recovery_hints=recovery_hints or ["Inspect the detached fetch command, runtime admission, and artifact path before retrying."],
        runtime_admission=runtime_admission,
    )
    dead_letter = materialize_dead_letter(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        source_type="detached-fetch",
        source_name=step_id,
        message=failure["message"],
        failure=failure,
        summary={"step_id": step_id, "source_skill": source_skill, "run_id": run_id, "round_id": round_id},
        related_paths={
            "policy_path": runtime_admission.get("policy_path", ""),
            "artifact_path": str(artifact_path),
        },
        command_hint=detached_fetch_command_hint(resolved["argv"], resolved["cwd"]),
    )
    failure["dead_letter_id"] = dead_letter["dead_letter_id"]
    event = append_detached_fetch_event(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        step_id=step_id,
        source_skill=source_skill,
        started_at_utc=started_at_utc,
        completed_at_utc=utc_now_iso(),
        status="failed",
        command_snapshot=command_snapshot,
        artifact_path=artifact_path,
        artifact_capture=resolved["artifact_capture"],
        execution_policy=policy,
        declared_side_effects=resolved["declared_side_effects"],
        requested_side_effect_approvals=resolved["requested_side_effect_approvals"],
        runtime_admission=runtime_admission,
        attempts=attempts,
        stdout_text=final_stdout,
        stderr_text=final_stderr,
        failure=failure,
        dead_letter_id=dead_letter["dead_letter_id"],
    )
    operator_surface = refresh_runtime_surfaces_safely(run_dir, round_id=round_id)
    raise DetachedFetchExecutionError(
        failure["message"],
        {
            "status": "failed",
            "summary": {"step_id": step_id, "source_skill": source_skill, "run_id": run_id, "round_id": round_id},
            "message": failure["message"],
            "failure": failure,
            "runtime_admission": runtime_admission,
            "dead_letter": dead_letter,
            "event": event,
            "operator_surface": operator_surface,
            "exit_code": final_exit_code,
        },
    )


def copy_import_artifact(step: dict[str, Any]) -> Path:
    step_id = maybe_text(step.get("step_id")) or "unknown-step"
    source_artifact_path = Path(maybe_text(step.get("source_artifact_path"))).expanduser().resolve()
    if not source_artifact_path.exists():
        raise FileNotFoundError(f"Source artifact is missing for {step_id}: {source_artifact_path}")
    artifact_path = resolved_artifact_path(step)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_artifact_path, artifact_path)
    return artifact_path


__all__ = [
    "DetachedFetchExecutionError",
    "copy_import_artifact",
    "declared_side_effects",
    "execute_detached_fetch_step",
    "fetch_cwd",
    "fetch_execution_policy",
    "render_fetch_argv",
    "resolved_artifact_path",
    "requested_side_effect_approvals",
    "validate_detached_fetch_step",
]
