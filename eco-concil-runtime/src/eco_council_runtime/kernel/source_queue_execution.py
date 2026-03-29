from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from .executor import backoff_delay_seconds
from .source_queue_contract import maybe_text, normalize_artifact_capture, stable_hash, unique_texts

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]


def render_fetch_argv(step: dict[str, Any], *, run_dir: Path, run_id: str, round_id: str) -> list[str]:
    artifact_path = maybe_text(step.get("artifact_path"))
    substitutions = {
        "artifact_path": artifact_path,
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


def allowed_side_effects(step: dict[str, Any]) -> list[str]:
    values = step.get("allow_side_effects") if isinstance(step.get("allow_side_effects"), list) else []
    return unique_texts(values)


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
    return {
        "step_id": step_id,
        "argv": argv,
        "cwd": cwd,
        "artifact_path": artifact_path,
        "artifact_capture": capture_mode,
        "execution_policy": policy,
        "allow_side_effects": allowed_side_effects(step),
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
    artifact_path = resolved["artifact_path"]
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    policy = resolved["execution_policy"]
    timeout_seconds = float(policy.get("timeout_seconds") or 0.0)
    retry_budget = int(policy.get("retry_budget") or 0)
    retry_backoff_ms = int(policy.get("retry_backoff_ms") or 0)

    attempts: list[dict[str, Any]] = []
    final_stdout = ""
    final_stderr = ""
    for attempt_number in range(1, retry_budget + 2):
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
            retryable = attempt_number <= retry_budget
            attempts.append(
                {
                    "attempt_number": attempt_number,
                    "outcome": "timeout",
                    "retryable": retryable,
                    "timeout_seconds": timeout_seconds,
                    "stdout_hash": stable_hash(final_stdout),
                    "stderr_hash": stable_hash(final_stderr),
                }
            )
            if retryable:
                time.sleep(backoff_delay_seconds(retry_backoff_ms, attempt_number))
                continue
            raise RuntimeError(f"Detached fetch timed out after {timeout_seconds:.3f}s.") from exc

        final_stdout = completed.stdout
        final_stderr = completed.stderr
        if completed.returncode != 0:
            retryable = attempt_number <= retry_budget
            attempts.append(
                {
                    "attempt_number": attempt_number,
                    "outcome": "exit-nonzero",
                    "retryable": retryable,
                    "exit_code": completed.returncode,
                    "stdout_hash": stable_hash(completed.stdout),
                    "stderr_hash": stable_hash(completed.stderr),
                }
            )
            if retryable:
                time.sleep(backoff_delay_seconds(retry_backoff_ms, attempt_number))
                continue
            detail = completed.stderr.strip() or completed.stdout.strip() or f"exit={completed.returncode}"
            raise RuntimeError(f"Detached fetch command failed: {detail}")

        materialize_captured_artifact(
            artifact_path=artifact_path,
            capture_mode=resolved["artifact_capture"],
            stdout_text=completed.stdout,
        )
        attempts.append(
            {
                "attempt_number": attempt_number,
                "outcome": "completed",
                "retryable": False,
                "exit_code": completed.returncode,
                "stdout_hash": stable_hash(completed.stdout),
                "stderr_hash": stable_hash(completed.stderr),
            }
        )
        return artifact_path, {
            "command_snapshot": {
                "argv": resolved["argv"],
                "cwd": str(resolved["cwd"]),
            },
            "artifact_capture": resolved["artifact_capture"],
            "execution_policy": policy,
            "allow_side_effects": resolved["allow_side_effects"],
            "attempts": attempts,
            "attempt_count": len(attempts),
            "recovered_after_retry": len(attempts) > 1,
            "stdout_hash": stable_hash(completed.stdout),
            "stderr_hash": stable_hash(completed.stderr),
        }

    raise RuntimeError("Detached fetch failed without producing a terminal outcome.")


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
    "allowed_side_effects",
    "copy_import_artifact",
    "execute_detached_fetch_step",
    "fetch_cwd",
    "fetch_execution_policy",
    "render_fetch_argv",
    "resolved_artifact_path",
    "validate_detached_fetch_step",
]
