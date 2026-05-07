from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.core.paths import dead_letter_path, dead_letters_dir, ensure_runtime_dirs
from eco_council_runtime.kernel.operator.operations_common import (
    DEFAULT_DEAD_LETTER_SCHEMA,
    RUNBOOK_SECTIONS,
    maybe_text,
    stable_hash,
    utc_now_iso,
)

def classify_failure(failure: dict[str, Any]) -> dict[str, str]:
    error_code = maybe_text(failure.get("error_code"))
    if error_code in {
        "runtime-admission-blocked",
        "contract-preflight-blocked",
        "contract-postflight-blocked",
        "blocked-side-effect",
        "missing-runtime-approval",
        "side-effect-not-permitted",
        "sandbox-read-boundary-violation",
        "sandbox-write-boundary-violation",
        "sandbox-cwd-boundary-violation",
        "timeout-exceeds-admission-limit",
        "retry-budget-exceeds-admission-limit",
        "retry-backoff-exceeds-admission-limit",
        "skill-approval-consumption-failed",
    }:
        return {"failure_class": "admission", "runbook_section": RUNBOOK_SECTIONS["admission"]}
    if error_code in {"skill-timeout", "detached-fetch-timeout"}:
        return {"failure_class": "timeout", "runbook_section": RUNBOOK_SECTIONS["timeout"]}
    if error_code in {
        "skill-exit-nonzero",
        "detached-fetch-exit-nonzero",
        "detached-fetch-artifact-capture-failed",
    }:
        return {"failure_class": "subprocess", "runbook_section": RUNBOOK_SECTIONS["subprocess"]}
    if error_code in {
        "invalid-json-output",
        "non-object-payload",
        "detached-fetch-invalid-json-output",
        "detached-fetch-direct-file-missing",
        "detached-fetch-invalid-artifact-capture",
    }:
        return {"failure_class": "payload-contract", "runbook_section": RUNBOOK_SECTIONS["payload-contract"]}
    if error_code in {"controller-stage-failed", "archive-step-failed", "history-bootstrap-failed"}:
        return {"failure_class": "workflow", "runbook_section": RUNBOOK_SECTIONS["workflow"]}
    return {"failure_class": "unknown", "runbook_section": RUNBOOK_SECTIONS["unknown"]}


def operator_resolution_steps(failure_class: str, retryable: bool) -> list[str]:
    if failure_class == "admission":
        return [
            "Inspect `runtime/admission_policy.json` and compare the blocked side effect or path against the declared contract.",
            "If the operation is legitimate, re-materialize the admission policy or pass an explicit approval flag before retrying.",
            "Re-run `preflight-skill` or the affected runtime command to confirm the block is cleared.",
        ]
    if failure_class == "timeout":
        return [
            "Inspect the last attempt stdout/stderr hashes and confirm whether the step is actually slow or hanging.",
            "Increase timeout only if the step is expected to be long-running under the current policy boundary.",
            "Retry after confirming the upstream dependency is healthy.",
        ]
    if failure_class == "subprocess":
        return [
            "Inspect stderr/stdout details for the failing subprocess or skill script.",
            "If the failure is transient, rely on retry budget; otherwise fix the upstream command or input artifact.",
            "Re-run the affected runtime command after correcting the root cause.",
        ]
    if failure_class == "payload-contract":
        return [
            "Inspect the emitted payload and align it with the declared JSON contract and artifact refs.",
            "Fix the producing skill or detached fetch wrapper before retrying.",
            "Use strict preflight/postflight again to verify the contract is now satisfied.",
        ]
    if failure_class == "workflow":
        return [
            "Inspect the persisted controller/post-round artifact to locate the exact failed stage.",
            "Decide whether the round should resume, restart, or remain blocked based on the stored recovery hints.",
            "Use the operator command surfaced by `show-run-state` rather than editing artifacts manually.",
        ]
    return [
        "Inspect the runtime ledger, dead letter payload, and the latest persisted runtime artifact together.",
        "Classify whether the issue is admission, subprocess, payload, or workflow related before retrying.",
        "Only retry after the root cause and the affected boundary are both understood.",
    ]


def materialize_dead_letter(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    source_type: str,
    source_name: str,
    message: str,
    failure: dict[str, Any],
    summary: dict[str, Any] | None = None,
    related_paths: dict[str, Any] | None = None,
    command_hint: str = "",
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    failure_payload = failure if isinstance(failure, dict) else {}
    summary_payload = summary if isinstance(summary, dict) else {}
    related_payload = related_paths if isinstance(related_paths, dict) else {}
    classification = classify_failure(failure_payload)
    generated_at_utc = utc_now_iso()
    dead_letter_id = "deadletter-" + stable_hash(run_id, round_id, source_type, source_name, message, generated_at_utc)[:20]
    payload = {
        "schema_version": DEFAULT_DEAD_LETTER_SCHEMA,
        "generated_at_utc": generated_at_utc,
        "dead_letter_id": dead_letter_id,
        "resolution_status": "open",
        "run_id": run_id,
        "round_id": round_id,
        "source_type": source_type,
        "source_name": source_name,
        "message": maybe_text(message) or maybe_text(failure_payload.get("message")) or "Runtime operation failed.",
        "failure": failure_payload,
        "failure_class": classification["failure_class"],
        "runbook_section": classification["runbook_section"],
        "retryable": bool(failure_payload.get("retryable")),
        "command_hint": maybe_text(command_hint),
        "summary": summary_payload,
        "related_paths": related_payload,
        "operator_resolution_steps": operator_resolution_steps(classification["failure_class"], bool(failure_payload.get("retryable"))),
    }
    write_json(dead_letter_path(run_dir, dead_letter_id), payload)
    return payload


def load_dead_letters(run_dir: Path, *, round_id: str = "", limit: int = 20) -> list[dict[str, Any]]:
    path = dead_letters_dir(run_dir)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for file_path in path.glob("*.json"):
        payload = load_json_if_exists(file_path)
        if not payload:
            continue
        if round_id and maybe_text(payload.get("round_id")) != round_id:
            continue
        rows.append(payload)
    rows.sort(key=lambda item: (maybe_text(item.get("generated_at_utc")), maybe_text(item.get("dead_letter_id"))), reverse=True)
    return rows[: max(1, limit)]


__all__ = (
    "classify_failure",
    "operator_resolution_steps",
    "materialize_dead_letter",
    "load_dead_letters",
)
