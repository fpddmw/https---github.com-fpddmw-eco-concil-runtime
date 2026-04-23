from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .ledger import load_ledger_tail
from .manifest import load_json_if_exists, write_json
from .paths import (
    admission_policy_path,
    dead_letter_path,
    dead_letters_dir,
    ensure_runtime_dirs,
    manifest_path,
    operator_runbook_path,
    runtime_health_path,
)
from .registry import workspace_root

PERMISSION_PROFILES = ("standard", "restricted", "network-enabled")
DEFAULT_ADMISSION_POLICY_SCHEMA = "runtime-admission-policy-v1"
DEFAULT_DEAD_LETTER_SCHEMA = "runtime-dead-letter-v1"
DEFAULT_HEALTH_SCHEMA = "runtime-health-v1"
ALWAYS_ALLOWED_SIDE_EFFECTS = {"reads-artifacts", "writes-artifacts"}
RUNBOOK_SECTIONS = {
    "admission": "Admission Blocks",
    "timeout": "Timeout Recovery",
    "subprocess": "Subprocess Failures",
    "payload-contract": "Payload Contract Failures",
    "workflow": "Workflow Stage Failures",
    "unknown": "Unknown Failures",
}


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def stable_hash(*parts: Any) -> str:
    import hashlib

    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def policy_roots_template() -> dict[str, list[str]]:
    return {
        "allowed_read_roots": ["<run_dir>", "<run_parent>/archives", "<workspace_root>"],
        "allowed_write_roots": ["<run_dir>", "<run_parent>/archives"],
        "allowed_cwd_roots": ["<workspace_root>", "<run_dir>"],
    }


def side_effect_profile(permission_profile: str) -> dict[str, list[str]]:
    if permission_profile == "restricted":
        return {
            "default_allow": ["reads-artifacts", "writes-artifacts"],
            "approval_required": ["reads-shared-state", "writes-shared-state", "network-external"],
            "blocked": ["destructive-write"],
        }
    if permission_profile == "network-enabled":
        return {
            "default_allow": ["reads-artifacts", "writes-artifacts", "reads-shared-state", "writes-shared-state", "network-external"],
            "approval_required": [],
            "blocked": ["destructive-write"],
        }
    return {
        "default_allow": ["reads-artifacts", "writes-artifacts", "reads-shared-state", "writes-shared-state"],
        "approval_required": ["network-external"],
        "blocked": ["destructive-write"],
    }


def canonical_side_effect_policy(
    *,
    default_allow: list[str],
    approval_required: list[str],
    blocked: list[str],
) -> dict[str, list[str]]:
    blocked_values = unique_texts(blocked)
    blocked_set = set(blocked_values)
    approval_values = [item for item in unique_texts(approval_required) if item not in blocked_set]
    approval_set = set(approval_values)
    default_allow_values = [
        item
        for item in unique_texts([*ALWAYS_ALLOWED_SIDE_EFFECTS, *default_allow])
        if item not in blocked_set and item not in approval_set
    ]
    return {
        "default_allow": default_allow_values,
        "approval_required": approval_values,
        "blocked": blocked_values,
    }


def policy_root_entries(values: list[str], defaults: list[str]) -> list[str]:
    if values:
        return unique_texts(values)
    return unique_texts(defaults)


def default_admission_policy(
    run_dir: Path,
    *,
    run_id: str = "",
    permission_profile: str = "standard",
    workspace: Path | None = None,
    max_timeout_seconds: float | None = None,
    max_retry_budget: int | None = None,
    max_retry_backoff_ms: int | None = None,
    default_allow_side_effects: list[str] | None = None,
    approval_required_side_effects: list[str] | None = None,
    blocked_side_effects: list[str] | None = None,
    allowed_read_roots: list[str] | None = None,
    allowed_write_roots: list[str] | None = None,
    allowed_cwd_roots: list[str] | None = None,
) -> dict[str, Any]:
    if permission_profile not in PERMISSION_PROFILES:
        raise ValueError(f"Unsupported permission_profile: {permission_profile}")
    root = (workspace or workspace_root()).resolve()
    root_policy = policy_roots_template()
    profile = side_effect_profile(permission_profile)
    policy = canonical_side_effect_policy(
        default_allow=[*(profile.get("default_allow", [])), *(default_allow_side_effects or [])],
        approval_required=[*(profile.get("approval_required", [])), *(approval_required_side_effects or [])],
        blocked=[*(profile.get("blocked", [])), *(blocked_side_effects or [])],
    )
    return {
        "schema_version": DEFAULT_ADMISSION_POLICY_SCHEMA,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "permission_profile": permission_profile,
        "approval_authority": "runtime-operator",
        "enforcement_mode": "enforce",
        "sandbox_boundary": {
            "allowed_read_roots": policy_root_entries(allowed_read_roots or [], root_policy["allowed_read_roots"]),
            "allowed_write_roots": policy_root_entries(allowed_write_roots or [], root_policy["allowed_write_roots"]),
            "allowed_cwd_roots": policy_root_entries(allowed_cwd_roots or [], root_policy["allowed_cwd_roots"]),
        },
        "side_effect_policy": policy,
        "execution_limits": {
            "max_timeout_seconds": max(0.0, float(max_timeout_seconds if max_timeout_seconds is not None else 900.0)),
            "max_retry_budget": max(0, int(max_retry_budget if max_retry_budget is not None else 3)),
            "max_retry_backoff_ms": max(0, int(max_retry_backoff_ms if max_retry_backoff_ms is not None else 5000)),
        },
        "dead_letter_policy": {
            "enabled": True,
            "retain_last": 200,
        },
        "rollback_policy": {
            "mode": "operator-mediated",
            "auto_rollback": False,
            "retry_before_dead_letter": True,
        },
        "alert_policy": {
            "failed_event_threshold": 1,
            "blocked_event_threshold": 1,
            "degraded_event_threshold": 1,
            "dead_letter_threshold": 1,
        },
        "operator_policy": {
            "owner_role": "runtime-operator",
            "runbook_required": True,
            "health_surface_required": True,
        },
    }


def materialize_admission_policy(
    run_dir: Path,
    *,
    run_id: str = "",
    permission_profile: str = "standard",
    workspace: Path | None = None,
    max_timeout_seconds: float | None = None,
    max_retry_budget: int | None = None,
    max_retry_backoff_ms: int | None = None,
    default_allow_side_effects: list[str] | None = None,
    approval_required_side_effects: list[str] | None = None,
    blocked_side_effects: list[str] | None = None,
    allowed_read_roots: list[str] | None = None,
    allowed_write_roots: list[str] | None = None,
    allowed_cwd_roots: list[str] | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    payload = default_admission_policy(
        run_dir,
        run_id=run_id,
        permission_profile=permission_profile,
        workspace=workspace,
        max_timeout_seconds=max_timeout_seconds,
        max_retry_budget=max_retry_budget,
        max_retry_backoff_ms=max_retry_backoff_ms,
        default_allow_side_effects=default_allow_side_effects,
        approval_required_side_effects=approval_required_side_effects,
        blocked_side_effects=blocked_side_effects,
        allowed_read_roots=allowed_read_roots,
        allowed_write_roots=allowed_write_roots,
        allowed_cwd_roots=allowed_cwd_roots,
    )
    write_json(admission_policy_path(run_dir), payload)
    return payload


def load_admission_policy(run_dir: Path, workspace: Path | None = None) -> dict[str, Any]:
    payload = load_json_if_exists(admission_policy_path(run_dir))
    if payload:
        return payload
    return default_admission_policy(run_dir, workspace=workspace)


def issue(code: str, message: str, *, severity: str = "error", blocking: bool = True, field: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": code,
        "severity": severity,
        "blocking": blocking,
        "message": message,
    }
    if field:
        payload["field"] = field
    return payload


def resolve_policy_root(path_text: str, run_dir: Path, root: Path) -> Path:
    text = maybe_text(path_text)
    if text.startswith("<run_dir>"):
        suffix = text.removeprefix("<run_dir>").lstrip("/")
        return ((run_dir / suffix) if suffix else run_dir).resolve()
    if text.startswith("<run_parent>"):
        suffix = text.removeprefix("<run_parent>").lstrip("/")
        return ((run_dir.parent / suffix) if suffix else run_dir.parent).resolve()
    if text.startswith("<workspace_root>"):
        suffix = text.removeprefix("<workspace_root>").lstrip("/")
        return ((root / suffix) if suffix else root).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def path_within_roots(candidate_path: str, roots: list[Path]) -> bool:
    candidate = Path(candidate_path).expanduser().resolve()
    for root in roots:
        try:
            candidate.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def side_effect_risk_level(side_effects: list[str]) -> str:
    values = set(side_effects)
    if "destructive-write" in values:
        return "critical"
    if "network-external" in values:
        return "high"
    if "writes-shared-state" in values:
        return "high"
    if "writes-artifacts" in values:
        return "medium"
    if "reads-shared-state" in values:
        return "medium"
    return "low"


def sandbox_profile(side_effects: list[str], write_paths: list[str]) -> str:
    values = set(side_effects)
    if "destructive-write" in values:
        return "destructive-blocked"
    if "network-external" in values:
        return "networked-execution"
    if "writes-shared-state" in values:
        return "shared-state-write"
    if values & {"writes-artifacts"} or write_paths:
        return "run-dir-write"
    return "read-only"


def evaluate_execution_admission(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    actor_kind: str,
    actor_name: str,
    declared_side_effects: list[Any],
    requested_side_effect_approvals: list[Any],
    execution_policy: dict[str, Any],
    resolved_read_paths: list[Any],
    resolved_write_paths: list[Any],
    cwd_path: str = "",
    workspace: Path | None = None,
) -> dict[str, Any]:
    root = (workspace or workspace_root()).resolve()
    policy = load_admission_policy(run_dir, workspace=root)
    side_effect_policy = policy.get("side_effect_policy", {}) if isinstance(policy.get("side_effect_policy"), dict) else {}
    limits = policy.get("execution_limits", {}) if isinstance(policy.get("execution_limits"), dict) else {}
    sandbox = policy.get("sandbox_boundary", {}) if isinstance(policy.get("sandbox_boundary"), dict) else {}
    read_roots = [resolve_policy_root(item, run_dir, root) for item in sandbox.get("allowed_read_roots", []) if maybe_text(item)]
    write_roots = [resolve_policy_root(item, run_dir, root) for item in sandbox.get("allowed_write_roots", []) if maybe_text(item)]
    cwd_roots = [resolve_policy_root(item, run_dir, root) for item in sandbox.get("allowed_cwd_roots", []) if maybe_text(item)]
    declared_effects = unique_texts([maybe_text(item) for item in declared_side_effects if maybe_text(item)])
    requested_approvals = unique_texts([maybe_text(item) for item in requested_side_effect_approvals if maybe_text(item)])
    default_allow = set(unique_texts(side_effect_policy.get("default_allow", []) if isinstance(side_effect_policy.get("default_allow"), list) else []))
    approval_required = set(unique_texts(side_effect_policy.get("approval_required", []) if isinstance(side_effect_policy.get("approval_required"), list) else []))
    blocked = set(unique_texts(side_effect_policy.get("blocked", []) if isinstance(side_effect_policy.get("blocked"), list) else []))
    issues: list[dict[str, Any]] = []

    for side_effect in declared_effects:
        if side_effect in blocked:
            issues.append(
                issue(
                    "blocked-side-effect",
                    f"{actor_kind} `{actor_name}` declares blocked side effect `{side_effect}` under permission profile `{policy.get('permission_profile')}`.",
                    field=side_effect,
                )
            )
            continue
        if side_effect in approval_required and side_effect not in requested_approvals:
            issues.append(
                issue(
                    "missing-runtime-approval",
                    f"{actor_kind} `{actor_name}` requires explicit runtime approval for side effect `{side_effect}` under permission profile `{policy.get('permission_profile')}`.",
                    field=side_effect,
                )
            )
            continue
        if side_effect not in default_allow and side_effect not in requested_approvals:
            issues.append(
                issue(
                    "side-effect-not-permitted",
                    f"{actor_kind} `{actor_name}` declares side effect `{side_effect}` that is not permitted by the current admission policy.",
                    field=side_effect,
                )
            )

    max_timeout_seconds = float(limits.get("max_timeout_seconds") or 0.0)
    max_retry_budget = int(limits.get("max_retry_budget") or 0)
    max_retry_backoff_ms = int(limits.get("max_retry_backoff_ms") or 0)
    timeout_seconds = float(execution_policy.get("timeout_seconds") or 0.0)
    retry_budget = int(execution_policy.get("retry_budget") or 0)
    retry_backoff_ms = int(execution_policy.get("retry_backoff_ms") or 0)
    if max_timeout_seconds and timeout_seconds > max_timeout_seconds:
        issues.append(
            issue(
                "timeout-exceeds-admission-limit",
                f"{actor_kind} `{actor_name}` requests timeout_seconds={timeout_seconds:.3f}, exceeding policy max {max_timeout_seconds:.3f}.",
                field="timeout_seconds",
            )
        )
    if retry_budget > max_retry_budget:
        issues.append(
            issue(
                "retry-budget-exceeds-admission-limit",
                f"{actor_kind} `{actor_name}` requests retry_budget={retry_budget}, exceeding policy max {max_retry_budget}.",
                field="retry_budget",
            )
        )
    if retry_backoff_ms > max_retry_backoff_ms:
        issues.append(
            issue(
                "retry-backoff-exceeds-admission-limit",
                f"{actor_kind} `{actor_name}` requests retry_backoff_ms={retry_backoff_ms}, exceeding policy max {max_retry_backoff_ms}.",
                field="retry_backoff_ms",
            )
        )

    read_paths = [maybe_text(item) for item in resolved_read_paths if maybe_text(item)]
    for candidate_path in read_paths:
        if read_roots and not path_within_roots(candidate_path, read_roots):
            issues.append(
                issue(
                    "sandbox-read-boundary-violation",
                    f"{actor_kind} `{actor_name}` reads `{candidate_path}`, which is outside allowed read roots.",
                    field=candidate_path,
                )
            )
    write_paths = [maybe_text(item) for item in resolved_write_paths if maybe_text(item)]
    for candidate_path in write_paths:
        if write_roots and not path_within_roots(candidate_path, write_roots):
            issues.append(
                issue(
                    "sandbox-write-boundary-violation",
                    f"{actor_kind} `{actor_name}` writes `{candidate_path}`, which is outside allowed write roots.",
                    field=candidate_path,
                )
            )
    cwd_value = maybe_text(cwd_path)
    if cwd_value and cwd_roots and not path_within_roots(cwd_value, cwd_roots):
        issues.append(
            issue(
                "sandbox-cwd-boundary-violation",
                f"{actor_kind} `{actor_name}` executes with cwd `{cwd_value}`, which is outside allowed cwd roots.",
                field="cwd_path",
            )
        )

    blocking_issue_count = len([item for item in issues if bool(item.get("blocking")) and maybe_text(item.get("severity")) == "error"])
    admitted = blocking_issue_count == 0
    return {
        "schema_version": "runtime-admission-decision-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "actor_kind": actor_kind,
        "actor_name": actor_name,
        "permission_profile": maybe_text(policy.get("permission_profile")) or "standard",
        "approval_authority": maybe_text(policy.get("approval_authority")) or "runtime-operator",
        "enforcement_mode": maybe_text(policy.get("enforcement_mode")) or "enforce",
        "sandbox_profile": sandbox_profile(declared_effects, write_paths),
        "risk_level": side_effect_risk_level(declared_effects),
        "declared_side_effects": declared_effects,
        "requested_side_effect_approvals": requested_approvals,
        "execution_policy": {
            "timeout_seconds": timeout_seconds,
            "retry_budget": retry_budget,
            "retry_backoff_ms": retry_backoff_ms,
        },
        "execution_limits": {
            "max_timeout_seconds": max_timeout_seconds,
            "max_retry_budget": max_retry_budget,
            "max_retry_backoff_ms": max_retry_backoff_ms,
        },
        "resolved_read_paths": read_paths,
        "resolved_write_paths": write_paths,
        "cwd_path": cwd_value,
        "allowed_read_roots": [str(item) for item in read_roots],
        "allowed_write_roots": [str(item) for item in write_roots],
        "allowed_cwd_roots": [str(item) for item in cwd_roots],
        "issues": issues,
        "issue_count": len(issues),
        "blocking_issue_count": blocking_issue_count,
        "admit_execution": admitted,
        "block_execution": not admitted,
        "admission_status": "admitted" if admitted else "blocked",
        "operator_summary": (
            f"{actor_kind} `{actor_name}` admitted with sandbox profile `{sandbox_profile(declared_effects, write_paths)}`."
            if admitted
            else f"{actor_kind} `{actor_name}` blocked by runtime admission policy."
        ),
        "policy_path": str(admission_policy_path(run_dir).resolve()),
    }


def admission_error_code(admission: dict[str, Any]) -> str:
    issues = admission.get("issues", []) if isinstance(admission.get("issues"), list) else []
    for entry in issues:
        if not isinstance(entry, dict):
            continue
        if bool(entry.get("blocking")) and maybe_text(entry.get("severity")) == "error" and maybe_text(entry.get("code")):
            return maybe_text(entry.get("code"))
    return "runtime-admission-blocked"


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


def runtime_health_payload(run_dir: Path, *, round_id: str = "") -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    policy = load_admission_policy(run_dir)
    alert_policy = policy.get("alert_policy", {}) if isinstance(policy.get("alert_policy"), dict) else {}
    events = load_ledger_tail(run_dir, 1_000_000)
    filtered_events = [
        event for event in events if isinstance(event, dict) and (not round_id or maybe_text(event.get("round_id")) == round_id)
    ]
    failed_events = [event for event in filtered_events if maybe_text(event.get("status")) == "failed"]
    blocked_events = [event for event in filtered_events if maybe_text(event.get("status")) == "blocked"]
    degraded_events = [
        event for event in filtered_events if maybe_text(event.get("status")) in {"completed-with-warnings", "degraded"}
    ]
    recovered_events = [event for event in filtered_events if bool(event.get("recovered_after_retry"))]
    dead_letters = load_dead_letters(run_dir, round_id=round_id, limit=50)
    open_dead_letters = [item for item in dead_letters if maybe_text(item.get("resolution_status")) != "closed"]
    alerts: list[dict[str, Any]] = []
    failed_threshold = int(alert_policy.get("failed_event_threshold") or 1)
    blocked_threshold = int(alert_policy.get("blocked_event_threshold") or 1)
    degraded_threshold = int(alert_policy.get("degraded_event_threshold") or 1)
    dead_letter_threshold = int(alert_policy.get("dead_letter_threshold") or 1)
    if len(failed_events) >= failed_threshold:
        alerts.append(
            {
                "severity": "critical",
                "code": "failed-events-present",
                "message": f"{len(failed_events)} failed runtime events are present.",
            }
        )
    if len(blocked_events) >= blocked_threshold:
        alerts.append(
            {
                "severity": "critical",
                "code": "blocked-events-present",
                "message": f"{len(blocked_events)} blocked runtime events are present.",
            }
        )
    if len(open_dead_letters) >= dead_letter_threshold:
        alerts.append(
            {
                "severity": "critical",
                "code": "open-dead-letters-present",
                "message": f"{len(open_dead_letters)} dead letters still require operator review.",
            }
        )
    if len(degraded_events) >= degraded_threshold:
        alerts.append(
            {
                "severity": "warning",
                "code": "degraded-events-present",
                "message": f"{len(degraded_events)} degraded runtime events are present.",
            }
        )
    if recovered_events:
        alerts.append(
            {
                "severity": "warning",
                "code": "retry-recoveries-observed",
                "message": f"{len(recovered_events)} runtime events only completed after retry.",
            }
        )
    alert_status = "green"
    if any(maybe_text(item.get("severity")) == "critical" for item in alerts):
        alert_status = "red"
    elif alerts:
        alert_status = "yellow"
    return {
        "schema_version": DEFAULT_HEALTH_SCHEMA,
        "generated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "permission_profile": maybe_text(policy.get("permission_profile")) or "standard",
        "alert_status": alert_status,
        "summary": {
            "event_count": len(filtered_events),
            "failed_event_count": len(failed_events),
            "blocked_event_count": len(blocked_events),
            "degraded_event_count": len(degraded_events),
            "recovered_after_retry_count": len(recovered_events),
            "open_dead_letter_count": len(open_dead_letters),
        },
        "alerts": alerts,
        "latest_failed_events": [
            {
                "event_type": maybe_text(item.get("event_type")),
                "skill_name": maybe_text(item.get("skill_name")),
                "failed_stage": maybe_text(item.get("failed_stage")),
                "status": maybe_text(item.get("status")),
            }
            for item in failed_events[-5:]
        ],
        "latest_blocked_events": [
            {
                "event_type": maybe_text(item.get("event_type")),
                "skill_name": maybe_text(item.get("skill_name")),
                "status": maybe_text(item.get("status")),
            }
            for item in blocked_events[-5:]
        ],
        "open_dead_letters": open_dead_letters[:10],
    }


def materialize_runtime_health(run_dir: Path, *, round_id: str = "") -> dict[str, Any]:
    payload = runtime_health_payload(run_dir, round_id=round_id)
    write_json(runtime_health_path(run_dir), payload)
    return payload


def refresh_runtime_surfaces(run_dir: Path, *, round_id: str = "") -> dict[str, Any]:
    health = materialize_runtime_health(run_dir)
    runbook_path = materialize_operator_runbook(run_dir, round_id=round_id)
    return {
        "runtime_health_path": str(runtime_health_path(run_dir).resolve()),
        "runtime_health": health,
        "operator_runbook_path": runbook_path,
    }


def operator_runbook_markdown(run_dir: Path, *, round_id: str = "") -> str:
    from ..runtime_command_hints import kernel_command, run_skill_command

    policy = load_admission_policy(run_dir)
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    health = runtime_health_payload(run_dir, round_id=round_id)
    dead_letters = health.get("open_dead_letters", []) if isinstance(health.get("open_dead_letters"), list) else []
    rollback_policy = policy.get("rollback_policy", {}) if isinstance(policy.get("rollback_policy"), dict) else {}
    run_id = maybe_text(manifest.get("run_id"))
    lines = [
        "# Runtime Operator Runbook",
        "",
        "## Control Plane",
        "",
        f"- Permission profile: `{maybe_text(policy.get('permission_profile')) or 'standard'}`",
        f"- Approval authority: `{maybe_text(policy.get('approval_authority')) or 'runtime-operator'}`",
        f"- Rollback mode: `{maybe_text(rollback_policy.get('mode')) or 'operator-mediated'}`",
        f"- Alert status: `{maybe_text(health.get('alert_status')) or 'green'}`",
        f"- Failed events: `{int(health.get('summary', {}).get('failed_event_count') or 0)}`",
        f"- Blocked events: `{int(health.get('summary', {}).get('blocked_event_count') or 0)}`",
        f"- Open dead letters: `{int(health.get('summary', {}).get('open_dead_letter_count') or 0)}`",
        "",
        "## Standard Commands",
        "",
        f"- Inspect runtime state: `{kernel_command('show-run-state', '--run-dir', str(run_dir), *(['--round-id', round_id] if round_id else []), '--tail', '20')}`",
        f"- Refresh health surface: `{kernel_command('materialize-runtime-health', '--run-dir', str(run_dir), *(['--round-id', round_id] if round_id else []))}`",
        f"- Rebuild runbook: `{kernel_command('materialize-operator-runbook', '--run-dir', str(run_dir), *(['--round-id', round_id] if round_id else []))}`",
        "",
    ]
    if run_id and round_id:
        lines.extend(
            [
                "## Agent Entry",
                "",
                f"- Materialize agent entry gate: `{kernel_command('materialize-agent-entry-gate', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id)}`",
                f"- Refresh agent advisory plan through runtime governance: `{run_skill_command(run_dir=run_dir, run_id=run_id, round_id=round_id, skill_name='eco-plan-round-orchestration', contract_mode='warn', actor_role='moderator', skill_args=['--planner-mode', 'agent-advisory', '--output-path', f'runtime/agent_advisory_plan_{round_id}.json'])}`",
                "",
            ]
        )
    lines.extend(
        [
        "## Failure Classes",
        "",
        ]
    )
    for title in RUNBOOK_SECTIONS.values():
        steps = operator_resolution_steps(
            next(key for key, value in RUNBOOK_SECTIONS.items() if value == title),
            False,
        )
        lines.append(f"### {title}")
        lines.append("")
        for step in steps:
            lines.append(f"1. {step}")
        lines.append("")
    lines.extend(["## Current Open Dead Letters", ""])
    if not dead_letters:
        lines.append("No open dead letters are currently present.")
        lines.append("")
        return "\n".join(lines)
    for payload in dead_letters:
        lines.append(f"### {maybe_text(payload.get('dead_letter_id'))}")
        lines.append("")
        lines.append(f"- Source: `{maybe_text(payload.get('source_type'))}:{maybe_text(payload.get('source_name'))}`")
        lines.append(f"- Failure class: `{maybe_text(payload.get('failure_class'))}`")
        lines.append(f"- Message: {maybe_text(payload.get('message'))}")
        if maybe_text(payload.get("command_hint")):
            lines.append(f"- Suggested command: `{maybe_text(payload.get('command_hint'))}`")
        lines.append("")
        for step in payload.get("operator_resolution_steps", []) if isinstance(payload.get("operator_resolution_steps"), list) else []:
            lines.append(f"1. {maybe_text(step)}")
        lines.append("")
    return "\n".join(lines)


def materialize_operator_runbook(run_dir: Path, *, round_id: str = "") -> str:
    ensure_runtime_dirs(run_dir)
    content = operator_runbook_markdown(run_dir, round_id=round_id)
    path = operator_runbook_path(run_dir, round_id)
    path.write_text(content + "\n", encoding="utf-8")
    return str(path)


__all__ = [
    "PERMISSION_PROFILES",
    "admission_error_code",
    "evaluate_execution_admission",
    "load_admission_policy",
    "load_dead_letters",
    "materialize_admission_policy",
    "materialize_dead_letter",
    "materialize_operator_runbook",
    "materialize_runtime_health",
    "refresh_runtime_surfaces",
    "runtime_health_payload",
]
