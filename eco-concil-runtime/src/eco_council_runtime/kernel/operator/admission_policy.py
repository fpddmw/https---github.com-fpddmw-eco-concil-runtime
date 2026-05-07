from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.core.paths import admission_policy_path, ensure_runtime_dirs
from eco_council_runtime.kernel.core.registry import workspace_root
from eco_council_runtime.kernel.operator.operations_common import (
    ALWAYS_ALLOWED_SIDE_EFFECTS,
    DEFAULT_ADMISSION_POLICY_SCHEMA,
    PERMISSION_PROFILES,
    maybe_text,
    unique_texts,
    utc_now_iso,
)

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


__all__ = (
    "policy_roots_template",
    "side_effect_profile",
    "canonical_side_effect_policy",
    "policy_root_entries",
    "default_admission_policy",
    "materialize_admission_policy",
    "load_admission_policy",
    "issue",
    "resolve_policy_root",
    "path_within_roots",
    "side_effect_risk_level",
    "sandbox_profile",
    "evaluate_execution_admission",
    "admission_error_code",
)
