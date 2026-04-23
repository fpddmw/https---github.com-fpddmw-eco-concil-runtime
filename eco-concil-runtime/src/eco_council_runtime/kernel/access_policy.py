from __future__ import annotations

from typing import Any

from .role_contracts import (
    ROLE_RUNTIME_OPERATOR,
    known_actor_role,
    normalize_actor_role,
    preferred_role_label,
    role_capabilities,
    role_contract,
)
from .skill_registry import (
    default_actor_role_hint,
    resolve_skill_policy,
    skill_requires_write_actor_role,
)

WRITE_KERNEL_COMMAND_POLICIES = {
    "init-run": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "preflight-skill": {
        "allowed_roles": [],
        "default_actor_role_hint": "<actor_role>",
    },
    "run-skill": {
        "allowed_roles": [],
        "default_actor_role_hint": "<actor_role>",
    },
    "request-phase-transition": {
        "allowed_roles": ["moderator"],
        "default_actor_role_hint": "moderator",
    },
    "approve-phase-transition": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "reject-phase-transition": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "apply-promotion-gate": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "run-phase2-round": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "resume-phase2-round": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "restart-phase2-round": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "close-round": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "bootstrap-history-context": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-scenario-fixture": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-benchmark-manifest": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "compare-benchmark-manifests": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "replay-runtime-scenario": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "supervise-round": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-admission-policy": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-runtime-health": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-operator-runbook": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-agent-entry-gate": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-phase2-exports": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
    "materialize-reporting-exports": {
        "allowed_roles": [ROLE_RUNTIME_OPERATOR],
        "default_actor_role_hint": ROLE_RUNTIME_OPERATOR,
    },
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


def issue(
    code: str,
    message: str,
    *,
    severity: str = "error",
    blocking: bool = True,
    field: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": code,
        "severity": severity,
        "blocking": blocking,
        "message": message,
    }
    if field:
        payload["field"] = field
    return payload


def resolve_execution_actor_role(
    skill_name: str,
    actor_role: Any = "",
    *,
    fallback_role_hint: Any = "",
) -> dict[str, str]:
    raw_actor_role = maybe_text(actor_role)
    fallback = maybe_text(fallback_role_hint)
    effective_actor_role = raw_actor_role or fallback or default_actor_role_hint(skill_name)
    return {
        "actor_role": raw_actor_role,
        "effective_actor_role": effective_actor_role,
        "resolved_actor_role": normalize_actor_role(effective_actor_role),
    }


def evaluate_skill_access(
    skill_name: str,
    *,
    actor_role: Any,
    contract_mode: str = "warn",
    fallback_role_hint: Any = "",
) -> dict[str, Any]:
    skill_policy = resolve_skill_policy(skill_name)
    actor = resolve_execution_actor_role(
        skill_name,
        actor_role,
        fallback_role_hint=fallback_role_hint,
    )
    raw_actor_role = maybe_text(actor.get("effective_actor_role"))
    resolved_actor_role = maybe_text(actor.get("resolved_actor_role"))
    issues: list[dict[str, Any]] = []

    if not raw_actor_role:
        issues.append(
            issue(
                "missing-actor-role",
                f"Skill {skill_name} requires an explicit actor role for governed execution.",
                field="actor_role",
            )
        )
    elif not known_actor_role(raw_actor_role):
        issues.append(
            issue(
                "unknown-actor-role",
                f"Actor role `{raw_actor_role}` is not registered in runtime role contracts.",
                field="actor_role",
            )
        )
    else:
        allowed_roles = (
            skill_policy.get("allowed_roles", [])
            if isinstance(skill_policy.get("allowed_roles"), list)
            else []
        )
        denied_roles = (
            skill_policy.get("denied_roles", [])
            if isinstance(skill_policy.get("denied_roles"), list)
            else []
        )
        if resolved_actor_role in denied_roles:
            issues.append(
                issue(
                    "actor-role-explicitly-denied",
                    f"Actor role `{resolved_actor_role}` is explicitly denied for {skill_name}.",
                    field="actor_role",
                )
            )
        elif allowed_roles and resolved_actor_role not in allowed_roles:
            issues.append(
                issue(
                    "actor-role-not-allowed",
                    f"Actor role `{resolved_actor_role}` cannot execute {skill_name}. Allowed roles: {', '.join(allowed_roles)}.",
                    field="actor_role",
                )
            )
        missing_capabilities = [
            capability
            for capability in skill_policy.get("required_capabilities", [])
            if capability not in role_capabilities(resolved_actor_role)
        ]
        if missing_capabilities:
            issues.append(
                issue(
                    "missing-role-capabilities",
                    f"Actor role `{resolved_actor_role}` is missing required capabilities for {skill_name}: {', '.join(missing_capabilities)}.",
                    field="actor_role",
                )
            )
        if bool(skill_policy.get("requires_operator_approval")) and resolved_actor_role != ROLE_RUNTIME_OPERATOR:
            strict_block = contract_mode == "strict"
            issues.append(
                issue(
                    "operator-approval-required",
                    f"Skill {skill_name} is marked as operator-audited and should not run without explicit runtime-operator approval.",
                    severity="error" if strict_block else "warning",
                    blocking=strict_block,
                )
            )

    blocking_issue_count = len([item for item in issues if bool(item.get("blocking"))])
    return {
        "schema_version": "runtime-skill-access-v1",
        "skill_name": skill_name,
        "actor_role": maybe_text(actor_role),
        "effective_actor_role": raw_actor_role,
        "resolved_actor_role": resolved_actor_role,
        "actor_role_display": preferred_role_label(raw_actor_role),
        "skill_policy": skill_policy,
        "role_contract": role_contract(resolved_actor_role),
        "issues": issues,
        "issue_count": len(issues),
        "blocking_issue_count": blocking_issue_count,
        "block_execution": blocking_issue_count > 0,
    }


def command_requires_explicit_actor_role(command_name: str) -> bool:
    return maybe_text(command_name) in WRITE_KERNEL_COMMAND_POLICIES


def kernel_command_actor_role_hint(command_name: str) -> str:
    command_policy = WRITE_KERNEL_COMMAND_POLICIES.get(maybe_text(command_name), {})
    allowed_roles = (
        command_policy.get("allowed_roles", [])
        if isinstance(command_policy.get("allowed_roles"), list)
        else []
    )
    explicit = maybe_text(command_policy.get("default_actor_role_hint"))
    if explicit:
        return explicit
    if len(allowed_roles) == 1:
        return maybe_text(allowed_roles[0])
    return "<actor_role>"


def evaluate_kernel_command_access(command_name: str, *, actor_role: Any) -> dict[str, Any]:
    normalized_command = maybe_text(command_name)
    actor_text = maybe_text(actor_role)
    resolved_actor_role = normalize_actor_role(actor_text)
    issues: list[dict[str, Any]] = []
    command_policy = WRITE_KERNEL_COMMAND_POLICIES.get(normalized_command, {})
    allowed_roles = (
        command_policy.get("allowed_roles", [])
        if isinstance(command_policy.get("allowed_roles"), list)
        else []
    )
    if not normalized_command or not command_requires_explicit_actor_role(normalized_command):
        return {
            "command_name": normalized_command,
            "actor_role": actor_text,
            "resolved_actor_role": resolved_actor_role,
            "allowed_roles": allowed_roles,
            "issues": [],
            "blocking_issue_count": 0,
            "block_execution": False,
        }
    if not actor_text:
        issues.append(
            issue(
                "missing-actor-role",
                f"Kernel command `{normalized_command}` requires an explicit `--actor-role`.",
                field="actor_role",
            )
        )
    elif not known_actor_role(actor_text):
        issues.append(
            issue(
                "unknown-actor-role",
                f"Actor role `{actor_text}` is not registered in runtime role contracts.",
                field="actor_role",
            )
        )
    elif allowed_roles and resolved_actor_role not in allowed_roles:
        issues.append(
            issue(
                "actor-role-not-allowed",
                f"Actor role `{resolved_actor_role}` cannot execute kernel command `{normalized_command}`. Allowed roles: {', '.join(allowed_roles)}.",
                field="actor_role",
            )
        )
    blocking_issue_count = len([item for item in issues if bool(item.get("blocking"))])
    return {
        "command_name": normalized_command,
        "actor_role": actor_text,
        "resolved_actor_role": resolved_actor_role,
        "allowed_roles": unique_texts(allowed_roles),
        "issues": issues,
        "blocking_issue_count": blocking_issue_count,
        "block_execution": blocking_issue_count > 0,
    }


__all__ = [
    "WRITE_KERNEL_COMMAND_POLICIES",
    "command_requires_explicit_actor_role",
    "evaluate_kernel_command_access",
    "evaluate_skill_access",
    "kernel_command_actor_role_hint",
    "resolve_execution_actor_role",
    "skill_requires_write_actor_role",
]
