from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.governance.access_policy import evaluate_skill_access
from eco_council_runtime.kernel.planes.deliberation_plane import (
    connect_db as connect_deliberation_db,
    maybe_text,
)
from eco_council_runtime.kernel.governance.role_contracts import (
    ROLE_MODERATOR,
    ROLE_RUNTIME_OPERATOR,
    known_actor_role,
    normalize_actor_role,
)
from eco_council_runtime.kernel.governance.skill_registry import (
    SKILL_LAYER_OPTIONAL_ANALYSIS,
    SKILL_LAYER_REPORTING,
    resolve_skill_policy,
)

OBJECT_KIND_SKILL_APPROVAL_REQUEST = "skill-approval-request"
OBJECT_KIND_SKILL_APPROVAL = "skill-approval"
OBJECT_KIND_SKILL_APPROVAL_REJECTION = "skill-approval-rejection"
OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION = "skill-approval-consumption"

REQUEST_STATUS_PENDING = "pending-operator-confirmation"
REQUEST_STATUS_APPROVED = "approved"
REQUEST_STATUS_REJECTED = "rejected"
REQUEST_STATUS_CONSUMED = "consumed"

DECISION_STATUS_APPROVED = "approved"
DECISION_STATUS_REJECTED = "rejected"
CONSUMPTION_STATUS_CONSUMED = "consumed"
SKILL_APPROVAL_LAYERS = {SKILL_LAYER_OPTIONAL_ANALYSIS, SKILL_LAYER_REPORTING}



def require_actor_role(
    actor_role: Any,
    *,
    expected_role: str,
    action_name: str,
) -> str:
    raw_role = maybe_text(actor_role)
    resolved_role = normalize_actor_role(raw_role) or raw_role
    if resolved_role != maybe_text(expected_role):
        raise ValueError(
            f"{action_name} requires actor role `{expected_role}`, got `{raw_role or '<empty>'}`."
        )
    return resolved_role


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


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def stable_hash(*parts: Any) -> str:
    import hashlib

    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def connect_db(run_dir: str | Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    return connect_deliberation_db(resolve_run_dir(run_dir), db_path)


def _skill_policy_for_approval(skill_name: str) -> dict[str, Any]:
    policy = resolve_skill_policy(skill_name)
    if not bool(policy.get("requires_operator_approval")):
        raise ValueError(
            f"Skill {skill_name} does not declare requires_operator_approval and cannot use skill approval requests."
        )
    skill_layer = maybe_text(policy.get("skill_layer"))
    if skill_layer not in SKILL_APPROVAL_LAYERS:
        raise ValueError(
            "Skill approval requests support optional-analysis and reporting skills. "
            f"Skill {skill_name} uses layer `{skill_layer or '<empty>'}`; state-transition approvals should use phase transition requests."
        )
    return policy


def _validate_requested_actor_for_skill(skill_name: str, requested_actor_role: str) -> str:
    resolved_role = normalize_actor_role(requested_actor_role) or maybe_text(requested_actor_role)
    access = evaluate_skill_access(
        skill_name,
        actor_role=resolved_role,
        contract_mode="strict",
    )
    issues = access.get("issues", []) if isinstance(access.get("issues"), list) else []
    blocking_issues = [
        issue
        for issue in issues
        if isinstance(issue, dict)
        and bool(issue.get("blocking"))
        and maybe_text(issue.get("code")) != "operator-approval-required"
    ]
    if blocking_issues:
        message = maybe_text(blocking_issues[0].get("message"))
        raise ValueError(message or f"Requested actor role `{resolved_role}` cannot execute {skill_name}.")
    return maybe_text(access.get("resolved_actor_role")) or resolved_role


def skill_approval_request_id(
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    requested_actor_role: str,
    created_at_utc: str,
) -> str:
    return (
        "skill-approval-request-"
        + stable_hash(
            "skill-approval-request",
            run_id,
            round_id,
            skill_name,
            requested_actor_role,
            created_at_utc,
        )[:12]
    )


def skill_approval_id(*, request_id: str, approved_at_utc: str) -> str:
    return "skill-approval-" + stable_hash(
        "skill-approval",
        request_id,
        approved_at_utc,
    )[:12]


def skill_approval_rejection_id(*, request_id: str, rejected_at_utc: str) -> str:
    return "skill-approval-rejection-" + stable_hash(
        "skill-approval-rejection",
        request_id,
        rejected_at_utc,
    )[:12]


def skill_approval_consumption_id(*, request_id: str, consumed_at_utc: str) -> str:
    return "skill-approval-consumption-" + stable_hash(
        "skill-approval-consumption",
        request_id,
        consumed_at_utc,
    )[:12]


__all__ = (
    "OBJECT_KIND_SKILL_APPROVAL_REQUEST",
    "OBJECT_KIND_SKILL_APPROVAL",
    "OBJECT_KIND_SKILL_APPROVAL_REJECTION",
    "OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION",
    "REQUEST_STATUS_PENDING",
    "REQUEST_STATUS_APPROVED",
    "REQUEST_STATUS_REJECTED",
    "REQUEST_STATUS_CONSUMED",
    "DECISION_STATUS_APPROVED",
    "DECISION_STATUS_REJECTED",
    "CONSUMPTION_STATUS_CONSUMED",
    "SKILL_APPROVAL_LAYERS",
    "ROLE_MODERATOR",
    "ROLE_RUNTIME_OPERATOR",
    "maybe_text",
    "normalize_actor_role",
    "require_actor_role",
    "unique_texts",
    "utc_now_iso",
    "stable_hash",
    "json_text",
    "resolve_run_dir",
    "connect_db",
    "_skill_policy_for_approval",
    "_validate_requested_actor_for_skill",
    "skill_approval_request_id",
    "skill_approval_id",
    "skill_approval_rejection_id",
    "skill_approval_consumption_id",
)
