from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..canonical_contracts import canonical_contract, validate_canonical_payload
from .access_policy import evaluate_skill_access
from .deliberation_plane import (
    connect_db as connect_deliberation_db,
    maybe_text,
    payload_from_db_row,
)
from .role_contracts import ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR, known_actor_role, normalize_actor_role
from .skill_registry import SKILL_LAYER_OPTIONAL_ANALYSIS, resolve_skill_policy

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
    if skill_layer != SKILL_LAYER_OPTIONAL_ANALYSIS:
        raise ValueError(
            f"Skill approval requests currently support optional-analysis skills only, got layer `{skill_layer or '<empty>'}` for {skill_name}."
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


def skill_approval_request_payload(
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    requested_by_role: Any,
    requested_actor_role: Any = "",
    rationale: Any = "",
    requested_skill_args: list[Any] | None = None,
    evidence_refs: list[Any] | None = None,
    basis_object_ids: list[Any] | None = None,
    request_payload: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    lineage: list[Any] | None = None,
    request_id: Any = "",
    request_status: Any = REQUEST_STATUS_PENDING,
    operator_notes: list[Any] | None = None,
    decision_ids: list[Any] | None = None,
    latest_decision_id: Any = "",
    latest_decision_status: Any = "",
    latest_decision_by_role: Any = "",
    latest_decision_reason: Any = "",
    approved_at_utc: Any = "",
    rejected_at_utc: Any = "",
    consumed_at_utc: Any = "",
    consumed_by_role: Any = "",
    consumed_receipt_id: Any = "",
    consumed_event_id: Any = "",
    created_at_utc: Any = "",
    updated_at_utc: Any = "",
) -> dict[str, Any]:
    policy = _skill_policy_for_approval(skill_name)
    created = maybe_text(created_at_utc) or utc_now_iso()
    resolved_requested_by_role = normalize_actor_role(requested_by_role) or maybe_text(
        requested_by_role
    )
    resolved_requested_actor_role = (
        normalize_actor_role(requested_actor_role)
        or normalize_actor_role(requested_by_role)
        or maybe_text(requested_actor_role)
        or maybe_text(requested_by_role)
    )
    if not known_actor_role(resolved_requested_by_role):
        raise ValueError(
            "Skill approval request requires a known requested_by_role, "
            f"got `{maybe_text(requested_by_role) or '<empty>'}`."
        )
    if not known_actor_role(resolved_requested_actor_role):
        raise ValueError(
            "Skill approval request requires a known requested_actor_role, "
            f"got `{maybe_text(requested_actor_role) or '<empty>'}`."
        )
    if (
        resolved_requested_by_role
        not in {resolved_requested_actor_role, ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR}
    ):
        raise ValueError(
            "Skill approval request may only be submitted by moderator, runtime-operator, "
            "or the requested actor role."
        )
    validated_requested_actor_role = _validate_requested_actor_for_skill(
        skill_name,
        resolved_requested_actor_role,
    )
    payload = {
        "schema_version": canonical_contract(
            OBJECT_KIND_SKILL_APPROVAL_REQUEST
        ).schema_version,
        "request_id": maybe_text(request_id)
        or skill_approval_request_id(
            run_id=maybe_text(run_id),
            round_id=maybe_text(round_id),
            skill_name=maybe_text(skill_name),
            requested_actor_role=validated_requested_actor_role,
            created_at_utc=created,
        ),
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "created_at_utc": created,
        "updated_at_utc": maybe_text(updated_at_utc) or created,
        "request_status": maybe_text(request_status) or REQUEST_STATUS_PENDING,
        "skill_name": maybe_text(skill_name),
        "skill_layer": maybe_text(policy.get("skill_layer")),
        "requested_by_role": resolved_requested_by_role,
        "requested_actor_role": validated_requested_actor_role,
        "required_approval_role": ROLE_RUNTIME_OPERATOR,
        "requested_surface": "kernel-command",
        "requested_action": "run-optional-analysis-skill",
        "requested_command_name": "run-skill",
        "rationale": maybe_text(rationale),
        "requested_skill_args": unique_texts(requested_skill_args or []),
        "evidence_refs": list(evidence_refs) if isinstance(evidence_refs, list) else [],
        "basis_object_ids": unique_texts(basis_object_ids or []),
        "request_payload": request_payload if isinstance(request_payload, dict) else {},
        "operator_notes": unique_texts(operator_notes or []),
        "decision_ids": unique_texts(decision_ids or []),
        "latest_decision_id": maybe_text(latest_decision_id),
        "latest_decision_status": maybe_text(latest_decision_status),
        "latest_decision_by_role": maybe_text(latest_decision_by_role),
        "latest_decision_reason": maybe_text(latest_decision_reason),
        "approved_at_utc": maybe_text(approved_at_utc),
        "rejected_at_utc": maybe_text(rejected_at_utc),
        "consumed_at_utc": maybe_text(consumed_at_utc),
        "consumed_by_role": maybe_text(consumed_by_role),
        "consumed_receipt_id": maybe_text(consumed_receipt_id),
        "consumed_event_id": maybe_text(consumed_event_id),
        "provenance": provenance
        if isinstance(provenance, dict)
        else {
            "source": "request-skill-approval",
            "requested_command_name": "run-skill",
        },
        "lineage": list(lineage) if isinstance(lineage, list) else [],
    }
    return validate_canonical_payload(OBJECT_KIND_SKILL_APPROVAL_REQUEST, payload)


def skill_approval_payload(
    *,
    request_payload: dict[str, Any],
    approved_by_role: Any,
    decision_reason: Any = "",
    evidence_refs: list[Any] | None = None,
    basis_object_ids: list[Any] | None = None,
    operator_notes: list[Any] | None = None,
    provenance: dict[str, Any] | None = None,
    lineage: list[Any] | None = None,
    approval_id: Any = "",
    approved_at_utc: Any = "",
) -> dict[str, Any]:
    approved_at = maybe_text(approved_at_utc) or utc_now_iso()
    payload = {
        "schema_version": canonical_contract(OBJECT_KIND_SKILL_APPROVAL).schema_version,
        "approval_id": maybe_text(approval_id)
        or skill_approval_id(
            request_id=maybe_text(request_payload.get("request_id")),
            approved_at_utc=approved_at,
        ),
        "run_id": maybe_text(request_payload.get("run_id")),
        "round_id": maybe_text(request_payload.get("round_id")),
        "request_id": maybe_text(request_payload.get("request_id")),
        "approved_at_utc": approved_at,
        "approved_by_role": normalize_actor_role(approved_by_role)
        or maybe_text(approved_by_role),
        "decision_status": DECISION_STATUS_APPROVED,
        "decision_reason": maybe_text(decision_reason),
        "skill_name": maybe_text(request_payload.get("skill_name")),
        "skill_layer": maybe_text(request_payload.get("skill_layer")),
        "requested_by_role": maybe_text(request_payload.get("requested_by_role")),
        "requested_actor_role": maybe_text(request_payload.get("requested_actor_role")),
        "requested_command_name": maybe_text(
            request_payload.get("requested_command_name")
        ),
        "requested_skill_args": unique_texts(
            request_payload.get("requested_skill_args", [])
            if isinstance(request_payload.get("requested_skill_args"), list)
            else []
        ),
        "evidence_refs": list(evidence_refs) if isinstance(evidence_refs, list) else [],
        "basis_object_ids": unique_texts(
            basis_object_ids
            if isinstance(basis_object_ids, list)
            else request_payload.get("basis_object_ids", [])
        ),
        "operator_notes": unique_texts(operator_notes or []),
        "request_snapshot": dict(request_payload),
        "provenance": provenance
        if isinstance(provenance, dict)
        else {"source": "approve-skill-approval"},
        "lineage": list(lineage) if isinstance(lineage, list) else [],
    }
    return validate_canonical_payload(OBJECT_KIND_SKILL_APPROVAL, payload)


def skill_approval_rejection_payload(
    *,
    request_payload: dict[str, Any],
    rejected_by_role: Any,
    decision_reason: Any,
    evidence_refs: list[Any] | None = None,
    basis_object_ids: list[Any] | None = None,
    operator_notes: list[Any] | None = None,
    provenance: dict[str, Any] | None = None,
    lineage: list[Any] | None = None,
    rejection_id: Any = "",
    rejected_at_utc: Any = "",
) -> dict[str, Any]:
    rejected_at = maybe_text(rejected_at_utc) or utc_now_iso()
    payload = {
        "schema_version": canonical_contract(
            OBJECT_KIND_SKILL_APPROVAL_REJECTION
        ).schema_version,
        "rejection_id": maybe_text(rejection_id)
        or skill_approval_rejection_id(
            request_id=maybe_text(request_payload.get("request_id")),
            rejected_at_utc=rejected_at,
        ),
        "run_id": maybe_text(request_payload.get("run_id")),
        "round_id": maybe_text(request_payload.get("round_id")),
        "request_id": maybe_text(request_payload.get("request_id")),
        "rejected_at_utc": rejected_at,
        "rejected_by_role": normalize_actor_role(rejected_by_role)
        or maybe_text(rejected_by_role),
        "decision_status": DECISION_STATUS_REJECTED,
        "decision_reason": maybe_text(decision_reason),
        "skill_name": maybe_text(request_payload.get("skill_name")),
        "skill_layer": maybe_text(request_payload.get("skill_layer")),
        "requested_by_role": maybe_text(request_payload.get("requested_by_role")),
        "requested_actor_role": maybe_text(request_payload.get("requested_actor_role")),
        "requested_command_name": maybe_text(
            request_payload.get("requested_command_name")
        ),
        "requested_skill_args": unique_texts(
            request_payload.get("requested_skill_args", [])
            if isinstance(request_payload.get("requested_skill_args"), list)
            else []
        ),
        "evidence_refs": list(evidence_refs) if isinstance(evidence_refs, list) else [],
        "basis_object_ids": unique_texts(
            basis_object_ids
            if isinstance(basis_object_ids, list)
            else request_payload.get("basis_object_ids", [])
        ),
        "operator_notes": unique_texts(operator_notes or []),
        "request_snapshot": dict(request_payload),
        "provenance": provenance
        if isinstance(provenance, dict)
        else {"source": "reject-skill-approval"},
        "lineage": list(lineage) if isinstance(lineage, list) else [],
    }
    return validate_canonical_payload(OBJECT_KIND_SKILL_APPROVAL_REJECTION, payload)


def skill_approval_consumption_payload(
    *,
    request_payload: dict[str, Any],
    approval_id: Any,
    consumed_by_role: Any,
    execution_receipt_id: Any,
    execution_event_id: Any,
    execution_status: Any = "completed",
    provenance: dict[str, Any] | None = None,
    lineage: list[Any] | None = None,
    consumption_id: Any = "",
    consumed_at_utc: Any = "",
) -> dict[str, Any]:
    consumed_at = maybe_text(consumed_at_utc) or utc_now_iso()
    payload = {
        "schema_version": canonical_contract(
            OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION
        ).schema_version,
        "consumption_id": maybe_text(consumption_id)
        or skill_approval_consumption_id(
            request_id=maybe_text(request_payload.get("request_id")),
            consumed_at_utc=consumed_at,
        ),
        "run_id": maybe_text(request_payload.get("run_id")),
        "round_id": maybe_text(request_payload.get("round_id")),
        "request_id": maybe_text(request_payload.get("request_id")),
        "approval_id": maybe_text(approval_id),
        "consumed_at_utc": consumed_at,
        "consumed_by_role": normalize_actor_role(consumed_by_role)
        or maybe_text(consumed_by_role),
        "consumption_status": CONSUMPTION_STATUS_CONSUMED,
        "skill_name": maybe_text(request_payload.get("skill_name")),
        "skill_layer": maybe_text(request_payload.get("skill_layer")),
        "requested_actor_role": maybe_text(request_payload.get("requested_actor_role")),
        "execution_receipt_id": maybe_text(execution_receipt_id),
        "execution_event_id": maybe_text(execution_event_id),
        "execution_status": maybe_text(execution_status) or "completed",
        "provenance": provenance
        if isinstance(provenance, dict)
        else {"source": "run-skill"},
        "lineage": list(lineage) if isinstance(lineage, list) else [],
    }
    return validate_canonical_payload(OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION, payload)


def skill_approval_request_row_from_payload(
    payload: dict[str, Any],
    *,
    artifact_path: str = "",
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "request_id": maybe_text(payload.get("request_id")),
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "created_at_utc": maybe_text(payload.get("created_at_utc")),
        "updated_at_utc": maybe_text(payload.get("updated_at_utc")),
        "request_status": maybe_text(payload.get("request_status")),
        "skill_name": maybe_text(payload.get("skill_name")),
        "skill_layer": maybe_text(payload.get("skill_layer")),
        "requested_by_role": maybe_text(payload.get("requested_by_role")),
        "requested_actor_role": maybe_text(payload.get("requested_actor_role")),
        "required_approval_role": maybe_text(payload.get("required_approval_role")),
        "requested_surface": maybe_text(payload.get("requested_surface")),
        "requested_action": maybe_text(payload.get("requested_action")),
        "requested_command_name": maybe_text(payload.get("requested_command_name")),
        "rationale": maybe_text(payload.get("rationale")),
        "requested_skill_args_json": json_text(payload.get("requested_skill_args", [])),
        "evidence_refs_json": json_text(payload.get("evidence_refs", [])),
        "basis_object_ids_json": json_text(payload.get("basis_object_ids", [])),
        "request_payload_json": json_text(payload.get("request_payload", {})),
        "operator_notes_json": json_text(payload.get("operator_notes", [])),
        "decision_ids_json": json_text(payload.get("decision_ids", [])),
        "latest_decision_id": maybe_text(payload.get("latest_decision_id")),
        "latest_decision_status": maybe_text(payload.get("latest_decision_status")),
        "latest_decision_by_role": maybe_text(payload.get("latest_decision_by_role")),
        "latest_decision_reason": maybe_text(payload.get("latest_decision_reason")),
        "approved_at_utc": maybe_text(payload.get("approved_at_utc")),
        "rejected_at_utc": maybe_text(payload.get("rejected_at_utc")),
        "consumed_at_utc": maybe_text(payload.get("consumed_at_utc")),
        "consumed_by_role": maybe_text(payload.get("consumed_by_role")),
        "consumed_receipt_id": maybe_text(payload.get("consumed_receipt_id")),
        "consumed_event_id": maybe_text(payload.get("consumed_event_id")),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "provenance_json": json_text(payload.get("provenance", {})),
        "lineage_json": json_text(payload.get("lineage", [])),
        "raw_json": json_text(payload),
    }


def skill_approval_row_from_payload(
    payload: dict[str, Any],
    *,
    artifact_path: str = "",
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "approval_id": maybe_text(payload.get("approval_id")),
        "request_id": maybe_text(payload.get("request_id")),
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "approved_at_utc": maybe_text(payload.get("approved_at_utc")),
        "approved_by_role": maybe_text(payload.get("approved_by_role")),
        "decision_status": maybe_text(payload.get("decision_status")),
        "decision_reason": maybe_text(payload.get("decision_reason")),
        "skill_name": maybe_text(payload.get("skill_name")),
        "skill_layer": maybe_text(payload.get("skill_layer")),
        "requested_by_role": maybe_text(payload.get("requested_by_role")),
        "requested_actor_role": maybe_text(payload.get("requested_actor_role")),
        "requested_command_name": maybe_text(payload.get("requested_command_name")),
        "requested_skill_args_json": json_text(payload.get("requested_skill_args", [])),
        "evidence_refs_json": json_text(payload.get("evidence_refs", [])),
        "basis_object_ids_json": json_text(payload.get("basis_object_ids", [])),
        "operator_notes_json": json_text(payload.get("operator_notes", [])),
        "request_snapshot_json": json_text(payload.get("request_snapshot", {})),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "provenance_json": json_text(payload.get("provenance", {})),
        "lineage_json": json_text(payload.get("lineage", [])),
        "raw_json": json_text(payload),
    }


def skill_approval_rejection_row_from_payload(
    payload: dict[str, Any],
    *,
    artifact_path: str = "",
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "rejection_id": maybe_text(payload.get("rejection_id")),
        "request_id": maybe_text(payload.get("request_id")),
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "rejected_at_utc": maybe_text(payload.get("rejected_at_utc")),
        "rejected_by_role": maybe_text(payload.get("rejected_by_role")),
        "decision_status": maybe_text(payload.get("decision_status")),
        "decision_reason": maybe_text(payload.get("decision_reason")),
        "skill_name": maybe_text(payload.get("skill_name")),
        "skill_layer": maybe_text(payload.get("skill_layer")),
        "requested_by_role": maybe_text(payload.get("requested_by_role")),
        "requested_actor_role": maybe_text(payload.get("requested_actor_role")),
        "requested_command_name": maybe_text(payload.get("requested_command_name")),
        "requested_skill_args_json": json_text(payload.get("requested_skill_args", [])),
        "evidence_refs_json": json_text(payload.get("evidence_refs", [])),
        "basis_object_ids_json": json_text(payload.get("basis_object_ids", [])),
        "operator_notes_json": json_text(payload.get("operator_notes", [])),
        "request_snapshot_json": json_text(payload.get("request_snapshot", {})),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "provenance_json": json_text(payload.get("provenance", {})),
        "lineage_json": json_text(payload.get("lineage", [])),
        "raw_json": json_text(payload),
    }


def skill_approval_consumption_row_from_payload(
    payload: dict[str, Any],
    *,
    artifact_path: str = "",
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "consumption_id": maybe_text(payload.get("consumption_id")),
        "request_id": maybe_text(payload.get("request_id")),
        "approval_id": maybe_text(payload.get("approval_id")),
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "consumed_at_utc": maybe_text(payload.get("consumed_at_utc")),
        "consumed_by_role": maybe_text(payload.get("consumed_by_role")),
        "consumption_status": maybe_text(payload.get("consumption_status")),
        "skill_name": maybe_text(payload.get("skill_name")),
        "skill_layer": maybe_text(payload.get("skill_layer")),
        "requested_actor_role": maybe_text(payload.get("requested_actor_role")),
        "execution_receipt_id": maybe_text(payload.get("execution_receipt_id")),
        "execution_event_id": maybe_text(payload.get("execution_event_id")),
        "execution_status": maybe_text(payload.get("execution_status")),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "provenance_json": json_text(payload.get("provenance", {})),
        "lineage_json": json_text(payload.get("lineage", [])),
        "raw_json": json_text(payload),
    }


def write_skill_approval_request_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO skill_approval_requests (
            request_id, run_id, round_id, created_at_utc, updated_at_utc,
            request_status, skill_name, skill_layer, requested_by_role,
            requested_actor_role, required_approval_role, requested_surface,
            requested_action, requested_command_name, rationale,
            requested_skill_args_json, evidence_refs_json, basis_object_ids_json,
            request_payload_json, operator_notes_json, decision_ids_json,
            latest_decision_id, latest_decision_status, latest_decision_by_role,
            latest_decision_reason, approved_at_utc, rejected_at_utc,
            consumed_at_utc, consumed_by_role, consumed_receipt_id,
            consumed_event_id, artifact_path, record_locator, provenance_json,
            lineage_json, raw_json
        ) VALUES (
            :request_id, :run_id, :round_id, :created_at_utc, :updated_at_utc,
            :request_status, :skill_name, :skill_layer, :requested_by_role,
            :requested_actor_role, :required_approval_role, :requested_surface,
            :requested_action, :requested_command_name, :rationale,
            :requested_skill_args_json, :evidence_refs_json, :basis_object_ids_json,
            :request_payload_json, :operator_notes_json, :decision_ids_json,
            :latest_decision_id, :latest_decision_status, :latest_decision_by_role,
            :latest_decision_reason, :approved_at_utc, :rejected_at_utc,
            :consumed_at_utc, :consumed_by_role, :consumed_receipt_id,
            :consumed_event_id, :artifact_path, :record_locator, :provenance_json,
            :lineage_json, :raw_json
        )
        """,
        row,
    )


def write_skill_approval_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO skill_approvals (
            approval_id, request_id, run_id, round_id, approved_at_utc,
            approved_by_role, decision_status, decision_reason, skill_name,
            skill_layer, requested_by_role, requested_actor_role,
            requested_command_name, requested_skill_args_json, evidence_refs_json,
            basis_object_ids_json, operator_notes_json, request_snapshot_json,
            artifact_path, record_locator, provenance_json, lineage_json, raw_json
        ) VALUES (
            :approval_id, :request_id, :run_id, :round_id, :approved_at_utc,
            :approved_by_role, :decision_status, :decision_reason, :skill_name,
            :skill_layer, :requested_by_role, :requested_actor_role,
            :requested_command_name, :requested_skill_args_json, :evidence_refs_json,
            :basis_object_ids_json, :operator_notes_json, :request_snapshot_json,
            :artifact_path, :record_locator, :provenance_json, :lineage_json, :raw_json
        )
        """,
        row,
    )


def write_skill_approval_rejection_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO skill_approval_rejections (
            rejection_id, request_id, run_id, round_id, rejected_at_utc,
            rejected_by_role, decision_status, decision_reason, skill_name,
            skill_layer, requested_by_role, requested_actor_role,
            requested_command_name, requested_skill_args_json, evidence_refs_json,
            basis_object_ids_json, operator_notes_json, request_snapshot_json,
            artifact_path, record_locator, provenance_json, lineage_json, raw_json
        ) VALUES (
            :rejection_id, :request_id, :run_id, :round_id, :rejected_at_utc,
            :rejected_by_role, :decision_status, :decision_reason, :skill_name,
            :skill_layer, :requested_by_role, :requested_actor_role,
            :requested_command_name, :requested_skill_args_json, :evidence_refs_json,
            :basis_object_ids_json, :operator_notes_json, :request_snapshot_json,
            :artifact_path, :record_locator, :provenance_json, :lineage_json, :raw_json
        )
        """,
        row,
    )


def write_skill_approval_consumption_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO skill_approval_consumptions (
            consumption_id, request_id, approval_id, run_id, round_id,
            consumed_at_utc, consumed_by_role, consumption_status, skill_name,
            skill_layer, requested_actor_role, execution_receipt_id,
            execution_event_id, execution_status, artifact_path, record_locator,
            provenance_json, lineage_json, raw_json
        ) VALUES (
            :consumption_id, :request_id, :approval_id, :run_id, :round_id,
            :consumed_at_utc, :consumed_by_role, :consumption_status, :skill_name,
            :skill_layer, :requested_actor_role, :execution_receipt_id,
            :execution_event_id, :execution_status, :artifact_path, :record_locator,
            :provenance_json, :lineage_json, :raw_json
        )
        """,
        row,
    )


def fetch_row_payload(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    record_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        f"SELECT * FROM {table_name} WHERE {id_column} = ?",
        (maybe_text(record_id),),
    ).fetchone()
    return payload_from_db_row(row) if row is not None else None


def load_skill_approval_request(
    run_dir: str | Path,
    *,
    request_id: str,
    db_path: str = "",
) -> dict[str, Any] | None:
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        return fetch_row_payload(
            connection,
            table_name="skill_approval_requests",
            id_column="request_id",
            record_id=request_id,
        )
    finally:
        connection.close()


def load_skill_approval_requests(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    skill_name: str = "",
    request_status: str = "",
    requested_by_role: str = "",
    requested_actor_role: str = "",
    limit: int = 20,
    db_path: str = "",
) -> list[dict[str, Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []
    if maybe_text(run_id):
        where_clauses.append("run_id = ?")
        params.append(maybe_text(run_id))
    if maybe_text(round_id):
        where_clauses.append("round_id = ?")
        params.append(maybe_text(round_id))
    if maybe_text(skill_name):
        where_clauses.append("skill_name = ?")
        params.append(maybe_text(skill_name))
    if maybe_text(request_status):
        where_clauses.append("request_status = ?")
        params.append(maybe_text(request_status))
    if maybe_text(requested_by_role):
        where_clauses.append("requested_by_role = ?")
        params.append(normalize_actor_role(requested_by_role) or maybe_text(requested_by_role))
    if maybe_text(requested_actor_role):
        where_clauses.append("requested_actor_role = ?")
        params.append(
            normalize_actor_role(requested_actor_role) or maybe_text(requested_actor_role)
        )

    query = "SELECT * FROM skill_approval_requests"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY updated_at_utc DESC, created_at_utc DESC, request_id DESC LIMIT ?"
    params.append(max(1, min(200, int(limit or 20))))
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        rows = connection.execute(query, tuple(params)).fetchall()
        return [payload_from_db_row(row) for row in rows]
    finally:
        connection.close()


def latest_skill_approval_request(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    skill_name: str = "",
    request_status: str = "",
    requested_actor_role: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    rows = load_skill_approval_requests(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        request_status=request_status,
        requested_actor_role=requested_actor_role,
        limit=1,
        db_path=db_path,
    )
    return rows[0] if rows else None


def store_skill_approval_request(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    requested_by_role: Any,
    requested_actor_role: Any = "",
    rationale: Any = "",
    requested_skill_args: list[Any] | None = None,
    evidence_refs: list[Any] | None = None,
    basis_object_ids: list[Any] | None = None,
    request_payload: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    lineage: list[Any] | None = None,
    db_path: str = "",
) -> dict[str, Any]:
    payload = skill_approval_request_payload(
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        requested_by_role=requested_by_role,
        requested_actor_role=requested_actor_role,
        rationale=rationale,
        requested_skill_args=requested_skill_args,
        evidence_refs=evidence_refs,
        basis_object_ids=basis_object_ids,
        request_payload=request_payload,
        provenance=provenance,
        lineage=lineage,
    )
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            write_skill_approval_request_row(
                connection,
                skill_approval_request_row_from_payload(payload),
            )
    finally:
        connection.close()
    return {**payload, "db_path": str(db_file)}


def approve_skill_approval_request(
    run_dir: str | Path,
    *,
    request_id: str,
    approved_by_role: Any,
    decision_reason: Any = "",
    evidence_refs: list[Any] | None = None,
    basis_object_ids: list[Any] | None = None,
    operator_notes: list[Any] | None = None,
    provenance: dict[str, Any] | None = None,
    lineage: list[Any] | None = None,
    db_path: str = "",
) -> dict[str, Any]:
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            request = fetch_row_payload(
                connection,
                table_name="skill_approval_requests",
                id_column="request_id",
                record_id=request_id,
            )
            if not isinstance(request, dict):
                raise ValueError(f"Unknown skill approval request: {request_id}")
            required_approval_role = (
                normalize_actor_role(request.get("required_approval_role"))
                or ROLE_RUNTIME_OPERATOR
            )
            resolved_approved_by_role = require_actor_role(
                approved_by_role,
                expected_role=required_approval_role,
                action_name="approve_skill_approval_request",
            )
            status = maybe_text(request.get("request_status"))
            if status == REQUEST_STATUS_REJECTED:
                raise ValueError(f"Skill approval request {request_id} is already rejected.")
            if status == REQUEST_STATUS_CONSUMED:
                approval_rows = connection.execute(
                    """
                    SELECT * FROM skill_approvals
                    WHERE request_id = ?
                    ORDER BY approved_at_utc DESC, approval_id DESC
                    LIMIT 1
                    """,
                    (request_id,),
                ).fetchall()
                existing_approval = (
                    payload_from_db_row(approval_rows[0]) if approval_rows else {}
                )
                return {
                    "request": request,
                    "approval": existing_approval,
                    "db_path": str(db_file),
                }
            if status == REQUEST_STATUS_APPROVED:
                approval_rows = connection.execute(
                    """
                    SELECT * FROM skill_approvals
                    WHERE request_id = ?
                    ORDER BY approved_at_utc DESC, approval_id DESC
                    LIMIT 1
                    """,
                    (request_id,),
                ).fetchall()
                existing_approval = (
                    payload_from_db_row(approval_rows[0]) if approval_rows else {}
                )
                return {
                    "request": request,
                    "approval": existing_approval,
                    "db_path": str(db_file),
                }
            approval = skill_approval_payload(
                request_payload=request,
                approved_by_role=resolved_approved_by_role,
                decision_reason=decision_reason,
                evidence_refs=evidence_refs,
                basis_object_ids=basis_object_ids,
                operator_notes=operator_notes,
                provenance=provenance,
                lineage=lineage,
            )
            write_skill_approval_row(
                connection,
                skill_approval_row_from_payload(approval),
            )
            updated_request = skill_approval_request_payload(
                run_id=maybe_text(request.get("run_id")),
                round_id=maybe_text(request.get("round_id")),
                skill_name=maybe_text(request.get("skill_name")),
                requested_by_role=maybe_text(request.get("requested_by_role")),
                requested_actor_role=maybe_text(request.get("requested_actor_role")),
                rationale=maybe_text(request.get("rationale")),
                requested_skill_args=request.get("requested_skill_args", [])
                if isinstance(request.get("requested_skill_args"), list)
                else [],
                evidence_refs=request.get("evidence_refs", [])
                if isinstance(request.get("evidence_refs"), list)
                else [],
                basis_object_ids=request.get("basis_object_ids", [])
                if isinstance(request.get("basis_object_ids"), list)
                else [],
                request_payload=request.get("request_payload")
                if isinstance(request.get("request_payload"), dict)
                else {},
                provenance=request.get("provenance")
                if isinstance(request.get("provenance"), dict)
                else {},
                lineage=request.get("lineage")
                if isinstance(request.get("lineage"), list)
                else [],
                request_id=maybe_text(request.get("request_id")),
                request_status=REQUEST_STATUS_APPROVED,
                operator_notes=unique_texts(
                    [
                        *(
                            request.get("operator_notes", [])
                            if isinstance(request.get("operator_notes"), list)
                            else []
                        ),
                        *(
                            approval.get("operator_notes", [])
                            if isinstance(approval.get("operator_notes"), list)
                            else []
                        ),
                    ]
                ),
                decision_ids=unique_texts(
                    [
                        *(
                            request.get("decision_ids", [])
                            if isinstance(request.get("decision_ids"), list)
                            else []
                        ),
                        maybe_text(approval.get("approval_id")),
                    ]
                ),
                latest_decision_id=maybe_text(approval.get("approval_id")),
                latest_decision_status=DECISION_STATUS_APPROVED,
                latest_decision_by_role=maybe_text(approval.get("approved_by_role")),
                latest_decision_reason=maybe_text(approval.get("decision_reason")),
                approved_at_utc=maybe_text(approval.get("approved_at_utc")),
                rejected_at_utc=maybe_text(request.get("rejected_at_utc")),
                consumed_at_utc=maybe_text(request.get("consumed_at_utc")),
                consumed_by_role=maybe_text(request.get("consumed_by_role")),
                consumed_receipt_id=maybe_text(request.get("consumed_receipt_id")),
                consumed_event_id=maybe_text(request.get("consumed_event_id")),
                created_at_utc=maybe_text(request.get("created_at_utc")),
                updated_at_utc=maybe_text(approval.get("approved_at_utc")),
            )
            write_skill_approval_request_row(
                connection,
                skill_approval_request_row_from_payload(updated_request),
            )
    finally:
        connection.close()
    return {"request": updated_request, "approval": approval, "db_path": str(db_file)}


def reject_skill_approval_request(
    run_dir: str | Path,
    *,
    request_id: str,
    rejected_by_role: Any,
    decision_reason: Any,
    evidence_refs: list[Any] | None = None,
    basis_object_ids: list[Any] | None = None,
    operator_notes: list[Any] | None = None,
    provenance: dict[str, Any] | None = None,
    lineage: list[Any] | None = None,
    db_path: str = "",
) -> dict[str, Any]:
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            request = fetch_row_payload(
                connection,
                table_name="skill_approval_requests",
                id_column="request_id",
                record_id=request_id,
            )
            if not isinstance(request, dict):
                raise ValueError(f"Unknown skill approval request: {request_id}")
            required_approval_role = (
                normalize_actor_role(request.get("required_approval_role"))
                or ROLE_RUNTIME_OPERATOR
            )
            resolved_rejected_by_role = require_actor_role(
                rejected_by_role,
                expected_role=required_approval_role,
                action_name="reject_skill_approval_request",
            )
            status = maybe_text(request.get("request_status"))
            if status == REQUEST_STATUS_CONSUMED:
                raise ValueError(f"Skill approval request {request_id} is already consumed.")
            if status == REQUEST_STATUS_APPROVED:
                raise ValueError(f"Skill approval request {request_id} is already approved.")
            if status == REQUEST_STATUS_REJECTED:
                rejection_rows = connection.execute(
                    """
                    SELECT * FROM skill_approval_rejections
                    WHERE request_id = ?
                    ORDER BY rejected_at_utc DESC, rejection_id DESC
                    LIMIT 1
                    """,
                    (request_id,),
                ).fetchall()
                existing_rejection = (
                    payload_from_db_row(rejection_rows[0]) if rejection_rows else {}
                )
                return {
                    "request": request,
                    "rejection": existing_rejection,
                    "db_path": str(db_file),
                }
            rejection = skill_approval_rejection_payload(
                request_payload=request,
                rejected_by_role=resolved_rejected_by_role,
                decision_reason=decision_reason,
                evidence_refs=evidence_refs,
                basis_object_ids=basis_object_ids,
                operator_notes=operator_notes,
                provenance=provenance,
                lineage=lineage,
            )
            write_skill_approval_rejection_row(
                connection,
                skill_approval_rejection_row_from_payload(rejection),
            )
            updated_request = skill_approval_request_payload(
                run_id=maybe_text(request.get("run_id")),
                round_id=maybe_text(request.get("round_id")),
                skill_name=maybe_text(request.get("skill_name")),
                requested_by_role=maybe_text(request.get("requested_by_role")),
                requested_actor_role=maybe_text(request.get("requested_actor_role")),
                rationale=maybe_text(request.get("rationale")),
                requested_skill_args=request.get("requested_skill_args", [])
                if isinstance(request.get("requested_skill_args"), list)
                else [],
                evidence_refs=request.get("evidence_refs", [])
                if isinstance(request.get("evidence_refs"), list)
                else [],
                basis_object_ids=request.get("basis_object_ids", [])
                if isinstance(request.get("basis_object_ids"), list)
                else [],
                request_payload=request.get("request_payload")
                if isinstance(request.get("request_payload"), dict)
                else {},
                provenance=request.get("provenance")
                if isinstance(request.get("provenance"), dict)
                else {},
                lineage=request.get("lineage")
                if isinstance(request.get("lineage"), list)
                else [],
                request_id=maybe_text(request.get("request_id")),
                request_status=REQUEST_STATUS_REJECTED,
                operator_notes=unique_texts(
                    [
                        *(
                            request.get("operator_notes", [])
                            if isinstance(request.get("operator_notes"), list)
                            else []
                        ),
                        *(
                            rejection.get("operator_notes", [])
                            if isinstance(rejection.get("operator_notes"), list)
                            else []
                        ),
                    ]
                ),
                decision_ids=unique_texts(
                    [
                        *(
                            request.get("decision_ids", [])
                            if isinstance(request.get("decision_ids"), list)
                            else []
                        ),
                        maybe_text(rejection.get("rejection_id")),
                    ]
                ),
                latest_decision_id=maybe_text(rejection.get("rejection_id")),
                latest_decision_status=DECISION_STATUS_REJECTED,
                latest_decision_by_role=maybe_text(rejection.get("rejected_by_role")),
                latest_decision_reason=maybe_text(rejection.get("decision_reason")),
                approved_at_utc=maybe_text(request.get("approved_at_utc")),
                rejected_at_utc=maybe_text(rejection.get("rejected_at_utc")),
                consumed_at_utc=maybe_text(request.get("consumed_at_utc")),
                consumed_by_role=maybe_text(request.get("consumed_by_role")),
                consumed_receipt_id=maybe_text(request.get("consumed_receipt_id")),
                consumed_event_id=maybe_text(request.get("consumed_event_id")),
                created_at_utc=maybe_text(request.get("created_at_utc")),
                updated_at_utc=maybe_text(rejection.get("rejected_at_utc")),
            )
            write_skill_approval_request_row(
                connection,
                skill_approval_request_row_from_payload(updated_request),
            )
    finally:
        connection.close()
    return {
        "request": updated_request,
        "rejection": rejection,
        "db_path": str(db_file),
    }


def mark_skill_approval_consumed(
    run_dir: str | Path,
    *,
    request_id: str,
    consumed_by_role: Any,
    execution_receipt_id: str,
    execution_event_id: str,
    execution_status: str = "completed",
    db_path: str = "",
) -> dict[str, Any]:
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            request = fetch_row_payload(
                connection,
                table_name="skill_approval_requests",
                id_column="request_id",
                record_id=request_id,
            )
            if not isinstance(request, dict):
                raise ValueError(f"Unknown skill approval request: {request_id}")
            status = maybe_text(request.get("request_status"))
            if status not in {REQUEST_STATUS_APPROVED, REQUEST_STATUS_CONSUMED}:
                raise ValueError(
                    f"Skill approval request {request_id} is not approved for consumption; current status is {status or '<empty>'}."
                )
            requested_actor_role = (
                normalize_actor_role(request.get("requested_actor_role"))
                or maybe_text(request.get("requested_actor_role"))
            )
            resolved_consumed_by_role = (
                normalize_actor_role(consumed_by_role) or maybe_text(consumed_by_role)
            )
            if resolved_consumed_by_role not in {requested_actor_role, ROLE_RUNTIME_OPERATOR}:
                raise ValueError(
                    "mark_skill_approval_consumed requires actor role matching "
                    f"requested_actor_role `{requested_actor_role}` or `runtime-operator`, "
                    f"got `{maybe_text(consumed_by_role) or '<empty>'}`."
                )
            if status == REQUEST_STATUS_CONSUMED:
                if (
                    maybe_text(request.get("consumed_receipt_id"))
                    == maybe_text(execution_receipt_id)
                    and maybe_text(request.get("consumed_event_id"))
                    == maybe_text(execution_event_id)
                ):
                    consumption_rows = connection.execute(
                        """
                        SELECT * FROM skill_approval_consumptions
                        WHERE request_id = ?
                        ORDER BY consumed_at_utc DESC, consumption_id DESC
                        LIMIT 1
                        """,
                        (request_id,),
                    ).fetchall()
                    existing_consumption = (
                        payload_from_db_row(consumption_rows[0])
                        if consumption_rows
                        else {}
                    )
                    return {
                        "request": request,
                        "consumption": existing_consumption,
                        "db_path": str(db_file),
                    }
                raise ValueError(
                    f"Skill approval request {request_id} is already consumed by receipt `{maybe_text(request.get('consumed_receipt_id'))}`."
                )
            approval_id = maybe_text(request.get("latest_decision_id"))
            if (
                maybe_text(request.get("latest_decision_status"))
                != DECISION_STATUS_APPROVED
                or not approval_id
            ):
                approval_rows = connection.execute(
                    """
                    SELECT * FROM skill_approvals
                    WHERE request_id = ?
                    ORDER BY approved_at_utc DESC, approval_id DESC
                    LIMIT 1
                    """,
                    (request_id,),
                ).fetchall()
                if not approval_rows:
                    raise ValueError(
                        f"Skill approval request {request_id} has no approved decision to consume."
                    )
                approval_id = maybe_text(payload_from_db_row(approval_rows[0]).get("approval_id"))
            consumption = skill_approval_consumption_payload(
                request_payload=request,
                approval_id=approval_id,
                consumed_by_role=resolved_consumed_by_role,
                execution_receipt_id=execution_receipt_id,
                execution_event_id=execution_event_id,
                execution_status=execution_status,
            )
            write_skill_approval_consumption_row(
                connection,
                skill_approval_consumption_row_from_payload(consumption),
            )
            updated_request = skill_approval_request_payload(
                run_id=maybe_text(request.get("run_id")),
                round_id=maybe_text(request.get("round_id")),
                skill_name=maybe_text(request.get("skill_name")),
                requested_by_role=maybe_text(request.get("requested_by_role")),
                requested_actor_role=maybe_text(request.get("requested_actor_role")),
                rationale=maybe_text(request.get("rationale")),
                requested_skill_args=request.get("requested_skill_args", [])
                if isinstance(request.get("requested_skill_args"), list)
                else [],
                evidence_refs=request.get("evidence_refs", [])
                if isinstance(request.get("evidence_refs"), list)
                else [],
                basis_object_ids=request.get("basis_object_ids", [])
                if isinstance(request.get("basis_object_ids"), list)
                else [],
                request_payload=request.get("request_payload")
                if isinstance(request.get("request_payload"), dict)
                else {},
                provenance=request.get("provenance")
                if isinstance(request.get("provenance"), dict)
                else {},
                lineage=request.get("lineage")
                if isinstance(request.get("lineage"), list)
                else [],
                request_id=maybe_text(request.get("request_id")),
                request_status=REQUEST_STATUS_CONSUMED,
                operator_notes=request.get("operator_notes", [])
                if isinstance(request.get("operator_notes"), list)
                else [],
                decision_ids=request.get("decision_ids", [])
                if isinstance(request.get("decision_ids"), list)
                else [],
                latest_decision_id=maybe_text(request.get("latest_decision_id")),
                latest_decision_status=maybe_text(request.get("latest_decision_status")),
                latest_decision_by_role=maybe_text(request.get("latest_decision_by_role")),
                latest_decision_reason=maybe_text(request.get("latest_decision_reason")),
                approved_at_utc=maybe_text(request.get("approved_at_utc")),
                rejected_at_utc=maybe_text(request.get("rejected_at_utc")),
                consumed_at_utc=maybe_text(consumption.get("consumed_at_utc")),
                consumed_by_role=resolved_consumed_by_role,
                consumed_receipt_id=maybe_text(execution_receipt_id),
                consumed_event_id=maybe_text(execution_event_id),
                created_at_utc=maybe_text(request.get("created_at_utc")),
                updated_at_utc=maybe_text(consumption.get("consumed_at_utc")),
            )
            write_skill_approval_request_row(
                connection,
                skill_approval_request_row_from_payload(updated_request),
            )
    finally:
        connection.close()
    return {
        "request": updated_request,
        "consumption": consumption,
        "db_path": str(db_file),
    }


def resolve_skill_approval_for_execution(
    run_dir: str | Path,
    *,
    request_id: str,
    skill_name: str,
    run_id: str,
    round_id: str,
    requested_actor_role: str,
    db_path: str = "",
) -> dict[str, Any]:
    request = load_skill_approval_request(run_dir, request_id=request_id, db_path=db_path)
    if not isinstance(request, dict):
        raise ValueError(f"Unknown skill approval request: {request_id}")
    status = maybe_text(request.get("request_status"))
    if status == REQUEST_STATUS_CONSUMED:
        raise ValueError(
            f"Skill approval request {request_id} is already consumed and cannot be reused."
        )
    if status != REQUEST_STATUS_APPROVED:
        raise ValueError(
            f"Skill approval request {request_id} is not approved for execution; current status is {status or '<empty>'}."
        )
    if maybe_text(request.get("skill_name")) != maybe_text(skill_name):
        raise ValueError(
            f"Skill approval request {request_id} is for `{maybe_text(request.get('skill_name'))}`, not `{maybe_text(skill_name)}`."
        )
    if maybe_text(request.get("run_id")) != maybe_text(run_id):
        raise ValueError(
            f"Skill approval request {request_id} belongs to run `{maybe_text(request.get('run_id'))}`, not `{maybe_text(run_id)}`."
        )
    if maybe_text(request.get("round_id")) != maybe_text(round_id):
        raise ValueError(
            f"Skill approval request {request_id} belongs to round `{maybe_text(request.get('round_id'))}`, not `{maybe_text(round_id)}`."
        )
    resolved_requested_actor_role = (
        normalize_actor_role(request.get("requested_actor_role"))
        or maybe_text(request.get("requested_actor_role"))
    )
    expected_actor_role = normalize_actor_role(requested_actor_role) or maybe_text(
        requested_actor_role
    )
    if resolved_requested_actor_role != expected_actor_role:
        raise ValueError(
            "Skill approval request "
            f"{request_id} is for actor `{resolved_requested_actor_role}`, not `{expected_actor_role}`."
        )
    return request


__all__ = [
    "DECISION_STATUS_APPROVED",
    "DECISION_STATUS_REJECTED",
    "OBJECT_KIND_SKILL_APPROVAL",
    "OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION",
    "OBJECT_KIND_SKILL_APPROVAL_REJECTION",
    "OBJECT_KIND_SKILL_APPROVAL_REQUEST",
    "REQUEST_STATUS_APPROVED",
    "REQUEST_STATUS_CONSUMED",
    "REQUEST_STATUS_PENDING",
    "REQUEST_STATUS_REJECTED",
    "approve_skill_approval_request",
    "latest_skill_approval_request",
    "load_skill_approval_request",
    "load_skill_approval_requests",
    "mark_skill_approval_consumed",
    "reject_skill_approval_request",
    "resolve_skill_approval_for_execution",
    "store_skill_approval_request",
]
