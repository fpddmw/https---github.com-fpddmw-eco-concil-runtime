from __future__ import annotations

from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from eco_council_runtime.kernel.governance.skill_approval_common import (
    CONSUMPTION_STATUS_CONSUMED,
    DECISION_STATUS_APPROVED,
    DECISION_STATUS_REJECTED,
    OBJECT_KIND_SKILL_APPROVAL,
    OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION,
    OBJECT_KIND_SKILL_APPROVAL_REJECTION,
    OBJECT_KIND_SKILL_APPROVAL_REQUEST,
    REQUEST_STATUS_PENDING,
    ROLE_MODERATOR,
    ROLE_RUNTIME_OPERATOR,
    _skill_policy_for_approval,
    _validate_requested_actor_for_skill,
    known_actor_role,
    maybe_text,
    normalize_actor_role,
    skill_approval_consumption_id,
    skill_approval_id,
    skill_approval_rejection_id,
    skill_approval_request_id,
    unique_texts,
    utc_now_iso,
)

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
        "requested_action": f"run-{maybe_text(policy.get('skill_layer'))}-skill",
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


__all__ = (
    "skill_approval_request_payload",
    "skill_approval_payload",
    "skill_approval_rejection_payload",
    "skill_approval_consumption_payload",
)
