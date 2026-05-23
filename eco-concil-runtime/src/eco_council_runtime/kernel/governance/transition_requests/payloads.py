from __future__ import annotations

from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from eco_council_runtime.kernel.governance.transition_requests.common import (
    DECISION_STATUS_APPROVED,
    DECISION_STATUS_REJECTED,
    OBJECT_KIND_TRANSITION_APPROVAL,
    OBJECT_KIND_TRANSITION_REJECTION,
    OBJECT_KIND_TRANSITION_REQUEST,
    REQUEST_STATUS_PENDING,
    ROLE_RUNTIME_OPERATOR,
    TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
    maybe_text,
    normalize_actor_role,
    normalize_transition_kind,
    transition_approval_id,
    transition_kind_spec,
    transition_rejection_id,
    transition_request_id,
    unique_texts,
    utc_now_iso,
    validate_supplemental_round_target,
)

def transition_request_payload(
    *,
    run_id: str,
    round_id: str,
    transition_kind: Any,
    requested_by_role: Any,
    target_round_id: Any = "",
    source_round_id: Any = "",
    rationale: Any = "",
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
    committed_at_utc: Any = "",
    committed_by_role: Any = "",
    committed_object_kind: Any = "",
    committed_object_id: Any = "",
    created_at_utc: Any = "",
    updated_at_utc: Any = "",
) -> dict[str, Any]:
    normalized_kind = normalize_transition_kind(transition_kind)
    spec = transition_kind_spec(normalized_kind)
    created = maybe_text(created_at_utc) or utc_now_iso()
    target_required_transition_kinds = {
        TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
        TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
    }
    normalized_target_round_id = (
        maybe_text(target_round_id)
        or (maybe_text(round_id) if normalized_kind not in target_required_transition_kinds else "")
    )
    if normalized_kind in target_required_transition_kinds and not normalized_target_round_id:
        raise ValueError(f"{normalized_kind} requests require a target_round_id.")
    validate_supplemental_round_target(
        transition_kind=normalized_kind,
        target_round_id=normalized_target_round_id,
        request_payload=request_payload,
    )
    resolved_requested_by_role = normalize_actor_role(requested_by_role) or maybe_text(
        requested_by_role
    )
    payload = {
        "schema_version": canonical_contract(OBJECT_KIND_TRANSITION_REQUEST).schema_version,
        "request_id": maybe_text(request_id)
        or transition_request_id(
            run_id=maybe_text(run_id),
            round_id=maybe_text(round_id),
            transition_kind=normalized_kind,
            target_round_id=normalized_target_round_id,
            created_at_utc=created,
        ),
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "created_at_utc": created,
        "updated_at_utc": maybe_text(updated_at_utc) or created,
        "transition_kind": normalized_kind,
        "request_status": maybe_text(request_status) or REQUEST_STATUS_PENDING,
        "requested_by_role": resolved_requested_by_role,
        "required_approval_role": ROLE_RUNTIME_OPERATOR,
        "requested_surface": spec["requested_surface"],
        "requested_action": spec["requested_action"],
        "requested_command_name": spec["requested_command_name"],
        "source_round_id": maybe_text(source_round_id) or maybe_text(round_id),
        "target_round_id": normalized_target_round_id,
        "rationale": maybe_text(rationale),
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
        "committed_at_utc": maybe_text(committed_at_utc),
        "committed_by_role": maybe_text(committed_by_role),
        "committed_object_kind": maybe_text(committed_object_kind),
        "committed_object_id": maybe_text(committed_object_id),
        "provenance": provenance
        if isinstance(provenance, dict)
        else {
            "source": "request-phase-transition",
            "requested_command_name": spec["requested_command_name"],
        },
        "lineage": list(lineage) if isinstance(lineage, list) else [],
    }
    return validate_canonical_payload(OBJECT_KIND_TRANSITION_REQUEST, payload)


def transition_approval_payload(
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
        "schema_version": canonical_contract(OBJECT_KIND_TRANSITION_APPROVAL).schema_version,
        "approval_id": maybe_text(approval_id)
        or transition_approval_id(
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
        "transition_kind": maybe_text(request_payload.get("transition_kind")),
        "requested_by_role": maybe_text(request_payload.get("requested_by_role")),
        "requested_command_name": maybe_text(
            request_payload.get("requested_command_name")
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
        else {"source": "approve-phase-transition"},
        "lineage": list(lineage) if isinstance(lineage, list) else [],
    }
    return validate_canonical_payload(OBJECT_KIND_TRANSITION_APPROVAL, payload)


def transition_rejection_payload(
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
        "schema_version": canonical_contract(OBJECT_KIND_TRANSITION_REJECTION).schema_version,
        "rejection_id": maybe_text(rejection_id)
        or transition_rejection_id(
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
        "transition_kind": maybe_text(request_payload.get("transition_kind")),
        "requested_by_role": maybe_text(request_payload.get("requested_by_role")),
        "requested_command_name": maybe_text(
            request_payload.get("requested_command_name")
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
        else {"source": "reject-phase-transition"},
        "lineage": list(lineage) if isinstance(lineage, list) else [],
    }
    return validate_canonical_payload(OBJECT_KIND_TRANSITION_REJECTION, payload)


__all__ = (
    "transition_request_payload",
    "transition_approval_payload",
    "transition_rejection_payload",
)
