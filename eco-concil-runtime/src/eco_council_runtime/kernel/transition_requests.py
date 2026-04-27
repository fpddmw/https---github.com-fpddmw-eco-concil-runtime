from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..canonical_contracts import canonical_contract, validate_canonical_payload
from .deliberation_plane import (
    connect_db as connect_deliberation_db,
    maybe_text,
    payload_from_db_row,
)
from .role_contracts import ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR, normalize_actor_role

OBJECT_KIND_TRANSITION_REQUEST = "transition-request"
OBJECT_KIND_TRANSITION_APPROVAL = "transition-approval"
OBJECT_KIND_TRANSITION_REJECTION = "transition-rejection"

REQUEST_STATUS_PENDING = "pending-operator-confirmation"
REQUEST_STATUS_APPROVED = "approved"
REQUEST_STATUS_REJECTED = "rejected"
REQUEST_STATUS_COMMITTED = "committed"

DECISION_STATUS_APPROVED = "approved"
DECISION_STATUS_REJECTED = "rejected"

TRANSITION_KIND_OPEN_INVESTIGATION_ROUND = "open-investigation-round"
TRANSITION_KIND_PROMOTE_EVIDENCE_BASIS = "promote-evidence-basis"
TRANSITION_KIND_CLOSE_ROUND = "close-round"

TRANSITION_KIND_ALIASES = {
    "open-round": TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    "open-investigation-round": TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    "open-follow-up-round": TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    "promote": TRANSITION_KIND_PROMOTE_EVIDENCE_BASIS,
    "promotion": TRANSITION_KIND_PROMOTE_EVIDENCE_BASIS,
    "freeze-report-basis": TRANSITION_KIND_PROMOTE_EVIDENCE_BASIS,
    "promote-evidence-basis": TRANSITION_KIND_PROMOTE_EVIDENCE_BASIS,
    "close": TRANSITION_KIND_CLOSE_ROUND,
    "close-round": TRANSITION_KIND_CLOSE_ROUND,
}

TRANSITION_KIND_SPECS: dict[str, dict[str, str]] = {
    TRANSITION_KIND_OPEN_INVESTIGATION_ROUND: {
        "requested_surface": "skill",
        "requested_action": "open-follow-up-round",
        "requested_command_name": "open-investigation-round",
    },
    TRANSITION_KIND_PROMOTE_EVIDENCE_BASIS: {
        "requested_surface": "skill",
        "requested_action": "freeze-report-basis",
        "requested_command_name": "promote-evidence-basis",
    },
    TRANSITION_KIND_CLOSE_ROUND: {
        "requested_surface": "kernel-command",
        "requested_action": "archive-close-round",
        "requested_command_name": "close-round",
    },
}


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


def list_dicts(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]


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


def normalize_transition_kind(transition_kind: Any) -> str:
    text = maybe_text(transition_kind)
    if not text:
        return ""
    return TRANSITION_KIND_ALIASES.get(text, text)


def transition_kind_spec(transition_kind: Any) -> dict[str, str]:
    normalized = normalize_transition_kind(transition_kind)
    spec = TRANSITION_KIND_SPECS.get(normalized)
    if not isinstance(spec, dict):
        supported = ", ".join(sorted(TRANSITION_KIND_SPECS))
        raise ValueError(
            f"Unsupported transition kind: {maybe_text(transition_kind) or '<empty>'}. "
            f"Supported kinds: {supported}."
        )
    return spec


def transition_request_id(
    *,
    run_id: str,
    round_id: str,
    transition_kind: str,
    target_round_id: str,
    created_at_utc: str,
) -> str:
    return (
        "transition-request-"
        + stable_hash(
            "transition-request",
            run_id,
            round_id,
            transition_kind,
            target_round_id,
            created_at_utc,
        )[:12]
    )


def transition_approval_id(
    *,
    request_id: str,
    approved_at_utc: str,
) -> str:
    return "transition-approval-" + stable_hash(
        "transition-approval",
        request_id,
        approved_at_utc,
    )[:12]


def transition_rejection_id(
    *,
    request_id: str,
    rejected_at_utc: str,
) -> str:
    return "transition-rejection-" + stable_hash(
        "transition-rejection",
        request_id,
        rejected_at_utc,
    )[:12]


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
    normalized_target_round_id = (
        maybe_text(target_round_id)
        or (maybe_text(round_id) if normalized_kind != TRANSITION_KIND_OPEN_INVESTIGATION_ROUND else "")
    )
    if normalized_kind == TRANSITION_KIND_OPEN_INVESTIGATION_ROUND and not normalized_target_round_id:
        raise ValueError("Open investigation round requests require a target_round_id.")
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


def transition_request_row_from_payload(
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
        "transition_kind": maybe_text(payload.get("transition_kind")),
        "request_status": maybe_text(payload.get("request_status")),
        "requested_by_role": maybe_text(payload.get("requested_by_role")),
        "required_approval_role": maybe_text(payload.get("required_approval_role")),
        "requested_surface": maybe_text(payload.get("requested_surface")),
        "requested_action": maybe_text(payload.get("requested_action")),
        "requested_command_name": maybe_text(payload.get("requested_command_name")),
        "source_round_id": maybe_text(payload.get("source_round_id")),
        "target_round_id": maybe_text(payload.get("target_round_id")),
        "rationale": maybe_text(payload.get("rationale")),
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
        "committed_at_utc": maybe_text(payload.get("committed_at_utc")),
        "committed_by_role": maybe_text(payload.get("committed_by_role")),
        "committed_object_kind": maybe_text(payload.get("committed_object_kind")),
        "committed_object_id": maybe_text(payload.get("committed_object_id")),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "provenance_json": json_text(payload.get("provenance", {})),
        "lineage_json": json_text(payload.get("lineage", [])),
        "raw_json": json_text(payload),
    }


def transition_approval_row_from_payload(
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
        "transition_kind": maybe_text(payload.get("transition_kind")),
        "requested_by_role": maybe_text(payload.get("requested_by_role")),
        "requested_command_name": maybe_text(payload.get("requested_command_name")),
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


def transition_rejection_row_from_payload(
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
        "transition_kind": maybe_text(payload.get("transition_kind")),
        "requested_by_role": maybe_text(payload.get("requested_by_role")),
        "requested_command_name": maybe_text(payload.get("requested_command_name")),
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


def write_transition_request_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO transition_requests (
            request_id, run_id, round_id, created_at_utc, updated_at_utc,
            transition_kind, request_status, requested_by_role, required_approval_role,
            requested_surface, requested_action, requested_command_name,
            source_round_id, target_round_id, rationale, evidence_refs_json,
            basis_object_ids_json, request_payload_json, operator_notes_json,
            decision_ids_json, latest_decision_id, latest_decision_status,
            latest_decision_by_role, latest_decision_reason, approved_at_utc,
            rejected_at_utc, committed_at_utc, committed_by_role,
            committed_object_kind, committed_object_id, artifact_path,
            record_locator, provenance_json, lineage_json, raw_json
        ) VALUES (
            :request_id, :run_id, :round_id, :created_at_utc, :updated_at_utc,
            :transition_kind, :request_status, :requested_by_role, :required_approval_role,
            :requested_surface, :requested_action, :requested_command_name,
            :source_round_id, :target_round_id, :rationale, :evidence_refs_json,
            :basis_object_ids_json, :request_payload_json, :operator_notes_json,
            :decision_ids_json, :latest_decision_id, :latest_decision_status,
            :latest_decision_by_role, :latest_decision_reason, :approved_at_utc,
            :rejected_at_utc, :committed_at_utc, :committed_by_role,
            :committed_object_kind, :committed_object_id, :artifact_path,
            :record_locator, :provenance_json, :lineage_json, :raw_json
        )
        """,
        row,
    )


def write_transition_approval_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO transition_approvals (
            approval_id, request_id, run_id, round_id, approved_at_utc,
            approved_by_role, decision_status, decision_reason, transition_kind,
            requested_by_role, requested_command_name, evidence_refs_json,
            basis_object_ids_json, operator_notes_json, request_snapshot_json,
            artifact_path, record_locator, provenance_json, lineage_json, raw_json
        ) VALUES (
            :approval_id, :request_id, :run_id, :round_id, :approved_at_utc,
            :approved_by_role, :decision_status, :decision_reason, :transition_kind,
            :requested_by_role, :requested_command_name, :evidence_refs_json,
            :basis_object_ids_json, :operator_notes_json, :request_snapshot_json,
            :artifact_path, :record_locator, :provenance_json, :lineage_json, :raw_json
        )
        """,
        row,
    )


def write_transition_rejection_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO transition_rejections (
            rejection_id, request_id, run_id, round_id, rejected_at_utc,
            rejected_by_role, decision_status, decision_reason, transition_kind,
            requested_by_role, requested_command_name, evidence_refs_json,
            basis_object_ids_json, operator_notes_json, request_snapshot_json,
            artifact_path, record_locator, provenance_json, lineage_json, raw_json
        ) VALUES (
            :rejection_id, :request_id, :run_id, :round_id, :rejected_at_utc,
            :rejected_by_role, :decision_status, :decision_reason, :transition_kind,
            :requested_by_role, :requested_command_name, :evidence_refs_json,
            :basis_object_ids_json, :operator_notes_json, :request_snapshot_json,
            :artifact_path, :record_locator, :provenance_json, :lineage_json, :raw_json
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


def load_transition_request(
    run_dir: str | Path,
    *,
    request_id: str,
    db_path: str = "",
) -> dict[str, Any] | None:
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        return fetch_row_payload(
            connection,
            table_name="transition_requests",
            id_column="request_id",
            record_id=request_id,
        )
    finally:
        connection.close()


def load_transition_requests(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    transition_kind: str = "",
    request_status: str = "",
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
    normalized_kind = normalize_transition_kind(transition_kind)
    if normalized_kind:
        where_clauses.append("transition_kind = ?")
        params.append(normalized_kind)
    if maybe_text(request_status):
        where_clauses.append("request_status = ?")
        params.append(maybe_text(request_status))
    query = "SELECT * FROM transition_requests"
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


def latest_transition_request(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    transition_kind: str = "",
    request_status: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    rows = load_transition_requests(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind=transition_kind,
        request_status=request_status,
        limit=1,
        db_path=db_path,
    )
    return rows[0] if rows else None


def store_transition_request(
    run_dir: str | Path,
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
    db_path: str = "",
) -> dict[str, Any]:
    resolved_requested_by_role = require_actor_role(
        requested_by_role,
        expected_role=ROLE_MODERATOR,
        action_name="store_transition_request",
    )
    payload = transition_request_payload(
        run_id=run_id,
        round_id=round_id,
        transition_kind=transition_kind,
        requested_by_role=resolved_requested_by_role,
        target_round_id=target_round_id,
        source_round_id=source_round_id,
        rationale=rationale,
        evidence_refs=evidence_refs,
        basis_object_ids=basis_object_ids,
        request_payload=request_payload,
        provenance=provenance,
        lineage=lineage,
    )
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            write_transition_request_row(
                connection,
                transition_request_row_from_payload(payload),
            )
    finally:
        connection.close()
    return {**payload, "db_path": str(db_file)}


def approve_transition_request(
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
                table_name="transition_requests",
                id_column="request_id",
                record_id=request_id,
            )
            if not isinstance(request, dict):
                raise ValueError(f"Unknown transition request: {request_id}")
            required_approval_role = (
                normalize_actor_role(request.get("required_approval_role"))
                or ROLE_RUNTIME_OPERATOR
            )
            resolved_approved_by_role = require_actor_role(
                approved_by_role,
                expected_role=required_approval_role,
                action_name="approve_transition_request",
            )
            if maybe_text(request.get("request_status")) == REQUEST_STATUS_REJECTED:
                raise ValueError(f"Transition request {request_id} is already rejected.")
            if maybe_text(request.get("request_status")) == REQUEST_STATUS_COMMITTED:
                raise ValueError(f"Transition request {request_id} is already committed.")
            if maybe_text(request.get("request_status")) == REQUEST_STATUS_APPROVED:
                approval_rows = connection.execute(
                    """
                    SELECT * FROM transition_approvals
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
            approval = transition_approval_payload(
                request_payload=request,
                approved_by_role=resolved_approved_by_role,
                decision_reason=decision_reason,
                evidence_refs=evidence_refs,
                basis_object_ids=basis_object_ids,
                operator_notes=operator_notes,
                provenance=provenance,
                lineage=lineage,
            )
            write_transition_approval_row(
                connection,
                transition_approval_row_from_payload(approval),
            )
            updated_request = transition_request_payload(
                run_id=maybe_text(request.get("run_id")),
                round_id=maybe_text(request.get("round_id")),
                transition_kind=maybe_text(request.get("transition_kind")),
                requested_by_role=maybe_text(request.get("requested_by_role")),
                target_round_id=maybe_text(request.get("target_round_id")),
                source_round_id=maybe_text(request.get("source_round_id")),
                rationale=maybe_text(request.get("rationale")),
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
                committed_at_utc=maybe_text(request.get("committed_at_utc")),
                committed_by_role=maybe_text(request.get("committed_by_role")),
                committed_object_kind=maybe_text(request.get("committed_object_kind")),
                committed_object_id=maybe_text(request.get("committed_object_id")),
                created_at_utc=maybe_text(request.get("created_at_utc")),
                updated_at_utc=maybe_text(approval.get("approved_at_utc")),
            )
            write_transition_request_row(
                connection,
                transition_request_row_from_payload(updated_request),
            )
    finally:
        connection.close()
    return {"request": updated_request, "approval": approval, "db_path": str(db_file)}


def reject_transition_request(
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
                table_name="transition_requests",
                id_column="request_id",
                record_id=request_id,
            )
            if not isinstance(request, dict):
                raise ValueError(f"Unknown transition request: {request_id}")
            required_approval_role = (
                normalize_actor_role(request.get("required_approval_role"))
                or ROLE_RUNTIME_OPERATOR
            )
            resolved_rejected_by_role = require_actor_role(
                rejected_by_role,
                expected_role=required_approval_role,
                action_name="reject_transition_request",
            )
            if maybe_text(request.get("request_status")) == REQUEST_STATUS_COMMITTED:
                raise ValueError(f"Transition request {request_id} is already committed.")
            if maybe_text(request.get("request_status")) == REQUEST_STATUS_REJECTED:
                rejection_rows = connection.execute(
                    """
                    SELECT * FROM transition_rejections
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
            rejection = transition_rejection_payload(
                request_payload=request,
                rejected_by_role=resolved_rejected_by_role,
                decision_reason=decision_reason,
                evidence_refs=evidence_refs,
                basis_object_ids=basis_object_ids,
                operator_notes=operator_notes,
                provenance=provenance,
                lineage=lineage,
            )
            write_transition_rejection_row(
                connection,
                transition_rejection_row_from_payload(rejection),
            )
            updated_request = transition_request_payload(
                run_id=maybe_text(request.get("run_id")),
                round_id=maybe_text(request.get("round_id")),
                transition_kind=maybe_text(request.get("transition_kind")),
                requested_by_role=maybe_text(request.get("requested_by_role")),
                target_round_id=maybe_text(request.get("target_round_id")),
                source_round_id=maybe_text(request.get("source_round_id")),
                rationale=maybe_text(request.get("rationale")),
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
                committed_at_utc=maybe_text(request.get("committed_at_utc")),
                committed_by_role=maybe_text(request.get("committed_by_role")),
                committed_object_kind=maybe_text(request.get("committed_object_kind")),
                committed_object_id=maybe_text(request.get("committed_object_id")),
                created_at_utc=maybe_text(request.get("created_at_utc")),
                updated_at_utc=maybe_text(rejection.get("rejected_at_utc")),
            )
            write_transition_request_row(
                connection,
                transition_request_row_from_payload(updated_request),
            )
    finally:
        connection.close()
    return {
        "request": updated_request,
        "rejection": rejection,
        "db_path": str(db_file),
    }


def mark_transition_request_committed(
    run_dir: str | Path,
    *,
    request_id: str,
    committed_by_role: Any,
    committed_object_kind: str,
    committed_object_id: str,
    db_path: str = "",
) -> dict[str, Any]:
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            request = fetch_row_payload(
                connection,
                table_name="transition_requests",
                id_column="request_id",
                record_id=request_id,
            )
            if not isinstance(request, dict):
                raise ValueError(f"Unknown transition request: {request_id}")
            required_approval_role = (
                normalize_actor_role(request.get("required_approval_role"))
                or ROLE_RUNTIME_OPERATOR
            )
            resolved_committed_by_role = require_actor_role(
                committed_by_role,
                expected_role=required_approval_role,
                action_name="mark_transition_request_committed",
            )
            status = maybe_text(request.get("request_status"))
            if status not in {REQUEST_STATUS_APPROVED, REQUEST_STATUS_COMMITTED}:
                raise ValueError(
                    f"Transition request {request_id} is not approved for commit; current status is {status or '<empty>'}."
                )
            if (
                status == REQUEST_STATUS_COMMITTED
                and maybe_text(request.get("committed_object_kind")) == maybe_text(committed_object_kind)
                and maybe_text(request.get("committed_object_id")) == maybe_text(committed_object_id)
            ):
                return {**request, "db_path": str(db_file)}
            committed_at = utc_now_iso()
            updated_request = transition_request_payload(
                run_id=maybe_text(request.get("run_id")),
                round_id=maybe_text(request.get("round_id")),
                transition_kind=maybe_text(request.get("transition_kind")),
                requested_by_role=maybe_text(request.get("requested_by_role")),
                target_round_id=maybe_text(request.get("target_round_id")),
                source_round_id=maybe_text(request.get("source_round_id")),
                rationale=maybe_text(request.get("rationale")),
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
                request_status=REQUEST_STATUS_COMMITTED,
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
                committed_at_utc=committed_at,
                committed_by_role=resolved_committed_by_role,
                committed_object_kind=committed_object_kind,
                committed_object_id=committed_object_id,
                created_at_utc=maybe_text(request.get("created_at_utc")),
                updated_at_utc=committed_at,
            )
            write_transition_request_row(
                connection,
                transition_request_row_from_payload(updated_request),
            )
    finally:
        connection.close()
    return {**updated_request, "db_path": str(db_file)}


def resolve_transition_request_for_execution(
    run_dir: str | Path,
    *,
    request_id: str,
    transition_kind: Any,
    run_id: str,
    round_id: str,
    source_round_id: str = "",
    target_round_id: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    request = load_transition_request(run_dir, request_id=request_id, db_path=db_path)
    if not isinstance(request, dict):
        raise ValueError(f"Unknown transition request: {request_id}")
    status = maybe_text(request.get("request_status"))
    if status not in {REQUEST_STATUS_APPROVED, REQUEST_STATUS_COMMITTED}:
        raise ValueError(
            f"Transition request {request_id} is not approved for execution; current status is {status or '<empty>'}."
        )
    expected_kind = normalize_transition_kind(transition_kind)
    actual_kind = normalize_transition_kind(request.get("transition_kind"))
    if actual_kind != expected_kind:
        raise ValueError(
            f"Transition request {request_id} expects `{actual_kind}`, not `{expected_kind}`."
        )
    if maybe_text(request.get("run_id")) != maybe_text(run_id):
        raise ValueError(
            f"Transition request {request_id} belongs to run `{maybe_text(request.get('run_id'))}`, not `{maybe_text(run_id)}`."
        )
    if expected_kind == TRANSITION_KIND_OPEN_INVESTIGATION_ROUND:
        expected_source_round_id = maybe_text(source_round_id)
        expected_target_round_id = maybe_text(target_round_id) or maybe_text(round_id)
        if maybe_text(request.get("round_id")) != expected_source_round_id:
            raise ValueError(
                f"Transition request {request_id} belongs to source round `{maybe_text(request.get('round_id'))}`, not `{expected_source_round_id}`."
            )
        if maybe_text(request.get("target_round_id")) != expected_target_round_id:
            raise ValueError(
                f"Transition request {request_id} targets round `{maybe_text(request.get('target_round_id'))}`, not `{expected_target_round_id}`."
            )
    else:
        expected_round_id = maybe_text(round_id)
        if maybe_text(request.get("round_id")) != expected_round_id:
            raise ValueError(
                f"Transition request {request_id} belongs to round `{maybe_text(request.get('round_id'))}`, not `{expected_round_id}`."
            )
    return request


def request_payload_option(
    request: dict[str, Any],
    key: str,
    default: Any = None,
) -> Any:
    request_payload = (
        request.get("request_payload", {})
        if isinstance(request.get("request_payload"), dict)
        else {}
    )
    return request_payload.get(key, default)


__all__ = [
    "DECISION_STATUS_APPROVED",
    "DECISION_STATUS_REJECTED",
    "OBJECT_KIND_TRANSITION_APPROVAL",
    "OBJECT_KIND_TRANSITION_REJECTION",
    "OBJECT_KIND_TRANSITION_REQUEST",
    "REQUEST_STATUS_APPROVED",
    "REQUEST_STATUS_COMMITTED",
    "REQUEST_STATUS_PENDING",
    "REQUEST_STATUS_REJECTED",
    "ROLE_MODERATOR",
    "ROLE_RUNTIME_OPERATOR",
    "TRANSITION_KIND_CLOSE_ROUND",
    "TRANSITION_KIND_OPEN_INVESTIGATION_ROUND",
    "TRANSITION_KIND_PROMOTE_EVIDENCE_BASIS",
    "approve_transition_request",
    "latest_transition_request",
    "load_transition_request",
    "load_transition_requests",
    "mark_transition_request_committed",
    "normalize_transition_kind",
    "reject_transition_request",
    "request_payload_option",
    "resolve_transition_request_for_execution",
    "store_transition_request",
    "transition_kind_spec",
]
