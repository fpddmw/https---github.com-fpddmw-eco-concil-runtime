from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.governance.transition_request_common import (
    DECISION_STATUS_APPROVED,
    DECISION_STATUS_REJECTED,
    REQUEST_STATUS_APPROVED,
    REQUEST_STATUS_COMMITTED,
    REQUEST_STATUS_REJECTED,
    ROLE_MODERATOR,
    ROLE_RUNTIME_OPERATOR,
    TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    connect_db,
    json_text,
    maybe_text,
    normalize_actor_role,
    normalize_transition_kind,
    require_actor_role,
    unique_texts,
    utc_now_iso,
)
from eco_council_runtime.kernel.planes.deliberation_plane import payload_from_db_row
from eco_council_runtime.kernel.governance.transition_request_payloads import (
    transition_approval_payload,
    transition_rejection_payload,
    transition_request_payload,
)
from eco_council_runtime.kernel.governance.transition_request_rows import (
    transition_approval_row_from_payload,
    transition_rejection_row_from_payload,
    transition_request_row_from_payload,
    write_transition_approval_row,
    write_transition_rejection_row,
    write_transition_request_row,
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
    requested_object_kind = maybe_text(committed_object_kind)
    requested_object_id = maybe_text(committed_object_id)
    if not requested_object_kind or not requested_object_id:
        raise ValueError(
            "mark_transition_request_committed requires a committed object kind and id."
        )
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
            if status == REQUEST_STATUS_COMMITTED:
                current_object_kind = maybe_text(request.get("committed_object_kind"))
                current_object_id = maybe_text(request.get("committed_object_id"))
                if (
                    current_object_kind == requested_object_kind
                    and current_object_id == requested_object_id
                ):
                    return {
                        **request,
                        "commit_status": "already-committed",
                        "db_path": str(db_file),
                    }
                raise ValueError(
                    f"Transition request {request_id} is already committed to "
                    f"`{current_object_kind}:{current_object_id}` and cannot be "
                    f"recommitted to `{requested_object_kind}:{requested_object_id}`."
                )
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
                committed_object_kind=requested_object_kind,
                committed_object_id=requested_object_id,
                created_at_utc=maybe_text(request.get("created_at_utc")),
                updated_at_utc=committed_at,
            )
            write_transition_request_row(
                connection,
                transition_request_row_from_payload(updated_request),
            )
    finally:
        connection.close()
    return {**updated_request, "commit_status": "committed", "db_path": str(db_file)}


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


__all__ = (
    "fetch_row_payload",
    "load_transition_request",
    "load_transition_requests",
    "latest_transition_request",
    "store_transition_request",
    "approve_transition_request",
    "reject_transition_request",
    "mark_transition_request_committed",
    "resolve_transition_request_for_execution",
)
