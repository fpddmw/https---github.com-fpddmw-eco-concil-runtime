from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.governance.skill_approvals.common import (
    DECISION_STATUS_APPROVED,
    DECISION_STATUS_REJECTED,
    REQUEST_STATUS_APPROVED,
    REQUEST_STATUS_CONSUMED,
    REQUEST_STATUS_REJECTED,
    ROLE_RUNTIME_OPERATOR,
    connect_db,
    json_text,
    maybe_text,
    normalize_actor_role,
    require_actor_role,
    unique_texts,
)
from eco_council_runtime.kernel.planes.deliberation_plane import payload_from_db_row
from eco_council_runtime.kernel.governance.skill_approvals.payloads import (
    skill_approval_consumption_payload,
    skill_approval_payload,
    skill_approval_rejection_payload,
    skill_approval_request_payload,
)
from eco_council_runtime.kernel.governance.skill_approvals.rows import (
    skill_approval_consumption_row_from_payload,
    skill_approval_rejection_row_from_payload,
    skill_approval_request_row_from_payload,
    skill_approval_row_from_payload,
    write_skill_approval_consumption_row,
    write_skill_approval_rejection_row,
    write_skill_approval_request_row,
    write_skill_approval_row,
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
    execution_skill_args: list[Any] | None = None,
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
    requested_skill_args = (
        request.get("requested_skill_args", [])
        if isinstance(request.get("requested_skill_args"), list)
        else []
    )
    requested_args = [maybe_text(item) for item in requested_skill_args]
    execution_args = [maybe_text(item) for item in (execution_skill_args or [])]
    if requested_args and requested_args != execution_args:
        raise ValueError(
            "Skill approval request "
            f"{request_id} is scoped to skill args `{json_text(requested_args)}`, "
            f"not `{json_text(execution_args)}`."
        )
    return request


__all__ = (
    "fetch_row_payload",
    "load_skill_approval_request",
    "load_skill_approval_requests",
    "latest_skill_approval_request",
    "store_skill_approval_request",
    "approve_skill_approval_request",
    "reject_skill_approval_request",
    "mark_skill_approval_consumed",
    "resolve_skill_approval_for_execution",
)
