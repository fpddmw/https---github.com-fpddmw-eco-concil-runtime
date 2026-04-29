from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .canonical_contracts import (
    PLANE_RUNTIME,
    canonical_contract,
    canonical_contract_kinds,
)
from .kernel.deliberation_plane import (
    connect_db as connect_deliberation_db,
    maybe_text,
    payload_from_db_row,
)
from .kernel.transition_requests import (
    OBJECT_KIND_TRANSITION_APPROVAL,
    OBJECT_KIND_TRANSITION_REJECTION,
    OBJECT_KIND_TRANSITION_REQUEST,
)
from .kernel.skill_approvals import (
    OBJECT_KIND_SKILL_APPROVAL,
    OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION,
    OBJECT_KIND_SKILL_APPROVAL_REJECTION,
    OBJECT_KIND_SKILL_APPROVAL_REQUEST,
)

OBJECT_KIND_RUNTIME_CONTROL_FREEZE = "runtime-control-freeze"
OBJECT_KIND_CONTROLLER_STATE = "controller-state"
OBJECT_KIND_GATE_STATE = "gate-state"
OBJECT_KIND_SUPERVISOR_STATE = "supervisor-state"
OBJECT_KIND_ORCHESTRATION_PLAN = "orchestration-plan"
OBJECT_KIND_ORCHESTRATION_PLAN_STEP = "orchestration-plan-step"

QUERY_CONFIGS: dict[str, dict[str, Any]] = {
    OBJECT_KIND_RUNTIME_CONTROL_FREEZE: {
        "table_name": "report_basis_freezes",
        "id_column": "freeze_id",
        "timestamp_column": "updated_at_utc",
        "order_by": "updated_at_utc DESC, freeze_id DESC",
        "status_column": "report_basis_status",
        "filter_columns": {
            "controller_status": "controller_status",
            "gate_status": "gate_status",
            "report_basis_status": "report_basis_status",
            "supervisor_status": "supervisor_status",
            "planning_mode": "planning_mode",
            "reporting_handoff_status": "reporting_handoff_status",
            "reporting_ready": "reporting_ready",
        },
    },
    OBJECT_KIND_CONTROLLER_STATE: {
        "table_name": "controller_snapshots",
        "id_column": "snapshot_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, snapshot_id DESC",
        "status_column": "controller_status",
        "filter_columns": {
            "controller_status": "controller_status",
            "gate_status": "gate_status",
            "report_basis_status": "report_basis_status",
            "planning_mode": "planning_mode",
            "current_stage": "current_stage",
            "failed_stage": "failed_stage",
            "resume_status": "resume_status",
        },
    },
    OBJECT_KIND_GATE_STATE: {
        "table_name": "gate_snapshots",
        "id_column": "snapshot_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, snapshot_id DESC",
        "status_column": "gate_status",
        "filter_columns": {
            "gate_status": "gate_status",
            "readiness_status": "readiness_status",
            "stage_name": "stage_name",
            "gate_handler": "gate_handler",
            "decision_source": "decision_source",
        },
    },
    OBJECT_KIND_SUPERVISOR_STATE: {
        "table_name": "supervisor_snapshots",
        "id_column": "snapshot_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, snapshot_id DESC",
        "status_column": "supervisor_status",
        "filter_columns": {
            "supervisor_status": "supervisor_status",
            "controller_status": "controller_status",
            "gate_status": "gate_status",
            "report_basis_status": "report_basis_status",
            "planning_mode": "planning_mode",
            "supervisor_substatus": "supervisor_substatus",
            "phase2_posture": "phase2_posture",
            "terminal_state": "terminal_state",
            "reporting_handoff_status": "reporting_handoff_status",
            "reporting_ready": "reporting_ready",
        },
    },
    OBJECT_KIND_ORCHESTRATION_PLAN: {
        "table_name": "orchestration_plans",
        "id_column": "plan_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, plan_id DESC",
        "status_column": "planning_status",
        "filter_columns": {
            "planning_mode": "planning_mode",
            "controller_authority": "controller_authority",
            "plan_source": "plan_source",
        },
    },
    OBJECT_KIND_ORCHESTRATION_PLAN_STEP: {
        "table_name": "orchestration_plan_steps",
        "id_column": "step_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, plan_step_group, step_index, step_id",
        "status_column": "",
        "filter_columns": {
            "planning_mode": "planning_mode",
            "controller_authority": "controller_authority",
            "plan_source": "plan_source",
            "plan_id": "plan_id",
            "plan_step_group": "plan_step_group",
            "phase_group": "phase_group",
            "stage_name": "stage_name",
            "stage_kind": "stage_kind",
            "skill_name": "skill_name",
            "assigned_role_hint": "assigned_role_hint",
        },
    },
    OBJECT_KIND_TRANSITION_REQUEST: {
        "table_name": "transition_requests",
        "id_column": "request_id",
        "timestamp_column": "updated_at_utc",
        "order_by": "updated_at_utc DESC, created_at_utc DESC, request_id DESC",
        "status_column": "request_status",
        "filter_columns": {
            "transition_kind": "transition_kind",
            "requested_by_role": "requested_by_role",
            "request_id": "request_id",
            "target_round_id": "target_round_id",
            "requested_command_name": "requested_command_name",
            "latest_decision_status": "latest_decision_status",
            "latest_decision_by_role": "latest_decision_by_role",
        },
    },
    OBJECT_KIND_TRANSITION_APPROVAL: {
        "table_name": "transition_approvals",
        "id_column": "approval_id",
        "timestamp_column": "approved_at_utc",
        "order_by": "approved_at_utc DESC, approval_id DESC",
        "status_column": "decision_status",
        "filter_columns": {
            "transition_kind": "transition_kind",
            "requested_by_role": "requested_by_role",
            "request_id": "request_id",
            "decision_by_role": "approved_by_role",
            "requested_command_name": "requested_command_name",
        },
    },
    OBJECT_KIND_TRANSITION_REJECTION: {
        "table_name": "transition_rejections",
        "id_column": "rejection_id",
        "timestamp_column": "rejected_at_utc",
        "order_by": "rejected_at_utc DESC, rejection_id DESC",
        "status_column": "decision_status",
        "filter_columns": {
            "transition_kind": "transition_kind",
            "requested_by_role": "requested_by_role",
            "request_id": "request_id",
            "decision_by_role": "rejected_by_role",
            "requested_command_name": "requested_command_name",
        },
    },
    OBJECT_KIND_SKILL_APPROVAL_REQUEST: {
        "table_name": "skill_approval_requests",
        "id_column": "request_id",
        "timestamp_column": "updated_at_utc",
        "order_by": "updated_at_utc DESC, created_at_utc DESC, request_id DESC",
        "status_column": "request_status",
        "filter_columns": {
            "skill_name": "skill_name",
            "requested_by_role": "requested_by_role",
            "requested_actor_role": "requested_actor_role",
            "request_id": "request_id",
            "requested_command_name": "requested_command_name",
            "latest_decision_status": "latest_decision_status",
            "latest_decision_by_role": "latest_decision_by_role",
        },
    },
    OBJECT_KIND_SKILL_APPROVAL: {
        "table_name": "skill_approvals",
        "id_column": "approval_id",
        "timestamp_column": "approved_at_utc",
        "order_by": "approved_at_utc DESC, approval_id DESC",
        "status_column": "decision_status",
        "filter_columns": {
            "skill_name": "skill_name",
            "requested_by_role": "requested_by_role",
            "requested_actor_role": "requested_actor_role",
            "request_id": "request_id",
            "decision_by_role": "approved_by_role",
            "requested_command_name": "requested_command_name",
        },
    },
    OBJECT_KIND_SKILL_APPROVAL_REJECTION: {
        "table_name": "skill_approval_rejections",
        "id_column": "rejection_id",
        "timestamp_column": "rejected_at_utc",
        "order_by": "rejected_at_utc DESC, rejection_id DESC",
        "status_column": "decision_status",
        "filter_columns": {
            "skill_name": "skill_name",
            "requested_by_role": "requested_by_role",
            "requested_actor_role": "requested_actor_role",
            "request_id": "request_id",
            "decision_by_role": "rejected_by_role",
            "requested_command_name": "requested_command_name",
        },
    },
    OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION: {
        "table_name": "skill_approval_consumptions",
        "id_column": "consumption_id",
        "timestamp_column": "consumed_at_utc",
        "order_by": "consumed_at_utc DESC, consumption_id DESC",
        "status_column": "consumption_status",
        "filter_columns": {
            "skill_name": "skill_name",
            "requested_actor_role": "requested_actor_role",
            "request_id": "request_id",
            "decision_by_role": "consumed_by_role",
        },
    },
}


def connect_db(run_dir: str | Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    return connect_deliberation_db(run_dir_path, db_path)


def control_queryable_object_kinds() -> list[str]:
    target_kinds = set(canonical_contract_kinds(plane=PLANE_RUNTIME))
    return sorted(object_kind for object_kind in QUERY_CONFIGS if object_kind in target_kinds)


def fetch_json_rows(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    where_clauses: list[str],
    params: list[Any],
    order_by: str,
    limit: int,
    offset: int,
) -> tuple[int, list[dict[str, Any]]]:
    count_query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
    if where_clauses:
        count_query += " WHERE " + " AND ".join(where_clauses)
    row = connection.execute(count_query, tuple(params)).fetchone()
    matching_count = int(row["row_count"]) if row is not None else 0

    query = f"SELECT * FROM {table_name}"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    rows = connection.execute(query, tuple([*params, limit, offset])).fetchall()
    return matching_count, [payload_from_db_row(row) for row in rows]


def _unsupported_filter_error(
    *,
    object_kind: str,
    filter_name: str,
) -> ValueError:
    if filter_name == "status":
        supported_kinds = [
            query_kind
            for query_kind, config in QUERY_CONFIGS.items()
            if maybe_text(config.get("status_column"))
        ]
    else:
        supported_kinds = [
            query_kind
            for query_kind, config in QUERY_CONFIGS.items()
            if filter_name in (config.get("filter_columns") or {})
        ]
    supported = ", ".join(sorted(supported_kinds))
    return ValueError(
        f"Unsupported {filter_name} filter for control object kind: {object_kind}. "
        f"Supported kinds: {supported}."
    )


def query_control_objects(
    run_dir: str | Path,
    *,
    object_kind: str,
    run_id: str = "",
    round_id: str = "",
    status: str = "",
    controller_status: str = "",
    gate_status: str = "",
    report_basis_status: str = "",
    supervisor_status: str = "",
    planning_mode: str = "",
    controller_authority: str = "",
    plan_source: str = "",
    plan_id: str = "",
    plan_step_group: str = "",
    phase_group: str = "",
    readiness_status: str = "",
    current_stage: str = "",
    failed_stage: str = "",
    resume_status: str = "",
    stage_name: str = "",
    stage_kind: str = "",
    skill_name: str = "",
    assigned_role_hint: str = "",
    gate_handler: str = "",
    decision_source: str = "",
    supervisor_substatus: str = "",
    phase2_posture: str = "",
    terminal_state: str = "",
    reporting_handoff_status: str = "",
    transition_kind: str = "",
    requested_by_role: str = "",
    requested_actor_role: str = "",
    request_id: str = "",
    target_round_id: str = "",
    requested_command_name: str = "",
    latest_decision_status: str = "",
    latest_decision_by_role: str = "",
    decision_by_role: str = "",
    reporting_ready_only: bool = False,
    include_contract: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    normalized_kind = maybe_text(object_kind)
    config = QUERY_CONFIGS.get(normalized_kind)
    if config is None:
        supported = ", ".join(control_queryable_object_kinds())
        raise ValueError(
            f"Unsupported control object kind: {normalized_kind or '<empty>'}. Supported kinds: {supported}."
        )

    safe_limit = max(1, min(200, int(limit or 20)))
    safe_offset = max(0, int(offset or 0))
    where_clauses: list[str] = []
    params: list[Any] = []

    if maybe_text(run_id):
        where_clauses.append("run_id = ?")
        params.append(maybe_text(run_id))
    if maybe_text(round_id):
        where_clauses.append("round_id = ?")
        params.append(maybe_text(round_id))

    status_column = maybe_text(config.get("status_column"))
    status_text = maybe_text(status)
    if status_text:
        if not status_column:
            raise _unsupported_filter_error(
                object_kind=normalized_kind,
                filter_name="status",
            )
        where_clauses.append(f"{status_column} = ?")
        params.append(status_text)

    filter_values = {
        "controller_status": maybe_text(controller_status),
        "gate_status": maybe_text(gate_status),
        "report_basis_status": maybe_text(report_basis_status),
        "supervisor_status": maybe_text(supervisor_status),
        "planning_mode": maybe_text(planning_mode),
        "controller_authority": maybe_text(controller_authority),
        "plan_source": maybe_text(plan_source),
        "plan_id": maybe_text(plan_id),
        "plan_step_group": maybe_text(plan_step_group),
        "phase_group": maybe_text(phase_group),
        "readiness_status": maybe_text(readiness_status),
        "current_stage": maybe_text(current_stage),
        "failed_stage": maybe_text(failed_stage),
        "resume_status": maybe_text(resume_status),
        "stage_name": maybe_text(stage_name),
        "stage_kind": maybe_text(stage_kind),
        "skill_name": maybe_text(skill_name),
        "assigned_role_hint": maybe_text(assigned_role_hint),
        "gate_handler": maybe_text(gate_handler),
        "decision_source": maybe_text(decision_source),
        "supervisor_substatus": maybe_text(supervisor_substatus),
        "phase2_posture": maybe_text(phase2_posture),
        "terminal_state": maybe_text(terminal_state),
        "reporting_handoff_status": maybe_text(reporting_handoff_status),
        "transition_kind": maybe_text(transition_kind),
        "requested_by_role": maybe_text(requested_by_role),
        "requested_actor_role": maybe_text(requested_actor_role),
        "request_id": maybe_text(request_id),
        "target_round_id": maybe_text(target_round_id),
        "requested_command_name": maybe_text(requested_command_name),
        "latest_decision_status": maybe_text(latest_decision_status),
        "latest_decision_by_role": maybe_text(latest_decision_by_role),
        "decision_by_role": maybe_text(decision_by_role),
    }
    filter_columns = (
        config.get("filter_columns", {})
        if isinstance(config.get("filter_columns"), dict)
        else {}
    )
    for filter_name, value in filter_values.items():
        if not value:
            continue
        column_name = maybe_text(filter_columns.get(filter_name))
        if not column_name:
            raise _unsupported_filter_error(
                object_kind=normalized_kind,
                filter_name=filter_name,
            )
        where_clauses.append(f"{column_name} = ?")
        params.append(value)

    if reporting_ready_only:
        column_name = maybe_text(filter_columns.get("reporting_ready"))
        if not column_name:
            raise _unsupported_filter_error(
                object_kind=normalized_kind,
                filter_name="reporting_ready",
            )
        where_clauses.append(f"{column_name} = ?")
        params.append(1)

    connection, db_file = connect_db(run_dir)
    try:
        matching_count, objects = fetch_json_rows(
            connection,
            table_name=maybe_text(config.get("table_name")),
            where_clauses=where_clauses,
            params=params,
            order_by=maybe_text(config.get("order_by")),
            limit=safe_limit,
            offset=safe_offset,
        )
    finally:
        connection.close()

    result: dict[str, Any] = {
        "schema_version": "control-object-query-v1",
        "status": "completed",
        "plane": PLANE_RUNTIME,
        "object_kind": normalized_kind,
        "summary": {
            "db_path": str(db_file),
            "matching_object_count": matching_count,
            "returned_object_count": len(objects),
        },
        "filters": {
            "run_id": maybe_text(run_id),
            "round_id": maybe_text(round_id),
            "status": status_text,
            "controller_status": maybe_text(controller_status),
            "gate_status": maybe_text(gate_status),
            "report_basis_status": maybe_text(report_basis_status),
            "supervisor_status": maybe_text(supervisor_status),
            "planning_mode": maybe_text(planning_mode),
            "readiness_status": maybe_text(readiness_status),
            "current_stage": maybe_text(current_stage),
            "failed_stage": maybe_text(failed_stage),
            "resume_status": maybe_text(resume_status),
            "stage_name": maybe_text(stage_name),
            "gate_handler": maybe_text(gate_handler),
            "decision_source": maybe_text(decision_source),
            "supervisor_substatus": maybe_text(supervisor_substatus),
            "phase2_posture": maybe_text(phase2_posture),
            "terminal_state": maybe_text(terminal_state),
            "reporting_handoff_status": maybe_text(reporting_handoff_status),
            "transition_kind": maybe_text(transition_kind),
            "requested_by_role": maybe_text(requested_by_role),
            "requested_actor_role": maybe_text(requested_actor_role),
            "request_id": maybe_text(request_id),
            "target_round_id": maybe_text(target_round_id),
            "requested_command_name": maybe_text(requested_command_name),
            "latest_decision_status": maybe_text(latest_decision_status),
            "latest_decision_by_role": maybe_text(latest_decision_by_role),
            "decision_by_role": maybe_text(decision_by_role),
            "reporting_ready_only": bool(reporting_ready_only),
        },
        "paging": {
            "limit": safe_limit,
            "offset": safe_offset,
            "returned_count": len(objects),
            "matching_count": matching_count,
        },
        "objects": objects,
    }
    if include_contract:
        result["contract"] = canonical_contract(normalized_kind).as_dict()
    return result


__all__ = [
    "OBJECT_KIND_CONTROLLER_STATE",
    "OBJECT_KIND_GATE_STATE",
    "OBJECT_KIND_RUNTIME_CONTROL_FREEZE",
    "OBJECT_KIND_SUPERVISOR_STATE",
    "OBJECT_KIND_TRANSITION_APPROVAL",
    "OBJECT_KIND_TRANSITION_REJECTION",
    "OBJECT_KIND_TRANSITION_REQUEST",
    "OBJECT_KIND_SKILL_APPROVAL",
    "OBJECT_KIND_SKILL_APPROVAL_CONSUMPTION",
    "OBJECT_KIND_SKILL_APPROVAL_REJECTION",
    "OBJECT_KIND_SKILL_APPROVAL_REQUEST",
    "control_queryable_object_kinds",
    "query_control_objects",
]
