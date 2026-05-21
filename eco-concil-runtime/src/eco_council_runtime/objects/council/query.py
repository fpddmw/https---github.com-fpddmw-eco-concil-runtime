from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.contracts import (
    PLANE_DELIBERATION,
    canonical_contract,
    canonical_contract_kinds,
)
from .schema import connect_db
from .payloads import (
    DYNAMIC_INVESTIGATION_OBJECT_KINDS,
    OBJECT_KIND_BOARD_TASK,
    OBJECT_KIND_CHALLENGE,
    OBJECT_KIND_CHALLENGE_DISPOSITION,
    OBJECT_KIND_CONTEXT_PACKET,
    OBJECT_KIND_DECISION_TRACE,
    OBJECT_KIND_DISCUSSION_MESSAGE,
    OBJECT_KIND_EVIDENCE_BUNDLE,
    OBJECT_KIND_EVIDENCE_REQUEST,
    OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT,
    OBJECT_KIND_FINDING,
    OBJECT_KIND_COUNCIL_INVESTIGATION_PROGRAM,
    OBJECT_KIND_HYPOTHESIS,
    OBJECT_KIND_INVESTIGATION_THEME,
    OBJECT_KIND_INVESTIGATION_PLAN,
    OBJECT_KIND_INVESTIGATION_SCOPE,
    OBJECT_KIND_NEXT_ACTION,
    OBJECT_KIND_PROBE,
    OBJECT_KIND_PROPOSAL,
    OBJECT_KIND_READINESS_ASSESSMENT,
    OBJECT_KIND_READINESS_OPINION,
    OBJECT_KIND_REPORT_BASIS_FREEZE,
    OBJECT_KIND_REPORT_BLUEPRINT,
    OBJECT_KIND_REVIEW_COMMENT,
    OBJECT_KIND_ROUND_BRIEF,
    OBJECT_KIND_ROUND_SYNTHESIS,
    OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL,
    OBJECT_KIND_AGENT_POSITION,
    OBJECT_KIND_SUBISSUE,
    OBJECT_KIND_THEME_EVIDENCE_BOUNDARY_PLAN,
    OBJECT_KIND_THEME_PROGRESS_REVIEW,
)
from eco_council_runtime.deliberation_target_semantics import canonical_target_kind
from eco_council_runtime.kernel.planes.deliberation_plane import maybe_text, payload_from_db_row


QUERY_CONFIGS: dict[str, dict[str, Any]] = {
    OBJECT_KIND_PROPOSAL: {
        "table_name": "council_proposals",
        "id_column": "proposal_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, proposal_id DESC",
        "agent_role_column": "agent_role",
        "status_column": "status",
        "decision_id_column": "",
        "filter_columns": {
            "target_kind": "target_kind",
            "target_id": "target_id",
        },
    },
    OBJECT_KIND_FINDING: {
        "table_name": "finding_records",
        "id_column": "finding_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, finding_id DESC",
        "agent_role_column": "agent_role",
        "status_column": "status",
        "decision_id_column": "",
        "filter_columns": {
            "target_kind": "target_kind",
            "target_id": "target_id",
        },
    },
    OBJECT_KIND_DISCUSSION_MESSAGE: {
        "table_name": "discussion_messages",
        "id_column": "message_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, message_id DESC",
        "agent_role_column": "author_role",
        "status_column": "status",
        "decision_id_column": "",
        "filter_columns": {
            "target_kind": "target_kind",
            "target_id": "target_id",
        },
    },
    OBJECT_KIND_EVIDENCE_BUNDLE: {
        "table_name": "evidence_bundles",
        "id_column": "bundle_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, bundle_id DESC",
        "agent_role_column": "agent_role",
        "status_column": "status",
        "decision_id_column": "",
        "filter_columns": {
            "target_kind": "target_kind",
            "target_id": "target_id",
        },
    },
    OBJECT_KIND_REVIEW_COMMENT: {
        "table_name": "review_comments",
        "id_column": "comment_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, comment_id DESC",
        "agent_role_column": "author_role",
        "status_column": "status",
        "decision_id_column": "",
        "filter_columns": {
            "target_kind": "target_kind",
            "target_id": "target_id",
        },
    },
    OBJECT_KIND_HYPOTHESIS: {
        "table_name": "hypothesis_cards",
        "id_column": "hypothesis_id",
        "timestamp_column": "updated_at_utc",
        "order_by": "updated_at_utc DESC, hypothesis_id DESC",
        "agent_role_column": "owner_role",
        "status_column": "status",
        "decision_id_column": "",
    },
    OBJECT_KIND_CHALLENGE: {
        "table_name": "challenge_tickets",
        "id_column": "ticket_id",
        "timestamp_column": "created_at_utc",
        "order_by": "created_at_utc DESC, ticket_id DESC",
        "agent_role_column": "owner_role",
        "status_column": "status",
        "decision_id_column": "",
    },
    OBJECT_KIND_BOARD_TASK: {
        "table_name": "board_tasks",
        "id_column": "task_id",
        "timestamp_column": "updated_at_utc",
        "order_by": "updated_at_utc DESC, task_id DESC",
        "agent_role_column": "owner_role",
        "status_column": "status",
        "decision_id_column": "",
    },
    OBJECT_KIND_NEXT_ACTION: {
        "table_name": "moderator_actions",
        "id_column": "action_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, action_rank ASC, action_id ASC",
        "agent_role_column": "assigned_role",
        "status_column": "",
        "decision_id_column": "",
        "readiness_blocker_column": "readiness_blocker",
        "filter_columns": {
            "target_kind": "target_object_kind",
            "target_id": "target_object_id",
            "issue_label": "issue_label",
            "route_id": "target_route_id",
            "actor_id": "target_actor_id",
            "assessment_id": "target_assessment_id",
            "linkage_id": "target_linkage_id",
            "gap_id": "target_gap_id",
            "proposal_id": "target_proposal_id",
            "source_proposal_id": "source_proposal_id",
        },
    },
    OBJECT_KIND_PROBE: {
        "table_name": "falsification_probes",
        "id_column": "probe_id",
        "timestamp_column": "opened_at_utc",
        "order_by": "opened_at_utc DESC, probe_id DESC",
        "agent_role_column": "owner_role",
        "status_column": "probe_status",
        "decision_id_column": "",
        "filter_columns": {
            "target_kind": "target_object_kind",
            "target_id": "target_object_id",
            "issue_label": "issue_label",
            "route_id": "target_route_id",
            "actor_id": "target_actor_id",
            "assessment_id": "target_assessment_id",
            "linkage_id": "target_linkage_id",
            "gap_id": "target_gap_id",
            "proposal_id": "target_proposal_id",
            "source_proposal_id": "source_proposal_id",
        },
    },
    OBJECT_KIND_READINESS_OPINION: {
        "table_name": "readiness_opinions",
        "id_column": "opinion_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, opinion_id DESC",
        "agent_role_column": "agent_role",
        "status_column": "readiness_status",
        "decision_id_column": "",
    },
    OBJECT_KIND_READINESS_ASSESSMENT: {
        "table_name": "round_readiness_assessments",
        "id_column": "readiness_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, readiness_id DESC",
        "agent_role_column": "",
        "status_column": "readiness_status",
        "decision_id_column": "",
    },
    OBJECT_KIND_REPORT_BASIS_FREEZE: {
        "table_name": "report_basis_freeze_records",
        "id_column": "basis_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, basis_id DESC",
        "agent_role_column": "",
        "status_column": "report_basis_status",
        "decision_id_column": "",
        "item_loader": "report-basis-freeze-items",
    },
    OBJECT_KIND_DECISION_TRACE: {
        "table_name": "decision_traces",
        "id_column": "trace_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, trace_id DESC",
        "agent_role_column": "",
        "status_column": "status",
        "decision_id_column": "decision_id",
    },
}

_DYNAMIC_INVESTIGATION_QUERY_OBJECT_KINDS = (
    OBJECT_KIND_REPORT_BLUEPRINT,
    OBJECT_KIND_INVESTIGATION_THEME,
    OBJECT_KIND_COUNCIL_INVESTIGATION_PROGRAM,
    OBJECT_KIND_THEME_EVIDENCE_BOUNDARY_PLAN,
    OBJECT_KIND_THEME_PROGRESS_REVIEW,
    OBJECT_KIND_INVESTIGATION_PLAN,
    OBJECT_KIND_SUBISSUE,
    OBJECT_KIND_INVESTIGATION_SCOPE,
    OBJECT_KIND_ROUND_BRIEF,
    OBJECT_KIND_ROUND_SYNTHESIS,
    OBJECT_KIND_EVIDENCE_REQUEST,
    OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL,
    OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT,
    OBJECT_KIND_AGENT_POSITION,
    OBJECT_KIND_CHALLENGE_DISPOSITION,
    OBJECT_KIND_CONTEXT_PACKET,
)

for _dynamic_object_kind in _DYNAMIC_INVESTIGATION_QUERY_OBJECT_KINDS:
    if _dynamic_object_kind not in DYNAMIC_INVESTIGATION_OBJECT_KINDS:
        continue
    filter_columns = {
        "target_kind": "target_kind",
        "target_id": "target_id",
    }
    if _dynamic_object_kind == OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL:
        filter_columns.update(
            {
                "source_skill": "json_extract(raw_json, '$.source_skill')",
                "target_evidence_request_id": (
                    "json_extract(raw_json, '$.target_evidence_request_id')"
                ),
            }
        )
    if _dynamic_object_kind == OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT:
        filter_columns.update(
            {
                "assessment_type": "json_extract(raw_json, '$.assessment_type')",
                "route_judgment": "json_extract(raw_json, '$.route_judgment')",
                "source_surface_status": (
                    "json_extract(raw_json, '$.source_surface_status')"
                ),
            }
        )
    QUERY_CONFIGS[_dynamic_object_kind] = {
        "table_name": "dynamic_investigation_objects",
        "id_column": "object_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, object_id DESC",
        "agent_role_column": "author_role",
        "status_column": "status",
        "decision_id_column": "",
        "object_kind_column": "object_kind",
        "object_kind_value": _dynamic_object_kind,
        "filter_columns": filter_columns,
    }


def council_queryable_object_kinds() -> list[str]:
    target_kinds = set(canonical_contract_kinds(plane=PLANE_DELIBERATION))
    return sorted(object_kind for object_kind in QUERY_CONFIGS if object_kind in target_kinds)


def fetch_json_rows(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    where_clauses: list[str],
    params: list[str],
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
    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(payload_from_db_row(row))
    return matching_count, results


def load_report_basis_freeze_items_for_record(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    round_id: str,
    basis_id: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM report_basis_freeze_items
        WHERE run_id = ? AND round_id = ? AND basis_id = ?
        ORDER BY item_group, item_index, item_row_id
        """,
        (run_id, round_id, basis_id),
    ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(payload_from_db_row(row))
    return results


def add_supported_filter(
    *,
    config: dict[str, Any],
    filter_name: str,
    filter_value: str,
    object_kind: str,
    where_clauses: list[str],
    params: list[str],
) -> None:
    value_text = maybe_text(filter_value)
    if filter_name == "target_kind":
        value_text = canonical_target_kind(value_text)
    if not value_text:
        return
    filter_columns = (
        config.get("filter_columns", {})
        if isinstance(config.get("filter_columns"), dict)
        else {}
    )
    column_name = maybe_text(filter_columns.get(filter_name))
    if not column_name:
        raise ValueError(
            f"Unsupported {filter_name} filter for object kind: {object_kind}."
        )
    where_clauses.append(f"{column_name} = ?")
    params.append(value_text)


def query_council_objects(
    run_dir: str | Path,
    *,
    object_kind: str,
    run_id: str = "",
    round_id: str = "",
    agent_role: str = "",
    status: str = "",
    decision_id: str = "",
    target_kind: str = "",
    target_id: str = "",
    issue_label: str = "",
    route_id: str = "",
    actor_id: str = "",
    assessment_id: str = "",
    linkage_id: str = "",
    gap_id: str = "",
    proposal_id: str = "",
    source_proposal_id: str = "",
    source_skill: str = "",
    target_evidence_request_id: str = "",
    readiness_blocker_only: bool = False,
    include_contract: bool = False,
    include_items: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    normalized_kind = maybe_text(object_kind)
    config = QUERY_CONFIGS.get(normalized_kind)
    if config is None:
        supported = ", ".join(council_queryable_object_kinds())
        raise ValueError(
            f"Unsupported council object kind: {normalized_kind or '<empty>'}. Supported kinds: {supported}."
        )
    safe_limit = max(1, min(200, int(limit or 20)))
    safe_offset = max(0, int(offset or 0))
    where_clauses: list[str] = []
    params: list[str] = []
    object_kind_column = maybe_text(config.get("object_kind_column"))
    if object_kind_column:
        where_clauses.append(f"{object_kind_column} = ?")
        params.append(maybe_text(config.get("object_kind_value")) or normalized_kind)
    if maybe_text(run_id):
        where_clauses.append("run_id = ?")
        params.append(maybe_text(run_id))
    if maybe_text(round_id):
        where_clauses.append("round_id = ?")
        params.append(maybe_text(round_id))
    agent_role_column = maybe_text(config.get("agent_role_column"))
    if agent_role_column and maybe_text(agent_role):
        where_clauses.append(f"{agent_role_column} = ?")
        params.append(maybe_text(agent_role))
    status_column = maybe_text(config.get("status_column"))
    if status_column and maybe_text(status):
        where_clauses.append(f"{status_column} = ?")
        params.append(maybe_text(status))
    decision_id_column = maybe_text(config.get("decision_id_column"))
    if decision_id_column and maybe_text(decision_id):
        where_clauses.append(f"{decision_id_column} = ?")
        params.append(maybe_text(decision_id))
    add_supported_filter(
        config=config,
        filter_name="target_kind",
        filter_value=target_kind,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="target_id",
        filter_value=target_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="issue_label",
        filter_value=issue_label,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="route_id",
        filter_value=route_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="actor_id",
        filter_value=actor_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="assessment_id",
        filter_value=assessment_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="linkage_id",
        filter_value=linkage_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="gap_id",
        filter_value=gap_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="proposal_id",
        filter_value=proposal_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="source_proposal_id",
        filter_value=source_proposal_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="source_skill",
        filter_value=source_skill,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    add_supported_filter(
        config=config,
        filter_name="target_evidence_request_id",
        filter_value=target_evidence_request_id,
        object_kind=normalized_kind,
        where_clauses=where_clauses,
        params=params,
    )
    if readiness_blocker_only:
        blocker_column = maybe_text(config.get("readiness_blocker_column"))
        if not blocker_column:
            raise ValueError(
                f"Unsupported readiness_blocker filter for object kind: {normalized_kind}."
            )
        where_clauses.append(f"{blocker_column} = 1")

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
        if include_items and maybe_text(config.get("item_loader")) == "report-basis-freeze-items":
            for payload in objects:
                payload["basis_items"] = load_report_basis_freeze_items_for_record(
                    connection,
                    run_id=maybe_text(payload.get("run_id")),
                    round_id=maybe_text(payload.get("round_id")),
                    basis_id=maybe_text(payload.get("basis_id")),
                )
    finally:
        connection.close()

    result: dict[str, Any] = {
        "schema_version": "council-object-query-v1",
        "status": "completed",
        "object_kind": normalized_kind,
        "summary": {
            "db_path": str(db_file),
            "matching_object_count": matching_count,
            "returned_object_count": len(objects),
        },
        "filters": {
            "run_id": maybe_text(run_id),
            "round_id": maybe_text(round_id),
            "agent_role": maybe_text(agent_role),
            "status": maybe_text(status),
            "decision_id": maybe_text(decision_id),
            "target_kind": maybe_text(target_kind),
            "target_id": maybe_text(target_id),
            "issue_label": maybe_text(issue_label),
            "route_id": maybe_text(route_id),
            "actor_id": maybe_text(actor_id),
            "assessment_id": maybe_text(assessment_id),
            "linkage_id": maybe_text(linkage_id),
            "gap_id": maybe_text(gap_id),
            "proposal_id": maybe_text(proposal_id),
            "source_proposal_id": maybe_text(source_proposal_id),
            "source_skill": maybe_text(source_skill),
            "target_evidence_request_id": maybe_text(target_evidence_request_id),
            "readiness_blocker_only": bool(readiness_blocker_only),
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


__all__ = (
    "QUERY_CONFIGS",
    "council_queryable_object_kinds",
    "fetch_json_rows",
    "load_report_basis_freeze_items_for_record",
    "add_supported_filter",
    "query_council_objects",
)
