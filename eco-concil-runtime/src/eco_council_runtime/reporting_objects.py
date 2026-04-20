from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .canonical_contracts import (
    PLANE_REPORTING,
    canonical_contract,
    canonical_contract_kinds,
)
from .kernel.deliberation_plane import (
    connect_db as connect_deliberation_db,
    decode_json,
    maybe_text,
)

OBJECT_KIND_REPORTING_HANDOFF = "reporting-handoff"
OBJECT_KIND_COUNCIL_DECISION = "council-decision"
OBJECT_KIND_EXPERT_REPORT = "expert-report"
OBJECT_KIND_FINAL_PUBLICATION = "final-publication"

REPORTING_STAGE_VALUES = {"draft", "canonical"}

QUERY_CONFIGS: dict[str, dict[str, Any]] = {
    OBJECT_KIND_REPORTING_HANDOFF: {
        "table_name": "reporting_handoffs",
        "id_column": "handoff_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, handoff_id DESC",
        "agent_role_column": "",
        "status_column": "handoff_status",
        "decision_id_column": "",
        "stage_column": "",
    },
    OBJECT_KIND_COUNCIL_DECISION: {
        "table_name": "council_decision_records",
        "id_column": "record_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, record_id DESC",
        "agent_role_column": "",
        "status_column": "moderator_status",
        "decision_id_column": "decision_id",
        "stage_column": "decision_stage",
    },
    OBJECT_KIND_EXPERT_REPORT: {
        "table_name": "expert_report_records",
        "id_column": "record_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, record_id DESC",
        "agent_role_column": "agent_role",
        "status_column": "status",
        "decision_id_column": "",
        "stage_column": "report_stage",
    },
    OBJECT_KIND_FINAL_PUBLICATION: {
        "table_name": "final_publications",
        "id_column": "publication_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, publication_id DESC",
        "agent_role_column": "",
        "status_column": "publication_status",
        "decision_id_column": "",
        "stage_column": "",
    },
}


def connect_db(run_dir: str | Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    return connect_deliberation_db(run_dir_path, db_path)


def reporting_queryable_object_kinds() -> list[str]:
    target_kinds = set(canonical_contract_kinds(plane=PLANE_REPORTING))
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

    query = f"SELECT raw_json FROM {table_name}"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    rows = connection.execute(query, tuple([*params, limit, offset])).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if isinstance(payload, dict):
            results.append(payload)
    return matching_count, results


def _unsupported_filter_error(
    *,
    object_kind: str,
    filter_name: str,
    supported_kinds: list[str],
) -> ValueError:
    supported = ", ".join(supported_kinds)
    return ValueError(
        f"Unsupported {filter_name} filter for reporting object kind: {object_kind}. "
        f"Supported kinds: {supported}."
    )


def query_reporting_objects(
    run_dir: str | Path,
    *,
    object_kind: str,
    run_id: str = "",
    round_id: str = "",
    agent_role: str = "",
    status: str = "",
    decision_id: str = "",
    stage: str = "",
    include_contract: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    normalized_kind = maybe_text(object_kind)
    config = QUERY_CONFIGS.get(normalized_kind)
    if config is None:
        supported = ", ".join(reporting_queryable_object_kinds())
        raise ValueError(
            f"Unsupported reporting object kind: {normalized_kind or '<empty>'}. Supported kinds: {supported}."
        )

    normalized_stage = maybe_text(stage)
    if normalized_stage and normalized_stage not in REPORTING_STAGE_VALUES:
        supported_stages = ", ".join(sorted(REPORTING_STAGE_VALUES))
        raise ValueError(
            f"Unsupported reporting stage: {normalized_stage}. Supported stages: {supported_stages}."
        )

    safe_limit = max(1, min(200, int(limit or 20)))
    safe_offset = max(0, int(offset or 0))
    where_clauses: list[str] = []
    params: list[str] = []

    if maybe_text(run_id):
        where_clauses.append("run_id = ?")
        params.append(maybe_text(run_id))
    if maybe_text(round_id):
        where_clauses.append("round_id = ?")
        params.append(maybe_text(round_id))

    agent_role_text = maybe_text(agent_role)
    agent_role_column = maybe_text(config.get("agent_role_column"))
    if agent_role_text:
        if not agent_role_column:
            raise _unsupported_filter_error(
                object_kind=normalized_kind,
                filter_name="agent_role",
                supported_kinds=[OBJECT_KIND_EXPERT_REPORT],
            )
        where_clauses.append(f"{agent_role_column} = ?")
        params.append(agent_role_text)

    status_text = maybe_text(status)
    status_column = maybe_text(config.get("status_column"))
    if status_text:
        where_clauses.append(f"{status_column} = ?")
        params.append(status_text)

    decision_id_text = maybe_text(decision_id)
    decision_id_column = maybe_text(config.get("decision_id_column"))
    if decision_id_text:
        if not decision_id_column:
            raise _unsupported_filter_error(
                object_kind=normalized_kind,
                filter_name="decision_id",
                supported_kinds=[OBJECT_KIND_COUNCIL_DECISION],
            )
        where_clauses.append(f"{decision_id_column} = ?")
        params.append(decision_id_text)

    stage_column = maybe_text(config.get("stage_column"))
    if normalized_stage:
        if not stage_column:
            raise _unsupported_filter_error(
                object_kind=normalized_kind,
                filter_name="stage",
                supported_kinds=[
                    OBJECT_KIND_COUNCIL_DECISION,
                    OBJECT_KIND_EXPERT_REPORT,
                ],
            )
        where_clauses.append(f"{stage_column} = ?")
        params.append(normalized_stage)

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
        "schema_version": "reporting-object-query-v1",
        "status": "completed",
        "plane": PLANE_REPORTING,
        "object_kind": normalized_kind,
        "summary": {
            "db_path": str(db_file),
            "matching_object_count": matching_count,
            "returned_object_count": len(objects),
        },
        "filters": {
            "run_id": maybe_text(run_id),
            "round_id": maybe_text(round_id),
            "agent_role": agent_role_text,
            "status": status_text,
            "decision_id": decision_id_text,
            "stage": normalized_stage,
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
    "OBJECT_KIND_COUNCIL_DECISION",
    "OBJECT_KIND_EXPERT_REPORT",
    "OBJECT_KIND_FINAL_PUBLICATION",
    "OBJECT_KIND_REPORTING_HANDOFF",
    "query_reporting_objects",
    "reporting_queryable_object_kinds",
]
