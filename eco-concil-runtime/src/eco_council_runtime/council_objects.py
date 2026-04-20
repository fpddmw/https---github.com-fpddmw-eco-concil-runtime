from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .canonical_contracts import (
    PLANE_DELIBERATION,
    canonical_contract,
    canonical_contract_kinds,
    validate_canonical_payload,
)
from .kernel.deliberation_plane import (
    connect_db as connect_deliberation_db,
    decode_json,
    json_text,
    maybe_text,
    stable_hash,
    utc_now_iso,
)

OBJECT_KIND_PROPOSAL = "proposal"
OBJECT_KIND_HYPOTHESIS = "hypothesis"
OBJECT_KIND_CHALLENGE = "challenge"
OBJECT_KIND_BOARD_TASK = "board-task"
OBJECT_KIND_NEXT_ACTION = "next-action"
OBJECT_KIND_PROBE = "probe"
OBJECT_KIND_READINESS_OPINION = "readiness-opinion"
OBJECT_KIND_READINESS_ASSESSMENT = "readiness-assessment"
OBJECT_KIND_PROMOTION_BASIS = "promotion-basis"
OBJECT_KIND_DECISION_TRACE = "decision-trace"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS council_proposals (
    proposal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    proposal_kind TEXT NOT NULL DEFAULT '',
    agent_role TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    target_kind TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    confidence REAL,
    rationale TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    response_to_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_council_proposals_round
ON council_proposals(run_id, round_id, generated_at_utc, proposal_id);
CREATE INDEX IF NOT EXISTS idx_council_proposals_role_status
ON council_proposals(run_id, round_id, agent_role, status, proposal_id);

CREATE TABLE IF NOT EXISTS readiness_opinions (
    opinion_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    agent_role TEXT NOT NULL DEFAULT '',
    opinion_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    sufficient_for_promotion INTEGER NOT NULL DEFAULT 0,
    confidence REAL,
    rationale TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_readiness_opinions_round
ON readiness_opinions(run_id, round_id, generated_at_utc, opinion_id);
CREATE INDEX IF NOT EXISTS idx_readiness_opinions_role_status
ON readiness_opinions(run_id, round_id, agent_role, readiness_status, opinion_id);

CREATE TABLE IF NOT EXISTS decision_traces (
    trace_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    decision_kind TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    selected_object_kind TEXT NOT NULL DEFAULT '',
    selected_object_id TEXT NOT NULL DEFAULT '',
    confidence REAL,
    rationale TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    accepted_object_ids_json TEXT NOT NULL DEFAULT '[]',
    rejected_object_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_decision_traces_round
ON decision_traces(run_id, round_id, generated_at_utc, trace_id);
CREATE INDEX IF NOT EXISTS idx_decision_traces_decision
ON decision_traces(run_id, round_id, decision_id, status, trace_id);
"""


QUERY_CONFIGS: dict[str, dict[str, Any]] = {
    OBJECT_KIND_PROPOSAL: {
        "table_name": "council_proposals",
        "id_column": "proposal_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, proposal_id DESC",
        "agent_role_column": "agent_role",
        "status_column": "status",
        "decision_id_column": "",
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
    },
    OBJECT_KIND_PROBE: {
        "table_name": "falsification_probes",
        "id_column": "probe_id",
        "timestamp_column": "opened_at_utc",
        "order_by": "opened_at_utc DESC, probe_id DESC",
        "agent_role_column": "owner_role",
        "status_column": "probe_status",
        "decision_id_column": "",
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
    OBJECT_KIND_PROMOTION_BASIS: {
        "table_name": "promotion_basis_records",
        "id_column": "basis_id",
        "timestamp_column": "generated_at_utc",
        "order_by": "generated_at_utc DESC, basis_id DESC",
        "agent_role_column": "",
        "status_column": "promotion_status",
        "decision_id_column": "",
        "item_loader": "promotion-basis-items",
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


def connect_db(run_dir: str | Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    connection, db_file = connect_deliberation_db(run_dir_path, db_path)
    connection.executescript(SCHEMA_SQL)
    return connection, db_file


def council_queryable_object_kinds() -> list[str]:
    target_kinds = set(canonical_contract_kinds(plane=PLANE_DELIBERATION))
    return sorted(object_kind for object_kind in QUERY_CONFIGS if object_kind in target_kinds)


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


def normalized_evidence_refs(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalized_lineage(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalized_provenance(
    value: Any,
    *,
    decision_source: str,
) -> dict[str, Any]:
    if isinstance(value, dict):
        normalized = dict(value)
    else:
        normalized = {}
    if decision_source and "decision_source" not in normalized:
        normalized["decision_source"] = decision_source
    return normalized


def proposal_id(
    run_id: str,
    round_id: str,
    proposal_kind: str,
    agent_role: str,
    proposal_index: int,
    rationale: str,
) -> str:
    return "proposal-" + stable_hash(
        "council-proposal",
        run_id,
        round_id,
        proposal_kind,
        agent_role,
        proposal_index,
        rationale,
    )[:12]


def readiness_opinion_id(
    run_id: str,
    round_id: str,
    agent_role: str,
    readiness_status: str,
    opinion_index: int,
) -> str:
    return "readiness-opinion-" + stable_hash(
        "readiness-opinion",
        run_id,
        round_id,
        agent_role,
        readiness_status,
        opinion_index,
    )[:12]


def decision_trace_id(
    run_id: str,
    round_id: str,
    decision_id: str,
    trace_index: int,
) -> str:
    return "decision-trace-" + stable_hash(
        "decision-trace",
        run_id,
        round_id,
        decision_id,
        trace_index,
    )[:12]


def normalized_proposal_payload(
    proposal: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    proposal_index: int,
) -> dict[str, Any]:
    normalized = dict(proposal)
    target = normalized.get("target", {}) if isinstance(normalized.get("target"), dict) else {}
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-council"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["proposal_kind"] = (
        maybe_text(normalized.get("proposal_kind")) or "general-proposal"
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "moderator"
    normalized["status"] = maybe_text(normalized.get("status")) or "open"
    normalized["target_kind"] = (
        maybe_text(normalized.get("target_kind"))
        or maybe_text(target.get("object_kind"))
        or maybe_text(target.get("kind"))
    )
    normalized["target_id"] = (
        maybe_text(normalized.get("target_id"))
        or maybe_text(target.get("object_id"))
        or maybe_text(target.get("id"))
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale"))
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_lineage(normalized.get("lineage"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["response_to_ids"] = unique_texts(
        normalized.get("response_to_ids", [])
        if isinstance(normalized.get("response_to_ids"), list)
        else []
    )
    normalized["proposal_id"] = (
        maybe_text(normalized.get("proposal_id"))
        or proposal_id(
            normalized_run_id,
            normalized_round_id,
            maybe_text(normalized.get("proposal_kind")),
            maybe_text(normalized.get("agent_role")),
            proposal_index,
            maybe_text(normalized.get("rationale")),
        )
    )
    normalized["schema_version"] = canonical_contract(OBJECT_KIND_PROPOSAL).schema_version
    return validate_canonical_payload(OBJECT_KIND_PROPOSAL, normalized)


def normalized_readiness_opinion_payload(
    opinion: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    opinion_index: int,
) -> dict[str, Any]:
    normalized = dict(opinion)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-council"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "moderator"
    normalized["opinion_status"] = maybe_text(normalized.get("opinion_status")) or "submitted"
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "blocked"
    )
    normalized["sufficient_for_promotion"] = bool(
        normalized.get("sufficient_for_promotion")
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale"))
    normalized["decision_source"] = decision_source
    normalized["basis_object_ids"] = unique_texts(
        normalized.get("basis_object_ids", [])
        if isinstance(normalized.get("basis_object_ids"), list)
        else []
    )
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_lineage(normalized.get("lineage"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["opinion_id"] = (
        maybe_text(normalized.get("opinion_id"))
        or readiness_opinion_id(
            normalized_run_id,
            normalized_round_id,
            maybe_text(normalized.get("agent_role")),
            maybe_text(normalized.get("readiness_status")),
            opinion_index,
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_READINESS_OPINION
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_READINESS_OPINION, normalized)


def normalized_decision_trace_payload(
    trace: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    trace_index: int,
) -> dict[str, Any]:
    normalized = dict(trace)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "council-trace"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["decision_id"] = maybe_text(normalized.get("decision_id"))
    normalized["decision_kind"] = (
        maybe_text(normalized.get("decision_kind")) or "round-decision"
    )
    normalized["status"] = maybe_text(normalized.get("status")) or "recorded"
    normalized["selected_object_kind"] = maybe_text(
        normalized.get("selected_object_kind")
    )
    normalized["selected_object_id"] = maybe_text(
        normalized.get("selected_object_id")
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale"))
    normalized["decision_source"] = decision_source
    normalized["accepted_object_ids"] = unique_texts(
        normalized.get("accepted_object_ids", [])
        if isinstance(normalized.get("accepted_object_ids"), list)
        else []
    )
    normalized["rejected_object_ids"] = unique_texts(
        normalized.get("rejected_object_ids", [])
        if isinstance(normalized.get("rejected_object_ids"), list)
        else []
    )
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_lineage(normalized.get("lineage"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["trace_id"] = (
        maybe_text(normalized.get("trace_id"))
        or decision_trace_id(
            normalized_run_id,
            normalized_round_id,
            maybe_text(normalized.get("decision_id")),
            trace_index,
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_DECISION_TRACE
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_DECISION_TRACE, normalized)


def proposal_row_from_payload(
    proposal: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "proposal_id": maybe_text(proposal.get("proposal_id")),
        "run_id": maybe_text(proposal.get("run_id")),
        "round_id": maybe_text(proposal.get("round_id")),
        "generated_at_utc": maybe_text(proposal.get("generated_at_utc")),
        "proposal_kind": maybe_text(proposal.get("proposal_kind")),
        "agent_role": maybe_text(proposal.get("agent_role")),
        "status": maybe_text(proposal.get("status")),
        "target_kind": maybe_text(proposal.get("target_kind")),
        "target_id": maybe_text(proposal.get("target_id")),
        "confidence": proposal.get("confidence"),
        "rationale": maybe_text(proposal.get("rationale")),
        "decision_source": maybe_text(proposal.get("decision_source")),
        "response_to_ids_json": json_text(proposal.get("response_to_ids", [])),
        "evidence_refs_json": json_text(proposal.get("evidence_refs", [])),
        "provenance_json": json_text(proposal.get("provenance", {})),
        "lineage_json": json_text(proposal.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(proposal),
    }


def readiness_opinion_row_from_payload(
    opinion: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "opinion_id": maybe_text(opinion.get("opinion_id")),
        "run_id": maybe_text(opinion.get("run_id")),
        "round_id": maybe_text(opinion.get("round_id")),
        "generated_at_utc": maybe_text(opinion.get("generated_at_utc")),
        "agent_role": maybe_text(opinion.get("agent_role")),
        "opinion_status": maybe_text(opinion.get("opinion_status")),
        "readiness_status": maybe_text(opinion.get("readiness_status")),
        "sufficient_for_promotion": 1
        if bool(opinion.get("sufficient_for_promotion"))
        else 0,
        "confidence": opinion.get("confidence"),
        "rationale": maybe_text(opinion.get("rationale")),
        "decision_source": maybe_text(opinion.get("decision_source")),
        "basis_object_ids_json": json_text(opinion.get("basis_object_ids", [])),
        "evidence_refs_json": json_text(opinion.get("evidence_refs", [])),
        "provenance_json": json_text(opinion.get("provenance", {})),
        "lineage_json": json_text(opinion.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(opinion),
    }


def decision_trace_row_from_payload(
    trace: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "trace_id": maybe_text(trace.get("trace_id")),
        "decision_id": maybe_text(trace.get("decision_id")),
        "run_id": maybe_text(trace.get("run_id")),
        "round_id": maybe_text(trace.get("round_id")),
        "generated_at_utc": maybe_text(trace.get("generated_at_utc")),
        "decision_kind": maybe_text(trace.get("decision_kind")),
        "status": maybe_text(trace.get("status")),
        "selected_object_kind": maybe_text(trace.get("selected_object_kind")),
        "selected_object_id": maybe_text(trace.get("selected_object_id")),
        "confidence": trace.get("confidence"),
        "rationale": maybe_text(trace.get("rationale")),
        "decision_source": maybe_text(trace.get("decision_source")),
        "accepted_object_ids_json": json_text(
            trace.get("accepted_object_ids", [])
        ),
        "rejected_object_ids_json": json_text(
            trace.get("rejected_object_ids", [])
        ),
        "evidence_refs_json": json_text(trace.get("evidence_refs", [])),
        "provenance_json": json_text(trace.get("provenance", {})),
        "lineage_json": json_text(trace.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(trace),
    }


def write_council_proposal_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO council_proposals (
            proposal_id, run_id, round_id, generated_at_utc, proposal_kind,
            agent_role, status, target_kind, target_id, confidence, rationale,
            decision_source, response_to_ids_json, evidence_refs_json,
            provenance_json, lineage_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :proposal_id, :run_id, :round_id, :generated_at_utc, :proposal_kind,
            :agent_role, :status, :target_kind, :target_id, :confidence, :rationale,
            :decision_source, :response_to_ids_json, :evidence_refs_json,
            :provenance_json, :lineage_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_readiness_opinion_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO readiness_opinions (
            opinion_id, run_id, round_id, generated_at_utc, agent_role,
            opinion_status, readiness_status, sufficient_for_promotion,
            confidence, rationale, decision_source, basis_object_ids_json,
            evidence_refs_json, provenance_json, lineage_json, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :opinion_id, :run_id, :round_id, :generated_at_utc, :agent_role,
            :opinion_status, :readiness_status, :sufficient_for_promotion,
            :confidence, :rationale, :decision_source, :basis_object_ids_json,
            :evidence_refs_json, :provenance_json, :lineage_json, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_decision_trace_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO decision_traces (
            trace_id, decision_id, run_id, round_id, generated_at_utc,
            decision_kind, status, selected_object_kind, selected_object_id,
            confidence, rationale, decision_source, accepted_object_ids_json,
            rejected_object_ids_json, evidence_refs_json, provenance_json,
            lineage_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :trace_id, :decision_id, :run_id, :round_id, :generated_at_utc,
            :decision_kind, :status, :selected_object_kind, :selected_object_id,
            :confidence, :rationale, :decision_source, :accepted_object_ids_json,
            :rejected_object_ids_json, :evidence_refs_json, :provenance_json,
            :lineage_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def next_round_object_index(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    run_id: str,
    round_id: str,
) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) AS row_count FROM {table_name} WHERE run_id = ? AND round_id = ?",
        (maybe_text(run_id), maybe_text(round_id)),
    ).fetchone()
    return int(row["row_count"]) if row is not None else 0


def append_council_proposal_record(
    run_dir: str | Path,
    *,
    proposal_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.proposal",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(proposal_payload) if isinstance(proposal_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            proposal_index = next_round_object_index(
                connection,
                table_name="council_proposals",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_proposal_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                proposal_index=proposal_index,
            )
            write_council_proposal_row(
                connection,
                proposal_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "council-proposal-append-v1",
        "db_path": str(db_file),
        "proposal": normalized,
    }


def append_readiness_opinion_record(
    run_dir: str | Path,
    *,
    opinion_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.opinion",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(opinion_payload) if isinstance(opinion_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            opinion_index = next_round_object_index(
                connection,
                table_name="readiness_opinions",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_readiness_opinion_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                opinion_index=opinion_index,
            )
            write_readiness_opinion_row(
                connection,
                readiness_opinion_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "readiness-opinion-append-v1",
        "db_path": str(db_file),
        "opinion": normalized,
    }


def store_council_proposal_records(
    run_dir: str | Path,
    *,
    proposal_bundle: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    bundle = dict(proposal_bundle) if isinstance(proposal_bundle, dict) else {}
    proposals = bundle.get("proposals", []) if isinstance(bundle.get("proposals"), list) else []
    run_id = maybe_text(bundle.get("run_id"))
    round_id = maybe_text(bundle.get("round_id"))
    normalized_proposals = [
        normalized_proposal_payload(
            proposal,
            run_id=run_id,
            round_id=round_id,
            proposal_index=index,
        )
        for index, proposal in enumerate(proposals)
        if isinstance(proposal, dict)
    ]
    if normalized_proposals:
        run_id = maybe_text(run_id) or maybe_text(normalized_proposals[0].get("run_id"))
        round_id = maybe_text(round_id) or maybe_text(normalized_proposals[0].get("round_id"))
    bundle["schema_version"] = "council-proposal-bundle-v1"
    bundle["run_id"] = run_id
    bundle["round_id"] = round_id
    bundle["generated_at_utc"] = (
        maybe_text(bundle.get("generated_at_utc"))
        or maybe_text(normalized_proposals[-1].get("generated_at_utc")) if normalized_proposals else utc_now_iso()
    )
    bundle["proposals"] = normalized_proposals
    bundle["proposal_count"] = len(normalized_proposals)
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM council_proposals WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, proposal in enumerate(normalized_proposals):
                write_council_proposal_row(
                    connection,
                    proposal_row_from_payload(
                        proposal,
                        artifact_path=artifact_path,
                        record_locator=f"$.proposals[{index}]",
                    ),
                )
    finally:
        connection.close()
    return bundle


def store_readiness_opinion_records(
    run_dir: str | Path,
    *,
    opinion_bundle: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    bundle = dict(opinion_bundle) if isinstance(opinion_bundle, dict) else {}
    opinions = bundle.get("opinions", []) if isinstance(bundle.get("opinions"), list) else []
    run_id = maybe_text(bundle.get("run_id"))
    round_id = maybe_text(bundle.get("round_id"))
    normalized_opinions = [
        normalized_readiness_opinion_payload(
            opinion,
            run_id=run_id,
            round_id=round_id,
            opinion_index=index,
        )
        for index, opinion in enumerate(opinions)
        if isinstance(opinion, dict)
    ]
    if normalized_opinions:
        run_id = maybe_text(run_id) or maybe_text(normalized_opinions[0].get("run_id"))
        round_id = maybe_text(round_id) or maybe_text(normalized_opinions[0].get("round_id"))
    bundle["schema_version"] = "readiness-opinion-bundle-v1"
    bundle["run_id"] = run_id
    bundle["round_id"] = round_id
    bundle["generated_at_utc"] = (
        maybe_text(bundle.get("generated_at_utc"))
        or maybe_text(normalized_opinions[-1].get("generated_at_utc")) if normalized_opinions else utc_now_iso()
    )
    bundle["opinions"] = normalized_opinions
    bundle["opinion_count"] = len(normalized_opinions)
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM readiness_opinions WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, opinion in enumerate(normalized_opinions):
                write_readiness_opinion_row(
                    connection,
                    readiness_opinion_row_from_payload(
                        opinion,
                        artifact_path=artifact_path,
                        record_locator=f"$.opinions[{index}]",
                    ),
                )
    finally:
        connection.close()
    return bundle


def store_decision_trace_records(
    run_dir: str | Path,
    *,
    trace_bundle: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    bundle = dict(trace_bundle) if isinstance(trace_bundle, dict) else {}
    traces = bundle.get("traces", []) if isinstance(bundle.get("traces"), list) else []
    run_id = maybe_text(bundle.get("run_id"))
    round_id = maybe_text(bundle.get("round_id"))
    normalized_traces = [
        normalized_decision_trace_payload(
            trace,
            run_id=run_id,
            round_id=round_id,
            trace_index=index,
        )
        for index, trace in enumerate(traces)
        if isinstance(trace, dict)
    ]
    if normalized_traces:
        run_id = maybe_text(run_id) or maybe_text(normalized_traces[0].get("run_id"))
        round_id = maybe_text(round_id) or maybe_text(normalized_traces[0].get("round_id"))
    bundle["schema_version"] = "decision-trace-bundle-v1"
    bundle["run_id"] = run_id
    bundle["round_id"] = round_id
    bundle["generated_at_utc"] = (
        maybe_text(bundle.get("generated_at_utc"))
        or maybe_text(normalized_traces[-1].get("generated_at_utc")) if normalized_traces else utc_now_iso()
    )
    bundle["traces"] = normalized_traces
    bundle["trace_count"] = len(normalized_traces)
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM decision_traces WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, trace in enumerate(normalized_traces):
                write_decision_trace_row(
                    connection,
                    decision_trace_row_from_payload(
                        trace,
                        artifact_path=artifact_path,
                        record_locator=f"$.traces[{index}]",
                    ),
                )
    finally:
        connection.close()
    return bundle


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


def load_promotion_basis_items_for_record(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    round_id: str,
    basis_id: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT raw_json
        FROM promotion_basis_items
        WHERE run_id = ? AND round_id = ? AND basis_id = ?
        ORDER BY item_group, item_index, item_row_id
        """,
        (run_id, round_id, basis_id),
    ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if isinstance(payload, dict):
            results.append(payload)
    return results


def query_council_objects(
    run_dir: str | Path,
    *,
    object_kind: str,
    run_id: str = "",
    round_id: str = "",
    agent_role: str = "",
    status: str = "",
    decision_id: str = "",
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
        if include_items and maybe_text(config.get("item_loader")) == "promotion-basis-items":
            for payload in objects:
                payload["basis_items"] = load_promotion_basis_items_for_record(
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
