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
from .deliberation_target_semantics import (
    canonical_target_kind,
    normalized_deliberation_target,
    proposal_target_from_payload,
)
from .kernel.deliberation_plane import (
    connect_db as connect_deliberation_db,
    json_text,
    maybe_text,
    payload_from_db_row,
    stable_hash,
    utc_now_iso,
)

OBJECT_KIND_PROPOSAL = "proposal"
OBJECT_KIND_FINDING = "finding"
OBJECT_KIND_DISCUSSION_MESSAGE = "discussion-message"
OBJECT_KIND_EVIDENCE_BUNDLE = "evidence-bundle"
OBJECT_KIND_REVIEW_COMMENT = "review-comment"
OBJECT_KIND_HYPOTHESIS = "hypothesis"
OBJECT_KIND_CHALLENGE = "challenge"
OBJECT_KIND_BOARD_TASK = "board-task"
OBJECT_KIND_NEXT_ACTION = "next-action"
OBJECT_KIND_PROBE = "probe"
OBJECT_KIND_READINESS_OPINION = "readiness-opinion"
OBJECT_KIND_READINESS_ASSESSMENT = "readiness-assessment"
OBJECT_KIND_REPORT_BASIS_FREEZE = "report-basis-freeze"
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

CREATE TABLE IF NOT EXISTS finding_records (
    finding_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    finding_kind TEXT NOT NULL DEFAULT '',
    agent_role TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    rationale TEXT NOT NULL DEFAULT '',
    target_kind TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    confidence REAL,
    decision_source TEXT NOT NULL DEFAULT '',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    source_signal_ids_json TEXT NOT NULL DEFAULT '[]',
    linked_bundle_ids_json TEXT NOT NULL DEFAULT '[]',
    response_to_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_finding_records_round
ON finding_records(run_id, round_id, generated_at_utc, finding_id);
CREATE INDEX IF NOT EXISTS idx_finding_records_role_status
ON finding_records(run_id, round_id, agent_role, status, finding_id);
CREATE INDEX IF NOT EXISTS idx_finding_records_target
ON finding_records(run_id, round_id, target_kind, target_id, finding_id);

CREATE TABLE IF NOT EXISTS discussion_messages (
    message_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    author_role TEXT NOT NULL DEFAULT '',
    message_kind TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    thread_id TEXT NOT NULL DEFAULT '',
    message_text TEXT NOT NULL DEFAULT '',
    target_kind TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    response_to_ids_json TEXT NOT NULL DEFAULT '[]',
    related_object_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_discussion_messages_round
ON discussion_messages(run_id, round_id, generated_at_utc, message_id);
CREATE INDEX IF NOT EXISTS idx_discussion_messages_role_status
ON discussion_messages(run_id, round_id, author_role, status, message_id);
CREATE INDEX IF NOT EXISTS idx_discussion_messages_thread
ON discussion_messages(run_id, round_id, thread_id, generated_at_utc, message_id);
CREATE INDEX IF NOT EXISTS idx_discussion_messages_target
ON discussion_messages(run_id, round_id, target_kind, target_id, message_id);

CREATE TABLE IF NOT EXISTS evidence_bundles (
    bundle_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    bundle_kind TEXT NOT NULL DEFAULT '',
    agent_role TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    rationale TEXT NOT NULL DEFAULT '',
    target_kind TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    confidence REAL,
    decision_source TEXT NOT NULL DEFAULT '',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    source_signal_ids_json TEXT NOT NULL DEFAULT '[]',
    finding_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_round
ON evidence_bundles(run_id, round_id, generated_at_utc, bundle_id);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_role_status
ON evidence_bundles(run_id, round_id, agent_role, status, bundle_id);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_target
ON evidence_bundles(run_id, round_id, target_kind, target_id, bundle_id);

CREATE TABLE IF NOT EXISTS review_comments (
    comment_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    author_role TEXT NOT NULL DEFAULT '',
    review_kind TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    thread_id TEXT NOT NULL DEFAULT '',
    comment_text TEXT NOT NULL DEFAULT '',
    target_kind TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    response_to_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_review_comments_round
ON review_comments(run_id, round_id, generated_at_utc, comment_id);
CREATE INDEX IF NOT EXISTS idx_review_comments_role_status
ON review_comments(run_id, round_id, author_role, status, comment_id);
CREATE INDEX IF NOT EXISTS idx_review_comments_thread
ON review_comments(run_id, round_id, thread_id, generated_at_utc, comment_id);
CREATE INDEX IF NOT EXISTS idx_review_comments_target
ON review_comments(run_id, round_id, target_kind, target_id, comment_id);

CREATE TABLE IF NOT EXISTS readiness_opinions (
    opinion_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    agent_role TEXT NOT NULL DEFAULT '',
    opinion_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    sufficient_for_report_basis INTEGER NOT NULL DEFAULT 0,
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


def maybe_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalized_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return unique_texts(value)


def confidence_or_default(value: Any, *, default: float = 0.7) -> float:
    parsed = maybe_number(value)
    if parsed is None:
        return float(default)
    return float(parsed)


def title_from_text(value: Any, *, prefix: str) -> str:
    text = maybe_text(value)
    if text:
        return text
    return maybe_text(prefix)


def default_deliberation_target(
    target: Any,
    *,
    round_id: str,
    target_kind: Any = "",
    target_id: Any = "",
) -> dict[str, Any]:
    resolved_target_id = maybe_text(target_id) or maybe_text(round_id)
    return normalized_deliberation_target(
        target,
        object_kind=maybe_text(target_kind) or "round",
        object_id=resolved_target_id,
        round_id=round_id,
    )


def require_non_empty_evidence_refs(
    object_kind: str,
    evidence_refs: list[Any],
) -> None:
    if evidence_refs:
        return
    raise ValueError(f"{object_kind} requires at least one evidence_ref.")


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


def finding_id(
    run_id: str,
    round_id: str,
    finding_kind: str,
    agent_role: str,
    finding_index: int,
    summary: str,
) -> str:
    return "finding-" + stable_hash(
        "finding-record",
        run_id,
        round_id,
        finding_kind,
        agent_role,
        finding_index,
        summary,
    )[:12]


def discussion_message_id(
    run_id: str,
    round_id: str,
    author_role: str,
    thread_id: str,
    message_index: int,
    message_text: str,
) -> str:
    return "discussion-message-" + stable_hash(
        "discussion-message",
        run_id,
        round_id,
        author_role,
        thread_id,
        message_index,
        message_text,
    )[:12]


def evidence_bundle_id(
    run_id: str,
    round_id: str,
    bundle_kind: str,
    agent_role: str,
    bundle_index: int,
    title: str,
) -> str:
    return "evidence-bundle-" + stable_hash(
        "evidence-bundle",
        run_id,
        round_id,
        bundle_kind,
        agent_role,
        bundle_index,
        title,
    )[:12]


def review_comment_id(
    run_id: str,
    round_id: str,
    author_role: str,
    thread_id: str,
    comment_index: int,
    comment_text: str,
) -> str:
    return "review-comment-" + stable_hash(
        "review-comment",
        run_id,
        round_id,
        author_role,
        thread_id,
        comment_index,
        comment_text,
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


def normalized_finding_payload(
    finding: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    finding_index: int,
) -> dict[str, Any]:
    normalized = dict(finding)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-investigation"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["finding_kind"] = (
        maybe_text(normalized.get("finding_kind")) or "finding"
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "environmental-investigator"
    normalized["status"] = maybe_text(normalized.get("status")) or "submitted"
    normalized["summary"] = maybe_text(normalized.get("summary"))
    normalized["title"] = title_from_text(
        normalized.get("title") or normalized.get("summary"),
        prefix="Finding",
    )
    normalized["rationale"] = (
        maybe_text(normalized.get("rationale"))
        or maybe_text(normalized.get("summary"))
    )
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["confidence"] = confidence_or_default(normalized.get("confidence"))
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    require_non_empty_evidence_refs(OBJECT_KIND_FINDING, normalized["evidence_refs"])
    normalized["basis_object_ids"] = normalized_text_list(
        normalized.get("basis_object_ids")
    )
    normalized["source_signal_ids"] = normalized_text_list(
        normalized.get("source_signal_ids")
    )
    normalized["linked_bundle_ids"] = normalized_text_list(
        normalized.get("linked_bundle_ids")
    )
    normalized["response_to_ids"] = normalized_text_list(
        normalized.get("response_to_ids")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            *normalized["basis_object_ids"],
            *normalized["source_signal_ids"],
            *normalized["linked_bundle_ids"],
            *normalized["response_to_ids"],
            normalized["target_id"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["finding_id"] = (
        maybe_text(normalized.get("finding_id"))
        or finding_id(
            normalized_run_id,
            normalized_round_id,
            normalized["finding_kind"],
            normalized["agent_role"],
            finding_index,
            normalized["summary"],
        )
    )
    normalized["schema_version"] = canonical_contract(OBJECT_KIND_FINDING).schema_version
    return validate_canonical_payload(OBJECT_KIND_FINDING, normalized)


def normalized_discussion_message_payload(
    message: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    message_index: int,
) -> dict[str, Any]:
    normalized = dict(message)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-discussion"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["author_role"] = maybe_text(normalized.get("author_role")) or "moderator"
    normalized["message_kind"] = (
        maybe_text(normalized.get("message_kind")) or "discussion"
    )
    normalized["status"] = maybe_text(normalized.get("status")) or "posted"
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["message_text"] = maybe_text(normalized.get("message_text"))
    normalized["response_to_ids"] = normalized_text_list(
        normalized.get("response_to_ids")
    )
    normalized["related_object_ids"] = normalized_text_list(
        normalized.get("related_object_ids")
    )
    normalized["thread_id"] = (
        maybe_text(normalized.get("thread_id"))
        or (
            normalized["response_to_ids"][0]
            if normalized["response_to_ids"]
            else normalized["target_id"]
        )
    )
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            normalized["thread_id"],
            normalized["target_id"],
            *normalized["response_to_ids"],
            *normalized["related_object_ids"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["message_id"] = (
        maybe_text(normalized.get("message_id"))
        or discussion_message_id(
            normalized_run_id,
            normalized_round_id,
            normalized["author_role"],
            normalized["thread_id"],
            message_index,
            normalized["message_text"],
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_DISCUSSION_MESSAGE
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_DISCUSSION_MESSAGE, normalized)


def normalized_evidence_bundle_payload(
    bundle: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    bundle_index: int,
) -> dict[str, Any]:
    normalized = dict(bundle)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-investigation"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["bundle_kind"] = (
        maybe_text(normalized.get("bundle_kind")) or "evidence-bundle"
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "moderator"
    normalized["status"] = maybe_text(normalized.get("status")) or "submitted"
    normalized["summary"] = maybe_text(normalized.get("summary"))
    normalized["title"] = title_from_text(
        normalized.get("title") or normalized.get("summary"),
        prefix="Evidence bundle",
    )
    normalized["rationale"] = (
        maybe_text(normalized.get("rationale"))
        or maybe_text(normalized.get("summary"))
    )
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["confidence"] = confidence_or_default(normalized.get("confidence"))
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    require_non_empty_evidence_refs(
        OBJECT_KIND_EVIDENCE_BUNDLE,
        normalized["evidence_refs"],
    )
    normalized["basis_object_ids"] = normalized_text_list(
        normalized.get("basis_object_ids")
    )
    normalized["source_signal_ids"] = normalized_text_list(
        normalized.get("source_signal_ids")
    )
    normalized["finding_ids"] = normalized_text_list(
        normalized.get("finding_ids")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            *normalized["basis_object_ids"],
            *normalized["source_signal_ids"],
            *normalized["finding_ids"],
            normalized["target_id"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["bundle_id"] = (
        maybe_text(normalized.get("bundle_id"))
        or evidence_bundle_id(
            normalized_run_id,
            normalized_round_id,
            normalized["bundle_kind"],
            normalized["agent_role"],
            bundle_index,
            normalized["title"],
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_EVIDENCE_BUNDLE
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_EVIDENCE_BUNDLE, normalized)


def normalized_review_comment_payload(
    comment: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    comment_index: int,
) -> dict[str, Any]:
    normalized = dict(comment)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "challenger-review"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["author_role"] = maybe_text(normalized.get("author_role")) or "challenger"
    normalized["review_kind"] = (
        maybe_text(normalized.get("review_kind")) or "review"
    )
    normalized["status"] = maybe_text(normalized.get("status")) or "open"
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["comment_text"] = maybe_text(normalized.get("comment_text"))
    normalized["response_to_ids"] = normalized_text_list(
        normalized.get("response_to_ids")
    )
    normalized["thread_id"] = (
        maybe_text(normalized.get("thread_id"))
        or (
            normalized["response_to_ids"][0]
            if normalized["response_to_ids"]
            else normalized["target_id"]
        )
    )
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            normalized["thread_id"],
            normalized["target_id"],
            *normalized["response_to_ids"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["comment_id"] = (
        maybe_text(normalized.get("comment_id"))
        or review_comment_id(
            normalized_run_id,
            normalized_round_id,
            normalized["author_role"],
            normalized["thread_id"],
            comment_index,
            normalized["comment_text"],
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_REVIEW_COMMENT
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_REVIEW_COMMENT, normalized)


def normalized_proposal_payload(
    proposal: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    proposal_index: int,
) -> dict[str, Any]:
    normalized = dict(proposal)
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
    target = proposal_target_from_payload(normalized)
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind"))
        or maybe_text(normalized.get("target_kind"))
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id"))
        or maybe_text(normalized.get("target_id"))
    )
    if maybe_text(target.get("claim_id")):
        normalized["target_claim_id"] = maybe_text(target.get("claim_id"))
    if maybe_text(target.get("hypothesis_id")):
        normalized["target_hypothesis_id"] = maybe_text(target.get("hypothesis_id"))
    if maybe_text(target.get("ticket_id")):
        normalized["target_ticket_id"] = maybe_text(target.get("ticket_id"))
    if maybe_text(target.get("task_id")):
        normalized["target_task_id"] = maybe_text(target.get("task_id"))
    normalized["rationale"] = maybe_text(normalized.get("rationale"))
    confidence = maybe_number(normalized.get("confidence"))
    if confidence is not None:
        normalized["confidence"] = confidence
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
    normalized["sufficient_for_report_basis"] = bool(
        normalized.get("sufficient_for_report_basis")
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


def finding_row_from_payload(
    finding: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "finding_id": maybe_text(finding.get("finding_id")),
        "run_id": maybe_text(finding.get("run_id")),
        "round_id": maybe_text(finding.get("round_id")),
        "generated_at_utc": maybe_text(finding.get("generated_at_utc")),
        "finding_kind": maybe_text(finding.get("finding_kind")),
        "agent_role": maybe_text(finding.get("agent_role")),
        "status": maybe_text(finding.get("status")),
        "title": maybe_text(finding.get("title")),
        "summary": maybe_text(finding.get("summary")),
        "rationale": maybe_text(finding.get("rationale")),
        "target_kind": maybe_text(finding.get("target_kind")),
        "target_id": maybe_text(finding.get("target_id")),
        "confidence": finding.get("confidence"),
        "decision_source": maybe_text(finding.get("decision_source")),
        "basis_object_ids_json": json_text(finding.get("basis_object_ids", [])),
        "source_signal_ids_json": json_text(finding.get("source_signal_ids", [])),
        "linked_bundle_ids_json": json_text(finding.get("linked_bundle_ids", [])),
        "response_to_ids_json": json_text(finding.get("response_to_ids", [])),
        "evidence_refs_json": json_text(finding.get("evidence_refs", [])),
        "provenance_json": json_text(finding.get("provenance", {})),
        "lineage_json": json_text(finding.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(finding),
    }


def discussion_message_row_from_payload(
    message: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "message_id": maybe_text(message.get("message_id")),
        "run_id": maybe_text(message.get("run_id")),
        "round_id": maybe_text(message.get("round_id")),
        "generated_at_utc": maybe_text(message.get("generated_at_utc")),
        "author_role": maybe_text(message.get("author_role")),
        "message_kind": maybe_text(message.get("message_kind")),
        "status": maybe_text(message.get("status")),
        "thread_id": maybe_text(message.get("thread_id")),
        "message_text": maybe_text(message.get("message_text")),
        "target_kind": maybe_text(message.get("target_kind")),
        "target_id": maybe_text(message.get("target_id")),
        "decision_source": maybe_text(message.get("decision_source")),
        "response_to_ids_json": json_text(message.get("response_to_ids", [])),
        "related_object_ids_json": json_text(message.get("related_object_ids", [])),
        "evidence_refs_json": json_text(message.get("evidence_refs", [])),
        "provenance_json": json_text(message.get("provenance", {})),
        "lineage_json": json_text(message.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(message),
    }


def evidence_bundle_row_from_payload(
    bundle: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "bundle_id": maybe_text(bundle.get("bundle_id")),
        "run_id": maybe_text(bundle.get("run_id")),
        "round_id": maybe_text(bundle.get("round_id")),
        "generated_at_utc": maybe_text(bundle.get("generated_at_utc")),
        "bundle_kind": maybe_text(bundle.get("bundle_kind")),
        "agent_role": maybe_text(bundle.get("agent_role")),
        "status": maybe_text(bundle.get("status")),
        "title": maybe_text(bundle.get("title")),
        "summary": maybe_text(bundle.get("summary")),
        "rationale": maybe_text(bundle.get("rationale")),
        "target_kind": maybe_text(bundle.get("target_kind")),
        "target_id": maybe_text(bundle.get("target_id")),
        "confidence": bundle.get("confidence"),
        "decision_source": maybe_text(bundle.get("decision_source")),
        "basis_object_ids_json": json_text(bundle.get("basis_object_ids", [])),
        "source_signal_ids_json": json_text(bundle.get("source_signal_ids", [])),
        "finding_ids_json": json_text(bundle.get("finding_ids", [])),
        "evidence_refs_json": json_text(bundle.get("evidence_refs", [])),
        "provenance_json": json_text(bundle.get("provenance", {})),
        "lineage_json": json_text(bundle.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(bundle),
    }


def review_comment_row_from_payload(
    comment: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "comment_id": maybe_text(comment.get("comment_id")),
        "run_id": maybe_text(comment.get("run_id")),
        "round_id": maybe_text(comment.get("round_id")),
        "generated_at_utc": maybe_text(comment.get("generated_at_utc")),
        "author_role": maybe_text(comment.get("author_role")),
        "review_kind": maybe_text(comment.get("review_kind")),
        "status": maybe_text(comment.get("status")),
        "thread_id": maybe_text(comment.get("thread_id")),
        "comment_text": maybe_text(comment.get("comment_text")),
        "target_kind": maybe_text(comment.get("target_kind")),
        "target_id": maybe_text(comment.get("target_id")),
        "decision_source": maybe_text(comment.get("decision_source")),
        "response_to_ids_json": json_text(comment.get("response_to_ids", [])),
        "evidence_refs_json": json_text(comment.get("evidence_refs", [])),
        "provenance_json": json_text(comment.get("provenance", {})),
        "lineage_json": json_text(comment.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(comment),
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
        "sufficient_for_report_basis": 1
        if bool(opinion.get("sufficient_for_report_basis"))
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


def write_finding_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO finding_records (
            finding_id, run_id, round_id, generated_at_utc, finding_kind,
            agent_role, status, title, summary, rationale, target_kind,
            target_id, confidence, decision_source, basis_object_ids_json,
            source_signal_ids_json, linked_bundle_ids_json, response_to_ids_json,
            evidence_refs_json, provenance_json, lineage_json, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :finding_id, :run_id, :round_id, :generated_at_utc, :finding_kind,
            :agent_role, :status, :title, :summary, :rationale, :target_kind,
            :target_id, :confidence, :decision_source, :basis_object_ids_json,
            :source_signal_ids_json, :linked_bundle_ids_json, :response_to_ids_json,
            :evidence_refs_json, :provenance_json, :lineage_json, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_discussion_message_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO discussion_messages (
            message_id, run_id, round_id, generated_at_utc, author_role,
            message_kind, status, thread_id, message_text, target_kind,
            target_id, decision_source, response_to_ids_json,
            related_object_ids_json, evidence_refs_json, provenance_json,
            lineage_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :message_id, :run_id, :round_id, :generated_at_utc, :author_role,
            :message_kind, :status, :thread_id, :message_text, :target_kind,
            :target_id, :decision_source, :response_to_ids_json,
            :related_object_ids_json, :evidence_refs_json, :provenance_json,
            :lineage_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_evidence_bundle_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO evidence_bundles (
            bundle_id, run_id, round_id, generated_at_utc, bundle_kind,
            agent_role, status, title, summary, rationale, target_kind,
            target_id, confidence, decision_source, basis_object_ids_json,
            source_signal_ids_json, finding_ids_json, evidence_refs_json,
            provenance_json, lineage_json, artifact_path, record_locator,
            raw_json
        ) VALUES (
            :bundle_id, :run_id, :round_id, :generated_at_utc, :bundle_kind,
            :agent_role, :status, :title, :summary, :rationale, :target_kind,
            :target_id, :confidence, :decision_source, :basis_object_ids_json,
            :source_signal_ids_json, :finding_ids_json, :evidence_refs_json,
            :provenance_json, :lineage_json, :artifact_path, :record_locator,
            :raw_json
        )
        """,
        row,
    )


def write_review_comment_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO review_comments (
            comment_id, run_id, round_id, generated_at_utc, author_role,
            review_kind, status, thread_id, comment_text, target_kind,
            target_id, decision_source, response_to_ids_json, evidence_refs_json,
            provenance_json, lineage_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :comment_id, :run_id, :round_id, :generated_at_utc, :author_role,
            :review_kind, :status, :thread_id, :comment_text, :target_kind,
            :target_id, :decision_source, :response_to_ids_json, :evidence_refs_json,
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
            opinion_status, readiness_status, sufficient_for_report_basis,
            confidence, rationale, decision_source, basis_object_ids_json,
            evidence_refs_json, provenance_json, lineage_json, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :opinion_id, :run_id, :round_id, :generated_at_utc, :agent_role,
            :opinion_status, :readiness_status, :sufficient_for_report_basis,
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


def append_finding_record(
    run_dir: str | Path,
    *,
    finding_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.finding",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(finding_payload) if isinstance(finding_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            finding_index = next_round_object_index(
                connection,
                table_name="finding_records",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_finding_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                finding_index=finding_index,
            )
            write_finding_row(
                connection,
                finding_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "finding-record-append-v1",
        "db_path": str(db_file),
        "finding": normalized,
    }


def append_discussion_message_record(
    run_dir: str | Path,
    *,
    message_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.message",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(message_payload) if isinstance(message_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            message_index = next_round_object_index(
                connection,
                table_name="discussion_messages",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_discussion_message_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                message_index=message_index,
            )
            write_discussion_message_row(
                connection,
                discussion_message_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "discussion-message-append-v1",
        "db_path": str(db_file),
        "message": normalized,
    }


def append_evidence_bundle_record(
    run_dir: str | Path,
    *,
    bundle_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.bundle",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(bundle_payload) if isinstance(bundle_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            bundle_index = next_round_object_index(
                connection,
                table_name="evidence_bundles",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_evidence_bundle_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                bundle_index=bundle_index,
            )
            write_evidence_bundle_row(
                connection,
                evidence_bundle_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "evidence-bundle-append-v1",
        "db_path": str(db_file),
        "bundle": normalized,
    }


def append_review_comment_record(
    run_dir: str | Path,
    *,
    comment_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.comment",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(comment_payload) if isinstance(comment_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            comment_index = next_round_object_index(
                connection,
                table_name="review_comments",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_review_comment_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                comment_index=comment_index,
            )
            write_review_comment_row(
                connection,
                review_comment_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "review-comment-append-v1",
        "db_path": str(db_file),
        "comment": normalized,
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
