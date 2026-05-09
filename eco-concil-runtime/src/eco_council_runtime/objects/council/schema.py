from __future__ import annotations

import sqlite3
from pathlib import Path

from eco_council_runtime.kernel.planes.deliberation_plane import (
    connect_db as connect_deliberation_db,
)


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

CREATE TABLE IF NOT EXISTS dynamic_investigation_objects (
    object_id TEXT PRIMARY KEY,
    object_kind TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    author_role TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    target_kind TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    rationale TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_dynamic_investigation_objects_round
ON dynamic_investigation_objects(run_id, round_id, object_kind, generated_at_utc, object_id);
CREATE INDEX IF NOT EXISTS idx_dynamic_investigation_objects_role_status
ON dynamic_investigation_objects(run_id, round_id, object_kind, author_role, status, object_id);
CREATE INDEX IF NOT EXISTS idx_dynamic_investigation_objects_target
ON dynamic_investigation_objects(run_id, round_id, object_kind, target_kind, target_id, object_id);

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


def connect_db(run_dir: str | Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    connection, db_file = connect_deliberation_db(run_dir_path, db_path)
    connection.executescript(SCHEMA_SQL)
    return connection, db_file


__all__ = (
    "SCHEMA_SQL",
    "connect_db",
)
