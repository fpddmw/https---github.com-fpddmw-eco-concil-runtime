from __future__ import annotations

import sqlite3
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane import json_text, maybe_text


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


def dynamic_investigation_object_row_from_payload(
    dynamic_object: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "object_id": maybe_text(dynamic_object.get("object_id")),
        "object_kind": maybe_text(dynamic_object.get("object_kind")),
        "run_id": maybe_text(dynamic_object.get("run_id")),
        "round_id": maybe_text(dynamic_object.get("round_id")),
        "generated_at_utc": maybe_text(dynamic_object.get("generated_at_utc")),
        "author_role": maybe_text(dynamic_object.get("author_role")),
        "status": maybe_text(dynamic_object.get("status")),
        "target_kind": maybe_text(dynamic_object.get("target_kind")),
        "target_id": maybe_text(dynamic_object.get("target_id")),
        "rationale": maybe_text(dynamic_object.get("rationale")),
        "decision_source": maybe_text(dynamic_object.get("decision_source")),
        "evidence_refs_json": json_text(dynamic_object.get("evidence_refs", [])),
        "provenance_json": json_text(dynamic_object.get("provenance", {})),
        "lineage_json": json_text(dynamic_object.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(dynamic_object),
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


def write_dynamic_investigation_object_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO dynamic_investigation_objects (
            object_id, object_kind, run_id, round_id, generated_at_utc,
            author_role, status, target_kind, target_id, rationale,
            decision_source, evidence_refs_json, provenance_json, lineage_json,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :object_id, :object_kind, :run_id, :round_id, :generated_at_utc,
            :author_role, :status, :target_kind, :target_id, :rationale,
            :decision_source, :evidence_refs_json, :provenance_json,
            :lineage_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


__all__ = (
    "proposal_row_from_payload",
    "finding_row_from_payload",
    "discussion_message_row_from_payload",
    "evidence_bundle_row_from_payload",
    "review_comment_row_from_payload",
    "dynamic_investigation_object_row_from_payload",
    "readiness_opinion_row_from_payload",
    "write_council_proposal_row",
    "write_finding_row",
    "write_discussion_message_row",
    "write_evidence_bundle_row",
    "write_review_comment_row",
    "write_dynamic_investigation_object_row",
    "write_readiness_opinion_row",
)
