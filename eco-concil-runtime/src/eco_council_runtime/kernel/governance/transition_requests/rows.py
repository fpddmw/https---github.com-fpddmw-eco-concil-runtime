from __future__ import annotations

import sqlite3
from typing import Any

from eco_council_runtime.kernel.governance.transition_requests.common import json_text, maybe_text

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


__all__ = (
    "transition_request_row_from_payload",
    "transition_approval_row_from_payload",
    "transition_rejection_row_from_payload",
    "write_transition_request_row",
    "write_transition_approval_row",
    "write_transition_rejection_row",
)
