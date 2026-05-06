from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..deliberation_target_semantics import (
    normalized_deliberation_target,
    source_proposal_id_from_payload,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def normalized_provenance(
    value: Any,
    *,
    source_skill: str = "",
    decision_source: str = "",
    artifact_path: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict_items(value)
    if source_skill and "source_skill" not in normalized:
        normalized["source_skill"] = source_skill
    if decision_source and "decision_source" not in normalized:
        normalized["decision_source"] = decision_source
    if artifact_path and "artifact_path" not in normalized:
        normalized["artifact_path"] = artifact_path
    if isinstance(extra, dict):
        for key, raw_value in extra.items():
            key_text = maybe_text(key)
            if (
                not key_text
                or key_text in normalized
                or raw_value in (None, "", [], {})
            ):
                continue
            normalized[key_text] = raw_value
    return normalized


def merged_lineage(existing: Any, *sources: Any) -> list[str]:
    values = list_items(existing)
    for source in sources:
        if isinstance(source, list):
            values.extend(source)
            continue
        values.append(source)
    return unique_texts(values)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    import hashlib

    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def decode_json(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default


BOOLEAN_ROW_COLUMNS = {
    "blocking",
    "next_round_required",
    "probe_candidate",
    "probe_stage_included",
    "report_basis_freeze_allowed",
    "readiness_blocker",
    "required_for_controller",
    "restart_recommended",
    "resume_recommended",
    "reporting_ready",
    "sufficient_for_report_basis",
}


def payload_from_db_row(row: sqlite3.Row) -> dict[str, Any]:
    payload = decode_json(maybe_text(row["raw_json"]), {})
    normalized = payload if isinstance(payload, dict) else {}
    for key in row.keys():
        if key == "raw_json":
            continue
        value = row[key]
        if key in BOOLEAN_ROW_COLUMNS:
            normalized[key] = bool(value)
            continue
        if key.endswith("_json"):
            decoded = decode_json(maybe_text(value), None)
            if isinstance(decoded, (list, dict)):
                normalized[key[:-5]] = decoded
            continue
        if isinstance(value, str):
            normalized[key] = maybe_text(value)
            continue
        if value is not None:
            normalized[key] = value
    if any(
        maybe_text(normalized.get(field_name))
        for field_name in (
            "target_object_kind",
            "target_object_id",
            "target_kind",
            "target_id",
            "target_claim_id",
            "target_hypothesis_id",
            "target_ticket_id",
            "target_route_id",
            "target_actor_id",
            "target_assessment_id",
            "target_linkage_id",
            "target_gap_id",
            "target_proposal_id",
        )
    ) or isinstance(normalized.get("target"), dict):
        normalized["target"] = normalized_deliberation_target(
            normalized.get("target"),
            object_kind=maybe_text(normalized.get("target_object_kind"))
            or maybe_text(normalized.get("target_kind")),
            object_id=maybe_text(normalized.get("target_object_id"))
            or maybe_text(normalized.get("target_id")),
            issue_label=maybe_text(normalized.get("issue_label")),
            claim_id=maybe_text(normalized.get("target_claim_id")),
            hypothesis_id=maybe_text(normalized.get("target_hypothesis_id")),
            ticket_id=maybe_text(normalized.get("target_ticket_id")),
            route_id=maybe_text(normalized.get("target_route_id")),
            actor_id=maybe_text(normalized.get("target_actor_id")),
            assessment_id=maybe_text(normalized.get("target_assessment_id")),
            linkage_id=maybe_text(normalized.get("target_linkage_id")),
            gap_id=maybe_text(normalized.get("target_gap_id")),
            proposal_id=maybe_text(normalized.get("target_proposal_id")),
            round_id=maybe_text(normalized.get("round_id")),
        )
    source_proposal_id = source_proposal_id_from_payload(normalized)
    if source_proposal_id:
        normalized["source_proposal_id"] = source_proposal_id
    if (
        maybe_text(normalized.get("supervisor_id"))
        and not maybe_text(normalized.get("supervisor_path"))
        and maybe_text(normalized.get("artifact_path"))
    ):
        normalized["supervisor_path"] = maybe_text(normalized.get("artifact_path"))
    return normalized


def cleaned_wrapper_record(
    payload: dict[str, Any],
    *,
    metadata_fields: tuple[str, ...],
    optional_empty_fields: tuple[str, ...],
) -> dict[str, Any]:
    normalized = dict(payload)
    for field_name in metadata_fields:
        normalized.pop(field_name, None)
    for field_name in optional_empty_fields:
        if field_name in normalized and not maybe_text(normalized.get(field_name)):
            normalized.pop(field_name, None)
    return normalized


def coerce_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def write_board_event_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO board_events (
            event_id, run_id, round_id, event_type, created_at_utc, payload_json,
            event_index, board_revision, artifact_path, record_locator, raw_json
        ) VALUES (
            :event_id, :run_id, :round_id, :event_type, :created_at_utc, :payload_json,
            :event_index, :board_revision, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_board_note_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO board_notes (
            note_id, run_id, round_id, created_at_utc, author_role, category, note_text,
            tags_json, linked_artifact_refs_json, related_ids_json, board_revision,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :note_id, :run_id, :round_id, :created_at_utc, :author_role, :category, :note_text,
            :tags_json, :linked_artifact_refs_json, :related_ids_json, :board_revision,
            :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_hypothesis_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO hypothesis_cards (
            hypothesis_id, run_id, round_id, title, statement, status, owner_role,
            linked_claim_ids_json, decision_source, evidence_refs_json, source_ids_json,
            provenance_json, lineage_json, confidence, created_at_utc, updated_at_utc,
            carryover_from_round_id, carryover_from_hypothesis_id, history_json,
            board_revision, artifact_path, record_locator, raw_json
        ) VALUES (
            :hypothesis_id, :run_id, :round_id, :title, :statement, :status, :owner_role,
            :linked_claim_ids_json, :decision_source, :evidence_refs_json, :source_ids_json,
            :provenance_json, :lineage_json, :confidence, :created_at_utc, :updated_at_utc,
            :carryover_from_round_id, :carryover_from_hypothesis_id, :history_json,
            :board_revision, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_challenge_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO challenge_tickets (
            ticket_id, run_id, round_id, created_at_utc, status, priority, owner_role,
            title, challenge_statement, target_claim_id, target_hypothesis_id, decision_source,
            evidence_refs_json, source_ids_json, provenance_json, lineage_json,
            linked_artifact_refs_json, related_task_ids_json, closed_at_utc, closed_by_role,
            resolution, resolution_note, history_json, board_revision, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :ticket_id, :run_id, :round_id, :created_at_utc, :status, :priority, :owner_role,
            :title, :challenge_statement, :target_claim_id, :target_hypothesis_id, :decision_source,
            :evidence_refs_json, :source_ids_json, :provenance_json, :lineage_json,
            :linked_artifact_refs_json, :related_task_ids_json, :closed_at_utc, :closed_by_role,
            :resolution, :resolution_note, :history_json, :board_revision, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_board_task_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO board_tasks (
            task_id, run_id, round_id, title, task_text, task_type, status, owner_role,
            priority, source_round_id, source_ticket_id, source_hypothesis_id,
            carryover_from_round_id, carryover_from_task_id, decision_source,
            evidence_refs_json, source_ids_json, provenance_json, lineage_json,
            linked_artifact_refs_json, related_ids_json, created_at_utc, updated_at_utc, claimed_at_utc, history_json,
            board_revision, artifact_path, record_locator, raw_json
        ) VALUES (
            :task_id, :run_id, :round_id, :title, :task_text, :task_type, :status, :owner_role,
            :priority, :source_round_id, :source_ticket_id, :source_hypothesis_id,
            :carryover_from_round_id, :carryover_from_task_id, :decision_source,
            :evidence_refs_json, :source_ids_json, :provenance_json, :lineage_json,
            :linked_artifact_refs_json, :related_ids_json, :created_at_utc, :updated_at_utc, :claimed_at_utc, :history_json,
            :board_revision, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_round_transition_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO round_transitions (
            transition_id, run_id, round_id, source_round_id, generated_at_utc, operation,
            event_id, board_revision, prior_round_ids_json, cross_round_query_hints_json,
            counts_json, warnings_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :transition_id, :run_id, :round_id, :source_round_id, :generated_at_utc, :operation,
            :event_id, :board_revision, :prior_round_ids_json, :cross_round_query_hints_json,
            :counts_json, :warnings_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_report_basis_freeze_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO report_basis_freezes (
            freeze_id, run_id, round_id, updated_at_utc, gate_status, readiness_status,
            report_basis_status, controller_status, supervisor_status, planning_mode,
            report_basis_freeze_allowed, gate_reasons_json, recommended_next_skills_json,
            reporting_ready, reporting_handoff_status, reporting_blockers_json,
            controller_artifact_path, gate_artifact_path, supervisor_artifact_path,
            record_locator, raw_json
        ) VALUES (
            :freeze_id, :run_id, :round_id, :updated_at_utc, :gate_status, :readiness_status,
            :report_basis_status, :controller_status, :supervisor_status, :planning_mode,
            :report_basis_freeze_allowed, :gate_reasons_json, :recommended_next_skills_json,
            :reporting_ready, :reporting_handoff_status, :reporting_blockers_json,
            :controller_artifact_path, :gate_artifact_path, :supervisor_artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_controller_snapshot_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO controller_snapshots (
            snapshot_id, controller_id, run_id, round_id, generated_at_utc,
            controller_status, planning_mode, current_stage, failed_stage,
            resume_status, readiness_status, gate_status, report_basis_status,
            resume_recommended, restart_recommended, resume_from_stage,
            completed_stage_names_json, pending_stage_names_json, gate_reasons_json,
            recommended_next_skills_json, execution_policy_json, progress_json,
            recovery_json, planning_json, planning_attempts_json,
            stage_contracts_json, steps_json, artifacts_json, failure_json,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :snapshot_id, :controller_id, :run_id, :round_id, :generated_at_utc,
            :controller_status, :planning_mode, :current_stage, :failed_stage,
            :resume_status, :readiness_status, :gate_status, :report_basis_status,
            :resume_recommended, :restart_recommended, :resume_from_stage,
            :completed_stage_names_json, :pending_stage_names_json, :gate_reasons_json,
            :recommended_next_skills_json, :execution_policy_json, :progress_json,
            :recovery_json, :planning_json, :planning_attempts_json,
            :stage_contracts_json, :steps_json, :artifacts_json, :failure_json,
            :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_gate_snapshot_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO gate_snapshots (
            snapshot_id, gate_id, run_id, round_id, generated_at_utc,
            stage_name, gate_handler, gate_status, readiness_status, report_basis_freeze_allowed,
            decision_source, report_basis_resolution_mode, gate_reasons_json,
            supporting_proposal_ids_json, rejected_proposal_ids_json,
            supporting_opinion_ids_json, rejected_opinion_ids_json,
            council_input_counts_json, recommended_next_skills_json,
            warnings_json, readiness_path, output_path, record_locator, raw_json
        ) VALUES (
            :snapshot_id, :gate_id, :run_id, :round_id, :generated_at_utc,
            :stage_name, :gate_handler, :gate_status, :readiness_status, :report_basis_freeze_allowed,
            :decision_source, :report_basis_resolution_mode, :gate_reasons_json,
            :supporting_proposal_ids_json, :rejected_proposal_ids_json,
            :supporting_opinion_ids_json, :rejected_opinion_ids_json,
            :council_input_counts_json, :recommended_next_skills_json,
            :warnings_json, :readiness_path, :output_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_supervisor_snapshot_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO supervisor_snapshots (
            snapshot_id, supervisor_id, run_id, round_id, generated_at_utc,
            supervisor_status, supervisor_substatus, governed_execution_posture, terminal_state,
            recovery_posture, operator_action, controller_status, planning_mode,
            readiness_status, gate_status, report_basis_status, reporting_ready,
            reporting_handoff_status, resume_status, current_stage, failed_stage,
            resume_recommended, restart_recommended, resume_from_stage,
            reporting_blockers_json, recommended_next_skills_json,
            execution_policy_json, round_transition_json, top_actions_json,
            operator_notes_json, inspection_paths_json, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :snapshot_id, :supervisor_id, :run_id, :round_id, :generated_at_utc,
            :supervisor_status, :supervisor_substatus, :governed_execution_posture, :terminal_state,
            :recovery_posture, :operator_action, :controller_status, :planning_mode,
            :readiness_status, :gate_status, :report_basis_status, :reporting_ready,
            :reporting_handoff_status, :resume_status, :current_stage, :failed_stage,
            :resume_recommended, :restart_recommended, :resume_from_stage,
            :reporting_blockers_json, :recommended_next_skills_json,
            :execution_policy_json, :round_transition_json, :top_actions_json,
            :operator_notes_json, :inspection_paths_json, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_orchestration_plan_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO orchestration_plans (
            plan_id, run_id, round_id, generated_at_utc, planning_status,
            planning_mode, controller_authority, plan_source,
            council_execution_mode, downstream_posture, probe_stage_included,
            artifact_path, execution_queue_count, gate_step_count,
            derived_export_count, post_gate_step_count, planned_stage_count,
            assigned_role_hints_json, phase_decision_basis_json,
            agent_turn_hints_json, observed_state_json, inputs_json,
            execution_queue_json, gate_steps_json, derived_exports_json,
            post_gate_steps_json, stop_conditions_json, fallback_path_json,
            planning_notes_json, deliberation_sync_json, step_counts_json,
            record_locator, raw_json
        ) VALUES (
            :plan_id, :run_id, :round_id, :generated_at_utc, :planning_status,
            :planning_mode, :controller_authority, :plan_source,
            :council_execution_mode, :downstream_posture, :probe_stage_included,
            :artifact_path, :execution_queue_count, :gate_step_count,
            :derived_export_count, :post_gate_step_count, :planned_stage_count,
            :assigned_role_hints_json, :phase_decision_basis_json,
            :agent_turn_hints_json, :observed_state_json, :inputs_json,
            :execution_queue_json, :gate_steps_json, :derived_exports_json,
            :post_gate_steps_json, :stop_conditions_json, :fallback_path_json,
            :planning_notes_json, :deliberation_sync_json, :step_counts_json,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_orchestration_plan_step_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO orchestration_plan_steps (
            step_id, plan_id, run_id, round_id, generated_at_utc,
            plan_step_group, step_index, planning_mode, controller_authority,
            plan_source, phase_group, stage_name, stage_kind, skill_name,
            expected_skill_name, assigned_role_hint, blocking, resume_policy,
            gate_handler, readiness_stage_name, reason, operator_summary,
            expected_output_path, required_for_controller, export_mode,
            required_previous_stages_json, skill_args_json, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :step_id, :plan_id, :run_id, :round_id, :generated_at_utc,
            :plan_step_group, :step_index, :planning_mode, :controller_authority,
            :plan_source, :phase_group, :stage_name, :stage_kind, :skill_name,
            :expected_skill_name, :assigned_role_hint, :blocking, :resume_policy,
            :gate_handler, :readiness_stage_name, :reason, :operator_summary,
            :expected_output_path, :required_for_controller, :export_mode,
            :required_previous_stages_json, :skill_args_json, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_moderator_action_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO moderator_actions (
            action_id, run_id, round_id, generated_at_utc, action_rank, action_kind,
            priority, assigned_role, target_hypothesis_id, target_claim_id,
            target_ticket_id, target_actor_id, target_proposal_id, target_object_kind,
            target_object_id, issue_label, target_route_id, target_assessment_id,
            target_linkage_id, target_gap_id, source_proposal_id, controversy_gap,
            recommended_lane,
            probe_candidate, readiness_blocker,
            objective, reason, evidence_refs_json, source_ids_json, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :action_id, :run_id, :round_id, :generated_at_utc, :action_rank, :action_kind,
            :priority, :assigned_role, :target_hypothesis_id, :target_claim_id,
            :target_ticket_id, :target_actor_id, :target_proposal_id,
            :target_object_kind, :target_object_id, :issue_label, :target_route_id,
            :target_assessment_id, :target_linkage_id, :target_gap_id,
            :source_proposal_id, :controversy_gap, :recommended_lane,
            :probe_candidate, :readiness_blocker,
            :objective, :reason, :evidence_refs_json, :source_ids_json, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_moderator_action_snapshot_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO moderator_action_snapshots (
            snapshot_id, run_id, round_id, generated_at_utc, action_source,
            board_state_source, coverage_source, action_count, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :snapshot_id, :run_id, :round_id, :generated_at_utc, :action_source,
            :board_state_source, :coverage_source, :action_count, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_falsification_probe_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO falsification_probes (
            probe_id, run_id, round_id, opened_at_utc, probe_status, action_id,
            priority, owner_role, target_hypothesis_id, target_claim_id,
            target_ticket_id, target_actor_id, target_proposal_id, target_object_kind,
            target_object_id, issue_label, target_route_id, target_assessment_id,
            target_linkage_id, target_gap_id, source_proposal_id, probe_type, controversy_gap,
            recommended_lane, probe_goal, falsification_question,
            requested_skills_json, evidence_refs_json, source_ids_json,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :probe_id, :run_id, :round_id, :opened_at_utc, :probe_status, :action_id,
            :priority, :owner_role, :target_hypothesis_id, :target_claim_id,
            :target_ticket_id, :target_actor_id, :target_proposal_id,
            :target_object_kind, :target_object_id, :issue_label, :target_route_id,
            :target_assessment_id, :target_linkage_id, :target_gap_id,
            :source_proposal_id, :probe_type, :controversy_gap,
            :recommended_lane, :probe_goal, :falsification_question,
            :requested_skills_json, :evidence_refs_json, :source_ids_json,
            :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_falsification_probe_snapshot_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO falsification_probe_snapshots (
            snapshot_id, run_id, round_id, generated_at_utc, action_source,
            board_state_source, coverage_source, probe_count, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :snapshot_id, :run_id, :round_id, :generated_at_utc, :action_source,
            :board_state_source, :coverage_source, :probe_count, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_round_readiness_assessment_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO round_readiness_assessments (
            readiness_id, run_id, round_id, generated_at_utc, readiness_status,
            sufficient_for_report_basis, board_state_source, coverage_source,
            next_actions_source, probes_source, agenda_counts_json, counts_json,
            controversy_gap_counts_json, probe_type_counts_json, gate_reasons_json,
            recommended_next_skills_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :readiness_id, :run_id, :round_id, :generated_at_utc, :readiness_status,
            :sufficient_for_report_basis, :board_state_source, :coverage_source,
            :next_actions_source, :probes_source, :agenda_counts_json, :counts_json,
            :controversy_gap_counts_json, :probe_type_counts_json, :gate_reasons_json,
            :recommended_next_skills_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_report_basis_freeze_record_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO report_basis_freeze_records (
            basis_id, run_id, round_id, generated_at_utc, report_basis_status,
            readiness_status, board_state_source, coverage_source, readiness_source,
            next_actions_source, board_brief_source, basis_selection_mode,
            basis_counts_json, selected_basis_object_ids_json,
            selected_evidence_refs_json, gate_reasons_json, remaining_risks_json,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :basis_id, :run_id, :round_id, :generated_at_utc, :report_basis_status,
            :readiness_status, :board_state_source, :coverage_source, :readiness_source,
            :next_actions_source, :board_brief_source, :basis_selection_mode,
            :basis_counts_json, :selected_basis_object_ids_json,
            :selected_evidence_refs_json, :gate_reasons_json, :remaining_risks_json,
            :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_report_basis_freeze_item_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO report_basis_freeze_items (
            item_row_id, basis_id, run_id, round_id, generated_at_utc, item_group,
            item_index, object_type, object_id, issue_label, claim_id,
            recommended_lane, route_status, readiness, evidence_refs_json,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :item_row_id, :basis_id, :run_id, :round_id, :generated_at_utc, :item_group,
            :item_index, :object_type, :object_id, :issue_label, :claim_id,
            :recommended_lane, :route_status, :readiness, :evidence_refs_json,
            :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_reporting_handoff_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO reporting_handoffs (
            handoff_id, run_id, round_id, generated_at_utc, handoff_status,
            reporting_ready, reporting_blockers_json,
            report_basis_status, readiness_status, supervisor_status,
            board_state_source, coverage_source, report_basis_source,
            readiness_source, board_brief_source, supervisor_state_source,
            selected_evidence_refs_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :handoff_id, :run_id, :round_id, :generated_at_utc, :handoff_status,
            :reporting_ready, :reporting_blockers_json,
            :report_basis_status, :readiness_status, :supervisor_status,
            :board_state_source, :coverage_source, :report_basis_source,
            :readiness_source, :board_brief_source, :supervisor_state_source,
            :selected_evidence_refs_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_council_decision_record_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO council_decision_records (
            record_id, decision_id, run_id, round_id, generated_at_utc,
            decision_stage, moderator_status, reporting_ready,
            publication_readiness, decision_gating_json,
            next_round_required, canonical_artifact, board_state_source,
            coverage_source, reporting_handoff_source, report_basis_source,
            decision_source, sociologist_report_source,
            environmentalist_report_source, selected_evidence_refs_json,
            published_report_refs_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :record_id, :decision_id, :run_id, :round_id, :generated_at_utc,
            :decision_stage, :moderator_status, :reporting_ready,
            :publication_readiness, :decision_gating_json,
            :next_round_required, :canonical_artifact, :board_state_source,
            :coverage_source, :reporting_handoff_source, :report_basis_source,
            :decision_source, :sociologist_report_source,
            :environmentalist_report_source, :selected_evidence_refs_json,
            :published_report_refs_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_expert_report_record_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO expert_report_records (
            record_id, report_id, run_id, round_id, generated_at_utc,
            report_stage, agent_role, status, handoff_status,
            reporting_ready,
            publication_readiness, canonical_artifact, board_state_source,
            coverage_source, reporting_handoff_source, decision_source,
            expert_report_draft_source, board_brief_source,
            selected_evidence_refs_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :record_id, :report_id, :run_id, :round_id, :generated_at_utc,
            :report_stage, :agent_role, :status, :handoff_status,
            :reporting_ready,
            :publication_readiness, :canonical_artifact, :board_state_source,
            :coverage_source, :reporting_handoff_source, :decision_source,
            :expert_report_draft_source, :board_brief_source,
            :selected_evidence_refs_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_final_publication_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO final_publications (
            publication_id, run_id, round_id, generated_at_utc,
            publication_status, publication_posture, board_state_source,
            coverage_source, reporting_handoff_source, decision_source,
            report_basis_source, supervisor_state_source,
            sociologist_report_source, environmentalist_report_source,
            selected_evidence_refs_json, published_report_refs_json,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :publication_id, :run_id, :round_id, :generated_at_utc,
            :publication_status, :publication_posture, :board_state_source,
            :coverage_source, :reporting_handoff_source, :decision_source,
            :report_basis_source, :supervisor_state_source,
            :sociologist_report_source, :environmentalist_report_source,
            :selected_evidence_refs_json, :published_report_refs_json,
            :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_round_task_snapshot_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO round_task_snapshots (
            snapshot_id, run_id, round_id, generated_at_utc, task_source,
            task_count, artifact_path, record_locator, raw_json
        ) VALUES (
            :snapshot_id, :run_id, :round_id, :generated_at_utc, :task_source,
            :task_count, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def event_row_from_payload(
    event: dict[str, Any],
    *,
    event_index: int,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "event_id": maybe_text(event.get("event_id")),
        "run_id": maybe_text(event.get("run_id")),
        "round_id": maybe_text(event.get("round_id")),
        "event_type": maybe_text(event.get("event_type")),
        "created_at_utc": maybe_text(event.get("created_at_utc")),
        "payload_json": json_text(event.get("payload", {})),
        "event_index": coerce_int(event_index),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(event),
    }


def note_row_from_payload(
    note: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "note_id": maybe_text(note.get("note_id")),
        "run_id": maybe_text(note.get("run_id")),
        "round_id": maybe_text(note.get("round_id")),
        "created_at_utc": maybe_text(note.get("created_at_utc")),
        "author_role": maybe_text(note.get("author_role")),
        "category": maybe_text(note.get("category")),
        "note_text": maybe_text(note.get("note_text")),
        "tags_json": json_text(note.get("tags", [])),
        "linked_artifact_refs_json": json_text(note.get("linked_artifact_refs", [])),
        "related_ids_json": json_text(note.get("related_ids", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(note),
    }


def hypothesis_row_from_payload(
    hypothesis: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
        "run_id": maybe_text(hypothesis.get("run_id")),
        "round_id": maybe_text(hypothesis.get("round_id")),
        "title": maybe_text(hypothesis.get("title")),
        "statement": maybe_text(hypothesis.get("statement")),
        "status": maybe_text(hypothesis.get("status")),
        "owner_role": maybe_text(hypothesis.get("owner_role")),
        "linked_claim_ids_json": json_text(hypothesis.get("linked_claim_ids", [])),
        "decision_source": maybe_text(hypothesis.get("decision_source")),
        "evidence_refs_json": json_text(hypothesis.get("evidence_refs", [])),
        "source_ids_json": json_text(hypothesis.get("source_ids", [])),
        "provenance_json": json_text(hypothesis.get("provenance", {})),
        "lineage_json": json_text(hypothesis.get("lineage", [])),
        "confidence": hypothesis.get("confidence"),
        "created_at_utc": maybe_text(hypothesis.get("created_at_utc")),
        "updated_at_utc": maybe_text(hypothesis.get("updated_at_utc")),
        "carryover_from_round_id": maybe_text(hypothesis.get("carryover_from_round_id")),
        "carryover_from_hypothesis_id": maybe_text(hypothesis.get("carryover_from_hypothesis_id")),
        "history_json": json_text(hypothesis.get("history", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(hypothesis),
    }


def challenge_row_from_payload(
    ticket: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "ticket_id": maybe_text(ticket.get("ticket_id")),
        "run_id": maybe_text(ticket.get("run_id")),
        "round_id": maybe_text(ticket.get("round_id")),
        "created_at_utc": maybe_text(ticket.get("created_at_utc")),
        "status": maybe_text(ticket.get("status")),
        "priority": maybe_text(ticket.get("priority")),
        "owner_role": maybe_text(ticket.get("owner_role")),
        "title": maybe_text(ticket.get("title")),
        "challenge_statement": maybe_text(ticket.get("challenge_statement")),
        "target_claim_id": maybe_text(ticket.get("target_claim_id")),
        "target_hypothesis_id": maybe_text(ticket.get("target_hypothesis_id")),
        "decision_source": maybe_text(ticket.get("decision_source")),
        "evidence_refs_json": json_text(ticket.get("evidence_refs", [])),
        "source_ids_json": json_text(ticket.get("source_ids", [])),
        "provenance_json": json_text(ticket.get("provenance", {})),
        "lineage_json": json_text(ticket.get("lineage", [])),
        "linked_artifact_refs_json": json_text(ticket.get("linked_artifact_refs", [])),
        "related_task_ids_json": json_text(ticket.get("related_task_ids", [])),
        "closed_at_utc": maybe_text(ticket.get("closed_at_utc")),
        "closed_by_role": maybe_text(ticket.get("closed_by_role")),
        "resolution": maybe_text(ticket.get("resolution")),
        "resolution_note": maybe_text(ticket.get("resolution_note")),
        "history_json": json_text(ticket.get("history", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(ticket),
    }


def board_task_row_from_payload(
    task: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "task_id": maybe_text(task.get("task_id")),
        "run_id": maybe_text(task.get("run_id")),
        "round_id": maybe_text(task.get("round_id")),
        "title": maybe_text(task.get("title")),
        "task_text": maybe_text(task.get("task_text")),
        "task_type": maybe_text(task.get("task_type")),
        "status": maybe_text(task.get("status")),
        "owner_role": maybe_text(task.get("owner_role")),
        "priority": maybe_text(task.get("priority")),
        "source_round_id": maybe_text(task.get("source_round_id")),
        "source_ticket_id": maybe_text(task.get("source_ticket_id")),
        "source_hypothesis_id": maybe_text(task.get("source_hypothesis_id")),
        "carryover_from_round_id": maybe_text(task.get("carryover_from_round_id")),
        "carryover_from_task_id": maybe_text(task.get("carryover_from_task_id")),
        "decision_source": maybe_text(task.get("decision_source")),
        "evidence_refs_json": json_text(task.get("evidence_refs", [])),
        "source_ids_json": json_text(task.get("source_ids", [])),
        "provenance_json": json_text(task.get("provenance", {})),
        "lineage_json": json_text(task.get("lineage", [])),
        "linked_artifact_refs_json": json_text(task.get("linked_artifact_refs", [])),
        "related_ids_json": json_text(task.get("related_ids", [])),
        "created_at_utc": maybe_text(task.get("created_at_utc")),
        "updated_at_utc": maybe_text(task.get("updated_at_utc")),
        "claimed_at_utc": maybe_text(task.get("claimed_at_utc")),
        "history_json": json_text(task.get("history", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(task),
    }


def round_transition_row_from_payload(
    transition: dict[str, Any],
    *,
    board_revision: int,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "transition_id": maybe_text(transition.get("transition_id")),
        "run_id": maybe_text(transition.get("run_id")),
        "round_id": maybe_text(transition.get("round_id")),
        "source_round_id": maybe_text(transition.get("source_round_id")),
        "generated_at_utc": maybe_text(transition.get("generated_at_utc")),
        "operation": maybe_text(transition.get("operation")),
        "event_id": maybe_text(transition.get("event_id")),
        "board_revision": coerce_int(
            transition.get("board_revision") or board_revision
        ),
        "prior_round_ids_json": json_text(transition.get("prior_round_ids", [])),
        "cross_round_query_hints_json": json_text(
            transition.get("cross_round_query_hints", {})
        ),
        "counts_json": json_text(transition.get("counts", {})),
        "warnings_json": json_text(transition.get("warnings", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(transition),
    }

def fetch_runtime_control_freeze(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    normalized_run_id = maybe_text(run_id)
    normalized_round_id = maybe_text(round_id)
    clauses: list[str] = []
    params: list[str] = []
    if normalized_run_id:
        clauses.append("run_id = ?")
        params.append(normalized_run_id)
    if normalized_round_id:
        clauses.append("round_id = ?")
        params.append(normalized_round_id)
    if not clauses:
        return None
    row = connection.execute(
        f"""
        SELECT *
        FROM report_basis_freezes
        WHERE {' AND '.join(clauses)}
        ORDER BY updated_at_utc DESC, freeze_id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    return payload_from_db_row(row)

def fetch_snapshot_payload(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    normalized_run_id = maybe_text(run_id)
    normalized_round_id = maybe_text(round_id)
    clauses: list[str] = []
    params: list[str] = []
    if normalized_run_id:
        clauses.append("run_id = ?")
        params.append(normalized_run_id)
    if normalized_round_id:
        clauses.append("round_id = ?")
        params.append(normalized_round_id)
    if not clauses:
        return None
    row = connection.execute(
        f"""
        SELECT raw_json
        FROM {table_name}
        WHERE {' AND '.join(clauses)}
        ORDER BY generated_at_utc DESC, snapshot_id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    payload = decode_json(maybe_text(row["raw_json"]), {})
    return payload if isinstance(payload, dict) else None

def fetch_json_rows(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    timestamp_column: str,
    run_id: str = "",
    round_id: str = "",
    extra_order_by: str = "",
) -> list[dict[str, Any]]:
    normalized_run_id = maybe_text(run_id)
    normalized_round_id = maybe_text(round_id)
    clauses: list[str] = []
    params: list[str] = []
    if normalized_run_id:
        clauses.append("run_id = ?")
        params.append(normalized_run_id)
    if normalized_round_id:
        clauses.append("round_id = ?")
        params.append(normalized_round_id)
    if not clauses:
        return []
    order_parts = [extra_order_by] if extra_order_by else []
    order_parts.append(timestamp_column)
    order_parts.append(id_column)
    rows = connection.execute(
        f"""
        SELECT *
        FROM {table_name}
        WHERE {' AND '.join(clauses)}
        ORDER BY {', '.join(order_parts)}
        """,
        tuple(params),
    ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(payload_from_db_row(row))
    return results

def latest_json_row(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    timestamp_column: str,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    rows = fetch_json_rows(
        connection,
        table_name=table_name,
        id_column=id_column,
        timestamp_column=timestamp_column,
        run_id=run_id,
        round_id=round_id,
    )
    return rows[-1] if rows else None

def latest_raw_json_row(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    timestamp_column: str,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    normalized_run_id = maybe_text(run_id)
    normalized_round_id = maybe_text(round_id)
    clauses: list[str] = []
    params: list[str] = []
    if normalized_run_id:
        clauses.append("run_id = ?")
        params.append(normalized_run_id)
    if normalized_round_id:
        clauses.append("round_id = ?")
        params.append(normalized_round_id)
    if not clauses:
        return None
    row = connection.execute(
        f"""
        SELECT raw_json
        FROM {table_name}
        WHERE {' AND '.join(clauses)}
        ORDER BY {timestamp_column} DESC, {id_column} DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    payload = decode_json(maybe_text(row["raw_json"]), {})
    return payload if isinstance(payload, dict) else None

def latest_json_row_where(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    timestamp_column: str,
    filters: dict[str, Any],
) -> dict[str, Any] | None:
    clauses: list[str] = []
    params: list[str] = []
    for column_name, value in filters.items():
        text = maybe_text(value)
        if not text:
            continue
        clauses.append(f"{column_name} = ?")
        params.append(text)
    if not clauses:
        return None
    row = connection.execute(
        f"""
        SELECT *
        FROM {table_name}
        WHERE {' AND '.join(clauses)}
        ORDER BY {timestamp_column} DESC, {id_column} DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    return payload_from_db_row(row)

def latest_raw_json_row_where(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    timestamp_column: str,
    filters: dict[str, Any],
) -> dict[str, Any] | None:
    clauses: list[str] = []
    params: list[str] = []
    for column_name, value in filters.items():
        text = maybe_text(value)
        if not text:
            continue
        clauses.append(f"{column_name} = ?")
        params.append(text)
    if not clauses:
        return None
    row = connection.execute(
        f"""
        SELECT raw_json
        FROM {table_name}
        WHERE {' AND '.join(clauses)}
        ORDER BY {timestamp_column} DESC, {id_column} DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    payload = decode_json(maybe_text(row["raw_json"]), {})
    return payload if isinstance(payload, dict) else None


__all__ = [
    "BOOLEAN_ROW_COLUMNS",
    "board_task_row_from_payload",
    "challenge_row_from_payload",
    "cleaned_wrapper_record",
    "coerce_int",
    "decode_json",
    "dict_items",
    "event_row_from_payload",
    "hypothesis_row_from_payload",
    "json_text",
    "list_items",
    "maybe_text",
    "merged_lineage",
    "normalize_space",
    "normalized_provenance",
    "note_row_from_payload",
    "payload_from_db_row",
    "round_transition_row_from_payload",
    "stable_hash",
    "unique_texts",
    "utc_now_iso",
    "write_board_event_row",
    "write_board_note_row",
    "write_board_task_row",
    "write_challenge_row",
    "write_controller_snapshot_row",
    "write_council_decision_record_row",
    "write_expert_report_record_row",
    "write_falsification_probe_row",
    "write_falsification_probe_snapshot_row",
    "write_final_publication_row",
    "write_gate_snapshot_row",
    "write_hypothesis_row",
    "write_moderator_action_row",
    "write_moderator_action_snapshot_row",
    "write_orchestration_plan_row",
    "write_orchestration_plan_step_row",
    "write_report_basis_freeze_item_row",
    "write_report_basis_freeze_record_row",
    "write_report_basis_freeze_row",
    "write_reporting_handoff_row",
    "write_round_readiness_assessment_row",
    "write_round_task_snapshot_row",
    "write_round_transition_row",
    "write_supervisor_snapshot_row",
    "fetch_runtime_control_freeze",
    "fetch_snapshot_payload",
    "fetch_json_rows",
    "latest_json_row",
    "latest_json_row_where",
    "latest_raw_json_row",
    "latest_raw_json_row_where",
]
