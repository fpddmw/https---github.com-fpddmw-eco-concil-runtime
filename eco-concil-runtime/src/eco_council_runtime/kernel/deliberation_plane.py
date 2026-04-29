from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..canonical_contracts import canonical_contract, validate_canonical_payload
from ..deliberation_target_semantics import (
    deliberation_anchor_fields,
    normalized_deliberation_target,
    source_proposal_id_from_payload,
)
from ..phase2_action_semantics import action_is_readiness_blocker, maybe_bool
from ..reporting_status import (
    normalize_reporting_handoff_status,
    reporting_gate_state,
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS board_runs (
    run_id TEXT PRIMARY KEY,
    board_revision INTEGER NOT NULL DEFAULT 0,
    updated_at_utc TEXT NOT NULL DEFAULT '',
    board_path TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS board_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    event_index INTEGER NOT NULL DEFAULT 0,
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_board_events_round_created
ON board_events(run_id, round_id, created_at_utc, event_id);
CREATE INDEX IF NOT EXISTS idx_board_events_round_sequence
ON board_events(run_id, round_id, event_index, event_id);

CREATE TABLE IF NOT EXISTS board_notes (
    note_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    author_role TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    note_text TEXT NOT NULL DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    linked_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
    related_ids_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_board_notes_round_created
ON board_notes(run_id, round_id, created_at_utc, note_id);

CREATE TABLE IF NOT EXISTS hypothesis_cards (
    hypothesis_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    statement TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    owner_role TEXT NOT NULL DEFAULT '',
    linked_claim_ids_json TEXT NOT NULL DEFAULT '[]',
    decision_source TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    source_ids_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    confidence REAL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    updated_at_utc TEXT NOT NULL DEFAULT '',
    carryover_from_round_id TEXT NOT NULL DEFAULT '',
    carryover_from_hypothesis_id TEXT NOT NULL DEFAULT '',
    history_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_hypothesis_cards_round_status
ON hypothesis_cards(run_id, round_id, status, updated_at_utc, hypothesis_id);

CREATE TABLE IF NOT EXISTS challenge_tickets (
    ticket_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    priority TEXT NOT NULL DEFAULT '',
    owner_role TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    challenge_statement TEXT NOT NULL DEFAULT '',
    target_claim_id TEXT NOT NULL DEFAULT '',
    target_hypothesis_id TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    source_ids_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    linked_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
    related_task_ids_json TEXT NOT NULL DEFAULT '[]',
    closed_at_utc TEXT NOT NULL DEFAULT '',
    closed_by_role TEXT NOT NULL DEFAULT '',
    resolution TEXT NOT NULL DEFAULT '',
    resolution_note TEXT NOT NULL DEFAULT '',
    history_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_challenge_tickets_round_status
ON challenge_tickets(run_id, round_id, status, created_at_utc, ticket_id);

CREATE TABLE IF NOT EXISTS board_tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    task_text TEXT NOT NULL DEFAULT '',
    task_type TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    owner_role TEXT NOT NULL DEFAULT '',
    priority TEXT NOT NULL DEFAULT '',
    source_round_id TEXT NOT NULL DEFAULT '',
    source_ticket_id TEXT NOT NULL DEFAULT '',
    source_hypothesis_id TEXT NOT NULL DEFAULT '',
    carryover_from_round_id TEXT NOT NULL DEFAULT '',
    carryover_from_task_id TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    source_ids_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    linked_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
    related_ids_json TEXT NOT NULL DEFAULT '[]',
    created_at_utc TEXT NOT NULL DEFAULT '',
    updated_at_utc TEXT NOT NULL DEFAULT '',
    claimed_at_utc TEXT NOT NULL DEFAULT '',
    history_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_board_tasks_round_status
ON board_tasks(run_id, round_id, status, updated_at_utc, task_id);

CREATE TABLE IF NOT EXISTS round_transitions (
    transition_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    source_round_id TEXT NOT NULL DEFAULT '',
    generated_at_utc TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    event_id TEXT NOT NULL DEFAULT '',
    board_revision INTEGER NOT NULL DEFAULT 0,
    prior_round_ids_json TEXT NOT NULL DEFAULT '[]',
    cross_round_query_hints_json TEXT NOT NULL DEFAULT '{}',
    counts_json TEXT NOT NULL DEFAULT '{}',
    warnings_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_round_transitions_round
ON round_transitions(run_id, round_id, generated_at_utc, transition_id);

CREATE TABLE IF NOT EXISTS transition_requests (
    request_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    updated_at_utc TEXT NOT NULL DEFAULT '',
    transition_kind TEXT NOT NULL DEFAULT '',
    request_status TEXT NOT NULL DEFAULT '',
    requested_by_role TEXT NOT NULL DEFAULT '',
    required_approval_role TEXT NOT NULL DEFAULT '',
    requested_surface TEXT NOT NULL DEFAULT '',
    requested_action TEXT NOT NULL DEFAULT '',
    requested_command_name TEXT NOT NULL DEFAULT '',
    source_round_id TEXT NOT NULL DEFAULT '',
    target_round_id TEXT NOT NULL DEFAULT '',
    rationale TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    request_payload_json TEXT NOT NULL DEFAULT '{}',
    operator_notes_json TEXT NOT NULL DEFAULT '[]',
    decision_ids_json TEXT NOT NULL DEFAULT '[]',
    latest_decision_id TEXT NOT NULL DEFAULT '',
    latest_decision_status TEXT NOT NULL DEFAULT '',
    latest_decision_by_role TEXT NOT NULL DEFAULT '',
    latest_decision_reason TEXT NOT NULL DEFAULT '',
    approved_at_utc TEXT NOT NULL DEFAULT '',
    rejected_at_utc TEXT NOT NULL DEFAULT '',
    committed_at_utc TEXT NOT NULL DEFAULT '',
    committed_by_role TEXT NOT NULL DEFAULT '',
    committed_object_kind TEXT NOT NULL DEFAULT '',
    committed_object_id TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_transition_requests_round_status
ON transition_requests(run_id, round_id, request_status, updated_at_utc, request_id);
CREATE INDEX IF NOT EXISTS idx_transition_requests_round_kind
ON transition_requests(run_id, round_id, transition_kind, updated_at_utc, request_id);
CREATE INDEX IF NOT EXISTS idx_transition_requests_requester
ON transition_requests(run_id, round_id, requested_by_role, request_id);

CREATE TABLE IF NOT EXISTS transition_approvals (
    approval_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    approved_at_utc TEXT NOT NULL DEFAULT '',
    approved_by_role TEXT NOT NULL DEFAULT '',
    decision_status TEXT NOT NULL DEFAULT '',
    decision_reason TEXT NOT NULL DEFAULT '',
    transition_kind TEXT NOT NULL DEFAULT '',
    requested_by_role TEXT NOT NULL DEFAULT '',
    requested_command_name TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    operator_notes_json TEXT NOT NULL DEFAULT '[]',
    request_snapshot_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_transition_approvals_request
ON transition_approvals(request_id, approved_at_utc, approval_id);
CREATE INDEX IF NOT EXISTS idx_transition_approvals_round
ON transition_approvals(run_id, round_id, approved_at_utc, approval_id);

CREATE TABLE IF NOT EXISTS transition_rejections (
    rejection_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    rejected_at_utc TEXT NOT NULL DEFAULT '',
    rejected_by_role TEXT NOT NULL DEFAULT '',
    decision_status TEXT NOT NULL DEFAULT '',
    decision_reason TEXT NOT NULL DEFAULT '',
    transition_kind TEXT NOT NULL DEFAULT '',
    requested_by_role TEXT NOT NULL DEFAULT '',
    requested_command_name TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    operator_notes_json TEXT NOT NULL DEFAULT '[]',
    request_snapshot_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_transition_rejections_request
ON transition_rejections(request_id, rejected_at_utc, rejection_id);
CREATE INDEX IF NOT EXISTS idx_transition_rejections_round
ON transition_rejections(run_id, round_id, rejected_at_utc, rejection_id);

CREATE TABLE IF NOT EXISTS skill_approval_requests (
    request_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    updated_at_utc TEXT NOT NULL DEFAULT '',
    request_status TEXT NOT NULL DEFAULT '',
    skill_name TEXT NOT NULL DEFAULT '',
    skill_layer TEXT NOT NULL DEFAULT '',
    requested_by_role TEXT NOT NULL DEFAULT '',
    requested_actor_role TEXT NOT NULL DEFAULT '',
    required_approval_role TEXT NOT NULL DEFAULT '',
    requested_surface TEXT NOT NULL DEFAULT '',
    requested_action TEXT NOT NULL DEFAULT '',
    requested_command_name TEXT NOT NULL DEFAULT '',
    rationale TEXT NOT NULL DEFAULT '',
    requested_skill_args_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    request_payload_json TEXT NOT NULL DEFAULT '{}',
    operator_notes_json TEXT NOT NULL DEFAULT '[]',
    decision_ids_json TEXT NOT NULL DEFAULT '[]',
    latest_decision_id TEXT NOT NULL DEFAULT '',
    latest_decision_status TEXT NOT NULL DEFAULT '',
    latest_decision_by_role TEXT NOT NULL DEFAULT '',
    latest_decision_reason TEXT NOT NULL DEFAULT '',
    approved_at_utc TEXT NOT NULL DEFAULT '',
    rejected_at_utc TEXT NOT NULL DEFAULT '',
    consumed_at_utc TEXT NOT NULL DEFAULT '',
    consumed_by_role TEXT NOT NULL DEFAULT '',
    consumed_receipt_id TEXT NOT NULL DEFAULT '',
    consumed_event_id TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_skill_approval_requests_round_status
ON skill_approval_requests(run_id, round_id, request_status, updated_at_utc, request_id);
CREATE INDEX IF NOT EXISTS idx_skill_approval_requests_round_skill
ON skill_approval_requests(run_id, round_id, skill_name, request_status, updated_at_utc, request_id);
CREATE INDEX IF NOT EXISTS idx_skill_approval_requests_requester
ON skill_approval_requests(run_id, round_id, requested_by_role, request_id);
CREATE INDEX IF NOT EXISTS idx_skill_approval_requests_actor
ON skill_approval_requests(run_id, round_id, requested_actor_role, request_id);

CREATE TABLE IF NOT EXISTS skill_approvals (
    approval_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    approved_at_utc TEXT NOT NULL DEFAULT '',
    approved_by_role TEXT NOT NULL DEFAULT '',
    decision_status TEXT NOT NULL DEFAULT '',
    decision_reason TEXT NOT NULL DEFAULT '',
    skill_name TEXT NOT NULL DEFAULT '',
    skill_layer TEXT NOT NULL DEFAULT '',
    requested_by_role TEXT NOT NULL DEFAULT '',
    requested_actor_role TEXT NOT NULL DEFAULT '',
    requested_command_name TEXT NOT NULL DEFAULT '',
    requested_skill_args_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    operator_notes_json TEXT NOT NULL DEFAULT '[]',
    request_snapshot_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_skill_approvals_request
ON skill_approvals(request_id, approved_at_utc, approval_id);
CREATE INDEX IF NOT EXISTS idx_skill_approvals_round
ON skill_approvals(run_id, round_id, approved_at_utc, approval_id);

CREATE TABLE IF NOT EXISTS skill_approval_rejections (
    rejection_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    rejected_at_utc TEXT NOT NULL DEFAULT '',
    rejected_by_role TEXT NOT NULL DEFAULT '',
    decision_status TEXT NOT NULL DEFAULT '',
    decision_reason TEXT NOT NULL DEFAULT '',
    skill_name TEXT NOT NULL DEFAULT '',
    skill_layer TEXT NOT NULL DEFAULT '',
    requested_by_role TEXT NOT NULL DEFAULT '',
    requested_actor_role TEXT NOT NULL DEFAULT '',
    requested_command_name TEXT NOT NULL DEFAULT '',
    requested_skill_args_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    operator_notes_json TEXT NOT NULL DEFAULT '[]',
    request_snapshot_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_skill_approval_rejections_request
ON skill_approval_rejections(request_id, rejected_at_utc, rejection_id);
CREATE INDEX IF NOT EXISTS idx_skill_approval_rejections_round
ON skill_approval_rejections(run_id, round_id, rejected_at_utc, rejection_id);

CREATE TABLE IF NOT EXISTS skill_approval_consumptions (
    consumption_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    approval_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    consumed_at_utc TEXT NOT NULL DEFAULT '',
    consumed_by_role TEXT NOT NULL DEFAULT '',
    consumption_status TEXT NOT NULL DEFAULT '',
    skill_name TEXT NOT NULL DEFAULT '',
    skill_layer TEXT NOT NULL DEFAULT '',
    requested_actor_role TEXT NOT NULL DEFAULT '',
    execution_receipt_id TEXT NOT NULL DEFAULT '',
    execution_event_id TEXT NOT NULL DEFAULT '',
    execution_status TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_skill_approval_consumptions_request
ON skill_approval_consumptions(request_id, consumed_at_utc, consumption_id);
CREATE INDEX IF NOT EXISTS idx_skill_approval_consumptions_round
ON skill_approval_consumptions(run_id, round_id, consumed_at_utc, consumption_id);

CREATE TABLE IF NOT EXISTS report_basis_freezes (
    freeze_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL DEFAULT '',
    gate_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    report_basis_status TEXT NOT NULL DEFAULT '',
    controller_status TEXT NOT NULL DEFAULT '',
    supervisor_status TEXT NOT NULL DEFAULT '',
    planning_mode TEXT NOT NULL DEFAULT '',
    report_basis_freeze_allowed INTEGER NOT NULL DEFAULT 0,
    gate_reasons_json TEXT NOT NULL DEFAULT '[]',
    recommended_next_skills_json TEXT NOT NULL DEFAULT '[]',
    reporting_ready INTEGER NOT NULL DEFAULT 0,
    reporting_handoff_status TEXT NOT NULL DEFAULT '',
    reporting_blockers_json TEXT NOT NULL DEFAULT '[]',
    controller_artifact_path TEXT NOT NULL DEFAULT '',
    gate_artifact_path TEXT NOT NULL DEFAULT '',
    supervisor_artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_report_basis_freezes_round_updated
ON report_basis_freezes(run_id, round_id, updated_at_utc, freeze_id);
CREATE INDEX IF NOT EXISTS idx_report_basis_freezes_round_statuses
ON report_basis_freezes(
    run_id,
    round_id,
    report_basis_status,
    gate_status,
    supervisor_status,
    freeze_id
);

CREATE TABLE IF NOT EXISTS controller_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    controller_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    controller_status TEXT NOT NULL DEFAULT '',
    planning_mode TEXT NOT NULL DEFAULT '',
    current_stage TEXT NOT NULL DEFAULT '',
    failed_stage TEXT NOT NULL DEFAULT '',
    resume_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    gate_status TEXT NOT NULL DEFAULT '',
    report_basis_status TEXT NOT NULL DEFAULT '',
    resume_recommended INTEGER NOT NULL DEFAULT 0,
    restart_recommended INTEGER NOT NULL DEFAULT 0,
    resume_from_stage TEXT NOT NULL DEFAULT '',
    completed_stage_names_json TEXT NOT NULL DEFAULT '[]',
    pending_stage_names_json TEXT NOT NULL DEFAULT '[]',
    gate_reasons_json TEXT NOT NULL DEFAULT '[]',
    recommended_next_skills_json TEXT NOT NULL DEFAULT '[]',
    execution_policy_json TEXT NOT NULL DEFAULT '{}',
    progress_json TEXT NOT NULL DEFAULT '{}',
    recovery_json TEXT NOT NULL DEFAULT '{}',
    planning_json TEXT NOT NULL DEFAULT '{}',
    planning_attempts_json TEXT NOT NULL DEFAULT '[]',
    stage_contracts_json TEXT NOT NULL DEFAULT '{}',
    steps_json TEXT NOT NULL DEFAULT '[]',
    artifacts_json TEXT NOT NULL DEFAULT '{}',
    failure_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_controller_snapshots_round
ON controller_snapshots(run_id, round_id, generated_at_utc, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_controller_snapshots_round_status
ON controller_snapshots(
    run_id,
    round_id,
    controller_status,
    planning_mode,
    snapshot_id
);

CREATE TABLE IF NOT EXISTS gate_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    gate_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    stage_name TEXT NOT NULL DEFAULT '',
    gate_handler TEXT NOT NULL DEFAULT '',
    gate_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    report_basis_freeze_allowed INTEGER NOT NULL DEFAULT 0,
    decision_source TEXT NOT NULL DEFAULT '',
    report_basis_resolution_mode TEXT NOT NULL DEFAULT '',
    gate_reasons_json TEXT NOT NULL DEFAULT '[]',
    supporting_proposal_ids_json TEXT NOT NULL DEFAULT '[]',
    rejected_proposal_ids_json TEXT NOT NULL DEFAULT '[]',
    supporting_opinion_ids_json TEXT NOT NULL DEFAULT '[]',
    rejected_opinion_ids_json TEXT NOT NULL DEFAULT '[]',
    council_input_counts_json TEXT NOT NULL DEFAULT '{}',
    recommended_next_skills_json TEXT NOT NULL DEFAULT '[]',
    warnings_json TEXT NOT NULL DEFAULT '[]',
    readiness_path TEXT NOT NULL DEFAULT '',
    output_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_gate_snapshots_round
ON gate_snapshots(run_id, round_id, generated_at_utc, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_gate_snapshots_round_handler
ON gate_snapshots(
    run_id,
    round_id,
    stage_name,
    gate_handler,
    gate_status,
    snapshot_id
);

CREATE TABLE IF NOT EXISTS supervisor_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    supervisor_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    supervisor_status TEXT NOT NULL DEFAULT '',
    supervisor_substatus TEXT NOT NULL DEFAULT '',
    phase2_posture TEXT NOT NULL DEFAULT '',
    terminal_state TEXT NOT NULL DEFAULT '',
    recovery_posture TEXT NOT NULL DEFAULT '',
    operator_action TEXT NOT NULL DEFAULT '',
    controller_status TEXT NOT NULL DEFAULT '',
    planning_mode TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    gate_status TEXT NOT NULL DEFAULT '',
    report_basis_status TEXT NOT NULL DEFAULT '',
    reporting_ready INTEGER NOT NULL DEFAULT 0,
    reporting_handoff_status TEXT NOT NULL DEFAULT '',
    resume_status TEXT NOT NULL DEFAULT '',
    current_stage TEXT NOT NULL DEFAULT '',
    failed_stage TEXT NOT NULL DEFAULT '',
    resume_recommended INTEGER NOT NULL DEFAULT 0,
    restart_recommended INTEGER NOT NULL DEFAULT 0,
    resume_from_stage TEXT NOT NULL DEFAULT '',
    reporting_blockers_json TEXT NOT NULL DEFAULT '[]',
    recommended_next_skills_json TEXT NOT NULL DEFAULT '[]',
    execution_policy_json TEXT NOT NULL DEFAULT '{}',
    round_transition_json TEXT NOT NULL DEFAULT '{}',
    top_actions_json TEXT NOT NULL DEFAULT '[]',
    operator_notes_json TEXT NOT NULL DEFAULT '[]',
    inspection_paths_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_supervisor_snapshots_round
ON supervisor_snapshots(run_id, round_id, generated_at_utc, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_supervisor_snapshots_round_status
ON supervisor_snapshots(
    run_id,
    round_id,
    supervisor_status,
    reporting_ready,
    snapshot_id
);

CREATE TABLE IF NOT EXISTS orchestration_plans (
    plan_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    planning_status TEXT NOT NULL DEFAULT '',
    planning_mode TEXT NOT NULL DEFAULT '',
    controller_authority TEXT NOT NULL DEFAULT '',
    plan_source TEXT NOT NULL DEFAULT '',
    council_execution_mode TEXT NOT NULL DEFAULT '',
    downstream_posture TEXT NOT NULL DEFAULT '',
    probe_stage_included INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    execution_queue_count INTEGER NOT NULL DEFAULT 0,
    gate_step_count INTEGER NOT NULL DEFAULT 0,
    derived_export_count INTEGER NOT NULL DEFAULT 0,
    post_gate_step_count INTEGER NOT NULL DEFAULT 0,
    planned_stage_count INTEGER NOT NULL DEFAULT 0,
    assigned_role_hints_json TEXT NOT NULL DEFAULT '[]',
    phase_decision_basis_json TEXT NOT NULL DEFAULT '{}',
    agent_turn_hints_json TEXT NOT NULL DEFAULT '{}',
    observed_state_json TEXT NOT NULL DEFAULT '{}',
    inputs_json TEXT NOT NULL DEFAULT '{}',
    execution_queue_json TEXT NOT NULL DEFAULT '[]',
    gate_steps_json TEXT NOT NULL DEFAULT '[]',
    derived_exports_json TEXT NOT NULL DEFAULT '[]',
    post_gate_steps_json TEXT NOT NULL DEFAULT '[]',
    stop_conditions_json TEXT NOT NULL DEFAULT '[]',
    fallback_path_json TEXT NOT NULL DEFAULT '[]',
    planning_notes_json TEXT NOT NULL DEFAULT '[]',
    deliberation_sync_json TEXT NOT NULL DEFAULT '{}',
    step_counts_json TEXT NOT NULL DEFAULT '{}',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_orchestration_plans_round
ON orchestration_plans(run_id, round_id, generated_at_utc, plan_id);
CREATE INDEX IF NOT EXISTS idx_orchestration_plans_round_mode
ON orchestration_plans(
    run_id,
    round_id,
    planning_mode,
    controller_authority,
    plan_source,
    plan_id
);

CREATE TABLE IF NOT EXISTS orchestration_plan_steps (
    step_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    plan_step_group TEXT NOT NULL DEFAULT '',
    step_index INTEGER NOT NULL DEFAULT 0,
    planning_mode TEXT NOT NULL DEFAULT '',
    controller_authority TEXT NOT NULL DEFAULT '',
    plan_source TEXT NOT NULL DEFAULT '',
    phase_group TEXT NOT NULL DEFAULT '',
    stage_name TEXT NOT NULL DEFAULT '',
    stage_kind TEXT NOT NULL DEFAULT '',
    skill_name TEXT NOT NULL DEFAULT '',
    expected_skill_name TEXT NOT NULL DEFAULT '',
    assigned_role_hint TEXT NOT NULL DEFAULT '',
    blocking INTEGER NOT NULL DEFAULT 0,
    resume_policy TEXT NOT NULL DEFAULT '',
    gate_handler TEXT NOT NULL DEFAULT '',
    readiness_stage_name TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    operator_summary TEXT NOT NULL DEFAULT '',
    expected_output_path TEXT NOT NULL DEFAULT '',
    required_for_controller INTEGER NOT NULL DEFAULT 1,
    export_mode TEXT NOT NULL DEFAULT '',
    required_previous_stages_json TEXT NOT NULL DEFAULT '[]',
    skill_args_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_orchestration_plan_steps_plan
ON orchestration_plan_steps(plan_id, plan_step_group, step_index, step_id);
CREATE INDEX IF NOT EXISTS idx_orchestration_plan_steps_round_stage
ON orchestration_plan_steps(run_id, round_id, stage_name, skill_name, step_id);

CREATE TABLE IF NOT EXISTS moderator_actions (
    action_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    action_rank INTEGER NOT NULL DEFAULT 0,
    action_kind TEXT NOT NULL DEFAULT '',
    priority TEXT NOT NULL DEFAULT '',
    assigned_role TEXT NOT NULL DEFAULT '',
    target_hypothesis_id TEXT NOT NULL DEFAULT '',
    target_claim_id TEXT NOT NULL DEFAULT '',
    target_ticket_id TEXT NOT NULL DEFAULT '',
    target_actor_id TEXT NOT NULL DEFAULT '',
    target_proposal_id TEXT NOT NULL DEFAULT '',
    target_object_kind TEXT NOT NULL DEFAULT '',
    target_object_id TEXT NOT NULL DEFAULT '',
    issue_label TEXT NOT NULL DEFAULT '',
    target_route_id TEXT NOT NULL DEFAULT '',
    target_assessment_id TEXT NOT NULL DEFAULT '',
    target_linkage_id TEXT NOT NULL DEFAULT '',
    target_gap_id TEXT NOT NULL DEFAULT '',
    source_proposal_id TEXT NOT NULL DEFAULT '',
    controversy_gap TEXT NOT NULL DEFAULT '',
    recommended_lane TEXT NOT NULL DEFAULT '',
    probe_candidate INTEGER NOT NULL DEFAULT 0,
    readiness_blocker INTEGER NOT NULL DEFAULT 1,
    objective TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    source_ids_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_rank
ON moderator_actions(run_id, round_id, action_rank, action_id);
CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_target
ON moderator_actions(run_id, round_id, target_object_kind, target_object_id, action_id);
CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_actor
ON moderator_actions(run_id, round_id, target_actor_id, action_id);
CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_proposal_target
ON moderator_actions(run_id, round_id, target_proposal_id, action_id);
CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_issue
ON moderator_actions(run_id, round_id, issue_label, source_proposal_id, action_id);

CREATE TABLE IF NOT EXISTS moderator_action_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    action_source TEXT NOT NULL DEFAULT '',
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    action_count INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_moderator_action_snapshots_round
ON moderator_action_snapshots(run_id, round_id, generated_at_utc, snapshot_id);

CREATE TABLE IF NOT EXISTS falsification_probes (
    probe_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    opened_at_utc TEXT NOT NULL DEFAULT '',
    probe_status TEXT NOT NULL DEFAULT '',
    action_id TEXT NOT NULL DEFAULT '',
    priority TEXT NOT NULL DEFAULT '',
    owner_role TEXT NOT NULL DEFAULT '',
    target_hypothesis_id TEXT NOT NULL DEFAULT '',
    target_claim_id TEXT NOT NULL DEFAULT '',
    target_ticket_id TEXT NOT NULL DEFAULT '',
    target_actor_id TEXT NOT NULL DEFAULT '',
    target_proposal_id TEXT NOT NULL DEFAULT '',
    target_object_kind TEXT NOT NULL DEFAULT '',
    target_object_id TEXT NOT NULL DEFAULT '',
    issue_label TEXT NOT NULL DEFAULT '',
    target_route_id TEXT NOT NULL DEFAULT '',
    target_assessment_id TEXT NOT NULL DEFAULT '',
    target_linkage_id TEXT NOT NULL DEFAULT '',
    target_gap_id TEXT NOT NULL DEFAULT '',
    source_proposal_id TEXT NOT NULL DEFAULT '',
    probe_type TEXT NOT NULL DEFAULT '',
    controversy_gap TEXT NOT NULL DEFAULT '',
    recommended_lane TEXT NOT NULL DEFAULT '',
    probe_goal TEXT NOT NULL DEFAULT '',
    falsification_question TEXT NOT NULL DEFAULT '',
    requested_skills_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    source_ids_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_status
ON falsification_probes(run_id, round_id, probe_status, opened_at_utc, probe_id);
CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_target
ON falsification_probes(run_id, round_id, target_object_kind, target_object_id, probe_id);
CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_actor
ON falsification_probes(run_id, round_id, target_actor_id, probe_id);
CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_proposal_target
ON falsification_probes(run_id, round_id, target_proposal_id, probe_id);
CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_issue
ON falsification_probes(run_id, round_id, issue_label, source_proposal_id, probe_id);

CREATE TABLE IF NOT EXISTS falsification_probe_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    action_source TEXT NOT NULL DEFAULT '',
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    probe_count INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_falsification_probe_snapshots_round
ON falsification_probe_snapshots(run_id, round_id, generated_at_utc, snapshot_id);

CREATE TABLE IF NOT EXISTS round_readiness_assessments (
    readiness_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    sufficient_for_report_basis INTEGER NOT NULL DEFAULT 0,
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    next_actions_source TEXT NOT NULL DEFAULT '',
    probes_source TEXT NOT NULL DEFAULT '',
    agenda_counts_json TEXT NOT NULL DEFAULT '{}',
    counts_json TEXT NOT NULL DEFAULT '{}',
    controversy_gap_counts_json TEXT NOT NULL DEFAULT '{}',
    probe_type_counts_json TEXT NOT NULL DEFAULT '{}',
    gate_reasons_json TEXT NOT NULL DEFAULT '[]',
    recommended_next_skills_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_round_readiness_assessments_round
ON round_readiness_assessments(run_id, round_id, generated_at_utc, readiness_id);

CREATE TABLE IF NOT EXISTS report_basis_freeze_records (
    basis_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    report_basis_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    readiness_source TEXT NOT NULL DEFAULT '',
    next_actions_source TEXT NOT NULL DEFAULT '',
    board_brief_source TEXT NOT NULL DEFAULT '',
    basis_selection_mode TEXT NOT NULL DEFAULT '',
    basis_counts_json TEXT NOT NULL DEFAULT '{}',
    selected_basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    selected_evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    gate_reasons_json TEXT NOT NULL DEFAULT '[]',
    remaining_risks_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_report_basis_freeze_records_round
ON report_basis_freeze_records(run_id, round_id, generated_at_utc, basis_id);

CREATE TABLE IF NOT EXISTS report_basis_freeze_items (
    item_row_id TEXT PRIMARY KEY,
    basis_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    item_group TEXT NOT NULL DEFAULT '',
    item_index INTEGER NOT NULL DEFAULT 0,
    object_type TEXT NOT NULL DEFAULT '',
    object_id TEXT NOT NULL DEFAULT '',
    issue_label TEXT NOT NULL DEFAULT '',
    claim_id TEXT NOT NULL DEFAULT '',
    recommended_lane TEXT NOT NULL DEFAULT '',
    route_status TEXT NOT NULL DEFAULT '',
    readiness TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_report_basis_freeze_items_round
ON report_basis_freeze_items(run_id, round_id, item_group, object_type, object_id, item_row_id);

CREATE TABLE IF NOT EXISTS reporting_handoffs (
    handoff_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    handoff_status TEXT NOT NULL DEFAULT '',
    reporting_ready INTEGER NOT NULL DEFAULT 0,
    reporting_blockers_json TEXT NOT NULL DEFAULT '[]',
    report_basis_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    supervisor_status TEXT NOT NULL DEFAULT '',
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    report_basis_source TEXT NOT NULL DEFAULT '',
    readiness_source TEXT NOT NULL DEFAULT '',
    board_brief_source TEXT NOT NULL DEFAULT '',
    supervisor_state_source TEXT NOT NULL DEFAULT '',
    selected_evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_reporting_handoffs_round
ON reporting_handoffs(run_id, round_id, generated_at_utc, handoff_id);

CREATE TABLE IF NOT EXISTS council_decision_records (
    record_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    decision_stage TEXT NOT NULL DEFAULT '',
    moderator_status TEXT NOT NULL DEFAULT '',
    reporting_ready INTEGER NOT NULL DEFAULT 0,
    publication_readiness TEXT NOT NULL DEFAULT '',
    decision_gating_json TEXT NOT NULL DEFAULT '{}',
    next_round_required INTEGER NOT NULL DEFAULT 0,
    canonical_artifact TEXT NOT NULL DEFAULT '',
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    reporting_handoff_source TEXT NOT NULL DEFAULT '',
    report_basis_source TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    sociologist_report_source TEXT NOT NULL DEFAULT '',
    environmentalist_report_source TEXT NOT NULL DEFAULT '',
    selected_evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    published_report_refs_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_council_decision_records_round
ON council_decision_records(run_id, round_id, decision_stage, generated_at_utc, record_id);

CREATE TABLE IF NOT EXISTS expert_report_records (
    record_id TEXT PRIMARY KEY,
    report_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    report_stage TEXT NOT NULL DEFAULT '',
    agent_role TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    handoff_status TEXT NOT NULL DEFAULT '',
    reporting_ready INTEGER NOT NULL DEFAULT 0,
    publication_readiness TEXT NOT NULL DEFAULT '',
    canonical_artifact TEXT NOT NULL DEFAULT '',
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    reporting_handoff_source TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    expert_report_draft_source TEXT NOT NULL DEFAULT '',
    board_brief_source TEXT NOT NULL DEFAULT '',
    selected_evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_expert_report_records_round
ON expert_report_records(run_id, round_id, report_stage, agent_role, generated_at_utc, record_id);
CREATE TABLE IF NOT EXISTS report_section_drafts (
    section_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    report_id TEXT NOT NULL DEFAULT '',
    agent_role TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    section_key TEXT NOT NULL DEFAULT '',
    section_title TEXT NOT NULL DEFAULT '',
    section_text TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    basis_object_ids_json TEXT NOT NULL DEFAULT '[]',
    bundle_ids_json TEXT NOT NULL DEFAULT '[]',
    finding_ids_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_report_section_drafts_round
ON report_section_drafts(run_id, round_id, generated_at_utc, section_id);
CREATE INDEX IF NOT EXISTS idx_report_section_drafts_round_role
ON report_section_drafts(run_id, round_id, agent_role, status, section_id);
CREATE INDEX IF NOT EXISTS idx_report_section_drafts_report
ON report_section_drafts(run_id, round_id, report_id, section_key, section_id);

CREATE TABLE IF NOT EXISTS final_publications (
    publication_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    publication_status TEXT NOT NULL DEFAULT '',
    publication_posture TEXT NOT NULL DEFAULT '',
    board_state_source TEXT NOT NULL DEFAULT '',
    coverage_source TEXT NOT NULL DEFAULT '',
    reporting_handoff_source TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    report_basis_source TEXT NOT NULL DEFAULT '',
    supervisor_state_source TEXT NOT NULL DEFAULT '',
    sociologist_report_source TEXT NOT NULL DEFAULT '',
    environmentalist_report_source TEXT NOT NULL DEFAULT '',
    selected_evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    published_report_refs_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_final_publications_round
ON final_publications(run_id, round_id, generated_at_utc, publication_id);

CREATE TABLE IF NOT EXISTS round_task_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL DEFAULT '',
    task_source TEXT NOT NULL DEFAULT '',
    task_count INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_round_task_snapshots_round
ON round_task_snapshots(run_id, round_id, generated_at_utc, snapshot_id);
"""


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


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_db_path(run_dir: Path) -> Path:
    # Transitional bootstrap: reuse the existing run-local SQLite surface.
    return run_dir / "analytics" / "signal_plane.sqlite"


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return default_db_path(run_dir)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    ensure_schema_migrations(connection)
    return connection, file_path


def ensure_schema_migrations(connection: sqlite3.Connection) -> None:
    ensure_column(
        connection,
        "board_events",
        "event_index",
        "INTEGER NOT NULL DEFAULT 0",
    )
    for table_name in ("hypothesis_cards", "challenge_tickets", "board_tasks"):
        ensure_column(
            connection,
            table_name,
            "decision_source",
            "TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            connection,
            table_name,
            "evidence_refs_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        ensure_column(
            connection,
            table_name,
            "source_ids_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        ensure_column(
            connection,
            table_name,
            "provenance_json",
            "TEXT NOT NULL DEFAULT '{}'",
        )
        ensure_column(
            connection,
            table_name,
            "lineage_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )
    ensure_column(
        connection,
        "moderator_actions",
        "readiness_blocker",
        "INTEGER NOT NULL DEFAULT 1",
    )
    for table_name in ("moderator_actions", "falsification_probes"):
        for column_name in (
            "target_object_kind",
            "target_object_id",
            "issue_label",
            "target_route_id",
            "target_actor_id",
            "target_assessment_id",
            "target_linkage_id",
            "target_gap_id",
            "target_proposal_id",
            "source_proposal_id",
        ):
            ensure_column(
                connection,
                table_name,
                column_name,
                "TEXT NOT NULL DEFAULT ''",
            )
    ensure_column(
        connection,
        "reporting_handoffs",
        "reporting_ready",
        "INTEGER NOT NULL DEFAULT 0",
    )
    ensure_column(
        connection,
        "reporting_handoffs",
        "reporting_blockers_json",
        "TEXT NOT NULL DEFAULT '[]'",
    )
    ensure_column(
        connection,
        "council_decision_records",
        "reporting_ready",
        "INTEGER NOT NULL DEFAULT 0",
    )
    ensure_column(
        connection,
        "council_decision_records",
        "decision_gating_json",
        "TEXT NOT NULL DEFAULT '{}'",
    )
    ensure_column(
        connection,
        "expert_report_records",
        "reporting_ready",
        "INTEGER NOT NULL DEFAULT 0",
    )
    for column_name, column_sql in (
        ("reporting_ready", "INTEGER NOT NULL DEFAULT 0"),
        ("reporting_handoff_status", "TEXT NOT NULL DEFAULT ''"),
        ("reporting_blockers_json", "TEXT NOT NULL DEFAULT '[]'"),
    ):
        ensure_column(connection, "report_basis_freezes", column_name, column_sql)
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_board_events_round_sequence
        ON board_events(run_id, round_id, event_index, event_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_target
        ON moderator_actions(run_id, round_id, target_object_kind, target_object_id, action_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_actor
        ON moderator_actions(run_id, round_id, target_actor_id, action_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_proposal_target
        ON moderator_actions(run_id, round_id, target_proposal_id, action_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_moderator_actions_round_issue
        ON moderator_actions(run_id, round_id, issue_label, source_proposal_id, action_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_target
        ON falsification_probes(run_id, round_id, target_object_kind, target_object_id, probe_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_actor
        ON falsification_probes(run_id, round_id, target_actor_id, probe_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_proposal_target
        ON falsification_probes(run_id, round_id, target_proposal_id, probe_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_falsification_probes_round_issue
        ON falsification_probes(run_id, round_id, issue_label, source_proposal_id, probe_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_report_basis_freezes_round_statuses
        ON report_basis_freezes(
            run_id,
            round_id,
            report_basis_status,
            gate_status,
            supervisor_status,
            freeze_id
        )
        """
    )


def ensure_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    if any(maybe_text(row["name"]) == column_name for row in rows):
        return
    connection.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"
    )


def resolve_board_path(run_dir: Path, board_path: str | Path = "") -> Path:
    if isinstance(board_path, Path):
        return board_path.expanduser().resolve()
    text = maybe_text(board_path)
    if not text:
        return (run_dir / "board" / "investigation_board.json").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temp_path, path)


def empty_round_state() -> dict[str, list[dict[str, Any]]]:
    return {
        "notes": [],
        "challenge_tickets": [],
        "hypotheses": [],
        "tasks": [],
    }


def ensure_round_state(rounds: dict[str, dict[str, list[dict[str, Any]]]], round_id: str) -> dict[str, list[dict[str, Any]]]:
    round_key = maybe_text(round_id)
    state = rounds.get(round_key)
    if not isinstance(state, dict):
        state = empty_round_state()
        rounds[round_key] = state
    state.setdefault("notes", [])
    state.setdefault("challenge_tickets", [])
    state.setdefault("hypotheses", [])
    state.setdefault("tasks", [])
    return state


def board_has_state(connection: sqlite3.Connection, *, run_id: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM (
            SELECT run_id FROM board_events WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM board_notes WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM hypothesis_cards WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM challenge_tickets WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM board_tasks WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM round_transitions WHERE run_id = ?
        )
        LIMIT 1
        """,
        (run_id, run_id, run_id, run_id, run_id, run_id),
    ).fetchone()
    return row is not None


def infer_board_revision(connection: sqlite3.Connection, *, run_id: str) -> int:
    revisions = [
        coerce_int(
            connection.execute(
                f"SELECT COALESCE(MAX(board_revision), 0) AS value FROM {table_name} WHERE run_id = ?",
                (run_id,),
            ).fetchone()["value"]
        )
        for table_name in (
            "board_events",
            "board_notes",
            "hypothesis_cards",
            "challenge_tickets",
            "board_tasks",
            "round_transitions",
        )
    ]
    return max(revisions) if revisions else 0


def infer_board_path(connection: sqlite3.Connection, *, run_id: str) -> str:
    for table_name in (
        "board_events",
        "board_notes",
        "hypothesis_cards",
        "challenge_tickets",
        "board_tasks",
    ):
        row = connection.execute(
            f"""
            SELECT artifact_path
            FROM {table_name}
            WHERE run_id = ? AND artifact_path != ''
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        if row is not None and maybe_text(row["artifact_path"]):
            return maybe_text(row["artifact_path"])
    return ""


def fetch_board_run(connection: sqlite3.Connection, *, run_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT run_id, board_revision, updated_at_utc, board_path
        FROM board_runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is not None:
        return {
            "run_id": maybe_text(row["run_id"]),
            "board_revision": coerce_int(row["board_revision"]),
            "updated_at_utc": maybe_text(row["updated_at_utc"]),
            "board_path": maybe_text(row["board_path"]),
        }
    if not board_has_state(connection, run_id=run_id):
        return None
    return {
        "run_id": run_id,
        "board_revision": infer_board_revision(connection, run_id=run_id),
        "updated_at_utc": "",
        "board_path": infer_board_path(connection, run_id=run_id),
    }


def upsert_board_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    board_revision: int,
    updated_at_utc: str,
    board_path: str,
) -> None:
    connection.execute(
        """
        INSERT INTO board_runs (run_id, board_revision, updated_at_utc, board_path)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            board_revision = excluded.board_revision,
            updated_at_utc = excluded.updated_at_utc,
            board_path = excluded.board_path
        """,
        (run_id, coerce_int(board_revision), maybe_text(updated_at_utc), maybe_text(board_path)),
    )


def next_event_index(connection: sqlite3.Connection, *, run_id: str) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(event_index), -1) AS max_event_index
        FROM board_events
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    return coerce_int(row["max_event_index"]) + 1 if row is not None else 0


def load_raw_board_record(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    record_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        f"SELECT raw_json FROM {table_name} WHERE {id_column} = ?",
        (maybe_text(record_id),),
    ).fetchone()
    if row is None:
        return None
    payload = decode_json(maybe_text(row["raw_json"]), {})
    return payload if isinstance(payload, dict) else None


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
            supervisor_status, supervisor_substatus, phase2_posture, terminal_state,
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
            :supervisor_status, :supervisor_substatus, :phase2_posture, :terminal_state,
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


def runtime_control_freeze_id(run_id: str, round_id: str) -> str:
    return "control-freeze-" + stable_hash("runtime-control-freeze", run_id, round_id)[:12]


def controller_snapshot_object_id(run_id: str, round_id: str) -> str:
    return "controller-" + stable_hash("controller-state", run_id, round_id)[:12]


def gate_snapshot_object_id(
    run_id: str,
    round_id: str,
    stage_name: str,
    gate_handler: str,
) -> str:
    return "gate-" + stable_hash(
        "gate-state",
        run_id,
        round_id,
        stage_name,
        gate_handler,
    )[:12]


def supervisor_snapshot_object_id(run_id: str, round_id: str) -> str:
    return "supervisor-" + stable_hash("supervisor-state", run_id, round_id)[:12]


def orchestration_plan_object_id(
    run_id: str,
    round_id: str,
    plan_source: str,
    artifact_path: str,
) -> str:
    return "orchestration-plan-" + stable_hash(
        "orchestration-plan",
        run_id,
        round_id,
        plan_source,
        artifact_path,
    )[:12]


def orchestration_plan_step_object_id(
    plan_id: str,
    plan_step_group: str,
    step_index: int,
    stage_name: str,
    skill_name: str,
) -> str:
    return "orchestration-step-" + stable_hash(
        "orchestration-plan-step",
        plan_id,
        plan_step_group,
        step_index,
        stage_name,
        skill_name,
    )[:12]


def moderator_action_snapshot_id(run_id: str, round_id: str) -> str:
    return "actions-" + stable_hash("moderator-actions", run_id, round_id)[:12]


def falsification_probe_snapshot_id(run_id: str, round_id: str) -> str:
    return "probes-" + stable_hash("falsification-probes", run_id, round_id)[:12]


def round_task_snapshot_id(run_id: str, round_id: str) -> str:
    return "round-tasks-" + stable_hash("round-task-snapshot", run_id, round_id)[:12]


def readiness_assessment_id(run_id: str, round_id: str, readiness_status: str) -> str:
    return "round-readiness-" + stable_hash(
        "round-readiness",
        run_id,
        round_id,
        readiness_status,
    )[:12]


def reporting_handoff_id(
    run_id: str,
    round_id: str,
    handoff_status: str,
    report_basis_status: str,
) -> str:
    return "reporting-handoff-" + stable_hash(
        "reporting-handoff",
        run_id,
        round_id,
        handoff_status,
        report_basis_status,
    )[:12]


def council_decision_record_id(
    run_id: str,
    round_id: str,
    decision_stage: str,
    decision_id: str,
) -> str:
    return "decision-record-" + stable_hash(
        "council-decision-record",
        run_id,
        round_id,
        decision_stage,
        decision_id,
    )[:12]


def expert_report_record_id(
    run_id: str,
    round_id: str,
    report_stage: str,
    agent_role: str,
    report_id: str,
) -> str:
    return "expert-report-record-" + stable_hash(
        "expert-report-record",
        run_id,
        round_id,
        report_stage,
        agent_role,
        report_id,
    )[:12]


def decision_stage_from_payload(payload: dict[str, Any]) -> str:
    explicit_stage = maybe_text(payload.get("decision_stage"))
    if explicit_stage in {"draft", "canonical"}:
        return explicit_stage
    if maybe_text(payload.get("canonical_artifact")) == "council-decision":
        return "canonical"
    return "draft"


def expert_report_stage_from_payload(payload: dict[str, Any]) -> str:
    explicit_stage = maybe_text(payload.get("report_stage"))
    if explicit_stage in {"draft", "canonical"}:
        return explicit_stage
    if maybe_text(payload.get("canonical_artifact")) == "expert-report":
        return "canonical"
    return "draft"


def action_target_id(action: dict[str, Any], field_name: str) -> str:
    target = action.get("target", {}) if isinstance(action.get("target"), dict) else {}
    direct_field_name = f"target_{field_name}"
    return (
        maybe_text(action.get(direct_field_name))
        or maybe_text(action.get(field_name))
        or maybe_text(target.get(direct_field_name))
        or maybe_text(target.get(field_name))
    )


def normalized_action_payload(
    action: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    action_rank: int,
    generated_at_utc: str = "",
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(action)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc"))
        or maybe_text(generated_at_utc)
        or utc_now_iso()
    )
    normalized["action_id"] = (
        maybe_text(normalized.get("action_id"))
        or "action-"
        + stable_hash(
            "moderator-action",
            run_id,
            round_id,
            action_rank,
            maybe_text(normalized.get("action_kind")),
            maybe_text(normalized.get("objective")),
            maybe_text(normalized.get("reason")),
            action_target_id(normalized, "hypothesis_id"),
            action_target_id(normalized, "claim_id"),
            action_target_id(normalized, "ticket_id"),
        )[:12]
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
    )
    normalized["action_kind"] = maybe_text(normalized.get("action_kind")) or "follow-up"
    normalized["assigned_role"] = (
        maybe_text(normalized.get("assigned_role")) or "moderator"
    )
    normalized["objective"] = (
        maybe_text(normalized.get("objective"))
        or maybe_text(normalized.get("reason"))
        or "Advance the current round posture."
    )
    normalized["reason"] = (
        maybe_text(normalized.get("reason"))
        or maybe_text(normalized.get("objective"))
        or "Advance the current round posture."
    )
    normalized["decision_source"] = decision_source
    normalized["source_ids"] = unique_texts(list_items(normalized.get("source_ids")))
    normalized["evidence_refs"] = list_items(normalized.get("evidence_refs"))
    normalized["probe_candidate"] = bool(normalized.get("probe_candidate"))
    normalized["readiness_blocker"] = action_is_readiness_blocker(normalized)
    source_proposal_id = source_proposal_id_from_payload(normalized)
    target = normalized_deliberation_target(
        normalized.get("target"),
        object_kind=maybe_text(normalized.get("target_object_kind")),
        object_id=maybe_text(normalized.get("target_object_id")),
        issue_label=maybe_text(normalized.get("issue_label")),
        claim_id=action_target_id(normalized, "claim_id"),
        hypothesis_id=action_target_id(normalized, "hypothesis_id"),
        ticket_id=action_target_id(normalized, "ticket_id"),
        route_id=maybe_text(normalized.get("target_route_id"))
        or maybe_text(normalized.get("route_id")),
        actor_id=action_target_id(normalized, "actor_id"),
        assessment_id=maybe_text(normalized.get("target_assessment_id"))
        or maybe_text(normalized.get("assessment_id")),
        linkage_id=maybe_text(normalized.get("target_linkage_id"))
        or maybe_text(normalized.get("linkage_id")),
        gap_id=maybe_text(normalized.get("target_gap_id"))
        or maybe_text(normalized.get("gap_id")),
        map_issue_id=maybe_text(normalized.get("target_map_issue_id"))
        or maybe_text(normalized.get("map_issue_id")),
        proposal_id=action_target_id(normalized, "proposal_id"),
        round_id=normalized["round_id"],
    )
    anchor_fields = deliberation_anchor_fields(
        target,
        source_proposal_id=source_proposal_id,
    )
    normalized["target"] = target
    normalized["target_hypothesis_id"] = maybe_text(target.get("hypothesis_id"))
    normalized["target_claim_id"] = maybe_text(target.get("claim_id"))
    normalized["target_ticket_id"] = maybe_text(target.get("ticket_id"))
    normalized["target_actor_id"] = maybe_text(anchor_fields.get("target_actor_id"))
    normalized["target_proposal_id"] = maybe_text(
        anchor_fields.get("target_proposal_id")
    )
    normalized["target_object_kind"] = maybe_text(anchor_fields.get("target_object_kind"))
    normalized["target_object_id"] = maybe_text(anchor_fields.get("target_object_id"))
    normalized["issue_label"] = (
        maybe_text(anchor_fields.get("issue_label"))
        or maybe_text(normalized.get("issue_label"))
    )
    normalized["target_route_id"] = maybe_text(anchor_fields.get("target_route_id"))
    normalized["target_assessment_id"] = maybe_text(
        anchor_fields.get("target_assessment_id")
    )
    normalized["target_linkage_id"] = maybe_text(
        anchor_fields.get("target_linkage_id")
    )
    normalized["target_gap_id"] = maybe_text(anchor_fields.get("target_gap_id"))
    normalized["source_proposal_id"] = maybe_text(
        anchor_fields.get("source_proposal_id")
    )
    if normalized["source_proposal_id"]:
        normalized["source_ids"] = unique_texts(
            [*normalized["source_ids"], normalized["source_proposal_id"]]
        )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("source_ids"),
        normalized.get("source_proposal_id"),
        normalized.get("target_hypothesis_id"),
        normalized.get("target_claim_id"),
        normalized.get("target_ticket_id"),
        normalized.get("target_actor_id"),
        normalized.get("target_proposal_id"),
        normalized.get("target_object_id"),
        normalized.get("target_route_id"),
        normalized.get("target_assessment_id"),
        normalized.get("target_linkage_id"),
        normalized.get("target_gap_id"),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "agenda_source": maybe_text(normalized.get("agenda_source")),
            "assigned_role": maybe_text(normalized.get("assigned_role")),
            "action_kind": maybe_text(normalized.get("action_kind")),
            "target_actor_id": normalized.get("target_actor_id"),
            "target_proposal_id": normalized.get("target_proposal_id"),
            "source_proposal_id": normalized.get("source_proposal_id"),
        },
    )
    return validate_canonical_payload("next-action", normalized)


def normalized_probe_payload(
    probe: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    probe_index: int,
    generated_at_utc: str = "",
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(probe)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["opened_at_utc"] = (
        maybe_text(normalized.get("opened_at_utc"))
        or maybe_text(generated_at_utc)
        or utc_now_iso()
    )
    normalized["probe_id"] = (
        maybe_text(normalized.get("probe_id"))
        or "probe-"
        + stable_hash(
            "falsification-probe",
            run_id,
            round_id,
            probe_index,
            maybe_text(normalized.get("action_id")),
            maybe_text(normalized.get("probe_type")),
            maybe_text(normalized.get("probe_goal")),
            maybe_text(normalized.get("target_hypothesis_id")),
            maybe_text(normalized.get("target_claim_id")),
            maybe_text(normalized.get("target_ticket_id")),
        )[:12]
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
    )
    normalized["probe_status"] = maybe_text(normalized.get("probe_status")) or "open"
    normalized["owner_role"] = maybe_text(normalized.get("owner_role")) or "challenger"
    normalized["probe_type"] = (
        maybe_text(normalized.get("probe_type")) or "uncertainty-probe"
    )
    normalized["probe_goal"] = (
        maybe_text(normalized.get("probe_goal"))
        or maybe_text(normalized.get("falsification_question"))
        or "Probe the current target."
    )
    normalized["falsification_question"] = (
        maybe_text(normalized.get("falsification_question"))
        or f"What evidence would materially weaken: {normalized['probe_goal']}"
    )
    normalized["decision_source"] = decision_source
    normalized["source_ids"] = unique_texts(list_items(normalized.get("source_ids")))
    normalized["requested_skills"] = unique_texts(
        list_items(normalized.get("requested_skills"))
    )
    normalized["success_criteria"] = unique_texts(
        list_items(normalized.get("success_criteria"))
    )
    normalized["disconfirm_signals"] = unique_texts(
        list_items(normalized.get("disconfirm_signals"))
    )
    normalized["evidence_refs"] = list_items(normalized.get("evidence_refs"))
    source_proposal_id = source_proposal_id_from_payload(normalized)
    target = normalized_deliberation_target(
        normalized.get("target"),
        object_kind=maybe_text(normalized.get("target_object_kind")),
        object_id=maybe_text(normalized.get("target_object_id")),
        issue_label=maybe_text(normalized.get("issue_label")),
        claim_id=maybe_text(normalized.get("target_claim_id")),
        hypothesis_id=maybe_text(normalized.get("target_hypothesis_id")),
        ticket_id=maybe_text(normalized.get("target_ticket_id")),
        route_id=maybe_text(normalized.get("target_route_id"))
        or maybe_text(normalized.get("route_id")),
        actor_id=action_target_id(normalized, "actor_id"),
        assessment_id=maybe_text(normalized.get("target_assessment_id"))
        or maybe_text(normalized.get("assessment_id")),
        linkage_id=maybe_text(normalized.get("target_linkage_id"))
        or maybe_text(normalized.get("linkage_id")),
        gap_id=maybe_text(normalized.get("target_gap_id"))
        or maybe_text(normalized.get("gap_id")),
        map_issue_id=maybe_text(normalized.get("target_map_issue_id"))
        or maybe_text(normalized.get("map_issue_id")),
        proposal_id=action_target_id(normalized, "proposal_id"),
        round_id=normalized["round_id"],
        action_id=maybe_text(normalized.get("action_id")),
    )
    anchor_fields = deliberation_anchor_fields(
        target,
        source_proposal_id=source_proposal_id,
    )
    normalized["target"] = target
    normalized["target_hypothesis_id"] = maybe_text(target.get("hypothesis_id"))
    normalized["target_claim_id"] = maybe_text(target.get("claim_id"))
    normalized["target_ticket_id"] = maybe_text(target.get("ticket_id"))
    normalized["target_actor_id"] = maybe_text(anchor_fields.get("target_actor_id"))
    normalized["target_proposal_id"] = maybe_text(
        anchor_fields.get("target_proposal_id")
    )
    normalized["target_object_kind"] = maybe_text(anchor_fields.get("target_object_kind"))
    normalized["target_object_id"] = maybe_text(anchor_fields.get("target_object_id"))
    normalized["issue_label"] = (
        maybe_text(anchor_fields.get("issue_label"))
        or maybe_text(normalized.get("issue_label"))
    )
    normalized["target_route_id"] = maybe_text(anchor_fields.get("target_route_id"))
    normalized["target_assessment_id"] = maybe_text(
        anchor_fields.get("target_assessment_id")
    )
    normalized["target_linkage_id"] = maybe_text(
        anchor_fields.get("target_linkage_id")
    )
    normalized["target_gap_id"] = maybe_text(anchor_fields.get("target_gap_id"))
    normalized["source_proposal_id"] = maybe_text(
        anchor_fields.get("source_proposal_id")
    )
    if normalized["source_proposal_id"]:
        normalized["source_ids"] = unique_texts(
            [*normalized["source_ids"], normalized["source_proposal_id"]]
        )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("source_ids"),
        maybe_text(normalized.get("action_id")),
        normalized.get("source_proposal_id"),
        normalized.get("target_hypothesis_id"),
        normalized.get("target_claim_id"),
        normalized.get("target_ticket_id"),
        normalized.get("target_actor_id"),
        normalized.get("target_proposal_id"),
        normalized.get("target_object_id"),
        normalized.get("target_route_id"),
        normalized.get("target_assessment_id"),
        normalized.get("target_linkage_id"),
        normalized.get("target_gap_id"),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "owner_role": maybe_text(normalized.get("owner_role")),
            "probe_status": maybe_text(normalized.get("probe_status")),
            "probe_type": maybe_text(normalized.get("probe_type")),
            "target_actor_id": normalized.get("target_actor_id"),
            "target_proposal_id": normalized.get("target_proposal_id"),
            "source_proposal_id": normalized.get("source_proposal_id"),
        },
    )
    return validate_canonical_payload("probe", normalized)


def normalized_readiness_payload(
    readiness_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(readiness_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "blocked"
    )
    normalized["readiness_id"] = (
        maybe_text(normalized.get("readiness_id"))
        or readiness_assessment_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["readiness_status"],
        )
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "policy-fallback"
    )
    normalized["decision_source"] = decision_source
    normalized["selected_basis_object_ids"] = list_items(
        normalized.get("selected_basis_object_ids")
    )
    normalized["basis_object_ids"] = list_items(normalized.get("basis_object_ids"))
    normalized["opinion_ids"] = list_items(normalized.get("opinion_ids"))
    normalized["evidence_refs"] = list_items(
        normalized.get("evidence_refs")
    ) or list_items(normalized.get("selected_evidence_refs"))
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("selected_basis_object_ids"),
        normalized.get("basis_object_ids"),
        normalized.get("opinion_ids"),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "board_state_source": maybe_text(normalized.get("board_state_source")),
            "coverage_source": maybe_text(normalized.get("coverage_source")),
            "next_actions_source": maybe_text(normalized.get("next_actions_source")),
            "probes_source": maybe_text(normalized.get("probes_source")),
        },
    )
    return validate_canonical_payload("readiness-assessment", normalized)


PROMOTION_BASIS_ITEM_GROUPS = (
    "issue_clusters",
    "verification_routes",
    "formal_public_links",
    "representation_gaps",
    "diffusion_edges",
    "coverages",
)


def normalized_report_basis_freeze_payload(
    report_basis_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(report_basis_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status"))
        or "withheld"
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "blocked"
    )
    normalized["selected_basis_object_ids"] = list_items(
        normalized.get("selected_basis_object_ids")
    )
    normalized["selected_evidence_refs"] = list_items(
        normalized.get("selected_evidence_refs")
    )
    normalized["basis_id"] = (
        maybe_text(normalized.get("basis_id"))
        or "evidence-basis-"
        + stable_hash(
            "report-basis-freeze",
            normalized["run_id"],
            normalized["round_id"],
            normalized["report_basis_status"],
        )[:12]
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "policy-fallback"
    )
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = list_items(
        normalized.get("evidence_refs")
    ) or list_items(normalized.get("selected_evidence_refs"))
    item_object_ids = [
        report_basis_freeze_item_object_id(item_group, item)
        for item_group, _item_index, item in iter_report_basis_freeze_items(normalized)
    ]
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("selected_basis_object_ids"),
        item_object_ids,
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "basis_selection_mode": maybe_text(
                normalized.get("basis_selection_mode")
            ),
            "board_state_source": maybe_text(normalized.get("board_state_source")),
            "coverage_source": maybe_text(normalized.get("coverage_source")),
            "readiness_source": maybe_text(normalized.get("readiness_source")),
            "next_actions_source": maybe_text(normalized.get("next_actions_source")),
            "board_brief_source": maybe_text(normalized.get("board_brief_source")),
        },
    )
    return validate_canonical_payload("report-basis-freeze", normalized)


REPORT_AGENT_ROLES = ("sociologist", "environmentalist")


def nested_evidence_refs(items: Any) -> list[str]:
    values: list[Any] = []
    for item in list_items(items):
        if isinstance(item, dict):
            values.extend(list_items(item.get("evidence_refs")))
    return unique_texts(values)


def nested_text_ids(items: Any, *field_names: str) -> list[str]:
    values: list[str] = []
    for item in list_items(items):
        if not isinstance(item, dict):
            continue
        for field_name in field_names:
            text = maybe_text(item.get(field_name))
            if text:
                values.append(text)
    return unique_texts(values)


def ensure_list_fields(normalized: dict[str, Any], *field_names: str) -> None:
    for field_name in field_names:
        normalized[field_name] = list_items(normalized.get(field_name))


def ensure_dict_fields(normalized: dict[str, Any], *field_names: str) -> None:
    for field_name in field_names:
        normalized[field_name] = dict_items(normalized.get(field_name))


def apply_reporting_contract_defaults(
    normalized: dict[str, Any],
    *,
    object_kind: str,
    decision_source: str,
    lineage_sources: tuple[Any, ...] = (),
    nested_evidence_sources: tuple[Any, ...] = (),
    provenance_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized["schema_version"] = canonical_contract(object_kind).schema_version
    normalized["decision_source"] = decision_source
    evidence_refs = list_items(normalized.get("evidence_refs"))
    evidence_refs.extend(list_items(normalized.get("selected_evidence_refs")))
    for source in nested_evidence_sources:
        evidence_refs.extend(nested_evidence_refs(source))
    normalized["evidence_refs"] = unique_texts(evidence_refs)
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        *lineage_sources,
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=maybe_text(normalized.get("skill")),
        decision_source=decision_source,
        extra=provenance_extra,
    )
    return normalized


def normalized_reporting_handoff_payload(
    handoff_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(handoff_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    gate_state = reporting_gate_state(
        report_basis_status=maybe_text(normalized.get("report_basis_status"))
        or "withheld",
        readiness_status=maybe_text(normalized.get("readiness_status")) or "blocked",
        supervisor_status=maybe_text(normalized.get("supervisor_status")),
        require_supervisor=True,
        reporting_ready=normalized.get("reporting_ready"),
        reporting_blockers_value=normalized.get("reporting_blockers"),
        handoff_status=maybe_text(normalized.get("handoff_status")),
    )
    normalized["handoff_status"] = (
        maybe_text(gate_state.get("handoff_status")) or "investigation-open"
    )
    normalized["report_basis_status"] = maybe_text(gate_state.get("report_basis_status")) or "withheld"
    normalized["readiness_status"] = maybe_text(gate_state.get("readiness_status")) or "blocked"
    normalized["supervisor_status"] = maybe_text(gate_state.get("supervisor_status"))
    normalized["reporting_ready"] = bool(gate_state.get("reporting_ready"))
    normalized["reporting_blockers"] = list_items(
        gate_state.get("reporting_blockers")
    )
    normalized["handoff_id"] = (
        maybe_text(normalized.get("handoff_id"))
        or reporting_handoff_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["handoff_status"],
            normalized["report_basis_status"],
        )
    )
    ensure_list_fields(
        normalized,
        "reporting_blockers",
        "selected_basis_object_ids",
        "selected_evidence_refs",
        "supporting_proposal_ids",
        "rejected_proposal_ids",
        "supporting_opinion_ids",
        "rejected_opinion_ids",
        "recommended_next_actions",
        "key_findings",
        "open_risks",
        "evidence_index",
        "uncertainty_register",
        "residual_disputes",
        "policy_recommendations",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "council_input_counts",
        "evidence_packet",
        "decision_packet",
        "report_packet",
    )
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("readiness_source"))
        or maybe_text(normalized.get("supervisor_state_source"))
        or maybe_text(normalized.get("skill"))
        or "reporting-handoff-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="reporting-handoff",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(normalized.get("report_basis_id")),
            normalized.get("selected_basis_object_ids"),
            normalized.get("supporting_proposal_ids"),
            normalized.get("rejected_proposal_ids"),
            normalized.get("supporting_opinion_ids"),
            normalized.get("rejected_opinion_ids"),
            nested_text_ids(normalized.get("key_findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("evidence_index"), "evidence_id", "object_id"),
        ),
        nested_evidence_sources=(
            normalized.get("key_findings"),
            normalized.get("evidence_index"),
        ),
        provenance_extra={
            "handoff_status": normalized["handoff_status"],
            "report_basis_status": normalized["report_basis_status"],
            "readiness_status": normalized["readiness_status"],
            "supervisor_status": normalized["supervisor_status"],
            "board_state_source": maybe_text(normalized.get("board_state_source")),
            "coverage_source": maybe_text(normalized.get("coverage_source")),
            "report_basis_source": maybe_text(normalized.get("report_basis_source")),
            "readiness_source": maybe_text(normalized.get("readiness_source")),
            "supervisor_state_source": maybe_text(
                normalized.get("supervisor_state_source")
            ),
        },
    )
    return validate_canonical_payload("reporting-handoff", normalized)


def normalized_council_decision_payload(
    decision_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(decision_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["decision_stage"] = decision_stage_from_payload(normalized)
    normalized["decision_id"] = (
        maybe_text(normalized.get("decision_id"))
        or "council-decision-"
        + stable_hash(
            "council-decision",
            normalized["run_id"],
            normalized["round_id"],
            normalized["decision_stage"],
            maybe_text(normalized.get("moderator_status")),
            maybe_text(normalized.get("publication_readiness")),
        )[:12]
    )
    normalized["record_id"] = (
        maybe_text(normalized.get("record_id"))
        or council_decision_record_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["decision_stage"],
            normalized["decision_id"],
        )
    )
    normalized["handoff_status"] = normalize_reporting_handoff_status(
        normalized.get("handoff_status")
    )
    normalized["reporting_ready"] = bool(maybe_bool(normalized.get("reporting_ready")))
    normalized["reporting_blockers"] = list_items(normalized.get("reporting_blockers"))
    normalized["publication_readiness"] = (
        maybe_text(normalized.get("publication_readiness"))
        or ("ready" if normalized["reporting_ready"] else "hold")
    )
    normalized["moderator_status"] = (
        maybe_text(normalized.get("moderator_status"))
        or (
            "finalize"
            if normalized["publication_readiness"] == "ready"
            else "continue"
        )
    )
    normalized["decision_summary"] = (
        maybe_text(normalized.get("decision_summary"))
        or (
            "Round is ready for final reporting."
            if normalized["publication_readiness"] == "ready"
            else "Another round is required before final reporting."
        )
    )
    ensure_list_fields(
        normalized,
        "reporting_blockers",
        "selected_basis_object_ids",
        "selected_evidence_refs",
        "supporting_proposal_ids",
        "rejected_proposal_ids",
        "supporting_opinion_ids",
        "rejected_opinion_ids",
        "decision_trace_ids",
        "published_report_refs",
        "recommended_next_actions",
        "key_findings",
        "open_risks",
        "accepted_object_ids",
        "rejected_object_ids",
        "report_basis_resolution_reasons",
        "memo_sections",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "decision_gating",
        "council_input_counts",
        "audit_refs",
        "decision_packet",
    )
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("reporting_handoff_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("skill"))
        or "council-decision-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="council-decision",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(normalized.get("report_basis_id")),
            normalized.get("selected_basis_object_ids"),
            normalized.get("supporting_proposal_ids"),
            normalized.get("rejected_proposal_ids"),
            normalized.get("supporting_opinion_ids"),
            normalized.get("rejected_opinion_ids"),
            normalized.get("decision_trace_ids"),
            nested_text_ids(normalized.get("key_findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("memo_sections"), "section_id"),
        ),
        nested_evidence_sources=(normalized.get("key_findings"),),
        provenance_extra={
            "decision_stage": normalized["decision_stage"],
            "moderator_status": normalized["moderator_status"],
            "publication_readiness": normalized["publication_readiness"],
            "canonical_artifact": maybe_text(normalized.get("canonical_artifact")),
            "reporting_handoff_source": maybe_text(
                normalized.get("reporting_handoff_source")
            ),
            "report_basis_source": maybe_text(normalized.get("report_basis_source")),
        },
    )
    return validate_canonical_payload("council-decision", normalized)


def normalized_expert_report_payload(
    report_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(report_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["report_stage"] = expert_report_stage_from_payload(normalized)
    normalized["agent_role"] = (
        maybe_text(normalized.get("agent_role"))
        or maybe_text(normalized.get("canonical_role"))
    )
    normalized["report_id"] = (
        maybe_text(normalized.get("report_id"))
        or (
            f"expert-report-{normalized['agent_role']}-{normalized['round_id']}"
            if normalized["agent_role"]
            else "expert-report-"
            + stable_hash(
                "expert-report",
                normalized["run_id"],
                normalized["round_id"],
                normalized["report_stage"],
            )[:12]
        )
    )
    normalized["record_id"] = (
        maybe_text(normalized.get("record_id"))
        or expert_report_record_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["report_stage"],
            normalized["agent_role"],
            normalized["report_id"],
        )
    )
    normalized["handoff_status"] = normalize_reporting_handoff_status(
        normalized.get("handoff_status")
    )
    normalized["reporting_ready"] = bool(maybe_bool(normalized.get("reporting_ready")))
    normalized["reporting_blockers"] = list_items(normalized.get("reporting_blockers"))
    normalized["publication_readiness"] = (
        maybe_text(normalized.get("publication_readiness"))
        or ("ready" if normalized["reporting_ready"] else "hold")
    )
    normalized["status"] = (
        maybe_text(normalized.get("status"))
        or (
            "ready-to-publish"
            if normalized["publication_readiness"] == "ready"
            else "needs-more-evidence"
        )
    )
    normalized["summary"] = (
        maybe_text(normalized.get("summary"))
        or f"Expert report for {normalized['agent_role'] or 'unspecified-role'}."
    )
    if normalized["report_stage"] == "canonical" and not maybe_text(
        normalized.get("canonical_artifact")
    ):
        normalized["canonical_artifact"] = "expert-report"
    ensure_list_fields(
        normalized,
        "reporting_blockers",
        "selected_evidence_refs",
        "findings",
        "open_questions",
        "recommended_next_actions",
        "report_sections",
        "section_draft_refs",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "audit_refs",
        "report_packet",
    )
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("reporting_handoff_source"))
        or maybe_text(normalized.get("expert_report_draft_source"))
        or maybe_text(normalized.get("skill"))
        or "expert-report-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="expert-report",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(normalized.get("decision_id")),
            nested_text_ids(normalized.get("findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("section_draft_refs"), "section_id"),
        ),
        nested_evidence_sources=(normalized.get("findings"),),
        provenance_extra={
            "report_stage": normalized["report_stage"],
            "agent_role": normalized["agent_role"],
            "status": normalized["status"],
            "publication_readiness": normalized["publication_readiness"],
            "canonical_artifact": maybe_text(normalized.get("canonical_artifact")),
            "reporting_handoff_source": maybe_text(
                normalized.get("reporting_handoff_source")
            ),
            "expert_report_draft_source": maybe_text(
                normalized.get("expert_report_draft_source")
            ),
        },
    )
    return validate_canonical_payload("expert-report", normalized)


def normalized_final_publication_payload(
    publication_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(publication_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["publication_status"] = (
        maybe_text(normalized.get("publication_status")) or "hold-release"
    )
    normalized["publication_posture"] = (
        maybe_text(normalized.get("publication_posture"))
        or (
            "release"
            if normalized["publication_status"] == "ready-for-release"
            else "withhold"
        )
    )
    normalized["publication_id"] = (
        maybe_text(normalized.get("publication_id"))
        or "final-publication-"
        + stable_hash(
            "final-publication",
            normalized["run_id"],
            normalized["round_id"],
            normalized["publication_posture"],
            maybe_text(normalized.get("decision_id")),
        )[:12]
    )
    normalized["publication_summary"] = (
        maybe_text(normalized.get("publication_summary"))
        or (
            "Round is ready for final publication."
            if normalized["publication_posture"] == "release"
            else "Final publication is currently withheld."
        )
    )
    ensure_list_fields(
        normalized,
        "published_sections",
        "decision_trace_ids",
        "decision_traces",
        "role_reports",
        "published_report_refs",
        "key_findings",
        "open_risks",
        "recommended_next_actions",
        "selected_evidence_refs",
        "operator_review_hints",
        "evidence_index",
        "uncertainty_register",
        "residual_disputes",
        "policy_recommendations",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "decision",
        "audit_refs",
        "decision_maker_report",
    )
    decision_payload = dict_items(normalized.get("decision"))
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("reporting_handoff_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("skill"))
        or "final-publication-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="final-publication",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(decision_payload.get("decision_id")),
            normalized.get("decision_trace_ids"),
            nested_text_ids(normalized.get("role_reports"), "report_id"),
            nested_text_ids(normalized.get("key_findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("evidence_index"), "evidence_id", "object_id"),
        ),
        nested_evidence_sources=(
            normalized.get("key_findings"),
            normalized.get("decision_traces"),
            normalized.get("evidence_index"),
        ),
        provenance_extra={
            "publication_status": normalized["publication_status"],
            "publication_posture": normalized["publication_posture"],
            "reporting_handoff_source": maybe_text(
                normalized.get("reporting_handoff_source")
            ),
            "report_basis_source": maybe_text(normalized.get("report_basis_source")),
            "supervisor_state_source": maybe_text(
                normalized.get("supervisor_state_source")
            ),
        },
    )
    return validate_canonical_payload("final-publication", normalized)


def report_basis_freeze_item_object_type(item_group: str, item: dict[str, Any]) -> str:
    explicit = maybe_text(item.get("object_type"))
    if explicit:
        return explicit
    if item_group == "coverages":
        return "coverage"
    return item_group.rstrip("s").replace("_", "-")


def report_basis_freeze_item_object_id(item_group: str, item: dict[str, Any]) -> str:
    for key in (
        "object_id",
        "coverage_id",
        "claim_id",
        "route_id",
        "linkage_id",
        "gap_id",
        "edge_id",
        "cluster_id",
        "map_issue_id",
    ):
        value = maybe_text(item.get(key))
        if value:
            return value
    return (
        item_group
        + "-"
        + stable_hash(
            "report-basis-freeze-item",
            item_group,
            maybe_text(item.get("summary")),
            maybe_text(item.get("issue_label")),
            json_text(item),
        )[:12]
    )


def iter_report_basis_freeze_items(
    report_basis_payload: dict[str, Any],
) -> list[tuple[str, int, dict[str, Any]]]:
    frozen_basis = (
        report_basis_payload.get("frozen_basis", {})
        if isinstance(report_basis_payload.get("frozen_basis"), dict)
        else {}
    )
    results: list[tuple[str, int, dict[str, Any]]] = []
    for item_group in PROMOTION_BASIS_ITEM_GROUPS:
        rows = (
            frozen_basis.get(item_group, [])
            if isinstance(frozen_basis.get(item_group), list)
            else []
        )
        if item_group == "coverages" and not rows:
            rows = (
                report_basis_payload.get("selected_coverages", [])
                if isinstance(report_basis_payload.get("selected_coverages"), list)
                else []
            )
        for index, item in enumerate(rows):
            if isinstance(item, dict):
                results.append((item_group, index, item))
    return results


def report_basis_freeze_item_row_id(
    basis_id: str,
    item_group: str,
    item_index: int,
    object_id: str,
) -> str:
    return "report_basis-item-" + stable_hash(
        "report-basis-freeze-item-row",
        basis_id,
        item_group,
        item_index,
        object_id,
    )[:12]


def moderator_action_row_from_payload(
    action: dict[str, Any],
    *,
    generated_at_utc: str,
    action_rank: int,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    target = (
        action.get("target", {})
        if isinstance(action.get("target"), dict)
        else {}
    )
    return {
        "action_id": maybe_text(action.get("action_id")),
        "run_id": maybe_text(action.get("run_id")),
        "round_id": maybe_text(action.get("round_id")),
        "generated_at_utc": maybe_text(generated_at_utc),
        "action_rank": coerce_int(action_rank),
        "action_kind": maybe_text(action.get("action_kind")),
        "priority": maybe_text(action.get("priority")),
        "assigned_role": maybe_text(action.get("assigned_role")),
        "target_hypothesis_id": action_target_id(action, "hypothesis_id"),
        "target_claim_id": action_target_id(action, "claim_id"),
        "target_ticket_id": action_target_id(action, "ticket_id"),
        "target_actor_id": action_target_id(action, "actor_id"),
        "target_proposal_id": action_target_id(action, "proposal_id"),
        "target_object_kind": maybe_text(action.get("target_object_kind"))
        or maybe_text(target.get("object_kind")),
        "target_object_id": maybe_text(action.get("target_object_id"))
        or maybe_text(target.get("object_id")),
        "issue_label": maybe_text(action.get("issue_label"))
        or maybe_text(target.get("issue_label")),
        "target_route_id": maybe_text(action.get("target_route_id"))
        or maybe_text(target.get("route_id")),
        "target_assessment_id": maybe_text(action.get("target_assessment_id"))
        or maybe_text(target.get("assessment_id")),
        "target_linkage_id": maybe_text(action.get("target_linkage_id"))
        or maybe_text(target.get("linkage_id")),
        "target_gap_id": maybe_text(action.get("target_gap_id"))
        or maybe_text(target.get("gap_id")),
        "source_proposal_id": maybe_text(action.get("source_proposal_id")),
        "controversy_gap": maybe_text(action.get("controversy_gap")),
        "recommended_lane": maybe_text(action.get("recommended_lane")),
        "probe_candidate": 1 if bool(action.get("probe_candidate")) else 0,
        "readiness_blocker": 1 if action_is_readiness_blocker(action) else 0,
        "objective": maybe_text(action.get("objective")),
        "reason": maybe_text(action.get("reason")),
        "evidence_refs_json": json_text(
            action.get("evidence_refs", [])
            if isinstance(action.get("evidence_refs"), list)
            else []
        ),
        "source_ids_json": json_text(
            action.get("source_ids", [])
            if isinstance(action.get("source_ids"), list)
            else []
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(action),
    }


def falsification_probe_row_from_payload(
    probe: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    target = probe.get("target", {}) if isinstance(probe.get("target"), dict) else {}
    return {
        "probe_id": maybe_text(probe.get("probe_id")),
        "run_id": maybe_text(probe.get("run_id")),
        "round_id": maybe_text(probe.get("round_id")),
        "opened_at_utc": maybe_text(probe.get("opened_at_utc")),
        "probe_status": maybe_text(probe.get("probe_status")),
        "action_id": maybe_text(probe.get("action_id")),
        "priority": maybe_text(probe.get("priority")),
        "owner_role": maybe_text(probe.get("owner_role")),
        "target_hypothesis_id": maybe_text(probe.get("target_hypothesis_id")),
        "target_claim_id": maybe_text(probe.get("target_claim_id")),
        "target_ticket_id": maybe_text(probe.get("target_ticket_id")),
        "target_actor_id": action_target_id(probe, "actor_id"),
        "target_proposal_id": action_target_id(probe, "proposal_id"),
        "target_object_kind": maybe_text(probe.get("target_object_kind"))
        or maybe_text(target.get("object_kind")),
        "target_object_id": maybe_text(probe.get("target_object_id"))
        or maybe_text(target.get("object_id")),
        "issue_label": maybe_text(probe.get("issue_label"))
        or maybe_text(target.get("issue_label")),
        "target_route_id": maybe_text(probe.get("target_route_id"))
        or maybe_text(target.get("route_id")),
        "target_assessment_id": maybe_text(probe.get("target_assessment_id"))
        or maybe_text(target.get("assessment_id")),
        "target_linkage_id": maybe_text(probe.get("target_linkage_id"))
        or maybe_text(target.get("linkage_id")),
        "target_gap_id": maybe_text(probe.get("target_gap_id"))
        or maybe_text(target.get("gap_id")),
        "source_proposal_id": maybe_text(probe.get("source_proposal_id")),
        "probe_type": maybe_text(probe.get("probe_type")),
        "controversy_gap": maybe_text(probe.get("controversy_gap")),
        "recommended_lane": maybe_text(probe.get("recommended_lane")),
        "probe_goal": maybe_text(probe.get("probe_goal")),
        "falsification_question": maybe_text(probe.get("falsification_question")),
        "requested_skills_json": json_text(
            probe.get("requested_skills", [])
            if isinstance(probe.get("requested_skills"), list)
            else []
        ),
        "evidence_refs_json": json_text(
            probe.get("evidence_refs", [])
            if isinstance(probe.get("evidence_refs"), list)
            else []
        ),
        "source_ids_json": json_text(
            probe.get("source_ids", [])
            if isinstance(probe.get("source_ids"), list)
            else []
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(probe),
    }


def round_readiness_assessment_row_from_payload(
    readiness_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "readiness_id": maybe_text(readiness_payload.get("readiness_id")),
        "run_id": maybe_text(readiness_payload.get("run_id")),
        "round_id": maybe_text(readiness_payload.get("round_id")),
        "generated_at_utc": maybe_text(readiness_payload.get("generated_at_utc")),
        "readiness_status": maybe_text(readiness_payload.get("readiness_status")),
        "sufficient_for_report_basis": 1
        if bool(readiness_payload.get("sufficient_for_report_basis"))
        else 0,
        "board_state_source": maybe_text(readiness_payload.get("board_state_source")),
        "coverage_source": maybe_text(readiness_payload.get("coverage_source")),
        "next_actions_source": maybe_text(readiness_payload.get("next_actions_source")),
        "probes_source": maybe_text(readiness_payload.get("probes_source")),
        "agenda_counts_json": json_text(readiness_payload.get("agenda_counts", {})),
        "counts_json": json_text(readiness_payload.get("counts", {})),
        "controversy_gap_counts_json": json_text(
            readiness_payload.get("controversy_gap_counts", {})
        ),
        "probe_type_counts_json": json_text(
            readiness_payload.get("probe_type_counts", {})
        ),
        "gate_reasons_json": json_text(readiness_payload.get("gate_reasons", [])),
        "recommended_next_skills_json": json_text(
            readiness_payload.get("recommended_next_skills", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(readiness_payload),
    }


def report_basis_freeze_record_row_from_payload(
    report_basis_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "basis_id": maybe_text(report_basis_payload.get("basis_id")),
        "run_id": maybe_text(report_basis_payload.get("run_id")),
        "round_id": maybe_text(report_basis_payload.get("round_id")),
        "generated_at_utc": maybe_text(report_basis_payload.get("generated_at_utc")),
        "report_basis_status": maybe_text(report_basis_payload.get("report_basis_status")),
        "readiness_status": maybe_text(report_basis_payload.get("readiness_status")),
        "board_state_source": maybe_text(report_basis_payload.get("board_state_source")),
        "coverage_source": maybe_text(report_basis_payload.get("coverage_source")),
        "readiness_source": maybe_text(report_basis_payload.get("readiness_source")),
        "next_actions_source": maybe_text(report_basis_payload.get("next_actions_source")),
        "board_brief_source": maybe_text(report_basis_payload.get("board_brief_source")),
        "basis_selection_mode": maybe_text(
            report_basis_payload.get("basis_selection_mode")
        ),
        "basis_counts_json": json_text(report_basis_payload.get("basis_counts", {})),
        "selected_basis_object_ids_json": json_text(
            report_basis_payload.get("selected_basis_object_ids", [])
        ),
        "selected_evidence_refs_json": json_text(
            report_basis_payload.get("selected_evidence_refs", [])
        ),
        "gate_reasons_json": json_text(report_basis_payload.get("gate_reasons", [])),
        "remaining_risks_json": json_text(
            report_basis_payload.get("remaining_risks", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(report_basis_payload),
    }


def report_basis_freeze_item_row_from_payload(
    item: dict[str, Any],
    *,
    basis_id: str,
    run_id: str,
    round_id: str,
    generated_at_utc: str,
    item_group: str,
    item_index: int,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    object_id = report_basis_freeze_item_object_id(item_group, item)
    claim_id = maybe_text(item.get("claim_id"))
    if not claim_id and isinstance(item.get("claim_ids"), list):
        claim_id = maybe_text((item.get("claim_ids") or [""])[0])
    return {
        "item_row_id": report_basis_freeze_item_row_id(
            basis_id,
            item_group,
            item_index,
            object_id,
        ),
        "basis_id": maybe_text(basis_id),
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "generated_at_utc": maybe_text(generated_at_utc),
        "item_group": maybe_text(item_group),
        "item_index": coerce_int(item_index),
        "object_type": report_basis_freeze_item_object_type(item_group, item),
        "object_id": object_id,
        "issue_label": maybe_text(item.get("issue_label")),
        "claim_id": claim_id,
        "recommended_lane": maybe_text(item.get("recommended_lane")),
        "route_status": maybe_text(item.get("route_status")),
        "readiness": maybe_text(item.get("readiness")),
        "evidence_refs_json": json_text(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else []
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(item),
    }


def reporting_handoff_row_from_payload(
    handoff_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "handoff_id": maybe_text(handoff_payload.get("handoff_id")),
        "run_id": maybe_text(handoff_payload.get("run_id")),
        "round_id": maybe_text(handoff_payload.get("round_id")),
        "generated_at_utc": maybe_text(handoff_payload.get("generated_at_utc")),
        "handoff_status": maybe_text(handoff_payload.get("handoff_status")),
        "reporting_ready": 1 if bool(handoff_payload.get("reporting_ready")) else 0,
        "reporting_blockers_json": json_text(
            handoff_payload.get("reporting_blockers", [])
        ),
        "report_basis_status": maybe_text(handoff_payload.get("report_basis_status")),
        "readiness_status": maybe_text(handoff_payload.get("readiness_status")),
        "supervisor_status": maybe_text(handoff_payload.get("supervisor_status")),
        "board_state_source": maybe_text(handoff_payload.get("board_state_source")),
        "coverage_source": maybe_text(handoff_payload.get("coverage_source")),
        "report_basis_source": maybe_text(handoff_payload.get("report_basis_source")),
        "readiness_source": maybe_text(handoff_payload.get("readiness_source")),
        "board_brief_source": maybe_text(handoff_payload.get("board_brief_source")),
        "supervisor_state_source": maybe_text(
            handoff_payload.get("supervisor_state_source")
        ),
        "selected_evidence_refs_json": json_text(
            handoff_payload.get("selected_evidence_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(handoff_payload),
    }


def council_decision_record_row_from_payload(
    decision_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "record_id": maybe_text(decision_payload.get("record_id")),
        "decision_id": maybe_text(decision_payload.get("decision_id")),
        "run_id": maybe_text(decision_payload.get("run_id")),
        "round_id": maybe_text(decision_payload.get("round_id")),
        "generated_at_utc": maybe_text(decision_payload.get("generated_at_utc")),
        "decision_stage": maybe_text(decision_payload.get("decision_stage")),
        "moderator_status": maybe_text(decision_payload.get("moderator_status")),
        "reporting_ready": 1 if bool(decision_payload.get("reporting_ready")) else 0,
        "publication_readiness": maybe_text(
            decision_payload.get("publication_readiness")
        ),
        "decision_gating_json": json_text(
            decision_payload.get("decision_gating", {})
        ),
        "next_round_required": 1
        if bool(decision_payload.get("next_round_required"))
        else 0,
        "canonical_artifact": maybe_text(decision_payload.get("canonical_artifact")),
        "board_state_source": maybe_text(decision_payload.get("board_state_source")),
        "coverage_source": maybe_text(decision_payload.get("coverage_source")),
        "reporting_handoff_source": maybe_text(
            decision_payload.get("reporting_handoff_source")
        ),
        "report_basis_source": maybe_text(decision_payload.get("report_basis_source")),
        "decision_source": maybe_text(decision_payload.get("decision_source")),
        "sociologist_report_source": maybe_text(
            decision_payload.get("sociologist_report_source")
        ),
        "environmentalist_report_source": maybe_text(
            decision_payload.get("environmentalist_report_source")
        ),
        "selected_evidence_refs_json": json_text(
            decision_payload.get("selected_evidence_refs", [])
        ),
        "published_report_refs_json": json_text(
            decision_payload.get("published_report_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(decision_payload),
    }


def expert_report_record_row_from_payload(
    report_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "record_id": maybe_text(report_payload.get("record_id")),
        "report_id": maybe_text(report_payload.get("report_id")),
        "run_id": maybe_text(report_payload.get("run_id")),
        "round_id": maybe_text(report_payload.get("round_id")),
        "generated_at_utc": maybe_text(report_payload.get("generated_at_utc")),
        "report_stage": maybe_text(report_payload.get("report_stage")),
        "agent_role": maybe_text(report_payload.get("agent_role")),
        "status": maybe_text(report_payload.get("status")),
        "handoff_status": maybe_text(report_payload.get("handoff_status")),
        "reporting_ready": 1 if bool(report_payload.get("reporting_ready")) else 0,
        "publication_readiness": maybe_text(
            report_payload.get("publication_readiness")
        ),
        "canonical_artifact": maybe_text(report_payload.get("canonical_artifact")),
        "board_state_source": maybe_text(report_payload.get("board_state_source")),
        "coverage_source": maybe_text(report_payload.get("coverage_source")),
        "reporting_handoff_source": maybe_text(
            report_payload.get("reporting_handoff_source")
        ),
        "decision_source": maybe_text(report_payload.get("decision_source")),
        "expert_report_draft_source": maybe_text(
            report_payload.get("expert_report_draft_source")
        ),
        "board_brief_source": maybe_text(report_payload.get("board_brief_source")),
        "selected_evidence_refs_json": json_text(
            report_payload.get("selected_evidence_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(report_payload),
    }


def final_publication_row_from_payload(
    publication_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "publication_id": maybe_text(publication_payload.get("publication_id")),
        "run_id": maybe_text(publication_payload.get("run_id")),
        "round_id": maybe_text(publication_payload.get("round_id")),
        "generated_at_utc": maybe_text(publication_payload.get("generated_at_utc")),
        "publication_status": maybe_text(
            publication_payload.get("publication_status")
        ),
        "publication_posture": maybe_text(
            publication_payload.get("publication_posture")
        ),
        "board_state_source": maybe_text(
            publication_payload.get("board_state_source")
        ),
        "coverage_source": maybe_text(publication_payload.get("coverage_source")),
        "reporting_handoff_source": maybe_text(
            publication_payload.get("reporting_handoff_source")
        ),
        "decision_source": maybe_text(publication_payload.get("decision_source")),
        "report_basis_source": maybe_text(publication_payload.get("report_basis_source")),
        "supervisor_state_source": maybe_text(
            publication_payload.get("supervisor_state_source")
        ),
        "sociologist_report_source": maybe_text(
            publication_payload.get("sociologist_report_source")
        ),
        "environmentalist_report_source": maybe_text(
            publication_payload.get("environmentalist_report_source")
        ),
        "selected_evidence_refs_json": json_text(
            publication_payload.get("selected_evidence_refs", [])
        ),
        "published_report_refs_json": json_text(
            publication_payload.get("published_report_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(publication_payload),
    }


def runtime_control_freeze_row_from_payload(
    freeze_record: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    artifacts = freeze_record.get("artifacts", {}) if isinstance(freeze_record.get("artifacts"), dict) else {}
    return {
        "freeze_id": maybe_text(freeze_record.get("freeze_id")),
        "run_id": maybe_text(freeze_record.get("run_id")),
        "round_id": maybe_text(freeze_record.get("round_id")),
        "updated_at_utc": maybe_text(freeze_record.get("updated_at_utc")),
        "gate_status": maybe_text(freeze_record.get("gate_status")),
        "readiness_status": maybe_text(freeze_record.get("readiness_status")),
        "report_basis_status": maybe_text(freeze_record.get("report_basis_status")),
        "controller_status": maybe_text(freeze_record.get("controller_status")),
        "supervisor_status": maybe_text(freeze_record.get("supervisor_status")),
        "planning_mode": maybe_text(freeze_record.get("planning_mode")),
        "report_basis_freeze_allowed": 1 if bool(freeze_record.get("report_basis_freeze_allowed")) else 0,
        "gate_reasons_json": json_text(freeze_record.get("gate_reasons", [])),
        "recommended_next_skills_json": json_text(freeze_record.get("recommended_next_skills", [])),
        "reporting_ready": 1 if bool(freeze_record.get("reporting_ready")) else 0,
        "reporting_handoff_status": maybe_text(
            freeze_record.get("reporting_handoff_status")
        ),
        "reporting_blockers_json": json_text(
            freeze_record.get("reporting_blockers", [])
        ),
        "controller_artifact_path": maybe_text(artifacts.get("controller_state_path")),
        "gate_artifact_path": maybe_text(artifacts.get("report_basis_gate_path")),
        "supervisor_artifact_path": maybe_text(artifacts.get("supervisor_state_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(freeze_record),
    }


def control_snapshot_row_id(
    prefix: str,
    payload: dict[str, Any],
    *parts: Any,
) -> str:
    snapshot_payload = dict(payload)
    snapshot_payload.pop("snapshot_id", None)
    return prefix + "-" + stable_hash(prefix, *parts, json_text(snapshot_payload))[:20]


def normalized_controller_snapshot_payload(
    controller_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(controller_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["controller_id"] = maybe_text(
        normalized.get("controller_id")
    ) or controller_snapshot_object_id(
        normalized["run_id"],
        normalized["round_id"],
    )
    normalized["controller_status"] = (
        maybe_text(normalized.get("controller_status")) or "running"
    )
    normalized["planning_mode"] = (
        maybe_text(normalized.get("planning_mode")) or "planner-backed"
    )
    normalized["current_stage"] = maybe_text(normalized.get("current_stage"))
    normalized["failed_stage"] = maybe_text(normalized.get("failed_stage"))
    normalized["resume_status"] = maybe_text(normalized.get("resume_status"))
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "pending"
    )
    normalized["gate_status"] = (
        maybe_text(normalized.get("gate_status")) or "not-evaluated"
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status")) or "not-evaluated"
    )
    normalized["resume_recommended"] = bool(
        maybe_bool(normalized.get("resume_recommended"))
    )
    normalized["restart_recommended"] = bool(
        maybe_bool(normalized.get("restart_recommended"))
    )
    normalized["completed_stage_names"] = unique_texts(
        list_items(normalized.get("completed_stage_names"))
    )
    normalized["pending_stage_names"] = unique_texts(
        list_items(normalized.get("pending_stage_names"))
    )
    normalized["gate_reasons"] = unique_texts(list_items(normalized.get("gate_reasons")))
    normalized["recommended_next_skills"] = unique_texts(
        list_items(normalized.get("recommended_next_skills"))
    )
    normalized["execution_policy"] = dict_items(normalized.get("execution_policy"))
    normalized["progress"] = dict_items(normalized.get("progress"))
    normalized["recovery"] = dict_items(normalized.get("recovery"))
    normalized["resume_from_stage"] = (
        maybe_text(normalized.get("resume_from_stage"))
        or maybe_text(normalized["recovery"].get("resume_from_stage"))
    )
    normalized["planning"] = dict_items(normalized.get("planning"))
    normalized["planning_attempts"] = list_items(normalized.get("planning_attempts"))
    normalized["stage_contracts"] = dict_items(normalized.get("stage_contracts"))
    normalized["steps"] = list_items(normalized.get("steps"))
    normalized["artifacts"] = dict_items(normalized.get("artifacts"))
    normalized["failure"] = dict_items(normalized.get("failure"))
    normalized["snapshot_id"] = maybe_text(
        normalized.get("snapshot_id")
    ) or maybe_text(normalized.get("controller_id"))
    return validate_canonical_payload("controller-state", normalized)


def controller_snapshot_row_from_payload(
    controller_payload: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_controller_snapshot_payload(controller_payload)
    return {
        "snapshot_id": maybe_text(normalized.get("snapshot_id")),
        "controller_id": maybe_text(normalized.get("controller_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "controller_status": maybe_text(normalized.get("controller_status")),
        "planning_mode": maybe_text(normalized.get("planning_mode")),
        "current_stage": maybe_text(normalized.get("current_stage")),
        "failed_stage": maybe_text(normalized.get("failed_stage")),
        "resume_status": maybe_text(normalized.get("resume_status")),
        "readiness_status": maybe_text(normalized.get("readiness_status")),
        "gate_status": maybe_text(normalized.get("gate_status")),
        "report_basis_status": maybe_text(normalized.get("report_basis_status")),
        "resume_recommended": 1 if bool(normalized.get("resume_recommended")) else 0,
        "restart_recommended": 1
        if bool(normalized.get("restart_recommended"))
        else 0,
        "resume_from_stage": maybe_text(normalized.get("resume_from_stage")),
        "completed_stage_names_json": json_text(
            normalized.get("completed_stage_names", [])
        ),
        "pending_stage_names_json": json_text(
            normalized.get("pending_stage_names", [])
        ),
        "gate_reasons_json": json_text(normalized.get("gate_reasons", [])),
        "recommended_next_skills_json": json_text(
            normalized.get("recommended_next_skills", [])
        ),
        "execution_policy_json": json_text(normalized.get("execution_policy", {})),
        "progress_json": json_text(normalized.get("progress", {})),
        "recovery_json": json_text(normalized.get("recovery", {})),
        "planning_json": json_text(normalized.get("planning", {})),
        "planning_attempts_json": json_text(
            normalized.get("planning_attempts", [])
        ),
        "stage_contracts_json": json_text(normalized.get("stage_contracts", {})),
        "steps_json": json_text(normalized.get("steps", [])),
        "artifacts_json": json_text(normalized.get("artifacts", {})),
        "failure_json": json_text(normalized.get("failure", {})),
        "artifact_path": maybe_text(normalized.get("artifacts", {}).get("controller_state_path"))
        or maybe_text(normalized.get("artifact_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }


def normalized_gate_snapshot_payload(
    gate_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(gate_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["stage_name"] = (
        maybe_text(normalized.get("stage_name"))
        or maybe_text(normalized.get("gate_handler"))
        or "report-basis-gate"
    )
    normalized["gate_handler"] = (
        maybe_text(normalized.get("gate_handler"))
        or maybe_text(normalized.get("stage_name"))
        or "report-basis-gate"
    )
    normalized["gate_semantics"] = (
        maybe_text(normalized.get("gate_semantics"))
        or normalized["stage_name"]
    )
    normalized["gate_id"] = maybe_text(
        normalized.get("gate_id")
    ) or gate_snapshot_object_id(
        normalized["run_id"],
        normalized["round_id"],
        normalized["stage_name"],
        normalized["gate_handler"],
    )
    normalized["gate_status"] = (
        maybe_text(normalized.get("gate_status")) or "not-evaluated"
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "pending"
    )
    normalized["report_basis_freeze_allowed"] = bool(
        maybe_bool(normalized.get("report_basis_freeze_allowed"))
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status"))
        or ("frozen" if normalized["report_basis_freeze_allowed"] else "withheld")
    )
    normalized["report_basis_gate_status"] = (
        maybe_text(normalized.get("report_basis_gate_status"))
        or (
            "report-basis-freeze-allowed"
            if normalized["report_basis_freeze_allowed"]
            else "report-basis-freeze-withheld"
        )
    )
    normalized["decision_source"] = (
        maybe_text(normalized.get("decision_source")) or "policy-fallback"
    )
    normalized["report_basis_resolution_mode"] = maybe_text(
        normalized.get("report_basis_resolution_mode")
    )
    normalized["report_basis_resolution_mode"] = (
        maybe_text(normalized.get("report_basis_resolution_mode"))
        or normalized["report_basis_resolution_mode"]
    )
    normalized["gate_reasons"] = unique_texts(list_items(normalized.get("gate_reasons")))
    normalized["supporting_proposal_ids"] = unique_texts(
        list_items(normalized.get("supporting_proposal_ids"))
    )
    normalized["rejected_proposal_ids"] = unique_texts(
        list_items(normalized.get("rejected_proposal_ids"))
    )
    normalized["supporting_opinion_ids"] = unique_texts(
        list_items(normalized.get("supporting_opinion_ids"))
    )
    normalized["rejected_opinion_ids"] = unique_texts(
        list_items(normalized.get("rejected_opinion_ids"))
    )
    normalized["council_input_counts"] = dict_items(
        normalized.get("council_input_counts")
    )
    normalized["recommended_next_skills"] = unique_texts(
        list_items(normalized.get("recommended_next_skills"))
    )
    normalized["warnings"] = list_items(normalized.get("warnings"))
    normalized["readiness_path"] = maybe_text(normalized.get("readiness_path"))
    normalized["output_path"] = (
        maybe_text(normalized.get("output_path"))
        or maybe_text(normalized.get("artifact_path"))
    )
    normalized["snapshot_id"] = maybe_text(
        normalized.get("snapshot_id")
    ) or maybe_text(normalized.get("gate_id"))
    return validate_canonical_payload("gate-state", normalized)


def gate_snapshot_row_from_payload(
    gate_payload: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_gate_snapshot_payload(gate_payload)
    return {
        "snapshot_id": maybe_text(normalized.get("snapshot_id")),
        "gate_id": maybe_text(normalized.get("gate_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "stage_name": maybe_text(normalized.get("stage_name")),
        "gate_handler": maybe_text(normalized.get("gate_handler")),
        "gate_status": maybe_text(normalized.get("gate_status")),
        "readiness_status": maybe_text(normalized.get("readiness_status")),
        "report_basis_freeze_allowed": 1 if bool(normalized.get("report_basis_freeze_allowed")) else 0,
        "decision_source": maybe_text(normalized.get("decision_source")),
        "report_basis_resolution_mode": maybe_text(
            normalized.get("report_basis_resolution_mode")
        ),
        "gate_reasons_json": json_text(normalized.get("gate_reasons", [])),
        "supporting_proposal_ids_json": json_text(
            normalized.get("supporting_proposal_ids", [])
        ),
        "rejected_proposal_ids_json": json_text(
            normalized.get("rejected_proposal_ids", [])
        ),
        "supporting_opinion_ids_json": json_text(
            normalized.get("supporting_opinion_ids", [])
        ),
        "rejected_opinion_ids_json": json_text(
            normalized.get("rejected_opinion_ids", [])
        ),
        "council_input_counts_json": json_text(
            normalized.get("council_input_counts", {})
        ),
        "recommended_next_skills_json": json_text(
            normalized.get("recommended_next_skills", [])
        ),
        "warnings_json": json_text(normalized.get("warnings", [])),
        "readiness_path": maybe_text(normalized.get("readiness_path")),
        "output_path": maybe_text(normalized.get("output_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }


def normalized_supervisor_snapshot_payload(
    supervisor_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(supervisor_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["supervisor_id"] = maybe_text(
        normalized.get("supervisor_id")
    ) or supervisor_snapshot_object_id(
        normalized["run_id"],
        normalized["round_id"],
    )
    normalized["supervisor_status"] = (
        maybe_text(normalized.get("supervisor_status")) or "unavailable"
    )
    normalized["supervisor_substatus"] = (
        maybe_text(normalized.get("supervisor_substatus")) or "unclassified"
    )
    normalized["phase2_posture"] = (
        maybe_text(normalized.get("phase2_posture"))
        or normalized["supervisor_status"]
    )
    normalized["terminal_state"] = (
        maybe_text(normalized.get("terminal_state"))
        or normalized["phase2_posture"]
    )
    normalized["recovery_posture"] = (
        maybe_text(normalized.get("recovery_posture")) or "terminal"
    )
    normalized["operator_action"] = (
        maybe_text(normalized.get("operator_action")) or "inspect-runtime"
    )
    normalized["controller_status"] = (
        maybe_text(normalized.get("controller_status")) or "missing"
    )
    normalized["planning_mode"] = (
        maybe_text(normalized.get("planning_mode")) or "planner-backed"
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "pending"
    )
    normalized["gate_status"] = (
        maybe_text(normalized.get("gate_status")) or "not-evaluated"
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status")) or "not-evaluated"
    )
    reporting_blockers = unique_texts(list_items(normalized.get("reporting_blockers")))
    reporting_ready = maybe_bool(normalized.get("reporting_ready"))
    if reporting_ready is None:
        reporting_ready = bool(
            reporting_gate_state(
                report_basis_status=normalized["report_basis_status"],
                readiness_status=normalized["readiness_status"],
                supervisor_status=normalized["supervisor_status"],
                require_supervisor=True,
                reporting_blockers_value=reporting_blockers,
                handoff_status=maybe_text(
                    normalized.get("reporting_handoff_status")
                )
                or maybe_text(normalized.get("handoff_status")),
            ).get("reporting_ready")
        )
    normalized["reporting_ready"] = bool(reporting_ready)
    normalized["reporting_handoff_status"] = normalize_reporting_handoff_status(
        maybe_text(normalized.get("reporting_handoff_status"))
        or maybe_text(normalized.get("handoff_status"))
    ) or ("reporting-ready" if normalized["reporting_ready"] else "investigation-open")
    normalized["resume_status"] = maybe_text(normalized.get("resume_status"))
    normalized["current_stage"] = maybe_text(normalized.get("current_stage"))
    normalized["failed_stage"] = maybe_text(normalized.get("failed_stage"))
    normalized["resume_recommended"] = bool(
        maybe_bool(normalized.get("resume_recommended"))
    )
    normalized["restart_recommended"] = bool(
        maybe_bool(normalized.get("restart_recommended"))
    )
    normalized["resume_from_stage"] = maybe_text(normalized.get("resume_from_stage"))
    normalized["reporting_blockers"] = reporting_blockers
    normalized["recommended_next_skills"] = unique_texts(
        list_items(normalized.get("recommended_next_skills"))
    )
    normalized["execution_policy"] = dict_items(normalized.get("execution_policy"))
    normalized["round_transition"] = dict_items(normalized.get("round_transition"))
    normalized["top_actions"] = list_items(normalized.get("top_actions"))
    normalized["operator_notes"] = list_items(normalized.get("operator_notes"))
    normalized["inspection_paths"] = dict_items(normalized.get("inspection_paths"))
    normalized["supervisor_path"] = (
        maybe_text(normalized.get("supervisor_path"))
        or maybe_text(normalized.get("artifact_path"))
    )
    normalized["snapshot_id"] = maybe_text(
        normalized.get("snapshot_id")
    ) or maybe_text(normalized.get("supervisor_id"))
    return validate_canonical_payload("supervisor-state", normalized)


def supervisor_snapshot_row_from_payload(
    supervisor_payload: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_supervisor_snapshot_payload(supervisor_payload)
    return {
        "snapshot_id": maybe_text(normalized.get("snapshot_id")),
        "supervisor_id": maybe_text(normalized.get("supervisor_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "supervisor_status": maybe_text(normalized.get("supervisor_status")),
        "supervisor_substatus": maybe_text(normalized.get("supervisor_substatus")),
        "phase2_posture": maybe_text(normalized.get("phase2_posture")),
        "terminal_state": maybe_text(normalized.get("terminal_state")),
        "recovery_posture": maybe_text(normalized.get("recovery_posture")),
        "operator_action": maybe_text(normalized.get("operator_action")),
        "controller_status": maybe_text(normalized.get("controller_status")),
        "planning_mode": maybe_text(normalized.get("planning_mode")),
        "readiness_status": maybe_text(normalized.get("readiness_status")),
        "gate_status": maybe_text(normalized.get("gate_status")),
        "report_basis_status": maybe_text(normalized.get("report_basis_status")),
        "reporting_ready": 1 if bool(normalized.get("reporting_ready")) else 0,
        "reporting_handoff_status": maybe_text(
            normalized.get("reporting_handoff_status")
        ),
        "resume_status": maybe_text(normalized.get("resume_status")),
        "current_stage": maybe_text(normalized.get("current_stage")),
        "failed_stage": maybe_text(normalized.get("failed_stage")),
        "resume_recommended": 1 if bool(normalized.get("resume_recommended")) else 0,
        "restart_recommended": 1
        if bool(normalized.get("restart_recommended"))
        else 0,
        "resume_from_stage": maybe_text(normalized.get("resume_from_stage")),
        "reporting_blockers_json": json_text(
            normalized.get("reporting_blockers", [])
        ),
        "recommended_next_skills_json": json_text(
            normalized.get("recommended_next_skills", [])
        ),
        "execution_policy_json": json_text(normalized.get("execution_policy", {})),
        "round_transition_json": json_text(
            normalized.get("round_transition", {})
        ),
        "top_actions_json": json_text(normalized.get("top_actions", [])),
        "operator_notes_json": json_text(normalized.get("operator_notes", [])),
        "inspection_paths_json": json_text(
            normalized.get("inspection_paths", {})
        ),
        "artifact_path": maybe_text(normalized.get("supervisor_path"))
        or maybe_text(normalized.get("artifact_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }


def planning_source_from_runtime_plan(plan_payload: dict[str, Any]) -> str:
    explicit_source = maybe_text(plan_payload.get("plan_source"))
    if explicit_source:
        return explicit_source
    return "runtime-planner"


def normalized_orchestration_plan_payload(
    plan_payload: dict[str, Any],
    *,
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(plan_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["planning_status"] = (
        maybe_text(normalized.get("planning_status")) or "ready-for-controller"
    )
    normalized["planning_mode"] = (
        maybe_text(normalized.get("planning_mode")) or "planner-backed-phase2"
    )
    normalized["controller_authority"] = (
        maybe_text(normalized.get("controller_authority"))
        or "queue-owner"
    )
    normalized["plan_source"] = planning_source_from_runtime_plan(normalized)
    normalized["council_execution_mode"] = maybe_text(
        normalized.get("council_execution_mode")
    )
    normalized["downstream_posture"] = (
        maybe_text(normalized.get("downstream_posture"))
        or "hold-investigation-open"
    )
    normalized["probe_stage_included"] = bool(
        maybe_bool(normalized.get("probe_stage_included"))
    )
    normalized["assigned_role_hints"] = unique_texts(
        list_items(normalized.get("assigned_role_hints"))
    )
    normalized["phase_decision_basis"] = dict_items(
        normalized.get("phase_decision_basis")
    )
    normalized["agent_turn_hints"] = dict_items(normalized.get("agent_turn_hints"))
    normalized["observed_state"] = dict_items(normalized.get("observed_state"))
    normalized["inputs"] = dict_items(normalized.get("inputs"))
    normalized["execution_queue"] = [
        dict(item)
        for item in list_items(normalized.get("execution_queue"))
        if isinstance(item, dict)
    ]
    normalized["gate_steps"] = [
        dict(item)
        for item in list_items(normalized.get("gate_steps"))
        if isinstance(item, dict)
    ]
    normalized["derived_exports"] = [
        dict(item)
        for item in list_items(normalized.get("derived_exports"))
        if isinstance(item, dict)
    ]
    normalized["post_gate_steps"] = [
        dict(item)
        for item in list_items(normalized.get("post_gate_steps"))
        if isinstance(item, dict)
    ]
    normalized["stop_conditions"] = [
        dict(item)
        for item in list_items(normalized.get("stop_conditions"))
        if isinstance(item, dict)
    ]
    normalized["fallback_path"] = [
        dict(item)
        for item in list_items(normalized.get("fallback_path"))
        if isinstance(item, dict)
    ]
    normalized["planning_notes"] = [
        maybe_text(item)
        for item in list_items(normalized.get("planning_notes"))
        if maybe_text(item)
    ]
    normalized["deliberation_sync"] = dict_items(normalized.get("deliberation_sync"))
    resolved_artifact_path = (
        maybe_text(artifact_path)
        or maybe_text(normalized.get("artifact_path"))
        or maybe_text(normalized.get("output_path"))
    )
    normalized["artifact_path"] = resolved_artifact_path
    normalized["step_counts"] = {
        "execution_queue_count": len(normalized["execution_queue"]),
        "gate_step_count": len(normalized["gate_steps"]),
        "derived_export_count": len(normalized["derived_exports"]),
        "post_gate_step_count": len(normalized["post_gate_steps"]),
        "planned_stage_count": len(normalized["execution_queue"])
        + len(normalized["gate_steps"])
        + len(normalized["derived_exports"])
        + len(normalized["post_gate_steps"]),
    }
    normalized["plan_id"] = maybe_text(normalized.get("plan_id")) or (
        orchestration_plan_object_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["plan_source"],
            normalized["artifact_path"],
        )
    )
    return validate_canonical_payload("orchestration-plan", normalized)


def normalized_orchestration_plan_step_payload(
    step_payload: dict[str, Any],
    *,
    plan_id: str,
    run_id: str,
    round_id: str,
    generated_at_utc: str,
    planning_mode: str,
    controller_authority: str,
    plan_source: str,
    plan_step_group: str,
    step_index: int,
    artifact_path: str,
) -> dict[str, Any]:
    normalized = dict(step_payload)
    skill_name = maybe_text(normalized.get("skill_name"))
    stage_name = (
        maybe_text(normalized.get("stage_name"))
        or maybe_text(normalized.get("stage"))
        or skill_name
        or maybe_text(normalized.get("gate_handler"))
    )
    default_stage_kind = "gate" if plan_step_group == "gate-step" else "skill"
    stage_kind = (
        maybe_text(normalized.get("stage_kind") or normalized.get("kind"))
        or default_stage_kind
    )
    default_phase_group = (
        "gate"
        if plan_step_group == "gate-step"
        else "exports"
        if plan_step_group == "derived-export"
        else "execution"
    )
    expected_output_path = (
        maybe_text(normalized.get("expected_output_path"))
        or maybe_text(normalized.get("output_path"))
        or artifact_path
    )
    required_previous_stages = [
        maybe_text(value)
        for value in list_items(normalized.get("required_previous_stages"))
        if maybe_text(value)
    ]
    skill_args = [
        maybe_text(value)
        for value in list_items(normalized.get("skill_args"))
        if maybe_text(value)
    ]
    blocking_value = maybe_bool(normalized.get("blocking"))
    if blocking_value is None:
        blocking_value = plan_step_group != "derived-export"
    required_for_controller_value = maybe_bool(
        normalized.get("required_for_controller")
    )
    if required_for_controller_value is None:
        required_for_controller_value = plan_step_group != "derived-export"
    normalized.update(
        {
            "run_id": run_id,
            "round_id": round_id,
            "plan_id": plan_id,
            "generated_at_utc": generated_at_utc,
            "plan_step_group": plan_step_group,
            "planning_mode": planning_mode,
            "controller_authority": controller_authority,
            "plan_source": plan_source,
            "phase_group": maybe_text(normalized.get("phase_group"))
            or default_phase_group,
            "stage_name": stage_name,
            "stage_kind": stage_kind,
            "skill_name": skill_name,
            "expected_skill_name": maybe_text(normalized.get("expected_skill_name"))
            or skill_name,
            "assigned_role_hint": maybe_text(normalized.get("assigned_role_hint")),
            "blocking": bool(blocking_value),
            "resume_policy": maybe_text(normalized.get("resume_policy"))
            or "skip-if-completed",
            "gate_handler": maybe_text(normalized.get("gate_handler")),
            "readiness_stage_name": maybe_text(
                normalized.get("readiness_stage_name")
            ),
            "reason": maybe_text(normalized.get("reason")),
            "operator_summary": maybe_text(normalized.get("operator_summary")),
            "expected_output_path": expected_output_path,
            "required_for_controller": bool(required_for_controller_value),
            "export_mode": maybe_text(normalized.get("export_mode")),
            "required_previous_stages": required_previous_stages,
            "skill_args": skill_args,
            "artifact_path": artifact_path,
        }
    )
    normalized["step_id"] = maybe_text(normalized.get("step_id")) or (
        orchestration_plan_step_object_id(
            plan_id,
            plan_step_group,
            step_index,
            stage_name,
            skill_name,
        )
    )
    return validate_canonical_payload("orchestration-plan-step", normalized)


def orchestration_plan_row_from_payload(
    plan_payload: dict[str, Any],
    *,
    artifact_path: str = "",
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_orchestration_plan_payload(
        plan_payload,
        artifact_path=artifact_path,
    )
    step_counts = (
        normalized.get("step_counts", {})
        if isinstance(normalized.get("step_counts"), dict)
        else {}
    )
    return {
        "plan_id": maybe_text(normalized.get("plan_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "planning_status": maybe_text(normalized.get("planning_status")),
        "planning_mode": maybe_text(normalized.get("planning_mode")),
        "controller_authority": maybe_text(
            normalized.get("controller_authority")
        ),
        "plan_source": maybe_text(normalized.get("plan_source")),
        "council_execution_mode": maybe_text(
            normalized.get("council_execution_mode")
        ),
        "downstream_posture": maybe_text(normalized.get("downstream_posture")),
        "probe_stage_included": 1
        if bool(normalized.get("probe_stage_included"))
        else 0,
        "artifact_path": maybe_text(normalized.get("artifact_path")),
        "execution_queue_count": coerce_int(
            step_counts.get("execution_queue_count")
        ),
        "gate_step_count": coerce_int(step_counts.get("gate_step_count")),
        "derived_export_count": coerce_int(step_counts.get("derived_export_count")),
        "post_gate_step_count": coerce_int(
            step_counts.get("post_gate_step_count")
        ),
        "planned_stage_count": coerce_int(step_counts.get("planned_stage_count")),
        "assigned_role_hints_json": json_text(
            normalized.get("assigned_role_hints", [])
        ),
        "phase_decision_basis_json": json_text(
            normalized.get("phase_decision_basis", {})
        ),
        "agent_turn_hints_json": json_text(
            normalized.get("agent_turn_hints", {})
        ),
        "observed_state_json": json_text(normalized.get("observed_state", {})),
        "inputs_json": json_text(normalized.get("inputs", {})),
        "execution_queue_json": json_text(normalized.get("execution_queue", [])),
        "gate_steps_json": json_text(normalized.get("gate_steps", [])),
        "derived_exports_json": json_text(normalized.get("derived_exports", [])),
        "post_gate_steps_json": json_text(normalized.get("post_gate_steps", [])),
        "stop_conditions_json": json_text(normalized.get("stop_conditions", [])),
        "fallback_path_json": json_text(normalized.get("fallback_path", [])),
        "planning_notes_json": json_text(normalized.get("planning_notes", [])),
        "deliberation_sync_json": json_text(
            normalized.get("deliberation_sync", {})
        ),
        "step_counts_json": json_text(step_counts),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }


def iter_orchestration_plan_step_rows(
    plan_payload: dict[str, Any],
    *,
    artifact_path: str = "",
) -> list[dict[str, Any]]:
    normalized = normalized_orchestration_plan_payload(
        plan_payload,
        artifact_path=artifact_path,
    )
    rows: list[dict[str, Any]] = []
    sections = (
        ("execution_queue", "execution-queue"),
        ("gate_steps", "gate-step"),
        ("derived_exports", "derived-export"),
        ("post_gate_steps", "post-gate-step"),
    )
    for section_key, step_group in sections:
        steps = (
            normalized.get(section_key, [])
            if isinstance(normalized.get(section_key), list)
            else []
        )
        for step_index, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            step_payload = normalized_orchestration_plan_step_payload(
                step,
                plan_id=maybe_text(normalized.get("plan_id")),
                run_id=maybe_text(normalized.get("run_id")),
                round_id=maybe_text(normalized.get("round_id")),
                generated_at_utc=maybe_text(normalized.get("generated_at_utc")),
                planning_mode=maybe_text(normalized.get("planning_mode")),
                controller_authority=maybe_text(
                    normalized.get("controller_authority")
                ),
                plan_source=maybe_text(normalized.get("plan_source")),
                plan_step_group=step_group,
                step_index=step_index,
                artifact_path=maybe_text(normalized.get("artifact_path")),
            )
            rows.append(
                {
                    "step_id": maybe_text(step_payload.get("step_id")),
                    "plan_id": maybe_text(step_payload.get("plan_id")),
                    "run_id": maybe_text(step_payload.get("run_id")),
                    "round_id": maybe_text(step_payload.get("round_id")),
                    "generated_at_utc": maybe_text(
                        step_payload.get("generated_at_utc")
                    ),
                    "plan_step_group": maybe_text(
                        step_payload.get("plan_step_group")
                    ),
                    "step_index": step_index,
                    "planning_mode": maybe_text(
                        step_payload.get("planning_mode")
                    ),
                    "controller_authority": maybe_text(
                        step_payload.get("controller_authority")
                    ),
                    "plan_source": maybe_text(step_payload.get("plan_source")),
                    "phase_group": maybe_text(step_payload.get("phase_group")),
                    "stage_name": maybe_text(step_payload.get("stage_name")),
                    "stage_kind": maybe_text(step_payload.get("stage_kind")),
                    "skill_name": maybe_text(step_payload.get("skill_name")),
                    "expected_skill_name": maybe_text(
                        step_payload.get("expected_skill_name")
                    ),
                    "assigned_role_hint": maybe_text(
                        step_payload.get("assigned_role_hint")
                    ),
                    "blocking": 1 if bool(step_payload.get("blocking")) else 0,
                    "resume_policy": maybe_text(step_payload.get("resume_policy")),
                    "gate_handler": maybe_text(step_payload.get("gate_handler")),
                    "readiness_stage_name": maybe_text(
                        step_payload.get("readiness_stage_name")
                    ),
                    "reason": maybe_text(step_payload.get("reason")),
                    "operator_summary": maybe_text(
                        step_payload.get("operator_summary")
                    ),
                    "expected_output_path": maybe_text(
                        step_payload.get("expected_output_path")
                    ),
                    "required_for_controller": 1
                    if bool(step_payload.get("required_for_controller"))
                    else 0,
                    "export_mode": maybe_text(step_payload.get("export_mode")),
                    "required_previous_stages_json": json_text(
                        step_payload.get("required_previous_stages", [])
                    ),
                    "skill_args_json": json_text(
                        step_payload.get("skill_args", [])
                    ),
                    "artifact_path": maybe_text(step_payload.get("artifact_path")),
                    "record_locator": f"$.{section_key}[{step_index}]",
                    "raw_json": json_text(step_payload),
                }
            )
    return rows


def moderator_action_snapshot_row_from_payload(
    snapshot_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "snapshot_id": maybe_text(snapshot_payload.get("snapshot_id")),
        "run_id": maybe_text(snapshot_payload.get("run_id")),
        "round_id": maybe_text(snapshot_payload.get("round_id")),
        "generated_at_utc": maybe_text(snapshot_payload.get("generated_at_utc")),
        "action_source": maybe_text(snapshot_payload.get("action_source")) or "next-actions-artifact",
        "board_state_source": maybe_text(snapshot_payload.get("board_state_source")),
        "coverage_source": maybe_text(snapshot_payload.get("coverage_source")),
        "action_count": coerce_int(
            snapshot_payload.get("action_count")
            or (
                len(snapshot_payload.get("ranked_actions", []))
                if isinstance(snapshot_payload.get("ranked_actions"), list)
                else 0
            )
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(snapshot_payload),
    }


def falsification_probe_snapshot_row_from_payload(
    snapshot_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "snapshot_id": maybe_text(snapshot_payload.get("snapshot_id")),
        "run_id": maybe_text(snapshot_payload.get("run_id")),
        "round_id": maybe_text(snapshot_payload.get("round_id")),
        "generated_at_utc": maybe_text(snapshot_payload.get("generated_at_utc")),
        "action_source": maybe_text(snapshot_payload.get("action_source")) or "falsification-probes-artifact",
        "board_state_source": maybe_text(snapshot_payload.get("board_state_source")),
        "coverage_source": maybe_text(snapshot_payload.get("coverage_source")),
        "probe_count": coerce_int(
            snapshot_payload.get("probe_count")
            or (
                len(snapshot_payload.get("probes", []))
                if isinstance(snapshot_payload.get("probes"), list)
                else 0
            )
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(snapshot_payload),
    }


def round_task_snapshot_row_from_payload(
    snapshot_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "snapshot_id": maybe_text(snapshot_payload.get("snapshot_id")),
        "run_id": maybe_text(snapshot_payload.get("run_id")),
        "round_id": maybe_text(snapshot_payload.get("round_id")),
        "generated_at_utc": maybe_text(snapshot_payload.get("generated_at_utc")),
        "task_source": maybe_text(snapshot_payload.get("task_source"))
        or "round-tasks-artifact",
        "task_count": coerce_int(
            snapshot_payload.get("task_count")
            or (
                len(snapshot_payload.get("tasks", []))
                if isinstance(snapshot_payload.get("tasks"), list)
                else 0
            )
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(snapshot_payload),
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


def build_moderator_action_payload(
    actions: list[dict[str, Any]],
    *,
    snapshot_payload: dict[str, Any] | None = None,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any]:
    payload = dict(snapshot_payload) if isinstance(snapshot_payload, dict) else {}
    action_run_id = maybe_text(actions[0].get("run_id")) if actions else ""
    action_round_id = maybe_text(actions[0].get("round_id")) if actions else ""
    action_generated_at = maybe_text(actions[-1].get("generated_at_utc")) if actions else ""
    payload["run_id"] = maybe_text(payload.get("run_id")) or maybe_text(run_id) or action_run_id
    payload["round_id"] = (
        maybe_text(payload.get("round_id")) or maybe_text(round_id) or action_round_id
    )
    payload["generated_at_utc"] = (
        maybe_text(payload.get("generated_at_utc")) or action_generated_at or utc_now_iso()
    )
    payload["ranked_actions"] = [
        cleaned_wrapper_record(
            dict(action),
            metadata_fields=("action_rank", "artifact_path", "record_locator"),
            optional_empty_fields=(
                "controversy_gap",
                "recommended_lane",
                "issue_label",
                "target_actor_id",
                "target_proposal_id",
                "source_proposal_id",
            ),
        )
        for action in actions
    ]
    payload["action_count"] = len(actions)
    payload["action_source"] = (
        maybe_text(payload.get("action_source")) or "deliberation-plane-actions"
    )
    return payload


def build_falsification_probe_payload(
    probes: list[dict[str, Any]],
    *,
    snapshot_payload: dict[str, Any] | None = None,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any]:
    payload = dict(snapshot_payload) if isinstance(snapshot_payload, dict) else {}
    probe_run_id = maybe_text(probes[0].get("run_id")) if probes else ""
    probe_round_id = maybe_text(probes[0].get("round_id")) if probes else ""
    probe_generated_at = maybe_text(probes[-1].get("opened_at_utc")) if probes else ""
    payload["run_id"] = maybe_text(payload.get("run_id")) or maybe_text(run_id) or probe_run_id
    payload["round_id"] = (
        maybe_text(payload.get("round_id")) or maybe_text(round_id) or probe_round_id
    )
    payload["generated_at_utc"] = (
        maybe_text(payload.get("generated_at_utc")) or probe_generated_at or utc_now_iso()
    )
    payload["probes"] = [
        cleaned_wrapper_record(
            dict(probe),
            metadata_fields=("artifact_path", "record_locator"),
            optional_empty_fields=(
                "action_id",
                "controversy_gap",
                "recommended_lane",
                "target_actor_id",
                "target_proposal_id",
                "source_proposal_id",
            ),
        )
        for probe in probes
    ]
    payload["probe_count"] = len(probes)
    return payload


def resolved_runtime_control_freeze_artifacts(
    existing_record: dict[str, Any],
    *,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, str]:
    existing_artifacts = (
        existing_record.get("artifacts", {})
        if isinstance(existing_record.get("artifacts"), dict)
        else {}
    )
    controller_artifacts = (
        controller_snapshot.get("artifacts", {})
        if isinstance(controller_snapshot, dict)
        and isinstance(controller_snapshot.get("artifacts"), dict)
        else {}
    )
    supervisor_inspection = (
        supervisor_snapshot.get("inspection_paths", {})
        if isinstance(supervisor_snapshot, dict)
        and isinstance(supervisor_snapshot.get("inspection_paths"), dict)
        else {}
    )
    explicit = artifact_paths if isinstance(artifact_paths, dict) else {}
    gate_output_path = (
        maybe_text(gate_snapshot.get("output_path"))
        if isinstance(gate_snapshot, dict)
        else ""
    )
    supervisor_gate_path = (
        maybe_text(supervisor_snapshot.get("report_basis_gate_path"))
        if isinstance(supervisor_snapshot, dict)
        else ""
    )
    supervisor_path = (
        maybe_text(supervisor_snapshot.get("supervisor_path"))
        if isinstance(supervisor_snapshot, dict)
        else ""
    )
    report_basis_gate_artifact_path = (
        maybe_text(explicit.get("report_basis_gate_path"))
        or gate_output_path
        or maybe_text(controller_artifacts.get("report_basis_gate_path"))
        or supervisor_gate_path
        or maybe_text(supervisor_inspection.get("gate_path"))
        or maybe_text(existing_artifacts.get("report_basis_gate_path"))
    )
    return {
        "controller_state_path": maybe_text(explicit.get("controller_state_path"))
        or maybe_text(controller_artifacts.get("controller_state_path"))
        or maybe_text(existing_artifacts.get("controller_state_path")),
        "report_basis_gate_path": report_basis_gate_artifact_path,
        "supervisor_state_path": maybe_text(explicit.get("supervisor_state_path"))
        or supervisor_path
        or maybe_text(existing_artifacts.get("supervisor_state_path")),
    }


def merged_runtime_control_freeze_record(
    *,
    run_id: str,
    round_id: str,
    existing_record: dict[str, Any] | None = None,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = dict(existing_record) if isinstance(existing_record, dict) else {}
    normalized_run_id = maybe_text(run_id) or maybe_text(record.get("run_id"))
    normalized_round_id = maybe_text(round_id) or maybe_text(record.get("round_id"))
    record["schema_version"] = "runtime-control-freeze-v1"
    record["freeze_id"] = maybe_text(record.get("freeze_id")) or runtime_control_freeze_id(
        normalized_run_id,
        normalized_round_id,
    )
    record["run_id"] = normalized_run_id
    record["round_id"] = normalized_round_id
    if isinstance(controller_snapshot, dict) and controller_snapshot:
        record["controller_snapshot"] = normalized_controller_snapshot_payload(
            {
                **controller_snapshot,
                "run_id": maybe_text(controller_snapshot.get("run_id"))
                or normalized_run_id,
                "round_id": maybe_text(controller_snapshot.get("round_id"))
                or normalized_round_id,
            }
        )
    if isinstance(gate_snapshot, dict) and gate_snapshot:
        record["gate_snapshot"] = normalized_gate_snapshot_payload(
            {
                **gate_snapshot,
                "run_id": maybe_text(gate_snapshot.get("run_id"))
                or normalized_run_id,
                "round_id": maybe_text(gate_snapshot.get("round_id"))
                or normalized_round_id,
            }
        )
    if isinstance(supervisor_snapshot, dict) and supervisor_snapshot:
        record["supervisor_snapshot"] = normalized_supervisor_snapshot_payload(
            {
                **supervisor_snapshot,
                "run_id": maybe_text(supervisor_snapshot.get("run_id"))
                or normalized_run_id,
                "round_id": maybe_text(supervisor_snapshot.get("round_id"))
                or normalized_round_id,
            }
        )

    resolved_controller = (
        record.get("controller_snapshot", {})
        if isinstance(record.get("controller_snapshot"), dict)
        else {}
    )
    resolved_gate = (
        record.get("gate_snapshot", {})
        if isinstance(record.get("gate_snapshot"), dict)
        else {}
    )
    resolved_supervisor = (
        record.get("supervisor_snapshot", {})
        if isinstance(record.get("supervisor_snapshot"), dict)
        else {}
    )
    record["updated_at_utc"] = (
        maybe_text(resolved_supervisor.get("generated_at_utc"))
        or maybe_text(resolved_controller.get("generated_at_utc"))
        or maybe_text(resolved_gate.get("generated_at_utc"))
        or maybe_text(record.get("updated_at_utc"))
        or utc_now_iso()
    )
    record["gate_status"] = (
        maybe_text(resolved_supervisor.get("gate_status"))
        or maybe_text(resolved_controller.get("gate_status"))
        or maybe_text(resolved_gate.get("gate_status"))
        or maybe_text(record.get("gate_status"))
    )
    record["readiness_status"] = (
        maybe_text(resolved_supervisor.get("readiness_status"))
        or maybe_text(resolved_controller.get("readiness_status"))
        or maybe_text(resolved_gate.get("readiness_status"))
        or maybe_text(record.get("readiness_status"))
    )
    record["report_basis_status"] = (
        maybe_text(resolved_supervisor.get("report_basis_status"))
        or maybe_text(resolved_controller.get("report_basis_status"))
        or maybe_text(resolved_gate.get("report_basis_status"))
        or maybe_text(record.get("report_basis_status"))
    )
    record["controller_status"] = (
        maybe_text(resolved_controller.get("controller_status"))
        or maybe_text(record.get("controller_status"))
    )
    record["supervisor_status"] = (
        maybe_text(resolved_supervisor.get("supervisor_status"))
        or maybe_text(record.get("supervisor_status"))
    )
    record["planning_mode"] = (
        maybe_text(resolved_supervisor.get("planning_mode"))
        or maybe_text(resolved_controller.get("planning_mode"))
        or maybe_text(
            resolved_controller.get("planning", {}).get("planning_mode")
            if isinstance(resolved_controller.get("planning"), dict)
            else ""
        )
        or maybe_text(record.get("planning_mode"))
    )
    gate_present = isinstance(resolved_gate, dict) and bool(resolved_gate)
    report_basis_freeze_allowed = (
        bool(resolved_gate.get("report_basis_freeze_allowed"))
        if gate_present
        else bool(record.get("report_basis_freeze_allowed"))
    )
    if record["gate_status"] == "report-basis-freeze-allowed":
        report_basis_freeze_allowed = True
    record["report_basis_freeze_allowed"] = report_basis_freeze_allowed
    record["report_basis_gate_status"] = (
        maybe_text(resolved_gate.get("report_basis_gate_status"))
        or maybe_text(record.get("report_basis_gate_status"))
        or (
            "report-basis-freeze-allowed"
            if report_basis_freeze_allowed
            else "report-basis-freeze-withheld"
        )
    )
    record["gate_reasons"] = unique_texts(
        (
            resolved_supervisor.get("gate_reasons", [])
            if isinstance(resolved_supervisor.get("gate_reasons"), list)
            else []
        )
        + (
            resolved_controller.get("gate_reasons", [])
            if isinstance(resolved_controller.get("gate_reasons"), list)
            else []
        )
        + (
            resolved_gate.get("gate_reasons", [])
            if isinstance(resolved_gate.get("gate_reasons"), list)
            else []
        )
        + (
            record.get("gate_reasons", [])
            if isinstance(record.get("gate_reasons"), list)
            else []
        )
    )
    record["recommended_next_skills"] = unique_texts(
        (
            resolved_supervisor.get("recommended_next_skills", [])
            if isinstance(resolved_supervisor.get("recommended_next_skills"), list)
            else []
        )
        + (
            resolved_controller.get("recommended_next_skills", [])
            if isinstance(resolved_controller.get("recommended_next_skills"), list)
            else []
        )
        + (
            resolved_gate.get("recommended_next_skills", [])
            if isinstance(resolved_gate.get("recommended_next_skills"), list)
            else []
        )
        + (
            record.get("recommended_next_skills", [])
            if isinstance(record.get("recommended_next_skills"), list)
            else []
        )
    )
    report_ready_value = maybe_bool(resolved_supervisor.get("reporting_ready"))
    if report_ready_value is None:
        report_ready_value = maybe_bool(record.get("reporting_ready"))
    record["reporting_ready"] = bool(report_ready_value)
    record["reporting_handoff_status"] = normalize_reporting_handoff_status(
        maybe_text(resolved_supervisor.get("reporting_handoff_status"))
        or maybe_text(resolved_supervisor.get("handoff_status"))
        or maybe_text(record.get("reporting_handoff_status"))
    ) or ("reporting-ready" if record["reporting_ready"] else "investigation-open")
    record["reporting_blockers"] = unique_texts(
        (
            resolved_supervisor.get("reporting_blockers", [])
            if isinstance(resolved_supervisor.get("reporting_blockers"), list)
            else []
        )
        + (
            record.get("reporting_blockers", [])
            if isinstance(record.get("reporting_blockers"), list)
            else []
        )
    )
    record["artifacts"] = resolved_runtime_control_freeze_artifacts(
        record,
        controller_snapshot=resolved_controller,
        gate_snapshot=resolved_gate,
        supervisor_snapshot=resolved_supervisor,
        artifact_paths=artifact_paths,
    )
    return validate_canonical_payload("runtime-control-freeze", record)


def store_runtime_control_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            existing_record = fetch_runtime_control_freeze(
                connection,
                run_id=run_id,
                round_id=round_id,
            )
            freeze_record = merged_runtime_control_freeze_record(
                run_id=run_id,
                round_id=round_id,
                existing_record=existing_record,
                controller_snapshot=controller_snapshot,
                gate_snapshot=gate_snapshot,
                supervisor_snapshot=supervisor_snapshot,
                artifact_paths=artifact_paths,
            )
            write_report_basis_freeze_row(
                connection,
                runtime_control_freeze_row_from_payload(freeze_record),
            )
            normalized_controller = (
                freeze_record.get("controller_snapshot", {})
                if isinstance(freeze_record.get("controller_snapshot"), dict)
                else {}
            )
            normalized_gate = (
                freeze_record.get("gate_snapshot", {})
                if isinstance(freeze_record.get("gate_snapshot"), dict)
                else {}
            )
            normalized_supervisor = (
                freeze_record.get("supervisor_snapshot", {})
                if isinstance(freeze_record.get("supervisor_snapshot"), dict)
                else {}
            )
            if normalized_controller:
                write_controller_snapshot_row(
                    connection,
                    controller_snapshot_row_from_payload(normalized_controller),
                )
            if normalized_gate:
                write_gate_snapshot_row(
                    connection,
                    gate_snapshot_row_from_payload(normalized_gate),
                )
            if normalized_supervisor:
                write_supervisor_snapshot_row(
                    connection,
                    supervisor_snapshot_row_from_payload(normalized_supervisor),
                )
    finally:
        connection.close()
    return freeze_record


def fetch_moderator_action_records(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> list[dict[str, Any]]:
    return fetch_json_rows(
        connection,
        table_name="moderator_actions",
        id_column="action_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
        extra_order_by="action_rank",
    )


def fetch_falsification_probe_records(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> list[dict[str, Any]]:
    return fetch_json_rows(
        connection,
        table_name="falsification_probes",
        id_column="probe_id",
        timestamp_column="opened_at_utc",
        run_id=run_id,
        round_id=round_id,
    )


def fetch_round_readiness_assessment(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row(
        connection,
        table_name="round_readiness_assessments",
        id_column="readiness_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
    )


def fetch_report_basis_freeze_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row(
        connection,
        table_name="report_basis_freeze_records",
        id_column="basis_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
    )


def fetch_report_basis_freeze_items(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> list[dict[str, Any]]:
    return fetch_json_rows(
        connection,
        table_name="report_basis_freeze_items",
        id_column="item_row_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
        extra_order_by="item_group, item_index",
    )


def fetch_reporting_handoff_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="reporting_handoffs",
        id_column="handoff_id",
        timestamp_column="generated_at_utc",
        filters={"run_id": run_id, "round_id": round_id},
    )


def fetch_council_decision_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
    decision_stage: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="council_decision_records",
        id_column="record_id",
        timestamp_column="generated_at_utc",
        filters={
            "run_id": run_id,
            "round_id": round_id,
            "decision_stage": decision_stage,
        },
    )


def fetch_expert_report_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
    report_stage: str = "",
    agent_role: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="expert_report_records",
        id_column="record_id",
        timestamp_column="generated_at_utc",
        filters={
            "run_id": run_id,
            "round_id": round_id,
            "report_stage": report_stage,
            "agent_role": agent_role,
        },
    )


def fetch_final_publication_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="final_publications",
        id_column="publication_id",
        timestamp_column="generated_at_utc",
        filters={"run_id": run_id, "round_id": round_id},
    )


def store_moderator_action_records(
    run_dir: str | Path,
    *,
    action_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(action_snapshot) if isinstance(action_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    generated_at_utc = maybe_text(snapshot_payload.get("generated_at_utc")) or utc_now_iso()
    ranked_actions = (
        snapshot_payload.get("ranked_actions", [])
        if isinstance(snapshot_payload.get("ranked_actions"), list)
        else []
    )
    normalized_actions = [
        normalized_action_payload(
            action,
            run_id=run_id,
            round_id=round_id,
            action_rank=index,
            generated_at_utc=generated_at_utc,
            source_skill=maybe_text(snapshot_payload.get("skill")),
            artifact_path=artifact_path,
        )
        for index, action in enumerate(ranked_actions)
        if isinstance(action, dict)
    ]
    snapshot_payload["generated_at_utc"] = generated_at_utc
    snapshot_payload["ranked_actions"] = normalized_actions
    snapshot_payload["action_count"] = len(normalized_actions)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM moderator_actions WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, action in enumerate(normalized_actions):
                write_moderator_action_row(
                    connection,
                    moderator_action_row_from_payload(
                        action,
                        generated_at_utc=generated_at_utc,
                        action_rank=index,
                        artifact_path=artifact_path,
                        record_locator=f"$.ranked_actions[{index}]",
                    ),
                )
    finally:
        connection.close()
    return snapshot_payload


def store_moderator_action_snapshot(
    run_dir: str | Path,
    *,
    action_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(action_snapshot) if isinstance(action_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    snapshot_payload["snapshot_id"] = (
        maybe_text(snapshot_payload.get("snapshot_id"))
        or moderator_action_snapshot_id(run_id, round_id)
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_moderator_action_snapshot_row(
                connection,
                moderator_action_snapshot_row_from_payload(
                    snapshot_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return snapshot_payload


def load_moderator_action_records(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_moderator_action_records(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_moderator_action_snapshot(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_snapshot_payload(
            connection,
            table_name="moderator_action_snapshots",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def store_falsification_probe_records(
    run_dir: str | Path,
    *,
    probe_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(probe_snapshot) if isinstance(probe_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    probes = (
        snapshot_payload.get("probes", [])
        if isinstance(snapshot_payload.get("probes"), list)
        else []
    )
    normalized_probes = [
        normalized_probe_payload(
            probe,
            run_id=run_id,
            round_id=round_id,
            probe_index=index,
            generated_at_utc=maybe_text(snapshot_payload.get("generated_at_utc")),
            source_skill=maybe_text(snapshot_payload.get("skill")),
            artifact_path=artifact_path,
        )
        for index, probe in enumerate(probes)
        if isinstance(probe, dict)
    ]
    snapshot_payload["generated_at_utc"] = (
        maybe_text(snapshot_payload.get("generated_at_utc"))
        or utc_now_iso()
    )
    snapshot_payload["probes"] = normalized_probes
    snapshot_payload["probe_count"] = len(normalized_probes)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM falsification_probes WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, probe in enumerate(normalized_probes):
                write_falsification_probe_row(
                    connection,
                    falsification_probe_row_from_payload(
                        probe,
                        artifact_path=artifact_path,
                        record_locator=f"$.probes[{index}]",
                    ),
                )
    finally:
        connection.close()
    return snapshot_payload


def store_falsification_probe_snapshot(
    run_dir: str | Path,
    *,
    probe_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(probe_snapshot) if isinstance(probe_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    snapshot_payload["snapshot_id"] = (
        maybe_text(snapshot_payload.get("snapshot_id"))
        or falsification_probe_snapshot_id(run_id, round_id)
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_falsification_probe_snapshot_row(
                connection,
                falsification_probe_snapshot_row_from_payload(
                    snapshot_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return snapshot_payload


def load_falsification_probe_records(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_falsification_probe_records(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_falsification_probe_snapshot(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_snapshot_payload(
            connection,
            table_name="falsification_probe_snapshots",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def store_round_readiness_assessment(
    run_dir: str | Path,
    *,
    readiness_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_readiness_payload(
        readiness_payload if isinstance(readiness_payload, dict) else {},
        run_id=maybe_text(
            readiness_payload.get("run_id")
            if isinstance(readiness_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            readiness_payload.get("round_id")
            if isinstance(readiness_payload, dict)
            else ""
        ),
        source_skill=maybe_text(
            readiness_payload.get("skill")
            if isinstance(readiness_payload, dict)
            else ""
        ),
        artifact_path=artifact_path,
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_round_readiness_assessment_row(
                connection,
                round_readiness_assessment_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload


def load_round_readiness_assessment(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_round_readiness_assessment(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def store_report_basis_freeze_record(
    run_dir: str | Path,
    *,
    report_basis_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_report_basis_freeze_payload(
        report_basis_payload if isinstance(report_basis_payload, dict) else {},
        run_id=maybe_text(
            report_basis_payload.get("run_id")
            if isinstance(report_basis_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            report_basis_payload.get("round_id")
            if isinstance(report_basis_payload, dict)
            else ""
        ),
        source_skill=maybe_text(
            report_basis_payload.get("skill")
            if isinstance(report_basis_payload, dict)
            else ""
        ),
        artifact_path=artifact_path,
    )
    basis_id = maybe_text(normalized_payload.get("basis_id"))
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    generated_at_utc = maybe_text(normalized_payload.get("generated_at_utc"))
    item_rows = iter_report_basis_freeze_items(normalized_payload)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM report_basis_freeze_records WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            connection.execute(
                "DELETE FROM report_basis_freeze_items WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            write_report_basis_freeze_record_row(
                connection,
                report_basis_freeze_record_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
            for item_group, item_index, item in item_rows:
                write_report_basis_freeze_item_row(
                    connection,
                    report_basis_freeze_item_row_from_payload(
                        item,
                        basis_id=basis_id,
                        run_id=run_id,
                        round_id=round_id,
                        generated_at_utc=generated_at_utc,
                        item_group=item_group,
                        item_index=item_index,
                        artifact_path=artifact_path,
                        record_locator=f"$.frozen_basis.{item_group}[{item_index}]",
                    ),
                )
    finally:
        connection.close()
    return normalized_payload


def load_report_basis_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_report_basis_freeze_record(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_report_basis_freeze_items(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_report_basis_freeze_items(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def store_reporting_handoff_record(
    run_dir: str | Path,
    *,
    handoff_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_reporting_handoff_payload(
        handoff_payload if isinstance(handoff_payload, dict) else {},
        run_id=maybe_text(
            handoff_payload.get("run_id")
            if isinstance(handoff_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            handoff_payload.get("round_id")
            if isinstance(handoff_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM reporting_handoffs WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            write_reporting_handoff_row(
                connection,
                reporting_handoff_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload


def load_reporting_handoff_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_reporting_handoff_record(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def store_council_decision_record(
    run_dir: str | Path,
    *,
    decision_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_council_decision_payload(
        decision_payload if isinstance(decision_payload, dict) else {},
        run_id=maybe_text(
            decision_payload.get("run_id")
            if isinstance(decision_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            decision_payload.get("round_id")
            if isinstance(decision_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    decision_stage = maybe_text(normalized_payload.get("decision_stage"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                """
                DELETE FROM council_decision_records
                WHERE run_id = ? AND round_id = ? AND decision_stage = ?
                """,
                (run_id, round_id, decision_stage),
            )
            write_council_decision_record_row(
                connection,
                council_decision_record_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload


def load_council_decision_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    decision_stage: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_council_decision_record(
            connection,
            run_id=run_id,
            round_id=round_id,
            decision_stage=decision_stage,
        )
    finally:
        connection.close()


def store_expert_report_record(
    run_dir: str | Path,
    *,
    report_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_expert_report_payload(
        report_payload if isinstance(report_payload, dict) else {},
        run_id=maybe_text(
            report_payload.get("run_id")
            if isinstance(report_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            report_payload.get("round_id")
            if isinstance(report_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    report_stage = maybe_text(normalized_payload.get("report_stage"))
    agent_role = maybe_text(normalized_payload.get("agent_role"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                """
                DELETE FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (run_id, round_id, report_stage, agent_role),
            )
            write_expert_report_record_row(
                connection,
                expert_report_record_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload


def load_expert_report_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    report_stage: str = "",
    agent_role: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_expert_report_record(
            connection,
            run_id=run_id,
            round_id=round_id,
            report_stage=report_stage,
            agent_role=agent_role,
        )
    finally:
        connection.close()


def store_final_publication_record(
    run_dir: str | Path,
    *,
    publication_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_final_publication_payload(
        publication_payload if isinstance(publication_payload, dict) else {},
        run_id=maybe_text(
            publication_payload.get("run_id")
            if isinstance(publication_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            publication_payload.get("round_id")
            if isinstance(publication_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM final_publications WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            write_final_publication_row(
                connection,
                final_publication_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload


def load_final_publication_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_final_publication_record(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def store_round_task_snapshot(
    run_dir: str | Path,
    *,
    task_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(task_snapshot) if isinstance(task_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    snapshot_payload["snapshot_id"] = (
        maybe_text(snapshot_payload.get("snapshot_id"))
        or round_task_snapshot_id(run_id, round_id)
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_round_task_snapshot_row(
                connection,
                round_task_snapshot_row_from_payload(
                    snapshot_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return snapshot_payload


def load_round_task_snapshot(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_snapshot_payload(
            connection,
            table_name="round_task_snapshots",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_orchestration_plan_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    artifact_path: str = "",
    controller_authority: str = "",
    allow_latest_fallback: bool = True,
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        normalized_run_id = maybe_text(run_id)
        normalized_round_id = maybe_text(round_id)
        if not normalized_run_id and not normalized_round_id:
            return None
        normalized_artifact_path = maybe_text(artifact_path)
        normalized_controller_authority = maybe_text(controller_authority)
        if normalized_artifact_path:
            record = latest_json_row_where(
                connection,
                table_name="orchestration_plans",
                id_column="plan_id",
                timestamp_column="generated_at_utc",
                filters={
                    "run_id": normalized_run_id,
                    "round_id": normalized_round_id,
                    "artifact_path": normalized_artifact_path,
                },
            )
            if record is not None:
                return record
        if normalized_controller_authority:
            record = latest_json_row_where(
                connection,
                table_name="orchestration_plans",
                id_column="plan_id",
                timestamp_column="generated_at_utc",
                filters={
                    "run_id": normalized_run_id,
                    "round_id": normalized_round_id,
                    "controller_authority": normalized_controller_authority,
                },
            )
            if record is not None:
                return record
        if not allow_latest_fallback:
            return None
        return latest_json_row_where(
            connection,
            table_name="orchestration_plans",
            id_column="plan_id",
            timestamp_column="generated_at_utc",
            filters={
                "run_id": normalized_run_id,
                "round_id": normalized_round_id,
            },
        )
    finally:
        connection.close()


def load_orchestration_plan_steps(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    plan_id: str = "",
    plan_step_group: str = "",
    stage_name: str = "",
    skill_name: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        clauses: list[str] = []
        params: list[str] = []
        for column_name, value in (
            ("run_id", run_id),
            ("round_id", round_id),
            ("plan_id", plan_id),
            ("plan_step_group", plan_step_group),
            ("stage_name", stage_name),
            ("skill_name", skill_name),
        ):
            text = maybe_text(value)
            if not text:
                continue
            clauses.append(f"{column_name} = ?")
            params.append(text)
        if not clauses:
            return []
        rows = connection.execute(
            f"""
            SELECT *
            FROM orchestration_plan_steps
            WHERE {' AND '.join(clauses)}
            ORDER BY generated_at_utc DESC, plan_step_group, step_index, step_id
            """,
            tuple(params),
        ).fetchall()
    finally:
        connection.close()
    return [payload_from_db_row(row) for row in rows]


def load_moderator_work_surface(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    next_action_rows = load_moderator_action_records(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    next_actions_snapshot = load_moderator_action_snapshot(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    next_actions = build_moderator_action_payload(
        next_action_rows,
        snapshot_payload=next_actions_snapshot,
        run_id=run_id,
        round_id=round_id,
    ) if next_action_rows or isinstance(next_actions_snapshot, dict) else {}
    probe_rows = load_falsification_probe_records(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    probes_snapshot = load_falsification_probe_snapshot(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    probes = build_falsification_probe_payload(
        probe_rows,
        snapshot_payload=probes_snapshot,
        run_id=run_id,
        round_id=round_id,
    ) if probe_rows or isinstance(probes_snapshot, dict) else {}
    round_tasks = load_round_task_snapshot(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    return {
        "next_actions": next_actions,
        "probes": probes,
        "round_tasks": round_tasks,
    }


def load_runtime_control_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_runtime_control_freeze(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_controller_snapshot_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return latest_json_row(
            connection,
            table_name="controller_snapshots",
            id_column="snapshot_id",
            timestamp_column="generated_at_utc",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_gate_snapshot_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    stage_name: str = "",
    gate_handler: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    filters = {
        "run_id": run_id,
        "round_id": round_id,
        "stage_name": stage_name,
        "gate_handler": gate_handler,
    }
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        if maybe_text(stage_name) or maybe_text(gate_handler):
            return latest_json_row_where(
                connection,
                table_name="gate_snapshots",
                id_column="snapshot_id",
                timestamp_column="generated_at_utc",
                filters=filters,
            )
        return latest_json_row(
            connection,
            table_name="gate_snapshots",
            id_column="snapshot_id",
            timestamp_column="generated_at_utc",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_supervisor_snapshot_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return latest_json_row(
            connection,
            table_name="supervisor_snapshots",
            id_column="snapshot_id",
            timestamp_column="generated_at_utc",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_phase2_control_state(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    runtime_control_freeze_record = load_runtime_control_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    readiness_record = load_round_readiness_assessment(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    report_basis_freeze_record = load_report_basis_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    reporting_handoff_record = load_reporting_handoff_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    decision_draft_record = load_council_decision_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
        db_path=db_path,
    ) or {}
    decision_record = load_council_decision_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="canonical",
        db_path=db_path,
    ) or {}
    expert_report_drafts = {
        role: (
            load_expert_report_record(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                report_stage="draft",
                agent_role=role,
                db_path=db_path,
            )
            or {}
        )
        for role in REPORT_AGENT_ROLES
    }
    expert_reports = {
        role: (
            load_expert_report_record(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                report_stage="canonical",
                agent_role=role,
                db_path=db_path,
            )
            or {}
        )
        for role in REPORT_AGENT_ROLES
    }
    final_publication_record = load_final_publication_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    controller_record = load_controller_snapshot_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    gate_record = load_gate_snapshot_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    supervisor_record = load_supervisor_snapshot_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    orchestration_plan_record = load_orchestration_plan_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_path=(
            maybe_text(controller_record.get("planning", {}).get("plan_path"))
            if isinstance(controller_record.get("planning"), dict)
            else ""
        ),
        db_path=db_path,
    ) or {}
    orchestration_plan_steps = (
        load_orchestration_plan_steps(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            plan_id=maybe_text(orchestration_plan_record.get("plan_id")),
            db_path=db_path,
        )
        if orchestration_plan_record
        else []
    )
    resolved_gate_record = gate_record or (
        runtime_control_freeze_record.get("gate_snapshot", {})
        if isinstance(runtime_control_freeze_record.get("gate_snapshot"), dict)
        else {}
    )
    return {
        "orchestration_plan": orchestration_plan_record,
        "orchestration_plan_steps": orchestration_plan_steps,
        "runtime_control_freeze": runtime_control_freeze_record,
        "round_readiness": readiness_record,
        "report_basis_freeze": report_basis_freeze_record,
        "reporting_handoff": reporting_handoff_record,
        "decision_draft": decision_draft_record,
        "decision": decision_record,
        "expert_report_drafts": expert_report_drafts,
        "expert_reports": expert_reports,
        "final_publication": final_publication_record,
        "controller": controller_record
        or (
            runtime_control_freeze_record.get("controller_snapshot", {})
            if isinstance(runtime_control_freeze_record.get("controller_snapshot"), dict)
            else {}
        ),
        "report_basis_gate": resolved_gate_record,
        "report_basis_gate": resolved_gate_record,
        "supervisor": supervisor_record
        or (
            runtime_control_freeze_record.get("supervisor_snapshot", {})
            if isinstance(runtime_control_freeze_record.get("supervisor_snapshot"), dict)
            else {}
        ),
    }


def iter_round_transition_rows(run_dir: Path, *, run_id: str) -> list[dict[str, Any]]:
    runtime_dir = run_dir / "runtime"
    if not runtime_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for file_path in sorted(runtime_dir.glob("round_transition_*.json")):
        payload = load_json_if_exists(file_path)
        if not isinstance(payload, dict):
            continue
        payload_run_id = maybe_text(payload.get("run_id"))
        if run_id and payload_run_id and payload_run_id != run_id:
            continue
        transition_id = maybe_text(payload.get("transition_id"))
        round_id = maybe_text(payload.get("round_id"))
        if not transition_id or not round_id:
            continue
        rows.append(
            round_transition_row_from_payload(
                payload,
                board_revision=coerce_int(payload.get("board_revision")),
                artifact_path=str(file_path.resolve()),
                record_locator="$",
            )
        )
    return rows


def sync_board_to_deliberation_plane(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    board_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    db_file = resolve_db_path(run_dir_path, db_path)
    board_payload = load_json_if_exists(board_file)
    if not isinstance(board_payload, dict):
        return {
            "status": "missing-board",
            "run_id": maybe_text(expected_run_id),
            "board_path": str(board_file),
            "db_path": str(db_file),
            "board_revision": 0,
            "event_count": 0,
            "round_count": 0,
            "note_count": 0,
            "hypothesis_count": 0,
            "challenge_ticket_count": 0,
            "task_count": 0,
            "round_transition_count": 0,
        }

    run_id = maybe_text(board_payload.get("run_id")) or maybe_text(expected_run_id)
    if maybe_text(expected_run_id) and run_id and run_id != maybe_text(expected_run_id):
        raise ValueError(
            f"Board run_id mismatch: board has {run_id!r} but expected {maybe_text(expected_run_id)!r}."
        )
    if not run_id:
        raise ValueError(f"Board artifact is missing run_id: {board_file}")

    board_revision = coerce_int(board_payload.get("board_revision"))
    updated_at_utc = maybe_text(board_payload.get("updated_at_utc"))
    rounds = board_payload.get("rounds", {}) if isinstance(board_payload.get("rounds"), dict) else {}
    events = board_payload.get("events", []) if isinstance(board_payload.get("events"), list) else []

    event_rows: list[dict[str, Any]] = []
    note_rows: list[dict[str, Any]] = []
    hypothesis_rows: list[dict[str, Any]] = []
    challenge_rows: list[dict[str, Any]] = []
    task_rows: list[dict[str, Any]] = []

    for index, event in enumerate(events):
        if not isinstance(event, dict):
            continue
        event_id = maybe_text(event.get("event_id"))
        round_id = maybe_text(event.get("round_id"))
        if not event_id or not round_id:
            continue
        resolved_event = {
            **event,
            "event_id": event_id,
            "run_id": maybe_text(event.get("run_id")) or run_id,
            "round_id": round_id,
        }
        event_rows.append(
            event_row_from_payload(
                resolved_event,
                event_index=index,
                board_revision=board_revision,
                board_path=board_file,
                record_locator=f"$.events[{index}]",
            )
        )

    for round_id, round_state in rounds.items():
        if not isinstance(round_state, dict):
            continue
        notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
        hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
        challenges = (
            round_state.get("challenge_tickets")
            if isinstance(round_state.get("challenge_tickets"), list)
            else []
        )
        tasks = round_state.get("tasks", []) if isinstance(round_state.get("tasks"), list) else []

        for index, note in enumerate(notes):
            if not isinstance(note, dict):
                continue
            note_id = maybe_text(note.get("note_id"))
            if not note_id:
                continue
            resolved_note = {
                **note,
                "note_id": note_id,
                "run_id": maybe_text(note.get("run_id")) or run_id,
                "round_id": maybe_text(note.get("round_id")) or maybe_text(round_id),
            }
            note_rows.append(
                note_row_from_payload(
                    resolved_note,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.notes[{index}]",
                )
            )

        for index, hypothesis in enumerate(hypotheses):
            if not isinstance(hypothesis, dict):
                continue
            hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
            if not hypothesis_id:
                continue
            resolved_hypothesis = {
                **hypothesis,
                "hypothesis_id": hypothesis_id,
                "run_id": maybe_text(hypothesis.get("run_id")) or run_id,
                "round_id": maybe_text(hypothesis.get("round_id")) or maybe_text(round_id),
            }
            hypothesis_rows.append(
                hypothesis_row_from_payload(
                    resolved_hypothesis,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.hypotheses[{index}]",
                )
            )

        for index, ticket in enumerate(challenges):
            if not isinstance(ticket, dict):
                continue
            ticket_id = maybe_text(ticket.get("ticket_id"))
            if not ticket_id:
                continue
            resolved_ticket = {
                **ticket,
                "ticket_id": ticket_id,
                "run_id": maybe_text(ticket.get("run_id")) or run_id,
                "round_id": maybe_text(ticket.get("round_id")) or maybe_text(round_id),
            }
            challenge_rows.append(
                challenge_row_from_payload(
                    resolved_ticket,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.challenge_tickets[{index}]",
                )
            )

        for index, task in enumerate(tasks):
            if not isinstance(task, dict):
                continue
            task_id = maybe_text(task.get("task_id"))
            if not task_id:
                continue
            resolved_task = {
                **task,
                "task_id": task_id,
                "run_id": maybe_text(task.get("run_id")) or run_id,
                "round_id": maybe_text(task.get("round_id")) or maybe_text(round_id),
            }
            task_rows.append(
                board_task_row_from_payload(
                    resolved_task,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.tasks[{index}]",
                )
            )

    round_transition_rows = iter_round_transition_rows(run_dir_path, run_id=run_id)

    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            for table_name in (
                "board_events",
                "board_notes",
                "hypothesis_cards",
                "challenge_tickets",
                "board_tasks",
                "round_transitions",
            ):
                connection.execute(f"DELETE FROM {table_name} WHERE run_id = ?", (run_id,))

            for row in event_rows:
                write_board_event_row(connection, row)
            for row in note_rows:
                write_board_note_row(connection, row)
            for row in hypothesis_rows:
                write_hypothesis_row(connection, row)
            for row in challenge_rows:
                write_challenge_row(connection, row)
            for row in task_rows:
                write_board_task_row(connection, row)
            for row in round_transition_rows:
                write_round_transition_row(connection, row)

            upsert_board_run(
                connection,
                run_id=run_id,
                board_revision=board_revision,
                updated_at_utc=updated_at_utc,
                board_path=str(board_file),
            )
    finally:
        connection.close()

    return {
        "status": "completed",
        "sync_mode": "json-import",
        "run_id": run_id,
        "board_path": str(board_file),
        "db_path": str(db_file),
        "board_revision": board_revision,
        "event_count": len(event_rows),
        "round_count": len(
            [round_id for round_id, round_state in rounds.items() if isinstance(round_state, dict)]
        ),
        "note_count": len(note_rows),
        "hypothesis_count": len(hypothesis_rows),
        "challenge_ticket_count": len(challenge_rows),
        "task_count": len(task_rows),
        "round_transition_count": len(round_transition_rows),
    }


def bootstrap_board_state(
    run_dir: str | Path,
    *,
    expected_run_id: str,
    board_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        board_run = fetch_board_run(connection, run_id=expected_run_id)
    finally:
        connection.close()

    board_payload = load_json_if_exists(board_file)
    if isinstance(board_payload, dict):
        board_run_id = maybe_text(board_payload.get("run_id")) or maybe_text(expected_run_id)
        if maybe_text(expected_run_id) and board_run_id and board_run_id != maybe_text(expected_run_id):
            raise ValueError(
                f"Board run_id mismatch: board has {board_run_id!r} but expected {maybe_text(expected_run_id)!r}."
            )
        file_revision = coerce_int(board_payload.get("board_revision"))
        current_revision = coerce_int(board_run.get("board_revision")) if isinstance(board_run, dict) else -1
        if board_run is None or file_revision > current_revision:
            sync_summary = sync_board_to_deliberation_plane(
                run_dir_path,
                expected_run_id=expected_run_id,
                board_path=board_file,
                db_path=db_path,
            )
            sync_summary["sync_mode"] = "json-import"
            return sync_summary
        return {
            "status": "completed",
            "sync_mode": "db-current",
            "run_id": maybe_text(expected_run_id),
            "board_path": str(board_file),
            "db_path": str(db_file),
            "board_revision": current_revision,
        }

    if isinstance(board_run, dict):
        return {
            "status": "completed",
            "sync_mode": "db-only",
            "run_id": maybe_text(expected_run_id),
            "board_path": maybe_text(board_run.get("board_path")) or str(board_file),
            "db_path": str(db_file),
            "board_revision": coerce_int(board_run.get("board_revision")),
        }

    return {
        "status": "missing-board",
        "sync_mode": "missing-board",
        "run_id": maybe_text(expected_run_id),
        "board_path": str(board_file),
        "db_path": str(db_file),
        "board_revision": 0,
    }


def export_board_from_connection(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    board_path: Path,
) -> dict[str, Any]:
    board_run = fetch_board_run(connection, run_id=run_id) or {
        "run_id": run_id,
        "board_revision": infer_board_revision(connection, run_id=run_id),
        "updated_at_utc": "",
        "board_path": str(board_path),
    }
    board_revision = coerce_int(board_run.get("board_revision"))
    updated_at_utc = maybe_text(board_run.get("updated_at_utc"))
    rounds: dict[str, dict[str, list[dict[str, Any]]]] = {}

    event_rows = connection.execute(
        """
        SELECT event_id, raw_json
        FROM board_events
        WHERE run_id = ?
        ORDER BY event_index, event_id
        """,
        (run_id,),
    ).fetchall()
    note_rows = connection.execute(
        """
        SELECT note_id, round_id, raw_json
        FROM board_notes
        WHERE run_id = ?
        ORDER BY round_id, created_at_utc, note_id
        """,
        (run_id,),
    ).fetchall()
    hypothesis_rows = connection.execute(
        """
        SELECT hypothesis_id, round_id, raw_json
        FROM hypothesis_cards
        WHERE run_id = ?
        ORDER BY round_id, updated_at_utc, hypothesis_id
        """,
        (run_id,),
    ).fetchall()
    challenge_rows = connection.execute(
        """
        SELECT ticket_id, round_id, raw_json
        FROM challenge_tickets
        WHERE run_id = ?
        ORDER BY round_id, created_at_utc, ticket_id
        """,
        (run_id,),
    ).fetchall()
    task_rows = connection.execute(
        """
        SELECT task_id, round_id, raw_json
        FROM board_tasks
        WHERE run_id = ?
        ORDER BY round_id, updated_at_utc, task_id
        """,
        (run_id,),
    ).fetchall()

    event_locators: dict[str, str] = {}
    note_locators: dict[str, str] = {}
    hypothesis_locators: dict[str, str] = {}
    challenge_locators: dict[str, str] = {}
    task_locators: dict[str, str] = {}
    events: list[dict[str, Any]] = []

    for row in event_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        event_id = maybe_text(payload.get("event_id")) or maybe_text(row["event_id"])
        round_id = maybe_text(payload.get("round_id"))
        if not event_id or not round_id:
            continue
        resolved = {
            **payload,
            "event_id": event_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        events.append(resolved)
        event_locators[event_id] = f"$.events[{len(events) - 1}]"
        ensure_round_state(rounds, round_id)

    for row in note_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        note_id = maybe_text(payload.get("note_id")) or maybe_text(row["note_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not note_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "note_id": note_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["notes"].append(resolved)
        note_locators[note_id] = f"$.rounds.{round_id}.notes[{len(state['notes']) - 1}]"

    for row in hypothesis_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        hypothesis_id = maybe_text(payload.get("hypothesis_id")) or maybe_text(row["hypothesis_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not hypothesis_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "hypothesis_id": hypothesis_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["hypotheses"].append(resolved)
        hypothesis_locators[hypothesis_id] = (
            f"$.rounds.{round_id}.hypotheses[{len(state['hypotheses']) - 1}]"
        )

    for row in challenge_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        ticket_id = maybe_text(payload.get("ticket_id")) or maybe_text(row["ticket_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not ticket_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "ticket_id": ticket_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["challenge_tickets"].append(resolved)
        challenge_locators[ticket_id] = (
            f"$.rounds.{round_id}.challenge_tickets[{len(state['challenge_tickets']) - 1}]"
        )

    for row in task_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        task_id = maybe_text(payload.get("task_id")) or maybe_text(row["task_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not task_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "task_id": task_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["tasks"].append(resolved)
        task_locators[task_id] = f"$.rounds.{round_id}.tasks[{len(state['tasks']) - 1}]"

    if not updated_at_utc and events:
        updated_at_utc = maybe_text(events[-1].get("created_at_utc"))
    if not updated_at_utc:
        updated_at_utc = utc_now_iso()

    ordered_rounds = {
        round_id: ensure_round_state(rounds, round_id)
        for round_id in sorted(rounds)
    }
    board_payload = {
        "schema_version": "board-v1",
        "run_id": run_id,
        "board_revision": board_revision,
        "updated_at_utc": updated_at_utc,
        "events": events,
        "rounds": ordered_rounds,
    }

    upsert_board_run(
        connection,
        run_id=run_id,
        board_revision=board_revision,
        updated_at_utc=updated_at_utc,
        board_path=str(board_path),
    )
    for event_id, locator in event_locators.items():
        connection.execute(
            """
            UPDATE board_events
            SET artifact_path = ?, record_locator = ?
            WHERE event_id = ?
            """,
            (str(board_path), locator, event_id),
        )
    for note_id, locator in note_locators.items():
        connection.execute(
            """
            UPDATE board_notes
            SET artifact_path = ?, record_locator = ?
            WHERE note_id = ?
            """,
            (str(board_path), locator, note_id),
        )
    for hypothesis_id, locator in hypothesis_locators.items():
        connection.execute(
            """
            UPDATE hypothesis_cards
            SET artifact_path = ?, record_locator = ?
            WHERE hypothesis_id = ?
            """,
            (str(board_path), locator, hypothesis_id),
        )
    for ticket_id, locator in challenge_locators.items():
        connection.execute(
            """
            UPDATE challenge_tickets
            SET artifact_path = ?, record_locator = ?
            WHERE ticket_id = ?
            """,
            (str(board_path), locator, ticket_id),
        )
    for task_id, locator in task_locators.items():
        connection.execute(
            """
            UPDATE board_tasks
            SET artifact_path = ?, record_locator = ?
            WHERE task_id = ?
            """,
            (str(board_path), locator, task_id),
        )

    write_json_atomic(board_path, board_payload)
    return {
        "status": "completed",
        "run_id": run_id,
        "board_path": str(board_path),
        "board_revision": board_revision,
        "event_count": len(events),
        "round_count": len(ordered_rounds),
        "note_count": sum(len(state.get("notes", [])) for state in ordered_rounds.values()),
        "hypothesis_count": sum(len(state.get("hypotheses", [])) for state in ordered_rounds.values()),
        "challenge_ticket_count": sum(
            len(state.get("challenge_tickets", []))
            for state in ordered_rounds.values()
        ),
        "task_count": sum(len(state.get("tasks", [])) for state in ordered_rounds.values()),
        "record_locators": {
            "events": event_locators,
            "notes": note_locators,
            "hypotheses": hypothesis_locators,
            "challenge_tickets": challenge_locators,
            "tasks": task_locators,
        },
    }


def commit_board_mutation(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    board_path: str | Path = "",
    db_path: str = "",
    note_records: list[dict[str, Any]] | None = None,
    hypothesis_records: list[dict[str, Any]] | None = None,
    challenge_records: list[dict[str, Any]] | None = None,
    task_records: list[dict[str, Any]] | None = None,
    round_transition_records: list[dict[str, Any]] | None = None,
    event_type: str,
    event_payload: dict[str, Any],
    event_created_at_utc: str = "",
    event_discriminator: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    bootstrap_board_state(
        run_dir_path,
        expected_run_id=run_id,
        board_path=board_file,
        db_path=db_path,
    )
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            board_run = fetch_board_run(connection, run_id=run_id) or {
                "run_id": run_id,
                "board_revision": 0,
                "updated_at_utc": "",
                "board_path": str(board_file),
            }
            next_revision = coerce_int(board_run.get("board_revision")) + 1
            event_timestamp = maybe_text(event_created_at_utc) or utc_now_iso()
            event_index = next_event_index(connection, run_id=run_id)
            event_id = "boardevt-" + stable_hash(
                run_id,
                round_id,
                event_type,
                event_index,
                event_timestamp,
                event_discriminator,
            )[:12]
            event = {
                "event_id": event_id,
                "run_id": run_id,
                "round_id": round_id,
                "event_type": maybe_text(event_type),
                "created_at_utc": event_timestamp,
                "payload": event_payload,
            }
            for note in note_records or []:
                write_board_note_row(
                    connection,
                    note_row_from_payload(
                        note,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for hypothesis in hypothesis_records or []:
                write_hypothesis_row(
                    connection,
                    hypothesis_row_from_payload(
                        hypothesis,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for ticket in challenge_records or []:
                write_challenge_row(
                    connection,
                    challenge_row_from_payload(
                        ticket,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for task in task_records or []:
                write_board_task_row(
                    connection,
                    board_task_row_from_payload(
                        task,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for transition in round_transition_records or []:
                artifact_path = maybe_text(transition.get("artifact_path"))
                write_round_transition_row(
                    connection,
                    round_transition_row_from_payload(
                        transition,
                        board_revision=next_revision,
                        artifact_path=artifact_path,
                        record_locator=maybe_text(transition.get("record_locator")) or "$",
                    ),
                )
            write_board_event_row(
                connection,
                event_row_from_payload(
                    event,
                    event_index=event_index,
                    board_revision=next_revision,
                    board_path=board_file,
                    record_locator="",
                ),
            )
            upsert_board_run(
                connection,
                run_id=run_id,
                board_revision=next_revision,
                updated_at_utc=event_timestamp,
                board_path=str(board_file),
            )
            export_summary = export_board_from_connection(
                connection,
                run_id=run_id,
                board_path=board_file,
            )
    finally:
        connection.close()
    return {
        "status": "completed",
        "write_surface": "deliberation-plane",
        "run_id": run_id,
        "round_id": round_id,
        "board_path": str(board_file),
        "db_path": str(db_file),
        "board_revision": coerce_int(export_summary.get("board_revision")),
        "event_id": event_id,
        "event": event,
        "board_export": export_summary,
        "record_locators": export_summary.get("record_locators", {}),
    }


def store_round_transition_record(
    run_dir: str | Path,
    *,
    transition_record: dict[str, Any],
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_round_transition_row(
                connection,
                round_transition_row_from_payload(
                    transition_record,
                    board_revision=coerce_int(transition_record.get("board_revision")),
                    artifact_path=maybe_text(transition_record.get("artifact_path")),
                    record_locator=maybe_text(transition_record.get("record_locator"))
                    or "$",
                ),
            )
    finally:
        connection.close()
    return {
        "status": "completed",
        "run_id": maybe_text(transition_record.get("run_id")),
        "round_id": maybe_text(transition_record.get("round_id")),
        "transition_id": maybe_text(transition_record.get("transition_id")),
        "db_path": str(db_file),
        "board_revision": coerce_int(transition_record.get("board_revision")),
        "artifact_path": maybe_text(transition_record.get("artifact_path")),
        "record_locator": maybe_text(transition_record.get("record_locator")) or "$",
    }


def store_orchestration_plan_record(
    run_dir: str | Path,
    *,
    plan_payload: dict[str, Any],
    artifact_path: str = "",
    run_id: str = "",
    round_id: str = "",
    controller_authority: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    resolved_payload = dict(plan_payload)
    resolved_payload["run_id"] = (
        maybe_text(resolved_payload.get("run_id")) or maybe_text(run_id)
    )
    resolved_payload["round_id"] = (
        maybe_text(resolved_payload.get("round_id")) or maybe_text(round_id)
    )
    if not maybe_text(resolved_payload.get("controller_authority")):
        resolved_payload["controller_authority"] = maybe_text(controller_authority)
    row = orchestration_plan_row_from_payload(
        resolved_payload,
        artifact_path=artifact_path,
    )
    step_rows = iter_orchestration_plan_step_rows(
        resolved_payload,
        artifact_path=maybe_text(row.get("artifact_path")),
    )
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_orchestration_plan_row(connection, row)
            connection.execute(
                "DELETE FROM orchestration_plan_steps WHERE plan_id = ?",
                (maybe_text(row.get("plan_id")),),
            )
            for step_row in step_rows:
                write_orchestration_plan_step_row(connection, step_row)
    finally:
        connection.close()
    return {
        "status": "completed",
        "run_id": maybe_text(row.get("run_id")),
        "round_id": maybe_text(row.get("round_id")),
        "plan_id": maybe_text(row.get("plan_id")),
        "artifact_path": maybe_text(row.get("artifact_path")),
        "db_path": str(db_file),
        "planned_stage_count": coerce_int(row.get("planned_stage_count")),
        "step_row_count": len(step_rows),
    }


def fetch_round_events(connection: sqlite3.Connection, *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT event_id, run_id, round_id, event_type, created_at_utc, payload_json, event_index
        FROM board_events
        WHERE run_id = ? AND round_id = ?
        ORDER BY event_index, event_id
        """,
        (run_id, round_id),
    ).fetchall()
    return [
        {
            "event_id": maybe_text(row["event_id"]),
            "run_id": maybe_text(row["run_id"]),
            "round_id": maybe_text(row["round_id"]),
            "event_type": maybe_text(row["event_type"]),
            "created_at_utc": maybe_text(row["created_at_utc"]),
            "payload": decode_json(maybe_text(row["payload_json"]), {}),
        }
        for row in rows
    ]


def fetch_round_state(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    round_id: str,
    include_closed: bool,
) -> dict[str, Any]:
    note_rows = connection.execute(
        """
        SELECT note_id, created_at_utc, author_role, category, note_text,
               tags_json, linked_artifact_refs_json, related_ids_json
        FROM board_notes
        WHERE run_id = ? AND round_id = ?
        ORDER BY created_at_utc, note_id
        """,
        (run_id, round_id),
    ).fetchall()
    hypothesis_sql = """
        SELECT hypothesis_id, title, statement, status, owner_role, linked_claim_ids_json,
               decision_source, evidence_refs_json, source_ids_json, provenance_json, lineage_json,
               confidence, created_at_utc, updated_at_utc,
               carryover_from_round_id, carryover_from_hypothesis_id
        FROM hypothesis_cards
        WHERE run_id = ? AND round_id = ?
    """
    challenge_sql = """
        SELECT ticket_id, created_at_utc, status, priority, owner_role, title,
               challenge_statement, target_claim_id, target_hypothesis_id,
               decision_source, evidence_refs_json, source_ids_json, provenance_json, lineage_json,
               linked_artifact_refs_json, related_task_ids_json,
               closed_at_utc, closed_by_role, resolution, resolution_note
        FROM challenge_tickets
        WHERE run_id = ? AND round_id = ?
    """
    task_sql = """
        SELECT task_id, title, task_text, task_type, status, owner_role, priority,
               source_round_id, source_ticket_id, source_hypothesis_id,
               carryover_from_round_id, carryover_from_task_id,
               decision_source, evidence_refs_json, source_ids_json,
               provenance_json, lineage_json, linked_artifact_refs_json, related_ids_json,
               created_at_utc, updated_at_utc, claimed_at_utc
        FROM board_tasks
        WHERE run_id = ? AND round_id = ?
    """
    params: tuple[Any, ...] = (run_id, round_id)
    if not include_closed:
        hypothesis_sql += " AND status NOT IN ('closed', 'rejected')"
        challenge_sql += " AND status != 'closed'"
        task_sql += " AND status NOT IN ('completed', 'closed', 'cancelled')"
    hypothesis_sql += " ORDER BY updated_at_utc, hypothesis_id"
    challenge_sql += " ORDER BY created_at_utc, ticket_id"
    task_sql += " ORDER BY updated_at_utc, task_id"

    hypothesis_rows = connection.execute(hypothesis_sql, params).fetchall()
    challenge_rows = connection.execute(challenge_sql, params).fetchall()
    task_rows = connection.execute(task_sql, params).fetchall()

    return {
        "include_closed": include_closed,
        "note_count": len(note_rows),
        "hypothesis_count": len(hypothesis_rows),
        "challenge_ticket_count": len(challenge_rows),
        "task_count": len(task_rows),
        "notes": [
            {
                "note_id": maybe_text(row["note_id"]),
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "author_role": maybe_text(row["author_role"]),
                "category": maybe_text(row["category"]),
                "note_text": maybe_text(row["note_text"]),
                "tags": decode_json(maybe_text(row["tags_json"]), []),
                "linked_artifact_refs": decode_json(
                    maybe_text(row["linked_artifact_refs_json"]), []
                ),
                "related_ids": decode_json(maybe_text(row["related_ids_json"]), []),
            }
            for row in note_rows
        ],
        "hypotheses": [
            {
                "hypothesis_id": maybe_text(row["hypothesis_id"]),
                "title": maybe_text(row["title"]),
                "statement": maybe_text(row["statement"]),
                "status": maybe_text(row["status"]),
                "owner_role": maybe_text(row["owner_role"]),
                "linked_claim_ids": decode_json(
                    maybe_text(row["linked_claim_ids_json"]), []
                ),
                "decision_source": maybe_text(row["decision_source"]),
                "evidence_refs": decode_json(
                    maybe_text(row["evidence_refs_json"]), []
                ),
                "source_ids": decode_json(
                    maybe_text(row["source_ids_json"]), []
                ),
                "provenance": decode_json(
                    maybe_text(row["provenance_json"]), {}
                ),
                "lineage": decode_json(maybe_text(row["lineage_json"]), []),
                "confidence": row["confidence"],
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "updated_at_utc": maybe_text(row["updated_at_utc"]),
                "carryover_from_round_id": maybe_text(row["carryover_from_round_id"]),
                "carryover_from_hypothesis_id": maybe_text(
                    row["carryover_from_hypothesis_id"]
                ),
            }
            for row in hypothesis_rows
        ],
        "challenge_tickets": [
            {
                "ticket_id": maybe_text(row["ticket_id"]),
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "status": maybe_text(row["status"]),
                "priority": maybe_text(row["priority"]),
                "owner_role": maybe_text(row["owner_role"]),
                "title": maybe_text(row["title"]),
                "challenge_statement": maybe_text(row["challenge_statement"]),
                "target_claim_id": maybe_text(row["target_claim_id"]),
                "target_hypothesis_id": maybe_text(row["target_hypothesis_id"]),
                "decision_source": maybe_text(row["decision_source"]),
                "evidence_refs": decode_json(
                    maybe_text(row["evidence_refs_json"]), []
                ),
                "source_ids": decode_json(
                    maybe_text(row["source_ids_json"]), []
                ),
                "provenance": decode_json(
                    maybe_text(row["provenance_json"]), {}
                ),
                "lineage": decode_json(maybe_text(row["lineage_json"]), []),
                "linked_artifact_refs": decode_json(
                    maybe_text(row["linked_artifact_refs_json"]), []
                ),
                "related_task_ids": decode_json(
                    maybe_text(row["related_task_ids_json"]), []
                ),
                "closed_at_utc": maybe_text(row["closed_at_utc"]),
                "closed_by_role": maybe_text(row["closed_by_role"]),
                "resolution": maybe_text(row["resolution"]),
                "resolution_note": maybe_text(row["resolution_note"]),
            }
            for row in challenge_rows
        ],
        "tasks": [
            {
                "task_id": maybe_text(row["task_id"]),
                "title": maybe_text(row["title"]),
                "task_text": maybe_text(row["task_text"]),
                "task_type": maybe_text(row["task_type"]),
                "status": maybe_text(row["status"]),
                "owner_role": maybe_text(row["owner_role"]),
                "priority": maybe_text(row["priority"]),
                "source_round_id": maybe_text(row["source_round_id"]),
                "source_ticket_id": maybe_text(row["source_ticket_id"]),
                "source_hypothesis_id": maybe_text(row["source_hypothesis_id"]),
                "carryover_from_round_id": maybe_text(row["carryover_from_round_id"]),
                "carryover_from_task_id": maybe_text(row["carryover_from_task_id"]),
                "decision_source": maybe_text(row["decision_source"]),
                "evidence_refs": decode_json(
                    maybe_text(row["evidence_refs_json"]), []
                ),
                "source_ids": decode_json(
                    maybe_text(row["source_ids_json"]), []
                ),
                "provenance": decode_json(
                    maybe_text(row["provenance_json"]), {}
                ),
                "lineage": decode_json(maybe_text(row["lineage_json"]), []),
                "linked_artifact_refs": decode_json(
                    maybe_text(row["linked_artifact_refs_json"]), []
                ),
                "related_ids": decode_json(maybe_text(row["related_ids_json"]), []),
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "updated_at_utc": maybe_text(row["updated_at_utc"]),
                "claimed_at_utc": maybe_text(row["claimed_at_utc"]),
            }
            for row in task_rows
        ],
    }


def load_round_snapshot(
    run_dir: str | Path,
    *,
    expected_run_id: str,
    round_id: str,
    board_path: str | Path = "",
    include_closed: bool = True,
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    sync_summary = bootstrap_board_state(
        run_dir_path,
        expected_run_id=expected_run_id,
        board_path=board_file,
        db_path=db_path,
    )
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        board_run = fetch_board_run(connection, run_id=expected_run_id)
        if maybe_text(sync_summary.get("status")) != "completed" and board_run is None:
            return {
                "status": "missing-board",
                "run_id": maybe_text(expected_run_id),
                "round_id": maybe_text(round_id),
                "board_path": str(board_file),
                "db_path": maybe_text(sync_summary.get("db_path")) or str(db_file),
                "state_source": "missing-board",
                "round_events": [],
                "round_state": {
                    "include_closed": bool(include_closed),
                    "note_count": 0,
                    "hypothesis_count": 0,
                    "challenge_ticket_count": 0,
                    "task_count": 0,
                    "notes": [],
                    "hypotheses": [],
                    "challenge_tickets": [],
                    "tasks": [],
                },
                "deliberation_sync": sync_summary,
            }
        round_events = fetch_round_events(
            connection,
            run_id=expected_run_id,
            round_id=round_id,
        )
        round_state = fetch_round_state(
            connection,
            run_id=expected_run_id,
            round_id=round_id,
            include_closed=include_closed,
        )
    finally:
        connection.close()
    return {
        "status": "completed",
        "run_id": maybe_text(expected_run_id),
        "round_id": maybe_text(round_id),
        "board_path": str(board_file),
        "db_path": str(db_file),
        "state_source": "deliberation-plane",
        "round_events": round_events,
        "round_state": round_state,
        "deliberation_sync": sync_summary,
    }


__all__ = [
    "build_falsification_probe_payload",
    "build_moderator_action_payload",
    "bootstrap_board_state",
    "commit_board_mutation",
    "connect_db",
    "decode_json",
    "default_db_path",
    "fetch_round_events",
    "fetch_round_state",
    "load_council_decision_record",
    "load_controller_snapshot_record",
    "load_expert_report_record",
    "load_falsification_probe_records",
    "load_falsification_probe_snapshot",
    "load_final_publication_record",
    "load_gate_snapshot_record",
    "load_moderator_action_records",
    "load_moderator_action_snapshot",
    "load_orchestration_plan_record",
    "load_orchestration_plan_steps",
    "load_phase2_control_state",
    "load_report_basis_freeze_items",
    "load_report_basis_freeze_record",
    "load_runtime_control_freeze_record",
    "load_raw_board_record",
    "load_reporting_handoff_record",
    "load_round_readiness_assessment",
    "load_round_snapshot",
    "load_supervisor_snapshot_record",
    "maybe_text",
    "resolve_board_path",
    "resolve_db_path",
    "resolve_run_dir",
    "store_council_decision_record",
    "store_expert_report_record",
    "store_falsification_probe_records",
    "store_falsification_probe_snapshot",
    "store_final_publication_record",
    "store_moderator_action_records",
    "store_moderator_action_snapshot",
    "store_orchestration_plan_record",
    "store_report_basis_freeze_record",
    "store_runtime_control_freeze_record",
    "store_reporting_handoff_record",
    "store_round_transition_record",
    "store_round_readiness_assessment",
    "sync_board_to_deliberation_plane",
]
