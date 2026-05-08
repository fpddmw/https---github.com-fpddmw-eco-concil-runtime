from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane_rows import maybe_text
from eco_council_runtime.kernel.core.schema_migrations import (
    apply_schema_migration,
    ensure_schema_migration_tables,
    load_schema_status as load_connection_schema_status,
    set_schema_version,
)

DELIBERATION_SCHEMA_NAME = "deliberation-plane"
DELIBERATION_SCHEMA_VERSION = "2026.05.06.1"

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
    governed_execution_posture TEXT NOT NULL DEFAULT '',
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
    social_investigator_report_source TEXT NOT NULL DEFAULT '',
    environmental_investigator_report_source TEXT NOT NULL DEFAULT '',
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
    social_investigator_report_source TEXT NOT NULL DEFAULT '',
    environmental_investigator_report_source TEXT NOT NULL DEFAULT '',
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
    ensure_schema_migration_tables(connection)
    preflight_existing_tables_for_schema_sql(connection)
    connection.executescript(SCHEMA_SQL)
    ensure_schema_migrations(connection)
    connection.commit()
    return connection, file_path


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_existing_table_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    if table_exists(connection, table_name):
        ensure_column(connection, table_name, column_name, column_sql)


def preflight_existing_tables_for_schema_sql(connection: sqlite3.Connection) -> None:
    ensure_existing_table_column(
        connection,
        "board_events",
        "event_index",
        "INTEGER NOT NULL DEFAULT 0",
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
            ensure_existing_table_column(
                connection,
                table_name,
                column_name,
                "TEXT NOT NULL DEFAULT ''",
            )
    for column_name, column_sql in (
        ("reporting_ready", "INTEGER NOT NULL DEFAULT 0"),
        ("reporting_handoff_status", "TEXT NOT NULL DEFAULT ''"),
        ("reporting_blockers_json", "TEXT NOT NULL DEFAULT '[]'"),
    ):
        ensure_existing_table_column(
            connection,
            "report_basis_freezes",
            column_name,
            column_sql,
        )


def ensure_schema_migrations(connection: sqlite3.Connection) -> None:
    ensure_schema_migration_tables(connection)
    apply_schema_migration(
        connection,
        schema_name=DELIBERATION_SCHEMA_NAME,
        migration_id="0001-deliberation-schema-baseline",
        target_version=DELIBERATION_SCHEMA_VERSION,
        description="Record the current deliberation-plane schema baseline.",
        operation=lambda: None,
    )
    apply_schema_migration(
        connection,
        schema_name=DELIBERATION_SCHEMA_NAME,
        migration_id="0002-deliberation-legacy-columns-and-indexes",
        target_version=DELIBERATION_SCHEMA_VERSION,
        description="Backfill legacy deliberation columns and indexes used by runtime governance, reporting, and governed-execution control surfaces.",
        operation=lambda: apply_deliberation_legacy_schema_migrations(connection),
    )
    set_schema_version(
        connection,
        schema_name=DELIBERATION_SCHEMA_NAME,
        current_version=DELIBERATION_SCHEMA_VERSION,
    )


def apply_deliberation_legacy_schema_migrations(connection: sqlite3.Connection) -> None:
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


def load_schema_status(
    run_dir: str | Path,
    *,
    db_path: str = "",
) -> dict[str, Any]:
    connection, db_file = connect_db(resolve_run_dir(run_dir), db_path)
    try:
        payload = load_connection_schema_status(connection)
    finally:
        connection.close()
    return {
        **payload,
        "db_path": str(db_file),
    }


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


__all__ = [
    "DELIBERATION_SCHEMA_NAME",
    "DELIBERATION_SCHEMA_VERSION",
    "SCHEMA_SQL",
    "apply_deliberation_legacy_schema_migrations",
    "connect_db",
    "default_db_path",
    "ensure_column",
    "ensure_existing_table_column",
    "ensure_schema_migrations",
    "load_schema_status",
    "preflight_existing_tables_for_schema_sql",
    "resolve_db_path",
    "resolve_run_dir",
    "table_exists",
]
