from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import load_json_if_exists
from eco_council_runtime.kernel.core.paths import ensure_runtime_dirs, manifest_path, operator_runbook_path
from eco_council_runtime.kernel.operator.admission_policy import load_admission_policy
from eco_council_runtime.kernel.operator.dead_letters import operator_resolution_steps
from eco_council_runtime.kernel.operator.operations_common import RUNBOOK_SECTIONS, maybe_text
from eco_council_runtime.kernel.operator.runtime_health import runtime_health_payload

def operator_runbook_markdown(run_dir: Path, *, round_id: str = "") -> str:
    from eco_council_runtime.runtime_command_hints import kernel_command, run_skill_command
    from eco_council_runtime.kernel.governance.skill_approvals import (
        REQUEST_STATUS_APPROVED as SKILL_REQUEST_STATUS_APPROVED,
        REQUEST_STATUS_CONSUMED as SKILL_REQUEST_STATUS_CONSUMED,
        REQUEST_STATUS_PENDING as SKILL_REQUEST_STATUS_PENDING,
        REQUEST_STATUS_REJECTED as SKILL_REQUEST_STATUS_REJECTED,
        load_skill_approval_requests,
    )
    from eco_council_runtime.kernel.governance.transition_requests import (
        REQUEST_STATUS_APPROVED,
        REQUEST_STATUS_COMMITTED,
        REQUEST_STATUS_PENDING,
        REQUEST_STATUS_REJECTED,
        TRANSITION_KIND_CLOSE_ROUND,
        TRANSITION_KIND_FREEZE_REPORT_BASIS,
        TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
        TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
        load_transition_requests,
    )

    policy = load_admission_policy(run_dir)
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    health = runtime_health_payload(run_dir, round_id=round_id)
    runtime_lock = (
        health.get("runtime_lock", {})
        if isinstance(health.get("runtime_lock"), dict)
        else {}
    )
    dead_letters = health.get("open_dead_letters", []) if isinstance(health.get("open_dead_letters"), list) else []
    rollback_policy = policy.get("rollback_policy", {}) if isinstance(policy.get("rollback_policy"), dict) else {}
    run_id = maybe_text(manifest.get("run_id"))
    display_run_id = run_id or "<run_id>"
    display_round_id = maybe_text(round_id) or "<round_id>"
    transition_requests = (
        load_transition_requests(run_dir, run_id=run_id, round_id=round_id, limit=20)
        if run_id and round_id
        else []
    )
    skill_approval_requests = (
        load_skill_approval_requests(run_dir, run_id=run_id, round_id=round_id, limit=20)
        if run_id and round_id
        else []
    )
    transition_counts = {
        REQUEST_STATUS_PENDING: 0,
        REQUEST_STATUS_APPROVED: 0,
        REQUEST_STATUS_REJECTED: 0,
        REQUEST_STATUS_COMMITTED: 0,
    }
    for request in transition_requests:
        if not isinstance(request, dict):
            continue
        status = maybe_text(request.get("request_status"))
        if status in transition_counts:
            transition_counts[status] += 1
    skill_approval_counts = {
        SKILL_REQUEST_STATUS_PENDING: 0,
        SKILL_REQUEST_STATUS_APPROVED: 0,
        SKILL_REQUEST_STATUS_REJECTED: 0,
        SKILL_REQUEST_STATUS_CONSUMED: 0,
    }
    for request in skill_approval_requests:
        if not isinstance(request, dict):
            continue
        status = maybe_text(request.get("request_status"))
        if status in skill_approval_counts:
            skill_approval_counts[status] += 1
    lines = [
        "# Runtime Operator Runbook",
        "",
        "## Control Plane",
        "",
        f"- Permission profile: `{maybe_text(policy.get('permission_profile')) or 'standard'}`",
        f"- Approval authority: `{maybe_text(policy.get('approval_authority')) or 'runtime-operator'}`",
        f"- Rollback mode: `{maybe_text(rollback_policy.get('mode')) or 'operator-mediated'}`",
        f"- Alert status: `{maybe_text(health.get('alert_status')) or 'green'}`",
        f"- Failed events: `{int(health.get('summary', {}).get('failed_event_count') or 0)}`",
        f"- Blocked events: `{int(health.get('summary', {}).get('blocked_event_count') or 0)}`",
        f"- Receipt conflicts: `{int(health.get('summary', {}).get('receipt_conflict_count') or 0)}`",
        f"- Open dead letters: `{int(health.get('summary', {}).get('open_dead_letter_count') or 0)}`",
        f"- Runtime lock: `{maybe_text(runtime_lock.get('lock_state')) or 'not-created'}`",
        f"- Runtime lock state path: `{maybe_text(runtime_lock.get('lock_state_path'))}`",
        f"- Pending transition requests: `{transition_counts[REQUEST_STATUS_PENDING]}`",
        f"- Approved transition requests: `{transition_counts[REQUEST_STATUS_APPROVED]}`",
        f"- Pending skill approval requests: `{skill_approval_counts[SKILL_REQUEST_STATUS_PENDING]}`",
        f"- Approved skill approval requests: `{skill_approval_counts[SKILL_REQUEST_STATUS_APPROVED]}`",
        "",
        "## Standard Commands",
        "",
        f"- Inspect runtime state: `{kernel_command('show-run-state', '--run-dir', str(run_dir), *(['--round-id', round_id] if round_id else []), '--tail', '20')}`",
        f"- Refresh health surface: `{kernel_command('materialize-runtime-health', '--run-dir', str(run_dir), *(['--round-id', round_id] if round_id else []))}`",
        f"- Rebuild runbook: `{kernel_command('materialize-operator-runbook', '--run-dir', str(run_dir), *(['--round-id', round_id] if round_id else []))}`",
        "",
        "## Case Run Start Checklist",
        "",
        "These checklists are operator surfaces only; they do not set agenda, rank sources, score evidence, or decide report readiness.",
        "",
        f"1. Start a council run from the user-facing mission envelope: `{kernel_command('start-council-run', '--run-dir', str(run_dir), '--run-id', display_run_id, '--round-id', display_round_id, '--mission-path', '<mission.json>', actor_role='runtime-operator')}`",
        f"1. Inspect the runtime and agent entry surfaces before agents begin: `{kernel_command('show-run-state', '--run-dir', str(run_dir), '--round-id', display_round_id, '--tail', '20')}`",
        f"1. Refresh the agent entry gate after mission or round-surface changes: `{kernel_command('materialize-agent-entry-gate', '--run-dir', str(run_dir), '--run-id', display_run_id, '--round-id', display_round_id)}`",
        f"1. Register agents from the generated registration plan, then use the role workspaces under `{str((run_dir / 'supervisor' / 'openclaw-workspaces').resolve())}`.",
        "",
    ]
    if run_id and round_id:
        lines.extend(
            [
                "## Agent Entry",
                "",
                f"- Materialize agent entry gate: `{kernel_command('materialize-agent-entry-gate', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id)}`",
                f"- Query current council proposals: `{kernel_command('query-council-objects', '--run-dir', str(run_dir), '--object-kind', 'proposal', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Query current readiness opinions: `{kernel_command('query-council-objects', '--run-dir', str(run_dir), '--object-kind', 'readiness-opinion', '--run-id', run_id, '--round-id', round_id)}`",
                "",
                "## Phase Transition Approval",
                "",
                f"- Query transition requests: `{kernel_command('query-control-objects', '--run-dir', str(run_dir), '--object-kind', 'transition-request', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Query approvals: `{kernel_command('query-control-objects', '--run-dir', str(run_dir), '--object-kind', 'transition-approval', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Query rejections: `{kernel_command('query-control-objects', '--run-dir', str(run_dir), '--object-kind', 'transition-rejection', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Moderator request report-basis freeze: `{kernel_command('request-phase-transition', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--transition-kind', TRANSITION_KIND_FREEZE_REPORT_BASIS, '--rationale', '<rationale>', actor_role='moderator')}`",
                f"- Moderator request close-round: `{kernel_command('request-phase-transition', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--transition-kind', TRANSITION_KIND_CLOSE_ROUND, '--rationale', '<rationale>', actor_role='moderator')}`",
                f"- Moderator request follow-up round: `{kernel_command('request-phase-transition', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--transition-kind', TRANSITION_KIND_OPEN_INVESTIGATION_ROUND, '--target-round-id', '<target_round_id>', '--source-round-id', round_id, '--rationale', '<rationale>', actor_role='moderator')}`",
                f"- Moderator request report-writing round: `{kernel_command('request-phase-transition', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--transition-kind', TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND, '--target-round-id', '<report_round_id>', '--source-round-id', round_id, '--request-payload-json', json.dumps({'round_mode': 'report-writing', 'basis_round_id': round_id, 'reporting_basis_refs': ['<reporting_basis_ref>'], 'scope': 'report-editor-only situation-analysis brief and narrative report production from existing council basis'}, ensure_ascii=True, sort_keys=True), '--rationale', '<rationale>', actor_role='moderator')}`",
                f"- Operator approve request: `{kernel_command('approve-phase-transition', '--run-dir', str(run_dir), '--request-id', '<request_id>', '--approval-reason', '<approval_reason>', actor_role='runtime-operator')}`",
                f"- Operator reject request: `{kernel_command('reject-phase-transition', '--run-dir', str(run_dir), '--request-id', '<request_id>', '--rejection-reason', '<rejection_reason>', actor_role='runtime-operator')}`",
                "",
                "## Optional Analysis Skill Approval",
                "",
                f"- Query skill approval requests: `{kernel_command('query-control-objects', '--run-dir', str(run_dir), '--object-kind', 'skill-approval-request', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Query skill approvals: `{kernel_command('query-control-objects', '--run-dir', str(run_dir), '--object-kind', 'skill-approval', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Query skill approval rejections: `{kernel_command('query-control-objects', '--run-dir', str(run_dir), '--object-kind', 'skill-approval-rejection', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Query skill approval consumptions: `{kernel_command('query-control-objects', '--run-dir', str(run_dir), '--object-kind', 'skill-approval-consumption', '--run-id', run_id, '--round-id', round_id)}`",
                f"- Moderator request optional-analysis run: `{kernel_command('request-skill-approval', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--skill-name', '<skill_name>', '--requested-actor-role', '<requested_actor_role>', '--rationale', '<rationale>', '--requested-skill-arg=<skill_arg>', actor_role='moderator')}`",
                f"- Operator approve skill request: `{kernel_command('approve-skill-approval', '--run-dir', str(run_dir), '--request-id', '<request_id>', '--approval-reason', '<approval_reason>', actor_role='runtime-operator')}`",
                f"- Operator reject skill request: `{kernel_command('reject-skill-approval', '--run-dir', str(run_dir), '--request-id', '<request_id>', '--rejection-reason', '<rejection_reason>', actor_role='runtime-operator')}`",
                f"- Run approved optional-analysis skill: `{run_skill_command(run_dir=run_dir, run_id=run_id, round_id=round_id, skill_name='<skill_name>', actor_role='<requested_actor_role>', contract_mode='warn', skill_args=['--example-arg', '<value>'], skill_approval_request_id='<request_id>')}`",
                "",
                "## Report Publication Checklist",
                "",
                "This checklist consumes frozen/reporting basis and validation state; it is not a path for adding new investigation evidence.",
                "",
                f"1. Inspect reporting state and blockers: `{kernel_command('show-reporting-state', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--pretty')}`",
                f"1. Query reporting handoff rows: `{kernel_command('query-reporting-objects', '--run-dir', str(run_dir), '--object-kind', 'reporting-handoff', '--run-id', run_id, '--round-id', round_id)}`",
                f"1. Query situation-analysis brief rows: `{kernel_command('query-reporting-objects', '--run-dir', str(run_dir), '--object-kind', 'situation-analysis-brief', '--run-id', run_id, '--round-id', round_id)}`",
                f"1. Materialize situation-analysis brief before narrative writing: `{run_skill_command(run_dir=run_dir, run_id=run_id, round_id=round_id, skill_name='materialize-situation-analysis-brief', actor_role='report-editor', contract_mode='warn', skill_args=['--basis-round-id', '<basis_round_id>', '--program-id', '<program_id>'])}`",
                f"1. Draft narrative report as report-editor: `{run_skill_command(run_dir=run_dir, run_id=run_id, round_id=round_id, skill_name='draft-narrative-report', actor_role='report-editor', contract_mode='warn', skill_args=['--basis-round-id', '<basis_round_id>', '--language', '<en|zh>'])}`",
                f"1. Validate the draft before publication: `{run_skill_command(run_dir=run_dir, run_id=run_id, round_id=round_id, skill_name='validate-narrative-report', actor_role='report-editor', contract_mode='warn', skill_args=[])}`",
                f"1. Publish only after validation allows publication: `{run_skill_command(run_dir=run_dir, run_id=run_id, round_id=round_id, skill_name='publish-narrative-report', actor_role='report-editor', contract_mode='warn', skill_args=[])}`",
                f"1. Materialize final publication from canonical reporting outputs: `{run_skill_command(run_dir=run_dir, run_id=run_id, round_id=round_id, skill_name='materialize-final-publication', actor_role='report-editor', contract_mode='warn', skill_args=[])}`",
                f"1. Rebuild reporting exports for inspection: `{kernel_command('materialize-reporting-exports', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, actor_role='runtime-operator')}`",
                f"1. Query final publication rows: `{kernel_command('query-reporting-objects', '--run-dir', str(run_dir), '--object-kind', 'final-publication', '--run-id', run_id, '--round-id', round_id)}`",
                "",
                "## Case Archive Checklist",
                "",
                "Archive steps preserve mission, timeline, final reports, key artifacts, and runtime health as case records; they do not turn archived material into newly accepted evidence.",
                "",
                f"1. Inspect archive/checkpoint status: `{kernel_command('show-archive-status', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--pretty')}`",
                f"1. Bootstrap history context for later continuation or replay: `{kernel_command('bootstrap-history-context', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, actor_role='runtime-operator')}`",
                f"1. Close a terminal round only after an approved close-round request: `{kernel_command('close-round', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, '--transition-request-id', '<request_id>', actor_role='runtime-operator')}`",
                f"1. Refresh runtime health after archive/closeout: `{kernel_command('materialize-runtime-health', '--run-dir', str(run_dir), '--round-id', round_id, actor_role='runtime-operator')}`",
                f"1. Materialize the case-run package manifest for demonstration/archive review: `{kernel_command('materialize-case-run-package', '--run-dir', str(run_dir), '--run-id', run_id, '--round-id', round_id, actor_role='runtime-operator')}`",
                f"1. Rebuild this runbook after closeout: `{kernel_command('materialize-operator-runbook', '--run-dir', str(run_dir), '--round-id', round_id, actor_role='runtime-operator')}`",
                "",
            ]
        )
        if transition_requests:
            lines.extend(["## Current Transition Requests", ""])
            for request in transition_requests:
                if not isinstance(request, dict):
                    continue
                lines.append(f"### {maybe_text(request.get('request_id'))}")
                lines.append("")
                lines.append(f"- Kind: `{maybe_text(request.get('transition_kind'))}`")
                lines.append(f"- Status: `{maybe_text(request.get('request_status'))}`")
                lines.append(f"- Requested by: `{maybe_text(request.get('requested_by_role'))}`")
                lines.append(f"- Requested command: `{maybe_text(request.get('requested_command_name'))}`")
                if maybe_text(request.get("target_round_id")) and maybe_text(request.get("target_round_id")) != maybe_text(request.get("round_id")):
                    lines.append(f"- Target round: `{maybe_text(request.get('target_round_id'))}`")
                if maybe_text(request.get("latest_decision_by_role")) or maybe_text(request.get("latest_decision_status")):
                    lines.append(
                        f"- Latest decision: `{maybe_text(request.get('latest_decision_status'))}` by `{maybe_text(request.get('latest_decision_by_role')) or 'unknown'}`"
                    )
                if maybe_text(request.get("latest_decision_reason")):
                    lines.append(f"- Decision reason: {maybe_text(request.get('latest_decision_reason'))}")
                if maybe_text(request.get("rationale")):
                    lines.append(f"- Request rationale: {maybe_text(request.get('rationale'))}")
                basis_object_ids = request.get("basis_object_ids", []) if isinstance(request.get("basis_object_ids"), list) else []
                if basis_object_ids:
                    lines.append(f"- Evidence basis objects: `{', '.join(maybe_text(item) for item in basis_object_ids if maybe_text(item))}`")
                evidence_refs = request.get("evidence_refs", []) if isinstance(request.get("evidence_refs"), list) else []
                if evidence_refs:
                    lines.append(f"- Evidence refs: `{json.dumps(evidence_refs, ensure_ascii=True)}`")
                if maybe_text(request.get("committed_object_kind")) or maybe_text(request.get("committed_object_id")):
                    lines.append(
                        f"- Commit record: `{maybe_text(request.get('committed_object_kind'))}:{maybe_text(request.get('committed_object_id'))}`"
                    )
                lines.append("")
        if skill_approval_requests:
            lines.extend(["## Current Skill Approval Requests", ""])
            for request in skill_approval_requests:
                if not isinstance(request, dict):
                    continue
                lines.append(f"### {maybe_text(request.get('request_id'))}")
                lines.append("")
                lines.append(f"- Skill: `{maybe_text(request.get('skill_name'))}`")
                lines.append(f"- Status: `{maybe_text(request.get('request_status'))}`")
                lines.append(f"- Requested by: `{maybe_text(request.get('requested_by_role'))}`")
                lines.append(f"- Requested actor: `{maybe_text(request.get('requested_actor_role'))}`")
                if maybe_text(request.get("latest_decision_by_role")) or maybe_text(request.get("latest_decision_status")):
                    lines.append(
                        f"- Latest decision: `{maybe_text(request.get('latest_decision_status'))}` by `{maybe_text(request.get('latest_decision_by_role')) or 'unknown'}`"
                    )
                if maybe_text(request.get("latest_decision_reason")):
                    lines.append(f"- Decision reason: {maybe_text(request.get('latest_decision_reason'))}")
                if maybe_text(request.get("consumed_receipt_id")):
                    lines.append(f"- Consumed receipt: `{maybe_text(request.get('consumed_receipt_id'))}`")
                if maybe_text(request.get("rationale")):
                    lines.append(f"- Request rationale: {maybe_text(request.get('rationale'))}")
                basis_object_ids = request.get("basis_object_ids", []) if isinstance(request.get("basis_object_ids"), list) else []
                if basis_object_ids:
                    lines.append(f"- Evidence basis objects: `{', '.join(maybe_text(item) for item in basis_object_ids if maybe_text(item))}`")
                evidence_refs = request.get("evidence_refs", []) if isinstance(request.get("evidence_refs"), list) else []
                if evidence_refs:
                    lines.append(f"- Evidence refs: `{json.dumps(evidence_refs, ensure_ascii=True)}`")
                lines.append("")
    lines.extend(
        [
        "## Failure Classes",
        "",
        ]
    )
    for title in RUNBOOK_SECTIONS.values():
        steps = operator_resolution_steps(
            next(key for key, value in RUNBOOK_SECTIONS.items() if value == title),
            False,
        )
        lines.append(f"### {title}")
        lines.append("")
        for step in steps:
            lines.append(f"1. {step}")
        lines.append("")
    lines.extend(["## Current Open Dead Letters", ""])
    if not dead_letters:
        lines.append("No open dead letters are currently present.")
        lines.append("")
        return "\n".join(lines)
    for payload in dead_letters:
        lines.append(f"### {maybe_text(payload.get('dead_letter_id'))}")
        lines.append("")
        lines.append(f"- Source: `{maybe_text(payload.get('source_type'))}:{maybe_text(payload.get('source_name'))}`")
        lines.append(f"- Failure class: `{maybe_text(payload.get('failure_class'))}`")
        lines.append(f"- Message: {maybe_text(payload.get('message'))}")
        if maybe_text(payload.get("command_hint")):
            lines.append(f"- Suggested command: `{maybe_text(payload.get('command_hint'))}`")
        lines.append("")
        for step in payload.get("operator_resolution_steps", []) if isinstance(payload.get("operator_resolution_steps"), list) else []:
            lines.append(f"1. {maybe_text(step)}")
        lines.append("")
    return "\n".join(lines)


def materialize_operator_runbook(run_dir: Path, *, round_id: str = "") -> str:
    ensure_runtime_dirs(run_dir)
    content = operator_runbook_markdown(run_dir, round_id=round_id)
    path = operator_runbook_path(run_dir, round_id)
    path.write_text(content + "\n", encoding="utf-8")
    return str(path)


__all__ = (
    "operator_runbook_markdown",
    "materialize_operator_runbook",
)
