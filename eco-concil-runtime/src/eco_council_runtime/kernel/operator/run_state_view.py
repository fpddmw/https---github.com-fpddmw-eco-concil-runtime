from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.ledger import load_ledger_tail
from eco_council_runtime.kernel.core.manifest import load_json_if_exists
from eco_council_runtime.kernel.core.paths import (
    admission_policy_path,
    benchmark_compare_path,
    benchmark_manifest_path,
    controller_state_path,
    cursor_path,
    history_bootstrap_state_path,
    ledger_path,
    manifest_path,
    operator_runbook_path,
    registry_path,
    replay_report_path,
    report_basis_gate_path,
    round_close_state_path,
    runtime_health_path,
    scenario_fixture_path,
    supervisor_state_path,
)
from eco_council_runtime.kernel.execution.executor import maybe_text
from eco_council_runtime.kernel.governance.agent_entry import agent_entry_state
from eco_council_runtime.kernel.governance.agent_entry.handoff import HardGateCommandBuilder
from eco_council_runtime.kernel.governance.round_liveness import build_round_liveness_surface
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
    latest_transition_request,
    load_transition_requests,
)
from eco_council_runtime.kernel.operator.operations import (
    load_admission_policy,
    load_dead_letters,
    runtime_health_payload,
)
from eco_council_runtime.kernel.operator.surfaces import (
    build_reporting_surface,
    load_controller_state_wrapper,
    load_council_decision_wrapper,
    load_expert_report_wrapper,
    load_final_publication_wrapper,
    load_orchestration_plan_wrapper,
    load_report_basis_gate_wrapper,
    load_reporting_handoff_wrapper,
    load_supervisor_state_wrapper,
)
from eco_council_runtime.kernel.planes.deliberation_plane import (
    load_governed_execution_control_state,
)
from eco_council_runtime.runtime_command_hints import kernel_command, run_skill_command


__all__ = (
    "benchmark_operator_view",
    "governed_execution_operator_view",
    "operations_state",
    "post_round_operator_view",
    "reporting_operator_view",
    "reporting_state_for_round",
    "show_run_state",
    "transition_request_state",
)


def transition_request_state(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    if not run_id or not round_id:
        return {
            "summary": {
                "pending_request_count": 0,
                "approved_request_count": 0,
                "rejected_request_count": 0,
                "committed_request_count": 0,
                "pending_skill_approval_request_count": 0,
                "approved_skill_approval_request_count": 0,
                "rejected_skill_approval_request_count": 0,
                "consumed_skill_approval_request_count": 0,
            },
            "latest_requests": [],
            "latest_skill_approval_requests": [],
        }
    latest_requests = load_transition_requests(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        limit=20,
    )
    status_counts = {
        REQUEST_STATUS_PENDING: 0,
        REQUEST_STATUS_APPROVED: 0,
        REQUEST_STATUS_REJECTED: 0,
        REQUEST_STATUS_COMMITTED: 0,
    }
    for request in latest_requests:
        if not isinstance(request, dict):
            continue
        status = maybe_text(request.get("request_status"))
        if status in status_counts:
            status_counts[status] += 1
    latest_skill_approval_requests = load_skill_approval_requests(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        limit=20,
    )
    skill_status_counts = {
        SKILL_REQUEST_STATUS_PENDING: 0,
        SKILL_REQUEST_STATUS_APPROVED: 0,
        SKILL_REQUEST_STATUS_REJECTED: 0,
        SKILL_REQUEST_STATUS_CONSUMED: 0,
    }
    for request in latest_skill_approval_requests:
        if not isinstance(request, dict):
            continue
        status = maybe_text(request.get("request_status"))
        if status in skill_status_counts:
            skill_status_counts[status] += 1
    return {
        "summary": {
            "pending_request_count": status_counts[REQUEST_STATUS_PENDING],
            "approved_request_count": status_counts[REQUEST_STATUS_APPROVED],
            "rejected_request_count": status_counts[REQUEST_STATUS_REJECTED],
            "committed_request_count": status_counts[REQUEST_STATUS_COMMITTED],
            "pending_skill_approval_request_count": skill_status_counts[
                SKILL_REQUEST_STATUS_PENDING
            ],
            "approved_skill_approval_request_count": skill_status_counts[
                SKILL_REQUEST_STATUS_APPROVED
            ],
            "rejected_skill_approval_request_count": skill_status_counts[
                SKILL_REQUEST_STATUS_REJECTED
            ],
            "consumed_skill_approval_request_count": skill_status_counts[
                SKILL_REQUEST_STATUS_CONSUMED
            ],
        },
        "latest_requests": latest_requests,
        "latest_skill_approval_requests": latest_skill_approval_requests,
        "query_transition_requests_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "transition-request",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        ),
        "query_transition_approvals_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "transition-approval",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        ),
        "query_transition_rejections_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "transition-rejection",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        ),
        "query_skill_approval_requests_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "skill-approval-request",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        ),
        "query_skill_approvals_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "skill-approval",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        ),
        "query_skill_approval_rejections_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "skill-approval-rejection",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        ),
        "query_skill_approval_consumptions_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "skill-approval-consumption",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        ),
        "request_skill_approval_command_template": kernel_command(
            "request-skill-approval",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--skill-name",
            "<skill_name>",
            "--requested-actor-role",
            "<requested_actor_role>",
            "--rationale",
            "<rationale>",
            actor_role="moderator",
        ),
        "approve_skill_approval_request_command_template": kernel_command(
            "approve-skill-approval",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--approval-reason",
            "<approval_reason>",
            actor_role="runtime-operator",
        ),
        "reject_skill_approval_request_command_template": kernel_command(
            "reject-skill-approval",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--rejection-reason",
            "<rejection_reason>",
            actor_role="runtime-operator",
        ),
        "request_open_round_command_template": kernel_command(
            "request-phase-transition",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--transition-kind",
            TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
            "--target-round-id",
            "<target_round_id>",
            "--source-round-id",
            round_id,
            "--request-payload-json",
            json.dumps(
                {
                    "round_mode": "continuation",
                    "primary_focus_refs": ["<object_kind:object_id>"],
                    "continuation_basis": "moderator-selected unresolved refs",
                    "closure_reason_if_not_continuing": "<report-ready|no-actionable-path|human-paused|out-of-scope>",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "--rationale",
            "<rationale>",
            actor_role="moderator",
        ),
        "request_report_basis_command_template": kernel_command(
            "request-phase-transition",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--transition-kind",
            TRANSITION_KIND_FREEZE_REPORT_BASIS,
            "--rationale",
            "<rationale>",
            actor_role="moderator",
        ),
        "request_close_round_command_template": kernel_command(
            "request-phase-transition",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--transition-kind",
            TRANSITION_KIND_CLOSE_ROUND,
            "--rationale",
            "<rationale>",
            actor_role="moderator",
        ),
        "approve_transition_request_command_template": kernel_command(
            "approve-phase-transition",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--approval-reason",
            "<approval_reason>",
        ),
        "reject_transition_request_command_template": kernel_command(
            "reject-phase-transition",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--rejection-reason",
            "<rejection_reason>",
        ),
    }


def governed_execution_operator_view(
    run_dir: Path,
    round_id: str,
    governed_execution_state: dict[str, Any],
    reporting_surface: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = governed_execution_state.get("plan", {}) if isinstance(governed_execution_state.get("plan"), dict) else {}
    gate = (
        governed_execution_state.get("report_basis_gate", {})
        if isinstance(governed_execution_state.get("report_basis_gate"), dict)
        else governed_execution_state.get("report_basis_gate", {})
        if isinstance(governed_execution_state.get("report_basis_gate"), dict)
        else {}
    )
    controller = governed_execution_state.get("controller", {}) if isinstance(governed_execution_state.get("controller"), dict) else {}
    supervisor = governed_execution_state.get("supervisor", {}) if isinstance(governed_execution_state.get("supervisor"), dict) else {}
    reporting = reporting_surface if isinstance(reporting_surface, dict) else {}
    run_id = (
        maybe_text(supervisor.get("run_id"))
        or maybe_text(controller.get("run_id"))
        or maybe_text(plan.get("run_id"))
        or maybe_text(gate.get("run_id"))
    )
    resume_command = maybe_text(supervisor.get("resume_command")) or (
        kernel_command(
            "resume-governed-execution-round",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        )
        if round_id and run_id
        else ""
    )
    restart_command = maybe_text(supervisor.get("restart_command")) or (
        kernel_command(
            "restart-governed-execution-round",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        )
        if round_id and run_id
        else ""
    )
    inspect_command = maybe_text(supervisor.get("inspect_command")) or (
        f"show-run-state --run-dir {run_dir} --round-id {round_id} --tail 20" if round_id else ""
    )
    query_public_signals_command = (
        run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-public-signals",
            contract_mode="warn",
        )
        if round_id and run_id
        else ""
    )
    query_formal_signals_command = (
        run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-formal-signals",
            contract_mode="warn",
        )
        if round_id and run_id
        else ""
    )
    query_environment_signals_command = (
        run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-environment-signals",
            contract_mode="warn",
        )
        if round_id and run_id
        else ""
    )
    approved_open_request = (
        latest_transition_request(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            transition_kind=TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
            request_status=REQUEST_STATUS_APPROVED,
        )
        if round_id and run_id
        else None
    )
    approved_report_basis_request = (
        latest_transition_request(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            transition_kind=TRANSITION_KIND_FREEZE_REPORT_BASIS,
            request_status=REQUEST_STATUS_APPROVED,
        )
        if round_id and run_id
        else None
    )
    round_transition = (
        supervisor.get("round_transition", {})
        if isinstance(supervisor.get("round_transition"), dict)
        else {}
    )
    suggested_next_round_id = maybe_text(round_transition.get("suggested_round_id"))
    return {
        "round_id": round_id,
        "plan_id": maybe_text(plan.get("plan_id")),
        "planning_status": maybe_text(plan.get("planning_status")),
        "planning_mode": maybe_text(controller.get("planning_mode")) or maybe_text(supervisor.get("planning_mode")),
        "plan_source": maybe_text(plan.get("plan_source")),
        "planned_stage_count": (
            int(plan.get("step_counts", {}).get("planned_stage_count") or 0)
            if isinstance(plan.get("step_counts"), dict)
            else 0
        ),
        "controller_status": maybe_text(controller.get("controller_status")) or "missing",
        "supervisor_status": maybe_text(supervisor.get("supervisor_status")),
        "supervisor_substatus": maybe_text(supervisor.get("supervisor_substatus")),
        "governed_execution_posture": maybe_text(supervisor.get("governed_execution_posture")),
        "terminal_state": maybe_text(supervisor.get("terminal_state")),
        "readiness_status": maybe_text(controller.get("readiness_status")) or maybe_text(supervisor.get("readiness_status")),
        "gate_status": maybe_text(controller.get("gate_status")) or maybe_text(gate.get("gate_status")),
        "report_basis_status": maybe_text(controller.get("report_basis_status"))
        or maybe_text(supervisor.get("report_basis_status"))
        or maybe_text(gate.get("report_basis_status")),
        "reporting_ready": bool(reporting.get("reporting_ready")),
        "reporting_blockers": reporting.get("reporting_blockers", [])
        if isinstance(reporting.get("reporting_blockers"), list)
        else [],
        "reporting_handoff_status": maybe_text(reporting.get("handoff_status")),
        "reporting_surface_source": maybe_text(reporting.get("surface_source")),
        "current_stage": maybe_text(controller.get("current_stage")),
        "failed_stage": maybe_text(controller.get("failed_stage")),
        "completed_stage_names": controller.get("completed_stage_names", []) if isinstance(controller.get("completed_stage_names"), list) else [],
        "pending_stage_names": controller.get("pending_stage_names", []) if isinstance(controller.get("pending_stage_names"), list) else [],
        "resume_recommended": bool(controller.get("resume_recommended")) or bool(supervisor.get("resume_recommended")),
        "restart_recommended": bool(controller.get("restart_recommended")) or bool(supervisor.get("restart_recommended")),
        "resume_from_stage": maybe_text(controller.get("recovery", {}).get("resume_from_stage"))
        if isinstance(controller.get("recovery"), dict)
        else maybe_text(supervisor.get("resume_from_stage")),
        "resume_command": resume_command,
        "restart_command": restart_command,
        "inspect_command": inspect_command,
        "show_reporting_state_command": (
            f"show-reporting-state --run-dir {run_dir} --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_controller_state_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind controller-state --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_gate_state_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind gate-state --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_supervisor_state_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind supervisor-state --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_runtime_control_freeze_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind runtime-control-freeze --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_orchestration_plans_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind orchestration-plan --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_orchestration_plan_steps_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind orchestration-plan-step --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "materialize_governed_execution_exports_command": (
            kernel_command(
                "materialize-governed-execution-exports",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if round_id and run_id
            else ""
        ),
        "query_public_signals_command": query_public_signals_command,
        "query_formal_signals_command": query_formal_signals_command,
        "query_environment_signals_command": query_environment_signals_command,
        "query_next_actions_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind next-action --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_probes_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind probe --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_readiness_assessments_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind readiness-assessment --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_report_basis_freeze_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind report-basis-freeze --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_transition_requests_command": (
            kernel_command(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "transition-request",
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if round_id and run_id
            else ""
        ),
        "request_report_basis_transition_command": (
            kernel_command(
                "request-phase-transition",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--transition-kind",
                TRANSITION_KIND_FREEZE_REPORT_BASIS,
                "--rationale",
                "<rationale>",
                actor_role="moderator",
            )
            if round_id and run_id
            else ""
        ),
        "approve_transition_request_command_template": (
            kernel_command(
                "approve-phase-transition",
                "--run-dir",
                str(run_dir),
                "--request-id",
                "<request_id>",
                "--approval-reason",
                "<approval_reason>",
            )
            if round_id and run_id
            else ""
        ),
        "reject_transition_request_command_template": (
            kernel_command(
                "reject-phase-transition",
                "--run-dir",
                str(run_dir),
                "--request-id",
                "<request_id>",
                "--rejection-reason",
                "<rejection_reason>",
            )
            if round_id and run_id
            else ""
        ),
        "freeze_report_basis_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="freeze-report-basis",
                actor_role="moderator",
                contract_mode="warn",
                skill_args=[
                    "--transition-request-id",
                    maybe_text(approved_report_basis_request.get("request_id")),
                ],
            )
            if round_id and run_id and isinstance(approved_report_basis_request, dict)
            else ""
        ),
        "request_open_round_command": (
            kernel_command(
                "request-phase-transition",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--transition-kind",
                TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
                "--target-round-id",
                suggested_next_round_id or "<target_round_id>",
                "--source-round-id",
                round_id,
                "--request-payload-json",
                json.dumps(
                    {
                        "round_mode": "continuation",
                        "primary_focus_refs": ["<object_kind:object_id>"],
                        "continuation_basis": "moderator-selected unresolved refs",
                        "closure_reason_if_not_continuing": "<report-ready|no-actionable-path|human-paused|out-of-scope>",
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                "--rationale",
                "<rationale>",
                actor_role="moderator",
            )
            if round_id and run_id
            else ""
        ),
        "open_follow_up_round_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=suggested_next_round_id or "<target_round_id>",
                skill_name="open-investigation-round",
                actor_role="moderator",
                contract_mode="warn",
                skill_args=[
                    "--source-round-id",
                    round_id,
                    "--transition-request-id",
                    maybe_text(approved_open_request.get("request_id")),
                ],
            )
            if round_id and run_id and suggested_next_round_id and isinstance(approved_open_request, dict)
            else ""
        ),
        "query_readiness_blockers_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind next-action --run-id {run_id} --round-id {round_id} --readiness-blocker-only"
            if round_id and run_id
            else ""
        ),
        "inspection_paths": {
            "plan_path": maybe_text(plan.get("artifact_path"))
            or (
                maybe_text(controller.get("artifacts", {}).get("orchestration_plan_path"))
                if isinstance(controller.get("artifacts"), dict)
                else maybe_text(supervisor.get("orchestration_plan_path"))
            ),
            "controller_path": maybe_text(controller.get("artifacts", {}).get("controller_state_path"))
            if isinstance(controller.get("artifacts"), dict)
            else maybe_text(supervisor.get("controller_path")),
            "gate_path": maybe_text(controller.get("artifacts", {}).get("report_basis_gate_path"))
            or maybe_text(controller.get("artifacts", {}).get("report_basis_gate_path"))
            if isinstance(controller.get("artifacts"), dict)
            else maybe_text(supervisor.get("report_basis_gate_path"))
            or maybe_text(supervisor.get("report_basis_gate_path")),
            "supervisor_path": (
                maybe_text(supervisor.get("supervisor_path"))
                or str(supervisor_state_path(run_dir, round_id).resolve())
            )
            if round_id
            else "",
        },
        "recommended_next_skills": supervisor.get("recommended_next_skills", []) if isinstance(supervisor.get("recommended_next_skills"), list) else [],
        "round_transition": round_transition,
        "operator_notes": supervisor.get("operator_notes", []) if isinstance(supervisor.get("operator_notes"), list) else [],
    }


def reporting_operator_view(
    run_dir: Path,
    round_id: str,
    run_id: str,
    reporting_state: dict[str, Any],
) -> dict[str, Any]:
    surface = (
        reporting_state.get("surface", {})
        if isinstance(reporting_state.get("surface"), dict)
        else {}
    )
    handoff = (
        reporting_state.get("handoff", {})
        if isinstance(reporting_state.get("handoff"), dict)
        else {}
    )
    decision_draft = (
        reporting_state.get("decision_draft", {})
        if isinstance(reporting_state.get("decision_draft"), dict)
        else {}
    )
    decision = (
        reporting_state.get("decision", {})
        if isinstance(reporting_state.get("decision"), dict)
        else {}
    )
    final_publication = (
        reporting_state.get("final_publication", {})
        if isinstance(reporting_state.get("final_publication"), dict)
        else {}
    )
    return {
        "round_id": round_id,
        "reporting_ready": bool(surface.get("reporting_ready")),
        "reporting_blockers": surface.get("reporting_blockers", [])
        if isinstance(surface.get("reporting_blockers"), list)
        else [],
        "handoff_status": maybe_text(surface.get("handoff_status")),
        "surface_source": maybe_text(surface.get("surface_source")),
        "publication_status": maybe_text(surface.get("publication_status")),
        "publication_posture": maybe_text(surface.get("publication_posture")),
        "handoff_present": bool(handoff),
        "decision_draft_present": bool(decision_draft),
        "decision_present": bool(decision),
        "final_publication_present": bool(final_publication),
        "query_reporting_handoff_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind reporting-handoff --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_council_decision_drafts_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind council-decision --run-id {run_id} --round-id {round_id} --stage draft"
            if round_id and run_id
            else ""
        ),
        "query_council_decisions_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind council-decision --run-id {run_id} --round-id {round_id} --stage canonical"
            if round_id and run_id
            else ""
        ),
        "query_expert_report_drafts_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind expert-report --run-id {run_id} --round-id {round_id} --stage draft"
            if round_id and run_id
            else ""
        ),
        "query_expert_reports_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind expert-report --run-id {run_id} --round-id {round_id} --stage canonical"
            if round_id and run_id
            else ""
        ),
        "query_report_section_drafts_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind report-section-draft --run-id {run_id} --round-id {round_id} --include-contract"
            if round_id and run_id
            else ""
        ),
        "query_final_publications_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind final-publication --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "materialize_reporting_exports_command": (
            kernel_command(
                "materialize-reporting-exports",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if round_id and run_id
            else ""
        ),
        "materialize_reporting_handoff_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="materialize-reporting-handoff",
                actor_role="moderator",
                contract_mode="warn",
            )
            if round_id and run_id
            else ""
        ),
        "draft_council_decision_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="draft-council-decision",
                actor_role="moderator",
                contract_mode="warn",
            )
            if round_id and run_id
            else ""
        ),
        "draft_social_investigator_report_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="draft-expert-report",
                actor_role="report-editor",
                contract_mode="warn",
                skill_args=["--role", "social-investigator"],
            )
            if round_id and run_id
            else ""
        ),
        "draft_environmental_investigator_report_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="draft-expert-report",
                actor_role="report-editor",
                contract_mode="warn",
                skill_args=["--role", "environmental-investigator"],
            )
            if round_id and run_id
            else ""
        ),
        "publish_council_decision_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="publish-council-decision",
                actor_role="moderator",
                contract_mode="warn",
            )
            if round_id and run_id
            else ""
        ),
        "publish_social_investigator_report_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="publish-expert-report",
                actor_role="report-editor",
                contract_mode="warn",
                skill_args=["--role", "social-investigator"],
            )
            if round_id and run_id
            else ""
        ),
        "publish_environmental_investigator_report_command": (
            run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="publish-expert-report",
                actor_role="report-editor",
                contract_mode="warn",
                skill_args=["--role", "environmental-investigator"],
            )
            if round_id and run_id
            else ""
        ),
        "show_run_state_command": (
            f"show-run-state --run-dir {run_dir} --round-id {round_id} --tail 20"
            if round_id
            else ""
        ),
        "inspection_paths": {
            "handoff_path": maybe_text(handoff.get("output_path")),
            "decision_draft_path": maybe_text(decision_draft.get("output_path")),
            "decision_path": maybe_text(decision.get("output_path")),
            "final_publication_path": maybe_text(final_publication.get("output_path")),
        },
    }


def reporting_state_for_round(run_dir: Path, run_id: str, round_id: str) -> dict[str, Any]:
    supervisor_context = load_supervisor_state_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    supervisor = (
        supervisor_context.get("payload")
        if isinstance(supervisor_context.get("payload"), dict)
        else {}
    )
    handoff_context = load_reporting_handoff_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    decision_draft_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
    )
    decision_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="canonical",
    )
    expert_report_drafts = {
        role: (
            load_expert_report_wrapper(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                agent_role=role,
                report_stage="draft",
            ).get("payload", {})
        )
        for role in ("social-investigator", "environmental-investigator")
    }
    expert_reports = {
        role: (
            load_expert_report_wrapper(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                agent_role=role,
                report_stage="canonical",
            ).get("payload", {})
        )
        for role in ("social-investigator", "environmental-investigator")
    }
    final_publication_context = load_final_publication_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    surface = build_reporting_surface(
        supervisor_payload=supervisor,
        handoff_payload=handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else {},
        decision_draft_payload=decision_draft_context.get("payload")
        if isinstance(decision_draft_context.get("payload"), dict)
        else {},
        decision_payload=decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else {},
        expert_report_payloads={
            role: payload
            for role, payload in expert_reports.items()
            if isinstance(payload, dict)
        },
        final_publication_payload=final_publication_context.get("payload")
        if isinstance(final_publication_context.get("payload"), dict)
        else {},
    )
    reporting_state = {
        "supervisor": supervisor,
        "handoff": handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else {},
        "decision_draft": decision_draft_context.get("payload")
        if isinstance(decision_draft_context.get("payload"), dict)
        else {},
        "decision": decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else {},
        "expert_report_drafts": expert_report_drafts,
        "expert_reports": expert_reports,
        "final_publication": final_publication_context.get("payload")
        if isinstance(final_publication_context.get("payload"), dict)
        else {},
        "surface": surface,
    }
    reporting_state["operator"] = reporting_operator_view(
        run_dir,
        round_id,
        run_id,
        reporting_state,
    )
    return reporting_state


def post_round_operator_view(run_dir: Path, round_id: str, post_round_state: dict[str, Any]) -> dict[str, Any]:
    round_close = post_round_state.get("round_close", {}) if isinstance(post_round_state.get("round_close"), dict) else {}
    history_bootstrap = post_round_state.get("history_bootstrap", {}) if isinstance(post_round_state.get("history_bootstrap"), dict) else {}
    run_id = maybe_text(round_close.get("run_id")) or maybe_text(history_bootstrap.get("run_id"))
    approved_close_request = (
        latest_transition_request(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            transition_kind=TRANSITION_KIND_CLOSE_ROUND,
            request_status=REQUEST_STATUS_APPROVED,
        )
        if run_id and round_id
        else None
    )
    return {
        "round_close_status": maybe_text(round_close.get("close_status")),
        "archive_status": maybe_text(round_close.get("archive_status")),
        "close_posture": maybe_text(round_close.get("close_posture")),
        "reporting_ready": bool(round_close.get("reporting_ready")),
        "reporting_blockers": round_close.get("reporting_blockers", [])
        if isinstance(round_close.get("reporting_blockers"), list)
        else [],
        "reporting_handoff_status": maybe_text(
            round_close.get("reporting_handoff_status")
        ),
        "history_bootstrap_status": maybe_text(history_bootstrap.get("bootstrap_status")),
        "selected_case_count": int(history_bootstrap.get("selected_case_count") or 0),
        "selected_signal_count": int(history_bootstrap.get("selected_signal_count") or 0),
        "close_command": (
            kernel_command(
                "close-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--transition-request-id",
                maybe_text(approved_close_request.get("request_id")),
            )
            if run_id and round_id and isinstance(approved_close_request, dict)
            else ""
        ),
        "request_close_round_command": (
            kernel_command(
                "request-phase-transition",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--transition-kind",
                TRANSITION_KIND_CLOSE_ROUND,
                "--rationale",
                "<rationale>",
                actor_role="moderator",
            )
            if run_id and round_id
            else ""
        ),
        "approve_transition_request_command_template": (
            kernel_command(
                "approve-phase-transition",
                "--run-dir",
                str(run_dir),
                "--request-id",
                "<request_id>",
                "--approval-reason",
                "<approval_reason>",
            )
            if run_id and round_id
            else ""
        ),
        "reject_transition_request_command_template": (
            kernel_command(
                "reject-phase-transition",
                "--run-dir",
                str(run_dir),
                "--request-id",
                "<request_id>",
                "--rejection-reason",
                "<rejection_reason>",
            )
            if run_id and round_id
            else ""
        ),
        "query_transition_requests_command": (
            kernel_command(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "transition-request",
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if run_id and round_id
            else ""
        ),
        "history_command": (
            kernel_command(
                "bootstrap-history-context",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if run_id and round_id
            else ""
        ),
        "round_close_path": str(round_close_state_path(run_dir, round_id).resolve()) if round_id else "",
        "history_bootstrap_path": str(history_bootstrap_state_path(run_dir, round_id).resolve()) if round_id else "",
    }


def benchmark_operator_view(run_dir: Path, round_id: str, benchmark_state: dict[str, Any]) -> dict[str, Any]:
    fixture = benchmark_state.get("scenario_fixture", {}) if isinstance(benchmark_state.get("scenario_fixture"), dict) else {}
    manifest = benchmark_state.get("benchmark_manifest", {}) if isinstance(benchmark_state.get("benchmark_manifest"), dict) else {}
    compare = benchmark_state.get("benchmark_compare", {}) if isinstance(benchmark_state.get("benchmark_compare"), dict) else {}
    replay = benchmark_state.get("replay_report", {}) if isinstance(benchmark_state.get("replay_report"), dict) else {}
    run_id = (
        maybe_text(manifest.get("run_id"))
        or maybe_text(fixture.get("run_id"))
        or maybe_text(replay.get("run_id"))
    )
    fixture_path = str(scenario_fixture_path(run_dir, round_id).resolve()) if round_id else ""
    benchmark_path = str(benchmark_manifest_path(run_dir, round_id).resolve()) if round_id else ""
    compare_path = str(benchmark_compare_path(run_dir, round_id).resolve()) if round_id else ""
    replay_path = str(replay_report_path(run_dir, round_id).resolve()) if round_id else ""
    baseline_manifest_path = maybe_text(fixture.get("baseline_manifest", {}).get("path")) if isinstance(fixture.get("baseline_manifest"), dict) else ""
    compare_command = ""
    replay_command = ""
    if round_id and run_id and baseline_manifest_path:
        compare_command = kernel_command(
            "compare-benchmark-manifests",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--left-manifest-path",
            baseline_manifest_path,
            "--right-manifest-path",
            benchmark_path,
        )
        replay_command = kernel_command(
            "replay-runtime-scenario",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--fixture-path",
            fixture_path,
        )
    return {
        "scenario_id": maybe_text(fixture.get("scenario_id")),
        "scenario_fingerprint": maybe_text(manifest.get("scenario_fingerprint")) or maybe_text(fixture.get("scenario_fingerprint")),
        "fixture_materialized": bool(fixture),
        "benchmark_materialized": bool(manifest),
        "reporting_ready": bool(manifest.get("governed_execution_summary", {}).get("reporting_ready"))
        if isinstance(manifest.get("governed_execution_summary"), dict)
        else False,
        "compare_verdict": maybe_text(compare.get("verdict")),
        "replay_verdict": maybe_text(replay.get("replay_verdict")),
        "fixture_command": (
            kernel_command(
                "materialize-scenario-fixture",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if run_id and round_id
            else ""
        ),
        "benchmark_command": (
            kernel_command(
                "materialize-benchmark-manifest",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if run_id and round_id
            else ""
        ),
        "compare_command": compare_command,
        "replay_command": replay_command,
        "fixture_path": fixture_path,
        "benchmark_manifest_path": benchmark_path,
        "benchmark_compare_path": compare_path,
        "replay_report_path": replay_path,
    }


def operations_state(run_dir: Path, selected_round_id: str) -> dict[str, Any]:
    admission_policy = load_admission_policy(run_dir)
    runtime_health = runtime_health_payload(run_dir, round_id=selected_round_id)
    runtime_lock = (
        runtime_health.get("runtime_lock", {})
        if isinstance(runtime_health.get("runtime_lock"), dict)
        else {}
    )
    dead_letters = load_dead_letters(run_dir, round_id=selected_round_id, limit=20)
    runbook_path = operator_runbook_path(run_dir, selected_round_id) if selected_round_id else operator_runbook_path(run_dir)
    run_id = maybe_text(admission_policy.get("run_id"))
    materialize_policy_command = (
        kernel_command(
            "materialize-admission-policy",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
        )
        if run_id
        else ""
    )
    return {
        "admission_policy": admission_policy,
        "runtime_health": runtime_health,
        "runtime_lock": runtime_lock,
        "dead_letters": dead_letters,
        "operator": {
            "permission_profile": maybe_text(admission_policy.get("permission_profile")) or "standard",
            "alert_status": maybe_text(runtime_health.get("alert_status")) or "green",
            "runtime_lock_state": maybe_text(runtime_lock.get("lock_state")),
            "runtime_lock_path": maybe_text(runtime_lock.get("lock_path")),
            "runtime_lock_state_path": maybe_text(runtime_lock.get("lock_state_path")),
            "admission_policy_path": str(admission_policy_path(run_dir).resolve()),
            "runtime_health_path": str(runtime_health_path(run_dir).resolve()),
            "operator_runbook_path": str(runbook_path.resolve()),
            "materialize_admission_policy_command": materialize_policy_command,
            "materialize_runtime_health_command": kernel_command(
                "materialize-runtime-health",
                "--run-dir",
                str(run_dir),
                *(["--round-id", selected_round_id] if selected_round_id else []),
            ),
            "materialize_operator_runbook_command": kernel_command(
                "materialize-operator-runbook",
                "--run-dir",
                str(run_dir),
                *(["--round-id", selected_round_id] if selected_round_id else []),
            ),
            "show_dead_letters_command": f"show-dead-letters --run-dir {run_dir}{f' --round-id {selected_round_id}' if selected_round_id else ''}",
            "open_dead_letter_count": int(runtime_health.get("summary", {}).get("open_dead_letter_count") or 0),
        },
    }


def show_run_state(
    run_dir: Path,
    tail: int,
    round_id: str = "",
    *,
    agent_entry_profile: dict[str, Any] | None = None,
    hard_gate_command_builder: HardGateCommandBuilder | None = None,
) -> dict[str, Any]:
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    cursor = load_json_if_exists(cursor_path(run_dir)) or {}
    registry = load_json_if_exists(registry_path(run_dir)) or {}
    current_round_id = str(cursor.get("current_round_id") or "")
    selected_round_id = maybe_text(round_id) or current_round_id
    resolved_run_id = maybe_text(manifest.get("run_id")) or maybe_text(cursor.get("run_id"))
    governed_execution_state: dict[str, Any] = {}
    reporting_state: dict[str, Any] = {}
    post_round_state: dict[str, Any] = {}
    benchmark_state: dict[str, Any] = {}
    transition_state: dict[str, Any] = {}
    round_liveness_state: dict[str, Any] = {}
    if selected_round_id:
        control_state = load_governed_execution_control_state(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
        )
        controller_context = load_controller_state_wrapper(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            controller_state_path=str(
                controller_state_path(run_dir, selected_round_id).resolve()
            ),
        )
        gate_context = load_report_basis_gate_wrapper(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            report_basis_gate_path=str(
                report_basis_gate_path(run_dir, selected_round_id).resolve()
            ),
        )
        supervisor_context = load_supervisor_state_wrapper(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            supervisor_state_path=str(
                supervisor_state_path(run_dir, selected_round_id).resolve()
            ),
        )
        plan_context = load_orchestration_plan_wrapper(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            orchestration_plan_path=str(
                (run_dir / "runtime" / f"orchestration_plan_{selected_round_id}.json").resolve()
            ),
        )
        governed_execution_state = {
            "plan": plan_context.get("payload", {})
            if isinstance(plan_context.get("payload"), dict)
            else (
                control_state.get("orchestration_plan", {})
                if isinstance(control_state.get("orchestration_plan"), dict)
                else {}
            ),
            "plan_steps": plan_context.get("step_rows", [])
            if isinstance(plan_context.get("step_rows"), list)
            else (
                control_state.get("orchestration_plan_steps", [])
                if isinstance(control_state.get("orchestration_plan_steps"), list)
                else []
            ),
            "report_basis_gate": gate_context.get("payload", {})
            if isinstance(gate_context.get("payload"), dict)
            else {},
            "controller": controller_context.get("payload", {})
            if isinstance(controller_context.get("payload"), dict)
            else {},
            "supervisor": supervisor_context.get("payload", {})
            if isinstance(supervisor_context.get("payload"), dict)
            else {},
            "report_basis_freeze": control_state.get("report_basis_freeze", {})
            if isinstance(control_state.get("report_basis_freeze"), dict)
            else {},
            "control_contexts": {
                "plan": plan_context,
                "controller": controller_context,
                "report_basis_gate": gate_context,
                "supervisor": supervisor_context,
            },
        }
        reporting_state = reporting_state_for_round(
            run_dir,
            resolved_run_id,
            selected_round_id,
        )
        governed_execution_state["operator"] = governed_execution_operator_view(
            run_dir,
            selected_round_id,
            governed_execution_state,
            reporting_state.get("surface", {})
            if isinstance(reporting_state.get("surface"), dict)
            else {},
        )
        post_round_state = {
            "round_close": load_json_if_exists(round_close_state_path(run_dir, selected_round_id)) or {},
            "history_bootstrap": load_json_if_exists(history_bootstrap_state_path(run_dir, selected_round_id)) or {},
        }
        post_round_state["operator"] = post_round_operator_view(run_dir, selected_round_id, post_round_state)
        benchmark_state = {
            "scenario_fixture": load_json_if_exists(scenario_fixture_path(run_dir, selected_round_id)) or {},
            "benchmark_manifest": load_json_if_exists(benchmark_manifest_path(run_dir, selected_round_id)) or {},
            "benchmark_compare": load_json_if_exists(benchmark_compare_path(run_dir, selected_round_id)) or {},
            "replay_report": load_json_if_exists(replay_report_path(run_dir, selected_round_id)) or {},
        }
        benchmark_state["operator"] = benchmark_operator_view(run_dir, selected_round_id, benchmark_state)
        transition_state = transition_request_state(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
        )
        round_liveness_state = build_round_liveness_surface(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
        )
    operations = operations_state(run_dir, selected_round_id)
    if not transition_state and selected_round_id:
        transition_state = transition_request_state(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
        )
    return {
        "status": "completed",
        "summary": {
            "run_dir": str(run_dir),
            "current_round_id": current_round_id,
            "selected_round_id": selected_round_id,
            "ledger_events": len(load_ledger_tail(run_dir, 1000000)) if ledger_path(run_dir).exists() else 0,
            "alert_status": maybe_text(operations.get("runtime_health", {}).get("alert_status")) if isinstance(operations.get("runtime_health"), dict) else "",
            "runtime_lock_state": maybe_text(operations.get("runtime_lock", {}).get("lock_state"))
            if isinstance(operations.get("runtime_lock"), dict)
            else "",
            "receipt_conflict_count": int(
                operations.get("runtime_health", {}).get("summary", {}).get("receipt_conflict_count")
                or 0
            )
            if isinstance(operations.get("runtime_health"), dict)
            else 0,
            "open_dead_letter_count": int(operations.get("runtime_health", {}).get("summary", {}).get("open_dead_letter_count") or 0)
            if isinstance(operations.get("runtime_health"), dict)
            else 0,
            "pending_transition_request_count": int(
                transition_state.get("summary", {}).get("pending_request_count") or 0
            )
            if isinstance(transition_state.get("summary"), dict)
            else 0,
            "pending_skill_approval_request_count": int(
                transition_state.get("summary", {}).get("pending_skill_approval_request_count")
                or 0
            )
            if isinstance(transition_state.get("summary"), dict)
            else 0,
        },
        "manifest": manifest,
        "cursor": cursor,
        "registry": registry,
        "operations": operations,
        "agent_entry": agent_entry_state(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            agent_entry_profile=agent_entry_profile,
            hard_gate_command_builder=hard_gate_command_builder,
        ),
        "round_liveness": round_liveness_state,
        "governed_execution": governed_execution_state,
        "reporting": reporting_state,
        "post_round": post_round_state,
        "benchmark": benchmark_state,
        "transitions": transition_state,
        "ledger_tail": load_ledger_tail(run_dir, tail),
    }
