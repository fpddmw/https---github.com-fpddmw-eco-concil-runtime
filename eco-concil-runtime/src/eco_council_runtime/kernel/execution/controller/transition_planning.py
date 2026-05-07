from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.executor_common import (
    SkillExecutionError,
    maybe_text,
)
from eco_council_runtime.kernel.execution.runtime_stage_profile import (
    DEFAULT_RUNTIME_PLANNER_SKILL_NAME,
)
from eco_council_runtime.kernel.governance.role_contracts import ROLE_MODERATOR
from eco_council_runtime.kernel.governance.skill_registry import default_actor_role_hint
from eco_council_runtime.kernel.governance.transition_requests import (
    REQUEST_STATUS_APPROVED,
    REQUEST_STATUS_COMMITTED,
    REQUEST_STATUS_PENDING,
    TRANSITION_KIND_FREEZE_REPORT_BASIS,
    latest_transition_request,
)

PLANNER_SKILL_NAME = DEFAULT_RUNTIME_PLANNER_SKILL_NAME
TRANSITION_EXECUTOR_PLANNING_MODE = "transition-executor"
TRANSITION_EXECUTOR_AUTHORITY = "transition-executor"
TRANSITION_EXECUTOR_PLAN_SOURCE = "approved-transition-request"
TRANSITION_EXECUTOR_INSPECTION_SOURCE = "transition-request-inspection"
SKILL_TRANSITION_KIND_REQUIREMENTS = {
    "freeze-report-basis": TRANSITION_KIND_FREEZE_REPORT_BASIS,
}


def skill_actor_role_hint(skill_name: str, *, preferred_role_hint: Any = "") -> str:
    return (
        maybe_text(preferred_role_hint)
        or default_actor_role_hint(skill_name)
        or ROLE_MODERATOR
    )


def normalized_skill_args(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def existing_transition_request_id(skill_args: list[str]) -> str:
    for index, item in enumerate(skill_args):
        if maybe_text(item) != "--transition-request-id":
            continue
        if index + 1 >= len(skill_args):
            return ""
        return maybe_text(skill_args[index + 1])
    return ""


def transition_request_block_payload(
    *,
    run_id: str,
    round_id: str,
    stage_name: str,
    skill_name: str,
    transition_kind: str,
    latest_request: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_id = (
        maybe_text(latest_request.get("request_id"))
        if isinstance(latest_request, dict)
        else ""
    )
    request_status = (
        maybe_text(latest_request.get("request_status"))
        if isinstance(latest_request, dict)
        else ""
    )
    latest_reason = (
        maybe_text(latest_request.get("latest_decision_reason"))
        if isinstance(latest_request, dict)
        else ""
    )
    if request_id:
        message = (
            f"Stage `{stage_name}` cannot execute `{skill_name}` because the latest "
            f"`{transition_kind}` request `{request_id}` is `{request_status or 'unknown'}`."
        )
    else:
        message = (
            f"Stage `{stage_name}` cannot execute `{skill_name}` because no "
            f"`{transition_kind}` transition request exists for run `{run_id}` round `{round_id}`."
        )
    recovery_hints = [
        f"Moderator must request `{transition_kind}` for run `{run_id}` round `{round_id}` before this stage can execute.",
        "Runtime operator must approve that transition request before rerunning governed-execution supervision.",
    ]
    if request_id and request_status == REQUEST_STATUS_COMMITTED:
        recovery_hints = [
            f"Reuse the already committed request `{request_id}` by supplying `--transition-request-id` explicitly if this stage is being replayed intentionally."
        ]
    if latest_reason:
        recovery_hints.append(f"Latest operator reason: {latest_reason}")
    return {
        "status": "blocked",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "stage_name": stage_name,
            "skill_name": skill_name,
            "transition_kind": transition_kind,
            "transition_request_id": request_id,
            "transition_request_status": request_status,
        },
        "message": message,
        "failure": {
            "error_code": "missing-approved-transition-request",
            "retryable": False,
            "transition_kind": transition_kind,
            "transition_request_id": request_id,
            "transition_request_status": request_status,
            "recovery_hints": recovery_hints,
        },
        "transition_request": latest_request if isinstance(latest_request, dict) else {},
    }


def controller_stage_skill_args(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    stage_name: str,
    skill_name: str,
    skill_args: Any,
) -> list[str]:
    resolved_skill_args = normalized_skill_args(skill_args)
    if existing_transition_request_id(resolved_skill_args):
        return resolved_skill_args
    required_transition_kind = SKILL_TRANSITION_KIND_REQUIREMENTS.get(
        maybe_text(skill_name)
    )
    if not required_transition_kind:
        return resolved_skill_args
    latest_request = latest_transition_request(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind=required_transition_kind,
    )
    request_status = (
        maybe_text(latest_request.get("request_status"))
        if isinstance(latest_request, dict)
        else ""
    )
    if request_status not in {REQUEST_STATUS_APPROVED, REQUEST_STATUS_COMMITTED}:
        failure_payload = transition_request_block_payload(
            run_id=run_id,
            round_id=round_id,
            stage_name=stage_name,
            skill_name=skill_name,
            transition_kind=required_transition_kind,
            latest_request=latest_request,
        )
        raise SkillExecutionError(
            failure_payload["message"],
            failure_payload,
        )
    return [
        *resolved_skill_args,
        "--transition-request-id",
        maybe_text(latest_request.get("request_id")),
    ]


def transition_executor_plan_payload(
    *,
    run_id: str,
    round_id: str,
    request_payload: dict[str, Any],
    artifacts: dict[str, str],
) -> dict[str, Any]:
    request_id = maybe_text(request_payload.get("request_id"))
    return {
        "schema_version": "runtime-orchestration-plan-v1",
        "run_id": run_id,
        "round_id": round_id,
        "plan_id": f"transition-executor-{request_id or round_id}",
        "planning_status": "ready-for-controller",
        "planning_mode": TRANSITION_EXECUTOR_PLANNING_MODE,
        "controller_authority": TRANSITION_EXECUTOR_AUTHORITY,
        "plan_source": TRANSITION_EXECUTOR_PLAN_SOURCE,
        "include_planner_stage": False,
        "probe_stage_included": False,
        "assigned_role_hints": ["moderator", "runtime-operator"],
        "execution_queue": [],
        "gate_steps": [
            {
                "stage_name": "report-basis-gate",
                "stage_kind": "gate",
                "phase_group": "gate",
                "required_previous_stages": [],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Validate DB-backed readiness and council inputs before freezing report_basis.",
                "reason": "Execute the approved freeze-report-basis transition request through the governed runtime gate.",
                "gate_handler": "report-basis-gate",
                "readiness_stage_name": "round-readiness",
                "expected_output_path": maybe_text(artifacts.get("report_basis_gate_path"))
                or maybe_text(artifacts.get("report_basis_gate_path")),
            }
        ],
        "post_gate_steps": [
            {
                "stage_name": "report-basis-freeze",
                "stage_kind": "skill",
                "phase_group": "report-basis",
                "skill_name": "freeze-report-basis",
                "expected_skill_name": "freeze-report-basis",
                "skill_args": [],
                "assigned_role_hint": "moderator",
                "required_previous_stages": ["report-basis-gate"],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Freeze the frozen or withheld evidence basis after the approved transition clears the runtime gate.",
                "reason": "Commit the approved freeze-report-basis transition request.",
                "expected_output_path": maybe_text(artifacts.get("report_basis_freeze_path")),
            }
        ],
        "stop_conditions": [
            {
                "condition_id": "report-basis-gate-withheld",
                "trigger": "Report-basis gate does not allow basis freeze after DB-backed readiness / council validation.",
                "effect": "Freeze the basis as withheld and keep investigation open for moderator follow-up.",
            },
            {
                "condition_id": "report_basis-transition-approved",
                "trigger": "Approved freeze-report-basis transition request is present for the round.",
                "effect": "Controller executes gate plus basis freeze without re-planning next-actions or advisory queues.",
            },
        ],
        "fallback_path": [],
        "phase_decision_basis": {
            "transition_request_id": request_id,
            "transition_request_status": maybe_text(request_payload.get("request_status")),
            "transition_kind": maybe_text(request_payload.get("transition_kind")),
            "requested_by_role": maybe_text(request_payload.get("requested_by_role")),
            "latest_decision_status": maybe_text(request_payload.get("latest_decision_status")),
            "latest_decision_by_role": maybe_text(request_payload.get("latest_decision_by_role")),
            "basis_object_ids": (
                request_payload.get("basis_object_ids", [])
                if isinstance(request_payload.get("basis_object_ids"), list)
                else []
            ),
            "evidence_refs": (
                request_payload.get("evidence_refs", [])
                if isinstance(request_payload.get("evidence_refs"), list)
                else []
            ),
        },
        "planning_notes": [
            "Runtime controller executed the approved transition request directly.",
            "No planner or advisory source was used on the default kernel path.",
        ],
    }


def approved_transition_request_planning(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    artifacts: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    latest_request = latest_transition_request(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind=TRANSITION_KIND_FREEZE_REPORT_BASIS,
    )
    if not isinstance(latest_request, dict):
        return {}, None
    request_status = maybe_text(latest_request.get("request_status"))
    if request_status not in {REQUEST_STATUS_APPROVED, REQUEST_STATUS_COMMITTED}:
        return {}, latest_request
    plan_payload = transition_executor_plan_payload(
        run_id=run_id,
        round_id=round_id,
        request_payload=latest_request,
        artifacts=artifacts,
    )
    return {
        "plan_id": maybe_text(plan_payload.get("plan_id")),
        "plan_path": maybe_text(artifacts.get("orchestration_plan_path")),
        "planning_status": maybe_text(plan_payload.get("planning_status"))
        or "ready-for-controller",
        "planning_mode": TRANSITION_EXECUTOR_PLANNING_MODE,
        "planner_skill_name": "",
        "controller_authority": TRANSITION_EXECUTOR_AUTHORITY,
        "plan_source": TRANSITION_EXECUTOR_PLAN_SOURCE,
        "probe_stage_included": False,
        "include_planner_stage": False,
        "assigned_role_hints": ["moderator", "runtime-operator"],
        "execution_queue": [],
        "gate_steps": plan_payload.get("gate_steps", []),
        "post_gate_steps": plan_payload.get("post_gate_steps", []),
        "stop_conditions": plan_payload.get("stop_conditions", []),
        "fallback_path": [],
        "fallback_suggested_next_skills": [],
        "plan_payload": plan_payload,
        "transition_request": latest_request,
    }, latest_request


def inspection_only_planning(
    *,
    artifacts: dict[str, str],
) -> dict[str, Any]:
    return {
        "plan_id": "",
        "plan_path": maybe_text(artifacts.get("orchestration_plan_path")),
        "planning_status": "no-approved-transition-request",
        "planning_mode": TRANSITION_EXECUTOR_PLANNING_MODE,
        "planner_skill_name": "",
        "controller_authority": TRANSITION_EXECUTOR_AUTHORITY,
        "plan_source": TRANSITION_EXECUTOR_INSPECTION_SOURCE,
        "probe_stage_included": False,
        "include_planner_stage": False,
        "assigned_role_hints": ["moderator", "runtime-operator"],
        "execution_queue": [],
        "gate_steps": [],
        "post_gate_steps": [],
        "stop_conditions": [],
        "fallback_path": [],
        "fallback_suggested_next_skills": [],
        "plan_payload": {},
    }


__all__ = [
    "PLANNER_SKILL_NAME",
    "SKILL_TRANSITION_KIND_REQUIREMENTS",
    "TRANSITION_EXECUTOR_AUTHORITY",
    "TRANSITION_EXECUTOR_INSPECTION_SOURCE",
    "TRANSITION_EXECUTOR_PLAN_SOURCE",
    "TRANSITION_EXECUTOR_PLANNING_MODE",
    "approved_transition_request_planning",
    "controller_stage_skill_args",
    "existing_transition_request_id",
    "inspection_only_planning",
    "normalized_skill_args",
    "skill_actor_role_hint",
    "transition_executor_plan_payload",
    "transition_request_block_payload",
]
