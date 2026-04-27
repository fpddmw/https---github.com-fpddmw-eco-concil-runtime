from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .kernel.executor import maybe_text
from .kernel.transition_requests import TRANSITION_KIND_OPEN_INVESTIGATION_ROUND
from .runtime_command_hints import kernel_command, run_skill_command
from .phase2_round_profile import default_next_round_id_builder
from .reporting_status import SUPERVISOR_REPORTING_READY_STATUS

PostureProfileCallable = Callable[..., Any]


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


def resolve_phase2_posture_profile(
    posture_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(posture_profile, dict):
        raise ValueError("No phase-2 posture profile was injected into runtime kernel.")
    return posture_profile


def posture_profile_callable(
    posture_profile: dict[str, Any],
    key: str,
) -> PostureProfileCallable:
    candidate = posture_profile.get(key)
    if not callable(candidate):
        raise ValueError(f"Phase-2 posture profile is missing callable: {key}")
    return candidate


def default_controller_completion_updates(
    *,
    controller_payload: dict[str, Any],
    gate_payload: dict[str, Any],
    planning: dict[str, Any],
) -> dict[str, Any]:
    del planning
    promotion_status = maybe_text(controller_payload.get("promotion_status")) or "withheld"
    if promotion_status == "promoted":
        return {
            "recommended_next_skills": [
                "materialize-reporting-handoff",
                "draft-council-decision",
            ]
        }
    return {
        "recommended_next_skills": unique_texts(
            gate_payload.get("recommended_next_skills", [])
            if isinstance(gate_payload.get("recommended_next_skills"), list)
            else []
        )
    }


def default_supervisor_classification(controller: dict[str, Any]) -> dict[str, str]:
    controller_status = maybe_text(controller.get("controller_status")) or "completed"
    readiness_status = maybe_text(controller.get("readiness_status")) or "blocked"
    gate_status = maybe_text(controller.get("gate_status")) or "freeze-withheld"
    promotion_status = maybe_text(controller.get("promotion_status")) or "withheld"
    if controller_status == "failed":
        return {
            "supervisor_status": "controller-failed",
            "supervisor_substatus": "phase2-recovery-required",
            "phase2_posture": "controller-failed",
            "terminal_state": "recovery-required",
            "recovery_posture": "resume-controller",
            "operator_action": "resume-phase2",
        }
    if promotion_status == "promoted":
        return {
            "supervisor_status": SUPERVISOR_REPORTING_READY_STATUS,
            "supervisor_substatus": "promotion-complete",
            "phase2_posture": "reporting-ready",
            "terminal_state": "reporting-ready",
            "recovery_posture": "terminal",
            "operator_action": "handoff-reporting",
        }
    substatus = "promotion-withheld"
    if readiness_status == "blocked":
        substatus = "blocked-before-promotion"
    elif readiness_status == "needs-more-data":
        substatus = "investigation-open"
    elif gate_status == "freeze-withheld":
        substatus = "gate-withheld"
    return {
        "supervisor_status": "hold-investigation-open",
        "supervisor_substatus": substatus,
        "phase2_posture": "investigation-hold",
        "terminal_state": "investigation-open",
        "recovery_posture": "continue-investigation",
        "operator_action": "continue-investigation",
    }


def default_supervisor_top_actions(next_actions: dict[str, Any]) -> list[dict[str, str]]:
    ranked_actions = (
        next_actions.get("ranked_actions", [])
        if isinstance(next_actions.get("ranked_actions"), list)
        else []
    )
    results: list[dict[str, str]] = []
    for action in ranked_actions[:3]:
        if not isinstance(action, dict):
            continue
        results.append(
            {
                "action_id": maybe_text(action.get("action_id")),
                "action_kind": maybe_text(action.get("action_kind")),
                "assigned_role": maybe_text(action.get("assigned_role")),
                "priority": maybe_text(action.get("priority")),
                "objective": maybe_text(action.get("objective")),
            }
        )
    return results


def default_supervisor_round_transition(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    next_round_id: str,
    contract_mode: str,
    classification: dict[str, Any],
) -> dict[str, str]:
    if maybe_text(classification.get("supervisor_status")) != "hold-investigation-open":
        return {}
    return {
        "skill_name": "open-investigation-round",
        "transition_kind": TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
        "source_round_id": round_id,
        "suggested_round_id": next_round_id,
        "request_command": kernel_command(
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
            next_round_id,
            "--source-round-id",
            round_id,
            "--rationale",
            "<rationale>",
            actor_role="moderator",
        ),
        "approve_command": kernel_command(
            "approve-phase-transition",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--approval-reason",
            "<approval_reason>",
        ),
        "command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=next_round_id,
            skill_name="open-investigation-round",
            actor_role="moderator",
            contract_mode=contract_mode,
            skill_args=[
                "--source-round-id",
                round_id,
                "--transition-request-id",
                "<approved_request_id>",
            ],
        ),
    }


def default_supervisor_recommended_next_skills(
    *,
    controller: dict[str, Any],
    classification: dict[str, Any],
    round_transition: dict[str, Any],
) -> list[str]:
    implicit_skills = [
        maybe_text(round_transition.get("skill_name"))
    ] if maybe_text(classification.get("supervisor_status")) == "hold-investigation-open" else []
    return unique_texts(
        implicit_skills
        + (
            controller.get("recommended_next_skills", [])
            if isinstance(controller.get("recommended_next_skills"), list)
            else []
        )
    )


def default_supervisor_operator_notes(
    *,
    promotion_status: str,
    gate_status: str,
    gate_reasons: list[Any],
    top_action_rows: list[dict[str, str]],
    round_transition: dict[str, Any],
    reporting_ready: bool = False,
    reporting_blockers: list[Any] | None = None,
    reporting_handoff_status: str = "",
) -> list[str]:
    blockers = [
        maybe_text(item)
        for item in (reporting_blockers or [])
        if maybe_text(item)
    ]
    if reporting_ready:
        notes = [
            "The round is now explicitly ready for downstream reporting handoff.",
            "No blocking board or probe objects remain in the current controller snapshot.",
        ]
    else:
        hold_anchor = maybe_text(reporting_handoff_status) or gate_status or "investigation-open"
        notes = [f"Reporting remains held at {hold_anchor}."]
        notes.extend(blockers)
        notes.extend(maybe_text(reason) for reason in gate_reasons if maybe_text(reason))
        if promotion_status == "withheld" and "promotion-withheld" not in blockers:
            notes.append("Promotion is still withheld in the current controller snapshot.")
        if top_action_rows:
            top_action_kind = top_action_rows[0].get("action_kind") or "unspecified"
            notes.append(f"Highest-priority follow-up remains {top_action_kind}.")
    if round_transition:
        notes.append(
            f"Moderator should request {round_transition['suggested_round_id']} as an explicit follow-up round transition before operator approval and governed round creation."
        )
    return notes[:4]


def default_supervisor_failure_notes(controller: dict[str, Any]) -> list[str]:
    return [
        f"Controller failed at stage {maybe_text(controller.get('failed_stage')) or maybe_text(controller.get('current_stage')) or 'unknown'}."
    ]


def default_phase2_posture_profile() -> dict[str, Any]:
    return {
        "controller_completion_builder": default_controller_completion_updates,
        "supervisor_classification_builder": default_supervisor_classification,
        "supervisor_next_round_id_builder": default_next_round_id_builder,
        "supervisor_top_actions_builder": default_supervisor_top_actions,
        "supervisor_round_transition_builder": default_supervisor_round_transition,
        "supervisor_recommended_skills_builder": default_supervisor_recommended_next_skills,
        "supervisor_operator_notes_builder": default_supervisor_operator_notes,
        "supervisor_failure_notes_builder": default_supervisor_failure_notes,
    }


__all__ = [
    "default_controller_completion_updates",
    "default_phase2_posture_profile",
    "default_supervisor_classification",
    "default_supervisor_failure_notes",
    "default_supervisor_operator_notes",
    "default_supervisor_recommended_next_skills",
    "default_supervisor_round_transition",
    "default_supervisor_top_actions",
    "posture_profile_callable",
    "resolve_phase2_posture_profile",
]
