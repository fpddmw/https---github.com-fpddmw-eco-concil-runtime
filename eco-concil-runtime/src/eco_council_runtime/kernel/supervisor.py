from __future__ import annotations

from pathlib import Path
from typing import Any

from ..phase2_posture_profile import (
    posture_profile_callable,
    resolve_phase2_posture_profile,
)
from .deliberation_plane import store_promotion_freeze_record
from .executor import SkillExecutionError
from .controller import run_phase2_round_with_contract_mode
from .executor import maybe_text, new_runtime_event_id, utc_now_iso
from .gate import GateHandler
from .ledger import append_ledger_event
from .manifest import load_json_if_exists, write_json
from .phase2_state_surfaces import load_next_actions_wrapper
from .paths import supervisor_state_path


def operator_commands(*, run_id: str, round_id: str, run_dir: Path) -> dict[str, str]:
    base = f"--run-dir {run_dir} --run-id {run_id} --round-id {round_id}"
    return {
        "resume_command": f"resume-phase2-round {base}",
        "restart_command": f"restart-phase2-round {base}",
        "inspect_command": f"show-run-state --run-dir {run_dir} --round-id {round_id} --tail 20",
    }


def supervise_round(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    gate_handlers: dict[str, GateHandler] | None = None,
    posture_profile: dict[str, Any] | None = None,
    planning_sources: list[dict[str, Any]] | None = None,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return supervise_round_with_contract_mode(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode="warn",
        gate_handlers=gate_handlers,
        posture_profile=posture_profile,
        planning_sources=planning_sources,
        stage_definitions=stage_definitions,
    )


def supervise_round_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    gate_handlers: dict[str, GateHandler] | None = None,
    posture_profile: dict[str, Any] | None = None,
    planning_sources: list[dict[str, Any]] | None = None,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
) -> dict[str, Any]:
    profile = resolve_phase2_posture_profile(posture_profile)
    classification_builder = posture_profile_callable(
        profile,
        "supervisor_classification_builder",
    )
    next_round_id_builder = posture_profile_callable(
        profile,
        "supervisor_next_round_id_builder",
    )
    top_actions_builder = posture_profile_callable(
        profile,
        "supervisor_top_actions_builder",
    )
    round_transition_builder = posture_profile_callable(
        profile,
        "supervisor_round_transition_builder",
    )
    recommended_skills_builder = posture_profile_callable(
        profile,
        "supervisor_recommended_skills_builder",
    )
    operator_notes_builder = posture_profile_callable(
        profile,
        "supervisor_operator_notes_builder",
    )
    failure_notes_builder = posture_profile_callable(
        profile,
        "supervisor_failure_notes_builder",
    )
    started_at = utc_now_iso()
    execution_policy = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects or [],
    }
    command_hints = operator_commands(run_id=run_id, round_id=round_id, run_dir=run_dir)
    try:
        controller_kwargs: dict[str, Any] = {
            "run_id": run_id,
            "round_id": round_id,
            "contract_mode": contract_mode,
            "gate_handlers": gate_handlers,
            "posture_profile": profile,
            "timeout_seconds": timeout_seconds,
            "retry_budget": retry_budget,
            "retry_backoff_ms": retry_backoff_ms,
            "allow_side_effects": allow_side_effects,
        }
        if planning_sources is not None:
            controller_kwargs["planning_sources"] = planning_sources
        if stage_definitions is not None:
            controller_kwargs["stage_definitions"] = stage_definitions
        controller_result = run_phase2_round_with_contract_mode(
            run_dir,
            **controller_kwargs,
        )
    except SkillExecutionError as exc:
        controller = exc.payload.get("controller", {}) if isinstance(exc.payload.get("controller"), dict) else {}
        artifacts = controller.get("artifacts", {}) if isinstance(controller.get("artifacts"), dict) else {}
        classification = classification_builder(controller or {"controller_status": "failed"})
        finished_at = utc_now_iso()
        payload = {
            "schema_version": "runtime-supervisor-v3",
            "generated_at_utc": finished_at,
            "run_id": run_id,
            "round_id": round_id,
            "supervisor_path": "",
            "supervisor_status": classification["supervisor_status"],
            "supervisor_substatus": classification["supervisor_substatus"],
            "phase2_posture": classification["phase2_posture"],
            "terminal_state": classification["terminal_state"],
            "recovery_posture": classification["recovery_posture"],
            "operator_action": classification["operator_action"],
            "controller_status": maybe_text(controller.get("controller_status")) or "failed",
            "resume_status": maybe_text(controller.get("resume_status")),
            "current_stage": maybe_text(controller.get("current_stage")),
            "failed_stage": maybe_text(controller.get("failed_stage")),
            "resume_recommended": bool(controller.get("resume_recommended")),
            "restart_recommended": bool(controller.get("restart_recommended")),
            "resume_from_stage": maybe_text(controller.get("recovery", {}).get("resume_from_stage"))
            if isinstance(controller.get("recovery"), dict)
            else "",
            "readiness_status": maybe_text(controller.get("readiness_status")) or "pending",
            "gate_status": maybe_text(controller.get("gate_status")) or "not-evaluated",
            "promotion_status": maybe_text(controller.get("promotion_status")) or "not-evaluated",
            "execution_policy": execution_policy,
            "planning_mode": maybe_text(controller.get("planning_mode")) or "planner-backed",
            "orchestration_plan_path": artifacts.get("orchestration_plan_path", ""),
            "controller_path": artifacts.get("controller_state_path", ""),
            "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
            "promotion_basis_path": artifacts.get("promotion_basis_path", ""),
            "recommended_next_skills": controller.get("recommended_next_skills", []),
            "round_transition": {},
            "top_actions": [],
            "operator_notes": failure_notes_builder(controller),
            "resume_command": command_hints["resume_command"],
            "restart_command": command_hints["restart_command"],
            "inspect_command": command_hints["inspect_command"],
            "inspection_paths": {
                "controller_path": artifacts.get("controller_state_path", ""),
                "plan_path": artifacts.get("orchestration_plan_path", ""),
                "gate_path": artifacts.get("promotion_gate_path", ""),
            },
        }
        output_file = supervisor_state_path(run_dir, round_id)
        payload["supervisor_path"] = str(output_file)
        write_json(output_file, payload)
        store_promotion_freeze_record(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            supervisor_snapshot=payload,
            artifact_paths={
                "controller_state_path": artifacts.get("controller_state_path", ""),
                "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
                "supervisor_state_path": str(output_file),
            },
        )
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "supervisor", started_at, finished_at, contract_mode, "failed"),
                "event_type": "supervisor",
                "run_id": run_id,
                "round_id": round_id,
                "started_at_utc": started_at,
                "completed_at_utc": finished_at,
                "status": "failed",
                "contract_mode": contract_mode,
                "execution_policy": execution_policy,
                "planning_mode": payload["planning_mode"],
                "supervisor_status": payload["supervisor_status"],
                "readiness_status": payload["readiness_status"],
                "gate_status": payload["gate_status"],
                "promotion_status": payload["promotion_status"],
                "supervisor_path": str(output_file),
            },
        )
        failure_payload = dict(exc.payload)
        failure_payload["supervisor"] = payload
        failure_payload["summary"] = {
            "run_id": run_id,
            "round_id": round_id,
            "supervisor_status": payload["supervisor_status"],
            "planning_mode": payload["planning_mode"],
            "supervisor_path": str(output_file),
            "promotion_status": payload["promotion_status"],
        }
        raise SkillExecutionError(failure_payload.get("message", str(exc)), failure_payload)
    controller = controller_result.get("controller", {}) if isinstance(controller_result.get("controller"), dict) else {}
    artifacts = controller.get("artifacts", {}) if isinstance(controller.get("artifacts"), dict) else {}
    next_actions_path = maybe_text(artifacts.get("next_actions_path"))
    next_actions_context = (
        load_next_actions_wrapper(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            next_actions_path=next_actions_path,
        )
        if next_actions_path
        else {"payload": {}}
    )
    next_actions = (
        next_actions_context.get("payload")
        if isinstance(next_actions_context.get("payload"), dict)
        else {}
    )
    next_actions = next_actions or {}
    top_action_rows = top_actions_builder(next_actions)
    gate_reasons = controller.get("gate_reasons", []) if isinstance(controller.get("gate_reasons"), list) else []
    promotion_status = maybe_text(controller.get("promotion_status")) or "withheld"
    gate_status = maybe_text(controller.get("gate_status")) or "freeze-withheld"
    classification = classification_builder(controller)
    round_transition = round_transition_builder(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        next_round_id=maybe_text(
            next_round_id_builder(
                run_dir=run_dir,
                current_round_id=round_id,
            )
        ),
        contract_mode=contract_mode,
        classification=classification,
    )
    recommended_next_skills = recommended_skills_builder(
        controller=controller,
        classification=classification,
        round_transition=round_transition,
    )
    resolved_operator_notes = operator_notes_builder(
        promotion_status=promotion_status,
        gate_status=gate_status,
        gate_reasons=gate_reasons,
        top_action_rows=top_action_rows,
        round_transition=round_transition,
    )
    finished_at = utc_now_iso()

    payload = {
        "schema_version": "runtime-supervisor-v3",
        "generated_at_utc": finished_at,
        "run_id": run_id,
        "round_id": round_id,
        "supervisor_path": "",
        "supervisor_status": classification["supervisor_status"],
        "supervisor_substatus": classification["supervisor_substatus"],
        "phase2_posture": classification["phase2_posture"],
        "terminal_state": classification["terminal_state"],
        "recovery_posture": classification["recovery_posture"],
        "operator_action": classification["operator_action"],
        "controller_status": maybe_text(controller.get("controller_status")) or "completed",
        "resume_status": maybe_text(controller.get("resume_status")),
        "current_stage": maybe_text(controller.get("current_stage")),
        "failed_stage": maybe_text(controller.get("failed_stage")),
        "resume_recommended": bool(controller.get("resume_recommended")),
        "restart_recommended": bool(controller.get("restart_recommended")),
        "resume_from_stage": maybe_text(controller.get("recovery", {}).get("resume_from_stage"))
        if isinstance(controller.get("recovery"), dict)
        else "",
        "readiness_status": maybe_text(controller.get("readiness_status")) or "blocked",
        "gate_status": gate_status,
        "promotion_status": promotion_status,
        "execution_policy": execution_policy,
        "planning_mode": maybe_text(controller.get("planning_mode")) or maybe_text(controller.get("planning", {}).get("planning_mode") if isinstance(controller.get("planning"), dict) else "") or "planner-backed",
        "orchestration_plan_path": artifacts.get("orchestration_plan_path", ""),
        "controller_path": artifacts.get("controller_state_path", ""),
        "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
        "promotion_basis_path": artifacts.get("promotion_basis_path", ""),
        "recommended_next_skills": recommended_next_skills,
        "round_transition": round_transition,
        "top_actions": top_action_rows,
        "operator_notes": resolved_operator_notes,
        "resume_command": command_hints["resume_command"],
        "restart_command": command_hints["restart_command"],
        "inspect_command": command_hints["inspect_command"],
        "inspection_paths": {
            "controller_path": artifacts.get("controller_state_path", ""),
            "plan_path": artifacts.get("orchestration_plan_path", ""),
            "gate_path": artifacts.get("promotion_gate_path", ""),
            "promotion_basis_path": artifacts.get("promotion_basis_path", ""),
        },
    }
    output_file = supervisor_state_path(run_dir, round_id)
    payload["supervisor_path"] = str(output_file)
    write_json(output_file, payload)
    store_promotion_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        supervisor_snapshot=payload,
        artifact_paths={
            "controller_state_path": artifacts.get("controller_state_path", ""),
            "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
            "supervisor_state_path": str(output_file),
        },
    )

    event_id = new_runtime_event_id("runtimeevt", run_id, round_id, "supervisor", started_at, finished_at, contract_mode)
    append_ledger_event(
        run_dir,
        {
            "schema_version": "runtime-event-v3",
            "event_id": event_id,
            "event_type": "supervisor",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "status": "completed",
            "contract_mode": contract_mode,
            "execution_policy": execution_policy,
            "planning_mode": payload["planning_mode"],
            "supervisor_status": payload["supervisor_status"],
            "readiness_status": payload["readiness_status"],
            "gate_status": gate_status,
            "promotion_status": promotion_status,
            "supervisor_path": str(output_file),
        },
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "supervisor_status": payload["supervisor_status"],
            "planning_mode": payload["planning_mode"],
            "supervisor_path": str(output_file),
            "promotion_status": promotion_status,
        },
        "supervisor": payload,
        "controller": controller,
    }
