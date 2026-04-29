from __future__ import annotations

from typing import Any

from .kernel.executor import maybe_text, new_runtime_event_id, utc_now_iso
from .phase2_planning_profile import (
    normalized_stage_list as normalized_stage_list_from_profile,
    planning_source_from_payload,
)
from .phase2_stage_profile import (
    DEFAULT_PHASE2_PLANNER_SKILL_NAME,
    default_gate_steps,
    default_post_gate_steps,
    expected_output_path as resolve_expected_output_path,
    lookup_stage_contract,
    validate_skill_stage,
    validate_stage_blueprints,
)


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


def normalized_stage_list(value: Any) -> list[str]:
    return normalized_stage_list_from_profile(value)


def planning_attempt_record(
    *,
    source: str,
    status: str,
    plan_path: str = "",
    planning_mode: str = "",
    controller_authority: str = "",
    receipt_id: str = "",
    event_id: str = "",
    message: str = "",
) -> dict[str, Any]:
    return {
        "recorded_at_utc": utc_now_iso(),
        "source": source,
        "status": status,
        "plan_path": plan_path,
        "planning_mode": planning_mode,
        "controller_authority": controller_authority,
        "receipt_id": receipt_id,
        "event_id": event_id,
        "message": message,
    }


def append_planning_attempt(controller_payload: dict[str, Any], attempt: dict[str, Any]) -> None:
    attempts = controller_payload.setdefault("planning_attempts", [])
    if isinstance(attempts, list):
        attempts.append(attempt)


def planning_includes_planner_stage(planning: dict[str, Any]) -> bool:
    explicit = planning.get("include_planner_stage")
    if isinstance(explicit, bool):
        return explicit
    return True


def stage_blueprint(
    stage_name: str,
    *,
    skill_name: str = "",
    skill_args: list[str] | None = None,
    assigned_role_hint: str = "",
    planner_reason: str = "",
    artifacts: dict[str, Any],
    explicit_output_path: str = "",
    planned_stage: dict[str, Any] | None = None,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    planned = planned_stage if isinstance(planned_stage, dict) else {}
    contract = lookup_stage_contract(
        stage_name,
        stage_definitions=stage_definitions,
    ) or {}
    has_explicit_previous_stages = "required_previous_stages" in planned
    required_previous_stages = (
        normalized_stage_list(planned.get("required_previous_stages"))
        if has_explicit_previous_stages
        else normalized_stage_list(contract.get("required_previous_stages"))
    )
    stage_kind = (
        maybe_text(planned.get("stage_kind") or planned.get("kind"))
        or maybe_text(contract.get("stage_kind"))
        or ("skill" if maybe_text(skill_name) or maybe_text(planned.get("skill_name")) else "gate")
    )
    resolved_skill_name = maybe_text(skill_name) or maybe_text(planned.get("skill_name"))
    expected_skill_name = (
        maybe_text(planned.get("expected_skill_name"))
        or maybe_text(contract.get("expected_skill_name"))
        or resolved_skill_name
    )
    if stage_kind == "skill":
        if contract:
            validate_skill_stage(
                stage_name,
                resolved_skill_name or expected_skill_name,
                stage_definitions=stage_definitions,
            )
        elif expected_skill_name and resolved_skill_name and expected_skill_name != resolved_skill_name:
            raise ValueError(
                f"Stage {stage_name} must execute {expected_skill_name}, but planner selected {resolved_skill_name}."
            )
        if not (resolved_skill_name or expected_skill_name):
            raise ValueError(f"Skill stage {stage_name} must declare a skill_name.")
    else:
        resolved_skill_name = ""
        expected_skill_name = ""
    return {
        "stage": stage_name,
        "phase_group": maybe_text(planned.get("phase_group")) or maybe_text(contract.get("phase_group")),
        "stage_kind": stage_kind,
        "kind": stage_kind,
        "skill_name": resolved_skill_name or expected_skill_name,
        "expected_skill_name": expected_skill_name,
        "skill_args": skill_args or [],
        "assigned_role_hint": maybe_text(assigned_role_hint) or maybe_text(planned.get("assigned_role_hint")),
        "planner_reason": maybe_text(planner_reason),
        "required_previous_stages": required_previous_stages,
        "blocking": planned.get("blocking") if isinstance(planned.get("blocking"), bool) else bool(contract.get("blocking")),
        "resume_policy": maybe_text(planned.get("resume_policy")) or maybe_text(contract.get("resume_policy")) or "skip-if-completed",
        "operator_summary": maybe_text(planned.get("operator_summary")) or maybe_text(contract.get("operator_summary")),
        "expected_output_path": resolve_expected_output_path(
            stage_name,
            artifacts,
            maybe_text(planned.get("expected_output_path")) or explicit_output_path,
            stage_definitions=stage_definitions,
        ),
        "gate_handler": maybe_text(planned.get("gate_handler")) or (stage_name if stage_kind == "gate" else ""),
        "readiness_stage_name": maybe_text(planned.get("readiness_stage_name")),
    }


def stage_blueprints(
    planning: dict[str, Any],
    artifacts: dict[str, Any],
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    blueprints: list[dict[str, Any]] = []
    if planning_includes_planner_stage(planning):
        validate_skill_stage(
            "orchestration-planner",
            planner_skill_name,
            stage_definitions=stage_definitions,
        )
        blueprints.append(
            stage_blueprint(
                "orchestration-planner",
                skill_name=planner_skill_name,
                artifacts=artifacts,
                explicit_output_path=artifacts.get("orchestration_plan_path", ""),
                stage_definitions=stage_definitions,
            )
        )
    for planned_step in planning.get("execution_queue", []):
        stage_name = maybe_text(planned_step.get("stage_name"))
        blueprints.append(
            stage_blueprint(
                stage_name,
                skill_name=maybe_text(planned_step.get("skill_name")),
                skill_args=planned_step.get("skill_args", []) if isinstance(planned_step.get("skill_args"), list) else [],
                assigned_role_hint=maybe_text(planned_step.get("assigned_role_hint")),
                planner_reason=maybe_text(planned_step.get("reason")),
                artifacts=artifacts,
                explicit_output_path=maybe_text(planned_step.get("expected_output_path")),
                planned_stage=planned_step,
                stage_definitions=stage_definitions,
            )
        )
    gate_steps = planning.get("gate_steps", []) if isinstance(planning.get("gate_steps"), list) else default_gate_steps()
    for planned_step in gate_steps:
        stage_name = maybe_text(planned_step.get("stage_name"))
        blueprints.append(
            stage_blueprint(
                stage_name,
                skill_name=maybe_text(planned_step.get("skill_name")),
                skill_args=planned_step.get("skill_args", []) if isinstance(planned_step.get("skill_args"), list) else [],
                assigned_role_hint=maybe_text(planned_step.get("assigned_role_hint")),
                planner_reason=maybe_text(planned_step.get("reason")),
                artifacts=artifacts,
                explicit_output_path=maybe_text(planned_step.get("expected_output_path")),
                planned_stage=planned_step,
                stage_definitions=stage_definitions,
            )
        )
    post_gate_steps = (
        planning.get("post_gate_steps", [])
        if isinstance(planning.get("post_gate_steps"), list)
        else default_post_gate_steps()
    )
    for planned_step in post_gate_steps:
        stage_name = maybe_text(planned_step.get("stage_name"))
        blueprints.append(
            stage_blueprint(
                stage_name,
                skill_name=maybe_text(planned_step.get("skill_name")),
                skill_args=planned_step.get("skill_args", []) if isinstance(planned_step.get("skill_args"), list) else [],
                assigned_role_hint=maybe_text(planned_step.get("assigned_role_hint")),
                planner_reason=maybe_text(planned_step.get("reason")),
                artifacts=artifacts,
                explicit_output_path=maybe_text(planned_step.get("expected_output_path")),
                planned_stage=planned_step,
                stage_definitions=stage_definitions,
            )
        )
    validate_stage_blueprints(
        blueprints,
        stage_definitions=stage_definitions,
    )
    return blueprints


def controller_planning_state(planning: dict[str, Any], blueprints: list[dict[str, Any]]) -> dict[str, Any]:
    execution_queue = planning.get("execution_queue", []) if isinstance(planning.get("execution_queue"), list) else []
    gate_steps = planning.get("gate_steps", []) if isinstance(planning.get("gate_steps"), list) else default_gate_steps()
    post_gate_steps = planning.get("post_gate_steps", []) if isinstance(planning.get("post_gate_steps"), list) else default_post_gate_steps()
    return {
        "plan_id": planning.get("plan_id", ""),
        "plan_path": planning.get("plan_path", ""),
        "planning_status": planning.get("planning_status", ""),
        "planning_mode": planning.get("planning_mode", ""),
        "planner_skill_name": planning.get("planner_skill_name", ""),
        "controller_authority": planning.get("controller_authority", ""),
        "plan_source": maybe_text(planning.get("plan_source")) or planning_source_from_payload(planning),
        "probe_stage_included": planning.get("probe_stage_included", False),
        "include_planner_stage": planning_includes_planner_stage(planning),
        "assigned_role_hints": planning.get("assigned_role_hints", []),
        "planned_skill_count": len(execution_queue) + len(post_gate_steps),
        "gate_step_count": len(gate_steps),
        "planned_stage_count": len(execution_queue) + len(gate_steps) + len(post_gate_steps),
        "stop_conditions": planning.get("stop_conditions", []),
        "fallback_path": planning.get("fallback_path", []),
        "execution_queue": execution_queue,
        "gate_steps": gate_steps,
        "post_gate_steps": post_gate_steps,
        "stage_sequence": [maybe_text(item.get("stage")) for item in blueprints],
    }


def stage_contracts_from_blueprints(blueprints: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for blueprint in blueprints:
        stage_name = maybe_text(blueprint.get("stage"))
        if not stage_name:
            continue
        contracts[stage_name] = {
            "stage": stage_name,
            "phase_group": maybe_text(blueprint.get("phase_group")),
            "stage_kind": maybe_text(blueprint.get("stage_kind")) or "skill",
            "expected_skill_name": maybe_text(blueprint.get("expected_skill_name")),
            "required_previous_stages": blueprint.get("required_previous_stages", []),
            "blocking": bool(blueprint.get("blocking")),
            "resume_policy": maybe_text(blueprint.get("resume_policy")),
            "operator_summary": maybe_text(blueprint.get("operator_summary")),
            "expected_output_path": maybe_text(blueprint.get("expected_output_path")),
            "gate_handler": maybe_text(blueprint.get("gate_handler")),
            "readiness_stage_name": maybe_text(blueprint.get("readiness_stage_name")),
        }
    return contracts


def adopted_planner_stage_summary(
    *,
    run_id: str,
    round_id: str,
    blueprint: dict[str, Any],
    planning: dict[str, Any],
    started_at: str,
    completed_at: str,
) -> dict[str, Any]:
    return {
        **blueprint,
        "status": "completed",
        "event_id": "",
        "receipt_id": "",
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "artifact_path": maybe_text(planning.get("plan_path")) or maybe_text(blueprint.get("expected_output_path")),
        "artifact_refs": [],
        "canonical_ids": unique_texts([maybe_text(planning.get("plan_id"))]),
        "payload_status": maybe_text(planning.get("planning_status")) or "ready-for-controller",
        "planning_mode": maybe_text(planning.get("planning_mode")),
        "controller_authority": maybe_text(planning.get("controller_authority")),
        "plan_source": maybe_text(planning.get("plan_source")) or planning_source_from_payload(planning),
        "run_id": run_id,
        "round_id": round_id,
    }


def stage_summary_from_result(stage_name: str, result: dict[str, Any], blueprint: dict[str, Any]) -> dict[str, Any]:
    del stage_name
    event = result.get("event", {}) if isinstance(result.get("event"), dict) else {}
    summary = result.get("summary", {}) if isinstance(result.get("summary"), dict) else {}
    skill_payload = result.get("skill_payload", {}) if isinstance(result.get("skill_payload"), dict) else {}
    payload_summary = skill_payload.get("summary", {}) if isinstance(skill_payload.get("summary"), dict) else {}
    return {
        **blueprint,
        "status": maybe_text(event.get("status")) or "completed",
        "event_id": maybe_text(summary.get("event_id")),
        "receipt_id": maybe_text(summary.get("receipt_id")),
        "started_at_utc": maybe_text(event.get("started_at_utc")),
        "completed_at_utc": maybe_text(event.get("completed_at_utc")) or utc_now_iso(),
        "artifact_path": maybe_text(payload_summary.get("output_path")) or maybe_text(blueprint.get("expected_output_path")),
        "artifact_refs": skill_payload.get("artifact_refs", []) if isinstance(skill_payload.get("artifact_refs"), list) else [],
        "canonical_ids": skill_payload.get("canonical_ids", []) if isinstance(skill_payload.get("canonical_ids"), list) else [],
        "payload_status": maybe_text(skill_payload.get("status")) or "completed",
        "attempt_count": summary.get("attempt_count"),
        "recovered_after_retry": bool(summary.get("recovered_after_retry")),
    }


def planner_stage_summary_from_result(
    result: dict[str, Any],
    blueprint: dict[str, Any],
    planning: dict[str, Any],
) -> dict[str, Any]:
    summary = stage_summary_from_result("orchestration-planner", result, blueprint)
    existing_canonical_ids = summary.get("canonical_ids", []) if isinstance(summary.get("canonical_ids"), list) else []
    summary["artifact_path"] = maybe_text(planning.get("plan_path")) or maybe_text(summary.get("artifact_path"))
    summary["canonical_ids"] = unique_texts(existing_canonical_ids + [maybe_text(planning.get("plan_id"))])
    summary["planning_mode"] = maybe_text(planning.get("planning_mode"))
    summary["controller_authority"] = maybe_text(planning.get("controller_authority"))
    summary["plan_source"] = maybe_text(planning.get("plan_source")) or planning_source_from_payload(planning)
    return summary


def merge_existing_steps(blueprints: list[dict[str, Any]], existing_steps: Any) -> list[dict[str, Any]]:
    existing_by_stage = {
        maybe_text(item.get("stage")): item
        for item in existing_steps
        if isinstance(existing_steps, list) and isinstance(item, dict) and maybe_text(item.get("stage"))
    }
    steps: list[dict[str, Any]] = []
    for blueprint in blueprints:
        stage_name = maybe_text(blueprint.get("stage"))
        step = {
            **blueprint,
            "status": "pending",
            "artifact_refs": [],
            "canonical_ids": [],
        }
        existing = existing_by_stage.get(stage_name, {})
        if isinstance(existing, dict):
            for key, value in existing.items():
                if key == "stage":
                    continue
                step[key] = value
            for key, value in blueprint.items():
                step[key] = value
        steps.append(step)
    return steps


def step_index(steps: list[dict[str, Any]], stage_name: str) -> int:
    for index, item in enumerate(steps):
        if maybe_text(item.get("stage")) == stage_name:
            return index
    raise ValueError(f"Missing phase-2 stage in controller state: {stage_name}")


def refresh_controller_payload(controller_payload: dict[str, Any]) -> dict[str, Any]:
    steps = controller_payload.get("steps", []) if isinstance(controller_payload.get("steps"), list) else []
    completed_stage_names: list[str] = []
    pending_stage_names: list[str] = []
    failed_stage = ""
    running_stage = ""
    for step in steps:
        stage_name = maybe_text(step.get("stage"))
        status = maybe_text(step.get("status")) or "pending"
        if status == "completed":
            completed_stage_names.append(stage_name)
        else:
            pending_stage_names.append(stage_name)
        if status == "running" and not running_stage:
            running_stage = stage_name
        if status == "failed" and not failed_stage:
            failed_stage = stage_name
    controller_status = maybe_text(controller_payload.get("controller_status")) or "running"
    if controller_status == "completed":
        current_stage = ""
    elif failed_stage:
        current_stage = failed_stage
    elif running_stage:
        current_stage = running_stage
    else:
        current_stage = pending_stage_names[0] if pending_stage_names else ""
    resume_from_stage = ""
    if controller_status in {"running", "failed"}:
        if failed_stage:
            resume_from_stage = failed_stage
        elif pending_stage_names:
            resume_from_stage = pending_stage_names[0]
    controller_payload["generated_at_utc"] = utc_now_iso()
    controller_payload["current_stage"] = current_stage
    controller_payload["completed_stage_names"] = completed_stage_names
    controller_payload["pending_stage_names"] = pending_stage_names
    controller_payload["failed_stage"] = failed_stage
    controller_payload["resume_recommended"] = bool(resume_from_stage) and controller_status != "completed"
    controller_payload["restart_recommended"] = controller_status == "failed" and not bool(resume_from_stage)
    controller_payload["progress"] = {
        "total_stage_count": len(steps),
        "completed_stage_count": len(completed_stage_names),
        "pending_stage_count": len(pending_stage_names),
        "failed_stage_count": 1 if failed_stage else 0,
    }
    controller_payload["recovery"] = {
        "can_resume": bool(resume_from_stage) and controller_status != "completed",
        "resume_from_stage": resume_from_stage,
        "last_completed_stage": completed_stage_names[-1] if completed_stage_names else "",
    }
    return controller_payload


def gate_stage_summary(blueprint: dict[str, Any], gate_payload: dict[str, Any], event_id: str, started_at: str) -> dict[str, Any]:
    return {
        **blueprint,
        "status": "completed",
        "event_id": event_id,
        "started_at_utc": started_at,
        "completed_at_utc": maybe_text(gate_payload.get("generated_at_utc")) or utc_now_iso(),
        "artifact_path": maybe_text(gate_payload.get("output_path")) or maybe_text(blueprint.get("expected_output_path")),
        "artifact_refs": [],
        "canonical_ids": [],
        "gate_status": maybe_text(gate_payload.get("gate_status")),
        "report_basis_gate_status": maybe_text(gate_payload.get("report_basis_gate_status")),
        "readiness_status": maybe_text(gate_payload.get("readiness_status")),
        "report_basis_freeze_allowed": bool(
            gate_payload.get("report_basis_freeze_allowed")
        ),
    }


def base_controller_payload(
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    execution_policy: dict[str, Any],
    artifacts: dict[str, Any],
    started_at: str,
    resume_status: str,
    resume_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-controller-v3",
        "generated_at_utc": started_at,
        "started_at_utc": started_at,
        "run_id": run_id,
        "round_id": round_id,
        "controller_status": "running",
        "contract_mode": contract_mode,
        "resume_status": resume_status,
        "resume_count": resume_count,
        "execution_policy": execution_policy,
        "planning_mode": "planner-pending",
        "readiness_status": "pending",
        "gate_status": "not-evaluated",
        "report_basis_status": "not-evaluated",
        "recommended_next_skills": [],
        "gate_reasons": [],
        "planning": {},
        "planning_attempts": [],
        "stage_contracts": {},
        "steps": [],
        "artifacts": artifacts,
        "failure": {},
    }


def controller_result_payload(controller_payload: dict[str, Any], gate_payload: dict[str, Any]) -> dict[str, Any]:
    artifacts = controller_payload.get("artifacts", {}) if isinstance(controller_payload.get("artifacts"), dict) else {}
    return {
        "status": "completed",
        "summary": {
            "run_id": controller_payload.get("run_id"),
            "round_id": controller_payload.get("round_id"),
            "requested_by_role": controller_payload.get("requested_by_role", ""),
            "controller_path": artifacts.get("controller_state_path", ""),
            "planning_mode": controller_payload.get("planning_mode"),
            "plan_source": controller_payload.get("planning", {}).get("plan_source", "")
            if isinstance(controller_payload.get("planning"), dict)
            else "",
            "plan_path": controller_payload.get("planning", {}).get("plan_path", "")
            if isinstance(controller_payload.get("planning"), dict)
            else "",
            "readiness_status": controller_payload.get("readiness_status"),
            "gate_status": controller_payload.get("gate_status"),
            "report_basis_status": controller_payload.get("report_basis_status"),
            "resume_status": controller_payload.get("resume_status"),
        },
        "controller": controller_payload,
        "gate": gate_payload,
    }


def round_controller_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    contract_mode: str,
    controller_payload: dict[str, Any],
    status: str,
    failure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    planning = controller_payload.get("planning", {}) if isinstance(controller_payload.get("planning"), dict) else {}
    event = {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "round-controller", started_at, completed_at, status),
        "event_type": "round-controller",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": status,
        "contract_mode": contract_mode,
        "requested_by_role": controller_payload.get("requested_by_role", ""),
        "execution_policy": controller_payload.get("execution_policy", {}),
        "planning_mode": controller_payload.get("planning_mode"),
        "plan_source": planning.get("plan_source", ""),
        "plan_id": planning.get("plan_id", ""),
        "plan_path": planning.get("plan_path", ""),
        "planning_attempt_count": len(controller_payload.get("planning_attempts", []))
        if isinstance(controller_payload.get("planning_attempts"), list)
        else 0,
        "readiness_status": controller_payload.get("readiness_status"),
        "gate_status": controller_payload.get("gate_status"),
        "report_basis_status": controller_payload.get("report_basis_status"),
        "resume_status": controller_payload.get("resume_status"),
        "failed_stage": controller_payload.get("failed_stage", ""),
        "controller_path": controller_payload.get("artifacts", {}).get("controller_state_path", "")
        if isinstance(controller_payload.get("artifacts"), dict)
        else "",
        "step_count": len(controller_payload.get("steps", [])) if isinstance(controller_payload.get("steps"), list) else 0,
    }
    if isinstance(failure, dict) and failure:
        event["failure"] = failure
    return event


def controller_failure_payload(
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    stage_name: str,
    message: str,
    controller_payload: dict[str, Any],
    stage_failure: dict[str, Any] | None = None,
    retryable: bool = False,
) -> dict[str, Any]:
    recovery_hints = []
    if stage_name:
        recovery_hints.append(f"Resume the round from {stage_name} after resolving the underlying issue.")
    recovery_hints.append("Inspect the controller artifact and runtime ledger before deciding whether to restart the round.")
    failure = {
        "error_code": "controller-stage-failed" if stage_name else "controller-execution-failed",
        "message": message,
        "retryable": retryable,
        "stage_name": stage_name,
        "recovery_hints": recovery_hints,
    }
    if isinstance(stage_failure, dict) and stage_failure:
        failure["stage_failure"] = stage_failure
    return {
        "status": "failed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "contract_mode": contract_mode,
            "controller_path": controller_payload.get("artifacts", {}).get("controller_state_path", "")
            if isinstance(controller_payload.get("artifacts"), dict)
            else "",
            "failed_stage": stage_name,
            "resume_status": controller_payload.get("resume_status", ""),
        },
        "message": message,
        "failure": failure,
        "controller": controller_payload,
    }


__all__ = [
    "append_planning_attempt",
    "base_controller_payload",
    "controller_failure_payload",
    "controller_planning_state",
    "controller_result_payload",
    "gate_stage_summary",
    "merge_existing_steps",
    "planning_attempt_record",
    "planner_stage_summary_from_result",
    "refresh_controller_payload",
    "round_controller_event",
    "stage_blueprint",
    "stage_blueprints",
    "stage_contracts_from_blueprints",
    "stage_summary_from_result",
    "step_index",
    "adopted_planner_stage_summary",
]
