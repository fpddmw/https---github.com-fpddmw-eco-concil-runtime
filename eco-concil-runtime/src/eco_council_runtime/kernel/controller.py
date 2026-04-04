from __future__ import annotations

from pathlib import Path
from typing import Any

from .deliberation_plane import load_phase2_control_state, store_promotion_freeze_record
from .executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill, utc_now_iso
from .gate import apply_promotion_gate
from .ledger import append_ledger_event
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists, write_json
from .paths import controller_state_path, ensure_runtime_dirs, orchestration_plan_path, promotion_gate_path
from .phase2_contract import expected_output_path as resolve_expected_output_path
from .phase2_contract import stage_contract, validate_skill_stage, validate_stage_sequence
from .registry import write_registry

PLANNER_SKILL_NAME = "eco-plan-round-orchestration"
STATIC_PHASE2_STAGES: list[tuple[str, str]] = [
    ("next-actions", "eco-propose-next-actions"),
    ("falsification-probes", "eco-open-falsification-probe"),
    ("round-readiness", "eco-summarize-round-readiness"),
]


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


def phase2_artifact_paths(run_dir: Path, round_id: str) -> dict[str, str]:
    return {
        "board_summary_path": str((run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()),
        "board_brief_path": str((run_dir / "board" / f"board_brief_{round_id}.md").resolve()),
        "next_actions_path": str((run_dir / "investigation" / f"next_actions_{round_id}.json").resolve()),
        "probes_path": str((run_dir / "investigation" / f"falsification_probes_{round_id}.json").resolve()),
        "readiness_path": str((run_dir / "reporting" / f"round_readiness_{round_id}.json").resolve()),
        "orchestration_plan_path": str(orchestration_plan_path(run_dir, round_id).resolve()),
        "promotion_gate_path": str(promotion_gate_path(run_dir, round_id).resolve()),
        "promotion_basis_path": str((run_dir / "promotion" / f"promoted_evidence_basis_{round_id}.json").resolve()),
        "controller_state_path": str(controller_state_path(run_dir, round_id).resolve()),
    }


def default_execution_queue() -> list[dict[str, Any]]:
    return [
        {
            "stage_name": stage_name,
            "skill_name": skill_name,
            "skill_args": [],
            "assigned_role_hint": "moderator",
            "reason": "Fallback static phase-2 step.",
        }
        for stage_name, skill_name in STATIC_PHASE2_STAGES
    ]


def default_post_gate_steps() -> list[dict[str, Any]]:
    return [
        {
            "stage_name": "promotion-basis",
            "skill_name": "eco-promote-evidence-basis",
            "skill_args": [],
            "assigned_role_hint": "moderator",
            "reason": "Fallback post-gate promotion basis write.",
        }
    ]


def normalized_planned_steps(entries: Any) -> list[dict[str, Any]]:
    if not isinstance(entries, list):
        return []
    results: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        skill_name = maybe_text(item.get("skill_name"))
        if not skill_name:
            continue
        raw_skill_args = item.get("skill_args", [])
        skill_args = [maybe_text(value) for value in raw_skill_args if maybe_text(value)] if isinstance(raw_skill_args, list) else []
        results.append(
            {
                "stage_name": maybe_text(item.get("stage_name") or item.get("stage") or skill_name),
                "skill_name": skill_name,
                "skill_args": skill_args,
                "assigned_role_hint": maybe_text(item.get("assigned_role_hint")),
                "reason": maybe_text(item.get("reason")),
                "expected_output_path": maybe_text(item.get("expected_output_path")),
            }
        )
    return results


def resolve_plan_path(run_dir: Path, round_id: str, plan_payload: dict[str, Any]) -> str:
    summary = plan_payload.get("summary", {}) if isinstance(plan_payload.get("summary"), dict) else {}
    output_path = maybe_text(summary.get("output_path")) or maybe_text(plan_payload.get("output_path"))
    if output_path:
        return output_path
    return str(orchestration_plan_path(run_dir, round_id).resolve())


def planning_bundle_from_payload(run_dir: Path, round_id: str, plan_path: str, plan_payload: dict[str, Any]) -> dict[str, Any]:
    explicit_execution_queue = normalized_planned_steps(plan_payload.get("execution_queue"))
    explicit_post_gate_steps = normalized_planned_steps(plan_payload.get("post_gate_steps"))
    execution_queue = explicit_execution_queue or default_execution_queue()
    post_gate_steps = explicit_post_gate_steps or default_post_gate_steps()
    fallback_path = plan_payload.get("fallback_path", []) if isinstance(plan_payload.get("fallback_path"), list) else []
    planning_mode = "planner-backed" if explicit_execution_queue else "fallback-static"
    return {
        "plan_id": maybe_text(plan_payload.get("plan_id")),
        "plan_path": plan_path,
        "planning_status": maybe_text(plan_payload.get("planning_status")) or "ready-for-controller",
        "planning_mode": planning_mode,
        "planner_skill_name": PLANNER_SKILL_NAME,
        "probe_stage_included": bool(plan_payload.get("probe_stage_included")),
        "assigned_role_hints": plan_payload.get("assigned_role_hints", []) if isinstance(plan_payload.get("assigned_role_hints"), list) else [],
        "execution_queue": execution_queue,
        "post_gate_steps": post_gate_steps,
        "stop_conditions": plan_payload.get("stop_conditions", []) if isinstance(plan_payload.get("stop_conditions"), list) else [],
        "fallback_path": fallback_path,
        "fallback_suggested_next_skills": unique_texts(
            [skill_name for row in fallback_path if isinstance(row, dict) for skill_name in row.get("suggested_next_skills", [])]
        ),
        "plan_payload": plan_payload,
    }


def planning_bundle(run_dir: Path, round_id: str, planner_result: dict[str, Any]) -> dict[str, Any]:
    planner_wrapper = planner_result.get("skill_payload", {}) if isinstance(planner_result.get("skill_payload"), dict) else {}
    plan_path = resolve_plan_path(run_dir, round_id, planner_wrapper)
    plan_payload = load_json_if_exists(Path(plan_path)) or {}
    return planning_bundle_from_payload(run_dir, round_id, plan_path, plan_payload)


def planning_from_controller(run_dir: Path, round_id: str, controller_payload: dict[str, Any]) -> dict[str, Any]:
    planning = controller_payload.get("planning", {}) if isinstance(controller_payload.get("planning"), dict) else {}
    execution_queue = normalized_planned_steps(planning.get("execution_queue"))
    post_gate_steps = normalized_planned_steps(planning.get("post_gate_steps"))
    if execution_queue or post_gate_steps:
        return {
            "plan_id": maybe_text(planning.get("plan_id")),
            "plan_path": maybe_text(planning.get("plan_path")) or str(orchestration_plan_path(run_dir, round_id).resolve()),
            "planning_status": maybe_text(planning.get("planning_status")) or "ready-for-controller",
            "planning_mode": maybe_text(controller_payload.get("planning_mode")) or maybe_text(planning.get("planning_mode")) or "planner-backed",
            "planner_skill_name": maybe_text(planning.get("planner_skill_name")) or PLANNER_SKILL_NAME,
            "probe_stage_included": bool(planning.get("probe_stage_included")),
            "assigned_role_hints": planning.get("assigned_role_hints", []) if isinstance(planning.get("assigned_role_hints"), list) else [],
            "execution_queue": execution_queue or default_execution_queue(),
            "post_gate_steps": post_gate_steps or default_post_gate_steps(),
            "stop_conditions": planning.get("stop_conditions", []) if isinstance(planning.get("stop_conditions"), list) else [],
            "fallback_path": planning.get("fallback_path", []) if isinstance(planning.get("fallback_path"), list) else [],
            "fallback_suggested_next_skills": unique_texts(
                [skill_name for row in planning.get("fallback_path", []) if isinstance(row, dict) for skill_name in row.get("suggested_next_skills", [])]
            ),
            "plan_payload": {},
        }
    plan_path = maybe_text(planning.get("plan_path")) or str(orchestration_plan_path(run_dir, round_id).resolve())
    plan_payload = load_json_if_exists(Path(plan_path)) or {}
    if not plan_payload:
        return {}
    return planning_bundle_from_payload(run_dir, round_id, plan_path, plan_payload)


def stage_blueprint(
    stage_name: str,
    *,
    skill_name: str = "",
    skill_args: list[str] | None = None,
    assigned_role_hint: str = "",
    planner_reason: str = "",
    artifacts: dict[str, Any],
    explicit_output_path: str = "",
) -> dict[str, Any]:
    contract = stage_contract(stage_name)
    expected_skill_name = maybe_text(contract.get("expected_skill_name")) or maybe_text(skill_name)
    return {
        "stage": stage_name,
        "phase_group": maybe_text(contract.get("phase_group")),
        "stage_kind": maybe_text(contract.get("stage_kind")) or "skill",
        "kind": maybe_text(contract.get("stage_kind")) or "skill",
        "skill_name": maybe_text(skill_name) or expected_skill_name,
        "expected_skill_name": expected_skill_name,
        "skill_args": skill_args or [],
        "assigned_role_hint": maybe_text(assigned_role_hint),
        "planner_reason": maybe_text(planner_reason),
        "required_previous_stages": [
            maybe_text(item) for item in contract.get("required_previous_stages", []) if maybe_text(item)
        ],
        "blocking": bool(contract.get("blocking")),
        "resume_policy": maybe_text(contract.get("resume_policy")) or "skip-if-completed",
        "operator_summary": maybe_text(contract.get("operator_summary")),
        "expected_output_path": resolve_expected_output_path(stage_name, artifacts, explicit_output_path),
    }


def stage_blueprints(planning: dict[str, Any], artifacts: dict[str, Any]) -> list[dict[str, Any]]:
    validate_skill_stage("orchestration-planner", PLANNER_SKILL_NAME)
    blueprints = [
        stage_blueprint(
            "orchestration-planner",
            skill_name=PLANNER_SKILL_NAME,
            artifacts=artifacts,
            explicit_output_path=artifacts.get("orchestration_plan_path", ""),
        )
    ]
    for planned_step in planning.get("execution_queue", []):
        stage_name = maybe_text(planned_step.get("stage_name"))
        skill_name = maybe_text(planned_step.get("skill_name"))
        validate_skill_stage(stage_name, skill_name)
        blueprints.append(
            stage_blueprint(
                stage_name,
                skill_name=skill_name,
                skill_args=planned_step.get("skill_args", []) if isinstance(planned_step.get("skill_args"), list) else [],
                assigned_role_hint=maybe_text(planned_step.get("assigned_role_hint")),
                planner_reason=maybe_text(planned_step.get("reason")),
                artifacts=artifacts,
                explicit_output_path=maybe_text(planned_step.get("expected_output_path")),
            )
        )
    blueprints.append(
        stage_blueprint(
            "promotion-gate",
            artifacts=artifacts,
            explicit_output_path=artifacts.get("promotion_gate_path", ""),
        )
    )
    for planned_step in planning.get("post_gate_steps", []):
        stage_name = maybe_text(planned_step.get("stage_name"))
        skill_name = maybe_text(planned_step.get("skill_name"))
        validate_skill_stage(stage_name, skill_name)
        blueprints.append(
            stage_blueprint(
                stage_name,
                skill_name=skill_name,
                skill_args=planned_step.get("skill_args", []) if isinstance(planned_step.get("skill_args"), list) else [],
                assigned_role_hint=maybe_text(planned_step.get("assigned_role_hint")),
                planner_reason=maybe_text(planned_step.get("reason")),
                artifacts=artifacts,
                explicit_output_path=maybe_text(planned_step.get("expected_output_path")),
            )
        )
    validate_stage_sequence([maybe_text(item.get("stage")) for item in blueprints])
    return blueprints


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
        }
    return contracts


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


def persist_controller_state(
    run_dir: Path,
    round_id: str,
    controller_payload: dict[str, Any],
    *,
    gate_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    controller_payload["artifacts"] = phase2_artifact_paths(run_dir, round_id)
    refreshed_payload = refresh_controller_payload(controller_payload)
    write_json(controller_state_path(run_dir, round_id), refreshed_payload)
    store_promotion_freeze_record(
        run_dir,
        run_id=maybe_text(refreshed_payload.get("run_id")),
        round_id=round_id,
        controller_snapshot=refreshed_payload,
        gate_snapshot=gate_payload,
        artifact_paths=refreshed_payload.get("artifacts", {})
        if isinstance(refreshed_payload.get("artifacts"), dict)
        else {},
    )
    return refreshed_payload


def stage_summary_from_result(stage_name: str, result: dict[str, Any], blueprint: dict[str, Any]) -> dict[str, Any]:
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
        "readiness_status": maybe_text(gate_payload.get("readiness_status")),
        "promote_allowed": bool(gate_payload.get("promote_allowed")),
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
        "promotion_status": "not-evaluated",
        "recommended_next_skills": [],
        "gate_reasons": [],
        "planning": {},
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
            "controller_path": artifacts.get("controller_state_path", ""),
            "planning_mode": controller_payload.get("planning_mode"),
            "plan_path": controller_payload.get("planning", {}).get("plan_path", "")
            if isinstance(controller_payload.get("planning"), dict)
            else "",
            "readiness_status": controller_payload.get("readiness_status"),
            "gate_status": controller_payload.get("gate_status"),
            "promotion_status": controller_payload.get("promotion_status"),
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
        "execution_policy": controller_payload.get("execution_policy", {}),
        "planning_mode": controller_payload.get("planning_mode"),
        "plan_id": planning.get("plan_id", ""),
        "plan_path": planning.get("plan_path", ""),
        "readiness_status": controller_payload.get("readiness_status"),
        "gate_status": controller_payload.get("gate_status"),
        "promotion_status": controller_payload.get("promotion_status"),
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


def run_phase2_round(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    return run_phase2_round_with_contract_mode(run_dir, run_id=run_id, round_id=round_id, contract_mode="warn")


def run_phase2_round_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
    force_restart: bool = False,
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    write_registry(run_dir)
    init_run_manifest(run_dir, run_id)
    init_round_cursor(run_dir, run_id)
    artifacts = phase2_artifact_paths(run_dir, round_id)
    execution_policy = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects or [],
    }
    execution_kwargs = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects,
    }

    existing_controller = load_json_if_exists(controller_state_path(run_dir, round_id)) or {}
    existing_gate = load_json_if_exists(Path(artifacts["promotion_gate_path"])) or {}
    phase2_control_state = (
        load_phase2_control_state(run_dir, run_id=run_id, round_id=round_id)
        if (not existing_controller or not existing_gate)
        else {}
    )
    if not existing_controller:
        existing_controller = (
            phase2_control_state.get("controller", {})
            if isinstance(phase2_control_state.get("controller"), dict)
            else {}
        )
    if not existing_gate:
        existing_gate = (
            phase2_control_state.get("promotion_gate", {})
            if isinstance(phase2_control_state.get("promotion_gate"), dict)
            else {}
        )
    existing_status = maybe_text(existing_controller.get("controller_status"))
    if not force_restart and existing_status == "completed":
        return controller_result_payload(existing_controller, existing_gate)

    started_at = utc_now_iso()
    resume_status = "restart-forced" if force_restart else "fresh-run"
    resume_count = 0
    planning: dict[str, Any] = {}
    blueprints: list[dict[str, Any]] = []
    controller_payload = base_controller_payload(
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
        execution_policy=execution_policy,
        artifacts=artifacts,
        started_at=started_at,
        resume_status=resume_status,
        resume_count=resume_count,
    )

    if not force_restart and existing_status in {"running", "failed"}:
        recovered_planning = planning_from_controller(run_dir, round_id, existing_controller)
        if recovered_planning:
            planning = recovered_planning
            blueprints = stage_blueprints(planning, artifacts)
            controller_payload = {
                **existing_controller,
                "schema_version": "runtime-controller-v3",
                "run_id": run_id,
                "round_id": round_id,
                "contract_mode": contract_mode,
                "execution_policy": execution_policy,
                "controller_status": "running",
                "resume_status": "resumed",
                "resume_count": int(existing_controller.get("resume_count") or 0) + 1,
                "started_at_utc": maybe_text(existing_controller.get("started_at_utc")) or started_at,
                "failure": {},
                "planning_mode": maybe_text(existing_controller.get("planning_mode")) or maybe_text(recovered_planning.get("planning_mode")) or "planner-backed",
                "planning": {
                    "plan_id": recovered_planning.get("plan_id", ""),
                    "plan_path": recovered_planning.get("plan_path", ""),
                    "planning_status": recovered_planning.get("planning_status", ""),
                    "planning_mode": recovered_planning.get("planning_mode", ""),
                    "planner_skill_name": recovered_planning.get("planner_skill_name", ""),
                    "probe_stage_included": recovered_planning.get("probe_stage_included", False),
                    "assigned_role_hints": recovered_planning.get("assigned_role_hints", []),
                    "planned_skill_count": len(recovered_planning.get("execution_queue", [])) + len(recovered_planning.get("post_gate_steps", [])),
                    "stop_conditions": recovered_planning.get("stop_conditions", []),
                    "fallback_path": recovered_planning.get("fallback_path", []),
                    "execution_queue": recovered_planning.get("execution_queue", []),
                    "post_gate_steps": recovered_planning.get("post_gate_steps", []),
                    "stage_sequence": [maybe_text(item.get("stage")) for item in blueprints],
                },
                "stage_contracts": stage_contracts_from_blueprints(blueprints),
                "steps": merge_existing_steps(blueprints, existing_controller.get("steps")),
                "artifacts": artifacts,
            }
            persist_controller_state(run_dir, round_id, controller_payload)

    planner_stage_ran = False
    planner_result: dict[str, Any] | None = None
    if not planning:
        planner_blueprint = stage_blueprint(
            "orchestration-planner",
            skill_name=PLANNER_SKILL_NAME,
            artifacts=artifacts,
            explicit_output_path=artifacts.get("orchestration_plan_path", ""),
        )
        controller_payload["steps"] = merge_existing_steps([planner_blueprint], [])
        controller_payload["stage_contracts"] = stage_contracts_from_blueprints([planner_blueprint])
        controller_payload["planning_mode"] = "planner-pending"
        controller_payload["controller_status"] = "running"
        planner_index = step_index(controller_payload["steps"], "orchestration-planner")
        controller_payload["steps"][planner_index]["status"] = "running"
        controller_payload["steps"][planner_index]["started_at_utc"] = started_at
        persist_controller_state(run_dir, round_id, controller_payload)
        try:
            planner_result = run_skill(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name=PLANNER_SKILL_NAME,
                skill_args=[],
                contract_mode=contract_mode,
                **execution_kwargs,
            )
            planner_stage_ran = True
        except SkillExecutionError as exc:
            controller_payload["controller_status"] = "failed"
            controller_payload["steps"][planner_index].update(
                {
                    "status": "failed",
                    "started_at_utc": maybe_text(controller_payload["steps"][planner_index].get("started_at_utc")) or started_at,
                    "completed_at_utc": utc_now_iso(),
                }
            )
            failure_payload = controller_failure_payload(
                run_id=run_id,
                round_id=round_id,
                contract_mode=contract_mode,
                stage_name="orchestration-planner",
                message=exc.payload.get("message", str(exc)),
                controller_payload=persist_controller_state(run_dir, round_id, controller_payload),
                stage_failure=exc.payload,
                retryable=bool(exc.payload.get("failure", {}).get("retryable")) if isinstance(exc.payload.get("failure"), dict) else False,
            )
            append_ledger_event(
                run_dir,
                round_controller_event(
                    run_id=run_id,
                    round_id=round_id,
                    started_at=started_at,
                    completed_at=utc_now_iso(),
                    contract_mode=contract_mode,
                    controller_payload=controller_payload,
                    status="failed",
                    failure=failure_payload.get("failure", {}),
                ),
            )
            raise SkillExecutionError(failure_payload["message"], failure_payload)

        planning = planning_bundle(run_dir, round_id, planner_result)
        blueprints = stage_blueprints(planning, artifacts)
        controller_payload["planning_mode"] = maybe_text(planning.get("planning_mode")) or "planner-backed"
        controller_payload["planning"] = {
            "plan_id": planning.get("plan_id", ""),
            "plan_path": planning.get("plan_path", ""),
            "planning_status": planning.get("planning_status", ""),
            "planning_mode": planning.get("planning_mode", ""),
            "planner_skill_name": planning.get("planner_skill_name", ""),
            "probe_stage_included": planning.get("probe_stage_included", False),
            "assigned_role_hints": planning.get("assigned_role_hints", []),
            "planned_skill_count": len(planning.get("execution_queue", [])) + len(planning.get("post_gate_steps", [])),
            "stop_conditions": planning.get("stop_conditions", []),
            "fallback_path": planning.get("fallback_path", []),
            "execution_queue": planning.get("execution_queue", []),
            "post_gate_steps": planning.get("post_gate_steps", []),
            "stage_sequence": [maybe_text(item.get("stage")) for item in blueprints],
        }
        controller_payload["stage_contracts"] = stage_contracts_from_blueprints(blueprints)
        controller_payload["steps"] = merge_existing_steps(blueprints, controller_payload.get("steps"))
        controller_payload["steps"][step_index(controller_payload["steps"], "orchestration-planner")] = stage_summary_from_result(
            "orchestration-planner",
            planner_result,
            blueprints[0],
        )
        persist_controller_state(run_dir, round_id, controller_payload)

    try:
        if not blueprints:
            blueprints = stage_blueprints(planning, artifacts)
            controller_payload["stage_contracts"] = stage_contracts_from_blueprints(blueprints)
            controller_payload["steps"] = merge_existing_steps(blueprints, controller_payload.get("steps"))
            persist_controller_state(run_dir, round_id, controller_payload)

        for blueprint in blueprints:
            stage_name = maybe_text(blueprint.get("stage"))
            if planner_stage_ran and stage_name == "orchestration-planner":
                continue
            step_pos = step_index(controller_payload["steps"], stage_name)
            existing_step = controller_payload["steps"][step_pos]
            if maybe_text(existing_step.get("status")) == "completed":
                continue
            controller_payload["controller_status"] = "running"
            controller_payload["failure"] = {}
            controller_payload["steps"][step_pos]["status"] = "running"
            controller_payload["steps"][step_pos]["started_at_utc"] = maybe_text(existing_step.get("started_at_utc")) or utc_now_iso()
            persist_controller_state(run_dir, round_id, controller_payload)

            if maybe_text(blueprint.get("stage_kind")) == "gate":
                gate_started_at = utc_now_iso()
                gate_payload = apply_promotion_gate(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    readiness_path_override=maybe_text(controller_payload["stage_contracts"].get("round-readiness", {}).get("expected_output_path"))
                    if isinstance(controller_payload.get("stage_contracts"), dict)
                    else "",
                    output_path_override=maybe_text(blueprint.get("expected_output_path")),
                )
                gate_event_id = new_runtime_event_id(
                    "runtimeevt",
                    run_id,
                    round_id,
                    "promotion-gate",
                    gate_started_at,
                    gate_payload.get("generated_at_utc"),
                    controller_payload.get("resume_status"),
                )
                append_ledger_event(
                    run_dir,
                    {
                        "schema_version": "runtime-event-v3",
                        "event_id": gate_event_id,
                        "event_type": "promotion-gate",
                        "run_id": run_id,
                        "round_id": round_id,
                        "started_at_utc": gate_started_at,
                        "completed_at_utc": gate_payload.get("generated_at_utc"),
                        "status": "completed",
                        "contract_mode": contract_mode,
                        "planning_mode": controller_payload.get("planning_mode"),
                        "plan_id": controller_payload.get("planning", {}).get("plan_id", "")
                        if isinstance(controller_payload.get("planning"), dict)
                        else "",
                        "plan_path": controller_payload.get("planning", {}).get("plan_path", "")
                        if isinstance(controller_payload.get("planning"), dict)
                        else "",
                        "gate_status": gate_payload.get("gate_status"),
                        "readiness_status": gate_payload.get("readiness_status"),
                        "promote_allowed": bool(gate_payload.get("promote_allowed")),
                        "gate_path": gate_payload.get("output_path"),
                    },
                )
                controller_payload["steps"][step_pos] = gate_stage_summary(blueprint, gate_payload, gate_event_id, gate_started_at)
                controller_payload["readiness_status"] = maybe_text(gate_payload.get("readiness_status")) or "blocked"
                controller_payload["gate_status"] = maybe_text(gate_payload.get("gate_status")) or "freeze-withheld"
                controller_payload["gate_reasons"] = gate_payload.get("gate_reasons", []) if isinstance(gate_payload.get("gate_reasons"), list) else []
                controller_payload["recommended_next_skills"] = (
                    gate_payload.get("recommended_next_skills", []) if isinstance(gate_payload.get("recommended_next_skills"), list) else []
                )
                persist_controller_state(run_dir, round_id, controller_payload, gate_payload=gate_payload)
                continue

            skill_result = run_skill(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name=maybe_text(blueprint.get("skill_name")),
                skill_args=blueprint.get("skill_args", []) if isinstance(blueprint.get("skill_args"), list) else [],
                contract_mode=contract_mode,
                **execution_kwargs,
            )
            controller_payload["steps"][step_pos] = stage_summary_from_result(stage_name, skill_result, blueprint)
            if stage_name == "round-readiness":
                readiness_summary = skill_result.get("skill_payload", {}) if isinstance(skill_result.get("skill_payload"), dict) else {}
                controller_payload["readiness_status"] = maybe_text(readiness_summary.get("summary", {}).get("readiness_status"))
            if stage_name == "promotion-basis":
                promotion_payload = skill_result.get("skill_payload", {}) if isinstance(skill_result.get("skill_payload"), dict) else {}
                promotion_summary = promotion_payload.get("summary", {}) if isinstance(promotion_payload.get("summary"), dict) else {}
                controller_payload["promotion_status"] = maybe_text(promotion_summary.get("promotion_status")) or "withheld"
            persist_controller_state(run_dir, round_id, controller_payload)
    except SkillExecutionError as exc:
        failed_stage = controller_payload.get("current_stage", "")
        if not failed_stage:
            failed_stage = controller_payload.get("recovery", {}).get("resume_from_stage", "") if isinstance(controller_payload.get("recovery"), dict) else ""
        if not failed_stage and isinstance(controller_payload.get("steps"), list):
            for step in controller_payload.get("steps", []):
                if maybe_text(step.get("status")) == "running":
                    failed_stage = maybe_text(step.get("stage"))
                    break
        if failed_stage:
            controller_payload["steps"][step_index(controller_payload["steps"], failed_stage)].update(
                {
                    "status": "failed",
                    "completed_at_utc": utc_now_iso(),
                }
            )
        controller_payload["controller_status"] = "failed"
        controller_payload["failure"] = exc.payload
        failure_payload = controller_failure_payload(
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            stage_name=failed_stage,
            message=exc.payload.get("message", str(exc)),
            controller_payload=persist_controller_state(run_dir, round_id, controller_payload),
            stage_failure=exc.payload,
            retryable=bool(exc.payload.get("failure", {}).get("retryable")) if isinstance(exc.payload.get("failure"), dict) else False,
        )
        append_ledger_event(
            run_dir,
            round_controller_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                contract_mode=contract_mode,
                controller_payload=controller_payload,
                status="failed",
                failure=failure_payload.get("failure", {}),
            ),
        )
        raise SkillExecutionError(failure_payload["message"], failure_payload)
    except Exception as exc:
        failed_stage = controller_payload.get("current_stage", "")
        if failed_stage and isinstance(controller_payload.get("steps"), list):
            controller_payload["steps"][step_index(controller_payload["steps"], failed_stage)].update(
                {
                    "status": "failed",
                    "completed_at_utc": utc_now_iso(),
                }
            )
        controller_payload["controller_status"] = "failed"
        controller_payload["failure"] = {"message": str(exc)}
        failure_payload = controller_failure_payload(
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            stage_name=failed_stage,
            message=str(exc),
            controller_payload=persist_controller_state(run_dir, round_id, controller_payload),
            stage_failure={"message": str(exc)},
            retryable=False,
        )
        append_ledger_event(
            run_dir,
            round_controller_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                contract_mode=contract_mode,
                controller_payload=controller_payload,
                status="failed",
                failure=failure_payload.get("failure", {}),
            ),
        )
        raise SkillExecutionError(failure_payload["message"], failure_payload)

    gate_payload = load_json_if_exists(Path(artifacts["promotion_gate_path"])) or (
        load_phase2_control_state(run_dir, run_id=run_id, round_id=round_id).get("promotion_gate", {})
    )
    controller_payload["controller_status"] = "completed"
    controller_payload["planning_mode"] = maybe_text(planning.get("planning_mode")) or maybe_text(controller_payload.get("planning_mode")) or "planner-backed"
    controller_payload["readiness_status"] = maybe_text(gate_payload.get("readiness_status")) or maybe_text(controller_payload.get("readiness_status")) or "blocked"
    controller_payload["gate_status"] = maybe_text(gate_payload.get("gate_status")) or maybe_text(controller_payload.get("gate_status")) or "freeze-withheld"
    if maybe_text(controller_payload.get("promotion_status")) in {"", "not-evaluated"}:
        promotion_basis_payload = load_json_if_exists(Path(artifacts["promotion_basis_path"])) or {}
        controller_payload["promotion_status"] = maybe_text(promotion_basis_payload.get("promotion_status")) or "withheld"
    if controller_payload["promotion_status"] == "promoted":
        controller_payload["recommended_next_skills"] = ["eco-materialize-reporting-handoff", "eco-draft-council-decision"]
    else:
        controller_payload["recommended_next_skills"] = unique_texts(
            (gate_payload.get("recommended_next_skills", []) if isinstance(gate_payload.get("recommended_next_skills"), list) else [])
            + (planning.get("fallback_suggested_next_skills", []) if isinstance(planning.get("fallback_suggested_next_skills"), list) else [])
        )
    controller_payload["gate_reasons"] = gate_payload.get("gate_reasons", []) if isinstance(gate_payload.get("gate_reasons"), list) else []
    persist_controller_state(run_dir, round_id, controller_payload, gate_payload=gate_payload)

    finished_at = utc_now_iso()
    append_ledger_event(
        run_dir,
        round_controller_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=finished_at,
            contract_mode=contract_mode,
            controller_payload=controller_payload,
            status="completed",
        ),
    )
    return controller_result_payload(controller_payload, gate_payload)
