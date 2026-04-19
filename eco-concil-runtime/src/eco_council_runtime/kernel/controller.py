from __future__ import annotations

from pathlib import Path
from typing import Any

from ..phase2_direct_advisory import materialize_direct_council_advisory_plan
from .deliberation_plane import load_phase2_control_state, store_promotion_freeze_record
from .executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill, utc_now_iso
from .gate import execute_gate_step as execute_runtime_gate_step
from .ledger import append_ledger_event
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists, write_json
from .paths import (
    agent_advisory_plan_path,
    agent_entry_gate_path,
    controller_state_path,
    ensure_runtime_dirs,
    mission_scaffold_path,
    orchestration_plan_path,
    promotion_gate_path,
)
from .phase2_contract import expected_output_path as resolve_expected_output_path
from .phase2_contract import lookup_stage_contract, validate_skill_stage, validate_stage_blueprints
from .registry import write_registry

PLANNER_SKILL_NAME = "eco-plan-round-orchestration"


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
        "agent_advisory_plan_path": str(agent_advisory_plan_path(run_dir, round_id).resolve()),
        "agent_entry_gate_path": str(agent_entry_gate_path(run_dir, round_id).resolve()),
        "board_summary_path": str((run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()),
        "board_brief_path": str((run_dir / "board" / f"board_brief_{round_id}.md").resolve()),
        "mission_scaffold_path": str(mission_scaffold_path(run_dir, round_id).resolve()),
        "next_actions_path": str((run_dir / "investigation" / f"next_actions_{round_id}.json").resolve()),
        "probes_path": str((run_dir / "investigation" / f"falsification_probes_{round_id}.json").resolve()),
        "readiness_path": str((run_dir / "reporting" / f"round_readiness_{round_id}.json").resolve()),
        "orchestration_plan_path": str(orchestration_plan_path(run_dir, round_id).resolve()),
        "promotion_gate_path": str(promotion_gate_path(run_dir, round_id).resolve()),
        "promotion_basis_path": str((run_dir / "promotion" / f"promoted_evidence_basis_{round_id}.json").resolve()),
        "controller_state_path": str(controller_state_path(run_dir, round_id).resolve()),
    }


def normalized_stage_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [maybe_text(item) for item in value if maybe_text(item)]


def default_gate_steps() -> list[dict[str, Any]]:
    return [
        {
            "stage_name": "promotion-gate",
            "stage_kind": "gate",
            "phase_group": "gate",
            "required_previous_stages": ["round-readiness"],
            "blocking": True,
            "resume_policy": "skip-if-completed",
            "operator_summary": "Evaluate whether the current round can move into promotion and reporting.",
            "reason": "Fallback runtime promotion gate evaluation.",
            "gate_handler": "promotion-gate",
            "readiness_stage_name": "round-readiness",
        }
    ]


def default_post_gate_steps() -> list[dict[str, Any]]:
    return [
        {
            "stage_name": "promotion-basis",
            "stage_kind": "skill",
            "phase_group": "promotion",
            "skill_name": "eco-promote-evidence-basis",
            "expected_skill_name": "eco-promote-evidence-basis",
            "skill_args": [],
            "assigned_role_hint": "moderator",
            "required_previous_stages": ["promotion-gate"],
            "blocking": True,
            "resume_policy": "skip-if-completed",
            "operator_summary": "Freeze the promoted or withheld evidence basis after gate evaluation.",
            "reason": "Fallback post-gate promotion basis write.",
        }
    ]


def normalized_planned_steps(entries: Any, *, default_stage_kind: str = "skill") -> list[dict[str, Any]]:
    if not isinstance(entries, list):
        return []
    results: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        stage_kind = maybe_text(item.get("stage_kind") or item.get("kind")) or default_stage_kind
        skill_name = maybe_text(item.get("skill_name"))
        stage_name = maybe_text(item.get("stage_name") or item.get("stage") or skill_name or item.get("gate_handler"))
        if not stage_name:
            continue
        if stage_kind == "skill" and not skill_name:
            continue
        raw_skill_args = item.get("skill_args", [])
        skill_args = [maybe_text(value) for value in raw_skill_args if maybe_text(value)] if isinstance(raw_skill_args, list) else []
        results.append(
            {
                "stage_name": stage_name,
                "stage_kind": stage_kind,
                "phase_group": maybe_text(item.get("phase_group")),
                "skill_name": skill_name,
                "expected_skill_name": maybe_text(item.get("expected_skill_name")),
                "skill_args": skill_args,
                "assigned_role_hint": maybe_text(item.get("assigned_role_hint")),
                "reason": maybe_text(item.get("reason")),
                "expected_output_path": maybe_text(item.get("expected_output_path")),
                "required_previous_stages": normalized_stage_list(item.get("required_previous_stages")),
                "blocking": item.get("blocking") if isinstance(item.get("blocking"), bool) else None,
                "resume_policy": maybe_text(item.get("resume_policy")),
                "operator_summary": maybe_text(item.get("operator_summary")),
                "gate_handler": maybe_text(item.get("gate_handler")),
                "readiness_stage_name": maybe_text(item.get("readiness_stage_name")),
            }
        )
    return results


def resolve_plan_path(run_dir: Path, round_id: str, plan_payload: dict[str, Any]) -> str:
    summary = plan_payload.get("summary", {}) if isinstance(plan_payload.get("summary"), dict) else {}
    output_path = maybe_text(summary.get("output_path")) or maybe_text(plan_payload.get("output_path"))
    if output_path:
        return output_path
    return str(orchestration_plan_path(run_dir, round_id).resolve())


def normalized_controller_planning_mode(value: Any, *, default: str = "planner-backed") -> str:
    mode = maybe_text(value)
    if mode == "agent-advisory":
        return mode
    if mode == "planner-pending":
        return mode
    if mode:
        return "planner-backed"
    return default


def planning_source_from_payload(plan_payload: dict[str, Any]) -> str:
    explicit_source = maybe_text(plan_payload.get("plan_source"))
    if explicit_source:
        return explicit_source
    planning_mode = maybe_text(plan_payload.get("planning_mode"))
    controller_authority = maybe_text(plan_payload.get("controller_authority"))
    if planning_mode == "agent-advisory" or controller_authority == "advisory-only":
        return "agent-advisory"
    return "runtime-planner"


def relative_runtime_path(run_dir: Path, path: Path) -> str:
    try:
        return str(path.relative_to(run_dir))
    except ValueError:
        return str(path)


def agent_orchestration_requested(run_dir: Path, round_id: str) -> bool:
    mission_payload = load_json_if_exists(mission_scaffold_path(run_dir, round_id)) or {}
    if maybe_text(mission_payload.get("orchestration_mode")) == "openclaw-agent":
        return True
    entry_gate_payload = load_json_if_exists(agent_entry_gate_path(run_dir, round_id)) or {}
    return maybe_text(entry_gate_payload.get("orchestration_mode")) in {"openclaw-agent", "openclaw-agent-compatible"}


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


def planning_bundle_from_payload(run_dir: Path, round_id: str, plan_path: str, plan_payload: dict[str, Any]) -> dict[str, Any]:
    explicit_execution_queue = normalized_planned_steps(plan_payload.get("execution_queue"))
    explicit_gate_steps = normalized_planned_steps(plan_payload.get("gate_steps"), default_stage_kind="gate")
    explicit_post_gate_steps = normalized_planned_steps(plan_payload.get("post_gate_steps"))
    gate_steps = explicit_gate_steps or default_gate_steps()
    post_gate_steps = explicit_post_gate_steps or default_post_gate_steps()
    fallback_path = plan_payload.get("fallback_path", []) if isinstance(plan_payload.get("fallback_path"), list) else []
    return {
        "plan_id": maybe_text(plan_payload.get("plan_id")),
        "plan_path": plan_path,
        "planning_status": maybe_text(plan_payload.get("planning_status")) or "ready-for-controller",
        "planning_mode": normalized_controller_planning_mode(
            plan_payload.get("planning_mode")
        ),
        "planner_skill_name": PLANNER_SKILL_NAME,
        "controller_authority": maybe_text(plan_payload.get("controller_authority")) or "queue-owner",
        "plan_source": planning_source_from_payload(plan_payload),
        "probe_stage_included": bool(plan_payload.get("probe_stage_included")),
        "assigned_role_hints": plan_payload.get("assigned_role_hints", []) if isinstance(plan_payload.get("assigned_role_hints"), list) else [],
        "execution_queue": explicit_execution_queue,
        "gate_steps": gate_steps,
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


def advisory_planning_bundle(run_dir: Path, round_id: str) -> dict[str, Any]:
    advisory_path = agent_advisory_plan_path(run_dir, round_id)
    advisory_payload = load_json_if_exists(advisory_path) or {}
    if not advisory_payload:
        return {}
    if (
        maybe_text(advisory_payload.get("planning_mode")) not in {"agent-advisory", ""}
        and maybe_text(advisory_payload.get("controller_authority")) != "advisory-only"
    ):
        return {}
    execution_queue = normalized_planned_steps(advisory_payload.get("execution_queue"))
    if not execution_queue:
        return {}
    planning = planning_bundle_from_payload(run_dir, round_id, str(advisory_path.resolve()), advisory_payload)
    planning["planning_mode"] = maybe_text(advisory_payload.get("planning_mode")) or "agent-advisory"
    planning["controller_authority"] = maybe_text(advisory_payload.get("controller_authority")) or "advisory-only"
    planning["plan_source"] = maybe_text(advisory_payload.get("plan_source")) or "agent-advisory"
    return planning


def planning_from_controller(run_dir: Path, round_id: str, controller_payload: dict[str, Any]) -> dict[str, Any]:
    planning = controller_payload.get("planning", {}) if isinstance(controller_payload.get("planning"), dict) else {}
    execution_queue = normalized_planned_steps(planning.get("execution_queue"))
    gate_steps = normalized_planned_steps(planning.get("gate_steps"), default_stage_kind="gate")
    post_gate_steps = normalized_planned_steps(planning.get("post_gate_steps"))
    if execution_queue:
        return {
            "plan_id": maybe_text(planning.get("plan_id")),
            "plan_path": maybe_text(planning.get("plan_path")) or str(orchestration_plan_path(run_dir, round_id).resolve()),
            "planning_status": maybe_text(planning.get("planning_status")) or "ready-for-controller",
            "planning_mode": normalized_controller_planning_mode(
                maybe_text(controller_payload.get("planning_mode"))
                or maybe_text(planning.get("planning_mode"))
            ),
            "planner_skill_name": maybe_text(planning.get("planner_skill_name")) or PLANNER_SKILL_NAME,
            "controller_authority": maybe_text(planning.get("controller_authority")) or "queue-owner",
            "plan_source": maybe_text(planning.get("plan_source")) or "controller-snapshot",
            "probe_stage_included": bool(planning.get("probe_stage_included")),
            "assigned_role_hints": planning.get("assigned_role_hints", []) if isinstance(planning.get("assigned_role_hints"), list) else [],
            "execution_queue": execution_queue,
            "gate_steps": gate_steps or default_gate_steps(),
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


def ensure_executable_planning(planning: dict[str, Any]) -> None:
    execution_queue = planning.get("execution_queue", []) if isinstance(planning.get("execution_queue"), list) else []
    if execution_queue:
        return
    plan_path = maybe_text(planning.get("plan_path")) or "<unknown>"
    raise ValueError(
        f"Planning artifact {plan_path} does not define an execution_queue; runtime kernel will not synthesize phase-2 deliberation stages."
    )


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
) -> dict[str, Any]:
    planned = planned_stage if isinstance(planned_stage, dict) else {}
    contract = lookup_stage_contract(stage_name) or {}
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
            validate_skill_stage(stage_name, resolved_skill_name or expected_skill_name)
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
        "required_previous_stages": normalized_stage_list(planned.get("required_previous_stages"))
        or normalized_stage_list(contract.get("required_previous_stages")),
        "blocking": planned.get("blocking") if isinstance(planned.get("blocking"), bool) else bool(contract.get("blocking")),
        "resume_policy": maybe_text(planned.get("resume_policy")) or maybe_text(contract.get("resume_policy")) or "skip-if-completed",
        "operator_summary": maybe_text(planned.get("operator_summary")) or maybe_text(contract.get("operator_summary")),
        "expected_output_path": resolve_expected_output_path(
            stage_name,
            artifacts,
            maybe_text(planned.get("expected_output_path")) or explicit_output_path,
        ),
        "gate_handler": maybe_text(planned.get("gate_handler")) or (stage_name if stage_kind == "gate" else ""),
        "readiness_stage_name": maybe_text(planned.get("readiness_stage_name")),
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
            )
        )
    validate_stage_blueprints(blueprints)
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


def execute_gate_step(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    blueprint: dict[str, Any],
    stage_contracts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return execute_runtime_gate_step(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        blueprint=blueprint,
        stage_contracts=stage_contracts,
    )


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
        "plan_source": planning.get("plan_source", ""),
        "plan_id": planning.get("plan_id", ""),
        "plan_path": planning.get("plan_path", ""),
        "planning_attempt_count": len(controller_payload.get("planning_attempts", []))
        if isinstance(controller_payload.get("planning_attempts"), list)
        else 0,
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

    phase2_control_state = load_phase2_control_state(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    existing_controller = (
        phase2_control_state.get("controller", {})
        if isinstance(phase2_control_state.get("controller"), dict)
        else {}
    ) or load_json_if_exists(controller_state_path(run_dir, round_id)) or {}
    existing_gate = (
        phase2_control_state.get("promotion_gate", {})
        if isinstance(phase2_control_state.get("promotion_gate"), dict)
        else {}
    ) or load_json_if_exists(Path(artifacts["promotion_gate_path"])) or {}
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
            ensure_executable_planning(recovered_planning)
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
                "planning_mode": normalized_controller_planning_mode(
                    maybe_text(existing_controller.get("planning_mode"))
                    or maybe_text(recovered_planning.get("planning_mode"))
                ),
                "planning": controller_planning_state(recovered_planning, blueprints),
                "stage_contracts": stage_contracts_from_blueprints(blueprints),
                "steps": merge_existing_steps(blueprints, existing_controller.get("steps")),
                "artifacts": artifacts,
            }
            persist_controller_state(run_dir, round_id, controller_payload)

    planner_stage_ran = False
    planner_result: dict[str, Any] | None = None
    if not planning:
        advisory_planning = advisory_planning_bundle(run_dir, round_id)
        if advisory_planning:
            ensure_executable_planning(advisory_planning)
            planning = advisory_planning
            blueprints = stage_blueprints(planning, artifacts)
            adopted_plan_source = maybe_text(planning.get("plan_source")) or "agent-advisory"
            controller_payload["planning_mode"] = maybe_text(planning.get("planning_mode")) or "agent-advisory"
            controller_payload["planning"] = controller_planning_state(planning, blueprints)
            controller_payload["stage_contracts"] = stage_contracts_from_blueprints(blueprints)
            controller_payload["steps"] = merge_existing_steps(blueprints, controller_payload.get("steps"))
            adopted_started_at = started_at
            adopted_completed_at = utc_now_iso()
            controller_payload["steps"][step_index(controller_payload["steps"], "orchestration-planner")] = adopted_planner_stage_summary(
                run_id=run_id,
                round_id=round_id,
                blueprint=blueprints[0],
                planning=planning,
                started_at=adopted_started_at,
                completed_at=adopted_completed_at,
            )
            append_planning_attempt(
                controller_payload,
                planning_attempt_record(
                    source=adopted_plan_source,
                    status="adopted",
                    plan_path=maybe_text(planning.get("plan_path")),
                    planning_mode=maybe_text(planning.get("planning_mode")),
                    controller_authority=maybe_text(planning.get("controller_authority")),
                    message=(
                        "Controller adopted a pre-materialized direct council advisory plan."
                        if adopted_plan_source == "direct-council-advisory"
                        else "Controller adopted a pre-materialized advisory plan."
                    ),
                ),
            )
            persist_controller_state(run_dir, round_id, controller_payload)
            planner_stage_ran = True

    if not planning and agent_orchestration_requested(run_dir, round_id):
        advisory_path = agent_advisory_plan_path(run_dir, round_id)
        planner_blueprint = stage_blueprint(
            "orchestration-planner",
            skill_name=PLANNER_SKILL_NAME,
            artifacts=artifacts,
            explicit_output_path=str(advisory_path.resolve()),
        )
        controller_payload["steps"] = merge_existing_steps([planner_blueprint], controller_payload.get("steps"))
        controller_payload["stage_contracts"] = stage_contracts_from_blueprints([planner_blueprint])
        controller_payload["planning_mode"] = "planner-pending"
        controller_payload["controller_status"] = "running"
        planner_index = step_index(controller_payload["steps"], "orchestration-planner")
        controller_payload["steps"][planner_index]["status"] = "running"
        controller_payload["steps"][planner_index]["started_at_utc"] = started_at
        persist_controller_state(run_dir, round_id, controller_payload)
        try:
            direct_advisory_result = materialize_direct_council_advisory_plan(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                output_path=relative_runtime_path(run_dir, advisory_path),
                contract_mode=contract_mode,
            )
        except Exception as exc:
            append_planning_attempt(
                controller_payload,
                planning_attempt_record(
                    source="direct-council-advisory",
                    status="failed",
                    plan_path=str(advisory_path.resolve()),
                    message=str(exc),
                ),
            )
            persist_controller_state(run_dir, round_id, controller_payload)
        else:
            advisory_planning = advisory_planning_bundle(run_dir, round_id)
            if direct_advisory_result and advisory_planning:
                ensure_executable_planning(advisory_planning)
                planning = advisory_planning
                blueprints = stage_blueprints(planning, artifacts)
                controller_payload["planning_mode"] = maybe_text(planning.get("planning_mode")) or "agent-advisory"
                controller_payload["planning"] = controller_planning_state(planning, blueprints)
                controller_payload["stage_contracts"] = stage_contracts_from_blueprints(blueprints)
                controller_payload["steps"] = merge_existing_steps(blueprints, controller_payload.get("steps"))
                controller_payload["steps"][step_index(controller_payload["steps"], "orchestration-planner")] = planner_stage_summary_from_result(
                    direct_advisory_result,
                    blueprints[0],
                    planning,
                )
                append_planning_attempt(
                    controller_payload,
                    planning_attempt_record(
                        source=maybe_text(planning.get("plan_source")) or "direct-council-advisory",
                        status="materialized",
                        plan_path=maybe_text(planning.get("plan_path")),
                        planning_mode=maybe_text(planning.get("planning_mode")),
                        controller_authority=maybe_text(planning.get("controller_authority")),
                        receipt_id=maybe_text(direct_advisory_result.get("summary", {}).get("receipt_id"))
                        if isinstance(direct_advisory_result.get("summary"), dict)
                        else "",
                        event_id=maybe_text(direct_advisory_result.get("summary", {}).get("event_id"))
                        if isinstance(direct_advisory_result.get("summary"), dict)
                        else "",
                        message="Controller compiled and selected a direct council advisory plan.",
                    ),
                )
                persist_controller_state(run_dir, round_id, controller_payload)
                planner_stage_ran = True
            elif direct_advisory_result:
                append_planning_attempt(
                    controller_payload,
                    planning_attempt_record(
                        source="direct-council-advisory",
                        status="failed",
                        plan_path=str(advisory_path.resolve()),
                        message="Direct council advisory compiler completed but did not produce a usable execution_queue.",
                    ),
                )
                persist_controller_state(run_dir, round_id, controller_payload)
            else:
                append_planning_attempt(
                    controller_payload,
                    planning_attempt_record(
                        source="direct-council-advisory",
                        status="unavailable",
                        plan_path=str(advisory_path.resolve()),
                        message="Direct council advisory compiler found no usable proposal or readiness inputs.",
                    ),
                )
                persist_controller_state(run_dir, round_id, controller_payload)

        if not planning:
            try:
                advisory_result = run_skill(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    skill_name=PLANNER_SKILL_NAME,
                    skill_args=[
                        "--planner-mode",
                        "agent-advisory",
                        "--output-path",
                        relative_runtime_path(run_dir, advisory_path),
                    ],
                    contract_mode=contract_mode,
                    **execution_kwargs,
                )
                advisory_planning = advisory_planning_bundle(run_dir, round_id)
                if advisory_planning:
                    ensure_executable_planning(advisory_planning)
                    planning = advisory_planning
                    blueprints = stage_blueprints(planning, artifacts)
                    controller_payload["planning_mode"] = maybe_text(planning.get("planning_mode")) or "agent-advisory"
                    controller_payload["planning"] = controller_planning_state(planning, blueprints)
                    controller_payload["stage_contracts"] = stage_contracts_from_blueprints(blueprints)
                    controller_payload["steps"] = merge_existing_steps(blueprints, controller_payload.get("steps"))
                    controller_payload["steps"][step_index(controller_payload["steps"], "orchestration-planner")] = planner_stage_summary_from_result(
                        advisory_result,
                        blueprints[0],
                        planning,
                    )
                    append_planning_attempt(
                        controller_payload,
                        planning_attempt_record(
                            source="agent-advisory",
                            status="materialized",
                            plan_path=maybe_text(planning.get("plan_path")),
                            planning_mode=maybe_text(planning.get("planning_mode")),
                            controller_authority=maybe_text(planning.get("controller_authority")),
                            receipt_id=maybe_text(advisory_result.get("summary", {}).get("receipt_id"))
                            if isinstance(advisory_result.get("summary"), dict)
                            else "",
                            event_id=maybe_text(advisory_result.get("summary", {}).get("event_id"))
                            if isinstance(advisory_result.get("summary"), dict)
                            else "",
                            message="Controller materialized and selected an agent-advisory plan.",
                        ),
                    )
                    persist_controller_state(run_dir, round_id, controller_payload)
                    planner_stage_ran = True
                else:
                    controller_payload["steps"][planner_index].update(
                        {
                            "status": "pending",
                            "event_id": "",
                            "receipt_id": "",
                            "started_at_utc": "",
                            "completed_at_utc": "",
                        }
                    )
                    append_planning_attempt(
                        controller_payload,
                        planning_attempt_record(
                            source="agent-advisory",
                            status="failed",
                            plan_path=str(advisory_path.resolve()),
                            message="Advisory planning completed but did not produce a usable execution_queue.",
                        ),
                    )
                    persist_controller_state(run_dir, round_id, controller_payload)
            except SkillExecutionError as exc:
                controller_payload["steps"][planner_index].update(
                    {
                        "status": "pending",
                        "event_id": "",
                        "receipt_id": "",
                        "started_at_utc": "",
                        "completed_at_utc": "",
                    }
                )
                append_planning_attempt(
                    controller_payload,
                    planning_attempt_record(
                        source="agent-advisory",
                        status="failed",
                        plan_path=str(advisory_path.resolve()),
                        message="Advisory planning completed but did not produce a usable execution_queue.",
                    ),
                )
                persist_controller_state(run_dir, round_id, controller_payload)

    if not planning:
        planner_blueprint = stage_blueprint(
            "orchestration-planner",
            skill_name=PLANNER_SKILL_NAME,
            artifacts=artifacts,
            explicit_output_path=artifacts.get("orchestration_plan_path", ""),
        )
        controller_payload["steps"] = merge_existing_steps([planner_blueprint], controller_payload.get("steps"))
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
            append_planning_attempt(
                controller_payload,
                planning_attempt_record(
                    source="runtime-planner",
                    status="failed",
                    plan_path=artifacts.get("orchestration_plan_path", ""),
                    message=exc.payload.get("message", str(exc)),
                ),
            )
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
        ensure_executable_planning(planning)
        blueprints = stage_blueprints(planning, artifacts)
        controller_payload["planning_mode"] = normalized_controller_planning_mode(
            planning.get("planning_mode")
        )
        controller_payload["planning"] = controller_planning_state(planning, blueprints)
        controller_payload["stage_contracts"] = stage_contracts_from_blueprints(blueprints)
        controller_payload["steps"] = merge_existing_steps(blueprints, controller_payload.get("steps"))
        controller_payload["steps"][step_index(controller_payload["steps"], "orchestration-planner")] = planner_stage_summary_from_result(
            planner_result,
            blueprints[0],
            planning,
        )
        append_planning_attempt(
            controller_payload,
            planning_attempt_record(
                source="runtime-planner",
                status="materialized",
                plan_path=maybe_text(planning.get("plan_path")),
                planning_mode=maybe_text(planning.get("planning_mode")),
                controller_authority=maybe_text(planning.get("controller_authority")),
                receipt_id=maybe_text(planner_result.get("summary", {}).get("receipt_id"))
                if isinstance(planner_result.get("summary"), dict)
                else "",
                event_id=maybe_text(planner_result.get("summary", {}).get("event_id"))
                if isinstance(planner_result.get("summary"), dict)
                else "",
                message="Controller fell back to the runtime planner queue.",
            ),
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
                gate_result = execute_gate_step(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    blueprint=blueprint,
                    stage_contracts=controller_payload.get("stage_contracts", {})
                    if isinstance(controller_payload.get("stage_contracts"), dict)
                    else {},
                )
                gate_handler = maybe_text(gate_result.get("gate_handler")) or maybe_text(blueprint.get("gate_handler")) or stage_name
                readiness_stage_name = maybe_text(gate_result.get("readiness_stage_name"))
                gate_payload = gate_result.get("gate_payload", {}) if isinstance(gate_result.get("gate_payload"), dict) else {}
                gate_updates = gate_result.get("controller_updates", {}) if isinstance(gate_result.get("controller_updates"), dict) else {}
                gate_event_id = new_runtime_event_id(
                    "runtimeevt",
                    run_id,
                    round_id,
                    gate_handler,
                    gate_started_at,
                    gate_payload.get("generated_at_utc"),
                    controller_payload.get("resume_status"),
                )
                append_ledger_event(
                    run_dir,
                    {
                        "schema_version": "runtime-event-v3",
                        "event_id": gate_event_id,
                        "event_type": gate_handler,
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
                        "readiness_stage_name": readiness_stage_name,
                    },
                )
                controller_payload["steps"][step_pos] = gate_stage_summary(blueprint, gate_payload, gate_event_id, gate_started_at)
                controller_payload["readiness_status"] = maybe_text(gate_updates.get("readiness_status")) or "blocked"
                controller_payload["gate_status"] = maybe_text(gate_updates.get("gate_status")) or "freeze-withheld"
                controller_payload["gate_reasons"] = (
                    gate_updates.get("gate_reasons", [])
                    if isinstance(gate_updates.get("gate_reasons"), list)
                    else []
                )
                controller_payload["recommended_next_skills"] = (
                    gate_updates.get("recommended_next_skills", [])
                    if isinstance(gate_updates.get("recommended_next_skills"), list)
                    else []
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

    phase2_control_state = load_phase2_control_state(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    gate_payload = (
        phase2_control_state.get("promotion_gate", {})
        if isinstance(phase2_control_state.get("promotion_gate"), dict)
        else {}
    ) or load_json_if_exists(Path(artifacts["promotion_gate_path"])) or {}
    controller_payload["controller_status"] = "completed"
    controller_payload["planning_mode"] = normalized_controller_planning_mode(
        maybe_text(planning.get("planning_mode"))
        or maybe_text(controller_payload.get("planning_mode"))
    )
    controller_payload["readiness_status"] = maybe_text(gate_payload.get("readiness_status")) or maybe_text(controller_payload.get("readiness_status")) or "blocked"
    controller_payload["gate_status"] = maybe_text(gate_payload.get("gate_status")) or maybe_text(controller_payload.get("gate_status")) or "freeze-withheld"
    if maybe_text(controller_payload.get("promotion_status")) in {"", "not-evaluated"}:
        promotion_basis_payload = (
            phase2_control_state.get("promotion_basis", {})
            if isinstance(phase2_control_state.get("promotion_basis"), dict)
            else {}
        ) or load_json_if_exists(Path(artifacts["promotion_basis_path"])) or {}
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
