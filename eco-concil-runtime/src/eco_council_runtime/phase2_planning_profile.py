from __future__ import annotations

from pathlib import Path
from typing import Any

from .phase2_stage_profile import (
    DEFAULT_PHASE2_PLANNER_SKILL_NAME,
    default_gate_steps,
    default_post_gate_steps,
)
from .kernel.deliberation_plane import load_orchestration_plan_record
from .kernel.manifest import load_json_if_exists
from .kernel.paths import (
    agent_advisory_plan_path,
    agent_entry_gate_path,
    mission_scaffold_path,
    orchestration_plan_path,
)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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
    if not isinstance(value, list):
        return []
    return [maybe_text(item) for item in value if maybe_text(item)]


def normalized_planned_steps(
    entries: Any,
    *,
    default_stage_kind: str = "skill",
) -> list[dict[str, Any]]:
    if not isinstance(entries, list):
        return []
    results: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        stage_kind = maybe_text(item.get("stage_kind") or item.get("kind")) or default_stage_kind
        skill_name = maybe_text(item.get("skill_name"))
        stage_name = maybe_text(
            item.get("stage_name") or item.get("stage") or skill_name or item.get("gate_handler")
        )
        if not stage_name:
            continue
        if stage_kind == "skill" and not skill_name:
            continue
        raw_skill_args = item.get("skill_args", [])
        skill_args = (
            [maybe_text(value) for value in raw_skill_args if maybe_text(value)]
            if isinstance(raw_skill_args, list)
            else []
        )
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
    if mode == "transition-executor":
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
    if planning_mode == "transition-executor" or controller_authority == "transition-executor":
        return "approved-transition-request"
    if planning_mode == "agent-advisory" or controller_authority == "advisory-only":
        return "agent-advisory"
    return "runtime-planner"


def relative_runtime_path(run_dir: Path, path: Path) -> str:
    try:
        return str(path.relative_to(run_dir))
    except ValueError:
        return str(path)


def inferred_plan_controller_authority(plan_path: str) -> str:
    if Path(plan_path).name.startswith("agent_advisory_plan_"):
        return "advisory-only"
    return "queue-owner"


def contextualized_runtime_plan_payload(
    plan_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    artifact_path: str,
    controller_authority: str,
) -> dict[str, Any]:
    payload = dict(plan_payload)
    payload["run_id"] = maybe_text(payload.get("run_id")) or maybe_text(run_id)
    payload["round_id"] = maybe_text(payload.get("round_id")) or maybe_text(round_id)
    payload["artifact_path"] = (
        maybe_text(payload.get("artifact_path")) or maybe_text(artifact_path)
    )
    resolved_controller_authority = (
        maybe_text(payload.get("controller_authority"))
        or maybe_text(controller_authority)
    )
    if resolved_controller_authority:
        payload["controller_authority"] = resolved_controller_authority
    if not maybe_text(payload.get("plan_source")):
        payload["plan_source"] = planning_source_from_payload(payload)
    return payload


def load_runtime_plan_payload(
    run_dir: Path,
    round_id: str,
    *,
    run_id: str = "",
    plan_path: str = "",
    controller_authority: str = "",
) -> dict[str, Any]:
    resolved_plan_path = maybe_text(plan_path) or str(
        orchestration_plan_path(run_dir, round_id).resolve()
    )
    resolved_controller_authority = (
        maybe_text(controller_authority)
        or inferred_plan_controller_authority(resolved_plan_path)
    )
    record = load_orchestration_plan_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_path=resolved_plan_path,
        controller_authority=resolved_controller_authority,
    ) or {}
    if isinstance(record, dict) and record:
        return contextualized_runtime_plan_payload(
            record,
            run_id=run_id,
            round_id=round_id,
            artifact_path=resolved_plan_path,
            controller_authority=resolved_controller_authority,
        )
    payload = load_json_if_exists(Path(resolved_plan_path)) or {}
    if not payload:
        return {}
    return contextualized_runtime_plan_payload(
        payload,
        run_id=run_id,
        round_id=round_id,
        artifact_path=resolved_plan_path,
        controller_authority=resolved_controller_authority,
    )


def agent_orchestration_requested(run_dir: Path, round_id: str) -> bool:
    mission_payload = load_json_if_exists(mission_scaffold_path(run_dir, round_id)) or {}
    if maybe_text(mission_payload.get("orchestration_mode")) == "openclaw-agent":
        return True
    entry_gate_payload = load_json_if_exists(agent_entry_gate_path(run_dir, round_id)) or {}
    return maybe_text(entry_gate_payload.get("orchestration_mode")) in {
        "openclaw-agent",
        "openclaw-agent-compatible",
    }


def planning_bundle_from_payload(
    run_dir: Path,
    round_id: str,
    plan_path: str,
    plan_payload: dict[str, Any],
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> dict[str, Any]:
    explicit_execution_queue = normalized_planned_steps(plan_payload.get("execution_queue"))
    explicit_gate_steps = normalized_planned_steps(
        plan_payload.get("gate_steps"),
        default_stage_kind="gate",
    )
    explicit_post_gate_steps = normalized_planned_steps(plan_payload.get("post_gate_steps"))
    gate_steps = explicit_gate_steps or default_gate_steps()
    post_gate_steps = explicit_post_gate_steps or default_post_gate_steps()
    fallback_path = plan_payload.get("fallback_path", []) if isinstance(plan_payload.get("fallback_path"), list) else []
    return {
        "plan_id": maybe_text(plan_payload.get("plan_id")),
        "plan_path": plan_path,
        "planning_status": maybe_text(plan_payload.get("planning_status")) or "ready-for-controller",
        "planning_mode": normalized_controller_planning_mode(plan_payload.get("planning_mode")),
        "planner_skill_name": maybe_text(plan_payload.get("planner_skill_name")) or planner_skill_name,
        "controller_authority": maybe_text(plan_payload.get("controller_authority")) or "queue-owner",
        "plan_source": planning_source_from_payload(plan_payload),
        "probe_stage_included": bool(plan_payload.get("probe_stage_included")),
        "include_planner_stage": (
            bool(plan_payload.get("include_planner_stage"))
            if isinstance(plan_payload.get("include_planner_stage"), bool)
            else True
        ),
        "assigned_role_hints": plan_payload.get("assigned_role_hints", []) if isinstance(plan_payload.get("assigned_role_hints"), list) else [],
        "execution_queue": explicit_execution_queue,
        "gate_steps": gate_steps,
        "post_gate_steps": post_gate_steps,
        "stop_conditions": plan_payload.get("stop_conditions", []) if isinstance(plan_payload.get("stop_conditions"), list) else [],
        "fallback_path": fallback_path,
        "fallback_suggested_next_skills": unique_texts(
            [
                skill_name
                for row in fallback_path
                if isinstance(row, dict)
                for skill_name in row.get("suggested_next_skills", [])
            ]
        ),
        "plan_payload": plan_payload,
    }


def planning_bundle(
    run_dir: Path,
    round_id: str,
    planner_result: dict[str, Any],
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> dict[str, Any]:
    planner_wrapper = planner_result.get("skill_payload", {}) if isinstance(planner_result.get("skill_payload"), dict) else {}
    plan_path = resolve_plan_path(run_dir, round_id, planner_wrapper)
    plan_payload = load_runtime_plan_payload(
        run_dir,
        round_id,
        run_id=maybe_text(planner_wrapper.get("run_id")),
        plan_path=plan_path,
    )
    return planning_bundle_from_payload(
        run_dir,
        round_id,
        plan_path,
        plan_payload,
        planner_skill_name=planner_skill_name,
    )


def advisory_planning_bundle(
    run_dir: Path,
    round_id: str,
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> dict[str, Any]:
    advisory_path = agent_advisory_plan_path(run_dir, round_id)
    advisory_payload = load_runtime_plan_payload(
        run_dir,
        round_id,
        plan_path=str(advisory_path.resolve()),
        controller_authority="advisory-only",
    )
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
    planning = planning_bundle_from_payload(
        run_dir,
        round_id,
        str(advisory_path.resolve()),
        advisory_payload,
        planner_skill_name=planner_skill_name,
    )
    planning["planning_mode"] = maybe_text(advisory_payload.get("planning_mode")) or "agent-advisory"
    planning["controller_authority"] = maybe_text(advisory_payload.get("controller_authority")) or "advisory-only"
    planning["plan_source"] = maybe_text(advisory_payload.get("plan_source")) or "agent-advisory"
    return planning


def planning_from_controller(
    run_dir: Path,
    round_id: str,
    controller_payload: dict[str, Any],
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> dict[str, Any]:
    planning = controller_payload.get("planning", {}) if isinstance(controller_payload.get("planning"), dict) else {}
    execution_queue = normalized_planned_steps(planning.get("execution_queue"))
    gate_steps = normalized_planned_steps(planning.get("gate_steps"), default_stage_kind="gate")
    post_gate_steps = normalized_planned_steps(planning.get("post_gate_steps"))
    if execution_queue or gate_steps or post_gate_steps:
        return {
            "plan_id": maybe_text(planning.get("plan_id")),
            "plan_path": maybe_text(planning.get("plan_path")) or str(orchestration_plan_path(run_dir, round_id).resolve()),
            "planning_status": maybe_text(planning.get("planning_status")) or "ready-for-controller",
            "planning_mode": normalized_controller_planning_mode(
                maybe_text(controller_payload.get("planning_mode")) or maybe_text(planning.get("planning_mode"))
            ),
            "planner_skill_name": maybe_text(planning.get("planner_skill_name")) or planner_skill_name,
            "controller_authority": maybe_text(planning.get("controller_authority")) or "queue-owner",
            "plan_source": maybe_text(planning.get("plan_source")) or "controller-snapshot",
            "probe_stage_included": bool(planning.get("probe_stage_included")),
            "include_planner_stage": (
                bool(planning.get("include_planner_stage"))
                if isinstance(planning.get("include_planner_stage"), bool)
                else True
            ),
            "assigned_role_hints": planning.get("assigned_role_hints", []) if isinstance(planning.get("assigned_role_hints"), list) else [],
            "execution_queue": execution_queue,
            "gate_steps": gate_steps or default_gate_steps(),
            "post_gate_steps": post_gate_steps or default_post_gate_steps(),
            "stop_conditions": planning.get("stop_conditions", []) if isinstance(planning.get("stop_conditions"), list) else [],
            "fallback_path": planning.get("fallback_path", []) if isinstance(planning.get("fallback_path"), list) else [],
            "fallback_suggested_next_skills": unique_texts(
                [
                    skill_name
                    for row in planning.get("fallback_path", [])
                    if isinstance(row, dict)
                    for skill_name in row.get("suggested_next_skills", [])
                ]
            ),
            "plan_payload": {},
        }
    plan_path = maybe_text(planning.get("plan_path")) or str(orchestration_plan_path(run_dir, round_id).resolve())
    plan_payload = load_runtime_plan_payload(
        run_dir,
        round_id,
        run_id=maybe_text(controller_payload.get("run_id")),
        plan_path=plan_path,
        controller_authority=maybe_text(planning.get("controller_authority")),
    )
    if not plan_payload:
        return {}
    return planning_bundle_from_payload(
        run_dir,
        round_id,
        plan_path,
        plan_payload,
        planner_skill_name=planner_skill_name,
    )


def ensure_executable_planning(planning: dict[str, Any]) -> None:
    execution_queue = planning.get("execution_queue", []) if isinstance(planning.get("execution_queue"), list) else []
    gate_steps = planning.get("gate_steps", []) if isinstance(planning.get("gate_steps"), list) else []
    post_gate_steps = planning.get("post_gate_steps", []) if isinstance(planning.get("post_gate_steps"), list) else []
    if execution_queue or gate_steps or post_gate_steps:
        return
    plan_path = maybe_text(planning.get("plan_path")) or "<unknown>"
    raise ValueError(
        f"Planning artifact {plan_path} does not define any executable controller stages; runtime kernel will not synthesize phase-2 deliberation stages."
    )


def phase2_planning_source(
    source_name: str,
    *,
    source_kind: str,
    output_path_key: str,
    planner_mode: str = "",
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
    requires_agent_orchestration: bool = False,
    adopted_message: str = "",
    materialized_message: str = "",
    failed_message: str = "",
    unavailable_message: str = "",
) -> dict[str, Any]:
    return {
        "source_name": maybe_text(source_name),
        "source_kind": maybe_text(source_kind),
        "output_path_key": maybe_text(output_path_key),
        "planner_mode": maybe_text(planner_mode),
        "planner_skill_name": maybe_text(planner_skill_name) or DEFAULT_PHASE2_PLANNER_SKILL_NAME,
        "requires_agent_orchestration": bool(requires_agent_orchestration),
        "adopted_message": maybe_text(adopted_message),
        "materialized_message": maybe_text(materialized_message),
        "failed_message": maybe_text(failed_message),
        "unavailable_message": maybe_text(unavailable_message),
    }


def default_phase2_planning_sources(
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> list[dict[str, Any]]:
    del planner_skill_name
    return []


def resolve_phase2_planning_sources(
    planning_sources: list[dict[str, Any]] | None = None,
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> list[dict[str, Any]]:
    if isinstance(planning_sources, list) and planning_sources:
        return [item for item in planning_sources if isinstance(item, dict)]
    return default_phase2_planning_sources(planner_skill_name=planner_skill_name)


def planning_source_output_path(
    run_dir: Path,
    round_id: str,
    artifacts: dict[str, Any],
    source_spec: dict[str, Any],
) -> Path:
    output_path_key = maybe_text(source_spec.get("output_path_key"))
    if output_path_key:
        artifact_path = maybe_text(artifacts.get(output_path_key))
        if artifact_path:
            return Path(artifact_path)
    if output_path_key == "agent_advisory_plan_path":
        return agent_advisory_plan_path(run_dir, round_id)
    return orchestration_plan_path(run_dir, round_id)


def planner_skill_args_for_source(
    run_dir: Path,
    source_spec: dict[str, Any],
    output_path: Path,
) -> list[str]:
    planner_mode = maybe_text(source_spec.get("planner_mode"))
    if not planner_mode:
        return []
    return [
        "--planner-mode",
        planner_mode,
        "--output-path",
        relative_runtime_path(run_dir, output_path),
    ]


def planning_bundle_for_source(
    run_dir: Path,
    round_id: str,
    source_spec: dict[str, Any],
    planner_result: dict[str, Any] | None = None,
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> dict[str, Any]:
    source_kind = maybe_text(source_spec.get("source_kind"))
    output_path_key = maybe_text(source_spec.get("output_path_key"))
    if output_path_key == "agent_advisory_plan_path" or source_kind in {
        "existing-advisory",
        "direct-council-advisory",
    }:
        return advisory_planning_bundle(
            run_dir,
            round_id,
            planner_skill_name=planner_skill_name,
        )
    if planner_result is None:
        return {}
    return planning_bundle(
        run_dir,
        round_id,
        planner_result,
        planner_skill_name=planner_skill_name,
    )
