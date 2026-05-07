from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.gate import (
    GateHandler,
    execute_gate_step as execute_runtime_gate_step,
)
from eco_council_runtime.kernel.execution.runtime_planning_profile import (
    agent_orchestration_requested as agent_orchestration_requested_from_profile,
    ensure_executable_planning as ensure_executable_planning_from_profile,
    normalized_controller_planning_mode as normalized_controller_planning_mode_from_profile,
    planning_bundle as planning_bundle_from_result,
    planning_from_controller as planning_from_controller_from_profile,
)
from eco_council_runtime.kernel.execution.controller.transition_planning import (
    PLANNER_SKILL_NAME,
)


def normalized_controller_planning_mode(value: Any, *, default: str = "planner-backed") -> str:
    return normalized_controller_planning_mode_from_profile(value, default=default)


def agent_orchestration_requested(run_dir: Path, round_id: str) -> bool:
    return agent_orchestration_requested_from_profile(run_dir, round_id)


def planning_bundle(run_dir: Path, round_id: str, planner_result: dict[str, Any]) -> dict[str, Any]:
    return planning_bundle_from_result(
        run_dir,
        round_id,
        planner_result,
        planner_skill_name=PLANNER_SKILL_NAME,
    )


def planning_from_controller(run_dir: Path, round_id: str, controller_payload: dict[str, Any]) -> dict[str, Any]:
    return planning_from_controller_from_profile(
        run_dir,
        round_id,
        controller_payload,
        planner_skill_name=PLANNER_SKILL_NAME,
    )


def ensure_executable_planning(planning: dict[str, Any]) -> None:
    ensure_executable_planning_from_profile(planning)


def execute_gate_step(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    blueprint: dict[str, Any],
    stage_contracts: dict[str, Any] | None = None,
    gate_handlers: dict[str, GateHandler] | None = None,
) -> dict[str, Any]:
    return execute_runtime_gate_step(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        blueprint=blueprint,
        stage_contracts=stage_contracts,
        gate_handlers=gate_handlers,
    )


__all__ = [
    "agent_orchestration_requested",
    "ensure_executable_planning",
    "execute_gate_step",
    "normalized_controller_planning_mode",
    "planning_bundle",
    "planning_from_controller",
]
