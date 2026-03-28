"""Fetch-plan assembly and input-snapshot validation for orchestration workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import file_snapshot, utc_now_iso
from eco_council_runtime.adapters.run_paths import load_mission, round_dir
from eco_council_runtime.application import orchestration_prepare
from eco_council_runtime.application.orchestration.governance import (
    role_selected_sources,
)
from eco_council_runtime.application.orchestration.query_builders import step_task_ids, task_objective_text
from eco_council_runtime.application.orchestration.step_synthesis import (
    build_environmentalist_steps,
    build_sociologist_steps,
)
from eco_council_runtime.controller.paths import source_selection_path
from eco_council_runtime.controller.policy import (
    allowed_sources_for_role,
    effective_constraints,
    load_override_requests,
    policy_profile_summary,
    role_evidence_requirements,
    role_source_governance,
)
from eco_council_runtime.domain.text import maybe_text

ensure_object = orchestration_prepare.ensure_object
load_tasks = orchestration_prepare.load_tasks
load_source_selection = orchestration_prepare.load_source_selection
tasks_for_role = orchestration_prepare.tasks_for_role
mission_window = orchestration_prepare.mission_window
mission_region = orchestration_prepare.mission_region


def fetch_plan_input_snapshot(
    *,
    run_dir: Path,
    round_id: str,
    sociologist_selection: dict[str, Any] | None,
    environmentalist_selection: dict[str, Any] | None,
) -> dict[str, Any]:
    tasks_file = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    sociologist_path = source_selection_path(run_dir, round_id, "sociologist")
    environmentalist_path = source_selection_path(run_dir, round_id, "environmentalist")
    return {
        "tasks": file_snapshot(tasks_file),
        "source_selections": {
            "sociologist": {
                **file_snapshot(sociologist_path),
                "status": maybe_text((sociologist_selection or {}).get("status")),
            },
            "environmentalist": {
                **file_snapshot(environmentalist_path),
                "status": maybe_text((environmentalist_selection or {}).get("status")),
            },
        },
    }


def ensure_fetch_plan_inputs_match(*, run_dir: Path, round_id: str, plan: dict[str, Any]) -> None:
    snapshot = ensure_object(plan.get("input_snapshot"), "fetch_plan.input_snapshot")
    task_snapshot = ensure_object(snapshot.get("tasks"), "fetch_plan.input_snapshot.tasks")
    task_path = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    current_task_snapshot = file_snapshot(task_path)
    issues: list[str] = []
    if maybe_text(task_snapshot.get("sha256")) != maybe_text(current_task_snapshot.get("sha256")):
        issues.append(f"tasks.json changed ({task_path})")

    source_snapshots = ensure_object(snapshot.get("source_selections"), "fetch_plan.input_snapshot.source_selections")
    for role in ("sociologist", "environmentalist"):
        expected = ensure_object(source_snapshots.get(role), f"fetch_plan.input_snapshot.source_selections.{role}")
        path = source_selection_path(run_dir, round_id, role)
        current = file_snapshot(path)
        current_payload = load_source_selection(run_dir, round_id, role)
        current_status = maybe_text((current_payload or {}).get("status"))
        if maybe_text(expected.get("sha256")) != maybe_text(current.get("sha256")):
            issues.append(f"{role} source_selection changed ({path})")
        if maybe_text(expected.get("status")) != current_status:
            issues.append(
                f"{role} source_selection status changed (expected {maybe_text(expected.get('status')) or '<empty>'}, found {current_status or '<empty>'})"
            )
    if issues:
        raise RuntimeError("Fetch plan inputs changed since prepare-round. Rerun prepare-round. " + "; ".join(issues))


def _role_plan_summary(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    role: str,
    tasks: list[dict[str, Any]],
    source_selection: dict[str, Any] | None,
) -> dict[str, Any]:
    governance = role_source_governance(mission, role)
    return {
        "task_ids": step_task_ids(tasks),
        "objective": task_objective_text(tasks),
        "allowed_sources": allowed_sources_for_role(mission, role),
        "evidence_requirements": role_evidence_requirements(tasks),
        "governed_families": [
            maybe_text(family.get("family_id"))
            for family in governance.get("families", [])
            if isinstance(family, dict) and maybe_text(family.get("family_id"))
        ],
        "override_requests": load_override_requests(run_dir, round_id, role),
        "source_selection_path": str(source_selection_path(run_dir, round_id, role)),
        "source_selection_status": maybe_text((source_selection or {}).get("status")),
        "selected_sources": role_selected_sources(
            mission=mission,
            tasks=tasks,
            role=role,
            source_selection=source_selection,
        ),
    }


def build_fetch_plan(
    *,
    run_dir: Path,
    round_id: str,
    firms_point_padding_deg: float,
) -> dict[str, Any]:
    mission = load_mission(run_dir)
    tasks = load_tasks(run_dir, round_id)
    sociologist_tasks = tasks_for_role(tasks, "sociologist")
    environmentalist_tasks = tasks_for_role(tasks, "environmentalist")
    sociologist_selection = load_source_selection(run_dir, round_id, "sociologist")
    environmentalist_selection = load_source_selection(run_dir, round_id, "environmentalist")

    steps: list[dict[str, Any]] = []
    steps.extend(
        build_sociologist_steps(
            run_dir=run_dir,
            round_id=round_id,
            mission=mission,
            tasks=tasks,
            source_selection=sociologist_selection,
        )
    )
    steps.extend(
        build_environmentalist_steps(
            run_dir=run_dir,
            round_id=round_id,
            mission=mission,
            tasks=tasks,
            source_selection=environmentalist_selection,
            firms_point_padding_deg=firms_point_padding_deg,
        )
    )
    return {
        "plan_kind": "eco-council-fetch-plan",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "policy_profile": policy_profile_summary(mission),
        "effective_constraints": effective_constraints(mission),
        "input_snapshot": fetch_plan_input_snapshot(
            run_dir=run_dir,
            round_id=round_id,
            sociologist_selection=sociologist_selection,
            environmentalist_selection=environmentalist_selection,
        ),
        "run": {
            "run_id": maybe_text(mission.get("run_id")),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region_label": maybe_text(mission_region(mission).get("label")),
            "window": mission_window(mission),
        },
        "roles": {
            "sociologist": _role_plan_summary(
                run_dir=run_dir,
                round_id=round_id,
                mission=mission,
                role="sociologist",
                tasks=sociologist_tasks,
                source_selection=sociologist_selection,
            ),
            "environmentalist": _role_plan_summary(
                run_dir=run_dir,
                round_id=round_id,
                mission=mission,
                role="environmentalist",
                tasks=environmentalist_tasks,
                source_selection=environmentalist_selection,
            ),
        },
        "steps": steps,
    }


__all__ = [
    "build_fetch_plan",
    "ensure_fetch_plan_inputs_match",
    "fetch_plan_input_snapshot",
]
