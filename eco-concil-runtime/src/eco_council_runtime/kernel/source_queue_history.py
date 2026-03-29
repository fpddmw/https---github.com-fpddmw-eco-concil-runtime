from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .manifest import load_json_if_exists
from .source_queue_contract import SOURCE_SELECTION_ROLES, maybe_text, role_source_governance, source_role, source_selection_path

ROUND_TASK_PATTERN = re.compile(r"^round_tasks_(?P<round_id>.+)\.json$")
IMPORT_EXECUTION_PATTERN = re.compile(r"^import_execution_(?P<round_id>.+)\.json$")
SOURCE_SELECTION_PATTERN = re.compile(r"^source_selection_(?P<role>[^_]+)_(?P<round_id>.+)\.json$")


def import_execution_path(run_dir: Path, round_id: str) -> Path:
    return run_dir / "runtime" / f"import_execution_{round_id}.json"


def observe_round_artifact(round_times: dict[str, int], round_id: str, path: Path) -> None:
    if not path.exists():
        return
    observed = path.stat().st_mtime_ns
    existing = round_times.get(round_id)
    if existing is None or observed < existing:
        round_times[round_id] = observed


def discovered_round_ids(run_dir: Path) -> list[str]:
    round_times: dict[str, int] = {}

    investigation_dir = run_dir / "investigation"
    if investigation_dir.exists():
        for path in investigation_dir.glob("round_tasks_*.json"):
            match = ROUND_TASK_PATTERN.match(path.name)
            if match is None:
                continue
            observe_round_artifact(round_times, maybe_text(match.group("round_id")), path)

    runtime_dir = run_dir / "runtime"
    if runtime_dir.exists():
        for path in runtime_dir.glob("import_execution_*.json"):
            match = IMPORT_EXECUTION_PATTERN.match(path.name)
            if match is None:
                continue
            observe_round_artifact(round_times, maybe_text(match.group("round_id")), path)
        for path in runtime_dir.glob("source_selection_*.json"):
            match = SOURCE_SELECTION_PATTERN.match(path.name)
            if match is None:
                continue
            role = maybe_text(match.group("role"))
            if role not in SOURCE_SELECTION_ROLES:
                continue
            observe_round_artifact(round_times, maybe_text(match.group("round_id")), path)

    return [round_id for round_id, _ in sorted(round_times.items(), key=lambda item: (item[1], item[0]))]


def prior_round_ids(run_dir: Path, current_round_id: str) -> list[str]:
    ordered = discovered_round_ids(run_dir)
    if current_round_id in ordered:
        return [round_id for round_id in ordered[: ordered.index(current_round_id)] if round_id != current_round_id]
    return [round_id for round_id in ordered if round_id != current_round_id]


def role_family_memory(run_dir: Path, current_round_id: str, role: str, mission: dict[str, Any]) -> list[dict[str, Any]]:
    governance = role_source_governance(mission, role)
    families = governance.get("families", []) if isinstance(governance.get("families"), list) else []
    if not families:
        return []

    history: list[dict[str, Any]] = []
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        family_skills = {
            maybe_text(skill)
            for skill in family.get("skills", [])
            if maybe_text(skill)
        }
        rounds: list[dict[str, Any]] = []
        completed_sources: set[str] = set()

        for observed_round_id in prior_round_ids(run_dir, current_round_id):
            selection = load_json_if_exists(source_selection_path(run_dir, observed_round_id, role))
            if not isinstance(selection, dict):
                continue
            family_plans = selection.get("family_plans") if isinstance(selection.get("family_plans"), list) else []
            matching_family = next(
                (
                    item
                    for item in family_plans
                    if isinstance(item, dict) and maybe_text(item.get("family_id")) == family_id
                ),
                None,
            )
            if not isinstance(matching_family, dict):
                continue

            selected_layers: list[str] = []
            selected_sources: list[str] = []
            layer_plans = matching_family.get("layer_plans") if isinstance(matching_family.get("layer_plans"), list) else []
            for layer_plan in layer_plans:
                if not isinstance(layer_plan, dict) or layer_plan.get("selected") is not True:
                    continue
                layer_id = maybe_text(layer_plan.get("layer_id"))
                if layer_id:
                    selected_layers.append(layer_id)
                source_skills = layer_plan.get("source_skills") if isinstance(layer_plan.get("source_skills"), list) else []
                selected_sources.extend(maybe_text(skill) for skill in source_skills if maybe_text(skill))

            execution = load_json_if_exists(import_execution_path(run_dir, observed_round_id)) or {}
            statuses = execution.get("statuses") if isinstance(execution.get("statuses"), list) else []
            completed_in_round: list[str] = []
            for status in statuses:
                if not isinstance(status, dict):
                    continue
                if maybe_text(status.get("status")) != "completed":
                    continue
                source_skill = maybe_text(status.get("source_skill"))
                if not source_skill or source_skill not in family_skills:
                    continue
                status_role = maybe_text(status.get("role"))
                if status_role and status_role != role:
                    continue
                if not status_role and source_role(source_skill) != role:
                    continue
                completed_sources.add(source_skill)
                completed_in_round.append(source_skill)

            rounds.append(
                {
                    "round_id": observed_round_id,
                    "selection_status": maybe_text(selection.get("status")),
                    "selected_layers": sorted({item for item in selected_layers if item}),
                    "selected_sources": sorted({item for item in selected_sources if item}),
                    "completed_sources": sorted({item for item in completed_in_round if item}),
                    "summary": maybe_text(selection.get("summary")),
                }
            )

        history.append(
            {
                "family_id": family_id,
                "label": maybe_text(family.get("label")),
                "prior_rounds": rounds[-3:],
                "completed_sources": sorted(completed_sources),
            }
        )
    return history


__all__ = [
    "discovered_round_ids",
    "import_execution_path",
    "prior_round_ids",
    "role_family_memory",
]
