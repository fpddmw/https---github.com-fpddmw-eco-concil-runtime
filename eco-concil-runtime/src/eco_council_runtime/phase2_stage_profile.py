from __future__ import annotations

from typing import Any


DEFAULT_PHASE2_PLANNER_SKILL_NAME = "eco-plan-round-orchestration"

DEFAULT_PHASE2_STAGE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "orchestration-planner": {
        "phase_group": "planning",
        "stage_kind": "skill",
        "expected_skill_name": DEFAULT_PHASE2_PLANNER_SKILL_NAME,
        "artifact_key": "orchestration_plan_path",
        "required_previous_stages": [],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Materialize one auditable phase-2 plan before controller execution.",
    },
    "board-summary": {
        "phase_group": "exports",
        "stage_kind": "skill",
        "expected_skill_name": "eco-summarize-board-state",
        "artifact_key": "board_summary_path",
        "required_previous_stages": ["orchestration-planner"],
        "blocking": False,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Refresh the structured board summary as a derived export when operators need a compact snapshot.",
    },
    "board-brief": {
        "phase_group": "exports",
        "stage_kind": "skill",
        "expected_skill_name": "eco-materialize-board-brief",
        "artifact_key": "board_brief_path",
        "required_previous_stages": ["orchestration-planner"],
        "blocking": False,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Refresh the compact board brief as a derived export for human handoff or archival context.",
    },
    "next-actions": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-propose-next-actions",
        "artifact_key": "next_actions_path",
        "required_previous_stages": ["orchestration-planner"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Re-rank investigation actions directly from shared board state and evidence context.",
    },
    "falsification-probes": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-open-falsification-probe",
        "artifact_key": "probes_path",
        "required_previous_stages": ["orchestration-planner"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Refresh contradiction and falsification work before readiness review.",
    },
    "round-readiness": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-summarize-round-readiness",
        "artifact_key": "readiness_path",
        "required_previous_stages": ["orchestration-planner"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Freeze one readiness posture before gate evaluation.",
    },
    "promotion-gate": {
        "phase_group": "gate",
        "stage_kind": "gate",
        "expected_skill_name": "",
        "artifact_key": "promotion_gate_path",
        "required_previous_stages": ["round-readiness"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Evaluate whether the current round can move into promotion and reporting.",
    },
    "promotion-basis": {
        "phase_group": "promotion",
        "stage_kind": "skill",
        "expected_skill_name": "eco-promote-evidence-basis",
        "artifact_key": "promotion_basis_path",
        "required_previous_stages": ["promotion-gate"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Freeze the promoted or withheld evidence basis after gate evaluation.",
    },
}


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def resolve_stage_definitions(
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    if isinstance(stage_definitions, dict) and stage_definitions:
        return stage_definitions
    return DEFAULT_PHASE2_STAGE_DEFINITIONS


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


def lookup_stage_contract(
    stage_name: str,
    *,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    normalized_name = maybe_text(stage_name)
    definition = resolve_stage_definitions(stage_definitions).get(normalized_name)
    if definition is None:
        return None
    return {"stage_name": normalized_name, **definition}


def stage_contract(
    stage_name: str,
    *,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    definition = lookup_stage_contract(stage_name, stage_definitions=stage_definitions)
    if definition is None:
        raise ValueError(f"Unknown phase-2 stage: {maybe_text(stage_name)}")
    return definition


def expected_output_path(
    stage_name: str,
    artifacts: dict[str, Any],
    explicit_path: str = "",
    *,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> str:
    text = maybe_text(explicit_path)
    if text:
        return text
    contract = stage_contract(stage_name, stage_definitions=stage_definitions)
    artifact_key = maybe_text(contract.get("artifact_key"))
    return maybe_text(artifacts.get(artifact_key))


def validate_skill_stage(
    stage_name: str,
    skill_name: str,
    *,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    contract = stage_contract(stage_name, stage_definitions=stage_definitions)
    if maybe_text(contract.get("stage_kind")) != "skill":
        raise ValueError(f"Stage {stage_name} is not a skill stage.")
    expected_skill_name = maybe_text(contract.get("expected_skill_name"))
    normalized_skill_name = maybe_text(skill_name)
    if expected_skill_name and normalized_skill_name != expected_skill_name:
        raise ValueError(
            f"Stage {stage_name} must execute {expected_skill_name}, but planner selected {normalized_skill_name or '<empty>'}."
        )
    return contract


def normalized_required_previous_stages(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [maybe_text(item) for item in value if maybe_text(item)]


def validate_stage_blueprints(
    stage_entries: list[dict[str, Any]],
    *,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> None:
    seen: set[str] = set()
    for entry in stage_entries:
        if not isinstance(entry, dict):
            continue
        stage_name = maybe_text(entry.get("stage") or entry.get("stage_name"))
        if not stage_name:
            raise ValueError("Missing phase-2 stage name in planned controller sequence.")
        if stage_name in seen:
            raise ValueError(f"Duplicate phase-2 stage detected: {stage_name}")
        has_explicit_previous_stages = "required_previous_stages" in entry
        required_previous_stages = (
            normalized_required_previous_stages(entry.get("required_previous_stages"))
            if has_explicit_previous_stages
            else []
        )
        if not has_explicit_previous_stages:
            contract = lookup_stage_contract(
                stage_name,
                stage_definitions=stage_definitions,
            )
            required_previous_stages = normalized_required_previous_stages(
                contract.get("required_previous_stages") if isinstance(contract, dict) else []
            )
        for dependency_name in required_previous_stages:
            if dependency_name and dependency_name not in seen:
                raise ValueError(
                    f"Stage {stage_name} requires {dependency_name} to appear earlier in the controller sequence."
                )
        seen.add(stage_name)


def validate_stage_sequence(
    stage_names: list[str],
    *,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> None:
    validate_stage_blueprints(
        [{"stage": stage_name} for stage_name in stage_names],
        stage_definitions=stage_definitions,
    )
