from __future__ import annotations

from typing import Any


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


PHASE2_STAGE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "orchestration-planner": {
        "phase_group": "planning",
        "stage_kind": "skill",
        "expected_skill_name": "eco-plan-round-orchestration",
        "artifact_key": "orchestration_plan_path",
        "required_previous_stages": [],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Materialize one auditable phase-2 plan before controller execution.",
    },
    "board-summary": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-summarize-board-state",
        "artifact_key": "board_summary_path",
        "required_previous_stages": ["orchestration-planner"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Refresh the structured board summary before downstream phase-2 actions.",
    },
    "board-brief": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-materialize-board-brief",
        "artifact_key": "board_brief_path",
        "required_previous_stages": ["board-summary"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Refresh the compact board brief used by planning and handoff stages.",
    },
    "next-actions": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-propose-next-actions",
        "artifact_key": "next_actions_path",
        "required_previous_stages": ["board-brief"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Re-rank investigation actions from refreshed board and evidence context.",
    },
    "falsification-probes": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-open-falsification-probe",
        "artifact_key": "probes_path",
        "required_previous_stages": ["next-actions"],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Refresh contradiction and falsification work before readiness review.",
    },
    "round-readiness": {
        "phase_group": "execution",
        "stage_kind": "skill",
        "expected_skill_name": "eco-summarize-round-readiness",
        "artifact_key": "readiness_path",
        "required_previous_stages": ["next-actions"],
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


def stage_contract(stage_name: str) -> dict[str, Any]:
    normalized_name = maybe_text(stage_name)
    definition = PHASE2_STAGE_DEFINITIONS.get(normalized_name)
    if definition is None:
        raise ValueError(f"Unknown phase-2 stage: {normalized_name}")
    return {"stage_name": normalized_name, **definition}


def expected_output_path(stage_name: str, artifacts: dict[str, Any], explicit_path: str = "") -> str:
    text = maybe_text(explicit_path)
    if text:
        return text
    contract = stage_contract(stage_name)
    artifact_key = maybe_text(contract.get("artifact_key"))
    return maybe_text(artifacts.get(artifact_key))


def validate_skill_stage(stage_name: str, skill_name: str) -> dict[str, Any]:
    contract = stage_contract(stage_name)
    if maybe_text(contract.get("stage_kind")) != "skill":
        raise ValueError(f"Stage {stage_name} is not a skill stage.")
    expected_skill_name = maybe_text(contract.get("expected_skill_name"))
    normalized_skill_name = maybe_text(skill_name)
    if expected_skill_name and normalized_skill_name != expected_skill_name:
        raise ValueError(
            f"Stage {stage_name} must execute {expected_skill_name}, but planner selected {normalized_skill_name or '<empty>'}."
        )
    return contract


def validate_stage_sequence(stage_names: list[str]) -> None:
    seen: set[str] = set()
    for raw_stage_name in stage_names:
        contract = stage_contract(raw_stage_name)
        stage_name = maybe_text(contract.get("stage_name"))
        if stage_name in seen:
            raise ValueError(f"Duplicate phase-2 stage detected: {stage_name}")
        for dependency in contract.get("required_previous_stages", []):
            dependency_name = maybe_text(dependency)
            if dependency_name and dependency_name not in seen:
                raise ValueError(f"Stage {stage_name} requires {dependency_name} to appear earlier in the controller sequence.")
        seen.add(stage_name)
