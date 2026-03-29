from __future__ import annotations

import json
from typing import Any

from .source_queue_contract import (
    SOURCE_SELECTION_ROLES,
    allowed_sources_for_role,
    maybe_text,
    normalize_artifact_imports,
    normalize_source_requests,
    role_source_governance,
    stable_hash,
    unique_texts,
)


def task_ids_for_role(tasks: list[dict[str, Any]], role: str) -> list[str]:
    return [maybe_text(task.get("task_id")) for task in tasks if maybe_text(task.get("assigned_role")) == role and maybe_text(task.get("task_id"))]


def tasks_for_role(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [task for task in tasks if maybe_text(task.get("assigned_role")) == role]


def role_evidence_requirements(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    requirements: list[dict[str, Any]] = []
    seen: set[str] = set()
    for task in tasks:
        inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
        values = inputs.get("evidence_requirements") if isinstance(inputs.get("evidence_requirements"), list) else []
        for requirement in values:
            if not isinstance(requirement, dict):
                continue
            requirement_id = maybe_text(requirement.get("requirement_id"))
            if not requirement_id or requirement_id.casefold() in seen:
                continue
            seen.add(requirement_id.casefold())
            requirements.append(json.loads(json.dumps(requirement, ensure_ascii=True)))
    return requirements


def selected_sources_from_payload(payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(payload, dict):
        return []
    selected_sources = payload.get("selected_sources")
    if isinstance(selected_sources, list):
        return unique_texts(selected_sources)
    family_plans = payload.get("family_plans") if isinstance(payload.get("family_plans"), list) else []
    values: list[str] = []
    for family in family_plans:
        if not isinstance(family, dict):
            continue
        layers = family.get("layer_plans") if isinstance(family.get("layer_plans"), list) else []
        for layer in layers:
            if not isinstance(layer, dict) or layer.get("selected") is not True:
                continue
            skills = layer.get("source_skills") if isinstance(layer.get("source_skills"), list) else []
            values.extend(skills)
    return unique_texts(values)


def infer_selected_sources(mission: dict[str, Any], role: str) -> list[str]:
    values: list[str] = []
    for item in normalize_artifact_imports(mission):
        if maybe_text(item.get("role")) == role:
            values.append(item.get("source_skill"))
    for item in normalize_source_requests(mission):
        if maybe_text(item.get("role")) == role:
            values.append(item.get("source_skill"))
    return unique_texts(values)


def source_decisions_for_role(mission: dict[str, Any], role: str, selected_sources: list[str]) -> list[dict[str, str | bool]]:
    selected_lookup = {item.casefold() for item in selected_sources}
    decisions: list[dict[str, str | bool]] = []
    for source_skill in allowed_sources_for_role(mission, role):
        selected = source_skill.casefold() in selected_lookup
        decisions.append(
            {
                "source_skill": source_skill,
                "selected": selected,
                "reason": f"{'Selected' if selected else 'Not selected'} for {role}.",
            }
        )
    return decisions


def family_plans_for_role(mission: dict[str, Any], role: str, selected_sources: list[str]) -> list[dict[str, Any]]:
    selected_lookup = {item.casefold() for item in selected_sources}
    governance = role_source_governance(mission, role)
    approved_layers = {
        (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id")))
        for item in governance.get("approved_layers", [])
        if isinstance(item, dict)
    }
    plans: list[dict[str, Any]] = []
    for family in governance.get("families", []):
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        layer_plans: list[dict[str, Any]] = []
        family_selected = False
        for layer in family.get("layers", []):
            if not isinstance(layer, dict):
                continue
            layer_skills = unique_texts(layer.get("skills", []) if isinstance(layer.get("skills"), list) else [])
            selected_layer_skills = [skill for skill in layer_skills if skill.casefold() in selected_lookup]
            selected = bool(selected_layer_skills)
            if selected:
                family_selected = True
            authorization_basis = "entry-layer"
            if (family_id, maybe_text(layer.get("layer_id"))) in approved_layers:
                authorization_basis = "upstream-approval"
            layer_plans.append(
                {
                    "layer_id": maybe_text(layer.get("layer_id")),
                    "tier": maybe_text(layer.get("tier")) or "l1",
                    "selected": selected,
                    "reason": f"{'Select' if selected else 'Skip'} {family_id}:{maybe_text(layer.get('layer_id'))}.",
                    "source_skills": selected_layer_skills,
                    "anchor_mode": "none",
                    "anchor_refs": [],
                    "authorization_basis": authorization_basis,
                }
            )
        plans.append(
            {
                "family_id": family_id,
                "selected": family_selected,
                "reason": f"{'Use' if family_selected else 'Skip'} {family_id}.",
                "evidence_requirement_ids": [],
                "layer_plans": layer_plans,
            }
        )
    return plans


def build_source_selection(
    *,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    run_id: str,
    round_id: str,
    role: str,
) -> dict[str, Any]:
    explicit = mission.get("source_selections") if isinstance(mission.get("source_selections"), dict) else {}
    explicit_payload = explicit.get(role) if isinstance(explicit.get(role), dict) else None
    selected_sources = selected_sources_from_payload(explicit_payload)
    if not selected_sources:
        selected_sources = infer_selected_sources(mission, role)

    allowed_sources = allowed_sources_for_role(mission, role)
    allowed_lookup = {item.casefold() for item in allowed_sources}
    invalid = [item for item in selected_sources if item.casefold() not in allowed_lookup]
    if invalid:
        raise ValueError(f"Role {role} selected invalid sources: {', '.join(invalid)}")

    max_selected = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    max_selected_value = max_selected.get("max_selected_sources_per_role")
    try:
        max_selected_count = int(max_selected_value) if max_selected_value not in (None, "") else 4
    except (TypeError, ValueError):
        max_selected_count = 4
    if len(selected_sources) > max_selected_count:
        raise ValueError(f"Role {role} selected too many sources: {len(selected_sources)} > {max_selected_count}")

    role_tasks = tasks_for_role(tasks, role)
    status = maybe_text((explicit_payload or {}).get("status")) or ("complete" if selected_sources else "pending")
    override_requests = (explicit_payload or {}).get("override_requests") if isinstance((explicit_payload or {}).get("override_requests"), list) else []
    selection_id = maybe_text((explicit_payload or {}).get("selection_id")) or f"source-selection-{role}-" + stable_hash(run_id, round_id, role, *selected_sources)[:12]
    evidence_requirements = role_evidence_requirements(role_tasks)
    evidence_requirement_ids = [
        maybe_text(item.get("requirement_id"))
        for item in evidence_requirements
        if isinstance(item, dict) and maybe_text(item.get("requirement_id"))
    ]
    family_plans = family_plans_for_role(mission, role, selected_sources)
    for family in family_plans:
        family["evidence_requirement_ids"] = evidence_requirement_ids
    return {
        "schema_version": "1.0.0",
        "selection_id": selection_id,
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": status,
        "summary": maybe_text((explicit_payload or {}).get("summary")) or f"Selection for {role}.",
        "task_ids": task_ids_for_role(tasks, role),
        "allowed_sources": allowed_sources,
        "selected_sources": selected_sources,
        "override_requests": [item for item in override_requests if isinstance(item, dict)],
        "evidence_requirements": evidence_requirements,
        "family_plans": family_plans,
        "source_decisions": source_decisions_for_role(mission, role, selected_sources),
    }


def build_source_selections(
    *,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    run_id: str,
    round_id: str,
) -> dict[str, dict[str, Any]]:
    return {
        role: build_source_selection(mission=mission, tasks=tasks, run_id=run_id, round_id=round_id, role=role)
        for role in SOURCE_SELECTION_ROLES
    }


def role_selected_sources(source_selection: dict[str, Any] | None) -> list[str]:
    return selected_sources_from_payload(source_selection)


__all__ = [
    "SOURCE_SELECTION_ROLES",
    "build_source_selection",
    "build_source_selections",
    "role_evidence_requirements",
    "role_selected_sources",
    "selected_sources_from_payload",
    "task_ids_for_role",
    "tasks_for_role",
]