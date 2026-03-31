from __future__ import annotations

import json
from pathlib import Path
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
from .source_queue_history import role_family_memory


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


def infer_same_round_anchor_refs(selected_lookup: set[str], anchor_source_skills: list[str]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for source_skill in anchor_source_skills:
        if source_skill.casefold() not in selected_lookup:
            continue
        refs.append({"source_skill": source_skill, "scope": "current-round"})
    return refs


def infer_prior_round_anchor_refs(
    family_memory: list[dict[str, Any]],
    *,
    family_id: str,
    anchor_source_skills: list[str],
) -> list[dict[str, str]]:
    matching_family = next(
        (
            item
            for item in family_memory
            if isinstance(item, dict) and maybe_text(item.get("family_id")) == family_id
        ),
        None,
    )
    if not isinstance(matching_family, dict):
        return []
    prior_rounds = matching_family.get("prior_rounds") if isinstance(matching_family.get("prior_rounds"), list) else []
    for prior_round in reversed(prior_rounds):
        if not isinstance(prior_round, dict):
            continue
        round_id = maybe_text(prior_round.get("round_id"))
        completed = {
            maybe_text(source_skill)
            for source_skill in prior_round.get("completed_sources", [])
            if maybe_text(source_skill)
        }
        refs = [
            {"source_skill": source_skill, "round_id": round_id, "scope": "prior-round"}
            for source_skill in anchor_source_skills
            if source_skill in completed and round_id
        ]
        if refs:
            return refs
    return []


def family_plans_for_role(
    mission: dict[str, Any],
    role: str,
    selected_sources: list[str],
    family_memory: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    selected_lookup = {item.casefold() for item in selected_sources}
    governance = role_source_governance(mission, role)
    approved_layers = {
        (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id")))
        for item in governance.get("approved_layers", [])
        if isinstance(item, dict)
    }
    allow_cross_round_anchors = bool(governance.get("allow_cross_round_anchors"))
    memory = family_memory or []
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
            tier = maybe_text(layer.get("tier")) or "l1"
            authorization_basis = "entry-layer"
            if tier != "l1":
                if (family_id, maybe_text(layer.get("layer_id"))) in approved_layers:
                    authorization_basis = "upstream-approval"
                elif layer.get("auto_selectable") is True:
                    authorization_basis = "policy-auto"
            anchor_mode = "none"
            anchor_refs: list[dict[str, str]] = []
            if selected and layer.get("requires_anchor") is True:
                anchor_source_skills = unique_texts(layer.get("anchor_source_skills", []) if isinstance(layer.get("anchor_source_skills"), list) else [])
                anchor_refs = infer_same_round_anchor_refs(selected_lookup, anchor_source_skills)
                if anchor_refs:
                    anchor_mode = "same-round-source"
                elif allow_cross_round_anchors:
                    anchor_refs = infer_prior_round_anchor_refs(memory, family_id=family_id, anchor_source_skills=anchor_source_skills)
                    if anchor_refs:
                        anchor_mode = "prior_round_l1"
            layer_plans.append(
                {
                    "layer_id": maybe_text(layer.get("layer_id")),
                    "tier": tier,
                    "selected": selected,
                    "reason": f"{'Select' if selected else 'Skip'} {family_id}:{maybe_text(layer.get('layer_id'))}.",
                    "source_skills": selected_layer_skills,
                    "anchor_mode": anchor_mode,
                    "anchor_refs": anchor_refs,
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


def validate_source_selection_payload(*, mission: dict[str, Any], role: str, source_selection: dict[str, Any]) -> None:
    governance = role_source_governance(mission, role)
    families = governance.get("families", []) if isinstance(governance.get("families"), list) else []
    if not families:
        return

    family_lookup = {
        maybe_text(family.get("family_id")): family
        for family in families
        if isinstance(family, dict) and maybe_text(family.get("family_id"))
    }
    family_plans = source_selection.get("family_plans")
    if not isinstance(family_plans, list):
        raise ValueError(f"Role {role} source_selection must include family_plans.")
    payload_lookup = {
        maybe_text(family_plan.get("family_id")): family_plan
        for family_plan in family_plans
        if isinstance(family_plan, dict) and maybe_text(family_plan.get("family_id"))
    }
    if set(payload_lookup) != set(family_lookup):
        missing = sorted(set(family_lookup) - set(payload_lookup))
        extra = sorted(set(payload_lookup) - set(family_lookup))
        raise ValueError(f"Role {role} family_plans must match governed families. Missing={missing}, extra={extra}")

    explicit_selected_sources = unique_texts(source_selection.get("selected_sources", []) if isinstance(source_selection.get("selected_sources"), list) else [])
    layer_selected_sources: list[str] = []
    allowed_lookup = {item.casefold() for item in allowed_sources_for_role(mission, role)}
    approved_lookup = {
        (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id"))): item
        for item in governance.get("approved_layers", [])
        if isinstance(item, dict) and maybe_text(item.get("family_id")) and maybe_text(item.get("layer_id"))
    }
    allow_cross_round_anchors = bool(governance.get("allow_cross_round_anchors"))
    selected_family_count = 0
    selected_non_entry_layers = 0

    for family_id, family_plan in payload_lookup.items():
        family_policy = family_lookup.get(family_id)
        if not isinstance(family_policy, dict):
            continue
        if family_plan.get("selected") is True:
            selected_family_count += 1
        layer_lookup = {
            maybe_text(layer.get("layer_id")): layer
            for layer in family_policy.get("layers", [])
            if isinstance(layer, dict) and maybe_text(layer.get("layer_id"))
        }
        layer_plans = family_plan.get("layer_plans")
        if not isinstance(layer_plans, list):
            raise ValueError(f"Role {role} family {family_id} must include layer_plans.")
        payload_layer_ids = {
            maybe_text(layer_plan.get("layer_id"))
            for layer_plan in layer_plans
            if isinstance(layer_plan, dict) and maybe_text(layer_plan.get("layer_id"))
        }
        if set(layer_lookup) != payload_layer_ids:
            missing = sorted(set(layer_lookup) - payload_layer_ids)
            extra = sorted(payload_layer_ids - set(layer_lookup))
            raise ValueError(f"Role {role} family {family_id} layer_plans mismatch. Missing={missing}, extra={extra}")

        family_selected_from_layers = False
        for layer_plan in layer_plans:
            if not isinstance(layer_plan, dict):
                continue
            layer_id = maybe_text(layer_plan.get("layer_id"))
            layer_policy = layer_lookup.get(layer_id)
            if not isinstance(layer_policy, dict):
                continue
            tier = maybe_text(layer_policy.get("tier")) or "l1"
            if maybe_text(layer_plan.get("tier")) != tier:
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} tier mismatch.")

            selected = layer_plan.get("selected") is True
            selected_skill_set = {
                maybe_text(skill)
                for skill in layer_plan.get("source_skills", [])
                if maybe_text(skill)
            }
            allowed_skill_set = {
                maybe_text(skill)
                for skill in layer_policy.get("skills", [])
                if maybe_text(skill)
            }
            if not selected_skill_set <= allowed_skill_set:
                invalid = sorted(selected_skill_set - allowed_skill_set)
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} selected invalid skills {invalid}.")
            if not selected and selected_skill_set:
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} cannot list source_skills when selected=false.")
            if selected and not selected_skill_set:
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} must list at least one selected source_skill.")
            if selected:
                family_selected_from_layers = True
                layer_selected_sources.extend(sorted(selected_skill_set))

            max_selected_skills = layer_policy.get("max_selected_skills")
            if isinstance(max_selected_skills, int) and max_selected_skills > 0 and len(selected_skill_set) > max_selected_skills:
                raise ValueError(
                    f"Role {role} family {family_id} layer {layer_id} selected {len(selected_skill_set)} skills but max_selected_skills={max_selected_skills}."
                )

            if not selected:
                anchor_mode = maybe_text(layer_plan.get("anchor_mode")) or "none"
                anchor_refs = layer_plan.get("anchor_refs") if isinstance(layer_plan.get("anchor_refs"), list) else []
                if anchor_mode != "none" or anchor_refs:
                    raise ValueError(
                        f"Role {role} family {family_id} layer {layer_id} cannot declare anchors when selected=false."
                    )
                continue

            anchor_mode = maybe_text(layer_plan.get("anchor_mode")) or "none"
            anchor_refs = layer_plan.get("anchor_refs") if isinstance(layer_plan.get("anchor_refs"), list) else []
            if layer_policy.get("requires_anchor") is True and (anchor_mode == "none" or not anchor_refs):
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} requires anchors.")
            if anchor_mode == "prior_round_l1" and not allow_cross_round_anchors:
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} cannot use prior_round_l1 anchors.")

            authorization_basis = maybe_text(layer_plan.get("authorization_basis"))
            if tier == "l1":
                if authorization_basis and authorization_basis != "entry-layer":
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use entry-layer authorization.")
            else:
                selected_non_entry_layers += 1
                if (family_id, layer_id) in approved_lookup:
                    if authorization_basis != "upstream-approval":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use upstream-approval.")
                elif layer_policy.get("auto_selectable") is True:
                    if authorization_basis != "policy-auto":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use policy-auto authorization.")
                else:
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} is not approved by governance.")

        if family_plan.get("selected") is True and not family_selected_from_layers:
            raise ValueError(f"Role {role} family {family_id} selected=true but no layer is selected.")
        if family_plan.get("selected") is not True and family_selected_from_layers:
            raise ValueError(f"Role {role} family {family_id} selected flag must match selected layers.")

    normalized_layer_selected_sources = unique_texts(layer_selected_sources)
    if explicit_selected_sources and {
        item.casefold() for item in explicit_selected_sources
    } != {
        item.casefold() for item in normalized_layer_selected_sources
    }:
        raise ValueError(
            f"Role {role} selected_sources does not match selected family layers. selected_sources={explicit_selected_sources}, layer_sources={normalized_layer_selected_sources}"
        )
    selected_sources = explicit_selected_sources or normalized_layer_selected_sources
    invalid_sources = [item for item in selected_sources if item.casefold() not in allowed_lookup]
    if invalid_sources:
        raise ValueError(f"Role {role} selected invalid sources: {', '.join(invalid_sources)}")

    max_selected = governance.get("max_selected_sources_per_role")
    if isinstance(max_selected, int) and max_selected > 0 and len(selected_sources) > max_selected:
        raise ValueError(f"Role {role} selected too many sources: {len(selected_sources)} > {max_selected}")

    max_families = governance.get("max_active_families_per_role")
    if isinstance(max_families, int) and max_families > 0 and selected_family_count > max_families:
        raise ValueError(f"Role {role} selected {selected_family_count} families but max_active_families_per_role={max_families}.")

    max_l2_layers = governance.get("max_non_entry_layers_per_role")
    if isinstance(max_l2_layers, int) and max_l2_layers >= 0 and selected_non_entry_layers > max_l2_layers:
        raise ValueError(
            f"Role {role} selected {selected_non_entry_layers} non-entry layers but max_non_entry_layers_per_role={max_l2_layers}."
        )


def build_source_selection(
    *,
    run_dir: str | Path | None = None,
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
    family_memory = role_family_memory(Path(run_dir).expanduser().resolve(), round_id, role, mission) if run_dir is not None else []
    explicit_family_plans = (explicit_payload or {}).get("family_plans")
    if isinstance(explicit_family_plans, list):
        family_plans = json.loads(json.dumps(explicit_family_plans, ensure_ascii=True))
    else:
        family_plans = family_plans_for_role(mission, role, selected_sources, family_memory)
    for family in family_plans:
        family["evidence_requirement_ids"] = evidence_requirement_ids
    payload = {
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
        "family_memory": family_memory,
        "source_decisions": source_decisions_for_role(mission, role, selected_sources),
    }
    validate_source_selection_payload(mission=mission, role=role, source_selection=payload)
    return payload


def build_source_selections(
    *,
    run_dir: str | Path | None = None,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    run_id: str,
    round_id: str,
) -> dict[str, dict[str, Any]]:
    return {
        role: build_source_selection(run_dir=run_dir, mission=mission, tasks=tasks, run_id=run_id, round_id=round_id, role=role)
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
    "validate_source_selection_payload",
]
