"""Governance checks and role-source selection helpers for orchestration planning."""

from __future__ import annotations

from typing import Any

from eco_council_runtime.controller.policy import allowed_sources_for_role, role_source_governance
from eco_council_runtime.domain.contract_bridge import contract_call
from eco_council_runtime.domain.text import maybe_text, unique_strings as shared_unique_strings

PUBLIC_SOURCES = (
    "gdelt-doc-search",
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
    "bluesky-cascade-fetch",
    "youtube-video-search",
    "youtube-comments-fetch",
    "federal-register-doc-fetch",
    "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch",
)
ENVIRONMENT_SOURCES = (
    "airnow-hourly-obs-fetch",
    "usgs-water-iv-fetch",
    "open-meteo-air-quality-fetch",
    "open-meteo-historical-fetch",
    "open-meteo-flood-fetch",
    "nasa-firms-fire-fetch",
    "openaq-data-fetch",
)
SUPPORTED_SOURCES_BY_ROLE = {
    "sociologist": list(PUBLIC_SOURCES),
    "environmentalist": list(ENVIRONMENT_SOURCES),
}


def unique_strings(values: list[str]) -> list[str]:
    return shared_unique_strings(values, casefold=True)


def role_supported_sources(role: str) -> list[str]:
    return list(SUPPORTED_SOURCES_BY_ROLE.get(role, []))


def source_selection_selected_sources(source_selection: dict[str, Any] | None) -> list[str]:
    if not isinstance(source_selection, dict):
        return []
    if maybe_text(source_selection.get("status")) == "pending":
        return []
    family_selected = contract_call("selected_sources_from_family_plans", source_selection)
    if isinstance(family_selected, list) and family_selected:
        return unique_strings([maybe_text(item) for item in family_selected if maybe_text(item)])
    value = source_selection.get("selected_sources")
    if not isinstance(value, list):
        return []
    return unique_strings([maybe_text(item) for item in value if maybe_text(item)])


def ensure_source_selection_respects_governance(
    *,
    mission: dict[str, Any],
    role: str,
    source_selection: dict[str, Any] | None,
) -> None:
    if not isinstance(source_selection, dict):
        return
    governance = role_source_governance(mission, role)
    families = governance.get("families") if isinstance(governance.get("families"), list) else []
    if not families:
        return
    family_plans = source_selection.get("family_plans")
    if not isinstance(family_plans, list):
        raise ValueError(f"Role {role} source_selection must include family_plans.")

    family_lookup: dict[str, dict[str, Any]] = {}
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        if family_id:
            family_lookup[family_id] = family
    payload_lookup: dict[str, dict[str, Any]] = {}
    for family_plan in family_plans:
        if not isinstance(family_plan, dict):
            continue
        family_id = maybe_text(family_plan.get("family_id"))
        if family_id:
            payload_lookup[family_id] = family_plan
    if set(payload_lookup) != set(family_lookup):
        missing = sorted(set(family_lookup) - set(payload_lookup))
        extra = sorted(set(payload_lookup) - set(family_lookup))
        raise ValueError(f"Role {role} family_plans must match governed families. Missing={missing}, extra={extra}")

    selected_sources = source_selection_selected_sources(source_selection)
    max_sources = governance.get("max_selected_sources_per_role")
    if isinstance(max_sources, int) and max_sources > 0 and len(selected_sources) > max_sources:
        raise ValueError(
            f"Role {role} selected {len(selected_sources)} sources but governance max_selected_sources_per_role={max_sources}."
        )

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

        for layer_plan in layer_plans:
            if not isinstance(layer_plan, dict):
                continue
            layer_id = maybe_text(layer_plan.get("layer_id"))
            layer_policy = layer_lookup.get(layer_id)
            if not isinstance(layer_policy, dict):
                continue
            if maybe_text(layer_plan.get("tier")) != maybe_text(layer_policy.get("tier")):
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
            max_selected_skills = layer_policy.get("max_selected_skills")
            if isinstance(max_selected_skills, int) and max_selected_skills > 0 and len(selected_skill_set) > max_selected_skills:
                raise ValueError(
                    f"Role {role} family {family_id} layer {layer_id} selected {len(selected_skill_set)} skills but max_selected_skills={max_selected_skills}."
                )
            if not selected:
                continue

            tier = maybe_text(layer_policy.get("tier"))
            authorization_basis = maybe_text(layer_plan.get("authorization_basis"))
            anchor_mode = maybe_text(layer_plan.get("anchor_mode"))
            anchor_refs = layer_plan.get("anchor_refs") if isinstance(layer_plan.get("anchor_refs"), list) else []
            if tier == "l2":
                selected_non_entry_layers += 1
            if layer_policy.get("requires_anchor") is True and (anchor_mode == "none" or not anchor_refs):
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} requires anchors.")
            if anchor_mode == "prior_round_l1" and not allow_cross_round_anchors:
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} cannot use prior_round_l1 anchors.")
            approval_key = (family_id, layer_id)
            if tier == "l1":
                if authorization_basis != "entry-layer":
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use entry-layer authorization.")
            else:
                auto_selectable = layer_policy.get("auto_selectable") is True
                if approval_key in approved_lookup:
                    if authorization_basis != "upstream-approval":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use upstream-approval.")
                elif auto_selectable:
                    if authorization_basis != "policy-auto":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use policy-auto authorization.")
                else:
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} is not approved by governance.")

    max_families = governance.get("max_active_families_per_role")
    if isinstance(max_families, int) and max_families > 0 and selected_family_count > max_families:
        raise ValueError(
            f"Role {role} selected {selected_family_count} families but governance max_active_families_per_role={max_families}."
        )
    max_l2_layers = governance.get("max_non_entry_layers_per_role")
    if isinstance(max_l2_layers, int) and max_l2_layers >= 0 and selected_non_entry_layers > max_l2_layers:
        raise ValueError(
            f"Role {role} selected {selected_non_entry_layers} non-entry layers but governance max_non_entry_layers_per_role={max_l2_layers}."
        )


def role_selected_sources(
    *,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    role: str,
    source_selection: dict[str, Any] | None,
) -> list[str]:
    del tasks
    ensure_source_selection_respects_governance(mission=mission, role=role, source_selection=source_selection)
    allowed = allowed_sources_for_role(mission, role)
    supported = role_supported_sources(role)
    allowed_lookup = {source.casefold() for source in allowed}
    supported_lookup = {source.casefold() for source in supported}
    selected_lookup = {
        source.casefold()
        for source in source_selection_selected_sources(source_selection)
        if source.casefold() in supported_lookup
    }
    if not selected_lookup:
        return []
    if not allowed_lookup:
        selected = sorted(selected_lookup)
        raise ValueError(f"Role {role} selected sources {selected}, but mission.source_governance exposes no allowed sources.")
    invalid = [source for source in supported if source.casefold() in selected_lookup and source.casefold() not in allowed_lookup]
    if invalid:
        raise ValueError(f"Role {role} selected unsupported or disallowed sources: {invalid}.")
    return [source for source in supported if source.casefold() in selected_lookup and source.casefold() in allowed_lookup]


__all__ = [
    "ENVIRONMENT_SOURCES",
    "PUBLIC_SOURCES",
    "SUPPORTED_SOURCES_BY_ROLE",
    "ensure_source_selection_respects_governance",
    "role_selected_sources",
    "role_supported_sources",
    "source_selection_selected_sources",
]
