"""Mission-policy and governance helpers for the eco-council controller."""

from __future__ import annotations

import json
from typing import Any

from eco_council_runtime.controller.constants import ROLES
from eco_council_runtime.controller.io import load_json_if_exists
from eco_council_runtime.controller.paths import (
    fetch_execution_path,
    override_requests_path,
    prior_round_ids,
    source_selection_path,
)
from eco_council_runtime.domain.contract_bridge import contract_call, resolve_schema_version
from eco_council_runtime.domain.text import maybe_text


def effective_constraints(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("effective_constraints", mission)
    if isinstance(value, dict):
        return value
    constraints = mission.get("constraints")
    return constraints if isinstance(constraints, dict) else {}


def effective_matching_authorization_payload(*, mission: dict[str, Any], round_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("apply_matching_authorization_policy", mission, round_id, payload)
    if isinstance(value, dict):
        return value
    return dict(payload)


def policy_profile_summary(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("policy_profile_summary", mission)
    if isinstance(value, dict):
        return value
    return {}


def load_override_requests(run_dir, round_id: str, role: str | None = None) -> list[dict[str, Any]]:
    roles = (role,) if role else ROLES
    output: list[dict[str, Any]] = []
    for role_name in roles:
        payload = load_json_if_exists(override_requests_path(run_dir, round_id, role_name))
        if not isinstance(payload, list):
            continue
        output.extend(item for item in payload if isinstance(item, dict))
    output.sort(key=lambda item: maybe_text(item.get("request_id")))
    return output


def role_source_governance(mission: dict[str, Any], role: str) -> dict[str, Any]:
    governance = contract_call("source_governance", mission)
    if not isinstance(governance, dict):
        return {}
    family_lookup = contract_call("source_family_lookup", mission, role=role)
    if isinstance(family_lookup, dict):
        role_families = list(family_lookup.values())
    else:
        role_families = [
            family
            for family in governance.get("families", [])
            if isinstance(family, dict) and maybe_text(family.get("role")) == role
        ]
    family_ids = {maybe_text(family.get("family_id")) for family in role_families if maybe_text(family.get("family_id"))}
    approved_layers = [
        approval
        for approval in governance.get("approved_layers", [])
        if isinstance(approval, dict) and maybe_text(approval.get("family_id")) in family_ids
    ]
    return {
        "approval_authority": maybe_text(governance.get("approval_authority")),
        "allow_cross_round_anchors": bool(governance.get("allow_cross_round_anchors")),
        "max_selected_sources_per_role": governance.get("max_selected_sources_per_role"),
        "max_active_families_per_role": governance.get("max_active_families_per_role"),
        "max_non_entry_layers_per_role": governance.get("max_non_entry_layers_per_role"),
        "approved_layers": approved_layers,
        "families": role_families,
    }


def role_evidence_requirements(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    requirements: list[dict[str, Any]] = []
    seen: set[str] = set()
    for task in tasks:
        inputs = task.get("inputs")
        if not isinstance(inputs, dict):
            continue
        values = inputs.get("evidence_requirements")
        if not isinstance(values, list):
            continue
        for requirement in values:
            if not isinstance(requirement, dict):
                continue
            requirement_id = maybe_text(requirement.get("requirement_id"))
            if not requirement_id or requirement_id.casefold() in seen:
                continue
            seen.add(requirement_id.casefold())
            requirements.append(json.loads(json.dumps(requirement)))
    return requirements


def role_family_memory(run_dir, round_id: str, role: str, mission: dict[str, Any]) -> list[dict[str, Any]]:
    governance = role_source_governance(mission, role)
    families = governance.get("families", []) if isinstance(governance.get("families"), list) else []
    if not families:
        return []

    prior_ids = prior_round_ids(run_dir, round_id)
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

        for observed_round_id in prior_ids:
            selection = load_json_if_exists(source_selection_path(run_dir, observed_round_id, role))
            if not isinstance(selection, dict):
                continue
            family_plans = selection.get("family_plans")
            if not isinstance(family_plans, list):
                continue
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
            layer_plans = matching_family.get("layer_plans")
            selected_layers: list[str] = []
            selected_sources: list[str] = []
            if isinstance(layer_plans, list):
                for layer_plan in layer_plans:
                    if not isinstance(layer_plan, dict) or layer_plan.get("selected") is not True:
                        continue
                    layer_id = maybe_text(layer_plan.get("layer_id"))
                    if layer_id:
                        selected_layers.append(layer_id)
                    skills = layer_plan.get("source_skills")
                    if isinstance(skills, list):
                        selected_sources.extend(maybe_text(skill) for skill in skills if maybe_text(skill))

            fetch_payload = load_json_if_exists(fetch_execution_path(run_dir, observed_round_id))
            statuses = fetch_payload.get("statuses") if isinstance(fetch_payload, dict) else []
            completed_in_round: list[str] = []
            if isinstance(statuses, list):
                for status in statuses:
                    if not isinstance(status, dict):
                        continue
                    if maybe_text(status.get("status")) != "completed":
                        continue
                    if maybe_text(status.get("assigned_role")) != role:
                        continue
                    source_skill = maybe_text(status.get("source_skill")) or maybe_text(status.get("source"))
                    if source_skill and source_skill in family_skills:
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


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    values = contract_call("allowed_sources_for_role", mission, role)
    if isinstance(values, list):
        return sorted({maybe_text(item) for item in values if maybe_text(item)})
    return []
