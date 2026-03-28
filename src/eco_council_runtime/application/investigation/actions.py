"""Deterministic investigation-action planning and persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import write_json
from eco_council_runtime.controller.paths import investigation_actions_path
from eco_council_runtime.domain.contract_bridge import contract_call
from eco_council_runtime.domain.text import maybe_text, unique_strings
from eco_council_runtime.investigation import SOURCE_SKILLS_BY_GAP_TYPE, SOURCE_SKILLS_BY_METRIC_FAMILY
from eco_council_runtime.planning.next_round import NEXT_ACTION_LIBRARY

from .state import build_investigation_state_from_round_state, materialize_investigation_state

SCHEMA_VERSION = "1.0.0"
MAX_PRIMARY_HYPOTHESES = 3
MAX_ALTERNATIVES_PER_HYPOTHESIS = 2
MAX_RANKED_ACTIONS = 6

AUTO_SELECTABLE_COST = 1
APPROVED_LAYER_COST = 2
ANCHOR_REQUIRED_COST = 3
UNKNOWN_OPTION_COST = 4

ROLE_PRIORITY = {"environmentalist": 0, "sociologist": 1, "moderator": 2, "historian": 3}
ACTION_KIND_PRIORITY = {
    "resolve-required-leg": 0,
    "resolve-contradiction": 1,
    "test-alternative-hypothesis": 2,
    "expand-coverage": 3,
}


def _role_governance(mission: dict[str, Any], role: str) -> dict[str, Any]:
    governance = contract_call("source_governance", mission)
    if not isinstance(governance, dict):
        return {}
    families = [
        family
        for family in governance.get("families", [])
        if isinstance(family, dict) and maybe_text(family.get("role")) == role
    ]
    family_ids = {
        maybe_text(family.get("family_id"))
        for family in families
        if maybe_text(family.get("family_id"))
    }
    approved_layers = [
        approval
        for approval in governance.get("approved_layers", [])
        if isinstance(approval, dict) and maybe_text(approval.get("family_id")) in family_ids
    ]
    return {
        "families": families,
        "approved_lookup": {
            (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id"))): item
            for item in approved_layers
            if maybe_text(item.get("family_id")) and maybe_text(item.get("layer_id"))
        },
    }


def _governed_source_options(*, mission: dict[str, Any], role: str, gap_types: list[str]) -> list[dict[str, Any]]:
    governance = _role_governance(mission, role)
    families = governance.get("families", []) if isinstance(governance.get("families"), list) else []
    approved_lookup = governance.get("approved_lookup", {}) if isinstance(governance.get("approved_lookup"), dict) else {}
    desired_skills = unique_strings(
        [
            maybe_text(skill)
            for gap_type in gap_types
            for skill in SOURCE_SKILLS_BY_GAP_TYPE.get(gap_type, [])
            if maybe_text(skill)
        ]
    )
    desired_metric_families = unique_strings(
        [
            maybe_text(metric_family)
            for gap_type in gap_types
            for metric_family, skills in SOURCE_SKILLS_BY_METRIC_FAMILY.items()
            if any(skill in SOURCE_SKILLS_BY_GAP_TYPE.get(gap_type, []) for skill in skills)
        ]
    )
    options: list[dict[str, Any]] = []
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        family_label = maybe_text(family.get("label")) or family_id
        for layer in family.get("layers", []):
            if not isinstance(layer, dict):
                continue
            layer_id = maybe_text(layer.get("layer_id"))
            skills = unique_strings(
                [maybe_text(skill) for skill in layer.get("skills", []) if maybe_text(skill)]
            )
            if desired_skills:
                relevant_skills = [skill for skill in skills if skill in desired_skills]
                if not relevant_skills:
                    continue
            else:
                relevant_skills = list(skills)
            tier = maybe_text(layer.get("tier")) or "l1"
            requires_anchor = bool(layer.get("requires_anchor"))
            auto_selectable = bool(layer.get("auto_selectable"))
            approval = approved_lookup.get((family_id, layer_id))
            if approval is not None:
                approval_state = "approved-layer"
                estimated_cost = APPROVED_LAYER_COST
            elif auto_selectable and not requires_anchor:
                approval_state = "auto-selectable"
                estimated_cost = AUTO_SELECTABLE_COST
            elif requires_anchor:
                approval_state = "anchor-required"
                estimated_cost = ANCHOR_REQUIRED_COST
            else:
                approval_state = "governed"
                estimated_cost = UNKNOWN_OPTION_COST
            options.append(
                {
                    "family_id": family_id,
                    "family_label": family_label,
                    "layer_id": layer_id,
                    "layer_tier": tier,
                    "approval_state": approval_state,
                    "estimated_token_cost": estimated_cost,
                    "metric_families": desired_metric_families,
                    "source_skills": relevant_skills,
                    "requires_anchor": requires_anchor,
                    "reason": maybe_text(layer.get("description")) or maybe_text(family.get("label")),
                }
            )
    options.sort(
        key=lambda item: (
            int(item.get("estimated_token_cost") or UNKNOWN_OPTION_COST),
            maybe_text(item.get("family_id")),
            maybe_text(item.get("layer_id")),
        )
    )
    return options[:4]


def _gap_types_for_target(*, target: dict[str, Any], fallback_gap_types: list[str]) -> list[str]:
    gap_types = [
        maybe_text(item)
        for item in target.get("remaining_gaps", [])
        if maybe_text(item) in NEXT_ACTION_LIBRARY
    ]
    if gap_types:
        return unique_strings(gap_types)
    return unique_strings([maybe_text(item) for item in fallback_gap_types if maybe_text(item) in NEXT_ACTION_LIBRARY])


def _template_for_gap_types(gap_types: list[str]) -> tuple[str, dict[str, Any] | None]:
    for gap_type in gap_types:
        template = NEXT_ACTION_LIBRARY.get(gap_type)
        if isinstance(template, dict):
            return gap_type, template
    return "", None


def _anchor_refs(round_id: str, refs: list[str]) -> list[str]:
    normalized_round_id = maybe_text(round_id)
    anchors: list[str] = []
    for ref in refs:
        text = maybe_text(ref)
        if not text:
            continue
        if ":" not in text:
            continue
        if text.startswith("card:"):
            anchors.append(f"{normalized_round_id}:card:{text.split(':', 1)[1]}")
        elif text.startswith("claim:"):
            anchors.append(f"{normalized_round_id}:claim:{text.split(':', 1)[1]}")
        elif text.startswith("observation:"):
            anchors.append(f"{normalized_round_id}:observation:{text.split(':', 1)[1]}")
        elif text.startswith("isolated:"):
            anchors.append(f"{normalized_round_id}:isolated:{text.split(':', 1)[1]}")
        elif text.startswith("remand:"):
            anchors.append(f"{normalized_round_id}:remand:{text.split(':', 1)[1]}")
    return unique_strings(anchors)


def _required_leg_target(
    *,
    mission: dict[str, Any],
    round_id: str,
    hypothesis: dict[str, Any],
    leg: dict[str, Any],
) -> dict[str, Any] | None:
    gap_types = _gap_types_for_target(target=leg, fallback_gap_types=hypothesis.get("remaining_gaps", []))
    if not gap_types:
        return None
    resolved_gap_type, template = _template_for_gap_types(gap_types)
    if not isinstance(template, dict):
        return None
    role = maybe_text(template.get("assigned_role"))
    contradiction_count = int(leg.get("contradiction", {}).get("count") or 0) if isinstance(leg.get("contradiction"), dict) else 0
    pending_ref_count = (
        int(leg.get("coverage", {}).get("pending_ref_count") or 0) if isinstance(leg.get("coverage"), dict) else 0
    )
    governed_options = _governed_source_options(mission=mission, role=role, gap_types=gap_types)
    selection_reason_codes = ["required-leg-unresolved", "gap-type-derived", "budget-bounded"]
    if contradiction_count > 0:
        selection_reason_codes.append("contradiction-active")
    if governed_options:
        selection_reason_codes.append("governed-sources-available")
    expected_evidence_gain = min(3.0, 1.4 + 0.4 * len(gap_types) + 0.3 * pending_ref_count)
    contradiction_resolution_value = min(2.0, float(contradiction_count))
    coverage_gain = 2.5 if bool(leg.get("required")) else 1.0
    audit_clarity = min(
        2.0,
        0.7
        + (0.5 if governed_options else 0.0)
        + (0.4 if leg.get("latest_evidence_refs") else 0.0)
        + (0.4 if resolved_gap_type else 0.0),
    )
    token_cost_penalty = (
        float(governed_options[0].get("estimated_token_cost") or AUTO_SELECTABLE_COST) / 2.0
        if governed_options
        else 1.8
    ) + max(0.0, (len(governed_options) - 1) * 0.2)
    total = round(
        expected_evidence_gain + contradiction_resolution_value + coverage_gain + audit_clarity - token_cost_penalty,
        3,
    )
    candidate_kind = "resolve-contradiction" if contradiction_count > 0 else "resolve-required-leg"
    reason_prefix = "Resolve contradictory evidence" if contradiction_count > 0 else "Close the unresolved required leg"
    return {
        "candidate_kind": candidate_kind,
        "assigned_role": role,
        "priority": maybe_text(template.get("priority")) or "high",
        "objective": maybe_text(template.get("objective")),
        "reason": (
            f"{reason_prefix} `{maybe_text(leg.get('leg_id'))}` for {maybe_text(hypothesis.get('hypothesis_id'))}: "
            f"{maybe_text(template.get('reason'))}"
        ),
        "target": {
            "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
            "leg_id": maybe_text(leg.get("leg_id")),
            "gap_types": gap_types[:4],
            "coverage_status": maybe_text(leg.get("status")),
            "uncertainty_level": maybe_text(leg.get("uncertainty", {}).get("level"))
            if isinstance(leg.get("uncertainty"), dict)
            else "",
        },
        "evidence_refs": unique_strings(
            [maybe_text(item) for item in leg.get("latest_evidence_refs", []) if maybe_text(item)]
        )[:6],
        "anchor_refs": _anchor_refs(
            maybe_text(round_id),
            [maybe_text(item) for item in leg.get("latest_evidence_refs", []) if maybe_text(item)],
        )[:6],
        "governed_source_options": governed_options,
        "selection_reason_codes": selection_reason_codes,
        "score": {
            "total": total,
            "components": {
                "expected_evidence_gain": round(expected_evidence_gain, 3),
                "contradiction_resolution_value": round(contradiction_resolution_value, 3),
                "coverage_gain": round(coverage_gain, 3),
                "audit_clarity": round(audit_clarity, 3),
                "token_cost_penalty": round(token_cost_penalty, 3),
            },
        },
        "budget": {
            "token_cost_estimate": int(
                sum(int(item.get("estimated_token_cost") or 0) for item in governed_options[:2]) or UNKNOWN_OPTION_COST
            ),
            "governed_option_count": len(governed_options),
        },
    }


def _alternative_target(
    *,
    mission: dict[str, Any],
    round_id: str,
    hypothesis: dict[str, Any],
    alternative: dict[str, Any],
) -> dict[str, Any] | None:
    gap_types = _gap_types_for_target(target=alternative, fallback_gap_types=hypothesis.get("remaining_gaps", []))
    if not gap_types:
        return None
    resolved_gap_type, template = _template_for_gap_types(gap_types)
    if not isinstance(template, dict):
        return None
    role = maybe_text(template.get("assigned_role"))
    governed_options = _governed_source_options(mission=mission, role=role, gap_types=gap_types)
    coverage_status = maybe_text(alternative.get("coverage", {}).get("status")) if isinstance(alternative.get("coverage"), dict) else ""
    priority = maybe_text(alternative.get("priority")) or maybe_text(template.get("priority")) or "medium"
    contradiction_count = int(hypothesis.get("contradiction", {}).get("count") or 0) if isinstance(hypothesis.get("contradiction"), dict) else 0
    expected_evidence_gain = min(2.5, 1.2 + 0.4 * len(gap_types) + (0.4 if priority == "high" else 0.0))
    contradiction_resolution_value = min(1.8, 0.5 + contradiction_count * 0.5)
    coverage_gain = 1.6 if coverage_status in {"planned", "seeded"} else 0.8
    audit_clarity = min(1.8, 0.6 + (0.6 if governed_options else 0.0) + (0.3 if resolved_gap_type else 0.0))
    token_cost_penalty = (
        float(governed_options[0].get("estimated_token_cost") or AUTO_SELECTABLE_COST) / 1.8
        if governed_options
        else 1.6
    ) + max(0.0, (len(governed_options) - 1) * 0.15)
    total = round(
        expected_evidence_gain + contradiction_resolution_value + coverage_gain + audit_clarity - token_cost_penalty,
        3,
    )
    return {
        "candidate_kind": "test-alternative-hypothesis",
        "assigned_role": role,
        "priority": priority,
        "objective": maybe_text(template.get("objective")),
        "reason": (
            f"Keep alternative `{maybe_text(alternative.get('alternative_id'))}` testable for {maybe_text(hypothesis.get('hypothesis_id'))}: "
            f"{maybe_text(alternative.get('summary')) or maybe_text(template.get('reason'))}"
        ),
        "target": {
            "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
            "alternative_id": maybe_text(alternative.get("alternative_id")),
            "gap_types": gap_types[:4],
            "coverage_status": coverage_status,
            "uncertainty_level": maybe_text(alternative.get("uncertainty", {}).get("level"))
            if isinstance(alternative.get("uncertainty"), dict)
            else "",
        },
        "evidence_refs": unique_strings(
            [maybe_text(item) for item in hypothesis.get("latest_evidence_refs", []) if maybe_text(item)]
        )[:6],
        "anchor_refs": _anchor_refs(
            maybe_text(round_id),
            [maybe_text(item) for item in hypothesis.get("latest_evidence_refs", []) if maybe_text(item)],
        )[:6],
        "governed_source_options": governed_options,
        "selection_reason_codes": [
            "alternative-hypothesis-active",
            "gap-type-derived",
            "budget-bounded",
            *(
                ["governed-sources-available"]
                if governed_options
                else []
            ),
        ],
        "score": {
            "total": total,
            "components": {
                "expected_evidence_gain": round(expected_evidence_gain, 3),
                "contradiction_resolution_value": round(contradiction_resolution_value, 3),
                "coverage_gain": round(coverage_gain, 3),
                "audit_clarity": round(audit_clarity, 3),
                "token_cost_penalty": round(token_cost_penalty, 3),
            },
        },
        "budget": {
            "token_cost_estimate": int(
                sum(int(item.get("estimated_token_cost") or 0) for item in governed_options[:2]) or UNKNOWN_OPTION_COST
            ),
            "governed_option_count": len(governed_options),
        },
    }


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[float, int, int, str, str]:
    score = candidate.get("score", {}) if isinstance(candidate.get("score"), dict) else {}
    total = float(score.get("total") or 0.0)
    return (
        -total,
        ACTION_KIND_PRIORITY.get(maybe_text(candidate.get("candidate_kind")), 99),
        ROLE_PRIORITY.get(maybe_text(candidate.get("assigned_role")), 99),
        maybe_text(candidate.get("objective")),
        maybe_text(candidate.get("reason")),
    )


def _ranked_actions(candidates: list[dict[str, Any]], *, round_id: str) -> list[dict[str, Any]]:
    ranked = sorted(
        [
            candidate
            for candidate in candidates
            if isinstance(candidate, dict)
            and maybe_text(candidate.get("assigned_role"))
            and maybe_text(candidate.get("objective"))
            and maybe_text(candidate.get("reason"))
        ],
        key=_candidate_sort_key,
    )[:MAX_RANKED_ACTIONS]
    output: list[dict[str, Any]] = []
    for index, candidate in enumerate(ranked, start=1):
        item = dict(candidate)
        item["action_id"] = f"investigation-action-{maybe_text(round_id)}-{index:02d}"
        item["rank"] = index
        output.append(item)
    return output


def recommendations_from_investigation_actions(payload: dict[str, Any], *, limit: int = 4) -> list[dict[str, Any]]:
    actions = payload.get("ranked_actions") if isinstance(payload.get("ranked_actions"), list) else []
    recommendations: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for action in actions:
        if not isinstance(action, dict):
            continue
        role = maybe_text(action.get("assigned_role"))
        objective = maybe_text(action.get("objective"))
        reason = maybe_text(action.get("reason"))
        if not role or not objective or not reason:
            continue
        signature = (role, objective.casefold())
        if signature in seen:
            continue
        seen.add(signature)
        recommendations.append(
            {
                "assigned_role": role,
                "objective": objective,
                "reason": reason,
            }
        )
        if len(recommendations) >= limit:
            break
    return recommendations


def build_investigation_actions_from_round_state(state: dict[str, Any]) -> dict[str, Any]:
    mission = state["mission"]
    round_id = maybe_text(state.get("round_id"))
    investigation_state = (
        state.get("investigation_state")
        if isinstance(state.get("investigation_state"), dict) and state.get("investigation_state")
        else build_investigation_state_from_round_state(state)
    )
    hypotheses = (
        investigation_state.get("hypotheses")
        if isinstance(investigation_state.get("hypotheses"), list)
        else []
    )
    candidates: list[dict[str, Any]] = []
    for hypothesis in hypotheses[:MAX_PRIMARY_HYPOTHESES]:
        if not isinstance(hypothesis, dict):
            continue
        for leg in hypothesis.get("legs", []):
            if not isinstance(leg, dict) or not bool(leg.get("required")):
                continue
            if maybe_text(leg.get("status")) == "supported":
                continue
            candidate = _required_leg_target(
                mission=mission,
                round_id=round_id,
                hypothesis=hypothesis,
                leg=leg,
            )
            if candidate is not None:
                candidates.append(candidate)
        for alternative in (
            hypothesis.get("alternative_hypotheses")
            if isinstance(hypothesis.get("alternative_hypotheses"), list)
            else []
        )[:MAX_ALTERNATIVES_PER_HYPOTHESIS]:
            if not isinstance(alternative, dict):
                continue
            candidate = _alternative_target(
                mission=mission,
                round_id=round_id,
                hypothesis=hypothesis,
                alternative=alternative,
            )
            if candidate is not None:
                candidates.append(candidate)
    ranked_actions = _ranked_actions(candidates, round_id=round_id)
    estimated_token_cost = sum(
        int(item.get("budget", {}).get("token_cost_estimate") or 0)
        for item in ranked_actions
        if isinstance(item.get("budget"), dict)
    )
    role_counts: dict[str, int] = {}
    for item in ranked_actions:
        role = maybe_text(item.get("assigned_role"))
        if not role:
            continue
        role_counts[role] = role_counts.get(role, 0) + 1
    contradictory_leg_count = sum(
        1
        for hypothesis in hypotheses[:MAX_PRIMARY_HYPOTHESES]
        if isinstance(hypothesis, dict)
        for leg in hypothesis.get("legs", [])
        if isinstance(leg, dict) and int(leg.get("contradiction", {}).get("count") or 0) > 0
    )
    required_leg_gap_count = sum(
        1
        for hypothesis in hypotheses[:MAX_PRIMARY_HYPOTHESES]
        if isinstance(hypothesis, dict)
        for leg in hypothesis.get("legs", [])
        if isinstance(leg, dict) and bool(leg.get("required")) and maybe_text(leg.get("status")) != "supported"
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "actions_id": f"investigation-actions-{round_id}",
        "run_id": maybe_text(mission.get("run_id")),
        "round_id": round_id,
        "investigation_state_id": maybe_text(investigation_state.get("state_id")),
        "generated_from": {
            "overall_status": maybe_text(investigation_state.get("overall_status")),
            "last_update_stage": maybe_text(investigation_state.get("last_update_stage")),
            "last_update_round_id": maybe_text(investigation_state.get("last_update_round_id")),
        },
        "budget": {
            "max_primary_hypotheses": MAX_PRIMARY_HYPOTHESES,
            "max_alternatives_per_hypothesis": MAX_ALTERNATIVES_PER_HYPOTHESIS,
            "max_ranked_actions": MAX_RANKED_ACTIONS,
            "candidate_count": len(candidates),
            "returned_count": len(ranked_actions),
            "truncated_by_cap": len(candidates) > len(ranked_actions),
            "estimated_token_cost": estimated_token_cost,
        },
        "summary": {
            "primary_hypothesis_count": min(
                len(hypotheses),
                MAX_PRIMARY_HYPOTHESES,
            ),
            "alternative_hypothesis_count": sum(
                len(hypothesis.get("alternative_hypotheses", [])[:MAX_ALTERNATIVES_PER_HYPOTHESIS])
                for hypothesis in hypotheses[:MAX_PRIMARY_HYPOTHESES]
                if isinstance(hypothesis, dict) and isinstance(hypothesis.get("alternative_hypotheses"), list)
            ),
            "required_leg_gap_count": required_leg_gap_count,
            "contradictory_leg_count": contradictory_leg_count,
            "role_counts": role_counts,
        },
        "ranked_actions": ranked_actions,
    }
    return payload


def materialize_investigation_actions(run_dir: Path, round_id: str, *, pretty: bool = True) -> dict[str, Any]:
    from eco_council_runtime.application.reporting_state import collect_round_state

    state = collect_round_state(run_dir, round_id)
    payload = build_investigation_actions_from_round_state(state)
    target_path = investigation_actions_path(run_dir, round_id)
    write_json(target_path, payload, pretty=pretty)
    return {
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "ranked_action_count": len(payload.get("ranked_actions", []))
        if isinstance(payload.get("ranked_actions"), list)
        else 0,
        "estimated_token_cost": int(payload.get("budget", {}).get("estimated_token_cost") or 0)
        if isinstance(payload.get("budget"), dict)
        else 0,
        "investigation_actions_path": str(target_path),
    }


def materialize_investigation_bundle(run_dir: Path, round_id: str, *, pretty: bool = True) -> dict[str, Any]:
    state_result = materialize_investigation_state(run_dir, round_id, pretty=pretty)
    actions_result = materialize_investigation_actions(run_dir, round_id, pretty=pretty)
    return {
        "investigation_state": state_result,
        "investigation_actions": actions_result,
    }


__all__ = [
    "build_investigation_actions_from_round_state",
    "materialize_investigation_bundle",
    "materialize_investigation_actions",
    "recommendations_from_investigation_actions",
]
