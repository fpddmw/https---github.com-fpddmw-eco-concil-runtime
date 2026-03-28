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
MAX_DISCOVERY_PROBES = 2
MAX_DISCOVERY_OPTIONS = 3

AUTO_SELECTABLE_COST = 1
APPROVED_LAYER_COST = 2
ANCHOR_REQUIRED_COST = 3
UNKNOWN_OPTION_COST = 4

ROLE_PRIORITY = {"environmentalist": 0, "sociologist": 1, "moderator": 2, "historian": 3}
ACTION_KIND_PRIORITY = {
    "resolve-required-leg": 0,
    "resolve-contradiction": 1,
    "test-alternative-hypothesis": 2,
    "governed-discovery-probe": 3,
    "expand-coverage": 4,
}

DISCOVERY_PUBLIC_TOKENS = {"claim", "comment", "coverage", "discussion", "public", "social", "policy"}
DISCOVERY_ENVIRONMENT_TOKENS = {
    "air",
    "fire",
    "flood",
    "heat",
    "hydrology",
    "meteorology",
    "pm",
    "precipitation",
    "smoke",
    "soil",
    "station",
    "temperature",
    "water",
    "weather",
    "wind",
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


def _unmapped_gap_types(*, target: dict[str, Any], fallback_gap_types: list[str]) -> list[str]:
    raw_gap_types = unique_strings(
        [
            maybe_text(item)
            for item in target.get("remaining_gaps", [])
            if maybe_text(item)
        ]
        + [maybe_text(item) for item in fallback_gap_types if maybe_text(item)]
    )
    return [gap_type for gap_type in raw_gap_types if gap_type not in NEXT_ACTION_LIBRARY]


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


def _probe_role_candidates(gap_types: list[str]) -> list[str]:
    probe_text = " ".join(maybe_text(item).lower() for item in gap_types if maybe_text(item))
    if any(token in probe_text for token in DISCOVERY_PUBLIC_TOKENS):
        return ["sociologist", "environmentalist"]
    if any(token in probe_text for token in DISCOVERY_ENVIRONMENT_TOKENS):
        return ["environmentalist", "sociologist"]
    return ["environmentalist", "sociologist"]


def _discovery_probe_target(
    *,
    mission: dict[str, Any],
    round_id: str,
    hypothesis: dict[str, Any],
) -> dict[str, Any] | None:
    remaining_gaps = [maybe_text(item) for item in hypothesis.get("remaining_gaps", []) if maybe_text(item)]
    atypical_gap_types = _unmapped_gap_types(target=hypothesis, fallback_gap_types=[])
    known_gap_types = [gap_type for gap_type in remaining_gaps if gap_type in NEXT_ACTION_LIBRARY]
    evidence_refs = unique_strings(
        [maybe_text(item) for item in hypothesis.get("latest_evidence_refs", []) if maybe_text(item)]
    )
    unresolved_required_leg_count = sum(
        1
        for leg in hypothesis.get("legs", [])
        if isinstance(leg, dict) and bool(leg.get("required")) and maybe_text(leg.get("status")) != "supported"
    )
    contradiction_count = int(hypothesis.get("contradiction", {}).get("count") or 0) if isinstance(hypothesis.get("contradiction"), dict) else 0
    low_evidence_density = len(evidence_refs) <= 1
    if not atypical_gap_types and not (low_evidence_density and unresolved_required_leg_count > 0):
        return None

    assigned_role = ""
    governed_options: list[dict[str, Any]] = []
    probe_gap_types = atypical_gap_types or known_gap_types
    for role in _probe_role_candidates(probe_gap_types):
        options = _governed_source_options(mission=mission, role=role, gap_types=known_gap_types)[:MAX_DISCOVERY_OPTIONS]
        if options:
            assigned_role = role
            governed_options = options
            break
    if not assigned_role:
        assigned_role = _probe_role_candidates(probe_gap_types)[0]

    selection_reason_codes = ["governed-discovery-probe", "budget-bounded"]
    if atypical_gap_types:
        selection_reason_codes.append("atypical-gap-types")
    if low_evidence_density:
        selection_reason_codes.append("low-evidence-density")
    if contradiction_count > 0:
        selection_reason_codes.append("contradiction-active")
    if governed_options:
        selection_reason_codes.append("governed-sources-available")

    expected_evidence_gain = min(2.8, 1.3 + (0.5 * unresolved_required_leg_count) + (0.4 if low_evidence_density else 0.0))
    contradiction_resolution_value = min(1.5, contradiction_count * 0.5)
    coverage_gain = min(2.0, 0.8 + (0.3 * unresolved_required_leg_count) + (0.4 if atypical_gap_types else 0.0))
    novelty_gain = 1.3 if atypical_gap_types else 0.8
    audit_clarity = min(2.0, 0.9 + (0.4 if governed_options else 0.0) + (0.3 if evidence_refs else 0.0))
    token_cost_penalty = (
        float(governed_options[0].get("estimated_token_cost") or AUTO_SELECTABLE_COST) / 2.0
        if governed_options
        else 1.8
    ) + max(0.0, (len(governed_options) - 1) * 0.2)
    total = round(
        expected_evidence_gain + contradiction_resolution_value + coverage_gain + novelty_gain + audit_clarity - token_cost_penalty,
        3,
    )

    question_parts: list[str] = []
    if atypical_gap_types:
        question_parts.append(f"Which governed source family could best test atypical gaps: {', '.join(atypical_gap_types[:3])}?")
    if low_evidence_density:
        question_parts.append("Which bounded governed probe would add the most new auditable evidence with minimal token cost?")
    question = " ".join(question_parts) or "Which bounded governed probe should run next?"
    reason_fragments: list[str] = []
    if atypical_gap_types:
        reason_fragments.append(f"Atypical gaps remain unmapped to the standard action library: {', '.join(atypical_gap_types[:3])}.")
    if low_evidence_density:
        reason_fragments.append("Current evidence density is too low to rank only template-driven follow-up actions confidently.")
    if contradiction_count > 0:
        reason_fragments.append("Contradictory evidence remains active, so the next move should stay tightly governed and auditable.")
    reason = " ".join(reason_fragments)

    probe_request = {
        "probe_id": f"probe-{round_id}-{maybe_text(hypothesis.get('hypothesis_id')) or 'hypothesis'}",
        "mode": "governance-aware-discovery",
        "assigned_role": assigned_role,
        "question": question,
        "reason_codes": selection_reason_codes,
        "governance_envelope": {
            "source_option_count": len(governed_options),
            "source_options": [
                {
                    "family_id": maybe_text(option.get("family_id")),
                    "layer_id": maybe_text(option.get("layer_id")),
                    "approval_state": maybe_text(option.get("approval_state")),
                    "requires_anchor": bool(option.get("requires_anchor")),
                    "source_skills": [maybe_text(skill) for skill in option.get("source_skills", []) if maybe_text(skill)][:4],
                }
                for option in governed_options[:MAX_DISCOVERY_OPTIONS]
                if isinstance(option, dict)
            ],
        },
        "budget": {
            "max_source_options": MAX_DISCOVERY_OPTIONS,
            "estimated_token_cost": int(
                sum(int(item.get("estimated_token_cost") or 0) for item in governed_options[:MAX_DISCOVERY_OPTIONS])
                or UNKNOWN_OPTION_COST
            ),
            "requires_review": True,
        },
        "outputs": ["recommendation", "probe-request"],
    }

    return {
        "candidate_kind": "governed-discovery-probe",
        "assigned_role": assigned_role,
        "priority": "high" if atypical_gap_types else "medium",
        "objective": (
            f"Draft a bounded governed discovery probe for {maybe_text(hypothesis.get('hypothesis_id'))} before selecting the next source family."
        ),
        "reason": reason,
        "target": {
            "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
            "gap_types": known_gap_types[:4],
            "atypical_gap_types": atypical_gap_types[:4],
            "unresolved_required_leg_count": unresolved_required_leg_count,
            "evidence_ref_count": len(evidence_refs),
            "coverage_status": maybe_text(hypothesis.get("overall_status")),
        },
        "evidence_refs": evidence_refs[:6],
        "anchor_refs": _anchor_refs(round_id, evidence_refs)[:6],
        "governed_source_options": governed_options,
        "selection_reason_codes": selection_reason_codes,
        "probe_request": probe_request,
        "score": {
            "total": total,
            "components": {
                "expected_evidence_gain": round(expected_evidence_gain, 3),
                "contradiction_resolution_value": round(contradiction_resolution_value, 3),
                "coverage_gain": round(coverage_gain, 3),
                "novelty_gain": round(novelty_gain, 3),
                "audit_clarity": round(audit_clarity, 3),
                "token_cost_penalty": round(token_cost_penalty, 3),
            },
        },
        "budget": {
            "token_cost_estimate": int(probe_request["budget"]["estimated_token_cost"]),
            "governed_option_count": len(governed_options),
            "max_source_options": MAX_DISCOVERY_OPTIONS,
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


def probe_requests_from_investigation_actions(payload: dict[str, Any], *, limit: int | None = None) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    probe_requests = payload.get("probe_requests") if isinstance(payload.get("probe_requests"), list) else []
    if probe_requests:
        requests = [item for item in probe_requests if isinstance(item, dict)]
    else:
        actions = payload.get("ranked_actions") if isinstance(payload.get("ranked_actions"), list) else []
        for action in actions:
            if not isinstance(action, dict):
                continue
            probe_request = action.get("probe_request")
            if isinstance(probe_request, dict):
                requests.append(probe_request)
    if isinstance(limit, int):
        return requests[:limit]
    return requests


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
    discovery_probe_count = 0
    for hypothesis in hypotheses[:MAX_PRIMARY_HYPOTHESES]:
        if discovery_probe_count >= MAX_DISCOVERY_PROBES:
            break
        if not isinstance(hypothesis, dict):
            continue
        candidate = _discovery_probe_target(
            mission=mission,
            round_id=round_id,
            hypothesis=hypothesis,
        )
        if candidate is None:
            continue
        candidates.append(candidate)
        discovery_probe_count += 1
    ranked_actions = _ranked_actions(candidates, round_id=round_id)
    probe_requests = probe_requests_from_investigation_actions({"ranked_actions": ranked_actions})
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
            "max_discovery_probes": MAX_DISCOVERY_PROBES,
            "candidate_count": len(candidates),
            "returned_count": len(ranked_actions),
            "truncated_by_cap": len(candidates) > len(ranked_actions),
            "estimated_token_cost": estimated_token_cost,
            "discovery_probe_count": len(probe_requests),
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
            "discovery_probe_count": len(probe_requests),
            "role_counts": role_counts,
        },
        "ranked_actions": ranked_actions,
        "probe_requests": probe_requests,
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
    "probe_requests_from_investigation_actions",
    "recommendations_from_investigation_actions",
]
