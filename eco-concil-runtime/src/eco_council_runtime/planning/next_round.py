"""Helpers for deriving next-round recommendations, tasks, and overrides."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Callable

PayloadValidator = Callable[[str, Any], None]
MissionRunIdFn = Callable[[dict[str, Any]], str]
MissionConstraintsFn = Callable[[dict[str, Any]], dict[str, int]]
ExpectedOutputKindsFn = Callable[[str], list[str]]
RoundNumberFn = Callable[[str], int | None]

NEXT_ACTION_LIBRARY: dict[str, dict[str, Any]] = {
    "normalized-public-claims": {
        "assigned_role": "sociologist",
        "objective": "Collect and normalize concrete mission-window public claims from approved news and discussion sources.",
        "reason": "The round did not produce enough normalized public claims to assess public concern, event severity, or attribution narratives.",
        "requirement_type": "public-claim-discovery",
        "requirement_summary": "Collect attributable public claims from independent public-discussion channels in the mission window.",
        "priority": "high",
    },
    "evidence-cards-linking-public-claims-to-physical-observations": {
        "assigned_role": "sociologist",
        "objective": "Recover more attributable public claims that can be linked directly against the available physical observations.",
        "reason": "Public-side evidence needs more concrete and attributable claim phrasing before physical evidence cards can be linked reliably.",
        "requirement_type": "claim-attribution-recovery",
        "requirement_summary": "Recover attributable public claims that can be matched against physical observations.",
        "priority": "high",
    },
    "station-air-quality": {
        "assigned_role": "environmentalist",
        "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
        "reason": "Station-grade corroboration remains incomplete or modeled fields still need cross-checking.",
        "requirement_type": "station-air-quality",
        "requirement_summary": "Add station-grade air-quality corroboration in the same mission window and geometry.",
        "priority": "high",
    },
    "fire-detection": {
        "assigned_role": "environmentalist",
        "objective": "Fetch fire-detection evidence aligned with the mission window and geometry.",
        "reason": "Wildfire-related claims still lack direct fire-detection corroboration.",
        "requirement_type": "fire-detection",
        "requirement_summary": "Add direct fire-detection evidence aligned with the mission window and geometry.",
        "priority": "high",
    },
    "meteorology-background": {
        "assigned_role": "environmentalist",
        "objective": "Add meteorology background such as wind, humidity, and precipitation for the same mission window.",
        "reason": "Physical interpretation still needs weather context.",
        "requirement_type": "meteorology-background",
        "requirement_summary": "Add weather context such as wind, humidity, and precipitation for interpretation.",
        "priority": "medium",
    },
    "precipitation-hydrology": {
        "assigned_role": "environmentalist",
        "objective": "Add precipitation or flood-related evidence for the same mission window and geometry.",
        "reason": "Flood or water-related claims still lack direct hydrometeorological corroboration.",
        "requirement_type": "precipitation-hydrology",
        "requirement_summary": "Add direct hydrometeorological evidence for flood or water-related claims.",
        "priority": "high",
    },
    "temperature-extremes": {
        "assigned_role": "environmentalist",
        "objective": "Add temperature-extreme evidence for the same mission window and geometry.",
        "reason": "Heat-related claims still lack direct thermal corroboration.",
        "requirement_type": "temperature-extremes",
        "requirement_summary": "Add direct temperature evidence for heat-related claims.",
        "priority": "high",
    },
    "precipitation-soil-moisture": {
        "assigned_role": "environmentalist",
        "objective": "Add precipitation and soil-moisture evidence for the same mission window and geometry.",
        "reason": "Drought-related claims still lack direct precipitation or soil-moisture corroboration.",
        "requirement_type": "precipitation-soil-moisture",
        "requirement_summary": "Add precipitation and soil-moisture evidence for drought-related claims.",
        "priority": "high",
    },
    "policy-comment-coverage": {
        "assigned_role": "sociologist",
        "objective": "Collect more policy-comment or docket evidence for the same environmental issue.",
        "reason": "Policy-reaction claims still need stronger docket or public-comment coverage.",
        "requirement_type": "policy-comment-coverage",
        "requirement_summary": "Expand rulemaking or docket evidence for policy-reaction claims.",
        "priority": "medium",
    },
    "public-discussion-coverage": {
        "assigned_role": "sociologist",
        "objective": "Collect more independent public-discussion evidence for the same mission window.",
        "reason": "Current public-claim coverage is too thin or concentrated in too few channels.",
        "requirement_type": "public-discussion-coverage",
        "requirement_summary": "Broaden independent public-discussion coverage beyond the currently dominant channels.",
        "priority": "medium",
    },
}


def _normalize_space(value: str) -> str:
    return " ".join(str(value).split())


def _maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return _normalize_space(str(value))


def _truncate_text(value: str, limit: int) -> str:
    text = _normalize_space(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def recommendation_template(recommendation: dict[str, Any]) -> dict[str, Any] | None:
    role = _maybe_text(recommendation.get("assigned_role"))
    objective = _maybe_text(recommendation.get("objective")).casefold()
    if not role or not objective:
        return None
    for template in NEXT_ACTION_LIBRARY.values():
        if _maybe_text(template.get("assigned_role")) != role:
            continue
        if _maybe_text(template.get("objective")).casefold() == objective:
            return template
    return None


def recommendation_key(recommendation: dict[str, Any]) -> tuple[str, str]:
    return (_maybe_text(recommendation.get("assigned_role")), _maybe_text(recommendation.get("objective")).lower())


def base_recommendations_from_missing_types(missing_types: list[str]) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    for missing_type in missing_types:
        template = NEXT_ACTION_LIBRARY.get(missing_type)
        if template is None:
            continue
        recommendations.append(
            {
                "assigned_role": template["assigned_role"],
                "objective": template["objective"],
                "reason": template["reason"],
            }
        )
    return recommendations


def combine_recommendations(*, reports: list[dict[str, Any]], missing_types: list[str]) -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    for report in reports:
        actions = report.get("recommended_next_actions")
        if not isinstance(actions, list):
            continue
        for action in actions:
            if not isinstance(action, dict):
                continue
            recommendation = {
                "assigned_role": _maybe_text(action.get("assigned_role")),
                "objective": _maybe_text(action.get("objective")),
                "reason": _maybe_text(action.get("reason")),
            }
            if all(recommendation.values()):
                combined.append(recommendation)
    combined.extend(base_recommendations_from_missing_types(missing_types))

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for recommendation in combined:
        key = recommendation_key(recommendation)
        if not key[0] or not key[1]:
            continue
        deduped.setdefault(key, recommendation)
    return list(deduped.values())


def build_task_notes(current_round_id: str, reason: str, evidence_requirement: dict[str, Any]) -> str:
    base = f"Keep the same mission geometry and UTC window. Derived from {current_round_id}."
    requirement_type = _maybe_text(evidence_requirement.get("requirement_type"))
    if requirement_type:
        base = f"{base} Evidence requirement: {requirement_type}."
    if _maybe_text(reason):
        return f"{base} Reason: {_maybe_text(reason)}"
    return base


def _evidence_requirement_for_recommendation(
    *,
    recommendation: dict[str, Any],
    template: dict[str, Any] | None,
    role: str,
    requirement_id: str,
    focus_claim_ids: list[str],
    upstream_round_id: str,
    anchor_refs: list[str] | None = None,
) -> dict[str, Any]:
    requirement_type = _maybe_text(template.get("requirement_type")) if isinstance(template, dict) else ""
    if not requirement_type:
        requirement_type = re.sub(r"[^a-z0-9]+", "-", _maybe_text(recommendation.get("objective")).lower()).strip("-") or "follow-up"
    summary = _maybe_text(template.get("requirement_summary")) if isinstance(template, dict) else ""
    if not summary:
        summary = _maybe_text(recommendation.get("reason")) or _maybe_text(recommendation.get("objective"))
    priority = _maybe_text(template.get("priority")) if isinstance(template, dict) else ""
    if priority not in {"low", "medium", "high"}:
        priority = "medium" if role == "sociologist" else "high"
    resolved_anchor_refs = [_maybe_text(item) for item in (anchor_refs or []) if _maybe_text(item)]
    if not resolved_anchor_refs:
        resolved_anchor_refs = [f"{upstream_round_id}:claim:{claim_id}" for claim_id in focus_claim_ids if _maybe_text(claim_id)]
    return {
        "requirement_id": requirement_id,
        "requirement_type": requirement_type,
        "summary": summary,
        "priority": priority,
        "focus_claim_ids": focus_claim_ids,
        "anchor_refs": resolved_anchor_refs,
    }


def _build_override_request(
    *,
    schema_version: str,
    mission: dict[str, Any],
    round_id: str,
    agent_role: str,
    origin_kind: str,
    request_id: str,
    target_path: str,
    current_value: Any,
    requested_value: Any,
    summary: str,
    reason: str,
    evidence_refs: list[str],
    anchor_refs: list[str],
    mission_run_id: MissionRunIdFn,
    validate_payload: PayloadValidator,
) -> dict[str, Any]:
    payload = {
        "schema_version": schema_version,
        "request_id": request_id,
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": agent_role,
        "request_origin_kind": origin_kind,
        "target_path": target_path,
        "current_value": current_value,
        "requested_value": requested_value,
        "summary": _truncate_text(summary, 240),
        "reason": _truncate_text(reason, 500),
        "evidence_refs": _unique_strings([_maybe_text(item) for item in evidence_refs if _maybe_text(item)]),
        "anchor_refs": _unique_strings([_maybe_text(item) for item in anchor_refs if _maybe_text(item)]),
    }
    validate_payload("override-request", payload)
    return payload


def build_next_round_tasks(
    *,
    schema_version: str,
    mission: dict[str, Any],
    current_round_id: str,
    next_round_id: str,
    recommendations: list[dict[str, Any]],
    focus_claim_ids: list[str],
    anchor_refs: list[str],
    mission_run_id: MissionRunIdFn,
    mission_constraints: MissionConstraintsFn,
    expected_output_kinds_for_role: ExpectedOutputKindsFn,
    validate_payload: PayloadValidator,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    run_id = mission_run_id(mission)
    counters: dict[str, int] = defaultdict(int)
    tasks: list[dict[str, Any]] = []
    seen_signatures: set[tuple[str, str]] = set()
    geometry = mission.get("region", {}).get("geometry") if isinstance(mission.get("region"), dict) else None
    window = mission.get("window")
    max_tasks = mission_constraints(mission).get("max_tasks_per_round", 4)
    normalized_focus_claim_ids = focus_claim_ids[:5]
    normalized_anchor_refs = _unique_strings(anchor_refs)
    candidate_count = 0

    for recommendation in recommendations:
        role = _maybe_text(recommendation.get("assigned_role"))
        if not role:
            continue
        objective = _maybe_text(recommendation.get("objective"))
        reason = _maybe_text(recommendation.get("reason"))
        template = recommendation_template(recommendation)
        requirement_type = _maybe_text(template.get("requirement_type")) if isinstance(template, dict) else ""
        signature = (role, requirement_type or objective.casefold())
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        candidate_count += 1
        if len(tasks) >= max_tasks:
            continue
        counters[role] += 1
        task_id = f"task-{role}-{next_round_id}-{counters[role]:02d}"
        requirement = _evidence_requirement_for_recommendation(
            recommendation=recommendation,
            template=template,
            role=role,
            requirement_id=f"req-{role}-{next_round_id}-{counters[role]:02d}-01",
            focus_claim_ids=normalized_focus_claim_ids,
            upstream_round_id=current_round_id,
            anchor_refs=normalized_anchor_refs,
        )
        task = {
            "schema_version": schema_version,
            "task_id": task_id,
            "run_id": run_id,
            "round_id": next_round_id,
            "assigned_role": role,
            "objective": objective,
            "status": "planned",
            "depends_on": [],
            "expected_output_kinds": expected_output_kinds_for_role(role),
            "inputs": {
                "mission_geometry": geometry,
                "mission_window": window,
                "focus_claim_ids": focus_claim_ids,
                "upstream_round_id": current_round_id,
                "evidence_requirements": [requirement],
            },
            "notes": build_task_notes(current_round_id, reason, requirement),
        }
        validate_payload("round-task", task)
        tasks.append(task)
    return tasks, {
        "max_tasks_per_round": max_tasks,
        "candidate_count": candidate_count,
        "returned_count": len(tasks),
        "truncated_by_cap": candidate_count > len(tasks),
    }


def build_decision_override_requests(
    *,
    schema_version: str,
    mission: dict[str, Any],
    round_id: str,
    next_round_id: str,
    focus_claim_ids: list[str],
    anchor_refs: list[str],
    task_plan_info: dict[str, Any],
    next_round_requested_but_blocked_by_max_rounds: bool,
    mission_run_id: MissionRunIdFn,
    mission_constraints: MissionConstraintsFn,
    current_round_number: RoundNumberFn,
    validate_payload: PayloadValidator,
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    if bool(task_plan_info.get("truncated_by_cap")):
        current_cap = int(task_plan_info.get("max_tasks_per_round") or 0)
        requested_cap = max(current_cap + 1, int(task_plan_info.get("candidate_count") or current_cap))
        requests.append(
            _build_override_request(
                schema_version=schema_version,
                mission=mission,
                round_id=round_id,
                agent_role="moderator",
                origin_kind="council-decision",
                request_id=f"override-moderator-{round_id}-max-tasks",
                target_path="constraints.max_tasks_per_round",
                current_value=current_cap,
                requested_value=requested_cap,
                summary="Request a higher next-round task cap.",
                reason=(
                    f"The current max_tasks_per_round={current_cap} truncates materially distinct follow-up tasks "
                    f"needed for {next_round_id}."
                ),
                evidence_refs=focus_claim_ids,
                anchor_refs=anchor_refs,
                mission_run_id=mission_run_id,
                validate_payload=validate_payload,
            )
        )
    if next_round_requested_but_blocked_by_max_rounds:
        current_round_cap = mission_constraints(mission).get("max_rounds")
        next_round_number = current_round_number(next_round_id)
        requested_round_cap = max(int(current_round_cap or 0) + 1, int(next_round_number or 0))
        requests.append(
            _build_override_request(
                schema_version=schema_version,
                mission=mission,
                round_id=round_id,
                agent_role="moderator",
                origin_kind="council-decision",
                request_id=f"override-moderator-{round_id}-max-rounds",
                target_path="constraints.max_rounds",
                current_value=current_round_cap,
                requested_value=requested_round_cap,
                summary="Request one additional round inside the mission envelope.",
                reason=(
                    f"The current max_rounds={current_round_cap} blocks {next_round_id}, "
                    f"but unresolved evidence still requires another round."
                ),
                evidence_refs=focus_claim_ids,
                anchor_refs=anchor_refs,
                mission_run_id=mission_run_id,
                validate_payload=validate_payload,
            )
        )
    return requests


def collect_unresolved_anchor_refs(state: dict[str, Any]) -> tuple[list[str], list[str]]:
    round_id = state["round_id"]
    focus_claim_ids: list[str] = []
    anchor_refs: list[str] = []
    for item in state.get("remands_open", []):
        if not isinstance(item, dict):
            continue
        entity_kind = _maybe_text(item.get("entity_kind"))
        entity_id = _maybe_text(item.get("entity_id"))
        if not entity_kind or not entity_id:
            continue
        anchor_refs.append(f"{round_id}:{entity_kind}:{entity_id}")
        if entity_kind == "claim":
            focus_claim_ids.append(entity_id)
    for item in state.get("isolated_active", []):
        if not isinstance(item, dict):
            continue
        entity_kind = _maybe_text(item.get("entity_kind"))
        entity_id = _maybe_text(item.get("entity_id"))
        if not entity_kind or not entity_id:
            continue
        anchor_refs.append(f"{round_id}:{entity_kind}:{entity_id}")
        if entity_kind == "claim":
            focus_claim_ids.append(entity_id)
    result = state.get("matching_result", {})
    if isinstance(result, dict):
        for claim_id in result.get("unmatched_claim_ids", []):
            if _maybe_text(claim_id):
                focus_claim_ids.append(_maybe_text(claim_id))
                anchor_refs.append(f"{round_id}:claim:{_maybe_text(claim_id)}")
        for observation_id in result.get("unmatched_observation_ids", []):
            if _maybe_text(observation_id):
                anchor_refs.append(f"{round_id}:observation:{_maybe_text(observation_id)}")
    for card in state.get("cards_active", []):
        if not isinstance(card, dict):
            continue
        verdict = _maybe_text(card.get("verdict"))
        claim_id = _maybe_text(card.get("claim_id"))
        evidence_id = _maybe_text(card.get("evidence_id"))
        if verdict in {"mixed", "insufficient"} and claim_id:
            focus_claim_ids.append(claim_id)
            anchor_refs.append(f"{round_id}:claim:{claim_id}")
        if verdict in {"mixed", "insufficient"} and evidence_id:
            anchor_refs.append(f"{round_id}:card:{evidence_id}")
    return _unique_strings(focus_claim_ids), _unique_strings(anchor_refs)


__all__ = [
    "base_recommendations_from_missing_types",
    "build_decision_override_requests",
    "build_next_round_tasks",
    "collect_unresolved_anchor_refs",
    "combine_recommendations",
    "recommendation_key",
]
