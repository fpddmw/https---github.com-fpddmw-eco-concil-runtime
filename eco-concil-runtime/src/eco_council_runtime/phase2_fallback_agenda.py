from __future__ import annotations

from typing import Any

from .phase2_fallback_agenda_profile import (
    action_from_claim_assessment,
    action_from_coverage,
    action_from_diffusion_edge,
    action_from_formal_public_link,
    action_from_hypothesis,
    action_from_issue_cluster,
    action_from_open_challenge,
    action_from_open_task,
    action_from_representation_gap,
    action_from_verification_route,
    agenda_action,
    build_fallback_agenda_context,
    default_fallback_agenda_profile,
    fallback_agenda_source,
    prepare_promotion_action,
)
from .phase2_fallback_common import (
    excerpt_text,
    indexed_by_claim_id,
    list_field,
    maybe_text,
    priority_score,
    unique_texts,
    weakest_coverage_for_claim_ids,
)
from .phase2_fallback_policy import (
    diffusion_edge_is_focus,
    empirical_issue_requires_followup,
    role_from_coverage,
    score_action,
)


def board_counts_from_round_state(
    round_state: dict[str, Any],
    *,
    state_source: str,
    include_notes: bool = False,
) -> dict[str, Any]:
    notes = (
        round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    )
    hypotheses = (
        [item for item in round_state.get("hypotheses", []) if isinstance(item, dict)]
        if isinstance(round_state.get("hypotheses"), list)
        else []
    )
    challenges = (
        [
            item
            for item in round_state.get("challenge_tickets", [])
            if isinstance(item, dict)
        ]
        if isinstance(round_state.get("challenge_tickets"), list)
        else []
    )
    tasks = (
        [item for item in round_state.get("tasks", []) if isinstance(item, dict)]
        if isinstance(round_state.get("tasks"), list)
        else []
    )
    active_hypotheses = [
        item
        for item in hypotheses
        if maybe_text(item.get("status")) not in {"closed", "rejected"}
    ]
    open_challenges = [
        item for item in challenges if maybe_text(item.get("status")) != "closed"
    ]
    open_tasks = [
        item
        for item in tasks
        if maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}
    ]
    counts = {
        "hypotheses_active": len(active_hypotheses),
        "challenge_open": len(open_challenges),
        "tasks_open": len(open_tasks),
    }
    if include_notes:
        counts["notes_total"] = len(notes)
    return {
        "state_source": state_source,
        "counts": counts,
        "active_hypotheses": active_hypotheses,
        "open_challenges": open_challenges,
        "open_tasks": open_tasks,
    }


def board_snapshot(
    round_state: dict[str, Any] | None,
    board_summary: dict[str, Any] | None,
    *,
    include_notes: bool = False,
) -> dict[str, Any]:
    if isinstance(round_state, dict):
        return board_counts_from_round_state(
            round_state,
            state_source="deliberation-plane",
            include_notes=include_notes,
        )
    if isinstance(board_summary, dict):
        counts = (
            board_summary.get("counts", {})
            if isinstance(board_summary.get("counts"), dict)
            else {}
        )
        summary_counts = {
            "hypotheses_active": int(
                counts.get("hypotheses_active")
                or len(board_summary.get("active_hypotheses", []))
            ),
            "challenge_open": int(
                counts.get("challenge_open")
                or len(board_summary.get("open_challenges", []))
            ),
            "tasks_open": int(
                counts.get("tasks_open") or len(board_summary.get("open_tasks", []))
            ),
        }
        if include_notes:
            summary_counts["notes_total"] = int(counts.get("notes_total") or 0)
        return {
            "state_source": maybe_text(board_summary.get("state_source"))
            or "board-summary-artifact",
            "counts": summary_counts,
            "active_hypotheses": (
                board_summary.get("active_hypotheses", [])
                if isinstance(board_summary.get("active_hypotheses"), list)
                else []
            ),
            "open_challenges": (
                board_summary.get("open_challenges", [])
                if isinstance(board_summary.get("open_challenges"), list)
                else []
            ),
            "open_tasks": (
                board_summary.get("open_tasks", [])
                if isinstance(board_summary.get("open_tasks"), list)
                else []
            ),
        }
    counts = {
        "hypotheses_active": 0,
        "challenge_open": 0,
        "tasks_open": 0,
    }
    if include_notes:
        counts["notes_total"] = 0
    return {
        "state_source": "missing-board",
        "counts": counts,
        "active_hypotheses": [],
        "open_challenges": [],
        "open_tasks": [],
    }


def _source_specs(agenda_profile: dict[str, Any]) -> list[dict[str, Any]]:
    source_specs = agenda_profile.get("source_specs")
    if not isinstance(source_specs, list):
        return []
    return [item for item in source_specs if isinstance(item, dict)]


def _source_enabled(source_spec: dict[str, Any], context: dict[str, Any]) -> bool:
    enabled = source_spec.get("enabled")
    if callable(enabled):
        return bool(enabled(context))
    return True


def _source_rows(source_spec: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    rows = source_spec.get("rows")
    if not callable(rows):
        return []
    loaded = rows(context)
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def _build_action(
    source_spec: dict[str, Any],
    row: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    builder = source_spec.get("build_action")
    if not callable(builder):
        return None
    candidate = builder(row, context)
    return candidate if isinstance(candidate, dict) else None


def _empty_actions(agenda_profile: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    builder = agenda_profile.get("empty_action_builder")
    if not callable(builder):
        return []
    loaded = builder(context)
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def build_actions(
    board_state: dict[str, Any],
    coverages: list[dict[str, Any]],
    issue_clusters: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    assessments: list[dict[str, Any]],
    links: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    brief_text: str,
    *,
    agenda_profile: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    brief_context = excerpt_text(brief_text)
    resolved_profile = (
        agenda_profile if isinstance(agenda_profile, dict) else default_fallback_agenda_profile()
    )
    context = build_fallback_agenda_context(
        board_state=board_state,
        coverages=coverages,
        issue_clusters=issue_clusters,
        routes=routes,
        assessments=assessments,
        links=links,
        gaps=gaps,
        edges=edges,
        brief_context=brief_context,
    )
    actions: list[dict[str, Any]] = []
    for source_spec in _source_specs(resolved_profile):
        if not _source_enabled(source_spec, context):
            continue
        for row in _source_rows(source_spec, context):
            candidate = _build_action(source_spec, row, context)
            if candidate is not None:
                actions.append(candidate)
    if not actions:
        actions.extend(_empty_actions(resolved_profile, context))
    deduped: dict[str, dict[str, Any]] = {}
    for action in actions:
        key = "|".join(
            unique_texts(
                [action.get("action_kind"), *(action.get("source_ids", []) or [])]
            )
        )
        if key in deduped:
            continue
        deduped[key] = action
    ranked = list(deduped.values())
    for action in ranked:
        action["score"] = score_action(action)
    ranked.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -priority_score(item.get("priority")),
            maybe_text(item.get("action_id")),
        )
    )
    for index, action in enumerate(ranked, start=1):
        action["rank"] = index
    return ranked


def controversy_context_counts(
    *,
    issue_clusters: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    links: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    coverages: list[dict[str, Any]],
) -> dict[str, int]:
    coverages_by_claim_id = indexed_by_claim_id(coverages)
    empirical_issue_count = 0
    non_empirical_issue_count = 0
    mixed_issue_count = 0
    routing_issue_count = 0
    observation_lane_issue_count = 0
    observation_lane_gap_count = 0
    formal_record_issue_count = 0
    public_discourse_issue_count = 0
    stakeholder_deliberation_issue_count = 0
    empirical_issue_gap_count = 0
    formal_public_linkage_gap_count = 0
    for issue in issue_clusters:
        if not isinstance(issue, dict):
            continue
        posture = maybe_text(issue.get("controversy_posture"))
        lane = maybe_text(issue.get("recommended_lane"))
        route_status = maybe_text(issue.get("route_status")) or "mixed-routing-review"
        claim_ids = list_field(issue, "claim_ids")
        weakest_coverage = weakest_coverage_for_claim_ids(claim_ids, coverages_by_claim_id)
        explicit_observation_lane = (
            route_status != "mixed-routing-review"
            and lane == "environmental-observation"
        )
        explicit_formal_record_lane = (
            route_status != "mixed-routing-review"
            and lane == "formal-comment-and-policy-record"
        )
        explicit_public_discourse_lane = (
            route_status != "mixed-routing-review"
            and lane == "public-discourse-analysis"
        )
        explicit_stakeholder_deliberation_lane = (
            route_status != "mixed-routing-review"
            and lane == "stakeholder-deliberation-analysis"
        )
        if posture == "empirical-issue" or lane == "environmental-observation":
            empirical_issue_count += 1
            if explicit_observation_lane and empirical_issue_requires_followup(weakest_coverage):
                empirical_issue_gap_count += 1
        elif posture == "non-empirical-issue":
            non_empirical_issue_count += 1
        elif posture:
            mixed_issue_count += 1
        if explicit_observation_lane:
            observation_lane_issue_count += 1
            if empirical_issue_requires_followup(weakest_coverage):
                observation_lane_gap_count += 1
        elif explicit_formal_record_lane:
            formal_record_issue_count += 1
        elif explicit_public_discourse_lane:
            public_discourse_issue_count += 1
        elif explicit_stakeholder_deliberation_lane:
            stakeholder_deliberation_issue_count += 1
        if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
            routing_issue_count += 1
    if not issue_clusters:
        for route in routes:
            if not isinstance(route, dict):
                continue
            lane = maybe_text(route.get("recommended_lane"))
            route_status = maybe_text(route.get("route_status")) or "mixed-routing-review"
            claim_id = maybe_text(route.get("claim_id"))
            weakest_coverage = weakest_coverage_for_claim_ids([claim_id], coverages_by_claim_id)
            explicit_observation_lane = (
                route_status != "mixed-routing-review"
                and lane == "environmental-observation"
            )
            explicit_formal_record_lane = (
                route_status != "mixed-routing-review"
                and lane == "formal-comment-and-policy-record"
            )
            explicit_public_discourse_lane = (
                route_status != "mixed-routing-review"
                and lane == "public-discourse-analysis"
            )
            explicit_stakeholder_deliberation_lane = (
                route_status != "mixed-routing-review"
                and lane == "stakeholder-deliberation-analysis"
            )
            if lane == "environmental-observation":
                empirical_issue_count += 1
                if explicit_observation_lane and empirical_issue_requires_followup(weakest_coverage):
                    empirical_issue_gap_count += 1
            if explicit_observation_lane:
                observation_lane_issue_count += 1
                if empirical_issue_requires_followup(weakest_coverage):
                    observation_lane_gap_count += 1
            elif explicit_formal_record_lane:
                formal_record_issue_count += 1
            elif explicit_public_discourse_lane:
                public_discourse_issue_count += 1
            elif explicit_stakeholder_deliberation_lane:
                stakeholder_deliberation_issue_count += 1
            elif lane in {
                "formal-comment-and-policy-record",
                "public-discourse-analysis",
                "stakeholder-deliberation-analysis",
            }:
                non_empirical_issue_count += 1
            else:
                mixed_issue_count += 1
            if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
                routing_issue_count += 1
    for link in links:
        if not isinstance(link, dict):
            continue
        if maybe_text(link.get("link_status")) not in {"", "aligned"}:
            formal_public_linkage_gap_count += 1
    diffusion_focus_count = 0
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        if diffusion_edge_is_focus(edge):
            diffusion_focus_count += 1
    return {
        "issue_cluster_count": len([item for item in issue_clusters if isinstance(item, dict)]),
        "empirical_issue_count": empirical_issue_count,
        "non_empirical_issue_count": non_empirical_issue_count,
        "mixed_issue_count": mixed_issue_count,
        "routing_issue_count": routing_issue_count,
        "observation_lane_issue_count": observation_lane_issue_count,
        "observation_lane_gap_count": observation_lane_gap_count,
        "formal_record_issue_count": formal_record_issue_count,
        "public_discourse_issue_count": public_discourse_issue_count,
        "stakeholder_deliberation_issue_count": stakeholder_deliberation_issue_count,
        "empirical_issue_gap_count": empirical_issue_gap_count,
        "representation_gap_count": len([item for item in gaps if isinstance(item, dict)]),
        "formal_public_linkage_gap_count": formal_public_linkage_gap_count,
        "diffusion_focus_count": diffusion_focus_count,
    }
