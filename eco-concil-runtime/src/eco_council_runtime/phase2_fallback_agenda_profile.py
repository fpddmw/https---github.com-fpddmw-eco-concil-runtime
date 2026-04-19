from __future__ import annotations

from typing import Any
from typing import Callable

from .phase2_fallback_common import (
    grouped_by_issue_label,
    indexed_by_claim_id,
    issue_label_for_item,
    list_field,
    maybe_number,
    maybe_text,
    stable_hash,
    unique_texts,
    weakest_coverage_for_claim_ids,
)
from .phase2_fallback_policy import (
    coverage_policy,
    fallback_policy_annotation,
    formal_public_link_policy,
    hypothesis_policy,
    issue_cluster_policy,
    open_challenge_policy,
    open_task_policy,
    promotion_action_policy,
    representation_gap_policy,
    role_from_coverage,
    verification_route_policy,
    claim_assessment_policy,
    diffusion_edge_policy,
)

AgendaSourceRowsLoader = Callable[[dict[str, Any]], list[dict[str, Any]]]
AgendaSourcePredicate = Callable[[dict[str, Any]], bool]
AgendaSourceActionBuilder = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any] | None]
EmptyAgendaBuilder = Callable[[dict[str, Any]], list[dict[str, Any]]]


def agenda_action(
    *,
    action_id: str,
    action_kind: str,
    priority: str,
    assigned_role: str,
    objective: str,
    reason: str,
    source_ids: list[str],
    target: dict[str, Any],
    controversy_gap: str,
    recommended_lane: str,
    expected_outcome: str,
    evidence_refs: list[Any],
    probe_candidate: bool,
    contradiction_link_count: int = 0,
    coverage_score: float = 1.0,
    confidence: float | None = None,
    brief_context: str = "",
    agenda_source: str = "",
    issue_label: str = "",
    pressure_score: float = 0.5,
    readiness_blocker: bool = True,
    policy_annotation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    annotation = (
        policy_annotation if isinstance(policy_annotation, dict) else fallback_policy_annotation()
    )
    return {
        "action_id": maybe_text(action_id),
        "action_kind": maybe_text(action_kind),
        "priority": maybe_text(priority) or "medium",
        "assigned_role": maybe_text(assigned_role) or "moderator",
        "objective": maybe_text(objective),
        "reason": maybe_text(reason),
        "source_ids": unique_texts(source_ids),
        "target": target if isinstance(target, dict) else {},
        "controversy_gap": maybe_text(controversy_gap),
        "recommended_lane": maybe_text(recommended_lane),
        "expected_outcome": maybe_text(expected_outcome),
        "evidence_refs": unique_texts(evidence_refs if isinstance(evidence_refs, list) else []),
        "probe_candidate": bool(probe_candidate),
        "contradiction_link_count": int(contradiction_link_count or 0),
        "coverage_score": round(float(coverage_score or 0.0), 3),
        "confidence": confidence,
        "brief_context": maybe_text(brief_context),
        "agenda_source": maybe_text(agenda_source),
        "issue_label": maybe_text(issue_label),
        "pressure_score": round(max(0.0, min(1.0, float(pressure_score or 0.0))), 3),
        "readiness_blocker": bool(readiness_blocker),
        "policy_profile": maybe_text(annotation.get("policy_profile")),
        "policy_source": maybe_text(annotation.get("policy_source")),
        "policy_owner": maybe_text(annotation.get("policy_owner")),
    }


def action_from_open_challenge(challenge: dict[str, Any], brief_context: str) -> dict[str, Any]:
    ticket_id = maybe_text(challenge.get("ticket_id"))
    target_claim_id = maybe_text(challenge.get("target_claim_id"))
    target_hypothesis_id = maybe_text(challenge.get("target_hypothesis_id"))
    policy = open_challenge_policy(challenge)
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", ticket_id, "challenge")[:12],
        source_ids=[ticket_id, target_claim_id, target_hypothesis_id],
        target={
            "ticket_id": ticket_id,
            "claim_id": target_claim_id,
            "hypothesis_id": target_hypothesis_id,
        },
        evidence_refs=challenge.get("linked_artifact_refs", [])
        if isinstance(challenge.get("linked_artifact_refs"), list)
        else [],
        brief_context=brief_context,
        agenda_source="board-challenge",
        issue_label=target_claim_id,
        **policy,
    )


def action_from_open_task(task: dict[str, Any], brief_context: str) -> dict[str, Any]:
    task_id = maybe_text(task.get("task_id"))
    policy = open_task_policy(task)
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", task_id, "task")[:12],
        source_ids=[task_id, task.get("source_ticket_id"), task.get("source_hypothesis_id")],
        target={
            "task_id": task_id,
            "ticket_id": maybe_text(task.get("source_ticket_id")),
            "hypothesis_id": maybe_text(task.get("source_hypothesis_id")),
        },
        evidence_refs=task.get("linked_artifact_refs", [])
        if isinstance(task.get("linked_artifact_refs"), list)
        else [],
        brief_context=brief_context,
        agenda_source="board-task",
        **policy,
    )


def action_from_hypothesis(
    hypothesis: dict[str, Any],
    brief_context: str,
) -> dict[str, Any] | None:
    confidence = maybe_number(hypothesis.get("confidence"))
    policy = hypothesis_policy(hypothesis, confidence=confidence)
    if policy is None:
        return None
    hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
    linked_claim_ids = unique_texts(
        hypothesis.get("linked_claim_ids", [])
        if isinstance(hypothesis.get("linked_claim_ids"), list)
        else []
    )
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", hypothesis_id, "hypothesis")[:12],
        source_ids=[hypothesis_id, *linked_claim_ids],
        target={
            "hypothesis_id": hypothesis_id,
            "claim_id": linked_claim_ids[0] if linked_claim_ids else "",
        },
        evidence_refs=[],
        brief_context=brief_context,
        agenda_source="board-hypothesis",
        issue_label=linked_claim_ids[0] if linked_claim_ids else hypothesis_id,
        **policy,
    )


def action_from_coverage(
    coverage: dict[str, Any],
    brief_context: str,
) -> dict[str, Any] | None:
    policy = coverage_policy(coverage)
    if policy is None:
        return None
    coverage_id = maybe_text(coverage.get("coverage_id"))
    claim_id = maybe_text(coverage.get("claim_id"))
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", coverage_id, policy["action_kind"])[:12],
        source_ids=[coverage_id, claim_id],
        target={"coverage_id": coverage_id, "claim_id": claim_id},
        evidence_refs=coverage.get("evidence_refs", [])
        if isinstance(coverage.get("evidence_refs"), list)
        else [],
        brief_context=brief_context,
        agenda_source="evidence-coverage",
        issue_label=claim_id,
        **policy,
    )


def prepare_promotion_action(
    coverage: dict[str, Any],
    brief_context: str,
) -> dict[str, Any]:
    coverage_id = maybe_text(coverage.get("coverage_id"))
    claim_id = maybe_text(coverage.get("claim_id"))
    policy = promotion_action_policy(coverage)
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", coverage_id, "promotion")[:12],
        source_ids=[coverage_id, claim_id],
        target={"coverage_id": coverage_id, "claim_id": claim_id},
        evidence_refs=coverage.get("evidence_refs", [])
        if isinstance(coverage.get("evidence_refs"), list)
        else [],
        brief_context=brief_context,
        agenda_source="promotion-basis",
        issue_label=claim_id,
        **policy,
    )


def action_from_issue_cluster(
    issue: dict[str, Any],
    *,
    coverages_by_claim_id: dict[str, dict[str, Any]],
    links_by_issue_label: dict[str, list[dict[str, Any]]],
    gaps_by_issue_label: dict[str, list[dict[str, Any]]],
    brief_context: str,
) -> dict[str, Any] | None:
    issue_label = issue_label_for_item(issue)
    claim_ids = list_field(issue, "claim_ids")
    weakest_coverage = weakest_coverage_for_claim_ids(claim_ids, coverages_by_claim_id)
    coverage_id = maybe_text(weakest_coverage.get("coverage_id"))
    link_rows = links_by_issue_label.get(issue_label, [])
    link_status = maybe_text(link_rows[0].get("link_status")) if link_rows else "unlinked"
    gap_rows = gaps_by_issue_label.get(issue_label, [])
    policy = issue_cluster_policy(
        issue,
        issue_label=issue_label,
        weakest_coverage=weakest_coverage,
        link_status=link_status,
        has_representation_gap=bool(gap_rows),
    )
    if policy is None:
        return None
    evidence_refs = unique_texts(
        list(issue.get("evidence_refs", []) if isinstance(issue.get("evidence_refs"), list) else [])
        + list(weakest_coverage.get("evidence_refs", []) if isinstance(weakest_coverage.get("evidence_refs"), list) else [])
    )
    source_ids = unique_texts(
        [issue.get("map_issue_id"), issue.get("cluster_id"), coverage_id, *claim_ids]
    )
    return agenda_action(
        action_id="action-" + stable_hash("d1-agenda", issue.get("map_issue_id"), policy["action_kind"])[:12],
        source_ids=source_ids,
        target={
            "map_issue_id": maybe_text(issue.get("map_issue_id")),
            "cluster_id": maybe_text(issue.get("cluster_id")),
            "claim_id": claim_ids[0] if claim_ids else "",
            "coverage_id": coverage_id,
        },
        evidence_refs=evidence_refs,
        brief_context=brief_context,
        agenda_source="controversy-map",
        issue_label=issue_label,
        **policy,
    )


def action_from_verification_route(
    route: dict[str, Any],
    *,
    coverages_by_claim_id: dict[str, dict[str, Any]],
    links_by_issue_label: dict[str, list[dict[str, Any]]],
    brief_context: str,
) -> dict[str, Any] | None:
    claim_id = maybe_text(route.get("claim_id"))
    issue_label = issue_label_for_item(route)
    weakest_coverage = weakest_coverage_for_claim_ids([claim_id], coverages_by_claim_id)
    link_rows = links_by_issue_label.get(issue_label, [])
    link_status = maybe_text(link_rows[0].get("link_status")) if link_rows else "unlinked"
    policy = verification_route_policy(
        route,
        issue_label=issue_label,
        weakest_coverage=weakest_coverage,
        link_status=link_status,
    )
    if policy is None:
        return None
    source_ids = unique_texts([route.get("route_id"), claim_id, route.get("assessment_id")])
    coverage_id = maybe_text(weakest_coverage.get("coverage_id"))
    if coverage_id and policy["action_kind"] == "advance-empirical-verification":
        source_ids = unique_texts([*source_ids, coverage_id])
    evidence_refs = list(route.get("evidence_refs", []) if isinstance(route.get("evidence_refs"), list) else [])
    if coverage_id and policy["action_kind"] == "advance-empirical-verification":
        evidence_refs += list(
            weakest_coverage.get("evidence_refs", [])
            if isinstance(weakest_coverage.get("evidence_refs"), list)
            else []
        )
    return agenda_action(
        action_id="action-" + stable_hash("d1-route", route.get("route_id"), policy["action_kind"])[:12],
        source_ids=source_ids,
        target={
            "route_id": maybe_text(route.get("route_id")),
            "claim_id": claim_id,
            "coverage_id": coverage_id,
        },
        evidence_refs=evidence_refs,
        brief_context=brief_context,
        agenda_source="verification-route",
        issue_label=issue_label,
        **policy,
    )


def action_from_claim_assessment(
    assessment: dict[str, Any],
    *,
    coverages_by_claim_id: dict[str, dict[str, Any]],
    brief_context: str,
) -> dict[str, Any] | None:
    claim_id = maybe_text(assessment.get("claim_id"))
    issue_label = issue_label_for_item(assessment)
    risk_flags = list_field(assessment, "risk_flags")
    weakest_coverage = weakest_coverage_for_claim_ids([claim_id], coverages_by_claim_id)
    policy = claim_assessment_policy(
        assessment,
        issue_label=issue_label,
        weakest_coverage=weakest_coverage,
        risk_flags=risk_flags,
    )
    if policy is None:
        return None
    source_ids = unique_texts([assessment.get("assessment_id"), claim_id, assessment.get("claim_scope_id")])
    coverage_id = maybe_text(weakest_coverage.get("coverage_id"))
    if coverage_id and policy["action_kind"] == "advance-empirical-verification":
        source_ids = unique_texts([*source_ids, coverage_id])
    evidence_refs = list(
        assessment.get("evidence_refs", [])
        if isinstance(assessment.get("evidence_refs"), list)
        else []
    )
    if coverage_id and policy["action_kind"] == "advance-empirical-verification":
        evidence_refs += list(
            weakest_coverage.get("evidence_refs", [])
            if isinstance(weakest_coverage.get("evidence_refs"), list)
            else []
        )
    return agenda_action(
        action_id="action-" + stable_hash("d1-assessment", assessment.get("assessment_id"), policy["action_kind"])[:12],
        source_ids=source_ids,
        target={
            "assessment_id": maybe_text(assessment.get("assessment_id")),
            "claim_id": claim_id,
            "coverage_id": coverage_id,
        },
        evidence_refs=evidence_refs,
        brief_context=brief_context,
        agenda_source="claim-verifiability",
        issue_label=issue_label,
        **policy,
    )


def action_from_formal_public_link(
    link: dict[str, Any],
    *,
    brief_context: str,
) -> dict[str, Any] | None:
    issue_label = issue_label_for_item(link)
    policy = formal_public_link_policy(link, issue_label=issue_label)
    if policy is None:
        return None
    linkage_id = maybe_text(link.get("linkage_id"))
    return agenda_action(
        action_id="action-" + stable_hash("d1-link", linkage_id, maybe_text(link.get("link_status")))[:12],
        source_ids=[
            linkage_id,
            *list_field(link, "claim_ids"),
            *list_field(link, "cluster_ids"),
        ],
        target={"linkage_id": linkage_id, "issue_label": issue_label},
        evidence_refs=link.get("evidence_refs", [])
        if isinstance(link.get("evidence_refs"), list)
        else [],
        brief_context=brief_context,
        agenda_source="formal-public-link",
        issue_label=issue_label,
        **policy,
    )


def action_from_representation_gap(
    gap: dict[str, Any],
    *,
    brief_context: str,
) -> dict[str, Any]:
    issue_label = issue_label_for_item(gap)
    gap_id = maybe_text(gap.get("gap_id"))
    severity_score = float(gap.get("severity_score") or 0.0)
    policy = representation_gap_policy(
        gap,
        issue_label=issue_label,
        severity_score=severity_score,
    )
    return agenda_action(
        action_id="action-" + stable_hash("d1-gap", gap_id, gap.get("gap_type"))[:12],
        source_ids=[
            gap_id,
            gap.get("linkage_id"),
            *list_field(gap, "claim_ids"),
            *list_field(gap, "cluster_ids"),
        ],
        target={"gap_id": gap_id, "linkage_id": maybe_text(gap.get("linkage_id"))},
        evidence_refs=gap.get("evidence_refs", [])
        if isinstance(gap.get("evidence_refs"), list)
        else [],
        brief_context=brief_context,
        agenda_source="representation-gap",
        issue_label=issue_label,
        **policy,
    )


def action_from_diffusion_edge(
    edge: dict[str, Any],
    *,
    brief_context: str,
) -> dict[str, Any] | None:
    confidence = maybe_number(edge.get("confidence")) or 0.0
    issue_label = issue_label_for_item(edge)
    policy = diffusion_edge_policy(
        edge,
        issue_label=issue_label,
        confidence=confidence,
    )
    if policy is None:
        return None
    edge_id = maybe_text(edge.get("edge_id"))
    return agenda_action(
        action_id="action-" + stable_hash("d1-diffusion", edge_id, edge.get("edge_type"))[:12],
        source_ids=[
            edge_id,
            *list_field(edge, "claim_ids"),
            *list_field(edge, "cluster_ids"),
            *list_field(edge, "source_signal_ids"),
            *list_field(edge, "target_signal_ids"),
        ],
        target={
            "edge_id": edge_id,
            "source_platform": maybe_text(edge.get("source_platform")),
            "target_platform": maybe_text(edge.get("target_platform")),
        },
        evidence_refs=edge.get("evidence_refs", [])
        if isinstance(edge.get("evidence_refs"), list)
        else [],
        brief_context=brief_context,
        agenda_source="diffusion-edge",
        issue_label=issue_label,
        **policy,
    )


def _brief_context(context: dict[str, Any]) -> str:
    return maybe_text(context.get("brief_context"))


def _dict_rows(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]


def _open_challenge_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    board_state = context.get("board_state", {})
    return _dict_rows(board_state.get("open_challenges") if isinstance(board_state, dict) else [])


def _open_task_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    board_state = context.get("board_state", {})
    return _dict_rows(board_state.get("open_tasks") if isinstance(board_state, dict) else [])


def _active_hypothesis_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    board_state = context.get("board_state", {})
    return _dict_rows(board_state.get("active_hypotheses") if isinstance(board_state, dict) else [])


def _issue_cluster_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return _dict_rows(context.get("issue_clusters"))


def _route_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return _dict_rows(context.get("routes"))


def _assessment_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return _dict_rows(context.get("assessments"))


def _representation_gap_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return _dict_rows(context.get("gaps"))


def _formal_public_link_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return _dict_rows(context.get("links"))


def _diffusion_edge_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return _dict_rows(context.get("edges"))


def _coverage_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return _dict_rows(context.get("coverages"))


def _strong_coverage_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        coverage
        for coverage in _coverage_rows(context)
        if maybe_text(coverage.get("readiness")) == "strong"
    ]


def _always_enabled(_: dict[str, Any]) -> bool:
    return True


def _routes_enabled(context: dict[str, Any]) -> bool:
    return not bool(_issue_cluster_rows(context))


def _assessments_enabled(context: dict[str, Any]) -> bool:
    return not bool(_issue_cluster_rows(context) or _route_rows(context))


def _formal_public_links_enabled(context: dict[str, Any]) -> bool:
    return not bool(_representation_gap_rows(context))


def _coverages_enabled(context: dict[str, Any]) -> bool:
    return not bool(
        _issue_cluster_rows(context)
        or _route_rows(context)
        or _assessment_rows(context)
        or _formal_public_link_rows(context)
        or _representation_gap_rows(context)
        or _diffusion_edge_rows(context)
    )


def _build_open_challenge_action(
    challenge: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return action_from_open_challenge(challenge, _brief_context(context))


def _build_open_task_action(
    task: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return action_from_open_task(task, _brief_context(context))


def _build_hypothesis_action(
    hypothesis: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    return action_from_hypothesis(hypothesis, _brief_context(context))


def _build_issue_cluster_action(
    issue: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    return action_from_issue_cluster(
        issue,
        coverages_by_claim_id=context.get("coverages_by_claim_id", {}),
        links_by_issue_label=context.get("links_by_issue_label", {}),
        gaps_by_issue_label=context.get("gaps_by_issue_label", {}),
        brief_context=_brief_context(context),
    )


def _build_verification_route_action(
    route: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    return action_from_verification_route(
        route,
        coverages_by_claim_id=context.get("coverages_by_claim_id", {}),
        links_by_issue_label=context.get("links_by_issue_label", {}),
        brief_context=_brief_context(context),
    )


def _build_claim_assessment_action(
    assessment: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    return action_from_claim_assessment(
        assessment,
        coverages_by_claim_id=context.get("coverages_by_claim_id", {}),
        brief_context=_brief_context(context),
    )


def _build_representation_gap_action(
    gap: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return action_from_representation_gap(gap, brief_context=_brief_context(context))


def _build_formal_public_link_action(
    link: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    return action_from_formal_public_link(link, brief_context=_brief_context(context))


def _build_diffusion_edge_action(
    edge: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    return action_from_diffusion_edge(edge, brief_context=_brief_context(context))


def _build_coverage_action(
    coverage: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    return action_from_coverage(coverage, _brief_context(context))


def default_empty_agenda_actions(context: dict[str, Any]) -> list[dict[str, Any]]:
    strong_coverages = _strong_coverage_rows(context)
    if not strong_coverages:
        return []
    return [prepare_promotion_action(strong_coverages[0], _brief_context(context))]


def fallback_agenda_source(
    source_name: str,
    *,
    rows: AgendaSourceRowsLoader,
    build_action: AgendaSourceActionBuilder,
    enabled: AgendaSourcePredicate | None = None,
) -> dict[str, Any]:
    return {
        "source_name": maybe_text(source_name),
        "rows": rows,
        "enabled": enabled if callable(enabled) else _always_enabled,
        "build_action": build_action,
    }


def default_fallback_agenda_profile(
    *,
    empty_action_builder: EmptyAgendaBuilder | None = None,
) -> dict[str, Any]:
    return {
        "profile_name": "phase2-fallback-agenda-profile-v1",
        "source_specs": [
            fallback_agenda_source(
                "open-challenges",
                rows=_open_challenge_rows,
                build_action=_build_open_challenge_action,
            ),
            fallback_agenda_source(
                "open-tasks",
                rows=_open_task_rows,
                build_action=_build_open_task_action,
            ),
            fallback_agenda_source(
                "active-hypotheses",
                rows=_active_hypothesis_rows,
                build_action=_build_hypothesis_action,
            ),
            fallback_agenda_source(
                "issue-clusters",
                rows=_issue_cluster_rows,
                build_action=_build_issue_cluster_action,
            ),
            fallback_agenda_source(
                "verification-routes",
                rows=_route_rows,
                build_action=_build_verification_route_action,
                enabled=_routes_enabled,
            ),
            fallback_agenda_source(
                "claim-assessments",
                rows=_assessment_rows,
                build_action=_build_claim_assessment_action,
                enabled=_assessments_enabled,
            ),
            fallback_agenda_source(
                "representation-gaps",
                rows=_representation_gap_rows,
                build_action=_build_representation_gap_action,
            ),
            fallback_agenda_source(
                "formal-public-links",
                rows=_formal_public_link_rows,
                build_action=_build_formal_public_link_action,
                enabled=_formal_public_links_enabled,
            ),
            fallback_agenda_source(
                "diffusion-edges",
                rows=_diffusion_edge_rows,
                build_action=_build_diffusion_edge_action,
            ),
            fallback_agenda_source(
                "coverages",
                rows=_coverage_rows,
                build_action=_build_coverage_action,
                enabled=_coverages_enabled,
            ),
        ],
        "empty_action_builder": (
            empty_action_builder if callable(empty_action_builder) else default_empty_agenda_actions
        ),
    }


def build_fallback_agenda_context(
    *,
    board_state: dict[str, Any],
    coverages: list[dict[str, Any]],
    issue_clusters: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    assessments: list[dict[str, Any]],
    links: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    brief_context: str,
) -> dict[str, Any]:
    return {
        "board_state": board_state if isinstance(board_state, dict) else {},
        "coverages": _dict_rows(coverages),
        "issue_clusters": _dict_rows(issue_clusters),
        "routes": _dict_rows(routes),
        "assessments": _dict_rows(assessments),
        "links": _dict_rows(links),
        "gaps": _dict_rows(gaps),
        "edges": _dict_rows(edges),
        "brief_context": maybe_text(brief_context),
        "coverages_by_claim_id": indexed_by_claim_id(coverages),
        "links_by_issue_label": grouped_by_issue_label(links),
        "gaps_by_issue_label": grouped_by_issue_label(gaps),
    }
