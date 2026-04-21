from __future__ import annotations

from typing import Any

from .phase2_fallback_common import (
    maybe_number,
    maybe_text,
    priority_from_score,
    priority_score,
    role_from_lane,
)

DEFAULT_FALLBACK_POLICY_PROFILE = "phase2-fallback-policy-v1"
DEFAULT_FALLBACK_POLICY_SOURCE = "runtime-fallback-policy"
DEFAULT_FALLBACK_POLICY_OWNER = "phase2-fallback"
DIFFUSION_FOCUS_EDGE_TYPES = {
    "public-to-formal-spillover",
    "formal-to-public-spillover",
    "cross-public-diffusion",
}
ACTION_SCORE_WEIGHTS = {
    "priority_multiplier": 1.0,
    "pressure_multiplier": 2.2,
    "contradiction_multiplier": 0.4,
    "contradiction_cap": 1.5,
    "coverage_multiplier": 1.0,
    "uncertainty_baseline": 0.9,
    "readiness_blocker_bonus": 0.6,
    "probe_candidate_bonus": 0.45,
}


def fallback_policy_annotation(
    *,
    policy_profile: str = DEFAULT_FALLBACK_POLICY_PROFILE,
    policy_source: str = DEFAULT_FALLBACK_POLICY_SOURCE,
    policy_owner: str = DEFAULT_FALLBACK_POLICY_OWNER,
) -> dict[str, str]:
    return {
        "policy_profile": maybe_text(policy_profile) or DEFAULT_FALLBACK_POLICY_PROFILE,
        "policy_source": maybe_text(policy_source) or DEFAULT_FALLBACK_POLICY_SOURCE,
        "policy_owner": maybe_text(policy_owner) or DEFAULT_FALLBACK_POLICY_OWNER,
    }


def score_action(
    payload: dict[str, Any],
    *,
    score_weights: dict[str, float] | None = None,
) -> float:
    weights = dict(ACTION_SCORE_WEIGHTS)
    if isinstance(score_weights, dict):
        for key, value in score_weights.items():
            try:
                weights[key] = float(value)
            except (TypeError, ValueError):
                continue
    priority_component = priority_score(payload.get("priority")) * weights["priority_multiplier"]
    pressure_component = max(
        0.0,
        min(1.0, float(payload.get("pressure_score") or 0.0)),
    ) * weights["pressure_multiplier"]
    contradiction_component = min(
        weights["contradiction_cap"],
        float(payload.get("contradiction_link_count") or 0)
        * weights["contradiction_multiplier"],
    )
    coverage_component = (
        max(0.0, 1.0 - float(payload.get("coverage_score") or 0.0))
        * weights["coverage_multiplier"]
    )
    confidence_value = maybe_number(payload.get("confidence"))
    uncertainty_component = (
        0.0
        if confidence_value is None
        else max(0.0, weights["uncertainty_baseline"] - float(confidence_value))
    )
    blocker_component = (
        weights["readiness_blocker_bonus"]
        if bool(payload.get("readiness_blocker"))
        else 0.0
    )
    probe_component = (
        weights["probe_candidate_bonus"] if bool(payload.get("probe_candidate")) else 0.0
    )
    return round(
        priority_component
        + pressure_component
        + contradiction_component
        + coverage_component
        + uncertainty_component
        + blocker_component
        + probe_component,
        3,
    )


def role_from_coverage(coverage: dict[str, Any]) -> str:
    if int(coverage.get("contradiction_link_count") or 0) > 0:
        return "challenger"
    if int(coverage.get("linked_observation_count") or 0) > 0:
        return "environmentalist"
    return "sociologist"


def empirical_issue_requires_followup(weakest_coverage: dict[str, Any]) -> bool:
    if not weakest_coverage:
        return True
    return (
        maybe_text(weakest_coverage.get("readiness")) != "strong"
        or int(weakest_coverage.get("contradiction_link_count") or 0) > 0
    )


def diffusion_edge_is_focus(edge: dict[str, Any]) -> bool:
    edge_type = maybe_text(edge.get("edge_type"))
    confidence = maybe_number(edge.get("confidence")) or 0.0
    return edge_type in DIFFUSION_FOCUS_EDGE_TYPES and confidence >= 0.72


def open_challenge_policy(challenge: dict[str, Any]) -> dict[str, Any]:
    return {
        "action_kind": "resolve-challenge",
        "priority": maybe_text(challenge.get("priority")) or "high",
        "assigned_role": maybe_text(challenge.get("owner_role")) or "challenger",
        "objective": maybe_text(challenge.get("title"))
        or "Resolve an open controversy point before closing the round.",
        "reason": maybe_text(challenge.get("title"))
        or "An open contested point still needs follow-up before the controversy map is stable.",
        "controversy_gap": "unresolved-contestation",
        "recommended_lane": "probe-before-closure",
        "expected_outcome": "Decide whether the contested point needs verification, rebuttal, or reframing.",
        "probe_candidate": True,
        "contradiction_link_count": 1,
        "coverage_score": 0.45,
        "confidence": None,
        "pressure_score": 0.95,
        "readiness_blocker": True,
    }


def open_task_policy(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "action_kind": "finish-board-task",
        "priority": maybe_text(task.get("priority")) or "medium",
        "assigned_role": maybe_text(task.get("owner_role")) or "moderator",
        "objective": maybe_text(task.get("title"))
        or "Finish a board task that blocks controversy-map completion.",
        "reason": maybe_text(task.get("title"))
        or maybe_text(task.get("task_text"))
        or "A board coordination task is still in flight.",
        "controversy_gap": "board-coordination-gap",
        "recommended_lane": "board-followthrough",
        "expected_outcome": "Finish the coordination work needed to advance the round.",
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": 0.55,
        "confidence": None,
        "pressure_score": 0.7,
        "readiness_blocker": True,
    }


def hypothesis_policy(
    hypothesis: dict[str, Any],
    *,
    confidence: float | None,
) -> dict[str, Any] | None:
    if confidence is not None and confidence >= 0.75:
        return None
    return {
        "action_kind": "stabilize-hypothesis",
        "priority": "high" if (confidence or 0.0) < 0.6 else "medium",
        "assigned_role": maybe_text(hypothesis.get("owner_role")) or "moderator",
        "objective": maybe_text(hypothesis.get("title"))
        or "Stabilize an active issue interpretation.",
        "reason": "The board still carries an active hypothesis with limited confidence.",
        "controversy_gap": "issue-structure-gap",
        "recommended_lane": "clarify-board-position",
        "expected_outcome": "Clarify whether the active interpretation should stay open, split, or be retired.",
        "probe_candidate": (confidence or 0.0) < 0.6,
        "contradiction_link_count": 0,
        "coverage_score": 0.5,
        "confidence": confidence,
        "pressure_score": max(0.55, 1.0 - float(confidence or 0.0)),
        "readiness_blocker": True,
    }


def coverage_policy(coverage: dict[str, Any]) -> dict[str, Any] | None:
    readiness = maybe_text(coverage.get("readiness"))
    contradiction_count = int(coverage.get("contradiction_link_count") or 0)
    if readiness == "strong" and contradiction_count == 0:
        return None
    claim_id = maybe_text(coverage.get("claim_id"))
    claim_scope_ready = bool(coverage.get("claim_scope_ready"))
    if contradiction_count > 0:
        action_kind = "resolve-contradiction"
        priority = "high"
        assigned_role = role_from_coverage(coverage)
        objective = "Check whether empirical signals materially contradict the current public narrative."
        reason = f"Claim {claim_id} has {contradiction_count} contradiction links."
        controversy_gap = "formal-public-misalignment"
        recommended_lane = "mixed-verification-review"
        expected_outcome = "Decide whether the contradiction is real, partial, or due to weak framing."
    elif not claim_scope_ready:
        action_kind = "classify-verifiability"
        priority = "high" if readiness == "weak" else "medium"
        assigned_role = "moderator"
        objective = "Clarify whether this controversy point should enter the verification lane."
        reason = (
            f"Claim {claim_id} lacks matching-ready scope and should be routed before more evidence expansion."
        )
        controversy_gap = "verification-routing-gap"
        recommended_lane = "route-before-matching"
        expected_outcome = "Classify the issue as empirical, procedural, representational, or mixed."
    else:
        action_kind = "expand-coverage"
        priority = "high" if readiness == "weak" else "medium"
        assigned_role = role_from_coverage(coverage)
        objective = "Expand verification coverage for a claim that still lacks enough support."
        reason = f"Claim {claim_id} is only {readiness or 'unknown'} on evidence coverage."
        controversy_gap = "verification-gap"
        recommended_lane = "environmental-observation"
        expected_outcome = "Add enough support to decide whether the claim should remain active."
    pressure_score = 0.64
    if contradiction_count > 0:
        pressure_score = 0.9
    elif readiness == "weak":
        pressure_score = 0.82
    elif not claim_scope_ready:
        pressure_score = 0.76
    return {
        "action_kind": action_kind,
        "priority": priority,
        "assigned_role": assigned_role,
        "objective": objective,
        "reason": reason,
        "controversy_gap": controversy_gap,
        "recommended_lane": recommended_lane,
        "expected_outcome": expected_outcome,
        "probe_candidate": (
            contradiction_count > 0
            or readiness == "weak"
            or action_kind == "classify-verifiability"
        ),
        "contradiction_link_count": contradiction_count,
        "coverage_score": float(coverage.get("coverage_score") or 0.0),
        "confidence": None,
        "pressure_score": pressure_score,
        "readiness_blocker": True,
    }


def promotion_action_policy(coverage: dict[str, Any]) -> dict[str, Any]:
    claim_id = maybe_text(coverage.get("claim_id"))
    return {
        "action_kind": "open-council-readiness-review",
        "priority": "medium",
        "assigned_role": "moderator",
        "objective": "Open explicit council readiness review once the controversy map is stable.",
        "reason": f"Claim {claim_id} already has strong evidence coverage and can move into explicit council readiness review.",
        "controversy_gap": "council-readiness-review",
        "recommended_lane": "council-deliberation",
        "expected_outcome": "Solicit explicit readiness and publication judgement before downstream reporting proceeds.",
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": float(coverage.get("coverage_score") or 0.0),
        "confidence": None,
        "pressure_score": 0.25,
        "readiness_blocker": False,
    }


def issue_cluster_policy(
    issue: dict[str, Any],
    *,
    issue_label: str,
    weakest_coverage: dict[str, Any],
    link_status: str,
    has_representation_gap: bool,
) -> dict[str, Any] | None:
    route_status = maybe_text(issue.get("route_status")) or "mixed-routing-review"
    lane = maybe_text(issue.get("recommended_lane")) or "mixed-review"
    coverage_readiness = maybe_text(weakest_coverage.get("readiness"))
    contradiction_count = int(weakest_coverage.get("contradiction_link_count") or 0)
    if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
        return {
            "action_kind": "clarify-verification-route",
            "priority": "high",
            "assigned_role": "moderator",
            "objective": f"Clarify which investigation lane should govern {issue_label}.",
            "reason": maybe_text(issue.get("controversy_summary"))
            or f"Issue {issue_label} is still routed ambiguously and should not be advanced through a single lane yet.",
            "controversy_gap": "verification-routing-gap",
            "recommended_lane": lane,
            "expected_outcome": "Decide whether the issue belongs in empirical verification, formal record review, discourse analysis, or mixed review.",
            "probe_candidate": True,
            "contradiction_link_count": contradiction_count,
            "coverage_score": float(weakest_coverage.get("coverage_score") or 1.0)
            if weakest_coverage
            else 1.0,
            "confidence": None,
            "pressure_score": 0.9,
            "readiness_blocker": True,
        }
    if maybe_text(issue.get("controversy_posture")) == "empirical-issue" or lane == "environmental-observation":
        if empirical_issue_requires_followup(weakest_coverage):
            coverage_score = (
                float(weakest_coverage.get("coverage_score") or 0.0)
                if weakest_coverage
                else 0.0
            )
            if contradiction_count > 0:
                controversy_gap = "formal-public-misalignment"
                reason = f"Issue {issue_label} still carries {contradiction_count} contradiction links against its empirical support path."
                pressure_score = 0.92
            else:
                controversy_gap = "verification-gap"
                reason = (
                    f"Issue {issue_label} is only {coverage_readiness or 'unscored'} on empirical evidence coverage."
                    if weakest_coverage
                    else f"Issue {issue_label} has not yet been grounded in a usable evidence-coverage object."
                )
                pressure_score = (
                    0.84
                    if not weakest_coverage or coverage_readiness == "weak"
                    else 0.66
                )
            return {
                "action_kind": "advance-empirical-verification",
                "priority": "high" if pressure_score >= 0.8 else "medium",
                "assigned_role": role_from_coverage(weakest_coverage)
                if weakest_coverage
                else "environmentalist",
                "objective": f"Advance empirical verification for {issue_label}.",
                "reason": reason,
                "controversy_gap": controversy_gap,
                "recommended_lane": "environmental-observation",
                "expected_outcome": "Decide whether available environmental observations materially support, weaken, or reframe the issue.",
                "probe_candidate": contradiction_count > 0 or coverage_readiness in {"", "weak"},
                "contradiction_link_count": contradiction_count,
                "coverage_score": coverage_score,
                "confidence": None,
                "pressure_score": pressure_score,
                "readiness_blocker": True,
            }
        return None
    if has_representation_gap:
        return None
    if lane == "formal-comment-and-policy-record" and link_status == "public-only":
        return {
            "action_kind": "review-formal-record",
            "priority": "high",
            "assigned_role": "moderator",
            "objective": f"Check whether the formal record sufficiently represents {issue_label}.",
            "reason": f"Issue {issue_label} is routed into formal record review but the current linkage posture is {link_status}.",
            "controversy_gap": "formal-record-gap",
            "recommended_lane": lane,
            "expected_outcome": "Establish whether the formal policy record captures the issue strongly enough to close the round.",
            "probe_candidate": True,
            "contradiction_link_count": 0,
            "coverage_score": 1.0,
            "confidence": None,
            "pressure_score": 0.78,
            "readiness_blocker": True,
        }
    if lane == "public-discourse-analysis" and link_status == "formal-only":
        return {
            "action_kind": "analyze-public-discourse",
            "priority": "high",
            "assigned_role": "sociologist",
            "objective": f"Check whether public discourse around {issue_label} is adequately represented and interpretable.",
            "reason": f"Issue {issue_label} stays in discourse analysis but the current linkage posture is {link_status}.",
            "controversy_gap": "public-discourse-gap",
            "recommended_lane": lane,
            "expected_outcome": "Decide whether discourse-side evidence is representative enough to stabilize the issue.",
            "probe_candidate": True,
            "contradiction_link_count": 0,
            "coverage_score": 1.0,
            "confidence": None,
            "pressure_score": 0.76,
            "readiness_blocker": True,
        }
    if lane == "stakeholder-deliberation-analysis" and link_status == "formal-only":
        return {
            "action_kind": "analyze-stakeholder-deliberation",
            "priority": "medium",
            "assigned_role": "sociologist",
            "objective": f"Check whether stakeholder positions around {issue_label} remain underrepresented.",
            "reason": f"Issue {issue_label} is staying in stakeholder-deliberation analysis while linkage posture is {link_status}.",
            "controversy_gap": "stakeholder-deliberation-gap",
            "recommended_lane": lane,
            "expected_outcome": "Clarify whether stakeholder positions need more explicit representation before the round can close.",
            "probe_candidate": True,
            "contradiction_link_count": 0,
            "coverage_score": 1.0,
            "confidence": None,
            "pressure_score": 0.64,
            "readiness_blocker": True,
        }
    return None


def verification_route_policy(
    route: dict[str, Any],
    *,
    issue_label: str,
    weakest_coverage: dict[str, Any],
    link_status: str,
) -> dict[str, Any] | None:
    claim_id = maybe_text(route.get("claim_id"))
    lane = maybe_text(route.get("recommended_lane")) or "mixed-review"
    route_status = maybe_text(route.get("route_status")) or "mixed-routing-review"
    if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
        return {
            "action_kind": "clarify-verification-route",
            "priority": "high",
            "assigned_role": "moderator",
            "objective": f"Clarify which lane should govern {issue_label}.",
            "reason": maybe_text(route.get("route_reason"))
            or f"Claim {claim_id or issue_label} is still under mixed routing review.",
            "controversy_gap": "verification-routing-gap",
            "recommended_lane": lane,
            "expected_outcome": "Freeze a single routing posture before more downstream work is queued.",
            "probe_candidate": True,
            "contradiction_link_count": 0,
            "coverage_score": float(weakest_coverage.get("coverage_score") or 1.0)
            if weakest_coverage
            else 1.0,
            "confidence": maybe_number(route.get("confidence")),
            "pressure_score": 0.88,
            "readiness_blocker": True,
        }
    if lane == "environmental-observation":
        coverage_readiness = maybe_text(weakest_coverage.get("readiness"))
        contradiction_count = int(weakest_coverage.get("contradiction_link_count") or 0)
        if empirical_issue_requires_followup(weakest_coverage):
            return {
                "action_kind": "advance-empirical-verification",
                "priority": "high",
                "assigned_role": role_from_coverage(weakest_coverage)
                if weakest_coverage
                else "environmentalist",
                "objective": f"Advance empirical verification for {issue_label}.",
                "reason": maybe_text(route.get("route_reason"))
                or f"Claim {claim_id or issue_label} is routed to environmental observation but its evidence basis is not yet stable.",
                "controversy_gap": (
                    "formal-public-misalignment"
                    if contradiction_count > 0
                    else "verification-gap"
                ),
                "recommended_lane": lane,
                "expected_outcome": "Decide whether the route should stay empirical and whether the issue can be stabilized with available signals.",
                "probe_candidate": contradiction_count > 0 or coverage_readiness in {"", "weak"},
                "contradiction_link_count": contradiction_count,
                "coverage_score": float(weakest_coverage.get("coverage_score") or 0.0)
                if weakest_coverage
                else 0.0,
                "confidence": maybe_number(route.get("confidence")),
                "pressure_score": 0.84 if contradiction_count == 0 else 0.9,
                "readiness_blocker": True,
            }
        return None
    if lane == "formal-comment-and-policy-record" and link_status == "public-only":
        return {
            "action_kind": "review-formal-record",
            "priority": "high",
            "assigned_role": "moderator",
            "objective": f"Review the formal record posture for {issue_label}.",
            "reason": maybe_text(route.get("route_reason"))
            or f"Claim {claim_id or issue_label} is routed to formal record review but linkage posture is {link_status}.",
            "controversy_gap": "formal-record-gap",
            "recommended_lane": lane,
            "expected_outcome": "Decide whether formal record material is enough to represent the issue cleanly.",
            "probe_candidate": True,
            "contradiction_link_count": 0,
            "coverage_score": 1.0,
            "confidence": maybe_number(route.get("confidence")),
            "pressure_score": 0.74,
            "readiness_blocker": True,
        }
    if lane in {"public-discourse-analysis", "stakeholder-deliberation-analysis"} and link_status == "formal-only":
        action_kind = (
            "analyze-public-discourse"
            if lane == "public-discourse-analysis"
            else "analyze-stakeholder-deliberation"
        )
        controversy_gap = (
            "public-discourse-gap"
            if lane == "public-discourse-analysis"
            else "stakeholder-deliberation-gap"
        )
        return {
            "action_kind": action_kind,
            "priority": "high",
            "assigned_role": "sociologist",
            "objective": f"Review the discourse-side representation posture for {issue_label}.",
            "reason": maybe_text(route.get("route_reason"))
            or f"Claim {claim_id or issue_label} stays in discourse-side review but linkage posture is {link_status}.",
            "controversy_gap": controversy_gap,
            "recommended_lane": lane,
            "expected_outcome": "Decide whether discourse or stakeholder representation is sufficient to stabilize the issue.",
            "probe_candidate": True,
            "contradiction_link_count": 0,
            "coverage_score": 1.0,
            "confidence": maybe_number(route.get("confidence")),
            "pressure_score": 0.72,
            "readiness_blocker": True,
        }
    return None


def claim_assessment_policy(
    assessment: dict[str, Any],
    *,
    issue_label: str,
    weakest_coverage: dict[str, Any],
    risk_flags: list[str],
) -> dict[str, Any] | None:
    claim_id = maybe_text(assessment.get("claim_id"))
    lane = maybe_text(assessment.get("recommended_lane")) or "mixed-review"
    route_ready = bool(assessment.get("route_to_observation_matching"))
    if lane in {"mixed-review", "route-before-matching"} or not route_ready:
        return {
            "action_kind": "clarify-verification-route",
            "priority": "high",
            "assigned_role": "moderator",
            "objective": f"Clarify which lane should govern {issue_label}.",
            "reason": maybe_text(assessment.get("assessment_summary"))
            or f"Claim {claim_id or issue_label} is not yet matching-ready and still needs routing clarification.",
            "controversy_gap": "verification-routing-gap",
            "recommended_lane": lane,
            "expected_outcome": "Freeze a routing decision before more downstream evidence work is queued.",
            "probe_candidate": True,
            "contradiction_link_count": 0,
            "coverage_score": float(weakest_coverage.get("coverage_score") or 1.0)
            if weakest_coverage
            else 1.0,
            "confidence": maybe_number(assessment.get("confidence")),
            "pressure_score": 0.86 if "not-matching-ready" in risk_flags else 0.76,
            "readiness_blocker": True,
        }
    if lane == "environmental-observation":
        coverage_readiness = maybe_text(weakest_coverage.get("readiness"))
        contradiction_count = int(weakest_coverage.get("contradiction_link_count") or 0)
        if empirical_issue_requires_followup(weakest_coverage):
            return {
                "action_kind": "advance-empirical-verification",
                "priority": "high",
                "assigned_role": role_from_coverage(weakest_coverage)
                if weakest_coverage
                else "environmentalist",
                "objective": f"Advance empirical verification for {issue_label}.",
                "reason": maybe_text(assessment.get("assessment_summary"))
                or f"Claim {claim_id or issue_label} is empirical but its evidence basis is not yet stable.",
                "controversy_gap": (
                    "formal-public-misalignment"
                    if contradiction_count > 0
                    else "verification-gap"
                ),
                "recommended_lane": lane,
                "expected_outcome": "Decide whether the claim can be grounded in environmental observations strongly enough to stabilize the issue.",
                "probe_candidate": contradiction_count > 0 or coverage_readiness in {"", "weak"},
                "contradiction_link_count": contradiction_count,
                "coverage_score": float(weakest_coverage.get("coverage_score") or 0.0)
                if weakest_coverage
                else 0.0,
                "confidence": maybe_number(assessment.get("confidence")),
                "pressure_score": 0.82 if contradiction_count == 0 else 0.9,
                "readiness_blocker": True,
            }
        return None
    action_kind = "review-formal-record"
    assigned_role = "moderator"
    controversy_gap = "formal-record-gap"
    if lane == "public-discourse-analysis":
        action_kind = "analyze-public-discourse"
        assigned_role = "sociologist"
        controversy_gap = "public-discourse-gap"
    elif lane == "stakeholder-deliberation-analysis":
        action_kind = "analyze-stakeholder-deliberation"
        assigned_role = "sociologist"
        controversy_gap = "stakeholder-deliberation-gap"
    return {
        "action_kind": action_kind,
        "priority": "medium",
        "assigned_role": assigned_role,
        "objective": f"Advance the {lane.replace('-', ' ')} posture for {issue_label}.",
        "reason": maybe_text(assessment.get("assessment_summary"))
        or f"Claim {claim_id or issue_label} is classified into the {lane.replace('-', ' ')} lane.",
        "controversy_gap": controversy_gap,
        "recommended_lane": lane,
        "expected_outcome": "Decide whether the issue has enough non-empirical structure to stabilize without further rerouting.",
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": 1.0,
        "confidence": maybe_number(assessment.get("confidence")),
        "pressure_score": 0.58,
        "readiness_blocker": True,
    }


def formal_public_link_policy(
    link: dict[str, Any],
    *,
    issue_label: str,
) -> dict[str, Any] | None:
    link_status = maybe_text(link.get("link_status"))
    if link_status in {"", "aligned"}:
        return None
    lane = maybe_text(link.get("recommended_lane")) or "mixed-review"
    pressure_score = max(0.56, 1.0 - float(link.get("alignment_score") or 0.0))
    return {
        "action_kind": "review-formal-public-linkage",
        "priority": priority_from_score(pressure_score),
        "assigned_role": role_from_lane(lane, default_role="moderator"),
        "objective": f"Review how formal and public material are linked for {issue_label}.",
        "reason": maybe_text(link.get("linkage_summary"))
        or f"Issue {issue_label} currently has a {link_status} formal/public linkage posture.",
        "controversy_gap": "formal-public-linkage-gap",
        "recommended_lane": lane,
        "expected_outcome": "Decide whether the issue still lacks enough formal or public representation to move forward cleanly.",
        "probe_candidate": link_status in {"public-only", "formal-only"},
        "contradiction_link_count": 0,
        "coverage_score": 1.0,
        "confidence": None,
        "pressure_score": pressure_score,
        "readiness_blocker": True,
    }


def representation_gap_policy(
    gap: dict[str, Any],
    *,
    issue_label: str,
    severity_score: float,
) -> dict[str, Any]:
    return {
        "action_kind": "address-representation-gap",
        "priority": maybe_text(gap.get("severity")) or priority_from_score(severity_score),
        "assigned_role": "sociologist",
        "objective": f"Address the representation gap around {issue_label}.",
        "reason": maybe_text(gap.get("gap_summary"))
        or maybe_text(gap.get("recommended_action"))
        or f"Issue {issue_label} currently carries a representation gap.",
        "controversy_gap": "representation-gap",
        "recommended_lane": maybe_text(gap.get("recommended_lane")) or "public-discourse-analysis",
        "expected_outcome": maybe_text(gap.get("recommended_action"))
        or "Decide how the round should repair the missing or imbalanced representation posture.",
        "probe_candidate": severity_score >= 0.72,
        "contradiction_link_count": 0,
        "coverage_score": 1.0,
        "confidence": None,
        "pressure_score": max(0.58, severity_score),
        "readiness_blocker": True,
    }


def diffusion_edge_policy(
    edge: dict[str, Any],
    *,
    issue_label: str,
    confidence: float,
) -> dict[str, Any] | None:
    edge_type = maybe_text(edge.get("edge_type"))
    if edge_type not in DIFFUSION_FOCUS_EDGE_TYPES or confidence < 0.72:
        return None
    priority = (
        "high"
        if edge_type in {"public-to-formal-spillover", "formal-to-public-spillover"}
        and confidence >= 0.8
        else "medium"
    )
    return {
        "action_kind": "trace-cross-platform-diffusion",
        "priority": priority,
        "assigned_role": "sociologist",
        "objective": f"Trace how {issue_label} is moving across platforms.",
        "reason": maybe_text(edge.get("edge_summary"))
        or f"Issue {issue_label} shows a {edge_type} pattern that may affect how the controversy is represented.",
        "controversy_gap": "cross-platform-diffusion",
        "recommended_lane": maybe_text(edge.get("recommended_lane")) or "public-discourse-analysis",
        "expected_outcome": "Decide whether diffusion across platforms materially changes how the issue should be represented or handed off.",
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": 1.0,
        "confidence": confidence,
        "pressure_score": min(0.86, confidence),
        "readiness_blocker": False,
    }
