"""Council-decision builders extracted from reporting_drafts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.application.reporting.common import (
    READINESS_ROLES,
    VERDICT_SCORES,
    expected_output_kinds_for_role,
    report_has_substance,
)
from eco_council_runtime.application.reporting.readiness import readiness_missing_types
from eco_council_runtime.application.reporting.recommendations import prioritized_recommendations_from_state
from eco_council_runtime.application.reporting_state import (
    matching_executed_for_state,
    mission_constraints,
    mission_run_id,
    state_auditable_submissions,
)
from eco_council_runtime.domain.contract_bridge import (
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.rounds import current_round_number
from eco_council_runtime.domain.text import maybe_text, truncate_text
from eco_council_runtime.planning import (
    build_decision_override_requests,
    build_next_round_tasks,
    collect_unresolved_anchor_refs,
)

SCHEMA_VERSION = resolve_schema_version("1.0.0")


def investigation_review_decision_gating(review: dict[str, Any]) -> dict[str, Any]:
    decision_gating = review.get("decision_gating") if isinstance(review.get("decision_gating"), dict) else {}
    if decision_gating:
        return {
            "another_round_required": bool(decision_gating.get("another_round_required")),
            "reason_codes": [
                maybe_text(item)
                for item in (decision_gating.get("reason_codes") if isinstance(decision_gating.get("reason_codes"), list) else [])
                if maybe_text(item)
            ],
            "reasons": [
                maybe_text(item)
                for item in (decision_gating.get("reasons") if isinstance(decision_gating.get("reasons"), list) else [])
                if maybe_text(item)
            ],
            "unresolved_required_leg_count": int(decision_gating.get("unresolved_required_leg_count") or 0),
            "contradiction_path_count": int(decision_gating.get("contradiction_path_count") or 0),
            "active_alternative_count": int(decision_gating.get("active_alternative_count") or 0),
        }

    hypothesis_reviews = review.get("hypothesis_reviews") if isinstance(review.get("hypothesis_reviews"), list) else []
    unresolved_required_leg_count = 0
    contradiction_path_count = 0
    active_alternative_count = 0
    reason_codes: list[str] = []
    reasons: list[str] = []
    for hypothesis_review in hypothesis_reviews:
        if not isinstance(hypothesis_review, dict):
            continue
        unresolved_required = [
            maybe_text(item)
            for item in (
                hypothesis_review.get("unresolved_required_leg_ids")
                if isinstance(hypothesis_review.get("unresolved_required_leg_ids"), list)
                else []
            )
            if maybe_text(item)
        ]
        if not unresolved_required:
            unresolved_required = [
                maybe_text(item.get("leg_id"))
                for item in (
                    hypothesis_review.get("leg_reviews") if isinstance(hypothesis_review.get("leg_reviews"), list) else []
                )
                if isinstance(item, dict) and bool(item.get("status") != "supported") and maybe_text(item.get("leg_id"))
            ]
        unresolved_required_leg_count += len(unresolved_required)
        if unresolved_required:
            reason_codes.append("required-leg-unresolved")
        contradiction_items = [
            item
            for item in (
                hypothesis_review.get("contradiction_paths")
                if isinstance(hypothesis_review.get("contradiction_paths"), list)
                else []
            )
            if isinstance(item, dict) and maybe_text(item.get("leg_id"))
        ]
        if not contradiction_items:
            contradiction_items = [
                item
                for item in (
                    hypothesis_review.get("leg_reviews") if isinstance(hypothesis_review.get("leg_reviews"), list) else []
                )
                if isinstance(item, dict) and maybe_text(item.get("status")) == "contradicted"
            ]
        contradiction_path_count += len(contradiction_items)
        if contradiction_items:
            reason_codes.append("contradiction-active")
        active_alternatives = [
            maybe_text(item.get("alternative_id"))
            for item in (
                hypothesis_review.get("alternative_reviews")
                if isinstance(hypothesis_review.get("alternative_reviews"), list)
                else []
            )
            if isinstance(item, dict)
            and maybe_text(item.get("alternative_id"))
            and maybe_text(item.get("status")) != "deprioritized"
        ]
        active_alternative_count += len(active_alternatives)
        if active_alternatives:
            reason_codes.append("alternative-still-active")
        reasons.extend(
            maybe_text(item)
            for item in (
                hypothesis_review.get("another_round_reasons")
                if isinstance(hypothesis_review.get("another_round_reasons"), list)
                else []
            )
            if maybe_text(item)
        )

    if not reasons:
        if unresolved_required_leg_count > 0:
            reasons.append(
                f"{unresolved_required_leg_count} required leg(s) remain unresolved in the moderator review."
            )
        if contradiction_path_count > 0:
            reasons.append(
                f"{contradiction_path_count} contradiction path(s) remain active in the moderator review."
            )
        if active_alternative_count > 0:
            reasons.append(
                f"{active_alternative_count} competing alternative(s) remain active in the moderator review."
            )

    return {
        "another_round_required": bool(reason_codes),
        "reason_codes": sorted({code for code in reason_codes if code}),
        "reasons": list(dict.fromkeys(reason for reason in reasons if reason))[:6],
        "unresolved_required_leg_count": unresolved_required_leg_count,
        "contradiction_path_count": contradiction_path_count,
        "active_alternative_count": active_alternative_count,
    }


def summarize_investigation_review_gating(gating: dict[str, Any]) -> str:
    reasons = [
        maybe_text(item)
        for item in (gating.get("reasons") if isinstance(gating.get("reasons"), list) else [])
        if maybe_text(item)
    ]
    if reasons:
        return "; ".join(reasons)
    fragments: list[str] = []
    unresolved_required_leg_count = int(gating.get("unresolved_required_leg_count") or 0)
    contradiction_path_count = int(gating.get("contradiction_path_count") or 0)
    active_alternative_count = int(gating.get("active_alternative_count") or 0)
    if unresolved_required_leg_count > 0:
        fragments.append(f"{unresolved_required_leg_count} required leg(s) remain unresolved")
    if contradiction_path_count > 0:
        fragments.append(f"{contradiction_path_count} contradiction path(s) remain active")
    if active_alternative_count > 0:
        fragments.append(f"{active_alternative_count} competing alternative(s) remain active")
    return "; ".join(fragments)


def readiness_score(state: dict[str, Any]) -> float:
    readiness_reports = state.get("readiness_reports", {}) if isinstance(state.get("readiness_reports"), dict) else {}
    if not readiness_reports:
        return 0.0
    completed = sum(1 for role in READINESS_ROLES if isinstance(readiness_reports.get(role), dict) and readiness_reports.get(role))
    sufficient = sum(1 for role in READINESS_ROLES if bool((readiness_reports.get(role) or {}).get("sufficient_for_matching")))
    total = max(1, len(READINESS_ROLES))
    return round(((completed / total) * 0.5) + ((sufficient / total) * 0.5), 2)


def missing_types_from_reason_texts(texts: list[str]) -> list[str]:
    missing: set[str] = set()
    for text in texts:
        lowered = maybe_text(text).lower()
        if not lowered:
            continue
        if "station" in lowered or "pm2" in lowered or "air-quality" in lowered:
            missing.add("station-air-quality")
        if "fire" in lowered or "wildfire" in lowered:
            missing.add("fire-detection")
        if "wind" in lowered or "humidity" in lowered or "weather" in lowered or "meteorology" in lowered:
            missing.add("meteorology-background")
        if "flood" in lowered or "river" in lowered or "hydrology" in lowered or "precipitation" in lowered:
            missing.add("precipitation-hydrology")
        if "temperature" in lowered or "heat" in lowered:
            missing.add("temperature-extremes")
        if "soil" in lowered or "drought" in lowered:
            missing.add("precipitation-soil-moisture")
        if "policy" in lowered or "comment" in lowered or "docket" in lowered:
            missing.add("policy-comment-coverage")
        if "public" in lowered or "discussion" in lowered or "claim" in lowered or "attributable" in lowered:
            missing.add("public-discussion-coverage")
    return sorted(missing)


def build_decision_missing_types(state: dict[str, Any]) -> list[str]:
    if matching_executed_for_state(state):
        reason_texts: list[str] = []
        for item in state.get("remands_open", []):
            if not isinstance(item, dict):
                continue
            reason_texts.extend(
                maybe_text(reason)
                for reason in item.get("reasons", [])
                if maybe_text(reason)
            )
        if reason_texts:
            return missing_types_from_reason_texts(reason_texts)
        return []
    return sorted(
        {
            *readiness_missing_types(state, "sociologist"),
            *readiness_missing_types(state, "environmentalist"),
        }
    )


def build_decision_summary_from_state(
    *,
    state: dict[str, Any],
    moderator_status: str,
    evidence_sufficiency: str,
    report_sources: dict[str, str],
    blocked_reason: str,
    review_gating: dict[str, Any] | None = None,
) -> str:
    if moderator_status == "blocked" and blocked_reason:
        return blocked_reason
    if matching_executed_for_state(state):
        cards = len(state.get("cards_active", []))
        isolated = len(state.get("isolated_active", []))
        remands = len(state.get("remands_open", []))
        moderator_adjudication = state.get("matching_adjudication", {})
        adjudication = state.get("evidence_adjudication", {})
        investigation_review = state.get("investigation_review", {})
        gating_summary = summarize_investigation_review_gating(review_gating or {})
        if moderator_status == "continue":
            return (
                f"Matching/adjudication produced {cards} cards, {isolated} isolated entries, and {remands} remands. "
                f"Another round is required before closure"
                f"{f' because {gating_summary}' if gating_summary else ''}. Report sources used: "
                f"{', '.join(f'{role}:{source}' for role, source in sorted(report_sources.items()))}."
            )
        return truncate_text(
            (
                f"Matching/adjudication is {evidence_sufficiency}. "
                f"{maybe_text(investigation_review.get('summary')) or maybe_text(moderator_adjudication.get('summary')) or maybe_text(adjudication.get('summary'))}"
            ),
            400,
        )
    authorization = state.get("matching_authorization", {})
    readiness_reports = state.get("readiness_reports", {})
    readiness_summary = " ".join(
        maybe_text(report.get("summary"))
        for role in READINESS_ROLES
        for report in [readiness_reports.get(role)]
        if isinstance(report, dict)
    )
    return truncate_text(
        (
            f"Matching was not executed. Authorization status is {maybe_text(authorization.get('authorization_status'))} "
            f"(basis={maybe_text(authorization.get('authorization_basis')) or 'unspecified'}). "
            f"Current evidence sufficiency is {evidence_sufficiency}. {readiness_summary}"
        ),
        400,
    )


def evidence_resolution_score(evidence_cards: list[dict[str, Any]]) -> float:
    if not evidence_cards:
        return 0.0
    total = 0.0
    for card in evidence_cards:
        total += VERDICT_SCORES.get(maybe_text(card.get("verdict")), 0.0)
    return total / len(evidence_cards)


def report_completion_score(reports: list[dict[str, Any]]) -> float:
    if not reports:
        return 0.0
    complete = 0
    for report in reports:
        if report_has_substance(report):
            complete += 1
    return complete / len(reports)


def completion_score_for_round(evidence_cards: list[dict[str, Any]], reports: list[dict[str, Any]]) -> float:
    score = 0.1 + 0.7 * evidence_resolution_score(evidence_cards) + 0.2 * report_completion_score(reports)
    score = max(0.0, min(1.0, score))
    return round(score, 2)


def evidence_sufficiency_for_round(evidence_cards: list[dict[str, Any]], missing_evidence_types: list[str]) -> str:
    if not evidence_cards:
        return "insufficient"
    verdicts = [maybe_text(item.get("verdict")) for item in evidence_cards]
    confidences = [maybe_text(item.get("confidence")) for item in evidence_cards]
    if any(verdict in {"mixed", "insufficient", ""} for verdict in verdicts):
        return "insufficient"
    if missing_evidence_types:
        return "partial"
    if confidences and all(confidence == "low" for confidence in confidences):
        return "partial"
    if len(evidence_cards) > 1 and any(confidence == "low" for confidence in confidences):
        return "partial"
    if set(verdicts) <= {"supports", "contradicts"}:
        return "sufficient"
    return "partial"


def build_final_brief(*, moderator_status: str, decision_summary: str, reports: dict[str, dict[str, Any] | None]) -> str:
    if moderator_status == "continue":
        return ""
    summaries: list[str] = []
    seen: set[str] = set()
    for item in [decision_summary] + [maybe_text(report.get("summary")) for report in reports.values() if isinstance(report, dict)]:
        text = maybe_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        summaries.append(text)
    return truncate_text(" ".join(summaries), 600)


def build_decision_draft_from_state(
    *,
    run_dir: Path,
    state: dict[str, Any],
    next_round_id: str,
    reports: dict[str, dict[str, Any] | None],
    report_sources: dict[str, str],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    mission = state["mission"]
    round_id = state["round_id"]
    usable_reports = [report for report in reports.values() if isinstance(report, dict)]
    missing_types = build_decision_missing_types(state)
    focus_claim_ids, anchor_refs = collect_unresolved_anchor_refs(state)
    readiness_reports = state.get("readiness_reports", {})
    recommendation_inputs = [
        item
        for item in [state.get("matching_adjudication"), state.get("investigation_review")]
        if isinstance(item, dict) and item
    ] + usable_reports + [
        report
        for report in readiness_reports.values()
        if isinstance(report, dict) and report
    ]
    recommendations = prioritized_recommendations_from_state(
        state=state,
        reports=recommendation_inputs,
        missing_types=missing_types,
        limit=None,
    )
    next_round_tasks, task_plan_info = build_next_round_tasks(
        schema_version=SCHEMA_VERSION,
        mission=mission,
        current_round_id=round_id,
        next_round_id=next_round_id,
        recommendations=recommendations,
        focus_claim_ids=focus_claim_ids,
        anchor_refs=anchor_refs,
        mission_run_id=mission_run_id,
        mission_constraints=mission_constraints,
        expected_output_kinds_for_role=expected_output_kinds_for_role,
        validate_payload=validate_payload,
    )
    max_rounds = mission_constraints(mission).get("max_rounds")
    current_number = current_round_number(round_id)
    next_number = current_round_number(next_round_id)
    authorization_status = maybe_text(state.get("matching_authorization", {}).get("authorization_status"))
    investigation_review = state.get("investigation_review", {}) if isinstance(state.get("investigation_review"), dict) else {}
    review_gating = investigation_review_decision_gating(investigation_review)
    review_requires_another_round = bool(review_gating.get("another_round_required"))
    blocked_reason = ""
    blocked_by_max_rounds = False
    if (
        not state.get("claims")
        and not state.get("observations")
        and not state_auditable_submissions(state, "sociologist")
        and not state_auditable_submissions(state, "environmentalist")
    ):
        moderator_status = "blocked"
        next_round_required = False
        blocked_reason = "The round did not produce enough auditable submissions, claims, or observations to continue."
    elif authorization_status == "authorized" and not matching_executed_for_state(state):
        moderator_status = "blocked"
        next_round_required = False
        blocked_reason = "Matching was authorized but matching/adjudication has not been executed yet."
    elif authorization_status in {"deferred", "not-authorized"}:
        if max_rounds is not None and current_number is not None and next_number is not None and next_number > max_rounds and next_round_tasks:
            moderator_status = "blocked"
            next_round_required = False
            blocked_reason = f"The configured max_rounds={max_rounds} would be exceeded by {next_round_id}."
            next_round_tasks = []
            blocked_by_max_rounds = True
        elif next_round_tasks:
            moderator_status = "continue"
            next_round_required = True
        else:
            moderator_status = "blocked"
            next_round_required = False
            blocked_reason = "Matching was not authorized and no concrete next-round tasks could be derived."
    elif state.get("remands_open") or missing_types or review_requires_another_round:
        if max_rounds is not None and current_number is not None and next_number is not None and next_number > max_rounds and next_round_tasks:
            moderator_status = "blocked"
            next_round_required = False
            blocked_reason = f"The configured max_rounds={max_rounds} would be exceeded by {next_round_id}."
            next_round_tasks = []
            blocked_by_max_rounds = True
        elif next_round_tasks:
            moderator_status = "continue"
            next_round_required = True
        else:
            moderator_status = "blocked"
            next_round_required = False
            if review_requires_another_round:
                blocked_reason = (
                    "The moderator review still requires another round, but no materially different next-round task could be derived."
                )
            else:
                blocked_reason = "The round still has unresolved evidence issues, but no materially different next-round task could be derived."
    else:
        moderator_status = "complete"
        next_round_required = False
        next_round_tasks = []
    if matching_executed_for_state(state):
        if state.get("remands_open") or review_requires_another_round:
            evidence_sufficiency = "partial"
        elif state.get("cards_active") or state.get("isolated_active"):
            evidence_sufficiency = "sufficient"
        else:
            evidence_sufficiency = "insufficient"
        completion_score = completion_score_for_round(state.get("cards_active", []), usable_reports)
    else:
        evidence_sufficiency = "partial" if readiness_score(state) >= 0.75 else "insufficient"
        completion_score = round(min(1.0, 0.2 + (0.4 * readiness_score(state)) + (0.4 * report_completion_score(usable_reports))), 2)
    override_requests = build_decision_override_requests(
        schema_version=SCHEMA_VERSION,
        mission=mission,
        round_id=round_id,
        next_round_id=next_round_id,
        focus_claim_ids=focus_claim_ids,
        anchor_refs=anchor_refs,
        task_plan_info=task_plan_info,
        next_round_requested_but_blocked_by_max_rounds=blocked_by_max_rounds,
        mission_run_id=mission_run_id,
        mission_constraints=mission_constraints,
        current_round_number=current_round_number,
        validate_payload=validate_payload,
    )
    decision_summary = build_decision_summary_from_state(
        state=state,
        moderator_status=moderator_status,
        evidence_sufficiency=evidence_sufficiency,
        report_sources=report_sources,
        blocked_reason=blocked_reason,
        review_gating=review_gating,
    )
    final_brief = build_final_brief(moderator_status=moderator_status, decision_summary=decision_summary, reports=reports)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "decision_id": f"decision-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "moderator_status": moderator_status,
        "completion_score": completion_score,
        "evidence_sufficiency": evidence_sufficiency,
        "decision_summary": decision_summary,
        "next_round_required": next_round_required,
        "decision_gating": review_gating,
        "missing_evidence_types": missing_types,
        "next_round_tasks": next_round_tasks,
        "override_requests": override_requests,
        "final_brief": final_brief,
    }
    validate_payload("council-decision", payload)
    return payload, next_round_tasks, missing_types


__all__ = [
    "build_decision_draft_from_state",
    "build_decision_missing_types",
    "build_decision_summary_from_state",
    "build_final_brief",
    "completion_score_for_round",
    "evidence_resolution_score",
    "evidence_sufficiency_for_round",
    "investigation_review_decision_gating",
    "missing_types_from_reason_texts",
    "readiness_score",
    "report_completion_score",
    "summarize_investigation_review_gating",
]
