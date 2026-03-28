"""Expert-report builders extracted from reporting_drafts."""

from __future__ import annotations

from typing import Any

from eco_council_runtime.application.reporting.common import (
    claim_sort_key,
    claims_by_id_map,
    evidence_by_claim_map,
    evidence_rank,
    gap_to_question,
    infer_missing_evidence_types,
    metrics_for_evidence,
    observations_by_id_map,
)
from eco_council_runtime.application.reporting.readiness import (
    build_pre_match_report_findings,
    generic_readiness_recommendations,
    readiness_missing_types,
)
from eco_council_runtime.application.reporting.recommendations import prioritized_recommendations_from_state
from eco_council_runtime.application.reporting_state import matching_executed_for_state, mission_run_id, state_submissions
from eco_council_runtime.domain.contract_bridge import (
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings
from eco_council_runtime.planning import combine_recommendations

SCHEMA_VERSION = resolve_schema_version("1.0.0")


def report_status_for_role(
    *,
    role: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
) -> str:
    if role == "sociologist":
        if not claims:
            return "blocked"
        if not evidence_cards or any(maybe_text(card.get("verdict")) in {"mixed", "insufficient"} for card in evidence_cards):
            return "needs-more-evidence"
        return "complete"
    if not observations and not evidence_cards:
        return "blocked"
    if not evidence_cards or any(maybe_text(card.get("verdict")) in {"mixed", "insufficient"} for card in evidence_cards):
        return "needs-more-evidence"
    return "complete"


def build_summary_for_role(
    *,
    role: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
) -> str:
    verdict_counts = {
        verdict: sum(1 for item in evidence_cards if maybe_text(item.get("verdict")) == verdict)
        for verdict in ("supports", "contradicts", "mixed", "insufficient")
    }
    if role == "sociologist":
        if not claims:
            return "No normalized public claims were available for this round."
        return (
            f"The round produced {len(claims)} candidate public claims. "
            f"Evidence verdicts currently include {verdict_counts.get('supports', 0)} supports, "
            f"{verdict_counts.get('contradicts', 0)} contradicts, "
            f"{verdict_counts.get('mixed', 0)} mixed, and "
            f"{verdict_counts.get('insufficient', 0)} insufficient."
        )
    if not observations and not evidence_cards:
        return "No mission-aligned physical observations were available for this round."
    metric_counts = {
        maybe_text(item.get("metric")): sum(1 for observation in observations if maybe_text(observation.get("metric")) == maybe_text(item.get("metric")))
        for item in observations
        if maybe_text(item.get("metric"))
    }
    metric_text = ", ".join(sorted(metric_counts)) if metric_counts else "no linked metrics"
    return (
        f"The round produced {len(observations)} observations and {len(evidence_cards)} evidence cards. "
        f"Current physical coverage includes {metric_text}."
    )


def build_sociologist_findings(
    *,
    claims: list[dict[str, Any]],
    evidence_by_claim: dict[str, dict[str, Any]],
    observations_by_id: dict[str, dict[str, Any]],
    max_findings: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, claim in enumerate(sorted(claims, key=claim_sort_key)[:max_findings], start=1):
        claim_id = maybe_text(claim.get("claim_id"))
        card = evidence_by_claim.get(claim_id)
        title = truncate_text(maybe_text(claim.get("summary")) or maybe_text(claim.get("statement")), 72)
        if card is None:
            summary = f"Claim {claim_id} was captured from public signals but has not yet been linked to physical evidence."
            confidence = "low"
            observation_ids: list[str] = []
            evidence_ids: list[str] = []
        else:
            metrics = metrics_for_evidence(card, observations_by_id)
            metric_text = f" Linked metrics: {', '.join(metrics[:4])}." if metrics else ""
            summary = f"Claim {claim_id} is currently {maybe_text(card.get('verdict'))}. {maybe_text(card.get('summary'))}{metric_text}".strip()
            confidence = maybe_text(card.get("confidence")) or "low"
            observation_ids = [maybe_text(item) for item in card.get("observation_ids", []) if maybe_text(item)]
            evidence_ids = [maybe_text(card.get("evidence_id"))] if maybe_text(card.get("evidence_id")) else []
        findings.append(
            {
                "finding_id": f"finding-{index:03d}",
                "title": title or f"Claim {claim_id}",
                "summary": truncate_text(summary, 300),
                "confidence": confidence,
                "claim_ids": [claim_id],
                "observation_ids": observation_ids[:6],
                "evidence_ids": evidence_ids,
            }
        )
    return findings


def build_environmentalist_findings(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    observations_by_id: dict[str, dict[str, Any]],
    claims_by_id: dict[str, dict[str, Any]],
    max_findings: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    ordered_cards = sorted(
        evidence_cards,
        key=lambda item: (
            evidence_rank(item),
            claim_sort_key(claims_by_id.get(maybe_text(item.get("claim_id")), {})),
            maybe_text(item.get("evidence_id")),
        ),
    )
    for index, card in enumerate(ordered_cards[:max_findings], start=1):
        claim_id = maybe_text(card.get("claim_id"))
        claim = claims_by_id.get(claim_id, {})
        metrics = metrics_for_evidence(card, observations_by_id)
        metric_text = ", ".join(metrics[:4]) if metrics else "linked observations"
        findings.append(
            {
                "finding_id": f"finding-{index:03d}",
                "title": truncate_text(maybe_text(claim.get("summary")) or f"Physical evidence for {claim_id}", 72),
                "summary": truncate_text(f"{maybe_text(card.get('summary'))} Main metrics: {metric_text}.", 300),
                "confidence": maybe_text(card.get("confidence")) or "low",
                "claim_ids": [claim_id] if claim_id else [],
                "observation_ids": [maybe_text(item) for item in card.get("observation_ids", []) if maybe_text(item)][:8],
                "evidence_ids": [maybe_text(card.get("evidence_id"))] if maybe_text(card.get("evidence_id")) else [],
            }
        )

    if findings:
        return findings

    for index, observation in enumerate(observations[:max_findings], start=1):
        findings.append(
            {
                "finding_id": f"finding-{index:03d}",
                "title": truncate_text(f"{maybe_text(observation.get('metric'))} observation", 72),
                "summary": truncate_text(
                    (
                        f"Observation {maybe_text(observation.get('observation_id'))} reports "
                        f"{maybe_text(observation.get('metric'))}={observation.get('value')} "
                        f"{maybe_text(observation.get('unit'))} from {maybe_text(observation.get('source_skill'))}."
                    ),
                    300,
                ),
                "confidence": "medium",
                "claim_ids": [],
                "observation_ids": [maybe_text(observation.get("observation_id"))] if maybe_text(observation.get("observation_id")) else [],
                "evidence_ids": [],
            }
        )
    return findings


def build_open_questions(evidence_cards: list[dict[str, Any]]) -> list[str]:
    questions: list[str] = []
    for card in evidence_cards:
        items = card.get("gaps")
        if not isinstance(items, list):
            continue
        for item in items:
            questions.append(gap_to_question(maybe_text(item)))
    return unique_strings(questions)[:5]


def build_report_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    role: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    max_findings: int,
) -> dict[str, Any]:
    evidence_by_claim = evidence_by_claim_map(evidence_cards)
    observations_by_id = observations_by_id_map(observations)
    claims_by_id = claims_by_id_map(claims)
    if role == "sociologist":
        findings = build_sociologist_findings(
            claims=claims,
            evidence_by_claim=evidence_by_claim,
            observations_by_id=observations_by_id,
            max_findings=max_findings,
        )
    else:
        findings = build_environmentalist_findings(
            claims=claims,
            observations=observations,
            evidence_cards=evidence_cards,
            observations_by_id=observations_by_id,
            claims_by_id=claims_by_id,
            max_findings=max_findings,
        )
    missing_types = infer_missing_evidence_types(claims=claims, observations=observations, evidence_cards=evidence_cards)
    recommendations = combine_recommendations(reports=[], missing_types=missing_types)[:4]
    open_questions = build_open_questions(evidence_cards)
    status = report_status_for_role(role=role, claims=claims, observations=observations, evidence_cards=evidence_cards)
    if status == "blocked" and not open_questions:
        if role == "sociologist":
            open_questions = ["Should the next round expand public-signal collection before report writing?"]
        else:
            open_questions = ["Should the next round expand physical-source coverage before physical validation resumes?"]
    draft = {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"report-{role}-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": role,
        "status": status,
        "summary": build_summary_for_role(role=role, claims=claims, observations=observations, evidence_cards=evidence_cards),
        "findings": findings,
        "open_questions": open_questions,
        "recommended_next_actions": recommendations,
        "override_requests": [],
    }
    validate_payload("expert-report", draft)
    return draft


def expert_report_status_from_state(state: dict[str, Any], role: str) -> str:
    authorization_status = maybe_text(state.get("matching_authorization", {}).get("authorization_status"))
    readiness_report = state.get("readiness_reports", {}).get(role)
    if isinstance(readiness_report, dict) and maybe_text(readiness_report.get("readiness_status")) == "blocked":
        return "blocked"
    if authorization_status == "authorized" and not matching_executed_for_state(state):
        return "blocked"
    if matching_executed_for_state(state):
        if state.get("remands_open") or bool(state.get("evidence_adjudication", {}).get("needs_additional_data")):
            return "needs-more-evidence"
        return "complete"
    if authorization_status in {"deferred", "not-authorized"}:
        return "needs-more-evidence"
    if isinstance(readiness_report, dict) and readiness_report:
        return "needs-more-evidence"
    return "blocked"


def build_report_summary_from_state(state: dict[str, Any], role: str, status: str) -> str:
    readiness_report = state.get("readiness_reports", {}).get(role)
    if matching_executed_for_state(state):
        cards = state.get("cards_active", [])
        isolated = state.get("isolated_active", [])
        remands = state.get("remands_open", [])
        moderator_adjudication = state.get("matching_adjudication", {})
        adjudication = state.get("evidence_adjudication", {})
        investigation_review = state.get("investigation_review", {})
        return truncate_text(
            (
                f"Matching/adjudication is available with {len(cards)} active evidence cards, "
                f"{len(isolated)} isolated entries, and {len(remands)} open remands. "
                f"Status={status}. "
                f"{maybe_text(investigation_review.get('summary')) or maybe_text(moderator_adjudication.get('summary')) or maybe_text(adjudication.get('summary'))}"
            ),
            400,
        )
    authorization = state.get("matching_authorization", {})
    readiness_summary = maybe_text(readiness_report.get("summary")) if isinstance(readiness_report, dict) else ""
    authorization_summary = maybe_text(authorization.get("summary"))
    if readiness_summary and authorization_summary:
        return truncate_text(f"{readiness_summary} {authorization_summary}", 400)
    if readiness_summary:
        return truncate_text(readiness_summary, 400)
    if authorization_summary:
        return truncate_text(authorization_summary, 400)
    return "The round does not yet have enough canonical readiness or matching artifacts to support a stronger expert summary."


def build_expert_report_draft_from_state(
    *,
    state: dict[str, Any],
    role: str,
    max_findings: int,
) -> dict[str, Any]:
    mission = state["mission"]
    round_id = state["round_id"]
    claims = state.get("claims", []) if isinstance(state.get("claims"), list) else []
    observations = state.get("observations", []) if isinstance(state.get("observations"), list) else []
    evidence_cards = state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else []
    if matching_executed_for_state(state):
        moderator_adjudication = state.get("matching_adjudication", {}) if isinstance(state.get("matching_adjudication"), dict) else {}
        investigation_review = state.get("investigation_review", {}) if isinstance(state.get("investigation_review"), dict) else {}
        evidence_by_claim = evidence_by_claim_map(evidence_cards)
        observations_by_id = observations_by_id_map(observations)
        claims_by_id = claims_by_id_map(claims)
        if role == "sociologist":
            findings = build_sociologist_findings(
                claims=claims,
                evidence_by_claim=evidence_by_claim,
                observations_by_id=observations_by_id,
                max_findings=max_findings,
            )
        else:
            findings = build_environmentalist_findings(
                claims=claims,
                observations=observations,
                evidence_cards=evidence_cards,
                observations_by_id=observations_by_id,
                claims_by_id=claims_by_id,
                max_findings=max_findings,
            )
        open_questions = build_open_questions(evidence_cards)
        open_questions.extend(
            f"How should the council resolve remand {maybe_text(item.get('remand_id'))}?"
            for item in state.get("remands_open", [])
            if isinstance(item, dict) and maybe_text(item.get("remand_id"))
        )
        open_questions.extend(
            maybe_text(item)
            for item in moderator_adjudication.get("open_questions", [])
            if maybe_text(item)
        )
        open_questions.extend(
            maybe_text(item)
            for item in investigation_review.get("open_questions", [])
            if maybe_text(item)
        )
        readiness_report = state.get("readiness_reports", {}).get(role)
        additional_reports = [
            item
            for item in [moderator_adjudication, investigation_review, readiness_report]
            if isinstance(item, dict) and item
        ]
        recommendations = prioritized_recommendations_from_state(
            state=state,
            reports=[item for item in additional_reports if isinstance(item, dict)],
            missing_types=[],
            limit=4,
        )
    else:
        findings = build_pre_match_report_findings(state, role, max_findings)
        readiness_report = state.get("readiness_reports", {}).get(role)
        open_questions = []
        recommendation_inputs: list[dict[str, Any]] = []
        if isinstance(readiness_report, dict):
            open_questions.extend(
                maybe_text(item)
                for item in readiness_report.get("open_questions", [])
                if maybe_text(item)
            )
            recommendation_inputs.append(readiness_report)
        missing_types = readiness_missing_types(state, role)
        recommendations = prioritized_recommendations_from_state(
            state=state,
            reports=recommendation_inputs,
            missing_types=missing_types,
            limit=4,
        )
        if not recommendations:
            recommendations = generic_readiness_recommendations(
                role,
                missing_types,
                has_submissions=bool(state_submissions(state, role)),
            )[:4]
    status = expert_report_status_from_state(state, role)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"report-{role}-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": role,
        "status": status,
        "summary": build_report_summary_from_state(state, role, status),
        "findings": findings,
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations[:4],
        "override_requests": [],
    }
    validate_payload("expert-report", payload)
    return payload


__all__ = [
    "build_environmentalist_findings",
    "build_expert_report_draft_from_state",
    "build_open_questions",
    "build_report_draft",
    "build_report_summary_from_state",
    "build_sociologist_findings",
    "build_summary_for_role",
    "expert_report_status_from_state",
    "report_status_for_role",
]
