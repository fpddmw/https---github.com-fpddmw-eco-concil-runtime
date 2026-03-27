"""Reporting, readiness, investigation, and decision draft builders."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.application.reporting_state import (
    matching_executed_for_state,
    mission_constraints,
    mission_run_id,
    state_auditable_submissions,
    state_current_submissions,
    state_submissions,
)
from eco_council_runtime.application.reporting_views import (
    aggregate_compact_audit,
    claim_submission_channels,
    compact_statistics,
    counter_dict,
    maybe_number,
    observation_metric_family,
    public_source_channels,
    representative_submissions,
    to_nonnegative_int,
)
from eco_council_runtime.domain.contract_bridge import (
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.normalize_semantics import HYDROLOGY_METRICS, METEOROLOGY_METRICS, PRECIPITATION_METRICS
from eco_council_runtime.domain.rounds import current_round_number
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings
from eco_council_runtime.planning import (
    base_recommendations_from_missing_types,
    build_decision_override_requests,
    build_next_round_tasks,
    collect_unresolved_anchor_refs,
    combine_recommendations,
    recommendation_key,
)

SCHEMA_VERSION = resolve_schema_version("1.0.0")
READINESS_ROLES = ("sociologist", "environmentalist")
VERDICT_SCORES = {"supports": 1.0, "contradicts": 1.0, "mixed": 0.6, "insufficient": 0.25}
QUESTION_RULES = (
    ("station-grade corroboration is missing", "Can station-grade air-quality measurements be added for the same mission window?"),
    ("modeled background fields should be cross-checked", "Can modeled air-quality fields be cross-checked with station or local observations?"),
    ("no mission-aligned observations matched", "Should the next round expand physical coverage or narrow claim scope so observations can be matched?"),
)
INVESTIGATION_LEG_METRIC_FAMILIES: dict[str, dict[str, set[str]]] = {
    "local-event": {
        "impact": {"air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other"},
        "public_interpretation": set(),
    },
    "smoke-transport": {
        "source": {"fire-detection"},
        "mechanism": {"meteorology"},
        "impact": {"air-quality"},
        "public_interpretation": set(),
    },
    "flood-upstream": {
        "source": {"meteorology", "hydrology"},
        "mechanism": {"meteorology", "hydrology"},
        "impact": {"hydrology"},
        "public_interpretation": set(),
    },
}


def report_is_placeholder(report: dict[str, Any] | None) -> bool:
    if not isinstance(report, dict):
        return False
    return maybe_text(report.get("summary")).lower().startswith("pending ")


def report_has_substance(report: dict[str, Any] | None) -> bool:
    if not isinstance(report, dict):
        return False
    if report_is_placeholder(report):
        return False
    if report.get("findings"):
        return True
    return bool(report.get("open_questions") or report.get("recommended_next_actions"))


def claim_sort_key(claim: dict[str, Any]) -> tuple[int, str]:
    priority = claim.get("priority")
    if not isinstance(priority, int):
        priority = 99
    return (priority, maybe_text(claim.get("claim_id")))


def evidence_rank(card: dict[str, Any]) -> int:
    verdict = maybe_text(card.get("verdict"))
    if verdict in {"supports", "contradicts"}:
        return 0
    if verdict == "mixed":
        return 1
    return 2


def gap_to_question(gap: str) -> str:
    lowered = maybe_text(gap).lower()
    for needle, question in QUESTION_RULES:
        if needle in lowered:
            return question
    if lowered.endswith("?"):
        return gap
    return f"How should the next round address this gap: {maybe_text(gap)}?"


def expected_output_kinds_for_role(role: str) -> list[str]:
    if role == "sociologist":
        return ["source-selection", "claim-curation", "claim-submission", "data-readiness-report", "expert-report"]
    if role == "environmentalist":
        return ["source-selection", "observation-curation", "observation-submission", "data-readiness-report", "expert-report"]
    if role == "historian":
        return ["expert-report"]
    return ["expert-report"]


def infer_missing_evidence_types(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
) -> list[str]:
    observation_metrics = {maybe_text(item.get("metric")) for item in observations}
    has_station_observation = any(maybe_text(item.get("source_skill")) == "openaq-data-fetch" for item in observations)
    cards_by_claim_id = {maybe_text(item.get("claim_id")): item for item in evidence_cards}
    unresolved_claims: list[dict[str, Any]] = []
    for claim in claims:
        claim_id = maybe_text(claim.get("claim_id"))
        card = cards_by_claim_id.get(claim_id)
        if card is None or maybe_text(card.get("verdict")) in {"mixed", "insufficient"}:
            unresolved_claims.append(claim)

    missing: set[str] = set()
    if not claims:
        missing.add("normalized-public-claims")
    if claims and observations and evidence_cards:
        if any(
            maybe_text(card.get("verdict")) in {"mixed", "insufficient"}
            for card in evidence_cards
            if isinstance(card, dict)
        ):
            missing.add("evidence-cards-linking-public-claims-to-physical-observations")

    for card in evidence_cards:
        gaps = card.get("gaps")
        if not isinstance(gaps, list):
            continue
        gap_text = " ".join(maybe_text(item) for item in gaps).lower()
        if "station" in gap_text or "modeled background" in gap_text:
            missing.add("station-air-quality")

    for claim in unresolved_claims:
        claim_id = maybe_text(claim.get("claim_id"))
        claim_type = maybe_text(claim.get("claim_type"))
        card = cards_by_claim_id.get(claim_id)
        gap_text = " ".join(card.get("gaps", [])) if isinstance(card, dict) and isinstance(card.get("gaps"), list) else ""
        lowered_gap_text = gap_text.lower()

        if "station" in lowered_gap_text or "modeled background" in lowered_gap_text:
            missing.add("station-air-quality")

        if claim_type in {"smoke", "air-pollution"} and not has_station_observation:
            missing.add("station-air-quality")

        if claim_type in {"smoke", "wildfire"} and "fire_detection_count" not in observation_metrics:
            if "wildfire" in maybe_text(claim.get("summary")).lower() or claim_type == "wildfire":
                missing.add("fire-detection")

        if claim_type == "wildfire" and not (observation_metrics & METEOROLOGY_METRICS):
            missing.add("meteorology-background")

        if claim_type == "flood" and not (observation_metrics & (PRECIPITATION_METRICS | HYDROLOGY_METRICS)):
            missing.add("precipitation-hydrology")

        if claim_type == "heat" and "temperature_2m" not in observation_metrics:
            missing.add("temperature-extremes")

        if claim_type == "drought" and not {"precipitation_sum", "soil_moisture_0_to_7cm"} <= observation_metrics:
            missing.add("precipitation-soil-moisture")

        if claim_type == "policy-reaction":
            refs = claim.get("public_refs")
            has_reggov = False
            if isinstance(refs, list):
                has_reggov = any(
                    isinstance(ref, dict)
                    and maybe_text(ref.get("source_skill")) in {"regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"}
                    for ref in refs
                )
            if not has_reggov:
                missing.add("policy-comment-coverage")

    if unresolved_claims and len(public_source_channels(claims)) < 2:
        if any(maybe_text(claim.get("claim_type")) != "policy-reaction" for claim in unresolved_claims):
            missing.add("public-discussion-coverage")

    return sorted(missing)


def observations_by_id_map(observations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {maybe_text(item.get("observation_id")): item for item in observations}


def claims_by_id_map(claims: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {maybe_text(item.get("claim_id")): item for item in claims}


def evidence_by_claim_map(evidence_cards: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {maybe_text(item.get("claim_id")): item for item in evidence_cards}


def metrics_for_evidence(card: dict[str, Any], observations_by_id: dict[str, dict[str, Any]]) -> list[str]:
    metrics: list[str] = []
    observation_ids = card.get("observation_ids")
    if not isinstance(observation_ids, list):
        return metrics
    for observation_id in observation_ids:
        observation = observations_by_id.get(maybe_text(observation_id))
        if observation is not None:
            metrics.append(maybe_text(observation.get("metric")))
    return unique_strings(metrics)


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
    verdict_counts = counter_dict([maybe_text(item.get("verdict")) for item in evidence_cards])
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
    metric_counts = counter_dict([maybe_text(item.get("metric")) for item in observations])
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


def observation_metrics_from_submissions(submissions: list[dict[str, Any]]) -> set[str]:
    return {maybe_text(item.get("metric")) for item in submissions if maybe_text(item.get("metric"))}


def environment_role_required(state: dict[str, Any]) -> bool:
    claims = state.get("claims", []) if isinstance(state.get("claims"), list) else []
    if any(bool(item.get("needs_physical_validation")) for item in claims if isinstance(item, dict)):
        return True
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    return any(maybe_text(task.get("assigned_role")) == "environmentalist" for task in tasks if isinstance(task, dict))


def readiness_missing_types(state: dict[str, Any], role: str) -> list[str]:
    submissions = state_submissions(state, role)
    claims = state.get("claims", []) if isinstance(state.get("claims"), list) else []
    missing: set[str] = set()
    if role == "sociologist":
        if not submissions:
            missing.add("normalized-public-claims")
            return sorted(missing)
        if len(claim_submission_channels(submissions)) < 2:
            if any(maybe_text(item.get("claim_type")) != "policy-reaction" for item in submissions):
                missing.add("public-discussion-coverage")
        has_policy_claim = any(maybe_text(item.get("claim_type")) == "policy-reaction" for item in submissions)
        has_reggov = False
        for submission in submissions:
            refs = submission.get("public_refs")
            if not isinstance(refs, list):
                continue
            if any(
                isinstance(ref, dict)
                and maybe_text(ref.get("source_skill")) in {"regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"}
                for ref in refs
            ):
                has_reggov = True
                break
        if has_policy_claim and not has_reggov:
            missing.add("policy-comment-coverage")
        return sorted(missing)

    if not environment_role_required(state):
        return []
    if not submissions:
        return []
    metrics = observation_metrics_from_submissions(submissions)
    has_station_observation = any(
        maybe_text(item.get("source_skill")) in {"openaq-data-fetch", "airnow-hourly-obs-fetch"}
        or "airnow" in maybe_text(item.get("source_skill"))
        for item in submissions
    )
    claim_types = {maybe_text(item.get("claim_type")) for item in claims if maybe_text(item.get("claim_type"))}
    if claim_types & {"smoke", "air-pollution"} and not has_station_observation:
        missing.add("station-air-quality")
    if "wildfire" in claim_types and "fire_detection_count" not in metrics:
        missing.add("fire-detection")
    if "wildfire" in claim_types and not (metrics & METEOROLOGY_METRICS):
        missing.add("meteorology-background")
    if "flood" in claim_types and not (metrics & (PRECIPITATION_METRICS | HYDROLOGY_METRICS)):
        missing.add("precipitation-hydrology")
    if "heat" in claim_types and "temperature_2m" not in metrics:
        missing.add("temperature-extremes")
    if "drought" in claim_types and not {"precipitation_sum", "soil_moisture_0_to_7cm"} <= metrics:
        missing.add("precipitation-soil-moisture")
    return sorted(missing)


def generic_readiness_recommendations(role: str, missing_types: list[str], *, has_submissions: bool) -> list[dict[str, Any]]:
    recommendations = base_recommendations_from_missing_types(missing_types)
    if not has_submissions:
        if role == "sociologist":
            recommendations.append(
                {
                    "assigned_role": "sociologist",
                    "objective": "Collect and normalize mission-window public claims from approved channels.",
                    "reason": "No auditable claim submissions are available for readiness review yet.",
                }
            )
        else:
            recommendations.append(
                {
                    "assigned_role": "environmentalist",
                    "objective": "Collect and normalize mission-window physical observations from approved sources.",
                    "reason": "No auditable observation submissions are available for readiness review yet.",
                }
            )
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for recommendation in recommendations:
        key = recommendation_key(recommendation)
        if not key[0] or not key[1]:
            continue
        deduped.setdefault(key, recommendation)
    return list(deduped.values())


def build_readiness_findings_from_submissions(
    *,
    state: dict[str, Any],
    role: str,
    submissions: list[dict[str, Any]],
    max_findings: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    selected_submissions = representative_submissions(state, role, submissions, max_findings)
    for index, submission in enumerate(selected_submissions, start=1):
        if role == "sociologist":
            claim_id = maybe_text(submission.get("claim_id"))
            findings.append(
                {
                    "finding_id": f"finding-{index:03d}",
                    "title": truncate_text(maybe_text(submission.get("summary")) or f"Claim {claim_id}", 72),
                    "summary": truncate_text(
                        (
                            f"{maybe_text(submission.get('meaning'))} "
                            f"Source-signal count: {to_nonnegative_int(submission.get('source_signal_count'))}."
                        ),
                        300,
                    ),
                    "confidence": "medium" if to_nonnegative_int(submission.get("source_signal_count")) >= 2 else "low",
                    "claim_ids": [claim_id] if claim_id else [],
                    "observation_ids": [],
                    "evidence_ids": [],
                }
            )
        else:
            observation_id = maybe_text(submission.get("observation_id"))
            statistics_obj = compact_statistics(submission.get("statistics"))
            stats_text = ""
            if isinstance(statistics_obj, dict):
                stats_parts = [
                    f"{key}={value:g}"
                    for key in ("max", "p95", "mean", "min")
                    for value in [maybe_number(statistics_obj.get(key))]
                    if value is not None
                ]
                if stats_parts:
                    stats_text = f" Summary stats: {', '.join(stats_parts[:4])}."
            findings.append(
                {
                    "finding_id": f"finding-{index:03d}",
                    "title": truncate_text(
                        f"{maybe_text(submission.get('metric'))} from {maybe_text(submission.get('source_skill'))}",
                        72,
                    ),
                    "summary": truncate_text(
                        f"{maybe_text(submission.get('meaning'))} "
                        f"Metric family: {observation_metric_family(submission.get('metric'))}.{stats_text}",
                        300,
                    ),
                    "confidence": "medium",
                    "claim_ids": [],
                    "observation_ids": [observation_id] if observation_id else [],
                    "evidence_ids": [],
                }
            )
    return findings


def build_data_readiness_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    role: str,
    state: dict[str, Any],
    max_findings: int,
) -> dict[str, Any]:
    submissions = state_submissions(state, role)
    current_submissions = state_current_submissions(state, role)
    fallback_summary = (
        "Auditable public submissions are available for readiness review."
        if role == "sociologist"
        else "Auditable physical submissions are available for readiness review."
    )
    compact_audit = aggregate_compact_audit(
        state,
        role,
        submissions,
        fallback_summary=fallback_summary,
        retained_limit=max_findings,
    )
    missing_types = readiness_missing_types(state, role)
    has_submissions = bool(submissions)
    environment_required = not (role == "environmentalist" and not environment_role_required(state))
    readiness_status = "ready"
    if not environment_required:
        compact_audit["representative"] = True
        compact_audit["coverage_summary"] = "Current claims do not require physical-side validation in this round."
    elif not has_submissions:
        readiness_status = "blocked"
    elif missing_types or not compact_audit.get("representative"):
        readiness_status = "needs-more-data"
    sufficient_for_matching = readiness_status == "ready"
    if not sufficient_for_matching:
        compact_audit["representative"] = False
    findings = build_readiness_findings_from_submissions(
        state=state,
        role=role,
        submissions=submissions,
        max_findings=max_findings,
    )
    open_questions = [
        f"Should the next round address compact-audit concentration: {flag}?"
        for flag in compact_audit.get("concentration_flags", [])
        if maybe_text(flag)
    ]
    open_questions.extend(
        f"Is the compact view missing required coverage dimension `{dimension}` for matching?"
        for dimension in compact_audit.get("missing_dimensions", [])
        if maybe_text(dimension)
    )
    if environment_required and not has_submissions:
        open_questions.append(
            "Which approved source families should be expanded first to produce auditable canonical submissions for readiness review?"
        )
    recommendations = [] if not environment_required else generic_readiness_recommendations(
        role,
        missing_types,
        has_submissions=has_submissions,
    )
    if missing_types:
        open_questions.extend(
            f"Should the next round address missing evidence type `{missing_type}` before matching?"
            for missing_type in missing_types
        )
    if role == "sociologist":
        summary_lead = (
            f"Public-side readiness reviewed {len(submissions)} auditable claim submissions "
            f"({len(current_submissions)} newly materialized this round)."
        )
    else:
        summary_lead = (
            f"Physical-side readiness reviewed {len(submissions)} auditable observation submissions "
            f"({len(current_submissions)} newly materialized this round)."
        )
    summary = f"{summary_lead} Status={readiness_status}. {compact_audit.get('coverage_summary')}"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "readiness_id": f"readiness-{role}-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": role,
        "readiness_status": readiness_status,
        "sufficient_for_matching": sufficient_for_matching,
        "summary": truncate_text(summary, 400),
        "findings": findings,
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations[:4],
        "override_requests": [],
        "referenced_submission_ids": [
            maybe_text(item.get("submission_id"))
            for item in submissions
            if maybe_text(item.get("submission_id"))
        ],
        "compact_audit": compact_audit,
    }
    validate_payload("data-readiness-report", payload)
    return payload


def investigation_leg_metric_families(profile_id: str, leg_id: str, leg: dict[str, Any] | None = None) -> set[str]:
    if isinstance(leg, dict):
        explicit = {
            maybe_text(item)
            for item in (leg.get("metric_families") if isinstance(leg.get("metric_families"), list) else [])
            if maybe_text(item)
        }
        if explicit:
            return explicit
    families = INVESTIGATION_LEG_METRIC_FAMILIES.get(profile_id, {})
    return set(families.get(leg_id, set()))


def summarize_investigation_leg_status(*, leg_label: str, status: str, matched_count: int, pending_count: int) -> str:
    label = maybe_text(leg_label) or "This leg"
    if status == "supported":
        return f"{label} is covered by {matched_count} matched evidence card(s)."
    if status == "partial":
        if pending_count > 0:
            return f"{label} is only partially covered; {pending_count} isolated/remand item(s) still need follow-up."
        return f"{label} has auditable signals but is not yet strongly resolved."
    if status == "contradicted":
        return f"{label} is currently contradicted by the available evidence."
    if status == "isolated":
        return f"{label} is represented only by isolated evidence and has not been integrated into a stronger chain."
    return f"{label} is not yet covered by the current auditable evidence."


def investigation_review_overall_status(hypothesis_reviews: list[dict[str, Any]], *, blocked: bool = False) -> str:
    if blocked:
        return "blocked"
    statuses = [maybe_text(item.get("overall_status")) for item in hypothesis_reviews if isinstance(item, dict)]
    if statuses and all(status == "supported" for status in statuses):
        return "supported"
    if any(status in {"supported", "partial"} for status in statuses):
        return "partial"
    return "unresolved"


def record_matches_hypothesis_leg(record: dict[str, Any], *, hypothesis_id: str, leg_id: str) -> bool:
    record_hypothesis_id = maybe_text(record.get("hypothesis_id"))
    if hypothesis_id and record_hypothesis_id and record_hypothesis_id != hypothesis_id:
        return False
    record_leg_id = maybe_text(record.get("leg_id"))
    if leg_id and record_leg_id and record_leg_id != leg_id:
        return False
    return True


def record_has_investigation_tags(record: dict[str, Any]) -> bool:
    return bool(maybe_text(record.get("hypothesis_id")) or maybe_text(record.get("leg_id")))


def observation_supports_investigation_leg(
    observation: dict[str, Any],
    *,
    hypothesis_id: str,
    leg_id: str,
    relevant_families: set[str],
) -> bool:
    if record_has_investigation_tags(observation):
        return record_matches_hypothesis_leg(observation, hypothesis_id=hypothesis_id, leg_id=leg_id)

    component_roles = observation.get("component_roles")
    explicit_component_seen = False
    if isinstance(component_roles, list):
        for component in component_roles:
            if not isinstance(component, dict):
                continue
            component_record = {
                "hypothesis_id": maybe_text(component.get("hypothesis_id")),
                "leg_id": maybe_text(component.get("leg_id")),
            }
            if not record_has_investigation_tags(component_record):
                continue
            explicit_component_seen = True
            if record_matches_hypothesis_leg(component_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                return True
        if explicit_component_seen:
            return False
        for component in component_roles:
            if not isinstance(component, dict):
                continue
            component_metric = maybe_text(component.get("metric")) or maybe_text(observation.get("metric"))
            if observation_metric_family(component_metric) in relevant_families:
                return True
    return observation_metric_family(observation.get("metric")) in relevant_families


def build_leg_review_from_state(
    *,
    state: dict[str, Any],
    profile_id: str,
    leg: dict[str, Any],
    hypothesis_id: str = "",
) -> dict[str, Any]:
    leg_id = maybe_text(leg.get("leg_id"))
    leg_label = maybe_text(leg.get("label")) or leg_id.replace("_", " ")
    region_hint = maybe_text(leg.get("region_hint"))
    success_criteria = maybe_text(leg.get("success_criteria"))
    evidence_focus = [maybe_text(item) for item in leg.get("evidence_focus", []) if maybe_text(item)] if isinstance(leg.get("evidence_focus"), list) else []
    relevant_claim_types = {
        maybe_text(item)
        for item in (leg.get("claim_types") if isinstance(leg.get("claim_types"), list) else [])
        if maybe_text(item)
    }
    cards = state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else []
    isolated_entries = state.get("isolated_active", []) if isinstance(state.get("isolated_active"), list) else []
    remand_entries = state.get("remands_open", []) if isinstance(state.get("remands_open"), list) else []
    observations = state.get("observations", []) if isinstance(state.get("observations"), list) else []
    claim_lookup = {
        maybe_text(item.get("claim_id")): item
        for item in state.get("claims", [])
        if isinstance(item, dict) and maybe_text(item.get("claim_id"))
    }
    for submission in state_auditable_submissions(state, "sociologist"):
        claim_id = maybe_text(submission.get("claim_id"))
        if claim_id:
            claim_lookup.setdefault(claim_id, submission)
    observation_lookup = {
        maybe_text(item.get("observation_id")): item
        for item in observations
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    for submission in state_auditable_submissions(state, "environmentalist"):
        observation_id = maybe_text(submission.get("observation_id"))
        if observation_id:
            observation_lookup.setdefault(observation_id, submission)

    matched_refs: list[str] = []
    isolated_refs: list[str] = []
    remand_refs: list[str] = []
    direct_refs: list[str] = []

    if leg_id == "public_interpretation":
        for card in cards:
            if not isinstance(card, dict):
                continue
            claim_id = maybe_text(card.get("claim_id"))
            claim_record = claim_lookup.get(claim_id, {})
            if not record_matches_hypothesis_leg(claim_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(claim_record.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if claim_id and maybe_text(card.get("evidence_id")):
                matched_refs.append(f"card:{maybe_text(card.get('evidence_id'))}")
        for entry in isolated_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "claim":
                continue
            claim_record = claim_lookup.get(maybe_text(entry.get("entity_id")), {})
            if not record_matches_hypothesis_leg(claim_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(claim_record.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if maybe_text(entry.get("isolated_id")):
                isolated_refs.append(f"isolated:{maybe_text(entry.get('isolated_id'))}")
        for entry in remand_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "claim":
                continue
            claim_record = claim_lookup.get(maybe_text(entry.get("entity_id")), {})
            if not record_matches_hypothesis_leg(claim_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(claim_record.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if maybe_text(entry.get("remand_id")):
                remand_refs.append(f"remand:{maybe_text(entry.get('remand_id'))}")
        for submission in state_auditable_submissions(state, "sociologist"):
            claim_id = maybe_text(submission.get("claim_id"))
            if not record_matches_hypothesis_leg(submission, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(submission.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if claim_id:
                direct_refs.append(f"claim:{claim_id}")
    else:
        relevant_families = investigation_leg_metric_families(profile_id, leg_id, leg)
        for card in cards:
            if not isinstance(card, dict):
                continue
            if not record_matches_hypothesis_leg(card, hypothesis_id=hypothesis_id, leg_id=""):
                continue
            observation_ids = [
                maybe_text(item)
                for item in card.get("observation_ids", [])
                if maybe_text(item)
            ]
            if any(
                observation_supports_investigation_leg(
                    observation_lookup.get(observation_id, {}),
                    hypothesis_id=hypothesis_id,
                    leg_id=leg_id,
                    relevant_families=relevant_families,
                )
                for observation_id in observation_ids
            ):
                evidence_id = maybe_text(card.get("evidence_id"))
                if evidence_id:
                    matched_refs.append(f"card:{evidence_id}")
        for entry in isolated_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "observation":
                continue
            observation = observation_lookup.get(maybe_text(entry.get("entity_id")))
            if observation is None:
                continue
            if observation_supports_investigation_leg(
                observation,
                hypothesis_id=hypothesis_id,
                leg_id=leg_id,
                relevant_families=relevant_families,
            ) and maybe_text(entry.get("isolated_id")):
                isolated_refs.append(f"isolated:{maybe_text(entry.get('isolated_id'))}")
        for entry in remand_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "observation":
                continue
            observation = observation_lookup.get(maybe_text(entry.get("entity_id")))
            if observation is None:
                continue
            if observation_supports_investigation_leg(
                observation,
                hypothesis_id=hypothesis_id,
                leg_id=leg_id,
                relevant_families=relevant_families,
            ) and maybe_text(entry.get("remand_id")):
                remand_refs.append(f"remand:{maybe_text(entry.get('remand_id'))}")
        for submission in state_auditable_submissions(state, "environmentalist"):
            observation_id = maybe_text(submission.get("observation_id"))
            if not observation_id:
                continue
            if observation_supports_investigation_leg(
                submission,
                hypothesis_id=hypothesis_id,
                leg_id=leg_id,
                relevant_families=relevant_families,
            ):
                direct_refs.append(f"observation:{observation_id}")

    matched_count = len(matched_refs)
    pending_count = len(isolated_refs) + len(remand_refs)
    if matched_count > 0:
        status = "supported"
    elif pending_count > 0 or direct_refs:
        status = "partial"
    else:
        status = "unresolved"
    notes: list[str] = []
    if status == "unresolved" and bool(leg.get("required")):
        notes.append(f"The required leg {leg_id} still lacks auditable coverage in this round.")
    if status != "supported" and success_criteria:
        notes.append(f"Success criterion: {success_criteria}")
    if status == "unresolved" and evidence_focus:
        notes.append(f"Evidence focus: {evidence_focus[0]}")
    if status in {"partial", "unresolved"} and region_hint:
        notes.append(f"Region hint: {region_hint}")
    evidence_refs = unique_strings(matched_refs + isolated_refs + remand_refs + direct_refs)[:8]
    return {
        "leg_id": leg_id,
        "status": status,
        "summary": summarize_investigation_leg_status(
            leg_label=leg_label,
            status=status,
            matched_count=matched_count,
            pending_count=pending_count,
        ),
        "evidence_refs": evidence_refs,
        "notes": unique_strings(notes),
    }


def build_hypothesis_review_from_state(*, state: dict[str, Any], hypothesis: dict[str, Any], profile_id: str) -> dict[str, Any]:
    leg_reviews = [
        build_leg_review_from_state(
            state=state,
            profile_id=profile_id,
            leg=leg,
            hypothesis_id=maybe_text(hypothesis.get("hypothesis_id")),
        )
        for leg in hypothesis.get("chain_legs", [])
        if isinstance(leg, dict) and maybe_text(leg.get("leg_id"))
    ]
    required_leg_ids = {
        maybe_text(leg.get("leg_id"))
        for leg in hypothesis.get("chain_legs", [])
        if isinstance(leg, dict) and bool(leg.get("required")) and maybe_text(leg.get("leg_id"))
    }
    statuses_by_leg = {
        maybe_text(item.get("leg_id")): maybe_text(item.get("status"))
        for item in leg_reviews
        if isinstance(item, dict) and maybe_text(item.get("leg_id"))
    }
    if required_leg_ids and all(statuses_by_leg.get(leg_id) == "supported" for leg_id in required_leg_ids):
        overall_status = "supported"
    elif any(statuses_by_leg.get(leg_id) in {"supported", "partial"} for leg_id in required_leg_ids):
        overall_status = "partial"
    else:
        overall_status = "unresolved"

    matched_card_ids: list[str] = []
    isolated_entry_ids: list[str] = []
    remand_ids: list[str] = []
    for leg_review in leg_reviews:
        for ref in leg_review.get("evidence_refs", []):
            text = maybe_text(ref)
            if text.startswith("card:"):
                matched_card_ids.append(text.split(":", 1)[1])
            elif text.startswith("isolated:"):
                isolated_entry_ids.append(text.split(":", 1)[1])
            elif text.startswith("remand:"):
                remand_ids.append(text.split(":", 1)[1])

    unresolved_required = [
        leg_id
        for leg_id in required_leg_ids
        if statuses_by_leg.get(leg_id) not in {"supported"}
    ]
    notes: list[str] = []
    if unresolved_required:
        notes.append("Required legs still unresolved or partial: " + ", ".join(sorted(unresolved_required)) + ".")
    return {
        "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")) or "hypothesis-001",
        "statement": maybe_text(hypothesis.get("statement")) or maybe_text(hypothesis.get("summary")) or "Mission hypothesis",
        "overall_status": overall_status,
        "matched_card_ids": unique_strings(matched_card_ids),
        "isolated_entry_ids": unique_strings(isolated_entry_ids),
        "remand_ids": unique_strings(remand_ids),
        "leg_reviews": leg_reviews,
        "notes": unique_strings(notes),
    }


def build_investigation_review_draft_from_state(state: dict[str, Any]) -> dict[str, Any]:
    mission = state["mission"]
    round_id = state["round_id"]
    if not matching_executed_for_state(state):
        raise ValueError("Investigation review requires matching/adjudication artifacts to exist first.")
    matching_result = state.get("matching_result", {}) if isinstance(state.get("matching_result"), dict) else {}
    evidence_adjudication = (
        state.get("evidence_adjudication", {})
        if isinstance(state.get("evidence_adjudication"), dict)
        else {}
    )
    matching_adjudication = (
        state.get("matching_adjudication", {})
        if isinstance(state.get("matching_adjudication"), dict)
        else {}
    )
    plan = state.get("investigation_plan") if isinstance(state.get("investigation_plan"), dict) else {}
    if not isinstance(plan, dict) or not plan:
        plan = {"profile_id": "local-event", "hypotheses": []}
    profile_id = maybe_text(plan.get("profile_id")) or "local-event"
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    if not hypotheses:
        hypotheses = [
            {
                "hypothesis_id": "hypothesis-001",
                "statement": maybe_text(mission.get("objective")) or maybe_text(mission.get("topic")) or "Mission hypothesis",
                "chain_legs": [],
            }
        ]
    hypothesis_reviews = [
        build_hypothesis_review_from_state(state=state, hypothesis=hypothesis, profile_id=profile_id)
        for hypothesis in hypotheses
        if isinstance(hypothesis, dict)
    ]
    review_status = investigation_review_overall_status(hypothesis_reviews)
    matched_card_ids = unique_strings(
        [
            maybe_text(item.get("evidence_id"))
            for item in state.get("cards_active", [])
            if isinstance(item, dict) and maybe_text(item.get("evidence_id"))
        ]
    )
    isolated_entry_ids = unique_strings(
        [
            maybe_text(item.get("isolated_id"))
            for item in state.get("isolated_active", [])
            if isinstance(item, dict) and maybe_text(item.get("isolated_id"))
        ]
    )
    remand_ids = unique_strings(
        [
            maybe_text(item.get("remand_id"))
            for item in state.get("remands_open", [])
            if isinstance(item, dict) and maybe_text(item.get("remand_id"))
        ]
    )
    missing_types = infer_missing_evidence_types(
        claims=state.get("claims", []) if isinstance(state.get("claims"), list) else [],
        observations=state.get("observations", []) if isinstance(state.get("observations"), list) else [],
        evidence_cards=state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else [],
    )
    recommendation_inputs = [
        item
        for item in [matching_adjudication, evidence_adjudication]
        if isinstance(item, dict) and item
    ]
    recommendations = combine_recommendations(reports=recommendation_inputs, missing_types=missing_types)[:4]
    open_questions: list[str] = []
    open_questions.extend(
        maybe_text(item)
        for item in evidence_adjudication.get("open_questions", [])
        if maybe_text(item)
    )
    open_questions.extend(
        maybe_text(item)
        for item in matching_adjudication.get("open_questions", [])
        if maybe_text(item)
    )
    for hypothesis_review in hypothesis_reviews:
        if not isinstance(hypothesis_review, dict):
            continue
        unresolved_legs = [
            maybe_text(item.get("leg_id"))
            for item in hypothesis_review.get("leg_reviews", [])
            if isinstance(item, dict) and maybe_text(item.get("status")) != "supported" and maybe_text(item.get("leg_id"))
        ]
        if unresolved_legs:
            open_questions.append(
                f"Does {maybe_text(hypothesis_review.get('hypothesis_id'))} need another round for these legs: {', '.join(unresolved_legs)}?"
            )
    open_questions.extend(
        f"How should the council resolve remand {maybe_text(item.get('remand_id'))}?"
        for item in state.get("remands_open", [])
        if isinstance(item, dict) and maybe_text(item.get("remand_id"))
    )
    matching_reasonable = bool(evidence_adjudication.get("matching_reasonable"))
    needs_additional_data = bool(evidence_adjudication.get("needs_additional_data")) or bool(remand_ids) or review_status != "supported"
    summary = (
        f"Investigation review is {review_status}. "
        f"Matched cards={len(matched_card_ids)}, isolated entries={len(isolated_entry_ids)}, remands={len(remand_ids)}. "
        f"{maybe_text(evidence_adjudication.get('summary')) or maybe_text(matching_result.get('summary'))}"
    ).strip()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "review_id": f"investigation-review-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "moderator",
        "authorization_id": maybe_text(matching_result.get("authorization_id"))
        or maybe_text(matching_adjudication.get("authorization_id"))
        or maybe_text(state.get("matching_authorization", {}).get("authorization_id")),
        "matching_result_id": maybe_text(matching_result.get("result_id")) or f"matching-result-{round_id}",
        "review_status": review_status,
        "matching_reasonable": matching_reasonable,
        "needs_additional_data": needs_additional_data,
        "summary": truncate_text(summary, 400),
        "hypothesis_reviews": hypothesis_reviews,
        "matched_card_ids": matched_card_ids,
        "isolated_entry_ids": isolated_entry_ids,
        "remand_ids": remand_ids,
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations,
    }
    validate_payload("investigation-review", payload)
    return payload


def build_pre_match_report_findings(state: dict[str, Any], role: str, max_findings: int) -> list[dict[str, Any]]:
    readiness = state.get("readiness_reports", {}).get(role)
    if isinstance(readiness, dict):
        findings = readiness.get("findings")
        if isinstance(findings, list) and findings:
            return [item for item in findings if isinstance(item, dict)][:max_findings]
    return build_readiness_findings_from_submissions(
        state=state,
        role=role,
        submissions=state_submissions(state, role),
        max_findings=max_findings,
    )


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
        recommendations = combine_recommendations(
            reports=[item for item in additional_reports if isinstance(item, dict)],
            missing_types=[],
        )[:4]
    else:
        findings = build_pre_match_report_findings(state, role, max_findings)
        readiness_report = state.get("readiness_reports", {}).get(role)
        open_questions = []
        recommendations = []
        if isinstance(readiness_report, dict):
            open_questions.extend(
                maybe_text(item)
                for item in readiness_report.get("open_questions", [])
                if maybe_text(item)
            )
            recommendations.extend(
                item
                for item in readiness_report.get("recommended_next_actions", [])
                if isinstance(item, dict)
            )
        if not recommendations:
            recommendations = generic_readiness_recommendations(
                role,
                readiness_missing_types(state, role),
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
        if moderator_status == "continue":
            return (
                f"Matching/adjudication produced {cards} cards, {isolated} isolated entries, and {remands} remands. "
                f"Another round is required before closure. Report sources used: "
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
    recommendations = combine_recommendations(reports=recommendation_inputs, missing_types=missing_types)
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
    elif state.get("remands_open") or missing_types:
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
            blocked_reason = "The round still has unresolved evidence issues, but no materially different next-round task could be derived."
    else:
        moderator_status = "complete"
        next_round_required = False
        next_round_tasks = []
    if matching_executed_for_state(state):
        if state.get("remands_open"):
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
        "missing_evidence_types": missing_types,
        "next_round_tasks": next_round_tasks,
        "override_requests": override_requests,
        "final_brief": final_brief,
    }
    validate_payload("council-decision", payload)
    return payload, next_round_tasks, missing_types


__all__ = [
    "build_data_readiness_draft",
    "build_decision_draft_from_state",
    "build_decision_missing_types",
    "build_decision_summary_from_state",
    "build_environmentalist_findings",
    "build_expert_report_draft_from_state",
    "build_final_brief",
    "build_hypothesis_review_from_state",
    "build_investigation_review_draft_from_state",
    "build_leg_review_from_state",
    "build_open_questions",
    "build_pre_match_report_findings",
    "build_readiness_findings_from_submissions",
    "build_report_draft",
    "build_report_summary_from_state",
    "build_sociologist_findings",
    "build_summary_for_role",
    "claim_sort_key",
    "completion_score_for_round",
    "evidence_by_claim_map",
    "evidence_rank",
    "evidence_resolution_score",
    "evidence_sufficiency_for_round",
    "environment_role_required",
    "expert_report_status_from_state",
    "expected_output_kinds_for_role",
    "gap_to_question",
    "generic_readiness_recommendations",
    "infer_missing_evidence_types",
    "investigation_leg_metric_families",
    "investigation_review_overall_status",
    "metrics_for_evidence",
    "missing_types_from_reason_texts",
    "observation_metrics_from_submissions",
    "observation_supports_investigation_leg",
    "observations_by_id_map",
    "readiness_missing_types",
    "readiness_score",
    "record_has_investigation_tags",
    "record_matches_hypothesis_leg",
    "report_completion_score",
    "report_has_substance",
    "report_is_placeholder",
    "report_status_for_role",
    "summarize_investigation_leg_status",
]
