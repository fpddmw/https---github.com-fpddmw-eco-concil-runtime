"""Readiness-specific reporting services extracted from reporting_drafts."""

from __future__ import annotations

from typing import Any

from eco_council_runtime.application.reporting_state import (
    mission_run_id,
    state_current_submissions,
    state_submissions,
)
from eco_council_runtime.application.reporting_views import (
    aggregate_compact_audit,
    claim_submission_channels,
    compact_statistics,
    maybe_number,
    observation_metric_family,
    representative_submissions,
    to_nonnegative_int,
)
from eco_council_runtime.domain.contract_bridge import (
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.normalize_semantics import HYDROLOGY_METRICS, METEOROLOGY_METRICS, PRECIPITATION_METRICS
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings
from eco_council_runtime.planning import base_recommendations_from_missing_types, recommendation_key

SCHEMA_VERSION = resolve_schema_version("1.0.0")


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


__all__ = [
    "build_data_readiness_draft",
    "build_pre_match_report_findings",
    "build_readiness_findings_from_submissions",
    "environment_role_required",
    "generic_readiness_recommendations",
    "observation_metrics_from_submissions",
    "readiness_missing_types",
]
