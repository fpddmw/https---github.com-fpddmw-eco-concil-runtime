"""Shared helpers for extracted reporting services."""

from __future__ import annotations

from typing import Any

from eco_council_runtime.application.reporting_views import public_source_channels
from eco_council_runtime.domain.normalize_semantics import HYDROLOGY_METRICS, METEOROLOGY_METRICS, PRECIPITATION_METRICS
from eco_council_runtime.domain.text import maybe_text, unique_strings

READINESS_ROLES = ("sociologist", "environmentalist")
VERDICT_SCORES = {"supports": 1.0, "contradicts": 1.0, "mixed": 0.6, "insufficient": 0.25}
QUESTION_RULES = (
    ("station-grade corroboration is missing", "Can station-grade air-quality measurements be added for the same mission window?"),
    ("modeled background fields should be cross-checked", "Can modeled air-quality fields be cross-checked with station or local observations?"),
    ("no mission-aligned observations matched", "Should the next round expand physical coverage or narrow claim scope so observations can be matched?"),
)


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


__all__ = [
    "READINESS_ROLES",
    "VERDICT_SCORES",
    "claim_sort_key",
    "claims_by_id_map",
    "evidence_by_claim_map",
    "evidence_rank",
    "expected_output_kinds_for_role",
    "gap_to_question",
    "infer_missing_evidence_types",
    "metrics_for_evidence",
    "observations_by_id_map",
    "report_has_substance",
    "report_is_placeholder",
]
