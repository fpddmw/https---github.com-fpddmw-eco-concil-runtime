"""Compact reporting views and packet-support helpers."""

from __future__ import annotations

import copy
from collections import Counter
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists, utc_now_iso
from eco_council_runtime.application.reporting_state import (
    augment_context_with_matching_state,
    compact_investigation_actions_summary,
    compact_investigation_state_summary,
    mission_run_id,
    state_auditable_submissions,
    state_current_submissions,
)
from eco_council_runtime.controller.paths import (
    claim_candidates_path,
    claim_curation_path,
    claim_submissions_path,
    evidence_adjudication_path,
    investigation_actions_path,
    investigation_state_path,
    matching_authorization_path,
    matching_result_path,
    observation_candidates_path,
    observation_curation_path,
    observation_submissions_path,
    role_context_path,
)
from eco_council_runtime.domain.normalize_semantics import (
    DEFAULT_OBSERVATION_FAMILY_ORDER,
    HYDROLOGY_METRICS,
    METEOROLOGY_METRICS,
    METRIC_FAMILY_GROUPS,
    PRECIPITATION_METRICS,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings
from eco_council_runtime.investigation import causal_focus_for_role

PUBLIC_SOURCE_FAMILIES = {
    "gdelt-doc-search": "gdelt",
    "gdelt-events-fetch": "gdelt",
    "gdelt-mentions-fetch": "gdelt",
    "gdelt-gkg-fetch": "gdelt",
    "bluesky-cascade-fetch": "bluesky",
    "youtube-video-search": "youtube",
    "youtube-comments-fetch": "youtube",
    "federal-register-doc-fetch": "rulemaking",
    "regulationsgov-comments-fetch": "rulemaking",
    "regulationsgov-comment-detail-fetch": "rulemaking",
}


def counter_dict(values: list[str]) -> dict[str, int]:
    return dict(Counter(item for item in values if item))


def top_counter_items(counter: Counter[str], limit: int = 8) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
    return items


def sorted_counter_items(counter: Counter[str], limit: int = 8) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key in sorted(counter):
        count = counter.get(key, 0)
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
        if len(items) >= limit:
            break
    return items


def public_source_channel(source_skill: str) -> str:
    return PUBLIC_SOURCE_FAMILIES.get(maybe_text(source_skill), maybe_text(source_skill) or "unknown")


def public_source_channels(claims: list[dict[str, Any]]) -> list[str]:
    channels: list[str] = []
    for claim in claims:
        refs = claim.get("public_refs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if isinstance(ref, dict):
                channels.append(public_source_channel(maybe_text(ref.get("source_skill"))))
    return unique_strings(channels)


def to_nonnegative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def maybe_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = maybe_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def compact_count_items(value: Any, *, limit: int = 6) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("value"))
        count = to_nonnegative_int(item.get("count"))
        if not label or count <= 0:
            continue
        items.append({"value": label, "count": count})
        if len(items) >= limit:
            break
    return items


def compact_distribution_summary(value: Any, *, limit: int = 6) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compacted: dict[str, Any] = {}
    signal_count = to_nonnegative_int(value.get("signal_count"))
    if signal_count > 0:
        compacted["signal_count"] = signal_count
    for field_name in ("distinct_day_count", "distinct_source_skill_count", "distinct_point_count"):
        count = to_nonnegative_int(value.get(field_name))
        if count > 0:
            compacted[field_name] = count
    for field_name in ("time_bucket_counts", "source_skill_counts", "metric_counts", "point_bucket_counts"):
        items = compact_count_items(value.get(field_name), limit=limit)
        if items:
            compacted[field_name] = items
    return compacted or None


def compact_statistics(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compacted: dict[str, Any] = {}
    sample_count = to_nonnegative_int(value.get("sample_count"))
    if sample_count > 0:
        compacted["sample_count"] = sample_count
    for key in ("min", "p05", "p25", "mean", "median", "p75", "p95", "max", "stddev"):
        number = maybe_number(value.get(key))
        compacted[key] = round(number, 3) if number is not None else None
    if all(number is None for key, number in compacted.items() if key != "sample_count") and "sample_count" not in compacted:
        return None
    return compacted


def observation_metric_family(metric: Any) -> str:
    normalized = maybe_text(metric)
    for family, metrics in METRIC_FAMILY_GROUPS.items():
        if normalized in metrics:
            return family
    return "other"


def environment_family_priority_order(claims: list[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    claim_types = {
        maybe_text(item.get("claim_type"))
        for item in claims
        if isinstance(item, dict) and bool(item.get("needs_physical_validation"))
    }
    for claim_type in sorted(claim_types):
        if claim_type in {"smoke", "air-pollution"}:
            ordered.extend(["air-quality", "fire-detection", "meteorology"])
        elif claim_type == "wildfire":
            ordered.extend(["fire-detection", "meteorology", "air-quality"])
        elif claim_type == "flood":
            ordered.extend(["hydrology", "meteorology"])
        elif claim_type == "heat":
            ordered.extend(["meteorology"])
        elif claim_type == "drought":
            ordered.extend(["soil", "meteorology"])
    ordered.extend(DEFAULT_OBSERVATION_FAMILY_ORDER)
    return unique_strings(ordered)


def compact_claim_scope(scope: Any) -> dict[str, Any]:
    if not isinstance(scope, dict):
        return {}
    payload: dict[str, Any] = {
        "time_source": maybe_text(scope.get("time_source")),
        "place_source": maybe_text(scope.get("place_source")),
        "usable_for_matching": bool(scope.get("usable_for_matching")),
    }
    time_window = scope.get("time_window")
    if isinstance(time_window, dict):
        payload["time_window"] = {
            "start_utc": maybe_text(time_window.get("start_utc")),
            "end_utc": maybe_text(time_window.get("end_utc")),
        }
    place_scope = scope.get("place_scope")
    if isinstance(place_scope, dict):
        payload["place_scope"] = {
            "label": maybe_text(place_scope.get("label")),
            "geometry": copy.deepcopy(place_scope.get("geometry")) if isinstance(place_scope.get("geometry"), dict) else {},
        }
    notes = [
        maybe_text(item)
        for item in (scope.get("notes") if isinstance(scope.get("notes"), list) else [])
        if maybe_text(item)
    ]
    if notes:
        payload["notes"] = notes[:3]
    return payload


def compact_claim(claim: dict[str, Any]) -> dict[str, Any]:
    refs = claim.get("public_refs")
    source_skills = []
    if isinstance(refs, list):
        source_skills = unique_strings(
            [
                maybe_text(ref.get("source_skill"))
                for ref in refs
                if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
            ]
        )
    payload = {
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": truncate_text(maybe_text(claim.get("summary")), 180),
        "priority": claim.get("priority"),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "public_source_skills": source_skills,
        "candidate_claim_ids": [maybe_text(item) for item in claim.get("candidate_claim_ids", []) if maybe_text(item)][:6],
        "selection_reason": truncate_text(maybe_text(claim.get("selection_reason")), 160),
    }
    hypothesis_id = maybe_text(claim.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(claim.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    compact_scope = compact_claim_scope(claim.get("claim_scope"))
    if compact_scope:
        payload["claim_scope"] = compact_scope
    return payload


def compact_observation(observation: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "observation_id": maybe_text(observation.get("observation_id")),
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "metric_family": observation_metric_family(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "observation_mode": maybe_text(observation.get("observation_mode")),
        "evidence_role": maybe_text(observation.get("evidence_role")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": compact_statistics(observation.get("statistics")),
        "distribution_summary": compact_distribution_summary(observation.get("distribution_summary")),
        "time_window": observation.get("time_window"),
        "source_skills": [maybe_text(item) for item in observation.get("source_skills", []) if maybe_text(item)][:4],
        "metric_bundle": [maybe_text(item) for item in observation.get("metric_bundle", []) if maybe_text(item)][:6],
        "quality_flags": [maybe_text(item) for item in observation.get("quality_flags", []) if maybe_text(item)][:4],
    }
    hypothesis_id = maybe_text(observation.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(observation.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    return payload


def compact_evidence_card(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "evidence_id": maybe_text(card.get("evidence_id")),
        "claim_id": maybe_text(card.get("claim_id")),
        "verdict": maybe_text(card.get("verdict")),
        "confidence": maybe_text(card.get("confidence")),
        "summary": truncate_text(maybe_text(card.get("summary")), 220),
        "observation_ids": [maybe_text(item) for item in card.get("observation_ids", []) if maybe_text(item)][:6],
        "gaps": [truncate_text(maybe_text(item), 120) for item in card.get("gaps", []) if maybe_text(item)][:3],
    }


def compact_claim_submission(submission: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "submission_id": maybe_text(submission.get("submission_id")),
        "claim_id": maybe_text(submission.get("claim_id")),
        "claim_type": maybe_text(submission.get("claim_type")),
        "summary": truncate_text(maybe_text(submission.get("summary")), 180),
        "meaning": truncate_text(maybe_text(submission.get("meaning")), 200),
        "worth_storing": bool(submission.get("worth_storing")),
        "source_signal_count": submission.get("source_signal_count"),
        "candidate_claim_ids": [maybe_text(item) for item in submission.get("candidate_claim_ids", []) if maybe_text(item)][:6],
        "selection_reason": truncate_text(maybe_text(submission.get("selection_reason")), 160),
    }
    hypothesis_id = maybe_text(submission.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(submission.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    compact_scope = compact_claim_scope(submission.get("claim_scope"))
    if compact_scope:
        payload["claim_scope"] = compact_scope
    return payload


def compact_observation_submission(submission: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "submission_id": maybe_text(submission.get("submission_id")),
        "observation_id": maybe_text(submission.get("observation_id")),
        "metric": maybe_text(submission.get("metric")),
        "metric_family": observation_metric_family(submission.get("metric")),
        "source_skill": maybe_text(submission.get("source_skill")),
        "aggregation": maybe_text(submission.get("aggregation")),
        "observation_mode": maybe_text(submission.get("observation_mode")),
        "evidence_role": maybe_text(submission.get("evidence_role")),
        "value": submission.get("value"),
        "unit": maybe_text(submission.get("unit")),
        "statistics": compact_statistics(submission.get("statistics")),
        "distribution_summary": compact_distribution_summary(submission.get("distribution_summary")),
        "time_window": submission.get("time_window"),
        "meaning": truncate_text(maybe_text(submission.get("meaning")), 200),
        "worth_storing": bool(submission.get("worth_storing")),
        "source_skills": [maybe_text(item) for item in submission.get("source_skills", []) if maybe_text(item)][:4],
        "metric_bundle": [maybe_text(item) for item in submission.get("metric_bundle", []) if maybe_text(item)][:6],
        "candidate_observation_ids": [
            maybe_text(item) for item in submission.get("candidate_observation_ids", []) if maybe_text(item)
        ][:6],
        "selection_reason": truncate_text(maybe_text(submission.get("selection_reason")), 160),
    }
    hypothesis_id = maybe_text(submission.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(submission.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    return payload


def compact_isolated_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "isolated_id": maybe_text(item.get("isolated_id")),
        "entity_kind": maybe_text(item.get("entity_kind")),
        "entity_id": maybe_text(item.get("entity_id")),
        "summary": truncate_text(maybe_text(item.get("summary")), 200),
        "reason": truncate_text(maybe_text(item.get("reason")), 160),
    }


def compact_remand_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "remand_id": maybe_text(item.get("remand_id")),
        "entity_kind": maybe_text(item.get("entity_kind")),
        "entity_id": maybe_text(item.get("entity_id")),
        "summary": truncate_text(maybe_text(item.get("summary")), 200),
        "reasons": [truncate_text(maybe_text(reason), 120) for reason in item.get("reasons", []) if maybe_text(reason)][:3],
    }


def compact_claim_candidate_for_curation(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = compact_claim(candidate)
    payload["statement"] = truncate_text(maybe_text(candidate.get("statement")), 220)
    payload["source_signal_count"] = candidate.get("source_signal_count")
    if isinstance(candidate.get("compact_audit"), dict):
        payload["compact_audit"] = candidate.get("compact_audit")
    return payload


def compact_observation_candidate_for_curation(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = compact_observation(candidate)
    if isinstance(candidate.get("compact_audit"), dict):
        payload["compact_audit"] = candidate.get("compact_audit")
    return payload


def merge_count_items(counter: Counter[str], items: Any) -> None:
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("value"))
        count = to_nonnegative_int(item.get("count"))
        if not label or count <= 0:
            continue
        counter[label] += count


def claim_day_bucket(candidate: dict[str, Any]) -> str:
    window = candidate.get("time_window") if isinstance(candidate.get("time_window"), dict) else {}
    start_utc = maybe_text(window.get("start_utc"))
    if len(start_utc) >= 10:
        return start_utc[:10]
    end_utc = maybe_text(window.get("end_utc"))
    if len(end_utc) >= 10:
        return end_utc[:10]
    return ""


def point_bucket_from_scope(scope: Any) -> str:
    if not isinstance(scope, dict):
        return ""
    geometry = scope.get("geometry") if isinstance(scope.get("geometry"), dict) else {}
    if maybe_text(geometry.get("type")) == "Point":
        latitude = maybe_number(geometry.get("latitude"))
        longitude = maybe_number(geometry.get("longitude"))
        if latitude is not None and longitude is not None:
            return f"{latitude:.3f},{longitude:.3f}"
    return maybe_text(scope.get("label"))


def observation_candidate_signal_count(candidate: dict[str, Any]) -> int:
    distribution_summary = candidate.get("distribution_summary") if isinstance(candidate.get("distribution_summary"), dict) else {}
    signal_count = to_nonnegative_int(distribution_summary.get("signal_count"))
    if signal_count > 0:
        return signal_count
    statistics_obj = candidate.get("statistics") if isinstance(candidate.get("statistics"), dict) else {}
    signal_count = to_nonnegative_int(statistics_obj.get("sample_count"))
    if signal_count > 0:
        return signal_count
    compact_audit = candidate.get("compact_audit") if isinstance(candidate.get("compact_audit"), dict) else {}
    signal_count = to_nonnegative_int(compact_audit.get("total_candidate_count"))
    if signal_count > 0:
        return signal_count
    return 1


def build_claim_candidate_pool_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    claim_type_counts = Counter(
        maybe_text(item.get("claim_type"))
        for item in candidates
        if maybe_text(item.get("claim_type"))
    )
    source_skill_counts = Counter()
    channel_counts = Counter()
    day_counts = Counter()
    total_source_signal_count = 0
    needs_physical_validation_count = 0
    for candidate in candidates:
        total_source_signal_count += to_nonnegative_int(candidate.get("source_signal_count")) or 1
        if bool(candidate.get("needs_physical_validation")):
            needs_physical_validation_count += 1
        public_refs = candidate.get("public_refs") if isinstance(candidate.get("public_refs"), list) else []
        source_skills = unique_strings(
            maybe_text(ref.get("source_skill"))
            for ref in public_refs
            if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
        )
        if not source_skills:
            source_skills = unique_strings(candidate.get("public_source_skills", []))
        for source_skill in source_skills:
            source_skill_counts[source_skill] += 1
            channel_counts[public_source_channel(source_skill)] += 1
        day_bucket = claim_day_bucket(candidate)
        if day_bucket:
            day_counts[day_bucket] += 1
    return {
        "claim_type_counts": top_counter_items(claim_type_counts),
        "source_skill_counts": top_counter_items(source_skill_counts),
        "channel_counts": top_counter_items(channel_counts),
        "day_bucket_counts": sorted_counter_items(day_counts),
        "needs_physical_validation_count": needs_physical_validation_count,
        "total_source_signal_count": total_source_signal_count,
    }


def build_observation_candidate_pool_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    metric_candidate_counts = Counter()
    metric_family_candidate_counts = Counter()
    source_skill_candidate_counts = Counter()
    quality_flag_counts = Counter()
    time_bucket_signal_counts = Counter()
    source_skill_signal_counts = Counter()
    metric_signal_counts = Counter()
    point_bucket_signal_counts = Counter()
    total_signal_count = 0
    with_statistics_count = 0
    with_distribution_summary_count = 0
    multi_signal_candidate_count = 0
    for candidate in candidates:
        metric = maybe_text(candidate.get("metric"))
        if metric:
            metric_candidate_counts[metric] += 1
            metric_family_candidate_counts[observation_metric_family(metric)] += 1
        source_skill = maybe_text(candidate.get("source_skill"))
        if source_skill:
            source_skill_candidate_counts[source_skill] += 1
        for quality_flag in candidate.get("quality_flags", []):
            text = maybe_text(quality_flag)
            if text:
                quality_flag_counts[text] += 1
        signal_count = observation_candidate_signal_count(candidate)
        total_signal_count += signal_count
        if signal_count > 1:
            multi_signal_candidate_count += 1
        if isinstance(candidate.get("statistics"), dict):
            with_statistics_count += 1
        distribution_summary = candidate.get("distribution_summary") if isinstance(candidate.get("distribution_summary"), dict) else {}
        if distribution_summary:
            with_distribution_summary_count += 1
            merge_count_items(time_bucket_signal_counts, distribution_summary.get("time_bucket_counts"))
            merge_count_items(source_skill_signal_counts, distribution_summary.get("source_skill_counts"))
            merge_count_items(metric_signal_counts, distribution_summary.get("metric_counts"))
            merge_count_items(point_bucket_signal_counts, distribution_summary.get("point_bucket_counts"))
            continue
        if source_skill:
            source_skill_signal_counts[source_skill] += signal_count
        if metric:
            metric_signal_counts[metric] += signal_count
        day_bucket = claim_day_bucket(candidate)
        if day_bucket:
            time_bucket_signal_counts[day_bucket] += signal_count
        point_bucket = point_bucket_from_scope(candidate.get("place_scope"))
        if point_bucket:
            point_bucket_signal_counts[point_bucket] += signal_count
    return {
        "metric_candidate_counts": top_counter_items(metric_candidate_counts),
        "metric_family_candidate_counts": top_counter_items(metric_family_candidate_counts),
        "source_skill_candidate_counts": top_counter_items(source_skill_candidate_counts),
        "quality_flag_counts": top_counter_items(quality_flag_counts),
        "distribution_coverage": {
            "with_statistics_count": with_statistics_count,
            "with_distribution_summary_count": with_distribution_summary_count,
            "multi_signal_candidate_count": multi_signal_candidate_count,
            "total_signal_count": total_signal_count,
            "time_bucket_signal_counts": sorted_counter_items(time_bucket_signal_counts),
            "source_skill_signal_counts": top_counter_items(source_skill_signal_counts),
            "metric_signal_counts": top_counter_items(metric_signal_counts),
            "point_bucket_signal_counts": sorted_counter_items(point_bucket_signal_counts),
        },
    }


def ranked_claim_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        candidates,
        key=lambda item: (
            -to_nonnegative_int(item.get("source_signal_count")),
            maybe_text(item.get("claim_type")),
            maybe_text(item.get("claim_id")),
        ),
    )


def candidate_claim_entry_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "claim_id": maybe_text(candidate.get("claim_id")),
        "candidate_claim_ids": [maybe_text(candidate.get("claim_id"))],
        "claim_type": maybe_text(candidate.get("claim_type")),
        "summary": maybe_text(candidate.get("summary")),
        "statement": maybe_text(candidate.get("statement")),
        "meaning": (
            f"Retain this curated claim because it represents a distinct public narrative about "
            f"{maybe_text(candidate.get('claim_type')) or 'the mission topic'}."
        ),
        "priority": max(1, min(5, int(candidate.get("priority") or 1))),
        "needs_physical_validation": bool(candidate.get("needs_physical_validation")),
        "worth_storing": True,
        "selection_reason": "Carry forward this candidate as a curated claim pending agent review.",
        "time_window": candidate.get("time_window"),
        "place_scope": candidate.get("place_scope"),
    }
    hypothesis_id = maybe_text(candidate.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(candidate.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    compact_scope = compact_claim_scope(candidate.get("claim_scope"))
    if compact_scope:
        payload["claim_scope"] = compact_scope
    return payload


def guess_observation_candidate_evidence_role(observation: dict[str, Any], claims: list[dict[str, Any]]) -> str:
    metric = maybe_text(observation.get("metric"))
    claim_types = {
        maybe_text(item.get("claim_type"))
        for item in claims
        if isinstance(item, dict) and maybe_text(item.get("claim_type"))
    }
    if "wildfire" in claim_types:
        if metric == "fire_detection_count":
            return "primary"
        if metric in {"precipitation_sum", "relative_humidity_2m"}:
            return "contradictory"
        if observation_metric_family(metric) == "meteorology":
            return "contextual"
    if claim_types & {"smoke", "air-pollution"}:
        if observation_metric_family(metric) == "air-quality":
            return "primary"
        if metric == "fire_detection_count":
            return "contextual"
    if "flood" in claim_types and (metric in PRECIPITATION_METRICS or metric in HYDROLOGY_METRICS):
        return "primary"
    if "heat" in claim_types and metric == "temperature_2m":
        return "primary"
    if "drought" in claim_types and metric in {"precipitation_sum", "soil_moisture_0_to_7cm"}:
        return "primary"
    return "contextual"


def candidate_observation_entry_from_candidate(candidate: dict[str, Any], claims: list[dict[str, Any]]) -> dict[str, Any]:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else None
    payload: dict[str, Any] = {
        "observation_id": maybe_text(candidate.get("observation_id")),
        "observation_mode": "atomic",
        "candidate_observation_ids": [maybe_text(candidate.get("observation_id"))],
        "metric": maybe_text(candidate.get("metric")),
        "aggregation": maybe_text(candidate.get("aggregation")),
        "value": candidate.get("value"),
        "unit": maybe_text(candidate.get("unit")),
        "meaning": (
            f"Retain this atomic observation as a curation candidate for metric "
            f"{maybe_text(candidate.get('metric')) or 'unknown'}."
        ),
        "worth_storing": True,
        "evidence_role": guess_observation_candidate_evidence_role(candidate, claims),
        "selection_reason": "Carry forward this atomic observation pending agent review and possible composition.",
        "source_skills": [maybe_text(candidate.get("source_skill"))] if maybe_text(candidate.get("source_skill")) else [],
        "metric_bundle": [maybe_text(candidate.get("metric"))] if maybe_text(candidate.get("metric")) else [],
        "time_window": candidate.get("time_window"),
        "place_scope": candidate.get("place_scope"),
        "statistics": candidate.get("statistics"),
        "distribution_summary": candidate.get("distribution_summary"),
        "quality_flags": candidate.get("quality_flags", []),
        "component_roles": [],
    }
    hypothesis_id = maybe_text(candidate.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(candidate.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    if provenance is not None:
        payload["provenance_refs"] = [provenance]
    return payload


def claim_submission_source_skills(submission: dict[str, Any]) -> list[str]:
    refs = submission.get("public_refs")
    if not isinstance(refs, list):
        return []
    return unique_strings(
        [
            maybe_text(ref.get("source_skill"))
            for ref in refs
            if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
        ]
    )


def claim_submission_channels_for_submission(submission: dict[str, Any]) -> list[str]:
    return unique_strings([public_source_channel(source_skill) for source_skill in claim_submission_source_skills(submission)])


def claim_submission_channels(submissions: list[dict[str, Any]]) -> list[str]:
    channels: list[str] = []
    for submission in submissions:
        channels.extend(claim_submission_channels_for_submission(submission))
    return unique_strings(channels)


def select_public_submissions(submissions: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    ordered = sorted(
        submissions,
        key=lambda item: (
            -to_nonnegative_int(item.get("source_signal_count")),
            maybe_text(item.get("claim_type")),
            maybe_text(item.get("submission_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        if len(selected) >= limit:
            return
        for candidate in ordered:
            submission_id = maybe_text(candidate.get("submission_id"))
            if not submission_id or submission_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(submission_id)
            return

    for channel in claim_submission_channels(ordered):
        take_first(lambda item, channel=channel: channel in claim_submission_channels_for_submission(item))
    for source_skill in unique_strings([skill for item in ordered for skill in claim_submission_source_skills(item)]):
        take_first(lambda item, source_skill=source_skill: source_skill in claim_submission_source_skills(item))
    for claim_type in unique_strings([maybe_text(item.get("claim_type")) for item in ordered]):
        take_first(lambda item, claim_type=claim_type: maybe_text(item.get("claim_type")) == claim_type)
    for candidate in ordered:
        submission_id = maybe_text(candidate.get("submission_id"))
        if len(selected) >= limit:
            break
        if not submission_id or submission_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(submission_id)
    return selected


def observation_submission_severity(submission: dict[str, Any]) -> float:
    statistics_obj = compact_statistics(submission.get("statistics"))
    if isinstance(statistics_obj, dict):
        for key in ("max", "p95", "mean", "min"):
            value = maybe_number(statistics_obj.get(key))
            if value is not None:
                return value
    value = maybe_number(submission.get("value"))
    return value if value is not None else 0.0


def representative_observation_order(observations: list[dict[str, Any]], claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    family_order = environment_family_priority_order(claims)
    ordered = sorted(
        observations,
        key=lambda item: (
            family_order.index(observation_metric_family(item.get("metric")))
            if observation_metric_family(item.get("metric")) in family_order
            else len(family_order),
            -observation_submission_severity(item),
            maybe_text(item.get("source_skill")),
            maybe_text(item.get("metric")),
            maybe_text(item.get("observation_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        for candidate in ordered:
            observation_id = maybe_text(candidate.get("observation_id"))
            if not observation_id or observation_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(observation_id)
            return

    for family in family_order:
        take_first(lambda item, family=family: observation_metric_family(item.get("metric")) == family)
    for source_skill in unique_strings([maybe_text(item.get("source_skill")) for item in ordered]):
        take_first(lambda item, source_skill=source_skill: maybe_text(item.get("source_skill")) == source_skill)
    for metric in unique_strings([maybe_text(item.get("metric")) for item in ordered]):
        take_first(lambda item, metric=metric: maybe_text(item.get("metric")) == metric)
    for candidate in ordered:
        observation_id = maybe_text(candidate.get("observation_id"))
        if not observation_id or observation_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(observation_id)
    return selected


def select_environment_submissions(submissions: list[dict[str, Any]], claims: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    family_order = environment_family_priority_order(claims)
    ordered = sorted(
        submissions,
        key=lambda item: (
            family_order.index(observation_metric_family(item.get("metric")))
            if observation_metric_family(item.get("metric")) in family_order
            else len(family_order),
            -observation_submission_severity(item),
            maybe_text(item.get("source_skill")),
            maybe_text(item.get("metric")),
            maybe_text(item.get("submission_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        if len(selected) >= limit:
            return
        for candidate in ordered:
            submission_id = maybe_text(candidate.get("submission_id"))
            if not submission_id or submission_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(submission_id)
            return

    for family in family_order:
        take_first(lambda item, family=family: observation_metric_family(item.get("metric")) == family)
    for source_skill in unique_strings([maybe_text(item.get("source_skill")) for item in ordered]):
        take_first(lambda item, source_skill=source_skill: maybe_text(item.get("source_skill")) == source_skill)
    for metric in unique_strings([maybe_text(item.get("metric")) for item in ordered]):
        take_first(lambda item, metric=metric: maybe_text(item.get("metric")) == metric)
    for candidate in ordered:
        submission_id = maybe_text(candidate.get("submission_id"))
        if len(selected) >= limit:
            break
        if not submission_id or submission_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(submission_id)
    return selected


def representative_submissions(state: dict[str, Any], role: str, submissions: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if role == "sociologist":
        return select_public_submissions(submissions, limit)
    return select_environment_submissions(submissions, state.get("claims", []), limit)


def aggregate_compact_audit(
    state: dict[str, Any],
    role: str,
    submissions: list[dict[str, Any]],
    *,
    fallback_summary: str,
    retained_limit: int,
) -> dict[str, Any]:
    selected = representative_submissions(state, role, submissions, retained_limit)
    coverage_dimensions: list[str] = []
    missing_dimensions: list[str] = []
    concentration_flags: list[str] = []
    sampling_notes: list[str] = []

    if role == "sociologist":
        full_channels = claim_submission_channels(submissions)
        full_sources = unique_strings([skill for item in submissions for skill in claim_submission_source_skills(item)])
        selected_channels = claim_submission_channels(selected)
        selected_sources = unique_strings([skill for item in selected for skill in claim_submission_source_skills(item)])
        channel_counts = Counter(channel for item in submissions for channel in claim_submission_channels_for_submission(item))
        source_counts = Counter(skill for item in submissions for skill in claim_submission_source_skills(item))
        coverage_dimensions = ["supporting-artifacts", "channel", "source-skill", "claim-type"]
        if len(full_channels) > 1 and len(selected_channels) < min(len(full_channels), max(1, retained_limit)):
            missing_dimensions.append("channel-coverage")
        if len(full_sources) > 1 and len(selected_sources) < min(len(full_sources), max(1, retained_limit)):
            missing_dimensions.append("source-skill-coverage")
        if submissions and all(to_nonnegative_int(item.get("source_signal_count")) <= 1 for item in submissions):
            missing_dimensions.append("multi-signal-corroboration")
        top_channel = channel_counts.most_common(1)[0] if channel_counts else None
        top_source = source_counts.most_common(1)[0] if source_counts else None
        if top_channel is not None and len(submissions) >= 4 and top_channel[1] / len(submissions) >= 0.8:
            concentration_flags.append(f"Auditable public submissions remain highly concentrated in the {top_channel[0]} channel.")
        if top_source is not None and len(submissions) >= 4 and top_source[1] / len(submissions) >= 0.8:
            concentration_flags.append(f"Auditable public submissions remain highly concentrated in {top_source[0]}.")
        coverage_summary = (
            f"Selected {len(selected)} auditable claim submissions from {len(submissions)} total while covering "
            f"{len(selected_channels)}/{len(full_channels) or 1} channels and {len(selected_sources)}/{len(full_sources) or 1} source skills."
        )
        sampling_notes.extend(
            [
                f"Dominant channels: {', '.join(f'{channel}:{count}' for channel, count in channel_counts.most_common(3))}" if channel_counts else "No channel distribution was available.",
                f"Dominant source skills: {', '.join(f'{source}:{count}' for source, count in source_counts.most_common(3))}" if source_counts else "No source-skill distribution was available.",
                f"Selected claim types: {', '.join(unique_strings([maybe_text(item.get('claim_type')) for item in selected]))}" if selected else "No claim submissions were selected.",
            ]
        )
    else:
        full_families = unique_strings([observation_metric_family(item.get("metric")) for item in submissions])
        full_sources = unique_strings([maybe_text(item.get("source_skill")) for item in submissions if maybe_text(item.get("source_skill"))])
        selected_families = unique_strings([observation_metric_family(item.get("metric")) for item in selected])
        selected_sources = unique_strings([maybe_text(item.get("source_skill")) for item in selected if maybe_text(item.get("source_skill"))])
        selected_with_stats = sum(1 for item in selected if compact_statistics(item.get("statistics")))
        available_with_stats = sum(1 for item in submissions if compact_statistics(item.get("statistics")))
        coverage_dimensions = ["metric-family", "source-skill", "time-window"]
        if selected_with_stats:
            coverage_dimensions.append("extrema-summary")
        if len(full_families) > 1 and len(selected_families) < min(len(full_families), max(1, retained_limit)):
            missing_dimensions.append("metric-family-coverage")
        if len(full_sources) > 1 and len(selected_sources) < min(len(full_sources), max(1, retained_limit)):
            missing_dimensions.append("source-skill-coverage")
        if available_with_stats and selected_with_stats == 0:
            missing_dimensions.append("extrema-retention")
        coverage_summary = (
            f"Selected {len(selected)} auditable observation submissions from {len(submissions)} total while covering "
            f"{len(selected_families)}/{len(full_families) or 1} metric families and {len(selected_sources)}/{len(full_sources) or 1} source skills."
        )
        sampling_notes.extend(
            [
                f"Selected metric families: {', '.join(selected_families)}" if selected_families else "No metric-family coverage was selected.",
                f"Selected source skills: {', '.join(selected_sources)}" if selected_sources else "No source-skill coverage was selected.",
                f"Selected submissions with statistics retained: {selected_with_stats} of {available_with_stats}." if available_with_stats else "No observation statistics were available.",
            ]
        )

    return {
        "representative": bool(submissions) and not concentration_flags and not missing_dimensions,
        "retained_count": len(selected),
        "total_candidate_count": len(submissions),
        "coverage_summary": coverage_summary if submissions else fallback_summary,
        "concentration_flags": unique_strings(concentration_flags),
        "coverage_dimensions": unique_strings(coverage_dimensions),
        "missing_dimensions": unique_strings(missing_dimensions),
        "sampling_notes": unique_strings(sampling_notes),
    }


def representative_observations_for_state(state: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    observation_by_id = {
        maybe_text(item.get("observation_id")): item
        for item in state.get("observations", [])
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    selected = representative_submissions(
        state,
        "environmentalist",
        state_auditable_submissions(state, "environmentalist"),
        limit,
    )
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for submission in selected:
        observation_id = maybe_text(submission.get("observation_id"))
        if not observation_id or observation_id in seen:
            continue
        observation = observation_by_id.get(observation_id)
        if observation is not None:
            ordered.append(observation)
            seen.add(observation_id)
    for observation in state.get("observations", []):
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in seen:
            continue
        ordered.append(observation)
        seen.add(observation_id)
        if len(ordered) >= limit:
            break
    return ordered


def build_fallback_context(
    *,
    mission: dict[str, Any],
    round_id: str,
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    role: str,
) -> dict[str, Any]:
    role_tasks = [task for task in tasks if role == "moderator" or task.get("assigned_role") == role]
    return {
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region": mission.get("region"),
            "window": mission.get("window"),
            "role": role,
        },
        "dataset": {
            "generated_at_utc": utc_now_iso(),
            "task_count": len(role_tasks),
            "claim_count": len(claims),
            "observation_count": len(observations),
            "evidence_count": len(evidence_cards),
        },
        "aggregates": {
            "claim_type_counts": counter_dict([maybe_text(item.get("claim_type")) for item in claims]),
            "observation_metric_counts": counter_dict([maybe_text(item.get("metric")) for item in observations]),
            "evidence_verdict_counts": counter_dict([maybe_text(item.get("verdict")) for item in evidence_cards]),
        },
        "tasks": role_tasks,
        "focus": {
            "task_ids": [maybe_text(task.get("task_id")) for task in role_tasks],
            "claims_needing_more_evidence": [
                maybe_text(card.get("claim_id"))
                for card in evidence_cards
                if maybe_text(card.get("verdict")) in {"mixed", "insufficient"}
            ],
        },
        "claims": claims,
        "observations": observations,
        "evidence_cards": evidence_cards,
    }


def load_context_or_fallback(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    path = role_context_path(run_dir, round_id, role)
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return payload
    return build_fallback_context(
        mission=mission,
        round_id=round_id,
        tasks=tasks,
        claims=claims,
        observations=observations,
        evidence_cards=evidence_cards,
        role=role,
    )


def build_fallback_context_from_state(*, run_dir: Path, state: dict[str, Any], role: str) -> dict[str, Any]:
    mission = state["mission"]
    round_id = state["round_id"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    role_tasks = [task for task in tasks if role == "moderator" or maybe_text(task.get("assigned_role")) == role]
    claim_candidates_current = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    observation_candidates_current = (
        state.get("observation_candidates_current", [])
        if isinstance(state.get("observation_candidates_current"), list)
        else []
    )
    claim_curation = state.get("claim_curation", {}) if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = state.get("observation_curation", {}) if isinstance(state.get("observation_curation"), dict) else {}
    claim_submissions_auditable = state_auditable_submissions(state, "sociologist")
    observation_submissions_auditable = state_auditable_submissions(state, "environmentalist")
    representative_claim_submissions = representative_submissions(state, "sociologist", claim_submissions_auditable, 6)
    representative_observation_submissions = representative_submissions(state, "environmentalist", observation_submissions_auditable, 8)
    representative_current_claim_submissions = representative_submissions(state, "sociologist", state_current_submissions(state, "sociologist"), 6)
    representative_current_observation_submissions = representative_submissions(
        state,
        "environmentalist",
        state_current_submissions(state, "environmentalist"),
        8,
    )
    return {
        "context_layer": "reporting-fallback-v2",
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region": mission.get("region"),
            "window": mission.get("window"),
            "role": role,
        },
        "dataset": {
            "generated_at_utc": utc_now_iso(),
            "task_count": len(role_tasks),
            "claim_count": len(state.get("claims", [])),
            "observation_count": len(state.get("observations", [])),
            "evidence_count": len(state.get("cards_active", [])),
            "claim_submission_count": len(claim_submissions_auditable),
            "observation_submission_count": len(observation_submissions_auditable),
            "claim_submission_current_count": len(state_current_submissions(state, "sociologist")),
            "observation_submission_current_count": len(state_current_submissions(state, "environmentalist")),
            "claim_candidate_count": len(claim_candidates_current),
            "observation_candidate_count": len(observation_candidates_current),
            "isolated_count": len(state.get("isolated_active", [])),
            "remand_count": len(state.get("remands_open", [])),
        },
        "causal_focus": causal_focus_for_role(
            state.get("investigation_plan", {}) if isinstance(state.get("investigation_plan"), dict) else {},
            role,
        ),
        "investigation_state": compact_investigation_state_summary(
            state.get("investigation_state", {}) if isinstance(state.get("investigation_state"), dict) else {}
        ),
        "investigation_actions": compact_investigation_actions_summary(
            state.get("investigation_actions", {}) if isinstance(state.get("investigation_actions"), dict) else {}
        ),
        "phase_state": state.get("phase_state", {}),
        "tasks": role_tasks,
        "claims": [compact_claim(item) for item in state.get("claims", [])[:6]],
        "observations": [compact_observation(item) for item in representative_observations_for_state(state, 8)],
        "evidence_cards": [compact_evidence_card(item) for item in state.get("cards_active", [])[:8]],
        "evidence_library": {
            "claim_submissions_auditable": [
                compact_claim_submission(item)
                for item in representative_claim_submissions
            ],
            "observation_submissions_auditable": [
                compact_observation_submission(item)
                for item in representative_observation_submissions
            ],
            "claim_submissions_current": [
                compact_claim_submission(item)
                for item in representative_current_claim_submissions
            ],
            "observation_submissions_current": [
                compact_observation_submission(item)
                for item in representative_current_observation_submissions
            ],
            "claim_candidates_current": [compact_claim_candidate_for_curation(item) for item in claim_candidates_current[:12]],
            "observation_candidates_current": [
                compact_observation_candidate_for_curation(item) for item in observation_candidates_current[:16]
            ],
            "claim_curation": {
                "status": maybe_text(claim_curation.get("status")),
                "curated_claim_count": len(claim_curation.get("curated_claims", []))
                if isinstance(claim_curation.get("curated_claims"), list)
                else 0,
            },
            "observation_curation": {
                "status": maybe_text(observation_curation.get("status")),
                "curated_observation_count": len(observation_curation.get("curated_observations", []))
                if isinstance(observation_curation.get("curated_observations"), list)
                else 0,
            },
            "claims_active": [compact_claim_submission(item) for item in representative_claim_submissions],
            "observations_active": [
                compact_observation_submission(item) for item in representative_observation_submissions
            ],
            "cards_active": [compact_evidence_card(item) for item in state.get("cards_active", [])[:8]],
            "isolated_active": [compact_isolated_entry(item) for item in state.get("isolated_active", [])[:6]],
            "remands_open": [compact_remand_entry(item) for item in state.get("remands_open", [])[:6]],
        },
        "canonical_paths": {
            "claim_candidates": str(claim_candidates_path(run_dir, round_id)),
            "observation_candidates": str(observation_candidates_path(run_dir, round_id)),
            "claim_curation": str(claim_curation_path(run_dir, round_id)),
            "observation_curation": str(observation_curation_path(run_dir, round_id)),
            "claim_submissions": str(claim_submissions_path(run_dir, round_id)),
            "observation_submissions": str(observation_submissions_path(run_dir, round_id)),
            "matching_authorization": str(matching_authorization_path(run_dir, round_id)),
            "matching_result": str(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": str(evidence_adjudication_path(run_dir, round_id)),
            "investigation_state": str(investigation_state_path(run_dir, round_id)),
            "investigation_actions": str(investigation_actions_path(run_dir, round_id)),
        },
    }


def load_context_or_fallback_from_state(*, run_dir: Path, state: dict[str, Any], role: str) -> dict[str, Any]:
    path = role_context_path(run_dir, state["round_id"], role)
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return augment_context_with_matching_state(run_dir=run_dir, state=state, context=payload)
    return augment_context_with_matching_state(
        run_dir=run_dir,
        state=state,
        context=build_fallback_context_from_state(run_dir=run_dir, state=state, role=role),
    )


__all__ = [
    "aggregate_compact_audit",
    "build_claim_candidate_pool_summary",
    "build_fallback_context",
    "build_fallback_context_from_state",
    "build_observation_candidate_pool_summary",
    "candidate_claim_entry_from_candidate",
    "candidate_observation_entry_from_candidate",
    "claim_day_bucket",
    "claim_submission_channels",
    "claim_submission_channels_for_submission",
    "claim_submission_source_skills",
    "compact_claim",
    "compact_claim_candidate_for_curation",
    "compact_claim_scope",
    "compact_claim_submission",
    "compact_count_items",
    "compact_distribution_summary",
    "compact_evidence_card",
    "compact_isolated_entry",
    "compact_observation",
    "compact_observation_candidate_for_curation",
    "compact_observation_submission",
    "compact_remand_entry",
    "compact_statistics",
    "counter_dict",
    "environment_family_priority_order",
    "guess_observation_candidate_evidence_role",
    "load_context_or_fallback",
    "load_context_or_fallback_from_state",
    "merge_count_items",
    "maybe_number",
    "observation_candidate_signal_count",
    "observation_metric_family",
    "observation_submission_severity",
    "point_bucket_from_scope",
    "public_source_channel",
    "public_source_channels",
    "ranked_claim_candidates",
    "representative_observation_order",
    "representative_observations_for_state",
    "representative_submissions",
    "select_environment_submissions",
    "select_public_submissions",
    "sorted_counter_items",
    "to_nonnegative_int",
    "top_counter_items",
]
