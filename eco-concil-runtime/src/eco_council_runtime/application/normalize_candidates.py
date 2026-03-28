"""Application builders for normalize claim and observation candidates."""

from __future__ import annotations

import copy
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from eco_council_runtime.adapters.filesystem import stable_hash
from eco_council_runtime.application.normalize_library import (
    build_compact_audit,
    default_distribution_summary_from_observation,
    point_bucket_key,
    sorted_counter_items,
    top_counter_items,
    top_counter_text,
)
from eco_council_runtime.domain.normalize_semantics import (
    NON_CLAIM_PUBLIC_SIGNAL_KINDS,
    PHYSICAL_CLAIM_TYPES,
    best_public_claim_hypothesis_id,
    build_public_claim_scope,
    candidate_statement,
    canonical_environment_metric,
    claim_type_from_text,
    infer_observation_investigation_tags,
    maybe_number,
    parse_loose_datetime,
    public_signal_channel,
    semantic_fingerprint,
    to_rfc3339_z,
)
from eco_council_runtime.domain.text import maybe_text, normalize_space, truncate_text

IdEmitter = Callable[[str, int], str]
PayloadValidator = Callable[[str, dict[str, Any]], Any]


def _default_emit_row_id(prefix: str, index: int) -> str:
    return f"{prefix}-{index:03d}"


def _noop_validate_payload(_kind: str, _payload: dict[str, Any]) -> None:
    return None


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    position = quantile * (len(ordered) - 1)
    lower = int(position // 1)
    upper = int(-(-position // 1))
    if lower == upper:
        return ordered[lower]
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    weight = position - lower
    return lower_value + (upper_value - lower_value) * weight


def percentile95(values: list[float]) -> float | None:
    return percentile(values, 0.95)


def artifact_ref(signal: dict[str, Any]) -> dict[str, Any]:
    ref = {
        "source_skill": signal["source_skill"],
        "artifact_path": signal["artifact_path"],
        "record_locator": signal["record_locator"],
    }
    if signal.get("external_id"):
        ref["external_id"] = signal["external_id"]
    if signal.get("sha256"):
        ref["sha256"] = signal["sha256"]
    return ref


def public_group_compact_audit(items: list[dict[str, Any]]) -> dict[str, Any]:
    source_counts = Counter(maybe_text(item.get("source_skill")) for item in items)
    channel_counts = Counter(public_signal_channel(maybe_text(item.get("source_skill"))) for item in items)
    day_counts = Counter(
        parsed.date().isoformat()
        for item in items
        for parsed in [parse_loose_datetime(item.get("published_at_utc"))]
        if parsed is not None
    )
    concentration_flags: list[str] = []
    missing_dimensions: list[str] = []
    top_source = source_counts.most_common(1)[0] if source_counts else None
    top_channel = channel_counts.most_common(1)[0] if channel_counts else None
    if top_source is not None and len(items) >= 4 and top_source[1] / len(items) >= 0.8:
        concentration_flags.append(f"Public evidence is highly concentrated in {top_source[0]}.")
    if top_channel is not None and len(items) >= 4 and top_channel[1] / len(items) >= 0.8:
        concentration_flags.append(f"Public evidence is highly concentrated in the {top_channel[0]} channel.")
    if len(items) >= 4 and len(source_counts) < min(2, len(items)):
        missing_dimensions.append("independent-source-diversity")
    if len(items) >= 4 and len(channel_counts) < min(2, len(items)):
        missing_dimensions.append("channel-diversity")
    return build_compact_audit(
        total_candidate_count=len(items),
        retained_count=min(len(items), 8),
        coverage_summary=(
            f"Retained {min(len(items), 8)} references from {len(items)} supporting public signals "
            f"across {len(source_counts)} source skills and {len(channel_counts)} channels."
        ),
        coverage_dimensions=["supporting-artifacts", "source-skill", "channel", "time-window"],
        missing_dimensions=missing_dimensions,
        concentration_flags=concentration_flags,
        sampling_notes=[
            f"Dominant source skills: {top_counter_text(source_counts, limit=3)}" if source_counts else "No dominant sources recorded.",
            f"Dominant channels: {top_counter_text(channel_counts, limit=3)}" if channel_counts else "No dominant channels recorded.",
            f"Observed publication days: {top_counter_text(day_counts, limit=3)}" if day_counts else "Publication-time spread was not available.",
        ],
    )


def distribution_summary_from_environment_group(
    group: list[dict[str, Any]],
    *,
    metric_override: str | None = None,
) -> dict[str, Any]:
    canonical_metric_override = canonical_environment_metric(metric_override) if maybe_text(metric_override) else ""
    day_counts = Counter(
        parsed.date().isoformat()
        for item in group
        for parsed in [parse_loose_datetime(item.get("observed_at_utc") or item.get("window_start_utc") or item.get("window_end_utc"))]
        if parsed is not None
    )
    source_counts = Counter(
        maybe_text(item.get("source_skill"))
        for item in group
        if maybe_text(item.get("source_skill"))
    )
    metric_counts = Counter(
        canonical_metric_override or canonical_environment_metric(item.get("metric"))
        for item in group
        if canonical_metric_override or maybe_text(item.get("metric"))
    )
    point_counts = Counter(
        point_bucket_key(item.get("latitude"), item.get("longitude"))
        for item in group
        if point_bucket_key(item.get("latitude"), item.get("longitude"))
    )
    return {
        "signal_count": len(group),
        "distinct_day_count": len(day_counts),
        "distinct_source_skill_count": len(source_counts),
        "distinct_point_count": len(point_counts),
        "time_bucket_counts": sorted_counter_items(day_counts, limit=10),
        "source_skill_counts": top_counter_items(source_counts, limit=10),
        "metric_counts": top_counter_items(metric_counts, limit=10),
        "point_bucket_counts": sorted_counter_items(point_counts, limit=10),
    }


def observation_group_compact_audit(
    group: list[dict[str, Any]],
    *,
    metric_override: str | None = None,
) -> dict[str, Any]:
    distribution_summary = distribution_summary_from_environment_group(group, metric_override=metric_override)
    source_counts = Counter(maybe_text(item.get("source_skill")) for item in group)
    day_counts = Counter(
        parsed.date().isoformat()
        for item in group
        for parsed in [parse_loose_datetime(item.get("observed_at_utc") or item.get("window_start_utc") or item.get("window_end_utc"))]
        if parsed is not None
    )
    point_counts = Counter(
        point_bucket_key(item.get("latitude"), item.get("longitude"))
        for item in group
        if point_bucket_key(item.get("latitude"), item.get("longitude"))
    )
    values = [float(item["value"]) for item in group if maybe_number(item.get("value")) is not None]
    missing_dimensions: list[str] = []
    if len(group) > 1 and not day_counts:
        missing_dimensions.append("time-window-coverage")
    if len(group) > 1 and not values:
        missing_dimensions.append("value-distribution")
    return build_compact_audit(
        total_candidate_count=len(group),
        retained_count=1,
        coverage_summary=(
            f"Aggregated {len(group)} raw environment signals into one canonical observation while retaining summary statistics."
        ),
        coverage_dimensions=["metric", "time-window", "place-scope", "distribution-summary", "extrema-summary", "source-skill"],
        missing_dimensions=missing_dimensions,
        concentration_flags=[],
        sampling_notes=[
            f"Dominant source skills: {top_counter_text(source_counts, limit=3)}" if source_counts else "No dominant sources recorded.",
            (
                f"Observed value range: min={min(values):g}, max={max(values):g}, mean={statistics.fmean(values):g}."
                if values
                else "Observed value range was unavailable."
            ),
            f"Observed days: {top_counter_text(day_counts, limit=3)}" if day_counts else "Observed-day coverage was unavailable.",
            (
                f"Signal count retained in summary: {distribution_summary.get('signal_count') or len(group)}."
                if isinstance(distribution_summary, dict)
                else f"Signal count retained in summary: {len(group)}."
            ),
            f"Distinct rounded points: {len(point_counts)}." if point_counts else "Spatial point spread was unavailable.",
        ],
    )


def first_datetime_and_last(values: list[dict[str, Any]]) -> tuple[str, str] | None:
    datetimes: list[datetime] = []
    for item in values:
        observed = parse_loose_datetime(item.get("observed_at_utc") or item.get("window_start_utc"))
        if observed is not None:
            datetimes.append(observed)
    if not datetimes:
        return None
    datetimes.sort()
    return to_rfc3339_z(datetimes[0]) or "", to_rfc3339_z(datetimes[-1]) or ""


def aggregate_stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {
            "sample_count": 0,
            "min": None,
            "p05": None,
            "p25": None,
            "mean": None,
            "median": None,
            "p75": None,
            "p95": None,
            "max": None,
            "stddev": None,
        }
    stddev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {
        "sample_count": len(values),
        "min": min(values),
        "p05": percentile(values, 0.05),
        "p25": percentile(values, 0.25),
        "max": max(values),
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "p75": percentile(values, 0.75),
        "p95": percentile95(values),
        "stddev": stddev,
    }


def observation_group_key(signal: dict[str, Any], mission_scope: dict[str, Any]) -> tuple[str, str, str]:
    metric = canonical_environment_metric(signal.get("metric"))
    source_skill = maybe_text(signal.get("source_skill"))
    lat = maybe_number(signal.get("latitude"))
    lon = maybe_number(signal.get("longitude"))
    if lat is None or lon is None:
        return (source_skill, metric, stable_hash(json.dumps(mission_scope, sort_keys=True))[:8])
    return (source_skill, metric, f"{lat:.3f},{lon:.3f}")


def derive_place_scope(signals: list[dict[str, Any]], mission_scope: dict[str, Any]) -> dict[str, Any]:
    if not signals:
        return mission_scope
    latitudes = [maybe_number(item.get("latitude")) for item in signals]
    longitudes = [maybe_number(item.get("longitude")) for item in signals]
    if any(value is None for value in latitudes + longitudes):
        return mission_scope
    unique_points = {(round(float(lat), 3), round(float(lon), 3)) for lat, lon in zip(latitudes, longitudes)}
    if len(unique_points) != 1:
        return mission_scope
    latitude = statistics.fmean(float(value) for value in latitudes if value is not None)
    longitude = statistics.fmean(float(value) for value in longitudes if value is not None)
    return {
        "label": mission_scope["label"],
        "geometry": {"type": "Point", "latitude": latitude, "longitude": longitude},
    }


def public_signals_to_claims(
    *,
    run_id: str,
    round_id: str,
    signals: list[dict[str, Any]],
    max_claims: int,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
    investigation_plan: dict[str, Any],
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> list[dict[str, Any]]:
    emit = emit_row_id or _default_emit_row_id
    validate = validate_payload or _noop_validate_payload
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        if maybe_text(signal.get("signal_kind")) in NON_CLAIM_PUBLIC_SIGNAL_KINDS:
            continue
        source_text = normalize_space(
            " ".join(
                part
                for part in (
                    maybe_text(signal.get("title")),
                    maybe_text(signal.get("text")),
                )
                if part
            )
        )
        if not source_text:
            continue
        claim_type = claim_type_from_text(source_text)
        if claim_type == "other":
            continue
        fingerprint = semantic_fingerprint(source_text) or signal["signal_id"]
        groups[f"{claim_type}|{fingerprint}"].append(signal)

    ranked = sorted(
        groups.values(),
        key=lambda items: (
            -len(items),
            -(parse_loose_datetime(items[0].get("published_at_utc")) or datetime(1970, 1, 1, tzinfo=timezone.utc)).timestamp(),
            items[0]["signal_id"],
        ),
    )

    claims: list[dict[str, Any]] = []
    for index, items in enumerate(ranked[:max_claims], start=1):
        lead = items[0]
        combined_text = maybe_text(lead.get("text")) or maybe_text(lead.get("title"))
        summary = truncate_text(maybe_text(lead.get("title")) or combined_text, 180)
        claim_type = claim_type_from_text(summary + " " + combined_text)
        statement = candidate_statement(summary, combined_text or summary)
        claim_scope = build_public_claim_scope(
            signals=items,
            mission_scope=mission_scope,
            mission_time_window=mission_time_window,
        )
        hypothesis_id = best_public_claim_hypothesis_id(
            investigation_plan,
            claim_type,
            statement,
        )
        claim = {
            "schema_version": schema_version,
            "claim_id": emit("claim", index),
            "run_id": run_id,
            "round_id": round_id,
            "agent_role": "sociologist",
            "claim_type": claim_type,
            "status": "candidate",
            "summary": summary or f"Candidate claim from {lead['source_skill']}",
            "statement": statement,
            "priority": min(index, 5),
            "needs_physical_validation": claim_type in PHYSICAL_CLAIM_TYPES,
            "time_window": copy.deepcopy(claim_scope["time_window"]),
            "place_scope": copy.deepcopy(claim_scope["place_scope"]),
            "claim_scope": claim_scope,
            "leg_id": "public_interpretation",
            "public_refs": [artifact_ref(item) for item in items[:8]],
            "source_signal_count": len(items),
            "compact_audit": public_group_compact_audit(items),
        }
        if hypothesis_id:
            claim["hypothesis_id"] = hypothesis_id
        validate("claim", claim)
        claims.append(claim)
    return claims


def environment_signals_to_observations(
    *,
    run_id: str,
    round_id: str,
    signals: list[dict[str, Any]],
    extra_observations: list[dict[str, Any]],
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
    investigation_plan: dict[str, Any],
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> list[dict[str, Any]]:
    emit = emit_row_id or _default_emit_row_id
    validate = validate_payload or _noop_validate_payload
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        grouped[observation_group_key(signal, mission_scope)].append(signal)

    observations: list[dict[str, Any]] = []
    counter = 1
    for (_, metric, _), group in sorted(grouped.items()):
        values = [float(signal["value"]) for signal in group if maybe_number(signal.get("value")) is not None]
        if not values:
            continue
        source_skill = group[0]["source_skill"]
        output_metric = metric
        aggregation = "window-summary" if len(values) > 1 else "point"
        value = statistics.fmean(values) if len(values) > 1 else values[0]
        if source_skill == "nasa-firms-fire-fetch" and metric == "fire_detection":
            output_metric = "fire_detection_count"
            aggregation = "event-count"
            value = float(len(group))
        window = first_datetime_and_last(group)
        if window is None:
            time_window = mission_time_window
        else:
            start_utc, end_utc = window
            time_window = {
                "start_utc": start_utc or mission_time_window["start_utc"],
                "end_utc": end_utc or mission_time_window["end_utc"],
            }
        quality_flags = sorted({flag for signal in group for flag in signal.get("quality_flags", [])})
        observation = {
            "schema_version": schema_version,
            "observation_id": emit("obs", counter),
            "run_id": run_id,
            "round_id": round_id,
            "agent_role": "environmentalist",
            "source_skill": source_skill,
            "metric": output_metric,
            "aggregation": aggregation,
            "value": value,
            "unit": "count" if output_metric == "fire_detection_count" else group[0]["unit"],
            "statistics": aggregate_stats(values),
            "time_window": time_window,
            "place_scope": derive_place_scope(group, mission_scope),
            "quality_flags": quality_flags,
            "provenance": artifact_ref(group[0]),
            "compact_audit": observation_group_compact_audit(group, metric_override=output_metric),
            "distribution_summary": distribution_summary_from_environment_group(group, metric_override=output_metric),
        }
        observation.update(
            infer_observation_investigation_tags(
                observation,
                plan=investigation_plan,
                mission_scope=mission_scope,
            )
        )
        validate("observation", observation)
        observations.append(observation)
        counter += 1

    for item in extra_observations:
        item["observation_id"] = emit("obs", counter)
        item["run_id"] = run_id
        item["round_id"] = round_id
        item["place_scope"] = mission_scope
        item["time_window"] = mission_time_window
        item.setdefault(
            "compact_audit",
            build_compact_audit(
                total_candidate_count=1,
                retained_count=1,
                coverage_summary="The canonical observation was emitted directly from an extra deterministic observation source.",
                concentration_flags=[],
                sampling_notes=[],
            ),
        )
        item.setdefault("distribution_summary", default_distribution_summary_from_observation(item))
        item.update(
            infer_observation_investigation_tags(
                item,
                plan=investigation_plan,
                mission_scope=mission_scope,
            )
        )
        validate("observation", item)
        observations.append(item)
        counter += 1
    return observations


__all__ = [
    "aggregate_stats",
    "artifact_ref",
    "derive_place_scope",
    "distribution_summary_from_environment_group",
    "environment_signals_to_observations",
    "first_datetime_and_last",
    "observation_group_compact_audit",
    "observation_group_key",
    "percentile",
    "percentile95",
    "public_group_compact_audit",
    "public_signals_to_claims",
]
