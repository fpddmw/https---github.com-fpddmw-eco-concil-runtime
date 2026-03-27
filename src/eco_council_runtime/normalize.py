#!/usr/bin/env python3
"""Deterministic normalization pipeline for eco-council runs."""

from __future__ import annotations

import argparse
import copy
import json
import math
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from eco_council_runtime.adapters.filesystem import (
    atomic_write_text_file,
    load_canonical_list,
    load_json_if_exists,
    pretty_json,
    read_json,
    read_jsonl,
    stable_hash,
    stable_json,
    utc_now_iso,
    write_json,
    write_jsonl,
)
from eco_council_runtime.application.normalize_sources import (
    NORMALIZE_CACHE_VERSION,
    normalize_cache_dir,
    normalize_environment_source as application_normalize_environment_source,
    normalize_environment_source_cached as application_normalize_environment_source_cached,
    normalize_public_source as application_normalize_public_source,
    normalize_public_source_cached as application_normalize_public_source_cached,
)
from eco_council_runtime.adapters.run_paths import discover_round_ids, load_mission
from eco_council_runtime.controller.audit_chain import ensure_audit_chain_ready, record_match_phase_receipt
from eco_council_runtime.controller.paths import (
    cards_active_path,
    claim_candidates_path,
    claim_curation_path,
    claim_submissions_path,
    claims_active_path,
    data_readiness_report_path,
    default_context_dir,
    evidence_adjudication_path,
    evidence_library_dir,
    evidence_library_ledger_path,
    investigation_plan_path,
    isolated_active_path,
    library_context_path,
    matching_adjudication_draft_path,
    matching_adjudication_path,
    matching_authorization_path,
    matching_candidate_set_path,
    matching_result_path,
    mission_path,
    moderator_derived_dir,
    observation_candidates_path,
    observation_curation_path,
    observation_submissions_path,
    observations_active_path,
    remands_open_path,
    role_normalized_dir,
    round_dir,
    round_dir_name as round_directory_name,
    shared_claims_path,
    shared_evidence_path,
    shared_observations_path,
)
from eco_council_runtime.domain.contract_bridge import (
    contract_call,
    effective_constraints as contract_effective_constraints,
    effective_matching_authorization,
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.normalize_semantics import (
    CLAIM_KEYWORDS,
    CLAIM_METRIC_RULES,
    DEFAULT_OBSERVATION_FAMILY_ORDER,
    ENVIRONMENT_METRIC_ALIASES,
    GENERIC_REGION_TOKENS,
    HYDROLOGY_METRICS,
    METEOROLOGY_METRICS,
    METRIC_FAMILY_GROUPS,
    NON_CLAIM_PUBLIC_SIGNAL_KINDS,
    PHYSICAL_CLAIM_TYPES,
    PHYSICAL_LEG_ORDER,
    POINT_MATCH_EPSILON_DEGREES,
    PRECIPITATION_METRICS,
    STOPWORDS,
    assess_claim_metric_value,
    assess_observation_against_claim,
    best_physical_observation_hypothesis_id,
    best_public_claim_hypothesis_id,
    bbox_scope_from_location_candidates,
    build_evidence_summary,
    build_public_claim_scope,
    candidate_statement,
    canonical_environment_metric,
    claim_matching_scope,
    claim_priority_metric_families,
    claim_type_from_text,
    compact_claim_scope,
    default_evidence_role_for_claim_metric,
    derive_public_claim_place_scope,
    derive_public_claim_time_window,
    direct_matching_gap_for_claim,
    effective_component_role_for_claim,
    extract_value_for_metric,
    geometry_overlap,
    geometry_to_bbox,
    infer_observation_investigation_tags,
    iter_observation_assessment_components,
    maybe_number,
    metric_relevant,
    observation_metric_family,
    observation_overlaps_mission_scope,
    parse_loose_datetime,
    physical_investigation_leg_lookup,
    point_matches_geometry,
    public_signal_channel,
    public_signal_location_candidates,
    public_signal_mentions_mission_region,
    region_core_tokens,
    row_token_set,
    score_observation_for_investigation_leg,
    semantic_fingerprint,
    text_tokens,
    time_windows_overlap,
    to_rfc3339_z,
)
from eco_council_runtime.domain.rounds import parse_round_components, round_sort_key
from eco_council_runtime.domain.text import maybe_text, normalize_space, truncate_text, unique_strings
from eco_council_runtime.layout import (
    NORMALIZE_ASSETS_DIR,
    NORMALIZE_ENVIRONMENT_DDL_PATH,
    NORMALIZE_PUBLIC_DDL_PATH,
)
from eco_council_runtime.investigation import build_investigation_plan, causal_focus_for_role

ASSETS_DIR = NORMALIZE_ASSETS_DIR
PUBLIC_DDL_PATH = NORMALIZE_PUBLIC_DDL_PATH
ENVIRONMENT_DDL_PATH = NORMALIZE_ENVIRONMENT_DDL_PATH

SCHEMA_VERSION = resolve_schema_version("1.0.0")
MAX_CONTEXT_TASKS = 4
MAX_CONTEXT_CLAIMS = 4
MAX_CONTEXT_OBSERVATIONS = 8
MAX_CONTEXT_EVIDENCE = 4

def maybe_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if not value.is_integer():
            return None
        return int(value) if value >= 0 else None
    if isinstance(value, str) and value.strip():
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def point_bucket_key(latitude: Any, longitude: Any) -> str:
    lat = maybe_number(latitude)
    lon = maybe_number(longitude)
    if lat is None or lon is None:
        return ""
    return f"{lat:.3f},{lon:.3f}"


def compact_count_items(value: Any, *, limit: int = 6) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("value"))
        count = maybe_nonnegative_int(item.get("count"))
        if not label or count is None or count <= 0:
            continue
        items.append({"value": label, "count": count})
        if len(items) >= limit:
            break
    return items


def compact_distribution_summary(distribution_obj: Any, *, limit: int = 6) -> dict[str, Any] | None:
    if not isinstance(distribution_obj, dict):
        return None
    compacted: dict[str, Any] = {}
    signal_count = maybe_nonnegative_int(distribution_obj.get("signal_count"))
    if signal_count is not None:
        compacted["signal_count"] = signal_count
    for field_name in ("distinct_day_count", "distinct_source_skill_count", "distinct_point_count"):
        count = maybe_nonnegative_int(distribution_obj.get(field_name))
        if count is not None:
            compacted[field_name] = count
    for field_name in ("time_bucket_counts", "source_skill_counts", "metric_counts", "point_bucket_counts"):
        items = compact_count_items(distribution_obj.get(field_name), limit=limit)
        if items:
            compacted[field_name] = items
    return compacted or None


def compact_statistics(statistics_obj: Any) -> dict[str, Any] | None:
    if not isinstance(statistics_obj, dict):
        return None
    compacted: dict[str, Any] = {}
    sample_count = maybe_nonnegative_int(statistics_obj.get("sample_count"))
    if sample_count is not None:
        compacted["sample_count"] = sample_count
    for key in ("min", "p05", "p25", "mean", "median", "p75", "p95", "max", "stddev"):
        value = maybe_number(statistics_obj.get(key))
        compacted[key] = round(value, 3) if value is not None else None
    if all(value is None for key, value in compacted.items() if key != "sample_count") and "sample_count" not in compacted:
        return None
    return compacted


def previous_round_id(run_dir: Path, round_id: str) -> str | None:
    components = parse_round_components(round_id)
    if components is None:
        candidates = [item for item in discover_round_ids(run_dir) if item < round_id]
        return candidates[-1] if candidates else None
    prefix, number, _width = components
    candidates: list[str] = []
    for item in discover_round_ids(run_dir):
        item_components = parse_round_components(item)
        if item_components is None:
            continue
        item_prefix, item_number, _item_width = item_components
        if item_prefix == prefix and item_number < number:
            candidates.append(item)
    return candidates[-1] if candidates else None
def mission_run_id(mission: dict[str, Any]) -> str:
    run_id = mission.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("mission.json missing run_id.")
    return run_id


def mission_window(mission: dict[str, Any]) -> dict[str, str]:
    window = mission.get("window")
    if not isinstance(window, dict):
        raise ValueError("mission.json missing window.")
    start_utc = maybe_text(window.get("start_utc"))
    end_utc = maybe_text(window.get("end_utc"))
    if not start_utc or not end_utc:
        raise ValueError("mission.json window is incomplete.")
    return {"start_utc": start_utc, "end_utc": end_utc}


def mission_place_scope(mission: dict[str, Any]) -> dict[str, Any]:
    region = mission.get("region")
    if not isinstance(region, dict):
        raise ValueError("mission.json missing region.")
    label = maybe_text(region.get("label")) or "Mission region"
    geometry = region.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError("mission.json region.geometry must be an object.")
    return {"label": label, "geometry": geometry}


def mission_constraints(mission: dict[str, Any]) -> dict[str, int]:
    values = contract_effective_constraints(mission)
    if isinstance(values, dict):
        return {key: int(value) for key, value in values.items() if isinstance(value, int) and value > 0}
    constraints = mission.get("constraints")
    if not isinstance(constraints, dict):
        return {}
    output: dict[str, int] = {}
    for key in (
        "max_rounds",
        "max_claims_per_round",
        "max_tasks_per_round",
        "claim_target_per_round",
        "claim_hard_cap_per_round",
    ):
        value = constraints.get(key)
        if isinstance(value, int) and value > 0:
            output[key] = value
    return output


def mission_region_tokens(mission: dict[str, Any]) -> list[str]:
    return text_tokens(mission_place_scope(mission).get("label"), minimum_length=3)


def mission_topic_tokens(mission: dict[str, Any]) -> list[str]:
    ignored = set(mission_region_tokens(mission))
    values: list[Any] = [mission.get("topic"), mission.get("objective")]
    hypotheses = mission.get("hypotheses")
    if isinstance(hypotheses, list):
        values.extend(item for item in hypotheses if item is not None)
    tokens: list[str] = []
    seen: set[str] = set()
    for value in values:
        for token in text_tokens(value, minimum_length=4):
            if token in ignored or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
    return tokens


def top_counter_items(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
    return items


def sorted_counter_items(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key in sorted(counter):
        count = counter.get(key, 0)
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
        if len(items) >= limit:
            break
    return items


def top_counter_text(counter: Counter[str], limit: int = 3) -> str:
    parts = [f"{item['value']} ({item['count']})" for item in top_counter_items(counter, limit=limit)]
    return ", ".join(parts)


def default_public_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "public_signals.sqlite"


def default_environment_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "environment_signals.sqlite"


def run_manifest_path(run_dir: Path) -> Path:
    return run_dir / "run_manifest.json"


def load_or_build_manifest(run_dir: Path, mission: dict[str, Any]) -> dict[str, Any]:
    manifest_file = run_manifest_path(run_dir)
    if manifest_file.exists():
        payload = read_json(manifest_file)
        if isinstance(payload, dict):
            return payload
    return {
        "run_id": mission_run_id(mission),
        "run_dir": str(run_dir),
        "analytics_backend": "sqlite",
        "databases": {
            "public_signals": str(default_public_db_path(run_dir)),
            "environment_signals": str(default_environment_db_path(run_dir)),
        },
    }


def load_ddl(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def init_sqlite_db(path: Path, ddl_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ddl = load_ddl(ddl_path)
    with sqlite3.connect(path) as conn:
        conn.executescript(ddl)
        conn.commit()


def emit_row_id(prefix: str, index: int) -> str:
    return f"{prefix}-{index:03d}"


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
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
def insert_many(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    data = list(rows)
    if not data:
        return
    conn.executemany(sql, data)
    conn.commit()


def parse_input_specs(values: list[str]) -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Invalid --input value {raw!r}. Use source-skill=/path/to/artifact.")
        source_skill, path_text = raw.split("=", 1)
        source_skill = source_skill.strip()
        path_text = path_text.strip()
        if not source_skill or not path_text:
            raise ValueError(f"Invalid --input value {raw!r}.")
        path = Path(path_text).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"Input artifact does not exist: {path}")
        parsed.append((source_skill, path))
    return parsed


def consensus_nonempty_text(values: list[Any]) -> str:
    unique = unique_strings([maybe_text(value) for value in values if maybe_text(value)])
    if len(unique) == 1:
        return unique[0]
    return ""


def enrich_component_roles_with_candidate_tags(
    component_roles: list[dict[str, Any]],
    *,
    candidate_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for component in component_roles:
        if not isinstance(component, dict):
            continue
        item = copy.deepcopy(component)
        candidate = candidate_lookup.get(maybe_text(component.get("candidate_observation_id")), {})
        if isinstance(candidate, dict):
            if not maybe_text(item.get("hypothesis_id")) and maybe_text(candidate.get("hypothesis_id")):
                item["hypothesis_id"] = maybe_text(candidate.get("hypothesis_id"))
            if not maybe_text(item.get("leg_id")) and maybe_text(candidate.get("leg_id")):
                item["leg_id"] = maybe_text(candidate.get("leg_id"))
        enriched.append(item)
    return enriched


def normalize_public_source(
    source_skill: str,
    path: Path,
    *,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    return application_normalize_public_source(
        source_skill,
        path,
        run_id=run_id,
        round_id=round_id,
        mission_scope=mission_place_scope(mission),
        mission_region_tokens=mission_region_tokens(mission),
        mission_topic_tokens=mission_topic_tokens(mission),
        mission_topic=maybe_text(mission.get("topic")),
    )


def normalize_public_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], str]:
    return application_normalize_public_source_cached(
        run_dir=run_dir,
        source_skill=source_skill,
        path=path,
        run_id=run_id,
        round_id=round_id,
        mission_scope=mission_place_scope(mission),
        mission_region_tokens=mission_region_tokens(mission),
        mission_topic_tokens=mission_topic_tokens(mission),
        mission_topic=maybe_text(mission.get("topic")),
    )


def build_compact_audit(
    *,
    total_candidate_count: int,
    retained_count: int,
    coverage_summary: str,
    coverage_dimensions: list[str] | None = None,
    missing_dimensions: list[str] | None = None,
    concentration_flags: list[str] | None = None,
    sampling_notes: list[str] | None = None,
) -> dict[str, Any]:
    normalized_coverage_dimensions = unique_strings(
        [maybe_text(item) for item in (coverage_dimensions or []) if maybe_text(item)]
    )
    normalized_missing_dimensions = unique_strings(
        [maybe_text(item) for item in (missing_dimensions or []) if maybe_text(item)]
    )
    normalized_concentration_flags = unique_strings(
        [maybe_text(item) for item in (concentration_flags or []) if maybe_text(item)]
    )
    return {
        "representative": not normalized_concentration_flags and not normalized_missing_dimensions,
        "retained_count": max(0, int(retained_count)),
        "total_candidate_count": max(0, int(total_candidate_count)),
        "coverage_summary": coverage_summary,
        "concentration_flags": normalized_concentration_flags,
        "coverage_dimensions": normalized_coverage_dimensions,
        "missing_dimensions": normalized_missing_dimensions,
        "sampling_notes": unique_strings([maybe_text(item) for item in (sampling_notes or []) if maybe_text(item)]),
    }


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
                f"Signal count retained in summary: {maybe_nonnegative_int(distribution_summary.get('signal_count')) or len(group)}."
                if isinstance(distribution_summary, dict)
                else f"Signal count retained in summary: {len(group)}."
            ),
            f"Distinct rounded points: {len(point_counts)}." if point_counts else "Spatial point spread was unavailable.",
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


def public_signals_to_claims(
    *,
    mission: dict[str, Any],
    round_id: str,
    signals: list[dict[str, Any]],
    max_claims: int,
) -> list[dict[str, Any]]:
    run_id = mission_run_id(mission)
    mission_scope = mission_place_scope(mission)
    mission_time_window = mission_window(mission)
    investigation_plan = build_investigation_plan(mission=mission, round_id=round_id)
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
        fingerprint = semantic_fingerprint(source_text)
        if not fingerprint:
            fingerprint = signal["signal_id"]
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
            "schema_version": SCHEMA_VERSION,
            "claim_id": emit_row_id("claim", index),
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
        validate_payload("claim", claim)
        claims.append(claim)
    return claims


def save_public_db(db_path: Path, signals: list[dict[str, Any]], claims: list[dict[str, Any]]) -> None:
    init_sqlite_db(db_path, PUBLIC_DDL_PATH)
    with sqlite3.connect(db_path) as conn:
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO public_signals (
                signal_id, run_id, round_id, source_skill, signal_kind, external_id, title, text,
                url, author_name, channel_name, language, query_text, published_at_utc,
                captured_at_utc, engagement_json, metadata_json, artifact_path, record_locator,
                sha256, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    signal["signal_id"],
                    signal["run_id"],
                    signal["round_id"],
                    signal["source_skill"],
                    signal["signal_kind"],
                    signal["external_id"],
                    signal["title"],
                    signal["text"],
                    signal["url"],
                    signal["author_name"],
                    signal["channel_name"],
                    signal["language"],
                    signal["query_text"],
                    signal["published_at_utc"],
                    signal["captured_at_utc"],
                    json.dumps(signal.get("engagement", {}), ensure_ascii=True, sort_keys=True),
                    json.dumps(signal.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    signal["artifact_path"],
                    signal["record_locator"],
                    signal["sha256"],
                    json.dumps(signal.get("raw_json"), ensure_ascii=True, sort_keys=True),
                )
                for signal in signals
            ),
        )
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO claim_candidates (
                claim_id, run_id, round_id, claim_type, priority, summary, statement,
                source_signal_ids_json, claim_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    claim["claim_id"],
                    claim["run_id"],
                    claim["round_id"],
                    claim["claim_type"],
                    claim["priority"],
                    claim["summary"],
                    claim["statement"],
                    json.dumps(
                        [ref.get("external_id") or ref.get("record_locator") for ref in claim.get("public_refs", [])],
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                    json.dumps(claim, ensure_ascii=True, sort_keys=True),
                )
                for claim in claims
            ),
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


def normalize_environment_source(
    source_skill: str,
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return application_normalize_environment_source(
        source_skill,
        path,
        run_id=run_id,
        round_id=round_id,
        schema_version=SCHEMA_VERSION,
    )


def normalize_environment_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    return application_normalize_environment_source_cached(
        run_dir=run_dir,
        source_skill=source_skill,
        path=path,
        run_id=run_id,
        round_id=round_id,
        schema_version=SCHEMA_VERSION,
    )


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


def environment_signals_to_observations(
    *,
    mission: dict[str, Any],
    round_id: str,
    signals: list[dict[str, Any]],
    extra_observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    run_id = mission_run_id(mission)
    mission_scope = mission_place_scope(mission)
    mission_time_window = mission_window(mission)
    investigation_plan = build_investigation_plan(mission=mission, round_id=round_id)
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
            time_window = {"start_utc": start_utc or mission_time_window["start_utc"], "end_utc": end_utc or mission_time_window["end_utc"]}
        quality_flags = sorted({flag for signal in group for flag in signal.get("quality_flags", [])})
        observation = {
            "schema_version": SCHEMA_VERSION,
            "observation_id": emit_row_id("obs", counter),
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
        validate_payload("observation", observation)
        observations.append(observation)
        counter += 1

    for item in extra_observations:
        item["observation_id"] = emit_row_id("obs", counter)
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
        validate_payload("observation", item)
        observations.append(item)
        counter += 1
    return observations


def save_environment_db(db_path: Path, signals: list[dict[str, Any]], observations: list[dict[str, Any]]) -> None:
    init_sqlite_db(db_path, ENVIRONMENT_DDL_PATH)
    with sqlite3.connect(db_path) as conn:
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO environment_signals (
                signal_id, run_id, round_id, source_skill, signal_kind, metric, value, unit,
                observed_at_utc, window_start_utc, window_end_utc, latitude, longitude,
                bbox_json, quality_flags_json, metadata_json, artifact_path, record_locator,
                sha256, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    signal["signal_id"],
                    signal["run_id"],
                    signal["round_id"],
                    signal["source_skill"],
                    signal["signal_kind"],
                    signal["metric"],
                    signal["value"],
                    signal["unit"],
                    signal["observed_at_utc"],
                    signal["window_start_utc"],
                    signal["window_end_utc"],
                    signal["latitude"],
                    signal["longitude"],
                    json.dumps(signal.get("bbox"), ensure_ascii=True, sort_keys=True) if signal.get("bbox") is not None else None,
                    json.dumps(signal.get("quality_flags", []), ensure_ascii=True, sort_keys=True),
                    json.dumps(signal.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    signal["artifact_path"],
                    signal["record_locator"],
                    signal["sha256"],
                    json.dumps(signal.get("raw_json"), ensure_ascii=True, sort_keys=True),
                )
                for signal in signals
            ),
        )
        insert_many(
            conn,
            """
            INSERT OR REPLACE INTO observation_summaries (
                observation_id, run_id, round_id, metric, source_skill, observation_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    observation["observation_id"],
                    observation["run_id"],
                    observation["round_id"],
                    observation["metric"],
                    observation["source_skill"],
                    json.dumps(observation, ensure_ascii=True, sort_keys=True),
                )
                for observation in observations
            ),
        )
def load_object_if_exists(path: Path) -> dict[str, Any] | None:
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return payload
    return None


def append_library_events(run_dir: Path, round_id: str, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    ledger_path = evidence_library_ledger_path(run_dir, round_id)
    existing = read_jsonl(ledger_path) if ledger_path.exists() else []
    entries = [item for item in existing if isinstance(item, dict)]
    for event in events:
        object_kind = maybe_text(event.get("object_kind"))
        payload = event.get("payload")
        if not object_kind or not isinstance(payload, dict):
            continue
        entries.append(
            {
                "recorded_at_utc": utc_now_iso(),
                "object_kind": object_kind,
                "payload": payload,
            }
        )
    write_jsonl(ledger_path, entries)


def merge_unique_items(*groups: list[dict[str, Any]], key_fn: Any) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for group in groups:
        for item in group:
            if not isinstance(item, dict):
                continue
            key = maybe_text(key_fn(item))
            if not key:
                key = stable_hash(stable_json(item))
            if key not in merged:
                ordered_keys.append(key)
            merged[key] = item
    return [merged[key] for key in ordered_keys]


def previous_active_list(run_dir: Path, round_id: str, path_fn: Any) -> list[dict[str, Any]]:
    prior_round = previous_round_id(run_dir, round_id)
    if prior_round is None:
        return []
    return load_canonical_list(path_fn(run_dir, prior_round))


def merge_claim_submissions(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("submission_id") or item.get("claim_id"))


def merge_observation_submissions(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(
        *groups,
        key_fn=lambda item: stable_hash(stable_json(observation_signature_payload(item))),
    )


def merge_evidence_cards(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("evidence_id"))


def merge_isolated_entries(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("isolated_id"))


def merge_remand_entries(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("remand_id"))


def observation_signature_payload(observation: dict[str, Any]) -> dict[str, Any]:
    provenance = observation.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    return {
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "observation_mode": maybe_text(observation.get("observation_mode")),
        "evidence_role": maybe_text(observation.get("evidence_role")),
        "hypothesis_id": maybe_text(observation.get("hypothesis_id")),
        "leg_id": maybe_text(observation.get("leg_id")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": observation.get("statistics"),
        "distribution_summary": observation.get("distribution_summary"),
        "time_window": observation.get("time_window"),
        "place_scope": observation.get("place_scope"),
        "source_skills": sorted(maybe_text(item) for item in observation.get("source_skills", []) if maybe_text(item)),
        "metric_bundle": sorted(maybe_text(item) for item in observation.get("metric_bundle", []) if maybe_text(item)),
        "candidate_observation_ids": sorted(
            maybe_text(item) for item in observation.get("candidate_observation_ids", []) if maybe_text(item)
        ),
        "provenance_refs": sorted(
            stable_hash(stable_json(item))
            for item in observation.get("provenance_refs", [])
            if isinstance(item, dict)
        ),
        "component_roles": observation.get("component_roles"),
        "quality_flags": sorted(
            maybe_text(item) for item in observation.get("quality_flags", []) if maybe_text(item)
        ),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }


def shared_observation_id(observation: dict[str, Any]) -> str:
    signature = stable_hash(stable_json(observation_signature_payload(observation)))
    return f"obs-{signature[:12]}"


def materialize_shared_observation(observation: dict[str, Any]) -> dict[str, Any]:
    item = dict(observation)
    item["observation_id"] = shared_observation_id(observation)
    return item


def observation_submission_id(observation_id: str) -> str:
    return f"obssub-{maybe_text(observation_id)}"


def merge_effective_observations(*observation_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged_by_signature: dict[str, dict[str, Any]] = {}
    ordered_signatures: list[str] = []
    for group in observation_groups:
        for observation in group:
            signature_payload = observation_signature_payload(observation)
            signature = stable_hash(stable_json(signature_payload))
            if signature not in merged_by_signature:
                ordered_signatures.append(signature)
            merged_by_signature[signature] = materialize_shared_observation(observation)
    return [merged_by_signature[signature] for signature in ordered_signatures]


def effective_shared_observations(
    run_dir: Path,
    round_id: str,
    *,
    current_round_observations: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    inherited: list[dict[str, Any]] = []
    prior_round_id = previous_round_id(run_dir, round_id)
    if prior_round_id is not None:
        inherited = effective_shared_observations(run_dir, prior_round_id)
    current = (
        current_round_observations
        if current_round_observations is not None
        else load_canonical_list(shared_observations_path(run_dir, round_id))
    )
    return merge_effective_observations(inherited, current)


def merge_effective_claims(*claim_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*claim_groups, key_fn=lambda item: item.get("claim_id"))


def effective_shared_claims(
    run_dir: Path,
    round_id: str,
    *,
    current_round_claims: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    inherited: list[dict[str, Any]] = []
    prior_round_id = previous_round_id(run_dir, round_id)
    if prior_round_id is not None:
        inherited = effective_shared_claims(run_dir, prior_round_id)
    current = (
        current_round_claims
        if current_round_claims is not None
        else load_canonical_list(shared_claims_path(run_dir, round_id))
    )
    return merge_effective_claims(inherited, current)


def active_library_list(run_dir: Path, round_id: str, path_fn: Any) -> list[dict[str, Any]]:
    current_path = path_fn(run_dir, round_id)
    if current_path.exists():
        current = load_canonical_list(current_path)
        if current:
            return current
    return previous_active_list(run_dir, round_id, path_fn)


def auditable_submission_list(current: Any, active: Any) -> list[dict[str, Any]]:
    if isinstance(active, list) and active:
        return [item for item in active if isinstance(item, dict)]
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    return []


def observation_match_key(item: dict[str, Any], *, include_investigation_tags: bool = True) -> str:
    provenance = item.get("provenance") if isinstance(item.get("provenance"), dict) else {}
    payload = {
        "source_skill": maybe_text(item.get("source_skill")),
        "metric": maybe_text(item.get("metric")),
        "aggregation": maybe_text(item.get("aggregation")),
        "observation_mode": maybe_text(item.get("observation_mode")),
        "evidence_role": maybe_text(item.get("evidence_role")),
        "value": item.get("value"),
        "unit": maybe_text(item.get("unit")),
        "time_window": item.get("time_window"),
        "place_scope": item.get("place_scope"),
        "source_skills": sorted(maybe_text(value) for value in item.get("source_skills", []) if maybe_text(value)),
        "metric_bundle": sorted(maybe_text(value) for value in item.get("metric_bundle", []) if maybe_text(value)),
        "candidate_observation_ids": sorted(
            maybe_text(value) for value in item.get("candidate_observation_ids", []) if maybe_text(value)
        ),
        "quality_flags": sorted(maybe_text(flag) for flag in item.get("quality_flags", []) if maybe_text(flag)),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }
    if include_investigation_tags:
        payload["hypothesis_id"] = maybe_text(item.get("hypothesis_id"))
        payload["leg_id"] = maybe_text(item.get("leg_id"))
        payload["component_roles"] = item.get("component_roles")
    return stable_hash(stable_json(payload))


def hydrate_observation_submissions_with_observations(
    submissions: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    observations_by_id = {
        maybe_text(item.get("observation_id")): item
        for item in observations
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    observations_by_key = {
        observation_match_key(item): item
        for item in observations
        if isinstance(item, dict)
    }
    observations_by_legacy_key = {
        observation_match_key(item, include_investigation_tags=False): item
        for item in observations
        if isinstance(item, dict)
    }
    hydrated: list[dict[str, Any]] = []
    for submission in submissions:
        if not isinstance(submission, dict):
            continue
        item = dict(submission)
        observation = observations_by_id.get(maybe_text(item.get("observation_id")))
        if observation is None:
            observation = observations_by_key.get(observation_match_key(item))
        if observation is None:
            observation = observations_by_legacy_key.get(
                observation_match_key(item, include_investigation_tags=False)
            )
        if observation is not None:
            canonical_observation_id = maybe_text(observation.get("observation_id"))
            if canonical_observation_id and maybe_text(item.get("observation_id")) != canonical_observation_id:
                item["observation_id"] = canonical_observation_id
                item["submission_id"] = observation_submission_id(canonical_observation_id)
            if not isinstance(item.get("statistics"), dict) and isinstance(observation.get("statistics"), dict):
                item["statistics"] = observation.get("statistics")
            if not isinstance(item.get("distribution_summary"), dict) and isinstance(observation.get("distribution_summary"), dict):
                item["distribution_summary"] = observation.get("distribution_summary")
            if not isinstance(item.get("time_window"), dict) and isinstance(observation.get("time_window"), dict):
                item["time_window"] = observation.get("time_window")
            if not maybe_text(item.get("unit")) and maybe_text(observation.get("unit")):
                item["unit"] = maybe_text(observation.get("unit"))
            if not maybe_text(item.get("hypothesis_id")) and maybe_text(observation.get("hypothesis_id")):
                item["hypothesis_id"] = maybe_text(observation.get("hypothesis_id"))
            if not maybe_text(item.get("leg_id")) and maybe_text(observation.get("leg_id")):
                item["leg_id"] = maybe_text(observation.get("leg_id"))
            if not isinstance(item.get("component_roles"), list) and isinstance(observation.get("component_roles"), list):
                item["component_roles"] = copy.deepcopy(observation.get("component_roles"))
        hydrated.append(item)
    return hydrated


def library_state(run_dir: Path, round_id: str) -> dict[str, Any]:
    shared_observations = effective_shared_observations(run_dir, round_id)
    shared_claims = effective_shared_claims(run_dir, round_id)
    claim_submissions_current = load_canonical_list(claim_submissions_path(run_dir, round_id))
    observation_submissions_current = hydrate_observation_submissions_with_observations(
        load_canonical_list(observation_submissions_path(run_dir, round_id)),
        shared_observations,
    )
    claims_active = active_library_list(run_dir, round_id, claims_active_path)
    observations_active = hydrate_observation_submissions_with_observations(
        active_library_list(run_dir, round_id, observations_active_path),
        shared_observations,
    )
    return {
        "claim_candidates_current": load_canonical_list(claim_candidates_path(run_dir, round_id)),
        "observation_candidates_current": load_canonical_list(observation_candidates_path(run_dir, round_id)),
        "claim_curation": load_object_if_exists(claim_curation_path(run_dir, round_id)) or {},
        "observation_curation": load_object_if_exists(observation_curation_path(run_dir, round_id)) or {},
        "shared_claims": shared_claims,
        "shared_observations": shared_observations,
        "claim_submissions_current": claim_submissions_current,
        "observation_submissions_current": observation_submissions_current,
        "claim_submissions_auditable": auditable_submission_list(claim_submissions_current, claims_active),
        "observation_submissions_auditable": auditable_submission_list(observation_submissions_current, observations_active),
        "claims_active": claims_active,
        "observations_active": observations_active,
        "cards_active": active_library_list(run_dir, round_id, cards_active_path),
        "isolated_active": active_library_list(run_dir, round_id, isolated_active_path),
        "remands_open": active_library_list(run_dir, round_id, remands_open_path),
        "matching_result": load_object_if_exists(matching_result_path(run_dir, round_id)) or {},
        "evidence_adjudication": load_object_if_exists(evidence_adjudication_path(run_dir, round_id)) or {},
        "matching_authorization": load_object_if_exists(matching_authorization_path(run_dir, round_id)) or {},
        "readiness_reports": {
            "sociologist": load_object_if_exists(data_readiness_report_path(run_dir, round_id, "sociologist")) or {},
            "environmentalist": load_object_if_exists(data_readiness_report_path(run_dir, round_id, "environmentalist")) or {},
        },
    }


def compact_task(task: dict[str, Any]) -> dict[str, Any]:
    inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
    evidence_requirements = inputs.get("evidence_requirements") if isinstance(inputs.get("evidence_requirements"), list) else []
    return {
        "task_id": maybe_text(task.get("task_id")),
        "assigned_role": maybe_text(task.get("assigned_role")),
        "objective": truncate_text(maybe_text(task.get("objective")), 180),
        "status": maybe_text(task.get("status")),
        "evidence_requirements": [
            maybe_text(item.get("requirement_type"))
            for item in evidence_requirements
            if isinstance(item, dict) and maybe_text(item.get("requirement_type"))
        ][:3],
    }


def claim_source_skills(claim: dict[str, Any]) -> list[str]:
    refs = claim.get("public_refs")
    if not isinstance(refs, list):
        return []
    return sorted(
        {
            maybe_text(ref.get("source_skill"))
            for ref in refs
            if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
        }
    )


def compact_claim(claim: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": truncate_text(maybe_text(claim.get("summary")), 180),
        "priority": claim.get("priority"),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "public_source_skills": claim_source_skills(claim),
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
    return {
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


def observation_severity(observation: dict[str, Any]) -> float:
    statistics_obj = compact_statistics(observation.get("statistics"))
    if isinstance(statistics_obj, dict):
        for key in ("max", "p95", "mean", "min"):
            value = maybe_number(statistics_obj.get(key))
            if value is not None:
                return value
    value = maybe_number(observation.get("value"))
    return value if value is not None else 0.0


def observation_family_priority_index(observation: dict[str, Any], family_order: list[str]) -> int:
    family = observation_metric_family(observation.get("metric"))
    if family in family_order:
        return family_order.index(family)
    return len(family_order)


def representative_observation_order(observations: list[dict[str, Any]], claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    family_order = claim_priority_metric_families(claims)
    candidates = sorted(
        observations,
        key=lambda item: (
            observation_family_priority_index(item, family_order),
            -observation_severity(item),
            maybe_text(item.get("source_skill")),
            maybe_text(item.get("metric")),
            maybe_text(item.get("observation_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        for candidate in candidates:
            observation_id = maybe_text(candidate.get("observation_id"))
            if not observation_id or observation_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(observation_id)
            return

    for family in family_order:
        take_first(lambda item, family=family: observation_metric_family(item.get("metric")) == family)
    for source_skill in unique_strings([maybe_text(item.get("source_skill")) for item in candidates]):
        take_first(lambda item, source_skill=source_skill: maybe_text(item.get("source_skill")) == source_skill)
    for metric in unique_strings([maybe_text(item.get("metric")) for item in candidates]):
        take_first(lambda item, metric=metric: maybe_text(item.get("metric")) == metric)
    for candidate in candidates:
        observation_id = maybe_text(candidate.get("observation_id"))
        if not observation_id or observation_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(observation_id)
    return selected


def ordered_context_observations(
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    claims: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    by_id = {maybe_text(item.get("observation_id")): item for item in observations}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for card in evidence_cards:
        ids = card.get("observation_ids")
        if not isinstance(ids, list):
            continue
        for observation_id in ids:
            key = maybe_text(observation_id)
            if not key or key in seen or key not in by_id:
                continue
            ordered.append(by_id[key])
            seen.add(key)
    remaining = [observation for observation in representative_observation_order(observations, claims or []) if maybe_text(observation.get("observation_id")) not in seen]
    for observation in remaining:
        key = maybe_text(observation.get("observation_id"))
        if not key or key in seen:
            continue
        ordered.append(observation)
        seen.add(key)
    return ordered


def representative_claim_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        submissions,
        key=lambda item: (
            -int(item.get("source_signal_count") or 0),
            maybe_text(item.get("claim_type")),
            maybe_text(item.get("submission_id")),
        ),
    )


def representative_observation_submissions(submissions: list[dict[str, Any]], claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations = observations_from_submissions(submissions)
    ordered_observations = representative_observation_order(observations, claims)
    by_id = {maybe_text(item.get("observation_id")): item for item in submissions}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for observation in ordered_observations:
        observation_id = maybe_text(observation.get("observation_id"))
        submission = by_id.get(observation_id)
        if observation_id and observation_id not in seen and submission is not None:
            ordered.append(submission)
            seen.add(observation_id)
    for submission in submissions:
        observation_id = maybe_text(submission.get("observation_id"))
        if not observation_id or observation_id in seen:
            continue
        ordered.append(submission)
        seen.add(observation_id)
    return ordered


def build_public_signal_summary(signals: list[dict[str, Any]], claims: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at_utc": utc_now_iso(),
        "signal_count": len(signals),
        "claim_count": len(claims),
        "source_skill_counts": dict(Counter(maybe_text(item.get("source_skill")) for item in signals)),
        "signal_kind_counts": dict(Counter(maybe_text(item.get("signal_kind")) for item in signals)),
        "top_signals": [
            {
                "signal_id": maybe_text(item.get("signal_id")),
                "source_skill": maybe_text(item.get("source_skill")),
                "title": truncate_text(maybe_text(item.get("title")), 120),
                "published_at_utc": maybe_text(item.get("published_at_utc")),
            }
            for item in signals[:5]
        ],
        "claims": [compact_claim(item) for item in claims[:MAX_CONTEXT_CLAIMS]],
    }


def build_environment_signal_summary(signals: list[dict[str, Any]], observations: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at_utc": utc_now_iso(),
        "signal_count": len(signals),
        "observation_count": len(observations),
        "source_skill_counts": dict(Counter(maybe_text(item.get("source_skill")) for item in signals)),
        "metric_counts": dict(Counter(maybe_text(item.get("metric")) for item in signals)),
        "top_observations": [
            compact_observation(item)
            for item in representative_observation_order(observations, [])[:MAX_CONTEXT_OBSERVATIONS]
        ],
    }


def claim_submission_from_claim(claim: dict[str, Any]) -> dict[str, Any]:
    submission = {
        "schema_version": SCHEMA_VERSION,
        "submission_id": f"claimsub-{maybe_text(claim.get('claim_id'))}",
        "run_id": maybe_text(claim.get("run_id")),
        "round_id": maybe_text(claim.get("round_id")),
        "agent_role": "sociologist",
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": maybe_text(claim.get("summary")),
        "statement": maybe_text(claim.get("statement")),
        "meaning": (
            f"This public-side claim captures the mission-relevant narrative for {maybe_text(claim.get('claim_type')) or 'the current event'}."
        ),
        "priority": int(claim.get("priority") or 1),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "worth_storing": True,
        "source_signal_count": int(claim.get("source_signal_count") or max(1, len(claim.get("public_refs", [])))),
        "time_window": copy.deepcopy(claim.get("time_window")) if isinstance(claim.get("time_window"), dict) else claim.get("time_window"),
        "place_scope": copy.deepcopy(claim.get("place_scope")) if isinstance(claim.get("place_scope"), dict) else claim.get("place_scope"),
        "public_refs": claim.get("public_refs", []),
        "compact_audit": claim.get("compact_audit")
        if isinstance(claim.get("compact_audit"), dict)
        else build_compact_audit(
            total_candidate_count=max(1, len(claim.get("public_refs", []))),
            retained_count=min(max(1, len(claim.get("public_refs", []))), 8),
            coverage_summary="Derived claim was materialized into a library submission without an explicit compact audit.",
            concentration_flags=[],
            sampling_notes=[],
        ),
    }
    if isinstance(claim.get("claim_scope"), dict):
        submission["claim_scope"] = copy.deepcopy(claim.get("claim_scope"))
    hypothesis_id = maybe_text(claim.get("hypothesis_id"))
    if hypothesis_id:
        submission["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(claim.get("leg_id"))
    if leg_id:
        submission["leg_id"] = leg_id
    validate_payload("claim-submission", submission)
    return submission


def observation_submission_from_observation(observation: dict[str, Any]) -> dict[str, Any]:
    canonical_observation = materialize_shared_observation(observation)
    observation_id = maybe_text(canonical_observation.get("observation_id"))
    submission = {
        "schema_version": SCHEMA_VERSION,
        "submission_id": observation_submission_id(observation_id),
        "run_id": maybe_text(canonical_observation.get("run_id")),
        "round_id": maybe_text(canonical_observation.get("round_id")),
        "agent_role": "environmentalist",
        "observation_id": observation_id,
        "source_skill": maybe_text(canonical_observation.get("source_skill")),
        "metric": maybe_text(canonical_observation.get("metric")),
        "aggregation": maybe_text(canonical_observation.get("aggregation")),
        "value": canonical_observation.get("value"),
        "unit": maybe_text(canonical_observation.get("unit")),
        "meaning": (
            f"This observation records mission-window physical evidence for metric {maybe_text(canonical_observation.get('metric'))}."
        ),
        "worth_storing": True,
        "time_window": canonical_observation.get("time_window"),
        "place_scope": canonical_observation.get("place_scope"),
        "quality_flags": canonical_observation.get("quality_flags", []),
        "provenance": canonical_observation.get("provenance"),
        "statistics": canonical_observation.get("statistics"),
        "compact_audit": canonical_observation.get("compact_audit")
        if isinstance(canonical_observation.get("compact_audit"), dict)
        else build_compact_audit(
            total_candidate_count=1,
            retained_count=1,
            coverage_summary="Derived observation was materialized into a library submission without an explicit compact audit.",
            concentration_flags=[],
            sampling_notes=[],
        ),
    }
    for field_name in (
        "observation_mode",
        "evidence_role",
        "hypothesis_id",
        "leg_id",
        "source_skills",
        "metric_bundle",
        "candidate_observation_ids",
        "provenance_refs",
        "component_roles",
        "distribution_summary",
    ):
        value = canonical_observation.get(field_name)
        if value is None:
            continue
        submission[field_name] = copy.deepcopy(value) if isinstance(value, (dict, list)) else value
    validate_payload("observation-submission", submission)
    return submission


def claims_from_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for index, submission in enumerate(submissions, start=1):
        claim = {
            "schema_version": SCHEMA_VERSION,
            "claim_id": maybe_text(submission.get("claim_id")) or emit_row_id("claim", index),
            "run_id": maybe_text(submission.get("run_id")),
            "round_id": maybe_text(submission.get("round_id")),
            "agent_role": "sociologist",
            "claim_type": maybe_text(submission.get("claim_type")),
            "status": "selected",
            "summary": maybe_text(submission.get("summary")),
            "statement": maybe_text(submission.get("statement")),
            "priority": int(submission.get("priority") or 1),
            "needs_physical_validation": bool(submission.get("needs_physical_validation")),
            "time_window": copy.deepcopy(submission.get("time_window")) if isinstance(submission.get("time_window"), dict) else submission.get("time_window"),
            "place_scope": copy.deepcopy(submission.get("place_scope")) if isinstance(submission.get("place_scope"), dict) else submission.get("place_scope"),
            "public_refs": submission.get("public_refs", []),
        }
        if isinstance(submission.get("claim_scope"), dict):
            claim["claim_scope"] = copy.deepcopy(submission.get("claim_scope"))
        hypothesis_id = maybe_text(submission.get("hypothesis_id"))
        if hypothesis_id:
            claim["hypothesis_id"] = hypothesis_id
        leg_id = maybe_text(submission.get("leg_id"))
        if leg_id:
            claim["leg_id"] = leg_id
        candidate_claim_ids = submission.get("candidate_claim_ids")
        if isinstance(candidate_claim_ids, list) and candidate_claim_ids:
            claim["candidate_claim_ids"] = [maybe_text(item) for item in candidate_claim_ids if maybe_text(item)]
        selection_reason = maybe_text(submission.get("selection_reason"))
        if selection_reason:
            claim["selection_reason"] = selection_reason
        validate_payload("claim", claim)
        claims.append(claim)
    return claims


def observations_from_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for index, submission in enumerate(submissions, start=1):
        observation = {
            "schema_version": SCHEMA_VERSION,
            "observation_id": maybe_text(submission.get("observation_id")) or emit_row_id("obs", index),
            "run_id": maybe_text(submission.get("run_id")),
            "round_id": maybe_text(submission.get("round_id")),
            "agent_role": "environmentalist",
            "source_skill": maybe_text(submission.get("source_skill")),
            "metric": maybe_text(submission.get("metric")),
            "aggregation": maybe_text(submission.get("aggregation")),
            "value": submission.get("value"),
            "unit": maybe_text(submission.get("unit")),
            "statistics": submission.get("statistics"),
            "time_window": submission.get("time_window"),
            "place_scope": submission.get("place_scope"),
            "quality_flags": submission.get("quality_flags", []),
            "provenance": submission.get("provenance"),
        }
        for field_name in (
            "observation_mode",
            "evidence_role",
            "hypothesis_id",
            "leg_id",
            "source_skills",
            "metric_bundle",
            "candidate_observation_ids",
            "provenance_refs",
            "component_roles",
            "distribution_summary",
        ):
            value = submission.get(field_name)
            if value is not None:
                observation[field_name] = value
        selection_reason = maybe_text(submission.get("selection_reason"))
        if selection_reason:
            observation["selection_reason"] = selection_reason
        validate_payload("observation", observation)
        observations.append(observation)
    return observations


def dedupe_artifact_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        key = stable_hash(stable_json(ref))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(copy.deepcopy(ref))
    return deduped


def merge_time_windows_from_records(records: list[dict[str, Any]], fallback: dict[str, Any]) -> dict[str, Any]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for record in records:
        window = record.get("time_window") if isinstance(record.get("time_window"), dict) else {}
        start_dt = parse_loose_datetime(window.get("start_utc"))
        end_dt = parse_loose_datetime(window.get("end_utc"))
        if start_dt is not None:
            starts.append(start_dt)
        if end_dt is not None:
            ends.append(end_dt)
    if not starts or not ends:
        return copy.deepcopy(fallback)
    return {
        "start_utc": to_rfc3339_z(min(starts)) or fallback.get("start_utc"),
        "end_utc": to_rfc3339_z(max(ends)) or fallback.get("end_utc"),
    }


def derive_place_scope_from_candidate_observations(records: list[dict[str, Any]], fallback: dict[str, Any]) -> dict[str, Any]:
    if not records:
        return copy.deepcopy(fallback)
    points: list[tuple[float, float]] = []
    for record in records:
        place_scope = record.get("place_scope") if isinstance(record.get("place_scope"), dict) else {}
        geometry = place_scope.get("geometry") if isinstance(place_scope.get("geometry"), dict) else {}
        if maybe_text(geometry.get("type")) != "Point":
            return copy.deepcopy(fallback)
        latitude = maybe_number(geometry.get("latitude"))
        longitude = maybe_number(geometry.get("longitude"))
        if latitude is None or longitude is None:
            return copy.deepcopy(fallback)
        points.append((float(latitude), float(longitude)))
    if not points:
        return copy.deepcopy(fallback)
    unique_points = {(round(lat, 3), round(lon, 3)) for lat, lon in points}
    if len(unique_points) != 1:
        return copy.deepcopy(fallback)
    lat = round(statistics.fmean(point[0] for point in points), 6)
    lon = round(statistics.fmean(point[1] for point in points), 6)
    label = maybe_text(((records[0].get("place_scope") or {}).get("label"))) or maybe_text(fallback.get("label")) or "Mission region"
    return {
        "label": label,
        "geometry": {
            "type": "Point",
            "latitude": lat,
            "longitude": lon,
        },
    }


def candidate_claim_source_signal_count(candidate: dict[str, Any]) -> int:
    value = candidate.get("source_signal_count")
    if isinstance(value, int) and value > 0:
        return value
    refs = candidate.get("public_refs")
    if isinstance(refs, list) and refs:
        return len([item for item in refs if isinstance(item, dict)])
    return 1


def compact_audit_from_curated_claim_candidates(
    candidates: list[dict[str, Any]],
    public_refs: list[dict[str, Any]],
) -> dict[str, Any]:
    audits = [item.get("compact_audit") for item in candidates if isinstance(item.get("compact_audit"), dict)]
    concentration_flags = unique_strings(
        [
            maybe_text(flag)
            for audit in audits
            for flag in audit.get("concentration_flags", [])
            if maybe_text(flag)
        ]
    )
    coverage_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("coverage_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    missing_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("missing_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    sampling_notes = unique_strings(
        [
            maybe_text(note)
            for audit in audits
            for note in audit.get("sampling_notes", [])
            if maybe_text(note)
        ]
    )
    total_candidate_count = 0
    for candidate in candidates:
        audit = candidate.get("compact_audit") if isinstance(candidate.get("compact_audit"), dict) else {}
        count = audit.get("total_candidate_count")
        if isinstance(count, int) and count > 0:
            total_candidate_count += count
        else:
            total_candidate_count += candidate_claim_source_signal_count(candidate)
    return build_compact_audit(
        total_candidate_count=max(1, total_candidate_count),
        retained_count=max(1, len(public_refs)),
        coverage_summary=(
            f"Curated this claim from {len(candidates)} candidate claims and retained {len(public_refs)} supporting public references."
        ),
        coverage_dimensions=coverage_dimensions or ["supporting-artifacts", "source-skill", "channel", "time-window"],
        missing_dimensions=missing_dimensions,
        concentration_flags=concentration_flags,
        sampling_notes=sampling_notes,
    )


def compact_audit_from_curated_observation_candidates(
    candidates: list[dict[str, Any]],
    source_skills: list[str],
    metric_bundle: list[str],
) -> dict[str, Any]:
    audits = [item.get("compact_audit") for item in candidates if isinstance(item.get("compact_audit"), dict)]
    concentration_flags = unique_strings(
        [
            maybe_text(flag)
            for audit in audits
            for flag in audit.get("concentration_flags", [])
            if maybe_text(flag)
        ]
    )
    coverage_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("coverage_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    missing_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("missing_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    sampling_notes = unique_strings(
        [
            maybe_text(note)
            for audit in audits
            for note in audit.get("sampling_notes", [])
            if maybe_text(note)
        ]
    )
    total_candidate_count = 0
    for candidate in candidates:
        audit = candidate.get("compact_audit") if isinstance(candidate.get("compact_audit"), dict) else {}
        count = audit.get("total_candidate_count")
        if isinstance(count, int) and count > 0:
            total_candidate_count += count
        else:
            total_candidate_count += distribution_signal_count(candidate)
    return build_compact_audit(
        total_candidate_count=max(1, total_candidate_count),
        retained_count=max(1, len(candidates)),
        coverage_summary=(
            f"Curated this observation from {len(candidates)} candidate observations representing "
            f"{max(1, total_candidate_count)} normalized signals across {len(source_skills) or 1} source skills "
            f"and {len(metric_bundle) or 1} metrics."
        ),
        coverage_dimensions=coverage_dimensions or ["metric-family", "source-skill", "time-window", "distribution-summary", "point-distribution"],
        missing_dimensions=missing_dimensions,
        concentration_flags=concentration_flags,
        sampling_notes=sampling_notes,
    )


def aggregate_candidate_statistics(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    metrics = {
        canonical_environment_metric(candidate.get("metric"))
        for candidate in candidates
        if maybe_text(candidate.get("metric"))
    }
    units = {
        maybe_text(candidate.get("unit"))
        for candidate in candidates
        if maybe_text(candidate.get("unit"))
    }
    if len(metrics) > 1 or len(units) > 1:
        return None
    weighted_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        stats_obj = candidate.get("statistics") if isinstance(candidate.get("statistics"), dict) else {}
        sample_count = maybe_nonnegative_int(stats_obj.get("sample_count")) or 1
        value = maybe_number(candidate.get("value"))
        row = {
            "sample_count": sample_count,
            "min": maybe_number(stats_obj.get("min")),
            "p05": maybe_number(stats_obj.get("p05")),
            "p25": maybe_number(stats_obj.get("p25")),
            "mean": maybe_number(stats_obj.get("mean")),
            "median": maybe_number(stats_obj.get("median")),
            "p75": maybe_number(stats_obj.get("p75")),
            "p95": maybe_number(stats_obj.get("p95")),
            "max": maybe_number(stats_obj.get("max")),
            "stddev": maybe_number(stats_obj.get("stddev")),
        }
        if row["mean"] is None and value is not None:
            row["mean"] = float(value)
        if row["median"] is None:
            row["median"] = row["mean"]
        if row["min"] is None:
            row["min"] = value if value is not None else row["mean"]
        if row["max"] is None:
            row["max"] = value if value is not None else row["mean"]
        if row["p05"] is None:
            row["p05"] = row["min"]
        if row["p25"] is None:
            row["p25"] = row["median"] if row["median"] is not None else row["mean"]
        if row["p75"] is None:
            row["p75"] = row["median"] if row["median"] is not None else row["mean"]
        if row["p95"] is None:
            row["p95"] = row["max"]
        if all(row[field_name] is None for field_name in ("min", "p05", "p25", "mean", "median", "p75", "p95", "max")):
            continue
        weighted_rows.append(row)
    if not weighted_rows:
        return None
    total_sample_count = sum(int(row["sample_count"]) for row in weighted_rows)

    def weighted_average(field_name: str) -> float | None:
        numerator = 0.0
        denominator = 0
        for row in weighted_rows:
            row_value = maybe_number(row.get(field_name))
            if row_value is None:
                continue
            weight = int(row["sample_count"])
            numerator += row_value * weight
            denominator += weight
        if denominator <= 0:
            return None
        return numerator / denominator

    mean_value = weighted_average("mean")
    stddev_value = None
    if mean_value is not None:
        variance_numerator = 0.0
        variance_denominator = 0
        for row in weighted_rows:
            row_mean = maybe_number(row.get("mean"))
            if row_mean is None:
                continue
            weight = int(row["sample_count"])
            row_stddev = maybe_number(row.get("stddev")) or 0.0
            variance_numerator += weight * ((row_stddev ** 2) + ((row_mean - mean_value) ** 2))
            variance_denominator += weight
        if variance_denominator > 0:
            stddev_value = math.sqrt(variance_numerator / variance_denominator)

    min_value = min(
        maybe_number(row.get("min"))
        for row in weighted_rows
        if maybe_number(row.get("min")) is not None
    )
    max_value = max(
        maybe_number(row.get("max"))
        for row in weighted_rows
        if maybe_number(row.get("max")) is not None
    )
    return compact_statistics(
        {
            "sample_count": total_sample_count,
            "min": min_value,
            "p05": weighted_average("p05"),
            "p25": weighted_average("p25"),
            "mean": mean_value,
            "median": weighted_average("median"),
            "p75": weighted_average("p75"),
            "p95": weighted_average("p95"),
            "max": max_value,
            "stddev": stddev_value,
        }
    )


def merge_count_items(counter: Counter[str], items: Any) -> None:
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("value"))
        count = maybe_nonnegative_int(item.get("count"))
        if not label or count is None or count <= 0:
            continue
        counter[label] += count


def distribution_signal_count(candidate: dict[str, Any]) -> int:
    distribution_summary = candidate.get("distribution_summary") if isinstance(candidate.get("distribution_summary"), dict) else {}
    signal_count = maybe_nonnegative_int(distribution_summary.get("signal_count"))
    if signal_count is not None and signal_count > 0:
        return signal_count
    stats_obj = candidate.get("statistics") if isinstance(candidate.get("statistics"), dict) else {}
    signal_count = maybe_nonnegative_int(stats_obj.get("sample_count"))
    if signal_count is not None and signal_count > 0:
        return signal_count
    compact_audit = candidate.get("compact_audit") if isinstance(candidate.get("compact_audit"), dict) else {}
    signal_count = maybe_nonnegative_int(compact_audit.get("total_candidate_count"))
    if signal_count is not None and signal_count > 0:
        return signal_count
    return 1


def point_bucket_from_scope(scope: Any) -> str:
    if not isinstance(scope, dict):
        return ""
    geometry = scope.get("geometry") if isinstance(scope.get("geometry"), dict) else {}
    if maybe_text(geometry.get("type")) == "Point":
        return point_bucket_key(geometry.get("latitude"), geometry.get("longitude"))
    return maybe_text(scope.get("label"))


def day_bucket_from_time_window(window: Any) -> str:
    if not isinstance(window, dict):
        return ""
    start_utc = maybe_text(window.get("start_utc"))
    if len(start_utc) >= 10:
        return start_utc[:10]
    end_utc = maybe_text(window.get("end_utc"))
    if len(end_utc) >= 10:
        return end_utc[:10]
    return ""


def default_distribution_summary_from_observation(observation: dict[str, Any]) -> dict[str, Any]:
    signal_count = distribution_signal_count(observation)
    day_counter: Counter[str] = Counter()
    day_bucket = day_bucket_from_time_window(observation.get("time_window"))
    if day_bucket:
        day_counter[day_bucket] += signal_count
    source_counter: Counter[str] = Counter()
    source_skill = maybe_text(observation.get("source_skill"))
    if source_skill:
        source_counter[source_skill] += signal_count
    metric_counter: Counter[str] = Counter()
    metric = maybe_text(observation.get("metric"))
    if metric:
        metric_counter[metric] += signal_count
    point_counter: Counter[str] = Counter()
    point_bucket = point_bucket_from_scope(observation.get("place_scope"))
    if point_bucket:
        point_counter[point_bucket] += signal_count
    return {
        "signal_count": signal_count,
        "distinct_day_count": len(day_counter),
        "distinct_source_skill_count": len(source_counter),
        "distinct_point_count": len(point_counter),
        "time_bucket_counts": sorted_counter_items(day_counter, limit=10),
        "source_skill_counts": top_counter_items(source_counter, limit=10),
        "metric_counts": top_counter_items(metric_counter, limit=10),
        "point_bucket_counts": sorted_counter_items(point_counter, limit=10),
    }


def aggregate_candidate_distribution_summary(
    candidates: list[dict[str, Any]],
    source_skills: list[str],
    metric_bundle: list[str],
) -> dict[str, Any] | None:
    if not candidates:
        return None
    total_signal_count = 0
    day_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    metric_counter: Counter[str] = Counter()
    point_counter: Counter[str] = Counter()
    for candidate in candidates:
        signal_count = distribution_signal_count(candidate)
        total_signal_count += signal_count
        distribution_summary = candidate.get("distribution_summary") if isinstance(candidate.get("distribution_summary"), dict) else {}
        if distribution_summary:
            merge_count_items(day_counter, distribution_summary.get("time_bucket_counts"))
            merge_count_items(source_counter, distribution_summary.get("source_skill_counts"))
            merge_count_items(metric_counter, distribution_summary.get("metric_counts"))
            merge_count_items(point_counter, distribution_summary.get("point_bucket_counts"))
            continue
        day_bucket = day_bucket_from_time_window(candidate.get("time_window"))
        if day_bucket:
            day_counter[day_bucket] += signal_count
        source_skill = maybe_text(candidate.get("source_skill"))
        if source_skill:
            source_counter[source_skill] += signal_count
        metric = maybe_text(candidate.get("metric"))
        if metric:
            metric_counter[metric] += signal_count
        point_bucket = point_bucket_from_scope(candidate.get("place_scope"))
        if point_bucket:
            point_counter[point_bucket] += signal_count
    if not source_counter:
        for source_skill in source_skills:
            if source_skill:
                source_counter[source_skill] += 1
    if not metric_counter:
        for metric in metric_bundle:
            if metric:
                metric_counter[metric] += 1
    return {
        "signal_count": max(1, total_signal_count),
        "distinct_day_count": len(day_counter),
        "distinct_source_skill_count": len(source_counter),
        "distinct_point_count": len(point_counter),
        "time_bucket_counts": sorted_counter_items(day_counter, limit=10),
        "source_skill_counts": top_counter_items(source_counter, limit=10),
        "metric_counts": top_counter_items(metric_counter, limit=10),
        "point_bucket_counts": sorted_counter_items(point_counter, limit=10),
    }


def canonical_source_skill_for_curated_observation(entry: dict[str, Any], candidates: list[dict[str, Any]]) -> tuple[str, list[str]]:
    source_skills = unique_strings([maybe_text(item) for item in entry.get("source_skills", []) if maybe_text(item)])
    if not source_skills:
        source_skills = unique_strings([maybe_text(item.get("source_skill")) for item in candidates if maybe_text(item.get("source_skill"))])
    if len(source_skills) == 1:
        return source_skills[0], source_skills
    if source_skills:
        return "composite-curation", source_skills
    fallback = unique_strings([maybe_text(item.get("source_skill")) for item in candidates if maybe_text(item.get("source_skill"))])
    if len(fallback) == 1:
        return fallback[0], fallback
    if fallback:
        return "composite-curation", fallback
    return "curation-derived-observation", []


def materialize_claim_submission_from_curated_entry(
    *,
    entry: dict[str, Any],
    candidate_lookup: dict[str, dict[str, Any]],
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
) -> dict[str, Any]:
    candidate_claim_ids = [maybe_text(item) for item in entry.get("candidate_claim_ids", []) if maybe_text(item)]
    candidates = [candidate_lookup[item] for item in candidate_claim_ids if item in candidate_lookup]
    public_refs = dedupe_artifact_refs(
        [
            ref
            for candidate in candidates
            for ref in candidate.get("public_refs", [])
            if isinstance(ref, dict)
        ]
    )
    compact_audit = compact_audit_from_curated_claim_candidates(candidates, public_refs)
    time_window = (
        copy.deepcopy(entry.get("time_window"))
        if isinstance(entry.get("time_window"), dict)
        else merge_time_windows_from_records(candidates, mission_time_window)
    )
    place_scope = copy.deepcopy(mission_scope)
    if isinstance(entry.get("place_scope"), dict):
        place_scope = copy.deepcopy(entry.get("place_scope"))
    elif candidates:
        candidate_scope = candidates[0].get("place_scope")
        if isinstance(candidate_scope, dict):
            place_scope = copy.deepcopy(candidate_scope)
    claim_scope = (
        copy.deepcopy(entry.get("claim_scope"))
        if isinstance(entry.get("claim_scope"), dict)
        else copy.deepcopy(candidates[0].get("claim_scope"))
        if candidates and isinstance(candidates[0].get("claim_scope"), dict)
        else {
            "time_window": copy.deepcopy(time_window),
            "place_scope": copy.deepcopy(place_scope),
            "time_source": "curation-override" if isinstance(entry.get("time_window"), dict) else "candidate-merged",
            "place_source": "curation-override" if isinstance(entry.get("place_scope"), dict) else "candidate-merged",
            "usable_for_matching": True,
            "notes": [],
        }
    )
    if isinstance(entry.get("time_window"), dict):
        claim_scope["time_window"] = copy.deepcopy(entry.get("time_window"))
        claim_scope["time_source"] = "curation-override"
    elif not isinstance(claim_scope.get("time_window"), dict):
        claim_scope["time_window"] = copy.deepcopy(time_window)
    if isinstance(entry.get("place_scope"), dict):
        claim_scope["place_scope"] = copy.deepcopy(entry.get("place_scope"))
        claim_scope["place_source"] = "curation-override"
    elif not isinstance(claim_scope.get("place_scope"), dict):
        claim_scope["place_scope"] = copy.deepcopy(place_scope)
    if not maybe_text(claim_scope.get("time_source")):
        claim_scope["time_source"] = "candidate-merged"
    if not maybe_text(claim_scope.get("place_source")):
        claim_scope["place_source"] = "candidate-merged"
    claim_scope["usable_for_matching"] = (
        maybe_text(claim_scope.get("time_source")) != "mission-fallback"
        and maybe_text(claim_scope.get("place_source")) != "mission-fallback"
    )
    submission = {
        "schema_version": SCHEMA_VERSION,
        "submission_id": f"claimsub-{maybe_text(entry.get('claim_id'))}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": "sociologist",
        "claim_id": maybe_text(entry.get("claim_id")),
        "claim_type": maybe_text(entry.get("claim_type")),
        "summary": maybe_text(entry.get("summary")),
        "statement": maybe_text(entry.get("statement")),
        "meaning": maybe_text(entry.get("meaning")),
        "priority": int(entry.get("priority") or 1),
        "needs_physical_validation": bool(entry.get("needs_physical_validation")),
        "worth_storing": bool(entry.get("worth_storing")),
        "source_signal_count": max(1, sum(candidate_claim_source_signal_count(item) for item in candidates)),
        "time_window": time_window,
        "place_scope": place_scope,
        "claim_scope": claim_scope,
        "public_refs": public_refs,
        "compact_audit": compact_audit,
        "candidate_claim_ids": candidate_claim_ids,
        "selection_reason": maybe_text(entry.get("selection_reason")),
    }
    hypothesis_id = maybe_text(entry.get("hypothesis_id")) or (
        maybe_text(candidates[0].get("hypothesis_id")) if candidates else ""
    )
    if hypothesis_id:
        submission["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(entry.get("leg_id")) or (
        maybe_text(candidates[0].get("leg_id")) if candidates else ""
    )
    if leg_id:
        submission["leg_id"] = leg_id
    validate_payload("claim-submission", submission)
    return submission


def materialize_observation_submission_from_curated_entry(
    *,
    entry: dict[str, Any],
    candidate_lookup: dict[str, dict[str, Any]],
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
) -> dict[str, Any]:
    candidate_observation_ids = [maybe_text(item) for item in entry.get("candidate_observation_ids", []) if maybe_text(item)]
    candidates = [candidate_lookup[item] for item in candidate_observation_ids if item in candidate_lookup]
    source_skill, source_skills = canonical_source_skill_for_curated_observation(entry, candidates)
    metric_bundle = unique_strings(
        [maybe_text(item) for item in entry.get("metric_bundle", []) if maybe_text(item)]
        or [maybe_text(item.get("metric")) for item in candidates if maybe_text(item.get("metric"))]
    )
    provenance_refs = dedupe_artifact_refs(
        [
            ref
            for ref in entry.get("provenance_refs", [])
            if isinstance(ref, dict)
        ]
        or [
            item.get("provenance")
            for item in candidates
            if isinstance(item.get("provenance"), dict)
        ]
    )
    metric = maybe_text(entry.get("metric")) or maybe_text(candidates[0].get("metric")) if candidates else ""
    aggregation = maybe_text(entry.get("aggregation")) or maybe_text(candidates[0].get("aggregation")) if candidates else ""
    unit = maybe_text(entry.get("unit")) or maybe_text(candidates[0].get("unit")) if candidates else ""
    candidate_metric_set = unique_strings(
        canonical_environment_metric(item.get("metric"))
        for item in candidates
        if maybe_text(item.get("metric"))
    )
    candidate_unit_set = unique_strings(
        maybe_text(item.get("unit"))
        for item in candidates
        if maybe_text(item.get("unit"))
    )
    candidate_statistics_comparable = len(candidate_metric_set) <= 1 and len(candidate_unit_set) <= 1
    if len(candidates) > 1 and not maybe_text(entry.get("unit")) and not candidate_statistics_comparable:
        unit = "mixed"
    value = entry.get("value")
    if value is None and candidates:
        if len(candidates) == 1:
            value = candidates[0].get("value")
        elif candidate_statistics_comparable:
            stats_candidate = aggregate_candidate_statistics(candidates)
            if isinstance(stats_candidate, dict):
                value = stats_candidate.get("mean")
    provenance = copy.deepcopy(provenance_refs[0]) if provenance_refs else {
        "source_skill": source_skill,
        "artifact_path": f"{round_dir_name(round_id)}/environmentalist/observation_curation.json",
    }
    time_window = (
        copy.deepcopy(entry.get("time_window"))
        if isinstance(entry.get("time_window"), dict)
        else merge_time_windows_from_records(candidates, mission_time_window)
    )
    place_scope = (
        copy.deepcopy(entry.get("place_scope"))
        if isinstance(entry.get("place_scope"), dict)
        else derive_place_scope_from_candidate_observations(candidates, mission_scope)
    )
    quality_flags = unique_strings(
        [maybe_text(item) for item in entry.get("quality_flags", []) if maybe_text(item)]
        + [
            maybe_text(flag)
            for candidate in candidates
            for flag in candidate.get("quality_flags", [])
            if maybe_text(flag)
        ]
    )
    if len(candidates) > 1 and len(candidate_metric_set) > 1:
        quality_flags = unique_strings(quality_flags + ["mixed-metric-composite"])
    if len(candidates) > 1 and len(candidate_unit_set) > 1:
        quality_flags = unique_strings(quality_flags + ["mixed-unit-composite"])
    statistics_obj = (
        copy.deepcopy(entry.get("statistics"))
        if isinstance(entry.get("statistics"), dict)
        else aggregate_candidate_statistics(candidates) if candidate_statistics_comparable else None
    )
    distribution_summary = (
        copy.deepcopy(entry.get("distribution_summary"))
        if isinstance(entry.get("distribution_summary"), dict)
        else aggregate_candidate_distribution_summary(candidates, source_skills, metric_bundle)
    )
    component_roles = enrich_component_roles_with_candidate_tags(
        copy.deepcopy(entry.get("component_roles")) if isinstance(entry.get("component_roles"), list) else [],
        candidate_lookup=candidate_lookup,
    )
    compact_audit = compact_audit_from_curated_observation_candidates(candidates, source_skills, metric_bundle)
    if len(candidates) > 1 and not isinstance(entry.get("statistics"), dict):
        if candidate_statistics_comparable and isinstance(statistics_obj, dict):
            quality_flags = unique_strings(quality_flags + ["statistics-derived-from-candidate-summaries"])
        elif not candidate_statistics_comparable:
            quality_flags = unique_strings(quality_flags + ["statistics-omitted-noncomparable-components"])
    submission = {
        "schema_version": SCHEMA_VERSION,
        "submission_id": observation_submission_id(maybe_text(entry.get("observation_id"))),
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": "environmentalist",
        "observation_id": maybe_text(entry.get("observation_id")),
        "source_skill": source_skill,
        "metric": metric,
        "aggregation": aggregation or ("composite" if len(candidate_observation_ids) > 1 else "point"),
        "value": value,
        "unit": unit or "index",
        "meaning": maybe_text(entry.get("meaning")),
        "worth_storing": bool(entry.get("worth_storing")),
        "time_window": time_window,
        "place_scope": place_scope,
        "quality_flags": quality_flags,
        "provenance": provenance,
        "statistics": statistics_obj,
        "distribution_summary": distribution_summary,
        "compact_audit": compact_audit,
        "observation_mode": maybe_text(entry.get("observation_mode")) or ("composite" if len(candidate_observation_ids) > 1 else "atomic"),
        "evidence_role": maybe_text(entry.get("evidence_role")) or "primary",
        "source_skills": source_skills,
        "metric_bundle": metric_bundle,
        "candidate_observation_ids": candidate_observation_ids,
        "provenance_refs": provenance_refs,
        "selection_reason": maybe_text(entry.get("selection_reason")),
        "component_roles": component_roles,
    }
    hypothesis_id = maybe_text(entry.get("hypothesis_id")) or consensus_nonempty_text(
        [candidate.get("hypothesis_id") for candidate in candidates]
    )
    if hypothesis_id:
        submission["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(entry.get("leg_id")) or consensus_nonempty_text(
        [candidate.get("leg_id") for candidate in candidates]
    )
    if leg_id:
        submission["leg_id"] = leg_id
    validate_payload("observation-submission", submission)
    return submission


def materialize_curated_claims(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    pretty: bool,
) -> dict[str, Any]:
    curation = load_object_if_exists(claim_curation_path(run_dir, round_id)) or {}
    candidates = load_canonical_list(claim_candidates_path(run_dir, round_id))
    candidate_lookup = {
        maybe_text(item.get("claim_id")): item
        for item in candidates
        if maybe_text(item.get("claim_id"))
    }
    curated_entries = curation.get("curated_claims") if isinstance(curation.get("curated_claims"), list) else []
    mission_scope = mission_place_scope(mission)
    mission_time_window = mission_window(mission)
    current_submissions = [
        materialize_claim_submission_from_curated_entry(
            entry=item,
            candidate_lookup=candidate_lookup,
            run_id=mission_run_id(mission),
            round_id=round_id,
            mission_scope=mission_scope,
            mission_time_window=mission_time_window,
        )
        for item in curated_entries
        if isinstance(item, dict)
    ]
    active_submissions = merge_claim_submissions(
        previous_active_list(run_dir, round_id, claims_active_path),
        [item for item in current_submissions if bool(item.get("worth_storing"))],
    )
    shared_claims = claims_from_submissions(active_submissions)
    write_json(claim_submissions_path(run_dir, round_id), current_submissions, pretty=pretty)
    write_json(claims_active_path(run_dir, round_id), active_submissions, pretty=pretty)
    write_json(shared_claims_path(run_dir, round_id), shared_claims, pretty=pretty)
    append_library_events(
        run_dir,
        round_id,
        [{"object_kind": "claim-submission", "payload": item} for item in current_submissions],
    )
    return {
        "candidate_count": len(candidates),
        "curated_count": len(curated_entries),
        "claim_submission_count": len(current_submissions),
        "claims_active_count": len(active_submissions),
        "shared_claims_count": len(shared_claims),
        "claim_submissions_path": str(claim_submissions_path(run_dir, round_id)),
        "claims_active_path": str(claims_active_path(run_dir, round_id)),
        "shared_claims_path": str(shared_claims_path(run_dir, round_id)),
    }


def materialize_curated_observations(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    pretty: bool,
) -> dict[str, Any]:
    curation = load_object_if_exists(observation_curation_path(run_dir, round_id)) or {}
    candidates = load_canonical_list(observation_candidates_path(run_dir, round_id))
    candidate_lookup = {
        maybe_text(item.get("observation_id")): item
        for item in candidates
        if maybe_text(item.get("observation_id"))
    }
    curated_entries = curation.get("curated_observations") if isinstance(curation.get("curated_observations"), list) else []
    mission_scope = mission_place_scope(mission)
    mission_time_window = mission_window(mission)
    current_submissions = [
        materialize_observation_submission_from_curated_entry(
            entry=item,
            candidate_lookup=candidate_lookup,
            run_id=mission_run_id(mission),
            round_id=round_id,
            mission_scope=mission_scope,
            mission_time_window=mission_time_window,
        )
        for item in curated_entries
        if isinstance(item, dict)
    ]
    active_submissions = merge_observation_submissions(
        previous_active_list(run_dir, round_id, observations_active_path),
        [item for item in current_submissions if bool(item.get("worth_storing"))],
    )
    shared_observations = observations_from_submissions(active_submissions)
    write_json(observation_submissions_path(run_dir, round_id), current_submissions, pretty=pretty)
    write_json(observations_active_path(run_dir, round_id), active_submissions, pretty=pretty)
    write_json(shared_observations_path(run_dir, round_id), shared_observations, pretty=pretty)
    append_library_events(
        run_dir,
        round_id,
        [{"object_kind": "observation-submission", "payload": item} for item in current_submissions],
    )
    return {
        "candidate_count": len(candidates),
        "curated_count": len(curated_entries),
        "observation_submission_count": len(current_submissions),
        "observations_active_count": len(active_submissions),
        "shared_observation_count": len(shared_observations),
        "observation_submissions_path": str(observation_submissions_path(run_dir, round_id)),
        "observations_active_path": str(observations_active_path(run_dir, round_id)),
        "shared_observations_path": str(shared_observations_path(run_dir, round_id)),
    }


def match_claims_to_observations(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for claim in claims:
        matching_scope = claim_matching_scope(claim)
        support_score = 0
        contradict_score = 0
        primary_support_hits = 0
        contradict_hits = 0
        contextual_hits = 0
        notes: list[str] = []
        gaps: list[str] = []
        observation_assessments: list[dict[str, Any]] = []
        if matching_scope is None:
            matching: list[dict[str, Any]] = []
            scope_gap = direct_matching_gap_for_claim(claim)
            if scope_gap:
                gaps.append(scope_gap)
        else:
            notes.extend(
                maybe_text(item)
                for item in matching_scope.get("notes", [])
                if maybe_text(item)
            )
            matching = [
                observation
                for observation in observations
                if metric_relevant(maybe_text(claim.get("claim_type")), maybe_text(observation.get("metric")))
                and time_windows_overlap(matching_scope.get("time_window", {}), observation.get("time_window", {}))
                and geometry_overlap(
                    (matching_scope.get("place_scope") or {}).get("geometry", {}),
                    observation.get("place_scope", {}).get("geometry", {}),
                )
            ]
        for observation in matching:
            assessment = assess_observation_against_claim(
                maybe_text(claim.get("claim_type")),
                observation,
            )
            observation_assessments.append(
                {
                    "observation": observation,
                    "assessment": assessment,
                }
            )
            support_score += int(assessment.get("support_score") or 0)
            contradict_score += int(assessment.get("contradict_score") or 0)
            primary_support_hits += int(assessment.get("primary_support_hits") or 0)
            contradict_hits += int(assessment.get("contradict_hits") or 0)
            contextual_hits += int(assessment.get("contextual_hits") or 0)
            notes.extend(
                maybe_text(item)
                for item in assessment.get("notes", [])
                if maybe_text(item)
            )

        if not matching:
            verdict = "insufficient"
            confidence = "low"
            if matching_scope is not None:
                gaps.append("No observations matched the claim's localized window and geometry.")
        elif support_score > 0 and contradict_score == 0 and primary_support_hits > 0:
            verdict = "supports"
            confidence = "high" if support_score >= 4 and primary_support_hits >= 2 else "medium"
        elif support_score == 0 and contradict_score > 0:
            verdict = "contradicts"
            confidence = "high" if contradict_hits >= 2 else "medium"
        elif support_score > 0 and contradict_score > 0:
            verdict = "mixed"
            confidence = "medium"
        else:
            verdict = "insufficient"
            confidence = "low"
            if contextual_hits > 0 and primary_support_hits == 0 and contradict_hits == 0:
                gaps.append("Matched observations were contextual only and did not provide direct corroboration.")
            else:
                gaps.append("Matched observations did not cross the direct support or contradiction thresholds.")

        if matching_scope is not None and maybe_text(claim.get("claim_type")) in {"smoke", "air-pollution"}:
            if not any(item.get("source_skill") == "openaq-data-fetch" for item in matching):
                gaps.append("Station-grade corroboration is missing.")
            if any("modeled-background" in item.get("quality_flags", []) for item in matching):
                gaps.append("Modeled background fields should be cross-checked with station or local observations.")

        matches.append(
            {
                "claim": claim,
                "observations": matching,
                "support_score": support_score,
                "contradict_score": contradict_score,
                "observation_assessments": observation_assessments,
                "notes": notes,
                "gaps": sorted(dict.fromkeys(gaps)),
                "verdict": verdict,
                "confidence": confidence,
                "matching_scope": matching_scope,
            }
        )
    return matches


def build_matching_result(
    *,
    authorization: dict[str, Any],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
) -> dict[str, Any]:
    matched_pairs = [
        {
            "claim_id": maybe_text(match["claim"].get("claim_id")),
            "observation_ids": [maybe_text(item.get("observation_id")) for item in match["observations"] if maybe_text(item.get("observation_id"))],
            "support_score": float(match["support_score"]),
            "contradict_score": float(match["contradict_score"]),
            "notes": [maybe_text(item) for item in match["notes"] if maybe_text(item)],
            "hypothesis_id": maybe_text(match["claim"].get("hypothesis_id")),
            "leg_id": maybe_text(match["claim"].get("leg_id")),
            "matching_scope": match.get("matching_scope"),
        }
        for match in matches
        if match["observations"]
    ]
    matched_claim_ids = [maybe_text(item["claim_id"]) for item in matched_pairs if maybe_text(item.get("claim_id"))]
    matched_observation_ids = unique_strings(
        [
            maybe_text(observation_id)
            for pair in matched_pairs
            for observation_id in pair.get("observation_ids", [])
            if maybe_text(observation_id)
        ]
    )
    all_claim_ids = [maybe_text(item.get("claim_id")) for item in claims if maybe_text(item.get("claim_id"))]
    all_observation_ids = [maybe_text(item.get("observation_id")) for item in observations if maybe_text(item.get("observation_id"))]
    unmatched_claim_ids = [claim_id for claim_id in all_claim_ids if claim_id not in set(matched_claim_ids)]
    unmatched_observation_ids = [obs_id for obs_id in all_observation_ids if obs_id not in set(matched_observation_ids)]
    if matched_pairs and (unmatched_claim_ids or unmatched_observation_ids):
        result_status = "partial"
    elif matched_pairs:
        result_status = "matched"
    else:
        result_status = "unmatched"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "result_id": f"matchres-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")) or maybe_text(claims[0].get("run_id")) if claims else "",
        "round_id": maybe_text(authorization.get("round_id")) or maybe_text(claims[0].get("round_id")) if claims else "",
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "result_status": result_status,
        "summary": (
            f"Matched {len(matched_pairs)} claim-observation clusters, leaving "
            f"{len(unmatched_claim_ids)} unmatched claims and {len(unmatched_observation_ids)} unmatched observations."
        ),
        "matched_pairs": matched_pairs,
        "matched_claim_ids": matched_claim_ids,
        "matched_observation_ids": matched_observation_ids,
        "unmatched_claim_ids": unmatched_claim_ids,
        "unmatched_observation_ids": unmatched_observation_ids,
    }
    validate_payload("matching-result", payload)
    return payload


def build_isolated_entries(
    *,
    run_id: str,
    round_id: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    allow_isolated_evidence: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not allow_isolated_evidence:
        return [], []
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match["observations"]
        if maybe_text(observation.get("observation_id"))
    }
    isolated: list[dict[str, Any]] = []
    for claim_index, match in enumerate(matches, start=1):
        claim = match["claim"]
        if match["observations"]:
            continue
        isolated.append(
            {
                "schema_version": SCHEMA_VERSION,
                "isolated_id": f"isolated-claim-{claim_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "claim",
                "entity_id": maybe_text(claim.get("claim_id")),
                "summary": maybe_text(claim.get("summary")),
                "reason": "Public-side evidence is currently isolated from physical corroboration.",
            }
        )
    observation_index = 1
    for observation in observations:
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in matched_observation_ids:
            continue
        isolated.append(
            {
                "schema_version": SCHEMA_VERSION,
                "isolated_id": f"isolated-observation-{observation_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "observation",
                "entity_id": observation_id,
                "summary": f"{maybe_text(observation.get('metric'))} from {maybe_text(observation.get('source_skill'))}",
                "reason": "Physical-side evidence is currently isolated from attributable public recognition.",
            }
        )
        observation_index += 1
    return isolated, []


def build_remand_entries(
    *,
    run_id: str,
    round_id: str,
    matches: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    allow_isolated_evidence: bool,
) -> list[dict[str, Any]]:
    remands: list[dict[str, Any]] = []
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match["observations"]
        if maybe_text(observation.get("observation_id"))
    }
    for index, match in enumerate(matches, start=1):
        claim = match["claim"]
        claim_id = maybe_text(claim.get("claim_id"))
        if not claim_id:
            continue
        has_observations = bool(match["observations"])
        verdict = maybe_text(match["verdict"])
        if not has_observations and allow_isolated_evidence:
            continue
        if verdict not in {"mixed", "insufficient"} and has_observations:
            continue
        remands.append(
            {
                "schema_version": SCHEMA_VERSION,
                "remand_id": f"remand-claim-{index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "claim",
                "entity_id": claim_id,
                "summary": maybe_text(claim.get("summary")),
                "reasons": [maybe_text(item) for item in match["gaps"] if maybe_text(item)] or ["Matching remained partial."],
            }
        )
    if allow_isolated_evidence:
        return remands
    observation_index = 1
    for observation in observations:
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in matched_observation_ids:
            continue
        remands.append(
            {
                "schema_version": SCHEMA_VERSION,
                "remand_id": f"remand-observation-{observation_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "observation",
                "entity_id": observation_id,
                "summary": f"{maybe_text(observation.get('metric'))} from {maybe_text(observation.get('source_skill'))}",
                "reasons": ["Observation remained unmatched and isolated evidence was not authorized."],
            }
        )
        observation_index += 1
    return remands


def build_evidence_adjudication(
    *,
    authorization: dict[str, Any],
    matching_result: dict[str, Any],
    evidence_cards: list[dict[str, Any]],
    isolated_entries: list[dict[str, Any]],
    remands: list[dict[str, Any]],
) -> dict[str, Any]:
    if remands and evidence_cards:
        status = "partial"
    elif remands:
        status = "remand-required"
    else:
        status = "complete"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "adjudication_id": f"adjudication-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")),
        "round_id": maybe_text(authorization.get("round_id")),
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "matching_result_id": maybe_text(matching_result.get("result_id")),
        "adjudication_status": status,
        "summary": (
            f"Produced {len(evidence_cards)} evidence cards, {len(isolated_entries)} isolated entries, "
            f"and {len(remands)} open remands."
        ),
        "matching_reasonable": bool(evidence_cards or isolated_entries or remands),
        "needs_additional_data": bool(remands),
        "card_ids": [maybe_text(item.get("evidence_id")) for item in evidence_cards if maybe_text(item.get("evidence_id"))],
        "isolated_entry_ids": [maybe_text(item.get("isolated_id")) for item in isolated_entries if maybe_text(item.get("isolated_id"))],
        "remand_ids": [maybe_text(item.get("remand_id")) for item in remands if maybe_text(item.get("remand_id"))],
        "open_questions": unique_strings(
            [
                f"How should the council resolve remand {maybe_text(item.get('remand_id'))}?"
                for item in remands
                if maybe_text(item.get("remand_id"))
            ]
        ),
        "recommended_next_actions": [],
    }
    validate_payload("evidence-adjudication", payload)
    return payload


def build_evidence_cards_from_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evidence_cards: list[dict[str, Any]] = []
    for index, match in enumerate(matches, start=1):
        if not isinstance(match.get("observations"), list) or not match.get("observations"):
            continue
        claim = match["claim"]
        evidence = {
            "schema_version": SCHEMA_VERSION,
            "evidence_id": emit_row_id("evidence", index),
            "run_id": claim["run_id"],
            "round_id": claim["round_id"],
            "claim_id": claim["claim_id"],
            "verdict": match["verdict"],
            "confidence": match["confidence"],
            "summary": build_evidence_summary(claim, match["notes"], match["verdict"], match["gaps"]),
            "public_refs": claim.get("public_refs", []),
            "observation_ids": [item["observation_id"] for item in match["observations"]],
            "gaps": match["gaps"],
        }
        hypothesis_id = maybe_text(claim.get("hypothesis_id"))
        if hypothesis_id:
            evidence["hypothesis_id"] = hypothesis_id
        leg_id = maybe_text(claim.get("leg_id"))
        if leg_id:
            evidence["leg_id"] = leg_id
        if isinstance(match.get("matching_scope"), dict):
            evidence["matching_scope"] = match.get("matching_scope")
        validate_payload("evidence-card", evidence)
        evidence_cards.append(evidence)
    return evidence_cards


def link_claims_to_evidence(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches = match_claims_to_observations(claims=claims, observations=observations)
    return build_evidence_cards_from_matches(matches)


def build_matching_candidate_set(
    *,
    authorization: dict[str, Any],
    matches: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match.get("observations", [])
        if isinstance(observation, dict) and maybe_text(observation.get("observation_id"))
    }
    claim_candidates: list[dict[str, Any]] = []
    for match in matches:
        claim = match.get("claim", {})
        observation_candidates: list[dict[str, Any]] = []
        for candidate in match.get("observation_assessments", []):
            observation = candidate.get("observation", {}) if isinstance(candidate, dict) else {}
            assessment = candidate.get("assessment", {}) if isinstance(candidate, dict) else {}
            observation_candidates.append(
                {
                    "observation": compact_observation(observation if isinstance(observation, dict) else {}),
                    "assessment": {
                        "support_score": int(assessment.get("support_score") or 0),
                        "contradict_score": int(assessment.get("contradict_score") or 0),
                        "primary_support_hits": int(assessment.get("primary_support_hits") or 0),
                        "contradict_hits": int(assessment.get("contradict_hits") or 0),
                        "contextual_hits": int(assessment.get("contextual_hits") or 0),
                    },
                    "notes": [maybe_text(item) for item in assessment.get("notes", []) if maybe_text(item)][:6],
                }
            )
        claim_candidates.append(
            {
                "claim": compact_claim(claim if isinstance(claim, dict) else {}),
                "suggested_verdict": maybe_text(match.get("verdict")),
                "suggested_confidence": maybe_text(match.get("confidence")),
                "support_score": int(match.get("support_score") or 0),
                "contradict_score": int(match.get("contradict_score") or 0),
                "matched_observation_ids": [
                    maybe_text(item.get("observation_id"))
                    for item in match.get("observations", [])
                    if isinstance(item, dict) and maybe_text(item.get("observation_id"))
                ],
                "matching_scope": match.get("matching_scope"),
                "gaps": [maybe_text(item) for item in match.get("gaps", []) if maybe_text(item)][:6],
                "notes": [maybe_text(item) for item in match.get("notes", []) if maybe_text(item)][:8],
                "observation_candidates": observation_candidates[:12],
            }
        )
    unpaired_observations = [
        compact_observation(observation)
        for observation in observations
        if isinstance(observation, dict)
        and maybe_text(observation.get("observation_id"))
        and maybe_text(observation.get("observation_id")) not in matched_observation_ids
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_set_id": f"matchcand-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")),
        "round_id": maybe_text(authorization.get("round_id")),
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "summary": (
            f"Rule nomination produced {len(claim_candidates)} claim-side candidate clusters and "
            f"{len(unpaired_observations)} currently unpaired observations."
        ),
        "claim_candidates": claim_candidates,
        "unpaired_observation_candidates": unpaired_observations[:24],
    }


def build_matching_adjudication_draft(
    *,
    authorization: dict[str, Any],
    candidate_set: dict[str, Any],
    matching_result: dict[str, Any],
    evidence_cards: list[dict[str, Any]],
    isolated_entries: list[dict[str, Any]],
    remands: list[dict[str, Any]],
    evidence_adjudication: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "adjudication_id": maybe_text(evidence_adjudication.get("adjudication_id")) or f"adjudication-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")),
        "round_id": maybe_text(authorization.get("round_id")),
        "agent_role": "moderator",
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "candidate_set_id": maybe_text(candidate_set.get("candidate_set_id")),
        "summary": (
            f"Rule draft proposes {len(evidence_cards)} evidence cards, {len(isolated_entries)} isolated entries, "
            f"and {len(remands)} remands for moderator review."
        ),
        "rationale": (
            "This draft is rule-nominated only. The moderator should merge, prune, or reclassify matches "
            "based on cross-source coherence, representativeness, and whether isolated evidence remains acceptable."
        ),
        "matching_result": matching_result,
        "evidence_cards": evidence_cards,
        "isolated_entries": isolated_entries,
        "remand_entries": remands,
        "evidence_adjudication": evidence_adjudication,
        "open_questions": [
            maybe_text(item)
            for item in evidence_adjudication.get("open_questions", [])
            if maybe_text(item)
        ],
        "recommended_next_actions": [],
    }
    validate_payload("matching-adjudication", payload)
    return payload


def build_round_snapshot(
    *,
    run_dir: Path,
    mission: dict[str, Any],
    round_id: str,
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    role: str,
) -> dict[str, Any]:
    state = library_state(run_dir, round_id)
    investigation_plan = load_object_if_exists(investigation_plan_path(run_dir, round_id)) or {}
    claim_candidates_current = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    observation_candidates_current = (
        state.get("observation_candidates_current", [])
        if isinstance(state.get("observation_candidates_current"), list)
        else []
    )
    claim_curation = state.get("claim_curation", {}) if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = (
        state.get("observation_curation", {})
        if isinstance(state.get("observation_curation"), dict)
        else {}
    )
    matching_authorization = state["matching_authorization"] if isinstance(state.get("matching_authorization"), dict) else {}
    if matching_authorization:
        matching_authorization = effective_matching_authorization(
            mission=mission,
            round_id=round_id,
            authorization=matching_authorization,
        )
    run = {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "topic": maybe_text(mission.get("topic")),
        "objective": maybe_text(mission.get("objective")),
        "region": mission_place_scope(mission),
        "window": mission_window(mission),
        "role": role,
    }
    role_tasks = [task for task in tasks if role == "moderator" or task.get("assigned_role") == role]
    verdict_counter = Counter(maybe_text(item.get("verdict")) for item in evidence_cards)
    focus_claims = claims
    if role == "environmentalist":
        focus_claims = [claim for claim in claims if claim.get("needs_physical_validation")]

    dataset = {
        "generated_at_utc": utc_now_iso(),
        "task_count": len(role_tasks),
        "claim_count": len(claims),
        "observation_count": len(observations),
        "evidence_count": len(evidence_cards),
        "claim_submission_count": len(state["claim_submissions_auditable"]),
        "observation_submission_count": len(state["observation_submissions_auditable"]),
        "claim_submission_current_count": len(state["claim_submissions_current"]),
        "observation_submission_current_count": len(state["observation_submissions_current"]),
        "claims_active_count": len(state["claims_active"]),
        "observations_active_count": len(state["observations_active"]),
        "claim_candidate_count": len(claim_candidates_current),
        "observation_candidate_count": len(observation_candidates_current),
        "cards_active_count": len(state["cards_active"]),
        "isolated_count": len(state["isolated_active"]),
        "remand_count": len(state["remands_open"]),
    }
    focus = {
        "task_ids": [maybe_text(task.get("task_id")) for task in role_tasks],
        "claims_needing_more_evidence": [
            card["claim_id"] for card in evidence_cards if card.get("verdict") in {"mixed", "insufficient"}
        ],
    }
    if role == "sociologist":
        focus["candidate_claim_ids"] = [
            maybe_text(item.get("claim_id"))
            for item in (focus_claims or claim_candidates_current)
            if maybe_text(item.get("claim_id"))
        ]
    if role == "environmentalist":
        focus["metrics_requested"] = sorted(
            {
                maybe_text(observation.get("metric"))
                for observation in (observations or observation_candidates_current)
                if maybe_text(observation.get("metric"))
            }
        )

    compact_claims_list = [compact_claim(item) for item in focus_claims[:MAX_CONTEXT_CLAIMS]]
    compact_evidence = [compact_evidence_card(item) for item in evidence_cards[:MAX_CONTEXT_EVIDENCE]]
    compact_observations = [
        compact_observation(item)
        for item in ordered_context_observations(observations, evidence_cards, claims=claims)[:MAX_CONTEXT_OBSERVATIONS]
    ]
    auditable_claim_submissions = representative_claim_submissions(state["claim_submissions_auditable"])
    auditable_observation_submissions = representative_observation_submissions(
        state["observation_submissions_auditable"],
        claims,
    )
    current_claim_submissions = representative_claim_submissions(state["claim_submissions_current"])
    current_observation_submissions = representative_observation_submissions(
        state["observation_submissions_current"],
        claims,
    )

    return {
        "context_layer": "evidence-library-v1",
        "run": run,
        "dataset": dataset,
        "causal_focus": causal_focus_for_role(investigation_plan, role) if isinstance(investigation_plan, dict) else {},
        "phase_state": {
            "claim_curation_status": maybe_text(claim_curation.get("status")),
            "observation_curation_status": maybe_text(observation_curation.get("status")),
            "readiness_statuses": {
                report_role: maybe_text(report.get("readiness_status"))
                for report_role, report in state["readiness_reports"].items()
                if isinstance(report, dict)
            },
            "matching_authorization_status": maybe_text(matching_authorization.get("authorization_status")),
            "matching_authorization_basis": maybe_text(matching_authorization.get("authorization_basis")),
            "matching_result_status": maybe_text(state["matching_result"].get("result_status")),
            "adjudication_status": maybe_text(state["evidence_adjudication"].get("adjudication_status")),
        },
        "aggregates": {
            "claim_type_counts": dict(Counter(maybe_text(item.get("claim_type")) for item in claims)),
            "observation_metric_counts": dict(Counter(maybe_text(item.get("metric")) for item in observations)),
            "evidence_verdict_counts": dict(verdict_counter),
        },
        "canonical_paths": {
            "tasks": str(round_dir(run_dir, round_id) / "moderator" / "tasks.json"),
            "claims": str(shared_claims_path(run_dir, round_id)),
            "observations": str(shared_observations_path(run_dir, round_id)),
            "evidence_cards": str(shared_evidence_path(run_dir, round_id)),
            "claim_submissions": str(claim_submissions_path(run_dir, round_id)),
            "observation_submissions": str(observation_submissions_path(run_dir, round_id)),
            "claim_candidates": str(claim_candidates_path(run_dir, round_id)),
            "observation_candidates": str(observation_candidates_path(run_dir, round_id)),
            "claim_curation": str(claim_curation_path(run_dir, round_id)),
            "observation_curation": str(observation_curation_path(run_dir, round_id)),
            "sociologist_data_readiness_report": str(data_readiness_report_path(run_dir, round_id, "sociologist")),
            "environmentalist_data_readiness_report": str(data_readiness_report_path(run_dir, round_id, "environmentalist")),
            "matching_authorization": str(matching_authorization_path(run_dir, round_id)),
            "matching_result": str(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": str(evidence_adjudication_path(run_dir, round_id)),
            "evidence_library_dir": str(evidence_library_dir(run_dir, round_id)),
        },
        "tasks": [compact_task(item) for item in role_tasks[:MAX_CONTEXT_TASKS]],
        "focus": focus,
        "claims": compact_claims_list,
        "observations": compact_observations,
        "evidence_cards": compact_evidence,
        "evidence_library": {
            "claim_submissions_auditable": [
                compact_claim_submission(item) for item in auditable_claim_submissions[:MAX_CONTEXT_CLAIMS]
            ],
            "observation_submissions_auditable": [
                compact_observation_submission(item)
                for item in auditable_observation_submissions[:MAX_CONTEXT_OBSERVATIONS]
            ],
            "claim_submissions_current": [
                compact_claim_submission(item) for item in current_claim_submissions[:MAX_CONTEXT_CLAIMS]
            ],
            "observation_submissions_current": [
                compact_observation_submission(item) for item in current_observation_submissions[:MAX_CONTEXT_OBSERVATIONS]
            ],
            "claim_candidates_current": [compact_claim(item) for item in claim_candidates_current[:MAX_CONTEXT_CLAIMS]],
            "observation_candidates_current": [
                compact_observation(item) for item in observation_candidates_current[:MAX_CONTEXT_OBSERVATIONS]
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
            "claims_active": [compact_claim_submission(item) for item in state["claims_active"][:MAX_CONTEXT_CLAIMS]],
            "observations_active": [
                compact_observation_submission(item) for item in state["observations_active"][:MAX_CONTEXT_OBSERVATIONS]
            ],
            "cards_active": [compact_evidence_card(item) for item in state["cards_active"][:MAX_CONTEXT_EVIDENCE]],
            "isolated_active": [compact_isolated_entry(item) for item in state["isolated_active"][:MAX_CONTEXT_EVIDENCE]],
            "remands_open": [compact_remand_entry(item) for item in state["remands_open"][:MAX_CONTEXT_EVIDENCE]],
        },
    }


def command_init_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    public_db = Path(args.public_db).expanduser().resolve() if args.public_db else default_public_db_path(run_dir_path)
    environment_db = (
        Path(args.environment_db).expanduser().resolve()
        if args.environment_db
        else default_environment_db_path(run_dir_path)
    )
    init_sqlite_db(public_db, PUBLIC_DDL_PATH)
    init_sqlite_db(environment_db, ENVIRONMENT_DDL_PATH)

    for role in ("moderator", "sociologist", "environmentalist"):
        default_context_dir(run_dir_path, args.round_id, role).mkdir(parents=True, exist_ok=True)
    (round_dir(run_dir_path, args.round_id) / "shared" / "contexts").mkdir(parents=True, exist_ok=True)
    evidence_library_dir(run_dir_path, args.round_id).mkdir(parents=True, exist_ok=True)
    for path, payload in (
        (claims_active_path(run_dir_path, args.round_id), []),
        (observations_active_path(run_dir_path, args.round_id), []),
        (cards_active_path(run_dir_path, args.round_id), []),
        (isolated_active_path(run_dir_path, args.round_id), []),
        (remands_open_path(run_dir_path, args.round_id), []),
    ):
        if not path.exists():
            write_json(path, payload, pretty=args.pretty)
    if not evidence_library_ledger_path(run_dir_path, args.round_id).exists():
        atomic_write_text_file(evidence_library_ledger_path(run_dir_path, args.round_id), "")
    ensure_audit_chain_ready(run_dir_path, args.round_id)

    manifest = load_or_build_manifest(run_dir_path, mission)
    manifest["round_id_initialized"] = args.round_id
    manifest["databases"] = {
        "public_signals": str(public_db),
        "environment_signals": str(environment_db),
    }
    manifest["normalization_cache"] = {
        "version": NORMALIZE_CACHE_VERSION,
        "directory": str(normalize_cache_dir(run_dir_path)),
    }
    manifest["initialized_at_utc"] = utc_now_iso()
    write_json(run_manifest_path(run_dir_path), manifest, pretty=args.pretty)

    return {
        "run_dir": str(run_dir_path),
        "public_db": str(public_db),
        "environment_db": str(environment_db),
        "manifest_path": str(run_manifest_path(run_dir_path)),
    }


def command_normalize_public(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    run_id = mission_run_id(mission)
    constraints = mission_constraints(mission)
    public_db = Path(args.public_db).expanduser().resolve() if args.public_db else default_public_db_path(run_dir_path)
    inputs = parse_input_specs(args.input)
    all_signals: list[dict[str, Any]] = []
    cache_hits = 0
    cache_misses = 0
    for source_skill, path in inputs:
        signals, cache_status = normalize_public_source_cached(
            run_dir=run_dir_path,
            source_skill=source_skill,
            path=path,
            mission=mission,
            run_id=run_id,
            round_id=args.round_id,
        )
        all_signals.extend(signals)
        if cache_status == "hit":
            cache_hits += 1
        else:
            cache_misses += 1

    deduped_by_id: dict[str, dict[str, Any]] = {signal["signal_id"]: signal for signal in all_signals}
    signals = sorted(
        deduped_by_id.values(),
        key=lambda item: (
            item.get("published_at_utc") or "",
            item["signal_id"],
        ),
        reverse=False,
    )
    candidate_limit = max(
        1,
        int(args.max_claims or 0),
        int(constraints.get("claim_hard_cap_per_round") or 0),
        int(constraints.get("max_claims_per_round") or 0) * 3,
    )
    claims = public_signals_to_claims(
        mission=mission,
        round_id=args.round_id,
        signals=signals,
        max_claims=candidate_limit,
    )

    save_public_db(public_db, signals, claims)
    normalized_dir = role_normalized_dir(run_dir_path, args.round_id, "sociologist")
    public_signals_file = normalized_dir / "public_signals.jsonl"
    claims_file = claim_candidates_path(run_dir_path, args.round_id)
    summary_file = normalized_dir / "public_signal_summary.json"
    write_jsonl(public_signals_file, signals)
    write_json(claims_file, claims, pretty=args.pretty)
    write_json(summary_file, build_public_signal_summary(signals, claims), pretty=args.pretty)
    write_json(claim_submissions_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(claims_active_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(shared_claims_path(run_dir_path, args.round_id), [], pretty=args.pretty)

    return {
        "public_db": str(public_db),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "signal_count": len(signals),
        "claim_candidate_count": len(claims),
        "signals_path": str(public_signals_file),
        "signal_summary_path": str(summary_file),
        "claim_candidates_path": str(claims_file),
    }


def command_normalize_environment(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    run_id = mission_run_id(mission)
    environment_db = (
        Path(args.environment_db).expanduser().resolve()
        if args.environment_db
        else default_environment_db_path(run_dir_path)
    )
    inputs = parse_input_specs(args.input)
    all_signals: list[dict[str, Any]] = []
    extra_observations: list[dict[str, Any]] = []
    cache_hits = 0
    cache_misses = 0
    for source_skill, path in inputs:
        source_signals, source_observations, cache_status = normalize_environment_source_cached(
            run_dir=run_dir_path,
            source_skill=source_skill,
            path=path,
            run_id=run_id,
            round_id=args.round_id,
        )
        all_signals.extend(source_signals)
        extra_observations.extend(source_observations)
        if cache_status == "hit":
            cache_hits += 1
        else:
            cache_misses += 1

    deduped_by_id: dict[str, dict[str, Any]] = {signal["signal_id"]: signal for signal in all_signals}
    signals = sorted(deduped_by_id.values(), key=lambda item: (item.get("metric") or "", item["signal_id"]))
    observations = environment_signals_to_observations(
        mission=mission,
        round_id=args.round_id,
        signals=signals,
        extra_observations=extra_observations,
    )

    save_environment_db(environment_db, signals, observations)
    normalized_dir = role_normalized_dir(run_dir_path, args.round_id, "environmentalist")
    signals_file = normalized_dir / "environment_signals.jsonl"
    observations_file = observation_candidates_path(run_dir_path, args.round_id)
    summary_file = normalized_dir / "environment_signal_summary.json"
    write_jsonl(signals_file, signals)
    write_json(observations_file, observations, pretty=args.pretty)
    write_json(summary_file, build_environment_signal_summary(signals, observations), pretty=args.pretty)
    write_json(observation_submissions_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(observations_active_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(shared_observations_path(run_dir_path, args.round_id), [], pretty=args.pretty)

    return {
        "environment_db": str(environment_db),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "signal_count": len(signals),
        "observation_candidate_count": len(observations),
        "signals_path": str(signals_file),
        "signal_summary_path": str(summary_file),
        "observation_candidates_path": str(observations_file),
    }


def command_materialize_curations(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    claim_result = materialize_curated_claims(
        run_dir=run_dir_path,
        round_id=args.round_id,
        mission=mission,
        pretty=args.pretty,
    )
    observation_result = materialize_curated_observations(
        run_dir=run_dir_path,
        round_id=args.round_id,
        mission=mission,
        pretty=args.pretty,
    )
    return {
        "run_id": mission_run_id(mission),
        "round_id": args.round_id,
        "claim_materialization": claim_result,
        "observation_materialization": observation_result,
    }


def authorized_matching_inputs(
    *,
    run_dir_path: Path,
    round_id: str,
    authorization_input: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    mission = load_mission(run_dir_path)
    input_path = (
        Path(authorization_input).expanduser().resolve()
        if maybe_text(authorization_input)
        else matching_authorization_path(run_dir_path, round_id)
    )
    authorization = load_object_if_exists(input_path)
    if authorization is None:
        raise ValueError(f"Matching authorization is missing or invalid: {input_path}")
    validate_payload("matching-authorization", authorization)
    effective_authorization = effective_matching_authorization(
        mission=mission,
        round_id=round_id,
        authorization=authorization,
    )
    validate_payload("matching-authorization", effective_authorization)
    if maybe_text(effective_authorization.get("authorization_status")) != "authorized":
        raise ValueError("Matching authorization exists but does not authorize matching.")
    claims = effective_shared_claims(run_dir_path, round_id)
    observations = effective_shared_observations(run_dir_path, round_id)
    raw_observation_submissions = load_canonical_list(observation_submissions_path(run_dir_path, round_id))
    raw_observations_active = active_library_list(run_dir_path, round_id, observations_active_path)
    hydrated_observation_submissions = hydrate_observation_submissions_with_observations(raw_observation_submissions, observations)
    hydrated_observations_active = hydrate_observation_submissions_with_observations(raw_observations_active, observations)
    observation_id_aliases: dict[str, str] = {}
    for raw_items, hydrated_items in (
        (raw_observation_submissions, hydrated_observation_submissions),
        (raw_observations_active, hydrated_observations_active),
    ):
        for raw_item, hydrated_item in zip(raw_items, hydrated_items):
            if not isinstance(raw_item, dict) or not isinstance(hydrated_item, dict):
                continue
            raw_observation_id = maybe_text(raw_item.get("observation_id"))
            hydrated_observation_id = maybe_text(hydrated_item.get("observation_id"))
            if raw_observation_id and hydrated_observation_id:
                observation_id_aliases[raw_observation_id] = hydrated_observation_id
    authorized_claim_ids = {
        maybe_text(item)
        for item in effective_authorization.get("claim_ids", [])
        if maybe_text(item)
    }
    authorized_observation_ids = {
        observation_id_aliases.get(maybe_text(item), maybe_text(item))
        for item in effective_authorization.get("observation_ids", [])
        if maybe_text(item)
    }
    filtered_claims = [item for item in claims if not authorized_claim_ids or maybe_text(item.get("claim_id")) in authorized_claim_ids]
    filtered_observations = [
        item
        for item in observations
        if not authorized_observation_ids or maybe_text(item.get("observation_id")) in authorized_observation_ids
    ]
    return mission, effective_authorization, filtered_claims, filtered_observations


def command_prepare_matching_adjudication(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission, effective_authorization, filtered_claims, filtered_observations = authorized_matching_inputs(
        run_dir_path=run_dir_path,
        round_id=args.round_id,
        authorization_input=maybe_text(args.authorization_input),
    )
    matches = match_claims_to_observations(claims=filtered_claims, observations=filtered_observations)
    evidence_cards = build_evidence_cards_from_matches(matches)
    matching_result = build_matching_result(
        authorization=effective_authorization,
        claims=filtered_claims,
        observations=filtered_observations,
        matches=matches,
    )
    allow_isolated_evidence = bool(effective_authorization.get("allow_isolated_evidence"))
    isolated_entries, _unused = build_isolated_entries(
        run_id=maybe_text(effective_authorization.get("run_id")) or mission_run_id(mission),
        round_id=args.round_id,
        claims=filtered_claims,
        observations=filtered_observations,
        matches=matches,
        allow_isolated_evidence=allow_isolated_evidence,
    )
    remands = build_remand_entries(
        run_id=maybe_text(effective_authorization.get("run_id")) or mission_run_id(mission),
        round_id=args.round_id,
        matches=matches,
        observations=filtered_observations,
        allow_isolated_evidence=allow_isolated_evidence,
    )
    validate_payload("isolated-entry", isolated_entries)
    validate_payload("remand-entry", remands)
    adjudication = build_evidence_adjudication(
        authorization=effective_authorization,
        matching_result=matching_result,
        evidence_cards=evidence_cards,
        isolated_entries=isolated_entries,
        remands=remands,
    )
    candidate_set = build_matching_candidate_set(
        authorization=effective_authorization,
        matches=matches,
        observations=filtered_observations,
    )
    draft = build_matching_adjudication_draft(
        authorization=effective_authorization,
        candidate_set=candidate_set,
        matching_result=matching_result,
        evidence_cards=evidence_cards,
        isolated_entries=isolated_entries,
        remands=remands,
        evidence_adjudication=adjudication,
    )
    candidate_path = matching_candidate_set_path(run_dir_path, args.round_id)
    draft_path = matching_adjudication_draft_path(run_dir_path, args.round_id)
    write_json(candidate_path, candidate_set, pretty=args.pretty)
    write_json(draft_path, draft, pretty=args.pretty)

    return {
        "run_id": mission_run_id(mission),
        "round_id": args.round_id,
        "candidate_set_path": str(candidate_path),
        "matching_adjudication_draft_path": str(draft_path),
        "claim_count": len(filtered_claims),
        "observation_count": len(filtered_observations),
        "evidence_count": len(evidence_cards),
        "isolated_count": len(isolated_entries),
        "remand_count": len(remands),
    }


def command_apply_matching_adjudication(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    _mission, effective_authorization, _filtered_claims, _filtered_observations = authorized_matching_inputs(
        run_dir_path=run_dir_path,
        round_id=args.round_id,
        authorization_input="",
    )
    input_path = (
        Path(args.adjudication_input).expanduser().resolve()
        if maybe_text(args.adjudication_input)
        else matching_adjudication_path(run_dir_path, args.round_id)
    )
    payload = load_object_if_exists(input_path)
    if payload is None:
        raise ValueError(f"Matching adjudication is missing or invalid: {input_path}")
    validate_payload("matching-adjudication", payload)
    if maybe_text(payload.get("run_id")) != maybe_text(effective_authorization.get("run_id")):
        raise ValueError("Matching adjudication run_id does not match the authorized run.")
    if maybe_text(payload.get("round_id")) != maybe_text(effective_authorization.get("round_id")):
        raise ValueError("Matching adjudication round_id does not match the authorized round.")
    if maybe_text(payload.get("authorization_id")) != maybe_text(effective_authorization.get("authorization_id")):
        raise ValueError("Matching adjudication authorization_id does not match matching_authorization.json.")
    write_json(matching_adjudication_path(run_dir_path, args.round_id), payload, pretty=args.pretty)
    matching_result = payload.get("matching_result", {}) if isinstance(payload.get("matching_result"), dict) else {}
    evidence_cards = payload.get("evidence_cards", []) if isinstance(payload.get("evidence_cards"), list) else []
    isolated_entries = payload.get("isolated_entries", []) if isinstance(payload.get("isolated_entries"), list) else []
    remands = payload.get("remand_entries", []) if isinstance(payload.get("remand_entries"), list) else []
    evidence_adjudication = payload.get("evidence_adjudication", {}) if isinstance(payload.get("evidence_adjudication"), dict) else {}

    write_json(shared_evidence_path(run_dir_path, args.round_id), evidence_cards, pretty=args.pretty)
    write_json(matching_result_path(run_dir_path, args.round_id), matching_result, pretty=args.pretty)
    write_json(evidence_adjudication_path(run_dir_path, args.round_id), evidence_adjudication, pretty=args.pretty)
    write_json(
        cards_active_path(run_dir_path, args.round_id),
        merge_evidence_cards(previous_active_list(run_dir_path, args.round_id, cards_active_path), evidence_cards),
        pretty=args.pretty,
    )
    write_json(
        isolated_active_path(run_dir_path, args.round_id),
        merge_isolated_entries(previous_active_list(run_dir_path, args.round_id, isolated_active_path), isolated_entries),
        pretty=args.pretty,
    )
    write_json(
        remands_open_path(run_dir_path, args.round_id),
        merge_remand_entries(previous_active_list(run_dir_path, args.round_id, remands_open_path), remands),
        pretty=args.pretty,
    )
    append_library_events(
        run_dir_path,
        args.round_id,
        [
            {"object_kind": "matching-result", "payload": matching_result},
            {"object_kind": "evidence-adjudication", "payload": evidence_adjudication},
        ],
    )
    record_match_phase_receipt(
        run_dir=run_dir_path,
        round_id=args.round_id,
        evidence_count=len(evidence_cards),
        isolated_count=len(isolated_entries),
        remand_count=len(remands),
    )

    return {
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "evidence_count": len(evidence_cards),
        "isolated_count": len(isolated_entries),
        "remand_count": len(remands),
        "shared_evidence_path": str(shared_evidence_path(run_dir_path, args.round_id)),
        "matching_result_path": str(matching_result_path(run_dir_path, args.round_id)),
        "evidence_adjudication_path": str(evidence_adjudication_path(run_dir_path, args.round_id)),
    }


def command_link_evidence(args: argparse.Namespace) -> dict[str, Any]:
    if not hasattr(args, "adjudication_input"):
        setattr(args, "adjudication_input", "")
    return command_apply_matching_adjudication(args)


def command_build_round_context(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    tasks_path = round_dir(run_dir_path, args.round_id) / "moderator" / "tasks.json"
    tasks = load_canonical_list(tasks_path)
    claims = effective_shared_claims(run_dir_path, args.round_id)
    observations = effective_shared_observations(run_dir_path, args.round_id)
    evidence_cards = load_canonical_list(shared_evidence_path(run_dir_path, args.round_id))

    outputs: dict[str, str] = {}
    for role in ("moderator", "sociologist", "environmentalist"):
        payload = build_round_snapshot(
            run_dir=run_dir_path,
            mission=mission,
            round_id=args.round_id,
            tasks=tasks,
            claims=claims,
            observations=observations,
            evidence_cards=evidence_cards,
            role=role,
        )
        context_path = default_context_dir(run_dir_path, args.round_id, role) / f"context_{role}.json"
        write_json(context_path, payload, pretty=args.pretty)
        write_json(library_context_path(run_dir_path, args.round_id, role), payload, pretty=args.pretty)
        outputs[role] = str(context_path)

    snapshot = build_round_snapshot(
        run_dir=run_dir_path,
        mission=mission,
        round_id=args.round_id,
        tasks=tasks,
        claims=claims,
        observations=observations,
        evidence_cards=evidence_cards,
        role="moderator",
    )
    shared_snapshot_path = round_dir(run_dir_path, args.round_id) / "shared" / "contexts" / "round_snapshot.json"
    write_json(shared_snapshot_path, snapshot, pretty=args.pretty)
    outputs["shared_snapshot"] = str(shared_snapshot_path)

    return {
        "claim_count": len(claims),
        "observation_count": len(observations),
        "evidence_count": len(evidence_cards),
        "cards_active_count": len(library_state(run_dir_path, args.round_id)["cards_active"]),
        "outputs": outputs,
    }


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic normalization pipeline for eco-council runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_run = sub.add_parser("init-run", help="Initialize normalization databases and derived directories.")
    init_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    init_run.add_argument("--round-id", default="round-001", help="Round identifier.")
    init_run.add_argument("--public-db", default="", help="Override public-signals SQLite path.")
    init_run.add_argument("--environment-db", default="", help="Override environment-signals SQLite path.")
    add_pretty_flag(init_run)

    normalize_public = sub.add_parser("normalize-public", help="Normalize sociologist-side raw artifacts.")
    normalize_public.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    normalize_public.add_argument("--round-id", required=True, help="Round identifier.")
    normalize_public.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input artifact in source-skill=/path form. Repeat for multiple artifacts.",
    )
    normalize_public.add_argument("--public-db", default="", help="Override public-signals SQLite path.")
    normalize_public.add_argument("--max-claims", type=int, default=12, help="Maximum claim candidates to emit before curation.")
    add_pretty_flag(normalize_public)

    normalize_environment = sub.add_parser("normalize-environment", help="Normalize environment raw artifacts.")
    normalize_environment.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    normalize_environment.add_argument("--round-id", required=True, help="Round identifier.")
    normalize_environment.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input artifact in source-skill=/path form. Repeat for multiple artifacts.",
    )
    normalize_environment.add_argument("--environment-db", default="", help="Override environment-signals SQLite path.")
    add_pretty_flag(normalize_environment)

    materialize_curations = sub.add_parser(
        "materialize-curations",
        help="Materialize curated claims and observations into canonical submission and library files.",
    )
    materialize_curations.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    materialize_curations.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(materialize_curations)

    prepare_matching = sub.add_parser(
        "prepare-matching-adjudication",
        help="Build rule-nominated matching candidates plus a moderator adjudication draft.",
    )
    prepare_matching.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    prepare_matching.add_argument("--round-id", required=True, help="Round identifier.")
    prepare_matching.add_argument(
        "--authorization-input",
        default="",
        help="Optional matching-authorization JSON path. Defaults to the canonical moderator path for the round.",
    )
    add_pretty_flag(prepare_matching)

    apply_matching = sub.add_parser(
        "apply-matching-adjudication",
        help="Materialize a moderator-approved matching-adjudication payload into shared evidence artifacts.",
    )
    apply_matching.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    apply_matching.add_argument("--round-id", required=True, help="Round identifier.")
    apply_matching.add_argument(
        "--adjudication-input",
        default="",
        help="Optional matching-adjudication JSON path. Defaults to the canonical moderator path for the round.",
    )
    add_pretty_flag(apply_matching)

    link_evidence = sub.add_parser("link-evidence", help="Deprecated alias for apply-matching-adjudication.")
    link_evidence.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    link_evidence.add_argument("--round-id", required=True, help="Round identifier.")
    link_evidence.add_argument(
        "--adjudication-input",
        default="",
        help="Optional matching-adjudication JSON path. Defaults to the canonical moderator path for the round.",
    )
    add_pretty_flag(link_evidence)

    build_context = sub.add_parser("build-round-context", help="Build role-specific round context payloads.")
    build_context.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    build_context.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(build_context)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-run": command_init_run,
        "normalize-public": command_normalize_public,
        "normalize-environment": command_normalize_environment,
        "materialize-curations": command_materialize_curations,
        "prepare-matching-adjudication": command_prepare_matching_adjudication,
        "apply-matching-adjudication": command_apply_matching_adjudication,
        "link-evidence": command_link_evidence,
        "build-round-context": command_build_round_context,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        result = {"command": args.command, "ok": False, "error": str(exc)}
        print(pretty_json(result, pretty=getattr(args, "pretty", False)))
        return 1

    result = {"command": args.command, "ok": True, "payload": payload}
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
