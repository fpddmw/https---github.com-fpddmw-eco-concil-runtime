from __future__ import annotations

from typing import Any

from ..support import (
    artifact_ref,
    helper_metadata,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    stable_hash,
    utc_now_iso,
    write_json,
)
from .common import (
    DEFAULT_AGGREGATION_METHOD,
    DEFAULT_GROUP_LIMIT,
    DEFAULT_SAMPLE_REF_LIMIT,
    SKILL_NAME,
    build_config,
    iter_environment_signals,
    load_environment_signal_stream,
)
from .coverage import CoverageAccumulator, sample_status
from .point_events import PointEventAccumulator
from .timeseries import TimeSeriesAccumulator


def selected_methods(
    *,
    resolved_method: str,
    time_series: TimeSeriesAccumulator,
    point_events: PointEventAccumulator,
) -> list[str]:
    if resolved_method == "coverage-summary":
        return ["coverage-summary"]
    if resolved_method == "time-series-summary":
        return ["coverage-summary", "time-series-summary"]
    if resolved_method == "point-event-summary":
        return ["coverage-summary", "point-event-summary"]
    methods = ["coverage-summary"]
    if time_series.has_output():
        methods.append("time-series-summary")
    if point_events.has_output():
        methods.append("point-event-summary")
    return methods


def build_sample_definition(
    *,
    config: Any,
    db_path: str,
    selected_round_ids: list[str],
    signal_count: int,
) -> dict[str, Any]:
    bbox = None
    if config.bbox is not None:
        west, south, east, north = config.bbox
        bbox = {"west": west, "south": south, "east": east, "north": north}
    return {
        "run_id": config.run_id,
        "round_id": config.round_id,
        "round_scope": config.round_scope,
        "queried_round_ids": selected_round_ids,
        "db_path": db_path,
        "filters": {
            "source_skill": config.source_skill,
            "metric": config.metric,
            "observed_after_utc": config.observed_after_utc,
            "observed_before_utc": config.observed_before_utc,
            "bbox": bbox,
        },
        "sample_limit": config.sample_limit,
        "group_limit": config.group_limit,
        "sample_ref_limit": config.sample_ref_limit,
        "sampling_status": sample_status(
            signal_count=signal_count,
            sample_limit=config.sample_limit,
        ),
        "statistics_scope": "full matched DB rows; limit controls output samples only",
    }


def run_aggregate_environment_evidence(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    aggregation_method: str = DEFAULT_AGGREGATION_METHOD,
    limit: int = 500,
    round_scope: str = "current",
    source_skill: str = "",
    metric: str = "",
    observed_after_utc: str = "",
    observed_before_utc: str = "",
    bbox: str = "",
    group_limit: int = DEFAULT_GROUP_LIMIT,
    sample_ref_limit: int = DEFAULT_SAMPLE_REF_LIMIT,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    config = build_config(
        run_dir=run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_path=output_path,
        aggregation_method=aggregation_method,
        round_scope=round_scope,
        source_skill=source_skill,
        metric=metric,
        observed_after_utc=observed_after_utc,
        observed_before_utc=observed_before_utc,
        bbox=bbox,
        limit=limit,
        group_limit=group_limit,
        sample_ref_limit=sample_ref_limit,
    )
    output_file = resolve_output_path(
        run_dir_path,
        output_path,
        f"environment_evidence_aggregation_{config.round_id}.json",
    )
    connection, db_path, selected_round_ids = load_environment_signal_stream(config)
    coverage = CoverageAccumulator(
        group_limit=config.group_limit,
        sample_ref_limit=config.sample_ref_limit,
        sample_limit=config.sample_limit,
    )
    time_series = TimeSeriesAccumulator(group_limit=config.group_limit)
    point_events = PointEventAccumulator(group_limit=config.group_limit)
    try:
        for signal in iter_environment_signals(connection, round_ids=selected_round_ids, config=config):
            coverage.add(signal)
            time_series.add(signal)
            point_events.add(signal)
    finally:
        connection.close()

    methods = selected_methods(
        resolved_method=config.aggregation_method,
        time_series=time_series,
        point_events=point_events,
    )
    metadata = helper_metadata(
        skill_name=SKILL_NAME,
        rule_trace=["db-normalized-environment-signal-summary", config.aggregation_method],
        caveats=[
            "Aggregation describes environment evidence coverage and descriptive statistics only.",
            "Aggregation must not be used for claim matching, risk scoring, evidence ranking, source ranking, source attribution, or readiness decisions.",
            "Helper output must be carried by a council object before report use.",
        ],
    )
    aggregation_id = "envagg-" + stable_hash(
        config.run_id,
        config.round_id,
        config.requested_method,
        config.round_scope,
        coverage.signal_count,
    )[:12]
    coverage_summary = coverage.coverage_summary()
    aggregation: dict[str, Any] = {
        "aggregation_id": aggregation_id,
        "run_id": config.run_id,
        "round_id": config.round_id,
        "helper_governance": metadata,
        "aggregation_method": config.aggregation_method,
        "requested_aggregation_method": config.requested_method,
        "aggregation_methods_included": methods,
        "sample_definition": build_sample_definition(
            config=config,
            db_path=db_path,
            selected_round_ids=selected_round_ids,
            signal_count=coverage.signal_count,
        ),
        "signal_count": coverage.signal_count,
        "statistics_summary": coverage.statistics_summary(),
        "coverage_summary": coverage_summary,
        "source_distribution": coverage_summary["source_distribution"],
        "metric_distribution": coverage_summary["metric_distribution"],
        "time_coverage": coverage_summary["time_coverage"],
        "spatial_coverage": coverage_summary["spatial_coverage"],
        "spatial_distribution": {
            "with_coordinates": coverage_summary["spatial_coverage"]["with_coordinates"],
            "missing_coordinates": coverage_summary["spatial_coverage"]["missing_coordinates"],
        },
        "temporal_distribution": coverage_summary["time_coverage"]["date_buckets"],
        "metadata_tags": {
            "signal_plane": "environment",
            "coverage_view": "descriptive",
            "analysis_scope": "descriptive-environment-aggregation",
            "helper_lane": "environment-evidence-compression",
        },
        "quality_or_metadata_limitations": coverage_summary["quality_or_metadata_limitations"],
        "coverage_limitations": coverage_summary["quality_or_metadata_limitations"],
        "evidence_ref_samples": coverage_summary["evidence_ref_samples"],
        "source_signal_ref_samples": coverage_summary["source_signal_ref_samples"],
        "source_signal_ids": coverage.source_signal_ids,
        "evidence_refs": coverage.evidence_ref_samples,
        "lineage": coverage.source_signal_ids,
        "provenance": {
            "source_skill": SKILL_NAME,
            "decision_source": metadata["decision_source"],
            "db_path": db_path,
        },
        "helper_boundaries": {
            "claim_matching": False,
            "ranked_sources": False,
            "evidence_weighting": False,
            "phase_gate": False,
            "professional_attribution_model": False,
        },
    }
    if "time-series-summary" in methods:
        aggregation["time_series_summary"] = time_series.to_payload()
    if "point-event-summary" in methods:
        aggregation["point_event_summary"] = point_events.to_payload()

    warnings = []
    if coverage.signal_count == 0:
        warnings.append(
            {
                "code": "no-environment-signals",
                "message": "No environment signals matched the selected round scope and filters.",
            }
        )
    if config.requested_method == "source-metric-day-summary":
        warnings.append(
            {
                "code": "legacy-aggregation-method-alias",
                "message": "source-metric-day-summary is accepted as a compatibility alias for coverage-summary.",
            }
        )
    payload = {
        "schema_version": "optional-analysis-environment-evidence-aggregation-v2",
        "skill": SKILL_NAME,
        "run_id": config.run_id,
        "round_id": config.round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "aggregation": aggregation,
        "observed_inputs": {
            "db_path": db_path,
            "environment_signal_count": coverage.signal_count,
            "queried_round_ids": selected_round_ids,
        },
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": config.run_id,
            "round_id": config.round_id,
            "round_scope": config.round_scope,
            "queried_round_ids": selected_round_ids,
            "output_path": str(output_file),
            "aggregation_id": aggregation_id,
            "aggregation_method": config.aggregation_method,
            "requested_aggregation_method": config.requested_method,
            "environment_signal_count": coverage.signal_count,
            "sampling_status": aggregation["sample_definition"]["sampling_status"],
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "envagg-receipt-" + stable_hash(SKILL_NAME, config.run_id, config.round_id, output_file)[:20],
        "batch_id": "envagg-batch-" + stable_hash(SKILL_NAME, config.run_id, config.round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.aggregation")],
        "canonical_ids": [aggregation_id],
        "warnings": payload["warnings"],
        "aggregation": aggregation,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.aggregation",
            candidate_ids=[aggregation_id],
            gap_hints=aggregation["coverage_limitations"],
        ),
    }
