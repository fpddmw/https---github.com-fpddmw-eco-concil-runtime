from __future__ import annotations

from collections import Counter
from typing import Any

from .support import (
    artifact_ref,
    date_key,
    first_timestamp,
    helper_metadata,
    list_items,
    maybe_text,
    query_signals,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    signal_metric_distribution,
    signal_source_distribution,
    stable_hash,
    unique_texts,
    unique_values,
    utc_now_iso,
    write_json,
)


__all__ = ("run_aggregate_environment_evidence",)


def run_aggregate_environment_evidence(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    aggregation_method: str = "source-metric-day-summary",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "aggregate-environment-evidence"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"environment_evidence_aggregation_{round_id}.json")
    signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="environment", limit=limit)
    timestamp_missing_count = sum(1 for signal in signals if not first_timestamp(signal))
    coordinate_missing_count = sum(1 for signal in signals if signal.get("latitude") is None or signal.get("longitude") is None)
    date_counts = Counter(date_key(first_timestamp(signal)) or "missing-timestamp" for signal in signals)
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-normalized-environment-signal-summary", aggregation_method],
        caveats=[
            "Aggregation describes evidence coverage and limitations only.",
            "Aggregation must not be used for claim matching or readiness scoring.",
        ],
    )
    aggregation_id = "envagg-" + stable_hash(run_id, round_id, aggregation_method, len(signals))[:12]
    aggregation = {
        "aggregation_id": aggregation_id,
        "run_id": run_id,
        "round_id": round_id,
        "helper_governance": metadata,
        "aggregation_method": maybe_text(aggregation_method),
        "statistics_summary": {
            "signal_count": len(signals),
            "numeric_signal_count": sum(1 for signal in signals if isinstance(signal.get("numeric_value"), (int, float))),
            "source_family_count": len({maybe_text(signal.get("source_skill")) for signal in signals if maybe_text(signal.get("source_skill"))}),
            "metric_count": len({maybe_text(signal.get("metric")) for signal in signals if maybe_text(signal.get("metric"))}),
        },
        "spatial_distribution": {
            "with_coordinates": len(signals) - coordinate_missing_count,
            "missing_coordinates": coordinate_missing_count,
        },
        "temporal_distribution": [
            {"date": key, "signal_count": count}
            for key, count in sorted(date_counts.items())
        ],
        "metric_distribution": signal_metric_distribution(signals),
        "source_distribution": signal_source_distribution(signals),
        "metadata_tags": {
            "signal_plane": "environment",
            "coverage_view": "descriptive",
            "analysis_scope": "descriptive-environment-aggregation",
        },
        "coverage_limitations": unique_texts(
            [
                "No environment signals found." if not signals else "",
                f"{coordinate_missing_count} environment signals lack coordinates." if coordinate_missing_count else "",
                f"{timestamp_missing_count} environment signals lack usable timestamps." if timestamp_missing_count else "",
            ]
        ),
        "source_signal_ids": [maybe_text(signal.get("signal_id")) for signal in signals],
        "evidence_refs": unique_values([ref for signal in signals for ref in list_items(signal.get("evidence_refs"))]),
        "lineage": [maybe_text(signal.get("signal_id")) for signal in signals],
        "provenance": {
            "source_skill": skill_name,
            "decision_source": metadata["decision_source"],
            "db_path": db_path,
        },
    }
    payload = {
        "schema_version": "optional-analysis-environment-evidence-aggregation-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "aggregation": aggregation,
        "observed_inputs": {"db_path": db_path, "environment_signal_count": len(signals)},
        "warnings": [
            {"code": "no-environment-signals", "message": "No environment signals were available for aggregation."}
        ]
        if not signals
        else [],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "aggregation_id": aggregation_id,
            "environment_signal_count": len(signals),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "envagg-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "envagg-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
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
