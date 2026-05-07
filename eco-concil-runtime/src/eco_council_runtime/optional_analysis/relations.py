from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from eco_council_runtime.objects.analysis import normalize_spatiotemporal_relation_cue_payload
from eco_council_runtime.contracts import (
    ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
    SPATIOTEMPORAL_OBJECTION_CODE_VALUES,
)
from .support import (
    artifact_ref,
    context_signals_for_relation,
    date_key,
    first_timestamp,
    haversine_km,
    helper_metadata,
    lineage_from_signals,
    list_items,
    maybe_text,
    parse_bbox,
    query_signals,
    refs_from_signals,
    relation_filter_matches,
    relation_signal_role,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    signal_source_distribution,
    stable_hash,
    timestamp_delta_hours,
    unique_texts,
    unique_values,
    utc_now_iso,
    write_json,
)


__all__ = (
    "RELATION_REJECTION_OBJECTION_MAP",
    "build_spatiotemporal_relation_cues",
    "relation_objection_candidate",
    "relation_objection_candidates",
    "relation_type_for_pair",
    "run_detect_temporal_cooccurrence_cues",
    "run_review_spatiotemporal_relation_alternatives",
    "structured_relation_requested",
)


def structured_relation_requested(
    *,
    source_role: str,
    target_role: str,
    source_class: str,
    target_class: str,
    observed_after_utc: str,
    observed_before_utc: str,
    lag_min_hours: float | None,
    lag_max_hours: float | None,
    bbox: str,
    max_distance_km: float | None,
    spatial_rule: str,
) -> bool:
    return any(
        [
            maybe_text(source_role),
            maybe_text(target_role),
            maybe_text(source_class),
            maybe_text(target_class),
            maybe_text(observed_after_utc),
            maybe_text(observed_before_utc),
            lag_min_hours is not None,
            lag_max_hours is not None,
            maybe_text(bbox),
            max_distance_km is not None,
            maybe_text(spatial_rule),
        ]
    )


def relation_type_for_pair(
    *,
    temporal_ok: bool,
    spatial_ok: bool,
    lag_rule_requested: bool,
    spatial_rule_requested: bool,
) -> str:
    if lag_rule_requested and spatial_rule_requested and temporal_ok and spatial_ok:
        return "spatiotemporal-window-candidate"
    if lag_rule_requested and temporal_ok:
        return "lag-window-candidate"
    if spatial_rule_requested and spatial_ok:
        return "spatial-window-candidate"
    return "temporal-window-candidate"


def build_spatiotemporal_relation_cues(
    *,
    signals: list[dict[str, Any]],
    run_id: str,
    round_id: str,
    skill_name: str,
    db_path: str,
    output_file: Path,
    metadata: dict[str, Any],
    source_role: str,
    target_role: str,
    source_class: str,
    target_class: str,
    observed_after_utc: str,
    observed_before_utc: str,
    lag_min_hours: float | None,
    lag_max_hours: float | None,
    bbox: str,
    max_distance_km: float | None,
    spatial_rule: str,
    limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], dict[str, Any]]:
    bbox_bounds = parse_bbox(bbox)
    warnings: list[dict[str, str]] = []
    if maybe_text(bbox) and bbox_bounds is None:
        warnings.append(
            {
                "code": "invalid-bbox",
                "message": "Structured relation mode ignored --bbox because it was not min_lon,min_lat,max_lon,max_lat.",
            }
        )
    source_candidates = [
        signal
        for signal in signals
        if relation_filter_matches(
            signal,
            role=source_role,
            environment_class=source_class,
            observed_after_utc=observed_after_utc,
            observed_before_utc=observed_before_utc,
            bbox=bbox_bounds,
        )
    ]
    target_candidates = [
        signal
        for signal in signals
        if relation_filter_matches(
            signal,
            role=target_role,
            environment_class=target_class,
            observed_after_utc=observed_after_utc,
            observed_before_utc=observed_before_utc,
            bbox=bbox_bounds,
        )
    ]
    lag_rule_requested = lag_min_hours is not None or lag_max_hours is not None
    spatial_rule_requested = max_distance_km is not None or bool(maybe_text(spatial_rule))
    max_relation_count = max(1, min(500, int(limit or 500)))
    relation_cues: list[dict[str, Any]] = []
    for source in source_candidates:
        source_signal_id = maybe_text(source.get("signal_id"))
        source_timestamp = first_timestamp(source)
        for target in target_candidates:
            target_signal_id = maybe_text(target.get("signal_id"))
            if not source_signal_id or source_signal_id == target_signal_id:
                continue
            target_timestamp = first_timestamp(target)
            delta_hours = timestamp_delta_hours(source_timestamp, target_timestamp)
            distance_km = haversine_km(source, target)
            rejection_reasons: list[str] = []
            temporal_ok = True
            spatial_ok = True
            if delta_hours is None:
                temporal_ok = False
                rejection_reasons.append("timestamp-missing")
            elif delta_hours < 0:
                temporal_ok = False
                rejection_reasons.append("temporal-window-mismatch")
            elif lag_rule_requested:
                if lag_min_hours is not None and delta_hours < lag_min_hours:
                    temporal_ok = False
                    rejection_reasons.append("temporal-window-mismatch")
                if lag_max_hours is not None and delta_hours > lag_max_hours:
                    temporal_ok = False
                    rejection_reasons.append("temporal-window-mismatch")
            if spatial_rule_requested:
                if distance_km is None:
                    spatial_ok = False
                    rejection_reasons.append("coordinate-missing")
                elif max_distance_km is not None and distance_km > max_distance_km:
                    spatial_ok = False
                    rejection_reasons.append("spatial-scope-overbroad")
            if "timestamp-missing" in rejection_reasons or "coordinate-missing" in rejection_reasons:
                relation_type = "insufficient-basis"
                relation_status = "insufficient-basis"
            elif not temporal_ok:
                relation_type = "rejected-by-temporal-rule"
                relation_status = "rejected-by-rule"
            elif not spatial_ok:
                relation_type = "rejected-by-spatial-rule"
                relation_status = "rejected-by-rule"
            else:
                relation_type = relation_type_for_pair(
                    temporal_ok=temporal_ok,
                    spatial_ok=spatial_ok,
                    lag_rule_requested=lag_rule_requested,
                    spatial_rule_requested=spatial_rule_requested,
                )
                relation_status = "candidate"
            context_signals = context_signals_for_relation(
                signals,
                source_signal_id=source_signal_id,
                target_signal_id=target_signal_id,
                source_timestamp=source_timestamp,
                target_timestamp=target_timestamp,
            )
            evidence_signals = [source, target, *context_signals]
            relation_id = "strel-" + stable_hash(
                run_id,
                round_id,
                source_signal_id,
                target_signal_id,
                relation_type,
                lag_min_hours,
                lag_max_hours,
                max_distance_km,
                spatial_rule,
            )[:16]
            relation_cues.append(
                normalize_spatiotemporal_relation_cue_payload(
                    {
                        "run_id": run_id,
                        "round_id": round_id,
                        "relation_id": relation_id,
                        "decision_source": metadata["decision_source"],
                        "relation_type": relation_type,
                        "relation_status": relation_status,
                        "source_signal_id": source_signal_id,
                        "target_signal_id": target_signal_id,
                        "context_signal_ids": lineage_from_signals(context_signals),
                        "source_role": relation_signal_role(source),
                        "target_role": relation_signal_role(target),
                        "temporal_rule": {
                            "rule": "target-observed-after-source",
                            "observed_after_utc": maybe_text(observed_after_utc),
                            "observed_before_utc": maybe_text(observed_before_utc),
                            "lag_min_hours": lag_min_hours,
                            "lag_max_hours": lag_max_hours,
                        },
                        "spatial_rule": {
                            "rule": maybe_text(spatial_rule) or "not-requested",
                            "bbox": maybe_text(bbox),
                            "max_distance_km": max_distance_km,
                        },
                        "lag_window": {
                            "min_hours": lag_min_hours,
                            "max_hours": lag_max_hours,
                        },
                        "time_delta": {"hours": delta_hours} if delta_hours is not None else {},
                        "distance": {"kilometers": distance_km} if distance_km is not None else {},
                        "spatial_basis": {
                            "source_latitude": source.get("latitude"),
                            "source_longitude": source.get("longitude"),
                            "target_latitude": target.get("latitude"),
                            "target_longitude": target.get("longitude"),
                        },
                        "temporal_basis": {
                            "source_timestamp": maybe_text(source_timestamp),
                            "target_timestamp": maybe_text(target_timestamp),
                        },
                        "rejection_reasons": rejection_reasons,
                        "caveats": [
                            "Candidate relation cue only; does not prove causality, transport, source attribution, or exclusion of alternatives."
                        ],
                        "evidence_refs": refs_from_signals(evidence_signals),
                        "lineage": lineage_from_signals(evidence_signals),
                        "provenance": {
                            "source_skill": skill_name,
                            "decision_source": metadata["decision_source"],
                            "db_path": db_path,
                        },
                        "helper_governance": metadata,
                        "relation_rule_ref": metadata["rule_id"],
                        "taxonomy_version": metadata["taxonomy_version"],
                        "verification_scope": {
                            "source_role": maybe_text(source_role),
                            "target_role": maybe_text(target_role),
                            "source_class": maybe_text(source_class),
                            "target_class": maybe_text(target_class),
                            "observed_after_utc": maybe_text(observed_after_utc),
                            "observed_before_utc": maybe_text(observed_before_utc),
                            "lag_min_hours": lag_min_hours,
                            "lag_max_hours": lag_max_hours,
                            "bbox": maybe_text(bbox),
                            "max_distance_km": max_distance_km,
                            "spatial_rule": maybe_text(spatial_rule),
                            "excluded_inferences": [
                                "causality",
                                "transport-proof",
                                "source-attribution-proof",
                            ],
                        },
                    },
                    source_skill=skill_name,
                    artifact_path=str(output_file),
                )
            )
            if len(relation_cues) >= max_relation_count:
                warnings.append(
                    {
                        "code": "relation-pair-limit-reached",
                        "message": f"Structured relation mode stopped after {max_relation_count} relation rows.",
                    }
                )
                return relation_cues, warnings, {
                    "source_candidate_count": len(source_candidates),
                    "target_candidate_count": len(target_candidates),
                    "relation_pair_limit": max_relation_count,
                }
    basis = {
        "source_candidate_count": len(source_candidates),
        "target_candidate_count": len(target_candidates),
        "relation_pair_limit": max_relation_count,
    }
    if not source_candidates:
        warnings.append({"code": "missing-source-candidates", "message": "No signals matched the requested source relation filters."})
    if not target_candidates:
        warnings.append({"code": "missing-target-candidates", "message": "No signals matched the requested target relation filters."})
    if not relation_cues:
        warnings.append({"code": "insufficient-relation-basis", "message": "No structured spatiotemporal relation cues were produced."})
    return relation_cues, warnings, basis


def run_detect_temporal_cooccurrence_cues(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    relation_output_path: str = "",
    source_role: str = "",
    target_role: str = "",
    source_class: str = "",
    target_class: str = "",
    observed_after_utc: str = "",
    observed_before_utc: str = "",
    lag_min_hours: float | None = None,
    lag_max_hours: float | None = None,
    bbox: str = "",
    max_distance_km: float | None = None,
    spatial_rule: str = "",
    taxonomy_version: str = "",
    limit: int = 700,
) -> dict[str, Any]:
    skill_name = "detect-temporal-cooccurrence-cues"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"temporal_cooccurrence_cues_{round_id}.json")
    relation_output_file = resolve_output_path(
        run_dir_path,
        relation_output_path,
        f"spatiotemporal_relation_cues_{round_id}.json",
    )
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    environment_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="environment", limit=limit)
    all_signals = [*public_signals, *formal_signals, *environment_signals]
    resolved_taxonomy_version = maybe_text(taxonomy_version) or ENVIRONMENT_SIGNAL_TAXONOMY_VERSION
    metadata = helper_metadata(
        skill_name=skill_name,
        taxonomy_version=resolved_taxonomy_version,
        rule_trace=["db-signal-temporal-cooccurrence-cues"],
        caveats=[
            "Temporal co-occurrence cues are descriptive only and do not indicate cross-source impact or movement.",
            "Signals without timestamps are excluded from date buckets and reported as limitations.",
            "Legacy same-day cues are not DB-backed spatiotemporal relation conclusions.",
            "Structured relation cues are candidate evidence organization objects only.",
        ],
    )
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_timestamp = 0
    for signal in all_signals:
        key = date_key(first_timestamp(signal))
        if not key:
            missing_timestamp += 1
            continue
        buckets[key].append(signal)
    cues: list[dict[str, Any]] = []
    for date_value, members in sorted(buckets.items()):
        planes = sorted({maybe_text(signal.get("plane")) for signal in members if maybe_text(signal.get("plane"))})
        if len(planes) < 2:
            continue
        cue_id = "temporal-cue-" + stable_hash(run_id, round_id, date_value, planes)[:12]
        cues.append(
            {
                "cue_id": cue_id,
                "date": date_value,
                "cooccurring_planes": planes,
                "signal_count": len(members),
                "source_distribution": signal_source_distribution(members),
                "interpretation_limit": "descriptive-cooccurrence-only",
                "relation_status": "deprecated-legacy-cue",
                "legacy_mode": "legacy-compatible",
                "evidence_refs": refs_from_signals(members),
                "lineage": lineage_from_signals(members),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                "helper_governance": metadata,
            }
        )
    warnings = []
    if missing_timestamp:
        warnings.append({"code": "missing-timestamps", "message": f"{missing_timestamp} signals lacked usable timestamps and were not bucketed."})
    if not cues:
        warnings.append({"code": "insufficient-temporal-basis", "message": "No multi-plane same-day co-occurrence cues could be produced from DB signal timestamps."})
    relation_cues: list[dict[str, Any]] = []
    relation_warnings: list[dict[str, str]] = []
    relation_sync: dict[str, Any] = {}
    relation_basis: dict[str, Any] = {}
    if structured_relation_requested(
        source_role=source_role,
        target_role=target_role,
        source_class=source_class,
        target_class=target_class,
        observed_after_utc=observed_after_utc,
        observed_before_utc=observed_before_utc,
        lag_min_hours=lag_min_hours,
        lag_max_hours=lag_max_hours,
        bbox=bbox,
        max_distance_km=max_distance_km,
        spatial_rule=spatial_rule,
    ):
        relation_cues, relation_warnings, relation_basis = build_spatiotemporal_relation_cues(
            signals=all_signals,
            run_id=run_id,
            round_id=round_id,
            skill_name=skill_name,
            db_path=db_path,
            output_file=relation_output_file,
            metadata=metadata,
            source_role=source_role,
            target_role=target_role,
            source_class=source_class,
            target_class=target_class,
            observed_after_utc=observed_after_utc,
            observed_before_utc=observed_before_utc,
            lag_min_hours=lag_min_hours,
            lag_max_hours=lag_max_hours,
            bbox=bbox,
            max_distance_km=max_distance_km,
            spatial_rule=spatial_rule,
            limit=limit,
        )
        relation_payload = {
            "schema_version": "optional-analysis-spatiotemporal-relation-cues-v1",
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "generated_at_utc": utc_now_iso(),
            "status": "completed" if relation_cues else "insufficient-relation-basis",
            "helper_governance": metadata,
            "taxonomy_version": resolved_taxonomy_version,
            "relation_rule_ref": metadata["rule_id"],
            "verification_scope": {
                "verification_question": "structured-spatiotemporal-relation-cue",
                "source_role": maybe_text(source_role),
                "target_role": maybe_text(target_role),
                "source_class": maybe_text(source_class),
                "target_class": maybe_text(target_class),
                "observed_after_utc": maybe_text(observed_after_utc),
                "observed_before_utc": maybe_text(observed_before_utc),
                "lag_min_hours": lag_min_hours,
                "lag_max_hours": lag_max_hours,
                "bbox": maybe_text(bbox),
                "max_distance_km": max_distance_km,
                "spatial_rule": maybe_text(spatial_rule),
                "excluded_inferences": [
                    "causality",
                    "transport-proof",
                    "source-attribution-proof",
                ],
            },
            "spatiotemporal_relation_cues": relation_cues,
            "relation_cue_count": len(relation_cues),
            "relation_basis": relation_basis,
            "warnings": relation_warnings,
        }
        write_json(relation_output_file, relation_payload)
        from eco_council_runtime.kernel.planes.analysis_plane import sync_spatiotemporal_relation_cue_result_set

        relation_sync = sync_spatiotemporal_relation_cue_result_set(
            run_dir_path,
            expected_run_id=run_id,
            round_id=round_id,
            relation_cues_path=relation_output_file,
        )
    status = "completed" if cues or relation_cues else "insufficient-temporal-basis"
    payload = {
        "schema_version": "optional-analysis-temporal-cooccurrence-cues-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "helper_governance": metadata,
        "temporal_cooccurrence_cues": cues,
        "temporal_basis": {
            "bucket_count": len(buckets),
            "missing_timestamp_count": missing_timestamp,
            "timestamp_fallback": "none",
        },
        "structured_relation_cues": {
            "enabled": bool(relation_cues or relation_warnings or relation_sync),
            "artifact_path": str(relation_output_file) if relation_sync else "",
            "relation_cue_count": len(relation_cues),
            "analysis_sync": relation_sync,
        },
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": status,
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "relation_output_path": str(relation_output_file) if relation_sync else "",
            "cue_count": len(cues),
            "relation_cue_count": len(relation_cues),
            "missing_timestamp_count": missing_timestamp,
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "temporal-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "temporal-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": unique_values(
            [
                *[artifact_ref(output_file, "$.temporal_cooccurrence_cues")],
                *(
                    [artifact_ref(relation_output_file, "$.spatiotemporal_relation_cues")]
                    if relation_sync
                    else []
                ),
            ]
        ),
        "canonical_ids": [
            *[maybe_text(item.get("cue_id")) for item in cues],
            *[maybe_text(item.get("relation_id")) for item in relation_cues],
        ],
        "warnings": [*warnings, *relation_warnings],
        "temporal_cooccurrence_cues": cues,
        "spatiotemporal_relation_cues": relation_cues,
        "analysis_sync": relation_sync,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.temporal_cooccurrence_cues",
            candidate_ids=[
                *[maybe_text(item.get("cue_id")) for item in cues],
                *[maybe_text(item.get("relation_id")) for item in relation_cues],
            ],
            gap_hints=[item["message"] for item in [*warnings, *relation_warnings]],
        ),
    }


RELATION_REJECTION_OBJECTION_MAP = {
    "temporal-window-mismatch": "temporal-window-mismatch",
    "lag-assumption-unsupported": "lag-assumption-unsupported",
    "spatial-scope-overbroad": "spatial-scope-overbroad",
    "spatial-scope-too-narrow": "spatial-scope-too-narrow",
    "coordinate-missing": "coordinate-missing",
    "timestamp-missing": "timestamp-missing",
    "context-variable-missing": "context-variable-missing",
    "provider-quality-limitation": "provider-quality-limitation",
    "taxonomy-misclassification": "taxonomy-misclassification",
}


def relation_objection_candidate(
    *,
    relation: dict[str, Any],
    objection_code: str,
    challenged_rule: str,
    alternative_explanation: str,
    required_followup_evidence: list[str],
    report_risk: str,
    metadata: dict[str, Any],
    db_path: str,
) -> dict[str, Any]:
    relation_id = maybe_text(relation.get("relation_id"))
    source_signal_id = maybe_text(relation.get("source_signal_id"))
    target_signal_id = maybe_text(relation.get("target_signal_id"))
    normalized_objection = maybe_text(objection_code)
    if normalized_objection not in SPATIOTEMPORAL_OBJECTION_CODE_VALUES:
        normalized_objection = "report-overclaim-risk"
    candidate_id = "strel-objection-" + stable_hash(
        relation_id,
        normalized_objection,
        challenged_rule,
        alternative_explanation,
    )[:16]
    return {
        "candidate_id": candidate_id,
        "relation_id": relation_id,
        "relation_type": maybe_text(relation.get("relation_type")),
        "relation_status": maybe_text(relation.get("relation_status")),
        "objection_code": normalized_objection,
        "challenged_rule": maybe_text(challenged_rule),
        "alternative_explanation": maybe_text(alternative_explanation),
        "required_followup_evidence": unique_texts(required_followup_evidence),
        "report_risk": maybe_text(report_risk),
        "evidence_refs": unique_values(list_items(relation.get("evidence_refs"))),
        "lineage": unique_texts(
            [
                relation_id,
                source_signal_id,
                target_signal_id,
                *list_items(relation.get("context_signal_ids")),
                *list_items(relation.get("lineage")),
            ]
        ),
        "provenance": {
            "source_skill": "review-spatiotemporal-relation-alternatives",
            "decision_source": metadata["decision_source"],
            "db_path": db_path,
        },
        "helper_governance": metadata,
    }


def relation_objection_candidates(
    relation: dict[str, Any],
    *,
    metadata: dict[str, Any],
    db_path: str,
) -> list[dict[str, Any]]:
    status = maybe_text(relation.get("relation_status"))
    relation_type = maybe_text(relation.get("relation_type"))
    rejection_reasons = unique_texts(list_items(relation.get("rejection_reasons")))
    candidates: list[dict[str, Any]] = []
    for reason in rejection_reasons:
        objection = RELATION_REJECTION_OBJECTION_MAP.get(reason)
        if not objection:
            continue
        candidates.append(
            relation_objection_candidate(
                relation=relation,
                objection_code=objection,
                challenged_rule=reason,
                alternative_explanation=f"Relation row reports {reason}.",
                required_followup_evidence=[
                    "Review the source and target signal timestamps, coordinates, and evidence refs.",
                ],
                report_risk="do-not-use-as-report-support-until-reviewed",
                metadata=metadata,
                db_path=db_path,
            )
        )
    if status in {"candidate", "weak-candidate", "needs-human-review"}:
        candidates.append(
            relation_objection_candidate(
                relation=relation,
                objection_code="report-overclaim-risk",
                challenged_rule=relation_type or "candidate-relation",
                alternative_explanation=(
                    "Candidate relation cue can organize evidence but cannot prove causality, transport, source attribution, or exclusion of alternatives."
                ),
                required_followup_evidence=[
                    "Confirm report language is limited to candidate relation and uncertainty.",
                ],
                report_risk="overclaim-if-used-as-causality-or-source-attribution",
                metadata=metadata,
                db_path=db_path,
            )
        )
        if not list_items(relation.get("context_signal_ids")):
            candidates.append(
                relation_objection_candidate(
                    relation=relation,
                    objection_code="context-variable-missing",
                    challenged_rule="context_signal_ids",
                    alternative_explanation="No context-observation signal is linked to this candidate relation cue.",
                    required_followup_evidence=[
                        "Add or explicitly waive relevant context observations before strengthening the relation.",
                    ],
                    report_risk="relation-context-gap",
                    metadata=metadata,
                    db_path=db_path,
                )
            )
    if status in {"insufficient-basis", "rejected-by-rule"} and not candidates:
        candidates.append(
            relation_objection_candidate(
                relation=relation,
                objection_code="provider-quality-limitation",
                challenged_rule=status,
                alternative_explanation="Relation status already indicates the row is not sufficient positive support.",
                required_followup_evidence=[
                    "Inspect relation temporal_basis, spatial_basis, caveats, and provider limitations.",
                ],
                report_risk="do-not-report-as-candidate-support",
                metadata=metadata,
                db_path=db_path,
            )
        )
    return candidates


def run_review_spatiotemporal_relation_alternatives(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    relation_id: str = "",
    relation_status: str = "",
    output_path: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    skill_name = "review-spatiotemporal-relation-alternatives"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(
        run_dir_path,
        output_path,
        f"spatiotemporal_relation_alternative_reviews_{round_id}.json",
    )
    metadata = helper_metadata(
        skill_name=skill_name,
        taxonomy_version=ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
        rule_trace=["spatiotemporal-relation-objection-taxonomy"],
        caveats=[
            "Objection candidates are prompts for challenger review, not findings.",
            "This helper does not close, prove, or invalidate relation cues by itself.",
            "A challenge, probe, review comment, finding, or report basis object must explicitly carry any report-facing use.",
        ],
    )
    from eco_council_runtime.kernel.planes.analysis_plane import query_spatiotemporal_relation_cues

    relation_query = query_spatiotemporal_relation_cues(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        relation_id=relation_id,
        relation_status=relation_status,
        latest_only=True,
        limit=limit,
    )
    db_path = (
        maybe_text(relation_query.get("summary", {}).get("db_path"))
        if isinstance(relation_query.get("summary"), dict)
        else ""
    )
    relations = [
        item
        for item in list_items(relation_query.get("relations"))
        if isinstance(item, dict)
    ]
    candidates: list[dict[str, Any]] = []
    for relation in relations:
        candidates.extend(
            relation_objection_candidates(
                relation,
                metadata=metadata,
                db_path=db_path,
            )
        )
    warnings: list[dict[str, Any]] = []
    if not relations:
        warnings.append(
            {
                "code": "no-relation-cues",
                "message": "No spatiotemporal relation cues matched the requested filters.",
            }
        )
    if relations and not candidates:
        warnings.append(
            {
                "code": "no-objection-candidates",
                "message": "No relation objection candidates were produced from the matched relation cues.",
            }
        )
    payload = {
        "schema_version": "optional-analysis-spatiotemporal-relation-alternatives-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed" if candidates else "insufficient-relation-objection-basis",
        "helper_governance": metadata,
        "taxonomy_version": ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
        "relation_filters": {
            "relation_id": maybe_text(relation_id),
            "relation_status": maybe_text(relation_status),
        },
        "relation_query_summary": relation_query.get("summary", {}),
        "objection_candidates": candidates,
        "objection_candidate_count": len(candidates),
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": payload["status"],
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "relation_count": len(relations),
            "objection_candidate_count": len(candidates),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "relation-alt-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "relation-alt-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.objection_candidates")],
        "canonical_ids": [maybe_text(item.get("candidate_id")) for item in candidates],
        "warnings": warnings,
        "objection_candidates": candidates,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.objection_candidates",
            candidate_ids=[maybe_text(item.get("candidate_id")) for item in candidates],
            gap_hints=[item["message"] for item in warnings],
            challenge_hints=[
                "Carry relation objection candidates through open-challenge-ticket, open-falsification-probe, or post-review-comment before using them downstream."
            ],
        ),
    }
