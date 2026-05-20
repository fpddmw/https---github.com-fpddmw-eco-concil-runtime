from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .public_discourse import public_discourse_lane, public_discourse_source_family
from .support import (
    artifact_ref,
    date_key,
    first_timestamp,
    helper_metadata,
    lineage_from_signals,
    list_items,
    maybe_text,
    query_signals,
    refs_from_signals,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    signal_source_distribution,
    stable_hash,
    unique_values,
    utc_now_iso,
    write_json,
)


__all__ = (
    "build_fact_policy_public_interaction_timeline",
    "run_build_fact_policy_public_interaction_timeline",
)


def _load_json_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _helper_artifacts(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    specs = [
        (
            "public_discourse_corpus",
            run_dir / "analytics" / f"public_discourse_corpus_{round_id}.json",
            "corpus_items",
        ),
        (
            "public_discourse_coverage_audit",
            run_dir / "analytics" / f"public_discourse_coverage_audit_{round_id}.json",
            "coverage_cues",
        ),
        (
            "public_discourse_annotation_aggregation",
            run_dir / "analytics" / f"public_discourse_annotation_aggregation_{round_id}.json",
            "semantic_distributions",
        ),
        (
            "public_discourse_sample_summary",
            run_dir / "analytics" / f"public_discourse_sample_summary_{round_id}.json",
            "summary",
        ),
    ]
    artifacts: list[dict[str, Any]] = []
    for artifact_key, path, item_key in specs:
        payload = _load_json_artifact(path)
        artifacts.append(
            {
                "artifact_key": artifact_key,
                "artifact_path": str(path.resolve()),
                "present": bool(payload),
                "item_key": item_key,
                "item_count": len(list_items(payload.get(item_key))),
                "artifact_ref": artifact_ref(path.resolve(), f"$.{item_key}"),
                "payload": payload,
            }
        )
    return artifacts


def _semantic_distribution_cues(helper_artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregation = next(
        (
            artifact
            for artifact in helper_artifacts
            if artifact.get("artifact_key") == "public_discourse_annotation_aggregation"
        ),
        {},
    )
    payload = aggregation.get("payload") if isinstance(aggregation.get("payload"), dict) else {}
    cues: list[dict[str, Any]] = []
    for distribution in list_items(payload.get("semantic_distributions"))[:12]:
        if not isinstance(distribution, dict):
            continue
        cues.append(
            {
                "label_family": maybe_text(distribution.get("label_family")),
                "source_family": maybe_text(distribution.get("source_family")),
                "discourse_lane": maybe_text(distribution.get("discourse_lane")),
                "semantic_scope": maybe_text(distribution.get("semantic_scope")),
                "sample_definition": distribution.get("sample_definition", {})
                if isinstance(distribution.get("sample_definition"), dict)
                else {},
                "denominator_scope": distribution.get("denominator_scope", {})
                if isinstance(distribution.get("denominator_scope"), dict)
                else {},
                "distribution": list_items(distribution.get("distribution"))[:8],
            }
        )
    return cues


def _source_family_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for signal in signals:
        source_family = public_discourse_source_family(signal)
        if not source_family:
            source_family = maybe_text(signal.get("plane")) or "unknown"
        counts[source_family] += 1
    return [
        {"source_family": source_family, "item_count": item_count}
        for source_family, item_count in sorted(counts.items())
        if source_family
    ]


def _lane_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for signal in signals:
        plane = maybe_text(signal.get("plane"))
        lane = public_discourse_lane(signal) if plane in {"public", "formal"} else plane
        if not lane:
            lane = plane or "unknown"
        counts[lane] += 1
    return [
        {"discourse_lane": lane, "item_count": item_count}
        for lane, item_count in sorted(counts.items())
        if lane
    ]


def _signal_excerpt(signal: dict[str, Any], limit: int = 180) -> dict[str, str]:
    title = maybe_text(signal.get("title"))
    body = maybe_text(signal.get("body_text"))
    text = title or body
    if len(text) > limit:
        text = text[: limit - 3].rstrip() + "..."
    return {
        "signal_id": maybe_text(signal.get("signal_id")),
        "source_skill": maybe_text(signal.get("source_skill")),
        "timestamp_utc": maybe_text(first_timestamp(signal)),
        "text": text,
    }


def _side_summary(
    signals: list[dict[str, Any]],
    *,
    side_key: str,
) -> dict[str, Any]:
    timestamps = [
        maybe_text(first_timestamp(signal))
        for signal in signals
        if maybe_text(first_timestamp(signal))
    ]
    return {
        "side_key": side_key,
        "item_count": len(signals),
        "signal_ids": lineage_from_signals(signals),
        "evidence_refs": refs_from_signals(signals),
        "source_skill_counts": signal_source_distribution(signals),
        "source_family_counts": _source_family_counts(signals),
        "discourse_lane_counts": _lane_counts(signals),
        "time_range_utc": {
            "start": min(timestamps) if timestamps else "",
            "end": max(timestamps) if timestamps else "",
        },
        "examples": [_signal_excerpt(signal) for signal in signals[:4]],
    }


def _warning(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _group_by_date(signals: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], int]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_timestamp_count = 0
    for signal in signals:
        key = date_key(first_timestamp(signal))
        if not key:
            missing_timestamp_count += 1
            continue
        buckets[key].append(signal)
    return buckets, missing_timestamp_count


def _context_node(
    *,
    run_id: str,
    round_id: str,
    date_value: str,
    signals: list[dict[str, Any]],
    node_kind: str,
) -> dict[str, Any]:
    node_id = "fpp-context-" + stable_hash(run_id, round_id, date_value, node_kind)[:14]
    return {
        "node_id": node_id,
        "node_kind": node_kind,
        "time_anchor_date": date_value,
        "interaction_status": "single-sided-context",
        "side_summary": _side_summary(signals, side_key=node_kind),
        "claim_boundary": {
            "report_boundary": (
                "Use this as one-sided chronology only; do not write an interaction "
                "claim without both fact/policy-side and public/media-side refs."
            ),
            "excluded_inferences": [
                "causality",
                "policy impact",
                "public response attribution",
                "evidence absence",
            ],
        },
        "evidence_refs": refs_from_signals(signals),
        "lineage": lineage_from_signals(signals),
    }


def _semantic_shift_events(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for previous, current in zip(nodes, nodes[1:]):
        previous_cues = list_items(previous.get("semantic_cues"))
        current_cues = list_items(current.get("semantic_cues"))
        if not previous_cues and not current_cues:
            continue
        events.append(
            {
                "event_id": "fpp-semantic-shift-"
                + stable_hash(previous.get("node_id"), current.get("node_id"))[:14],
                "event_status": "candidate-review-needed",
                "from_node_id": maybe_text(previous.get("node_id")),
                "to_node_id": maybe_text(current.get("node_id")),
                "from_date": maybe_text(previous.get("time_anchor_date")),
                "to_date": maybe_text(current.get("time_anchor_date")),
                "semantic_basis": "sample-local helper distributions only",
                "claim_boundary": {
                    "report_boundary": (
                        "Do not write a semantic shift claim unless comparable "
                        "sample definitions, source-family denominators, and "
                        "council-carried interpretation are present."
                    ),
                    "excluded_inferences": [
                        "public opinion trend",
                        "causal response",
                        "representative semantic change",
                    ],
                },
                "lineage": unique_values(
                    [
                        *list_items(previous.get("lineage")),
                        *list_items(current.get("lineage")),
                    ]
                ),
            }
        )
    return events


def build_fact_policy_public_interaction_timeline(
    *,
    environment_signals: list[dict[str, Any]],
    formal_signals: list[dict[str, Any]],
    public_signals: list[dict[str, Any]],
    run_id: str,
    round_id: str,
    skill_name: str,
    db_path: str,
    output_file: Path,
    metadata: dict[str, Any],
    helper_artifacts: list[dict[str, Any]],
    max_nodes: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]], dict[str, Any]]:
    env_buckets, env_missing = _group_by_date(environment_signals)
    formal_buckets, formal_missing = _group_by_date(formal_signals)
    public_buckets, public_missing = _group_by_date(public_signals)
    all_dates = sorted(set(env_buckets) | set(formal_buckets) | set(public_buckets))
    semantic_cues = _semantic_distribution_cues(helper_artifacts)
    interaction_nodes: list[dict[str, Any]] = []
    parallel_timeline_nodes: list[dict[str, Any]] = []
    max_node_count = max(1, min(500, int(max_nodes or 200)))

    for date_value in all_dates:
        fact_signals = env_buckets.get(date_value, [])
        policy_signals = formal_buckets.get(date_value, [])
        public_media_signals = public_buckets.get(date_value, [])
        fact_or_policy_signals = [*fact_signals, *policy_signals]
        if fact_or_policy_signals and public_media_signals and len(interaction_nodes) < max_node_count:
            node_id = "fpp-node-" + stable_hash(run_id, round_id, date_value)[:16]
            fact_or_policy_refs = refs_from_signals(fact_or_policy_signals)
            public_refs = refs_from_signals(public_media_signals)
            interaction_nodes.append(
                {
                    "node_id": node_id,
                    "node_kind": "fact-policy-public-interaction-context",
                    "time_anchor_date": date_value,
                    "interaction_status": "candidate-context",
                    "fact_side": _side_summary(fact_signals, side_key="fact"),
                    "policy_side": _side_summary(policy_signals, side_key="policy"),
                    "public_media_side": _side_summary(
                        public_media_signals,
                        side_key="public-media",
                    ),
                    "fact_or_policy_evidence_refs": fact_or_policy_refs,
                    "public_or_media_evidence_refs": public_refs,
                    "semantic_cues": semantic_cues,
                    "communication_gap_notes": [
                        "Compare what changed or was officially recorded with what the visible public/media sample discussed on the same date.",
                        "Treat differences as chronology and framing context until a council object carries a stronger claim.",
                    ],
                    "misalignment_and_uncertainty_register": [
                        "Same-date visibility does not establish response, influence, policy effect, or representativeness.",
                        "Source-family denominators and text coverage must be cited for any semantic public claim.",
                    ],
                    "claim_boundary": {
                        "report_boundary": (
                            "Can support bounded wording that fact/policy-side records and "
                            "public/media records were visible in the same timeline window; "
                            "cannot support causality, policy impact, or public sentiment "
                            "without council-carried basis and denominators."
                        ),
                        "excluded_inferences": [
                            "causality",
                            "policy impact",
                            "public response attribution",
                            "representative public opinion",
                            "evidence absence",
                        ],
                    },
                    "evidence_refs": unique_values([*fact_or_policy_refs, *public_refs]),
                    "lineage": lineage_from_signals(
                        [*fact_or_policy_signals, *public_media_signals]
                    ),
                    "provenance": {
                        "source_skill": skill_name,
                        "decision_source": metadata["decision_source"],
                        "db_path": db_path,
                        "artifact_path": str(output_file),
                    },
                    "helper_governance": metadata,
                }
            )
            continue
        if fact_or_policy_signals:
            parallel_timeline_nodes.append(
                _context_node(
                    run_id=run_id,
                    round_id=round_id,
                    date_value=date_value,
                    signals=fact_or_policy_signals,
                    node_kind="fact-policy-only-context",
                )
            )
        if public_media_signals:
            parallel_timeline_nodes.append(
                _context_node(
                    run_id=run_id,
                    round_id=round_id,
                    date_value=date_value,
                    signals=public_media_signals,
                    node_kind="public-media-only-context",
                )
            )

    warnings: list[dict[str, str]] = []
    missing_total = env_missing + formal_missing + public_missing
    if missing_total:
        warnings.append(
            _warning(
                "missing-timestamps",
                f"{missing_total} signals lacked usable timestamps and were not placed on the interaction timeline.",
            )
        )
    if not interaction_nodes:
        warnings.append(
            _warning(
                "insufficient-interaction-basis",
                "No date bucket had both fact/policy-side and public/media-side signal refs.",
            )
        )
    basis = {
        "environment_signal_count": len(environment_signals),
        "formal_signal_count": len(formal_signals),
        "public_signal_count": len(public_signals),
        "missing_timestamp_count": missing_total,
        "date_bucket_count": len(all_dates),
        "interaction_node_limit": max_node_count,
        "helper_artifacts_present": [
            maybe_text(artifact.get("artifact_key"))
            for artifact in helper_artifacts
            if artifact.get("present")
        ],
    }
    return interaction_nodes, parallel_timeline_nodes, warnings, basis


def run_build_fact_policy_public_interaction_timeline(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    max_nodes: int = 200,
    limit: int = 1000,
) -> dict[str, Any]:
    skill_name = "build-fact-policy-public-interaction-timeline"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(
        run_dir_path,
        output_path,
        f"fact_policy_public_interaction_timeline_{round_id}.json",
    )
    environment_signals, db_path = query_signals(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        plane="environment",
        limit=limit,
    )
    formal_signals, _ = query_signals(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        plane="formal",
        limit=limit,
    )
    public_signals, _ = query_signals(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        plane="public",
        limit=limit,
    )
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-signal-date-buckets", "helper-artifact-context"],
        caveats=[
            "Interaction timeline nodes are descriptive chronology/context only.",
            "Nodes do not prove causality, policy impact, public response attribution, representativeness, or evidence absence.",
            "Public semantic claims still require corpus, coverage, denominator, and council/reporting uptake.",
        ],
    )
    helper_artifacts = _helper_artifacts(run_dir_path, round_id)
    interaction_nodes, parallel_nodes, warnings, basis = (
        build_fact_policy_public_interaction_timeline(
            environment_signals=environment_signals,
            formal_signals=formal_signals,
            public_signals=public_signals,
            run_id=run_id,
            round_id=round_id,
            skill_name=skill_name,
            db_path=db_path,
            output_file=output_file,
            metadata=metadata,
            helper_artifacts=helper_artifacts,
            max_nodes=max_nodes,
        )
    )
    helper_artifact_inputs = [
        {
            key: value
            for key, value in artifact.items()
            if key != "payload"
        }
        for artifact in helper_artifacts
    ]
    status = "completed" if interaction_nodes else "insufficient-interaction-basis"
    semantic_shift_events = _semantic_shift_events(interaction_nodes)
    payload = {
        "schema_version": "optional-analysis-fact-policy-public-interaction-timeline-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "helper_governance": metadata,
        "timeline_scope": {
            "bucket_key": "UTC date from first available signal timestamp",
            "fact_side": "environment-plane normalized signals",
            "policy_side": "formal-plane normalized signals",
            "public_media_side": "public-plane normalized signals",
        },
        "interaction_policy": {
            "advisory_only": True,
            "does_not_schedule_or_execute": True,
            "does_not_sort_or_select_sources": True,
            "report_use_requires_council_or_reporting_uptake": True,
            "excluded_inferences": [
                "causality",
                "policy impact",
                "public response attribution",
                "representative public opinion",
                "evidence absence",
            ],
        },
        "observed_input_summary": basis,
        "helper_artifact_inputs": helper_artifact_inputs,
        "interaction_nodes": interaction_nodes,
        "interaction_node_count": len(interaction_nodes),
        "semantic_shift_events": semantic_shift_events,
        "semantic_shift_event_count": len(semantic_shift_events),
        "semantic_shift_policy": {
            "advisory_only": True,
            "report_use_requires_comparable_denominators": True,
            "excluded_inferences": [
                "public opinion trend",
                "causal response",
                "representative semantic change",
            ],
        },
        "parallel_timeline_nodes": parallel_nodes,
        "parallel_timeline_node_count": len(parallel_nodes),
        "warnings": warnings,
        "query_basis": {
            "run_id": run_id,
            "round_id": round_id,
            "db_path": db_path,
            "input_artifact_refs": [
                artifact["artifact_ref"]
                for artifact in helper_artifact_inputs
                if artifact.get("present")
            ],
        },
    }
    write_json(output_file, payload)
    from eco_council_runtime.kernel.planes.analysis_plane import (
        sync_fact_policy_public_interaction_node_result_set,
    )

    analysis_sync = sync_fact_policy_public_interaction_node_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        interaction_timeline_path=output_file,
    )
    payload["analysis_sync"] = analysis_sync
    write_json(output_file, payload)
    return {
        "status": status,
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "interaction_node_count": len(interaction_nodes),
            "parallel_timeline_node_count": len(parallel_nodes),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
            "analysis_kind": analysis_sync.get("analysis_kind"),
        },
        "receipt_id": "fpp-timeline-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "fpp-timeline-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.interaction_nodes")],
        "canonical_ids": [
            maybe_text(item.get("node_id")) for item in interaction_nodes
        ],
        "warnings": warnings,
        "interaction_nodes": interaction_nodes,
        "parallel_timeline_nodes": parallel_nodes,
        "analysis_sync": analysis_sync,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.interaction_nodes",
            candidate_ids=[maybe_text(item.get("node_id")) for item in interaction_nodes],
            gap_hints=[item["message"] for item in warnings],
            challenge_hints=[
                "Treat the timeline as descriptive context, not causality, policy impact, or public response proof."
            ],
        ),
    }
