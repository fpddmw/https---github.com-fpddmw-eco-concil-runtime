from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analysis_objects import (
    WP4_DECISION_SOURCE_APPROVED_HELPER_VIEW,
    wp4_helper_metadata,
)
from .council_objects import query_council_objects
from .kernel.signal_plane_normalizer import ensure_signal_plane_schema


WP4_RULE_IDS: dict[str, str] = {
    "aggregate-environment-evidence": "HEUR-ENV-AGGREGATE-001",
    "review-fact-check-evidence-scope": "HEUR-FACT-SCOPE-001",
    "discover-discourse-issues": "HEUR-DISCOURSE-DISCOVERY-001",
    "suggest-evidence-lanes": "HEUR-EVIDENCE-LANE-001",
    "materialize-research-issue-surface": "HEUR-RESEARCH-ISSUE-SURFACE-001",
    "project-research-issue-views": "HEUR-RESEARCH-ISSUE-PROJECTION-001",
    "export-research-issue-map": "HEUR-RESEARCH-ISSUE-MAP-001",
    "apply-approved-formal-public-taxonomy": "HEUR-TAXONOMY-APPLY-001",
    "compare-formal-public-footprints": "HEUR-FORMAL-PUBLIC-FOOTPRINT-001",
    "identify-representation-audit-cues": "HEUR-REPRESENTATION-AUDIT-001",
    "detect-temporal-cooccurrence-cues": "HEUR-TEMPORAL-COOCCURRENCE-001",
}

def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_output_path(run_dir: Path, output_path: str, default_name: str) -> Path:
    text = maybe_text(output_path)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def parse_json_text(value: Any, default: Any) -> Any:
    text = maybe_text(value)
    if not text:
        return default
    try:
        payload = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return payload if isinstance(payload, type(default)) else default


def unique_values(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    results: list[Any] = []
    for value in values:
        try:
            key = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        except TypeError:
            key = maybe_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        results.append(value)
    return results


def unique_texts(values: list[Any]) -> list[str]:
    return [maybe_text(value) for value in unique_values(values) if maybe_text(value)]


def artifact_ref(path: Path, locator: str = "$") -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": locator,
        "artifact_ref": f"{path}:{locator}",
    }


def helper_metadata(
    *,
    skill_name: str,
    decision_source: str = WP4_DECISION_SOURCE_APPROVED_HELPER_VIEW,
    rule_id: str = "",
    destination: str = "",
    taxonomy_version: str = "",
    rubric_version: str = "",
    approval_ref: str = "required:skill_approval_request",
    rule_trace: list[Any] | None = None,
    caveats: list[Any] | None = None,
    helper_status: str = "approval-gated-helper-view",
) -> dict[str, Any]:
    return wp4_helper_metadata(
        skill_name=skill_name,
        rule_id=maybe_text(rule_id) or WP4_RULE_IDS.get(skill_name, ""),
        destination=destination or skill_name,
        decision_source=decision_source,
        taxonomy_version=taxonomy_version,
        rubric_version=rubric_version,
        approval_ref=approval_ref,
        audit_ref="docs/openclaw-wp4-skills-refactor-workplan.md#8",
        rule_trace=list(rule_trace or []),
        caveats=list(caveats or []),
        audit_status="default-frozen; approval-required; audit-pending",
        helper_status=helper_status,
    )


def safe_board_handoff(
    *,
    artifact_path: Path,
    locator: str,
    candidate_ids: list[str],
    gap_hints: list[str] | None = None,
    challenge_hints: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "candidate_ids": unique_texts(candidate_ids),
        "evidence_refs": [artifact_ref(artifact_path, locator)],
        "gap_hints": unique_texts(gap_hints or []),
        "challenge_hints": unique_texts(
            challenge_hints
            or [
                "Review helper scope, taxonomy/rubric, source coverage, aggregation, framing, and report usage before citing this artifact."
            ]
        ),
        "suggested_next_skills": [],
    }


def connect_signal_db(run_dir: Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    path_text = maybe_text(db_path)
    db_file = Path(path_text).expanduser() if path_text else run_dir / "analytics" / "signal_plane.sqlite"
    if not db_file.is_absolute():
        db_file = run_dir / db_file
    db_file = db_file.resolve()
    db_file.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_file)
    connection.row_factory = sqlite3.Row
    ensure_signal_plane_schema(connection)
    return connection, db_file


def signal_evidence_ref(row: sqlite3.Row) -> dict[str, str]:
    artifact_path = maybe_text(row["artifact_path"])
    record_locator = maybe_text(row["record_locator"]) or "$"
    return {
        "signal_id": maybe_text(row["signal_id"]),
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": f"{artifact_path}:{record_locator}" if artifact_path else maybe_text(row["signal_id"]),
    }


def row_to_signal(row: sqlite3.Row) -> dict[str, Any]:
    metadata = parse_json_text(row["metadata_json"], {})
    quality_flags = parse_json_text(row["quality_flags_json"], [])
    raw_payload = parse_json_text(row["raw_json"], {})
    return {
        "signal_id": maybe_text(row["signal_id"]),
        "run_id": maybe_text(row["run_id"]),
        "round_id": maybe_text(row["round_id"]),
        "plane": maybe_text(row["plane"]),
        "source_skill": maybe_text(row["source_skill"]),
        "signal_kind": maybe_text(row["signal_kind"]),
        "canonical_object_kind": maybe_text(row["canonical_object_kind"]),
        "title": maybe_text(row["title"]),
        "body_text": maybe_text(row["body_text"]),
        "author_name": maybe_text(row["author_name"]),
        "channel_name": maybe_text(row["channel_name"]),
        "metric": maybe_text(row["metric"]),
        "numeric_value": row["numeric_value"],
        "unit": maybe_text(row["unit"]),
        "published_at_utc": maybe_text(row["published_at_utc"]),
        "observed_at_utc": maybe_text(row["observed_at_utc"]),
        "window_start_utc": maybe_text(row["window_start_utc"]),
        "window_end_utc": maybe_text(row["window_end_utc"]),
        "captured_at_utc": maybe_text(row["captured_at_utc"]),
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "metadata": metadata,
        "quality_flags": quality_flags,
        "raw": raw_payload,
        "evidence_refs": [signal_evidence_ref(row)],
    }


def query_signals(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    plane: str = "",
    limit: int = 200,
    db_path: str = "",
) -> tuple[list[dict[str, Any]], str]:
    connection, db_file = connect_signal_db(run_dir, db_path)
    clauses = ["run_id = ?"]
    params: list[Any] = [run_id]
    if maybe_text(round_id):
        clauses.append("round_id = ?")
        params.append(round_id)
    if maybe_text(plane):
        clauses.append("plane = ?")
        params.append(plane)
    query = (
        "SELECT * FROM normalized_signals WHERE "
        + " AND ".join(clauses)
        + " ORDER BY COALESCE(NULLIF(observed_at_utc, ''), NULLIF(published_at_utc, ''), signal_id), signal_id LIMIT ?"
    )
    try:
        rows = connection.execute(query, tuple([*params, max(1, min(1000, int(limit or 200)))])).fetchall()
    finally:
        connection.close()
    return [row_to_signal(row) for row in rows], str(db_file)


def first_timestamp(signal: dict[str, Any]) -> str:
    for key in ("observed_at_utc", "published_at_utc", "window_start_utc", "window_end_utc", "captured_at_utc"):
        text = maybe_text(signal.get(key))
        if text:
            return text
    return ""


def date_key(value: str) -> str:
    text = maybe_text(value)
    if len(text) >= 10:
        return text[:10]
    return ""


def text_terms(text: str, *, min_len: int = 4, limit: int = 12) -> list[str]:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in maybe_text(text))
    stop = {
        "about",
        "after",
        "also",
        "because",
        "before",
        "from",
        "have",
        "into",
        "that",
        "their",
        "there",
        "this",
        "with",
        "would",
        "should",
    }
    counts = Counter(
        token
        for token in cleaned.split()
        if len(token) >= min_len and token not in stop and not token.isdigit()
    )
    return [token for token, _ in counts.most_common(limit)]


def signal_source_distribution(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(maybe_text(signal.get("source_skill")) for signal in signals)
    return [
        {"source_skill": key, "signal_count": count}
        for key, count in sorted(counts.items())
        if key
    ]


def signal_metric_distribution(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for signal in signals:
        metric = maybe_text(signal.get("metric")) or "unspecified"
        value = signal.get("numeric_value")
        if isinstance(value, (int, float)):
            buckets[metric].append(float(value))
        else:
            buckets.setdefault(metric, [])
    results: list[dict[str, Any]] = []
    for metric, values in sorted(buckets.items()):
        item: dict[str, Any] = {"metric": metric, "signal_count": len(values)}
        if values:
            item.update(
                {
                    "numeric_count": len(values),
                    "min_value": min(values),
                    "max_value": max(values),
                    "average_value": round(sum(values) / len(values), 4),
                }
            )
        else:
            item["numeric_count"] = 0
        results.append(item)
    return results


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
        "wp4_helper_metadata": metadata,
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
        "schema_version": "wp4-environment-evidence-aggregation-v1",
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


def required_scope_fields(payload: dict[str, Any]) -> list[str]:
    required = [
        "verification_question",
        "geographic_scope",
        "study_period",
        "evidence_window",
        "lag_assumptions",
        "metric_requirements",
        "source_requirements",
    ]
    return [field for field in required if not maybe_text(payload.get(field))]


def run_review_fact_check_evidence_scope(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    verification_question: str = "",
    geographic_scope: str = "",
    study_period: str = "",
    evidence_window: str = "",
    lag_assumptions: str = "",
    metric_requirements: str = "",
    source_requirements: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "review-fact-check-evidence-scope"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"fact_check_evidence_scope_review_{round_id}.json")
    scope = {
        "verification_question": maybe_text(verification_question),
        "geographic_scope": maybe_text(geographic_scope),
        "study_period": maybe_text(study_period),
        "evidence_window": maybe_text(evidence_window),
        "lag_assumptions": maybe_text(lag_assumptions),
        "metric_requirements": maybe_text(metric_requirements),
        "source_requirements": maybe_text(source_requirements),
    }
    missing_fields = required_scope_fields(scope)
    signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="environment", limit=limit)
    metric_terms = set(text_terms(scope["metric_requirements"], min_len=2, limit=20))
    source_terms = set(text_terms(scope["source_requirements"], min_len=2, limit=20))
    metric_matches = [
        signal
        for signal in signals
        if not metric_terms or maybe_text(signal.get("metric")).casefold() in metric_terms
    ]
    source_matches = [
        signal
        for signal in signals
        if not source_terms or maybe_text(signal.get("source_skill")).casefold() in source_terms
    ]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["explicit-scope-required", "db-environment-signal-scope-review"],
        caveats=[
            "This helper checks evidence scope coverage only and never outputs factual outcome labels.",
            "A finding or evidence bundle must cite this review before report use.",
        ],
    )
    review_id = "factscope-" + stable_hash(run_id, round_id, json.dumps(scope, sort_keys=True))[:12]
    review = {
        "review_id": review_id,
        "run_id": run_id,
        "round_id": round_id,
        "wp4_helper_metadata": metadata,
        "scope": scope,
        "scope_status": "missing-required-scope" if missing_fields else "reviewed-with-caveats",
        "missing_required_fields": missing_fields,
        "environment_signal_count": len(signals),
        "scope_coverage_notes": [
            {
                "dimension": "metric-requirements",
                "status": "not-reviewable" if missing_fields else "descriptive-match-count",
                "summary": f"{len(metric_matches)} environment signals match requested metric terms descriptively.",
            },
            {
                "dimension": "source-requirements",
                "status": "not-reviewable" if missing_fields else "descriptive-match-count",
                "summary": f"{len(source_matches)} environment signals match requested source terms descriptively.",
            },
            {
                "dimension": "time-place-scope",
                "status": "requires-human-review",
                "summary": "Geographic and study-period scope are recorded for audit; this helper does not decide factual consistency.",
            },
        ],
        "disabled_judgement_surface": "factual-outcome-labels-disabled",
        "evidence_refs": unique_values([ref for signal in signals for ref in list_items(signal.get("evidence_refs"))]),
        "lineage": [maybe_text(signal.get("signal_id")) for signal in signals],
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
    }
    warnings = [
        {
            "code": "missing-required-scope",
            "message": "Explicit verification question, geographic scope, study period, evidence window, lag assumptions, metric requirements, and source requirements are required.",
        }
    ] if missing_fields else []
    payload = {
        "schema_version": "wp4-fact-check-evidence-scope-review-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "scope-required" if missing_fields else "completed",
        "review": review,
        "observed_inputs": {"db_path": db_path, "environment_signal_count": len(signals)},
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
            "review_id": review_id,
            "scope_status": review["scope_status"],
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "factscope-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "factscope-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.review")],
        "canonical_ids": [review_id] if not missing_fields else [],
        "warnings": warnings,
        "review": review,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.review",
            candidate_ids=[review_id] if not missing_fields else [],
            gap_hints=[f"Missing explicit scope fields: {', '.join(missing_fields)}"] if missing_fields else [],
        ),
    }


def run_discover_discourse_issues(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    limit: int = 300,
) -> dict[str, Any]:
    skill_name = "discover-discourse-issues"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"discourse_issue_discovery_{round_id}.json")
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    source_signals = [*public_signals, *formal_signals]
    term_counter = Counter()
    for signal in source_signals:
        term_counter.update(text_terms(" ".join([maybe_text(signal.get("title")), maybe_text(signal.get("body_text"))]), limit=20))
    terms = [term for term, _ in term_counter.most_common(8)]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-public-formal-signal-discourse-hints"],
        caveats=[
            "Discourse issue hints are not factual claims and do not define study scope.",
            "Mentioned scope metadata records text mentions only.",
        ],
    )
    hints: list[dict[str, Any]] = []
    if source_signals:
        for index, term in enumerate(terms or ["general-discourse"], start=1):
            members = [
                signal
                for signal in source_signals
                if term == "general-discourse" or term in " ".join([maybe_text(signal.get("title")), maybe_text(signal.get("body_text"))]).casefold()
            ]
            hint_id = "discourse-hint-" + stable_hash(run_id, round_id, term, index)[:12]
            snippets = [
                maybe_text(signal.get("body_text") or signal.get("title"))[:220]
                for signal in members[:5]
            ]
            hints.append(
                {
                    "hint_id": hint_id,
                    "run_id": run_id,
                    "round_id": round_id,
                    "hint_label": term,
                    "hint_kind": "public-discourse-issue-hint",
                    "member_signal_ids": [maybe_text(signal.get("signal_id")) for signal in members],
                    "text_evidence_snippets": snippets,
                    "source_distribution": signal_source_distribution(members),
                    "taxonomy_labels": [],
                    "mentioned_scope_metadata": {
                        "mentioned_places": [],
                        "mentioned_time_refs": unique_texts([first_timestamp(signal) for signal in members])[:8],
                        "mentioned_metrics": unique_texts([maybe_text(signal.get("metric")) for signal in members if maybe_text(signal.get("metric"))]),
                        "mentioned_policy_objects": [],
                        "mentioned_actors": unique_texts([maybe_text(signal.get("author_name")) for signal in members if maybe_text(signal.get("author_name"))])[:8],
                    },
                    "coverage_caveats": [
                        "Grouping confidence describes reversible text grouping only, not truth, importance, or representativeness."
                    ],
                    "evidence_refs": unique_values([ref for signal in members for ref in list_items(signal.get("evidence_refs"))]),
                    "lineage": [maybe_text(signal.get("signal_id")) for signal in members],
                    "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                }
            )
    payload = {
        "schema_version": "wp4-discourse-issue-discovery-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "wp4_helper_metadata": metadata,
        "discourse_issue_hints": hints,
        "observed_inputs": {"db_path": db_path, "public_signal_count": len(public_signals), "formal_signal_count": len(formal_signals)},
        "warnings": [] if hints else [{"code": "no-discourse-signals", "message": "No public or formal discourse signals were available."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "hint_count": len(hints),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "discourse-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "discourse-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.discourse_issue_hints")],
        "canonical_ids": [maybe_text(hint.get("hint_id")) for hint in hints],
        "warnings": payload["warnings"],
        "discourse_issue_hints": hints,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.discourse_issue_hints",
            candidate_ids=[maybe_text(hint.get("hint_id")) for hint in hints],
            gap_hints=[] if hints else ["No discourse issue hints were available."],
        ),
    }


def load_json_file(path_text: str, default: Any) -> Any:
    text = maybe_text(path_text)
    if not text:
        return default
    path = Path(text).expanduser()
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return default
    return payload


def run_suggest_evidence_lanes(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    skill_name = "suggest-evidence-lanes"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"evidence_lane_suggestions_{round_id}.json")
    input_payload = load_json_file(input_path, {})
    hints = list_items(input_payload.get("discourse_issue_hints")) if isinstance(input_payload, dict) else []
    if not hints:
        findings = query_council_objects(run_dir_path, object_kind="finding", run_id=run_id, round_id=round_id, limit=100).get("objects", [])
        hints = [
            {
                "hint_id": maybe_text(item.get("finding_id")),
                "hint_label": maybe_text(item.get("title") or item.get("summary")),
                "text_evidence_snippets": [maybe_text(item.get("summary"))],
                "evidence_refs": list_items(item.get("evidence_refs")),
                "lineage": [maybe_text(item.get("finding_id"))],
            }
            for item in list_items(findings)
        ]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["advisory-evidence-lane-keyword-cues"],
        caveats=[
            "Lane suggestions are advisory tags only and cannot drive workflow, source queue, or phase transitions.",
            "This helper does not assign owners or readiness posture.",
        ],
    )
    suggestions: list[dict[str, Any]] = []
    for index, hint in enumerate(hints, start=1):
        text = " ".join([maybe_text(hint.get("hint_label")), " ".join(maybe_text(item) for item in list_items(hint.get("text_evidence_snippets")))]).casefold()
        lanes: list[str] = []
        if any(token in text for token in ("air", "water", "smoke", "flood", "soil", "river", "emission", "pollution")):
            lanes.append("environmental-evidence")
        if any(token in text for token in ("permit", "rule", "agency", "docket", "eia", "regulation")):
            lanes.append("formal-record")
        if any(token in text for token in ("community", "resident", "stakeholder", "concern", "public")):
            lanes.append("public-discourse")
        if not lanes:
            lanes.append("general-policy-research")
        suggestion_id = "lane-suggestion-" + stable_hash(run_id, round_id, maybe_text(hint.get("hint_id")), index)[:12]
        suggestions.append(
            {
                "suggestion_id": suggestion_id,
                "source_hint_id": maybe_text(hint.get("hint_id")),
                "advisory_lanes": unique_texts(lanes),
                "review_status": "advisory-only",
                "disabled_workflow_controls": ["owner-assignment", "queue-driver", "phase-transition"],
                "evidence_refs": list_items(hint.get("evidence_refs")),
                "lineage": unique_texts([maybe_text(hint.get("hint_id")), *list_items(hint.get("lineage"))]),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
            }
        )
    payload = {
        "schema_version": "wp4-evidence-lane-suggestions-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "wp4_helper_metadata": metadata,
        "suggestions": suggestions,
        "warnings": [] if suggestions else [{"code": "no-approved-inputs", "message": "No discovery hints or finding records were available."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "suggestion_count": len(suggestions), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "lane-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "lane-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.suggestions")],
        "canonical_ids": [maybe_text(item.get("suggestion_id")) for item in suggestions],
        "warnings": payload["warnings"],
        "suggestions": suggestions,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.suggestions", candidate_ids=[maybe_text(item.get("suggestion_id")) for item in suggestions]),
    }


def signal_text(signal: dict[str, Any]) -> str:
    metadata = dict_items(signal.get("metadata"))
    raw = dict_items(signal.get("raw"))
    return " ".join(
        maybe_text(value)
        for value in (
            signal.get("title"),
            signal.get("body_text"),
            signal.get("author_name"),
            signal.get("channel_name"),
            metadata.get("docket_id"),
            metadata.get("agency_id"),
            metadata.get("submitter_name"),
            " ".join(unique_texts(list_items(metadata.get("issue_labels")))),
            " ".join(unique_texts(list_items(metadata.get("issue_terms")))),
            " ".join(unique_texts(list_items(metadata.get("concern_facets")))),
            " ".join(unique_texts(list_items(metadata.get("evidence_citation_types")))),
            maybe_text(raw.get("comment")),
            maybe_text(raw.get("text")),
        )
        if maybe_text(value)
    )


def issue_terms_for_signal(signal: dict[str, Any]) -> list[str]:
    metadata = dict_items(signal.get("metadata"))
    values: list[Any] = []
    values.extend(list_items(metadata.get("issue_labels")))
    values.extend(list_items(metadata.get("issue_terms")))
    values.extend(text_terms(signal_text(signal), limit=8))
    return unique_texts(values)[:8]


def load_issue_hints_from_path(input_path: str) -> list[dict[str, Any]]:
    payload = load_json_file(input_path, {})
    if not isinstance(payload, dict):
        return []
    for field_name in (
        "research_issues",
        "issue_views",
        "discourse_issue_hints",
        "suggestions",
    ):
        items = list_items(payload.get(field_name))
        if items:
            return [dict_items(item) for item in items]
    return []


def refs_from_signals(signals: list[dict[str, Any]]) -> list[Any]:
    return unique_values([ref for signal in signals for ref in list_items(signal.get("evidence_refs"))])


def lineage_from_signals(signals: list[dict[str, Any]]) -> list[str]:
    return unique_texts([maybe_text(signal.get("signal_id")) for signal in signals])


def run_materialize_research_issue_surface(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str = "",
    output_path: str = "",
    limit: int = 400,
) -> dict[str, Any]:
    skill_name = "materialize-research-issue-surface"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"research_issue_surface_{round_id}.json")
    loaded_hints = load_issue_hints_from_path(input_path)
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    signals = [*public_signals, *formal_signals]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-discourse-signal-issue-surface"],
        caveats=[
            "Research issue records are reversible issue-surface cues, not factual conclusions.",
            "Report use requires moderator-approved DB basis objects.",
        ],
    )
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        terms = issue_terms_for_signal(signal)
        bucket = terms[0] if terms else "general-policy-issue"
        buckets[bucket].append(signal)
    if loaded_hints and not buckets:
        for hint in loaded_hints:
            label = maybe_text(hint.get("issue_label") or hint.get("hint_label") or hint.get("source_hint_id") or "input-policy-issue")
            buckets[label].append(
                {
                    "signal_id": maybe_text(hint.get("issue_id") or hint.get("hint_id") or hint.get("suggestion_id")),
                    "title": label,
                    "body_text": " ".join(maybe_text(item) for item in list_items(hint.get("text_evidence_snippets"))),
                    "source_skill": maybe_text(hint.get("skill")),
                    "evidence_refs": list_items(hint.get("evidence_refs")),
                    "metadata": {},
                    "raw": {},
                }
            )
    issues: list[dict[str, Any]] = []
    for index, (label, members) in enumerate(sorted(buckets.items()), start=1):
        issue_id = "research-issue-" + stable_hash(run_id, round_id, label, index)[:12]
        member_terms = unique_texts([term for signal in members for term in issue_terms_for_signal(signal)])
        issue = {
            "issue_id": issue_id,
            "run_id": run_id,
            "round_id": round_id,
            "issue_label": maybe_text(label),
            "issue_terms": member_terms[:12],
            "source_signal_ids": lineage_from_signals(members),
            "source_distribution": signal_source_distribution(members),
            "issue_surface_status": "candidate-for-human-review",
            "report_usage": "appendix-or-audit-only-until-db-basis-cites-it",
            "evidence_refs": refs_from_signals(members),
            "lineage": lineage_from_signals(members),
            "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
            "wp4_helper_metadata": metadata,
        }
        issues.append(issue)
    payload = {
        "schema_version": "wp4-research-issue-surface-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "wp4_helper_metadata": metadata,
        "research_issues": issues,
        "observed_inputs": {
            "db_path": db_path,
            "public_signal_count": len(public_signals),
            "formal_signal_count": len(formal_signals),
            "input_hint_count": len(loaded_hints),
        },
        "warnings": [] if issues else [{"code": "no-issue-surface-inputs", "message": "No DB discourse signals or approved input hints were available."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "issue_count": len(issues), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "issue-surface-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "issue-surface-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.research_issues")],
        "canonical_ids": [maybe_text(item.get("issue_id")) for item in issues],
        "warnings": payload["warnings"],
        "research_issues": issues,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.research_issues", candidate_ids=[maybe_text(item.get("issue_id")) for item in issues]),
    }


def run_project_research_issue_views(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str = "",
    output_path: str = "",
    limit: int = 400,
) -> dict[str, Any]:
    skill_name = "project-research-issue-views"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"research_issue_views_{round_id}.json")
    issues = load_issue_hints_from_path(input_path)
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    signals = [*public_signals, *formal_signals]
    if not issues and signals:
        issues = [
            {
                "issue_id": "research-issue-" + stable_hash(run_id, round_id, "all-discourse")[:12],
                "issue_label": "round-discourse-surface",
                "issue_terms": unique_texts([term for signal in signals for term in issue_terms_for_signal(signal)])[:12],
                "source_signal_ids": lineage_from_signals(signals),
                "evidence_refs": refs_from_signals(signals),
                "lineage": lineage_from_signals(signals),
            }
        ]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-discourse-typed-projection-cues"],
        caveats=[
            "Typed projections are candidate cues only and must remain auditable.",
            "This helper does not write report prose or research conclusions.",
        ],
    )
    views: list[dict[str, Any]] = []
    for index, issue in enumerate(issues, start=1):
        issue_terms = unique_texts(list_items(issue.get("issue_terms")))
        issue_ids = set(unique_texts(list_items(issue.get("source_signal_ids"))))
        members = [signal for signal in signals if not issue_ids or maybe_text(signal.get("signal_id")) in issue_ids]
        if not members and signals:
            members = signals
        metadata_items = [dict_items(signal.get("metadata")) for signal in members]
        actor_cues = unique_texts(
            [signal.get("author_name") for signal in members]
            + [metadata.get("submitter_name") for metadata in metadata_items]
        )[:20]
        concern_cues = unique_texts(
            [
                cue
                for metadata in metadata_items
                for cue in list_items(metadata.get("concern_facets"))
            ]
            + issue_terms
        )[:20]
        citation_cues = unique_texts(
            [
                cue
                for metadata in metadata_items
                for cue in list_items(metadata.get("evidence_citation_types"))
            ]
        )[:20]
        stance_cues = unique_texts([metadata.get("stance_hint") for metadata in metadata_items])[:20]
        view_id = "issue-view-" + stable_hash(run_id, round_id, maybe_text(issue.get("issue_id")), index)[:12]
        views.append(
            {
                "view_id": view_id,
                "issue_id": maybe_text(issue.get("issue_id")),
                "issue_label": maybe_text(issue.get("issue_label")),
                "typed_cues": {
                    "actor_cues": actor_cues,
                    "concern_cues": concern_cues,
                    "citation_cues": citation_cues,
                    "stance_cues": stance_cues,
                },
                "projection_status": "candidate-for-human-review",
                "evidence_refs": refs_from_signals(members) or list_items(issue.get("evidence_refs")),
                "lineage": unique_texts([maybe_text(issue.get("issue_id")), *lineage_from_signals(members), *list_items(issue.get("lineage"))]),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                "wp4_helper_metadata": metadata,
            }
        )
    payload = {
        "schema_version": "wp4-research-issue-views-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "wp4_helper_metadata": metadata,
        "issue_views": views,
        "observed_inputs": {"db_path": db_path, "issue_count": len(issues), "signal_count": len(signals)},
        "warnings": [] if views else [{"code": "no-issue-view-inputs", "message": "No issue surface or DB discourse signals were available."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "view_count": len(views), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "issue-view-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "issue-view-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.issue_views")],
        "canonical_ids": [maybe_text(item.get("view_id")) for item in views],
        "warnings": payload["warnings"],
        "issue_views": views,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.issue_views", candidate_ids=[maybe_text(item.get("view_id")) for item in views]),
    }


def run_export_research_issue_map(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    issue_surface_path: str = "",
    issue_views_path: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    skill_name = "export-research-issue-map"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"research_issue_map_{round_id}.json")
    issues = load_issue_hints_from_path(issue_surface_path)
    views = load_issue_hints_from_path(issue_views_path)
    if not issues:
        default_surface = run_dir_path / "analytics" / f"research_issue_surface_{round_id}.json"
        issues = load_issue_hints_from_path(str(default_surface))
    if not views:
        default_views = run_dir_path / "analytics" / f"research_issue_views_{round_id}.json"
        views = load_issue_hints_from_path(str(default_views))
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["research-issue-map-export"],
        caveats=[
            "The issue map is a navigation export, not a conclusion graph.",
            "Edges are traceability cues only and do not imply causal relationships.",
        ],
    )
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for issue in issues:
        issue_id = maybe_text(issue.get("issue_id") or issue.get("hint_id"))
        if not issue_id:
            continue
        nodes.append(
            {
                "node_id": issue_id,
                "node_kind": "research-issue",
                "label": maybe_text(issue.get("issue_label") or issue.get("hint_label")),
                "evidence_refs": list_items(issue.get("evidence_refs")),
                "lineage": list_items(issue.get("lineage")),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
            }
        )
    for view in views:
        view_id = maybe_text(view.get("view_id"))
        issue_id = maybe_text(view.get("issue_id"))
        if not view_id:
            continue
        nodes.append(
            {
                "node_id": view_id,
                "node_kind": "issue-view",
                "label": maybe_text(view.get("issue_label")),
                "evidence_refs": list_items(view.get("evidence_refs")),
                "lineage": list_items(view.get("lineage")),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
            }
        )
        if issue_id:
            edges.append(
                {
                    "edge_id": "issue-map-edge-" + stable_hash(run_id, round_id, issue_id, view_id)[:12],
                    "from_node_id": issue_id,
                    "to_node_id": view_id,
                    "relationship_kind": "traceability-cue",
                    "evidence_refs": list_items(view.get("evidence_refs")),
                    "lineage": unique_texts([issue_id, view_id, *list_items(view.get("lineage"))]),
                    "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
                }
            )
    issue_map = {
        "map_id": "research-issue-map-" + stable_hash(run_id, round_id, len(nodes), len(edges))[:12],
        "run_id": run_id,
        "round_id": round_id,
        "nodes": nodes,
        "edges": edges,
        "map_status": "navigation-export",
        "wp4_helper_metadata": metadata,
    }
    payload = {
        "schema_version": "wp4-research-issue-map-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "research_issue_map": issue_map,
        "warnings": [] if nodes else [{"code": "no-map-inputs", "message": "No issue surface or issue views were available."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "node_count": len(nodes), "edge_count": len(edges), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "issue-map-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "issue-map-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.research_issue_map")],
        "canonical_ids": [issue_map["map_id"]] if nodes else [],
        "warnings": payload["warnings"],
        "research_issue_map": issue_map,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.research_issue_map", candidate_ids=[issue_map["map_id"]] if nodes else []),
    }


def taxonomy_labels(taxonomy_payload: Any) -> list[dict[str, Any]]:
    if isinstance(taxonomy_payload, list):
        raw_items = taxonomy_payload
    elif isinstance(taxonomy_payload, dict):
        raw_items = list_items(taxonomy_payload.get("labels") or taxonomy_payload.get("taxonomy_labels"))
    else:
        raw_items = []
    labels: list[dict[str, Any]] = []
    for item in raw_items:
        if isinstance(item, str):
            labels.append({"label": maybe_text(item), "terms": [maybe_text(item)]})
        elif isinstance(item, dict):
            label = maybe_text(item.get("label") or item.get("name") or item.get("id"))
            terms = unique_texts(list_items(item.get("terms")) + [label])
            if label:
                labels.append({"label": label, "terms": terms, "description": maybe_text(item.get("description"))})
    return labels


def run_apply_approved_formal_public_taxonomy(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    taxonomy_path: str = "",
    taxonomy_version: str = "",
    approval_ref: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "apply-approved-formal-public-taxonomy"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"formal_public_taxonomy_labels_{round_id}.json")
    taxonomy_payload = load_json_file(taxonomy_path, {})
    labels = taxonomy_labels(taxonomy_payload)
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    metadata = helper_metadata(
        skill_name=skill_name,
        taxonomy_version=maybe_text(taxonomy_version) or maybe_text(dict_items(taxonomy_payload).get("version")),
        approval_ref=maybe_text(approval_ref) or "required:approved_taxonomy_record",
        rule_trace=["approved-taxonomy-label-cues"],
        caveats=[
            "No global taxonomy is applied without an explicit approved taxonomy file or record.",
            "Labels are candidate cues and require human audit before report use.",
        ],
    )
    signals = [*public_signals, *formal_signals]
    label_cues: list[dict[str, Any]] = []
    if labels:
        for signal in signals:
            text = signal_text(signal).casefold()
            matched_labels: list[str] = []
            for label in labels:
                terms = unique_texts(list_items(label.get("terms")))
                if any(term.casefold() in text for term in terms if term):
                    matched_labels.append(maybe_text(label.get("label")))
            if not matched_labels:
                continue
            cue_id = "taxonomy-cue-" + stable_hash(run_id, round_id, signal.get("signal_id"), matched_labels)[:12]
            label_cues.append(
                {
                    "cue_id": cue_id,
                    "signal_id": maybe_text(signal.get("signal_id")),
                    "plane": maybe_text(signal.get("plane")),
                    "candidate_labels": unique_texts(matched_labels),
                    "taxonomy_version": metadata["taxonomy_version"],
                    "taxonomy_approval_ref": metadata["approval_ref"],
                    "audit_status": "candidate-for-human-review",
                    "evidence_refs": list_items(signal.get("evidence_refs")),
                    "lineage": [maybe_text(signal.get("signal_id"))],
                    "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                    "wp4_helper_metadata": metadata,
                }
            )
    warnings = []
    status = "completed"
    if not labels:
        status = "taxonomy-required"
        warnings.append({"code": "taxonomy-required", "message": "Provide an approved mission-scoped taxonomy before applying labels."})
    elif not label_cues:
        warnings.append({"code": "no-taxonomy-cues", "message": "No DB public/formal signals matched the approved taxonomy terms."})
    payload = {
        "schema_version": "wp4-formal-public-taxonomy-labels-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "wp4_helper_metadata": metadata,
        "taxonomy_labels": label_cues,
        "observed_inputs": {"db_path": db_path, "taxonomy_path": maybe_text(taxonomy_path), "label_count": len(labels), "signal_count": len(signals)},
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": status,
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "label_cue_count": len(label_cues), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "taxonomy-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "taxonomy-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.taxonomy_labels")],
        "canonical_ids": [maybe_text(item.get("cue_id")) for item in label_cues],
        "warnings": warnings,
        "taxonomy_labels": label_cues,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.taxonomy_labels", candidate_ids=[maybe_text(item.get("cue_id")) for item in label_cues], gap_hints=[item["message"] for item in warnings]),
    }


def run_compare_formal_public_footprints(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    taxonomy_labels_path: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "compare-formal-public-footprints"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"formal_public_footprints_{round_id}.json")
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    label_payload = load_json_file(taxonomy_labels_path, {})
    label_cues = list_items(label_payload.get("taxonomy_labels")) if isinstance(label_payload, dict) else []
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["formal-public-footprint-overlap-cues"],
        caveats=[
            "Footprint comparison describes source-family overlap and absence cues only.",
            "It does not produce paired discourse links or decide representativeness.",
        ],
    )
    public_terms = Counter(term for signal in public_signals for term in text_terms(signal_text(signal), limit=16))
    formal_terms = Counter(term for signal in formal_signals for term in text_terms(signal_text(signal), limit=16))
    shared_terms = sorted(set(public_terms) & set(formal_terms))
    public_only = [term for term, _ in public_terms.most_common(20) if term not in formal_terms]
    formal_only = [term for term, _ in formal_terms.most_common(20) if term not in public_terms]
    footprints = {
        "footprint_id": "formal-public-footprint-" + stable_hash(run_id, round_id, len(public_signals), len(formal_signals))[:12],
        "run_id": run_id,
        "round_id": round_id,
        "formal_record_summary": {
            "signal_count": len(formal_signals),
            "source_distribution": signal_source_distribution(formal_signals),
            "top_terms": [term for term, _ in formal_terms.most_common(20)],
        },
        "public_discourse_summary": {
            "signal_count": len(public_signals),
            "source_distribution": signal_source_distribution(public_signals),
            "top_terms": [term for term, _ in public_terms.most_common(20)],
        },
        "overlap_terms": shared_terms[:20],
        "formal_only_cues": formal_only[:20],
        "public_only_cues": public_only[:20],
        "taxonomy_label_cue_count": len(label_cues),
        "evidence_refs": refs_from_signals([*formal_signals, *public_signals]),
        "lineage": lineage_from_signals([*formal_signals, *public_signals]),
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
        "wp4_helper_metadata": metadata,
    }
    warnings = [] if public_signals and formal_signals else [{"code": "missing-source-family", "message": "Both public and formal DB signals are needed for a complete footprint comparison."}]
    payload = {
        "schema_version": "wp4-formal-public-footprints-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "formal_public_footprints": footprints,
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "public_signal_count": len(public_signals), "formal_signal_count": len(formal_signals), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "footprint-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "footprint-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.formal_public_footprints")],
        "canonical_ids": [footprints["footprint_id"]],
        "warnings": warnings,
        "formal_public_footprints": footprints,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.formal_public_footprints", candidate_ids=[footprints["footprint_id"]], gap_hints=[item["message"] for item in warnings]),
    }


def run_identify_representation_audit_cues(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "identify-representation-audit-cues"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"representation_audit_cues_{round_id}.json")
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["source-family-representation-audit-cues"],
        caveats=[
            "Representation audit cues are prompts for human review, not findings of exclusion or harm.",
            "No cue may be used in report text without DB council-object review.",
        ],
    )
    cues: list[dict[str, Any]] = []
    source_counts = {
        "public": len(public_signals),
        "formal": len(formal_signals),
    }
    if not public_signals or not formal_signals:
        cues.append(
            {
                "cue_id": "representation-cue-" + stable_hash(run_id, round_id, "source-family-presence")[:12],
                "cue_kind": "source-family-presence-audit",
                "review_prompt": "Review whether available source families are sufficient for the decision context.",
                "source_counts": source_counts,
                "audit_status": "requires-human-review",
                "evidence_refs": refs_from_signals([*public_signals, *formal_signals]),
                "lineage": lineage_from_signals([*public_signals, *formal_signals]),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                "wp4_helper_metadata": metadata,
            }
        )
    public_authors = unique_texts([signal.get("author_name") or signal.get("channel_name") for signal in public_signals])
    formal_submitters = unique_texts([dict_items(signal.get("metadata")).get("submitter_name") or signal.get("author_name") for signal in formal_signals])
    cues.append(
        {
            "cue_id": "representation-cue-" + stable_hash(run_id, round_id, "participant-name-coverage")[:12],
            "cue_kind": "participant-name-coverage-audit",
            "review_prompt": "Review participant-name coverage across public and formal source families before inferring public participation.",
            "source_counts": {
                "public_named_sources": len(public_authors),
                "formal_named_submitters": len(formal_submitters),
            },
            "sample_public_names": public_authors[:10],
            "sample_formal_names": formal_submitters[:10],
            "audit_status": "requires-human-review",
            "evidence_refs": refs_from_signals([*public_signals, *formal_signals]),
            "lineage": lineage_from_signals([*public_signals, *formal_signals]),
            "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
            "wp4_helper_metadata": metadata,
        }
    )
    payload = {
        "schema_version": "wp4-representation-audit-cues-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "wp4_helper_metadata": metadata,
        "representation_audit_cues": cues,
        "warnings": [] if public_signals and formal_signals else [{"code": "missing-source-family", "message": "Representation audit cues need human review because at least one source family is absent."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "cue_count": len(cues), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "representation-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "representation-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.representation_audit_cues")],
        "canonical_ids": [maybe_text(item.get("cue_id")) for item in cues],
        "warnings": payload["warnings"],
        "representation_audit_cues": cues,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.representation_audit_cues", candidate_ids=[maybe_text(item.get("cue_id")) for item in cues], gap_hints=[item["message"] for item in payload["warnings"]]),
    }


def run_detect_temporal_cooccurrence_cues(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    limit: int = 700,
) -> dict[str, Any]:
    skill_name = "detect-temporal-cooccurrence-cues"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"temporal_cooccurrence_cues_{round_id}.json")
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    environment_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="environment", limit=limit)
    all_signals = [*public_signals, *formal_signals, *environment_signals]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-signal-temporal-cooccurrence-cues"],
        caveats=[
            "Temporal co-occurrence cues are descriptive only and do not indicate cross-source impact or movement.",
            "Signals without timestamps are excluded from date buckets and reported as limitations.",
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
                "evidence_refs": refs_from_signals(members),
                "lineage": lineage_from_signals(members),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                "wp4_helper_metadata": metadata,
            }
        )
    status = "completed" if cues else "insufficient-temporal-basis"
    warnings = []
    if missing_timestamp:
        warnings.append({"code": "missing-timestamps", "message": f"{missing_timestamp} signals lacked usable timestamps and were not bucketed."})
    if not cues:
        warnings.append({"code": "insufficient-temporal-basis", "message": "No multi-plane same-day co-occurrence cues could be produced from DB signal timestamps."})
    payload = {
        "schema_version": "wp4-temporal-cooccurrence-cues-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "wp4_helper_metadata": metadata,
        "temporal_cooccurrence_cues": cues,
        "temporal_basis": {
            "bucket_count": len(buckets),
            "missing_timestamp_count": missing_timestamp,
            "timestamp_fallback": "none",
        },
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": status,
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "cue_count": len(cues), "missing_timestamp_count": missing_timestamp, "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "temporal-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "temporal-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.temporal_cooccurrence_cues")],
        "canonical_ids": [maybe_text(item.get("cue_id")) for item in cues],
        "warnings": warnings,
        "temporal_cooccurrence_cues": cues,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.temporal_cooccurrence_cues", candidate_ids=[maybe_text(item.get("cue_id")) for item in cues], gap_hints=[item["message"] for item in warnings]),
    }
