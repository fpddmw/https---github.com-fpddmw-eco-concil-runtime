from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eco_council_runtime.objects.analysis import (
    HELPER_DECISION_SOURCE_APPROVED_VIEW,
    helper_governance_metadata,
)
from eco_council_runtime.kernel.planes.signal import ensure_signal_plane_schema


__all__ = (
    "OPTIONAL_ANALYSIS_RULE_IDS",
    "artifact_ref",
    "connect_signal_db",
    "context_signals_for_relation",
    "date_key",
    "dict_items",
    "first_timestamp",
    "haversine_km",
    "helper_metadata",
    "lineage_from_signals",
    "list_items",
    "maybe_float",
    "maybe_text",
    "normalize_space",
    "parse_bbox",
    "parse_json_text",
    "parse_utc_datetime",
    "pretty_json",
    "query_signals",
    "refs_from_signals",
    "relation_environment_class",
    "relation_filter_matches",
    "relation_signal_role",
    "resolve_output_path",
    "resolve_run_dir",
    "row_to_signal",
    "safe_board_handoff",
    "signal_evidence_ref",
    "signal_metadata_text",
    "signal_metric_distribution",
    "signal_source_distribution",
    "signal_within_bbox",
    "signal_within_time_filter",
    "stable_hash",
    "text_terms",
    "timestamp_delta_hours",
    "unique_texts",
    "unique_values",
    "utc_now_iso",
    "write_json",
)


OPTIONAL_ANALYSIS_RULE_IDS: dict[str, str] = {
    "aggregate-environment-evidence": "HEUR-ENV-AGGREGATE-001",
    "review-fact-check-evidence-scope": "HEUR-FACT-SCOPE-001",
    "discover-discourse-issues": "HEUR-DISCOURSE-DISCOVERY-001",
    "suggest-evidence-lanes": "HEUR-EVIDENCE-LANE-001",
    "materialize-claim-gap-action-cards": "HEUR-CLAIM-GAP-ACTION-CARDS-001",
    "materialize-research-issue-surface": "HEUR-RESEARCH-ISSUE-SURFACE-001",
    "project-research-issue-views": "HEUR-RESEARCH-ISSUE-PROJECTION-001",
    "export-research-issue-map": "HEUR-RESEARCH-ISSUE-MAP-001",
    "apply-approved-formal-public-taxonomy": "HEUR-TAXONOMY-APPLY-001",
    "compare-formal-public-footprints": "HEUR-FORMAL-PUBLIC-FOOTPRINT-001",
    "identify-representation-audit-cues": "HEUR-REPRESENTATION-AUDIT-001",
    "materialize-public-discourse-corpus": "HEUR-PUBLIC-DISCOURSE-CORPUS-001",
    "audit-formal-comment-candidate-corpus": "HEUR-FORMAL-COMMENT-CANDIDATE-CORPUS-001",
    "audit-public-discourse-sample-coverage": "HEUR-PUBLIC-DISCOURSE-COVERAGE-001",
    "classify-formal-comment-issues": "HEUR-FORMAL-COMMENT-ISSUE-ANNOTATION-001",
    "classify-public-discourse-affect": "HEUR-PUBLIC-DISCOURSE-AFFECT-ANNOTATION-001",
    "aggregate-public-discourse-annotations": "HEUR-PUBLIC-DISCOURSE-ANNOTATION-001",
    "compare-public-media-narratives": "HEUR-PUBLIC-MEDIA-NARRATIVE-COMPARE-001",
    "summarize-public-discourse-sample": "HEUR-PUBLIC-DISCOURSE-SUMMARY-001",
    "detect-temporal-cooccurrence-cues": "HEUR-SPATIOTEMPORAL-RELATION-001",
    "review-spatiotemporal-relation-alternatives": "HEUR-SPATIOTEMPORAL-ALTERNATIVES-001",
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
    decision_source: str = HELPER_DECISION_SOURCE_APPROVED_VIEW,
    rule_id: str = "",
    destination: str = "",
    taxonomy_version: str = "",
    rubric_version: str = "",
    approval_ref: str = "required:skill_approval_request",
    rule_trace: list[Any] | None = None,
    caveats: list[Any] | None = None,
    helper_status: str = "approval-gated-helper-view",
) -> dict[str, Any]:
    return helper_governance_metadata(
        skill_name=skill_name,
        rule_id=maybe_text(rule_id) or OPTIONAL_ANALYSIS_RULE_IDS.get(skill_name, ""),
        destination=destination or skill_name,
        decision_source=decision_source,
        taxonomy_version=taxonomy_version,
        rubric_version=rubric_version,
        approval_ref=approval_ref,
        audit_ref="docs/openclaw-project-overview.md#skill-分层与治理边界",
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


def parse_utc_datetime(value: str) -> datetime | None:
    text = maybe_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def timestamp_delta_hours(source_value: str, target_value: str) -> float | None:
    source_dt = parse_utc_datetime(source_value)
    target_dt = parse_utc_datetime(target_value)
    if source_dt is None or target_dt is None:
        return None
    return round((target_dt - source_dt).total_seconds() / 3600.0, 6)


def signal_metadata_text(signal: dict[str, Any], field_name: str) -> str:
    metadata = signal.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    return maybe_text(metadata.get(field_name))


def relation_signal_role(signal: dict[str, Any]) -> str:
    explicit = signal_metadata_text(signal, "signal_role")
    if explicit:
        return explicit
    plane = maybe_text(signal.get("plane"))
    if plane in {"public", "formal"}:
        return "claim-or-report-signal"
    return "unknown-environment-signal-role"


def relation_environment_class(signal: dict[str, Any]) -> str:
    explicit = signal_metadata_text(signal, "environment_signal_class")
    if explicit:
        return explicit
    return "unknown-environment-class"


def maybe_float(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_bbox(value: str) -> tuple[float, float, float, float] | None:
    text = maybe_text(value)
    if not text:
        return None
    parts = [maybe_float(part.strip()) for part in text.split(",")]
    if len(parts) != 4 or any(part is None for part in parts):
        return None
    west, south, east, north = [float(part) for part in parts if part is not None]
    if west > east or south > north:
        return None
    return west, south, east, north


def signal_within_bbox(
    signal: dict[str, Any],
    bbox: tuple[float, float, float, float] | None,
) -> bool:
    if bbox is None:
        return True
    latitude = maybe_float(signal.get("latitude"))
    longitude = maybe_float(signal.get("longitude"))
    if latitude is None or longitude is None:
        return False
    west, south, east, north = bbox
    return west <= longitude <= east and south <= latitude <= north


def signal_within_time_filter(
    signal: dict[str, Any],
    *,
    observed_after_utc: str,
    observed_before_utc: str,
) -> bool:
    timestamp = parse_utc_datetime(first_timestamp(signal))
    if timestamp is None:
        return True
    after_dt = parse_utc_datetime(observed_after_utc)
    before_dt = parse_utc_datetime(observed_before_utc)
    if after_dt is not None and timestamp < after_dt:
        return False
    if before_dt is not None and timestamp > before_dt:
        return False
    return True


def relation_filter_matches(
    signal: dict[str, Any],
    *,
    role: str,
    environment_class: str,
    observed_after_utc: str,
    observed_before_utc: str,
    bbox: tuple[float, float, float, float] | None,
) -> bool:
    if maybe_text(role) and relation_signal_role(signal) != maybe_text(role):
        return False
    if maybe_text(environment_class) and relation_environment_class(signal) != maybe_text(environment_class):
        return False
    if not signal_within_time_filter(
        signal,
        observed_after_utc=observed_after_utc,
        observed_before_utc=observed_before_utc,
    ):
        return False
    return signal_within_bbox(signal, bbox)


def haversine_km(
    source: dict[str, Any],
    target: dict[str, Any],
) -> float | None:
    source_lat = maybe_float(source.get("latitude"))
    source_lon = maybe_float(source.get("longitude"))
    target_lat = maybe_float(target.get("latitude"))
    target_lon = maybe_float(target.get("longitude"))
    if None in {source_lat, source_lon, target_lat, target_lon}:
        return None
    radius_km = 6371.0088
    phi1 = math.radians(float(source_lat))
    phi2 = math.radians(float(target_lat))
    delta_phi = math.radians(float(target_lat) - float(source_lat))
    delta_lambda = math.radians(float(target_lon) - float(source_lon))
    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    return round(radius_km * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a)), 6)


def context_signals_for_relation(
    signals: list[dict[str, Any]],
    *,
    source_signal_id: str,
    target_signal_id: str,
    source_timestamp: str,
    target_timestamp: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    source_dt = parse_utc_datetime(source_timestamp)
    target_dt = parse_utc_datetime(target_timestamp)
    source_date = date_key(source_timestamp)
    target_date = date_key(target_timestamp)
    contexts: list[dict[str, Any]] = []
    for signal in signals:
        signal_id = maybe_text(signal.get("signal_id"))
        if signal_id in {source_signal_id, target_signal_id}:
            continue
        if relation_signal_role(signal) != "context-observation":
            continue
        timestamp_text = first_timestamp(signal)
        timestamp = parse_utc_datetime(timestamp_text)
        if source_dt is not None and target_dt is not None and timestamp is not None:
            start_dt, end_dt = sorted([source_dt, target_dt])
            if not (start_dt <= timestamp <= end_dt):
                continue
        elif date_key(timestamp_text) not in {source_date, target_date}:
            continue
        contexts.append(signal)
        if len(contexts) >= limit:
            break
    return contexts


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


def refs_from_signals(signals: list[dict[str, Any]]) -> list[Any]:
    return unique_values([ref for signal in signals for ref in list_items(signal.get("evidence_refs"))])


def lineage_from_signals(signals: list[dict[str, Any]]) -> list[str]:
    return unique_texts([maybe_text(signal.get("signal_id")) for signal in signals])
