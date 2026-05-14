from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from eco_council_runtime.kernel.source_queue.source_queue_history import discovered_round_ids

from ..support import (
    connect_signal_db,
    first_timestamp,
    maybe_float,
    maybe_text,
    parse_bbox,
    row_to_signal,
    unique_texts,
)


SKILL_NAME = "aggregate-environment-evidence"
VALID_AGGREGATION_METHODS = (
    "coverage-summary",
    "time-series-summary",
    "point-event-summary",
    "auto-summary",
    "source-metric-day-summary",
)
VALID_ROUND_SCOPES = ("current", "up-to-current", "all")
DEFAULT_AGGREGATION_METHOD = "auto-summary"
DEFAULT_GROUP_LIMIT = 50
DEFAULT_SAMPLE_REF_LIMIT = 25


@dataclass(frozen=True)
class AggregationConfig:
    run_dir: Path
    run_id: str
    round_id: str
    output_path: str
    requested_method: str
    aggregation_method: str
    round_scope: str
    source_skill: str
    metric: str
    observed_after_utc: str
    observed_before_utc: str
    bbox: tuple[float, float, float, float] | None
    sample_limit: int
    group_limit: int
    sample_ref_limit: int


def normalize_aggregation_method(value: Any) -> tuple[str, str]:
    requested = maybe_text(value) or DEFAULT_AGGREGATION_METHOD
    if requested not in VALID_AGGREGATION_METHODS:
        raise ValueError(
            "Unsupported --aggregation-method "
            f"{requested!r}. Expected one of {', '.join(VALID_AGGREGATION_METHODS)}."
        )
    if requested == "source-metric-day-summary":
        return requested, "coverage-summary"
    return requested, requested


def normalize_round_scope(value: Any) -> str:
    scope = maybe_text(value) or "current"
    if scope not in VALID_ROUND_SCOPES:
        raise ValueError(
            f"Unsupported --round-scope {scope!r}. Expected one of {', '.join(VALID_ROUND_SCOPES)}."
        )
    return scope


def build_config(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    output_path: str,
    aggregation_method: str,
    round_scope: str,
    source_skill: str,
    metric: str,
    observed_after_utc: str,
    observed_before_utc: str,
    bbox: str,
    limit: int,
    group_limit: int,
    sample_ref_limit: int,
) -> AggregationConfig:
    requested_method, resolved_method = normalize_aggregation_method(aggregation_method)
    parsed_bbox = parse_bbox(bbox)
    return AggregationConfig(
        run_dir=run_dir,
        run_id=maybe_text(run_id),
        round_id=maybe_text(round_id),
        output_path=maybe_text(output_path),
        requested_method=requested_method,
        aggregation_method=resolved_method,
        round_scope=normalize_round_scope(round_scope),
        source_skill=maybe_text(source_skill),
        metric=maybe_text(metric),
        observed_after_utc=maybe_text(observed_after_utc),
        observed_before_utc=maybe_text(observed_before_utc),
        bbox=parsed_bbox,
        sample_limit=max(0, int(limit or 0)),
        group_limit=max(1, int(group_limit or DEFAULT_GROUP_LIMIT)),
        sample_ref_limit=max(0, int(sample_ref_limit or 0)),
    )


def observed_round_ids(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plane: str,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT
            round_id,
            MIN(
                COALESCE(
                    NULLIF(captured_at_utc, ''),
                    NULLIF(observed_at_utc, ''),
                    NULLIF(window_start_utc, ''),
                    NULLIF(window_end_utc, ''),
                    NULLIF(published_at_utc, ''),
                    signal_id
                )
            ) AS first_seen
        FROM normalized_signals
        WHERE run_id = ? AND plane = ?
        GROUP BY round_id
        ORDER BY first_seen, round_id
        """,
        (run_id, plane),
    ).fetchall()
    return [maybe_text(row["round_id"]) for row in rows if maybe_text(row["round_id"])]


def ordered_round_ids(
    run_dir: Path,
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plane: str,
    current_round_id: str,
) -> list[str]:
    ordered = discovered_round_ids(run_dir)
    for round_id in observed_round_ids(connection, run_id=run_id, plane=plane):
        if round_id not in ordered:
            ordered.append(round_id)
    current = maybe_text(current_round_id)
    if current and current not in ordered:
        ordered.append(current)
    return ordered


def selected_round_ids(
    run_dir: Path,
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plane: str,
    current_round_id: str,
    round_scope: str,
) -> list[str]:
    current = maybe_text(current_round_id)
    if round_scope == "current":
        return [current] if current else []
    ordered = ordered_round_ids(
        run_dir,
        connection,
        run_id=run_id,
        plane=plane,
        current_round_id=current,
    )
    if round_scope == "all":
        return ordered
    if current not in ordered:
        ordered.append(current)
    return ordered[: ordered.index(current) + 1]


def timestamp_expr() -> str:
    return (
        "COALESCE(NULLIF(observed_at_utc, ''), NULLIF(window_start_utc, ''), "
        "NULLIF(window_end_utc, ''), NULLIF(published_at_utc, ''), "
        "NULLIF(captured_at_utc, ''), '')"
    )


def signal_query_sql(round_ids: list[str], config: AggregationConfig) -> tuple[str, list[Any]]:
    selected = [round_id for round_id in round_ids if maybe_text(round_id)]
    clauses = ["run_id = ?", "plane = ?"]
    params: list[Any] = [config.run_id, "environment"]
    if selected:
        placeholders = ",".join("?" for _ in selected)
        clauses.append(f"round_id IN ({placeholders})")
        params.extend(selected)
    else:
        clauses.append("0")
    if config.source_skill:
        clauses.append("source_skill = ?")
        params.append(config.source_skill)
    if config.metric:
        clauses.append("metric = ?")
        params.append(config.metric)
    timestamp = timestamp_expr()
    if config.observed_after_utc:
        clauses.append(f"({timestamp} = '' OR {timestamp} >= ?)")
        params.append(config.observed_after_utc)
    if config.observed_before_utc:
        clauses.append(f"({timestamp} = '' OR {timestamp} <= ?)")
        params.append(config.observed_before_utc)
    if config.bbox is not None:
        west, south, east, north = config.bbox
        clauses.extend(
            [
                "latitude IS NOT NULL",
                "longitude IS NOT NULL",
                "longitude >= ?",
                "longitude <= ?",
                "latitude >= ?",
                "latitude <= ?",
            ]
        )
        params.extend([west, east, south, north])
    query = (
        "SELECT * FROM normalized_signals WHERE "
        + " AND ".join(clauses)
        + f" ORDER BY {timestamp}, source_skill, metric, signal_id"
    )
    return query, params


def iter_environment_signals(
    connection: sqlite3.Connection,
    *,
    round_ids: list[str],
    config: AggregationConfig,
    chunk_size: int = 1000,
) -> Iterator[dict[str, Any]]:
    query, params = signal_query_sql(round_ids, config)
    cursor = connection.execute(query, tuple(params))
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        for row in rows:
            yield row_to_signal(row)


def signal_timestamp(signal: dict[str, Any]) -> str:
    return maybe_text(first_timestamp(signal))


def date_bucket(value: Any) -> str:
    text = maybe_text(value)
    return text[:10] if len(text) >= 10 else ""


def metadata_dict(signal: dict[str, Any]) -> dict[str, Any]:
    metadata = signal.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def metadata_text(signal: dict[str, Any], *field_names: str) -> str:
    metadata = metadata_dict(signal)
    for field_name in field_names:
        value = maybe_text(metadata.get(field_name))
        if value:
            return value
    return ""


def metadata_float(signal: dict[str, Any], *field_names: str) -> float | None:
    metadata = metadata_dict(signal)
    for field_name in field_names:
        value = maybe_float(metadata.get(field_name))
        if value is not None:
            return value
    return None


def signal_location_key(signal: dict[str, Any]) -> str:
    metadata_value = metadata_text(
        signal,
        "site_number",
        "site_id",
        "station_id",
        "site_name",
        "station_name",
        "location_id",
        "location_name",
    )
    if metadata_value:
        return metadata_value
    channel_name = maybe_text(signal.get("channel_name"))
    if channel_name:
        return channel_name
    latitude = maybe_float(signal.get("latitude"))
    longitude = maybe_float(signal.get("longitude"))
    if latitude is not None and longitude is not None:
        return f"{round(latitude, 5)},{round(longitude, 5)}"
    return "unspecified-location"


def signal_location_payload(signal: dict[str, Any]) -> dict[str, Any]:
    metadata = metadata_dict(signal)
    return {
        "location_key": signal_location_key(signal),
        "site_number": maybe_text(metadata.get("site_number")),
        "site_name": maybe_text(metadata.get("site_name")),
        "location_name": maybe_text(metadata.get("location_name")),
        "latitude": signal.get("latitude"),
        "longitude": signal.get("longitude"),
    }


def numeric_value(signal: dict[str, Any]) -> float | None:
    return maybe_float(signal.get("numeric_value"))


def is_point_event_signal(signal: dict[str, Any]) -> bool:
    signal_kind = maybe_text(signal.get("signal_kind")).casefold()
    metric = maybe_text(signal.get("metric")).casefold()
    environment_class = metadata_text(signal, "environment_signal_class").casefold()
    if "fire-detection" in signal_kind or environment_class == "fire-detection":
        return True
    if metric in {"fire_detection", "fire_detection_count"}:
        return True
    return False


def sorted_counter_rows(
    counts: dict[str, int],
    *,
    key_name: str,
    limit: int,
) -> list[dict[str, Any]]:
    rows = [
        {key_name: key, "signal_count": count}
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        if key
    ]
    return rows[:limit]


def limited_unique_texts(values: list[Any], *, limit: int) -> list[str]:
    return unique_texts(values)[: max(0, limit)]


def load_environment_signal_stream(
    config: AggregationConfig,
) -> tuple[sqlite3.Connection, str, list[str]]:
    connection, db_file = connect_signal_db(config.run_dir)
    round_ids = selected_round_ids(
        config.run_dir,
        connection,
        run_id=config.run_id,
        plane="environment",
        current_round_id=config.round_id,
        round_scope=config.round_scope,
    )
    return connection, str(db_file), round_ids
