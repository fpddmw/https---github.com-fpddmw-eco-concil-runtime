#!/usr/bin/env python3
"""Extract board-ready observation candidates from local signal-plane storage."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean, median
from typing import Any

SKILL_NAME = "eco-extract-observation-candidates"
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    external_id TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    author_name TEXT NOT NULL DEFAULT '',
    channel_name TEXT NOT NULL DEFAULT '',
    language TEXT NOT NULL DEFAULT '',
    query_text TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL DEFAULT '',
    numeric_value REAL,
    unit TEXT NOT NULL DEFAULT '',
    published_at_utc TEXT NOT NULL DEFAULT '',
    observed_at_utc TEXT NOT NULL DEFAULT '',
    window_start_utc TEXT NOT NULL DEFAULT '',
    window_end_utc TEXT NOT NULL DEFAULT '',
    captured_at_utc TEXT NOT NULL DEFAULT '',
    latitude REAL,
    longitude REAL,
    bbox_json TEXT NOT NULL DEFAULT '{}',
    quality_flags_json TEXT NOT NULL DEFAULT '[]',
    engagement_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT 'null',
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL DEFAULT ''
);
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return run_dir / "analytics" / "signal_plane.sqlite"
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_output_path(run_dir: Path, output_path: str, default_name: str) -> Path:
    text = maybe_text(output_path)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str) -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    return connection, file_path


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def signal_ref(row: sqlite3.Row) -> dict[str, str]:
    return {
        "signal_id": row["signal_id"],
        "artifact_path": maybe_text(row["artifact_path"]),
        "record_locator": maybe_text(row["record_locator"]),
        "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}",
    }


def rounded_point(row: sqlite3.Row) -> str:
    if row["latitude"] is None or row["longitude"] is None:
        return "no-point"
    return f"{float(row['latitude']):.3f},{float(row['longitude']):.3f}"


def extract_observation_candidates_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    source_skill: str,
    metric: str,
    quality_flag_any: list[str],
    max_candidates: int,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    candidate_path = resolve_output_path(run_dir_path, output_path, f"observation_candidates_{round_id}.json")
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        rows = connection.execute(
            "SELECT * FROM normalized_signals WHERE plane = 'environment' AND run_id = ? AND round_id = ? ORDER BY COALESCE(observed_at_utc, captured_at_utc) DESC, signal_id",
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()
    wanted_flags = {maybe_text(flag).casefold() for flag in quality_flag_any if maybe_text(flag)}
    groups: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        if maybe_text(source_skill) and maybe_text(row["source_skill"]) != maybe_text(source_skill):
            continue
        if maybe_text(metric) and maybe_text(row["metric"]) != maybe_text(metric):
            continue
        flags = {maybe_text(flag).casefold() for flag in json.loads(row["quality_flags_json"] or "[]")}
        if wanted_flags and not flags.intersection(wanted_flags):
            continue
        group_key = f"{row['source_skill']}|{row['metric']}|{rounded_point(row)}"
        groups.setdefault(group_key, []).append(row)
    ordered_groups = [groups[key] for key in sorted(groups.keys())]
    candidates: list[dict[str, Any]] = []
    for group in ordered_groups[: max(1, max_candidates)]:
        values = [float(row["numeric_value"]) for row in group if row["numeric_value"] is not None]
        if not values:
            continue
        lead = group[0]
        candidate_id = "obscand-" + stable_hash(run_id, round_id, lead["source_skill"], lead["metric"], rounded_point(lead))[:12]
        time_values = sorted(maybe_text(row["observed_at_utc"] or row["window_start_utc"]) for row in group if maybe_text(row["observed_at_utc"] or row["window_start_utc"]))
        quality_flags = sorted({maybe_text(flag) for row in group for flag in json.loads(row["quality_flags_json"] or "[]") if maybe_text(flag)})
        if lead["latitude"] is not None and lead["longitude"] is not None:
            place_scope = {"label": "Signal-cluster point", "geometry": {"type": "Point", "latitude": float(lead["latitude"]), "longitude": float(lead["longitude"])} }
        else:
            place_scope = {"label": "Mixed or unknown observation footprint", "geometry": {}}
        candidates.append(
            {
                "schema_version": "n2",
                "observation_id": candidate_id,
                "run_id": run_id,
                "round_id": round_id,
                "agent_role": "environmentalist",
                "source_skill": maybe_text(lead["source_skill"]),
                "source_skills": sorted({maybe_text(row["source_skill"]) for row in group if maybe_text(row["source_skill"])}),
                "metric": maybe_text(lead["metric"]),
                "aggregation": "window-summary" if len(values) > 1 else "point",
                "value": fmean(values),
                "unit": maybe_text(lead["unit"]),
                "statistics": {"sample_count": len(values), "min": min(values), "median": median(values), "mean": fmean(values), "max": max(values)},
                "time_window": {"start_utc": time_values[0] if time_values else "", "end_utc": time_values[-1] if time_values else ""},
                "place_scope": place_scope,
                "quality_flags": quality_flags,
                "source_signal_count": len(group),
                "source_signal_ids": [row["signal_id"] for row in group],
                "provenance_refs": [signal_ref(row) for row in group[:8]],
                "distribution_summary": {"signal_count": len(group), "metric": maybe_text(lead["metric"]), "distinct_source_skill_count": len({row['source_skill'] for row in group}), "distinct_point_count": len({rounded_point(row) for row in group})},
                "compact_audit": {"representative": lead["latitude"] is not None and lead["longitude"] is not None, "retained_count": 1, "total_candidate_count": len(group), "coverage_summary": f"Aggregated {len(group)} normalized environment signals into one observation candidate.", "concentration_flags": [], "coverage_dimensions": ["metric", "time-window", "place-scope"], "missing_dimensions": [] if lead["latitude"] is not None and lead["longitude"] is not None else ["coordinate-coverage"], "sampling_notes": []},
            }
        )
    wrapper = {"schema_version": "n2", "skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "generated_at_utc": utc_now_iso(), "candidate_count": len(candidates), "candidates": candidates}
    write_json(candidate_path, wrapper)
    artifact_refs = [{"signal_id": "", "artifact_path": str(candidate_path), "record_locator": "$.candidates", "artifact_ref": f"{candidate_path}:$.candidates"}]
    for candidate in candidates:
        artifact_refs.extend(candidate["provenance_refs"])
    batch_id = "candbatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(candidate_path))[:16]
    warnings = [] if candidates else [{"code": "no-candidates", "message": "No observation candidates were extracted from the current environment signal plane."}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "candidate_count": len(candidates), "output_path": str(candidate_path), "db_path": str(db_file)},
        "receipt_id": "candidate-receipt-" + stable_hash(SKILL_NAME, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": artifact_refs[:40],
        "canonical_ids": [candidate["observation_id"] for candidate in candidates],
        "warnings": warnings,
        "board_handoff": {"candidate_ids": [candidate["observation_id"] for candidate in candidates], "evidence_refs": artifact_refs[:20], "gap_hints": ["Observation candidates without stable coordinates still need spatial refinement."] if candidates else [], "challenge_hints": ["Check whether provider-specific clusters should be merged before matching."] if candidates else [], "suggested_next_skills": ["eco-merge-observation-candidates", "eco-build-normalization-audit"]},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract board-ready observation candidates from local signal-plane storage.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--metric", default="")
    parser.add_argument("--quality-flag", action="append", default=[])
    parser.add_argument("--max-candidates", type=int, default=100)
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = extract_observation_candidates_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        db_path=args.db_path,
        source_skill=args.source_skill,
        metric=args.metric,
        quality_flag_any=args.quality_flag,
        max_candidates=args.max_candidates,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())