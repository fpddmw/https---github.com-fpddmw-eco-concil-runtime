#!/usr/bin/env python3
"""Archive one run's normalized signal plane into a cross-run signal corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-archive-signal-corpus"
SIGNAL_TABLE = "normalized_signals"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS corpus_runs (
    run_id TEXT PRIMARY KEY,
    run_dir TEXT NOT NULL,
    topic TEXT NOT NULL DEFAULT '',
    objective TEXT NOT NULL DEFAULT '',
    region_label TEXT NOT NULL DEFAULT '',
    current_round_id TEXT NOT NULL DEFAULT '',
    round_count INTEGER NOT NULL DEFAULT 0,
    public_signal_count INTEGER NOT NULL DEFAULT 0,
    environment_signal_count INTEGER NOT NULL DEFAULT 0,
    metric_families_json TEXT NOT NULL DEFAULT '[]',
    source_skills_json TEXT NOT NULL DEFAULT '[]',
    mission_json TEXT NOT NULL DEFAULT '{}',
    imported_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corpus_signals (
    archived_signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    metric TEXT NOT NULL DEFAULT '',
    metric_family TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    snippet TEXT NOT NULL DEFAULT '',
    query_text TEXT NOT NULL DEFAULT '',
    numeric_value REAL,
    unit TEXT NOT NULL DEFAULT '',
    published_at_utc TEXT NOT NULL DEFAULT '',
    observed_at_utc TEXT NOT NULL DEFAULT '',
    captured_at_utc TEXT NOT NULL DEFAULT '',
    latitude REAL,
    longitude REAL,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    artifact_ref TEXT NOT NULL DEFAULT '',
    region_label TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(run_id) REFERENCES corpus_runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_corpus_signals_run_round ON corpus_signals(run_id, round_id);
CREATE INDEX IF NOT EXISTS idx_corpus_signals_plane_metric ON corpus_signals(plane, metric_family, source_skill);
CREATE INDEX IF NOT EXISTS idx_corpus_signals_region ON corpus_signals(region_label);
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_archive_db_path(run_dir: Path) -> Path:
    return (run_dir / ".." / "archives" / "eco_signal_corpus.sqlite").resolve()


def default_output_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "archive" / f"signal_corpus_import_{round_id}.json").resolve()


def source_signal_db_path(run_dir: Path) -> Path:
    return (run_dir / "analytics" / "signal_plane.sqlite").resolve()


def resolve_path(run_dir: Path, override: str, default_path: Path) -> Path:
    text = maybe_text(override)
    if not text:
        return default_path
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def connect_archive_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(SCHEMA_SQL)
    return connection


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def canonical_metric(metric: Any) -> str:
    text = maybe_text(metric).casefold().replace(".", "_").replace("-", "_")
    if text in {"pm25", "pm2_5", "pm2_5_", "pm2_5m"}:
        return "pm2_5"
    if text in {"pm10", "pm_10"}:
        return "pm10"
    if text in {"o3", "ozone"}:
        return "ozone"
    if text in {"river_discharge_mean", "river_discharge_max", "river_discharge_min"}:
        return "river_discharge"
    return text


def metric_family(metric: Any) -> str:
    normalized = canonical_metric(metric)
    if normalized in {"pm2_5", "pm10", "ozone", "nitrogen_dioxide", "sulfur_dioxide", "sulphur_dioxide", "carbon_monoxide", "us_aqi"}:
        return "air-quality"
    if normalized in {"temperature_2m", "wind_speed_10m", "relative_humidity_2m", "precipitation", "precipitation_sum"}:
        return "meteorology"
    if normalized in {"river_discharge", "gage_height"}:
        return "hydrology"
    if normalized in {"fire_detection", "fire_detection_count"}:
        return "fire-detection"
    if normalized:
        return "other"
    return ""


def load_signal_rows(signal_db: Path, run_id: str) -> list[sqlite3.Row]:
    if not signal_db.exists():
        return []
    connection = sqlite3.connect(signal_db)
    connection.row_factory = sqlite3.Row
    try:
        if not table_exists(connection, SIGNAL_TABLE):
            return []
        return connection.execute(
            "SELECT * FROM normalized_signals WHERE run_id = ? ORDER BY round_id, signal_id",
            (run_id,),
        ).fetchall()
    finally:
        connection.close()


def infer_topic(mission: dict[str, Any], board_brief_text: str, run_id: str) -> str:
    topic = maybe_text(mission.get("topic"))
    if topic:
        return topic
    first_line = board_brief_text.splitlines()[0].strip() if board_brief_text else ""
    if first_line:
        return first_line.lstrip("# ")
    return f"Archived signal corpus snapshot for {run_id}"


def infer_objective(mission: dict[str, Any], board_brief_text: str) -> str:
    objective = maybe_text(mission.get("objective"))
    if objective:
        return objective
    return maybe_text(board_brief_text)[:220]


def infer_region_label(mission: dict[str, Any], rows: list[sqlite3.Row]) -> str:
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    label = maybe_text(region.get("label"))
    if label:
        return label
    with_coordinates = [row for row in rows if row["latitude"] is not None and row["longitude"] is not None]
    if with_coordinates:
        return "signal-plane-coordinates-present"
    return ""


def archive_artifact_ref(path: Path) -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": "",
        "artifact_ref": str(path),
    }


def archive_signal_corpus_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    archive_db = resolve_path(run_dir_path, db_path, default_archive_db_path(run_dir_path))
    output_file = resolve_path(run_dir_path, output_path, default_output_path(run_dir_path, round_id))
    signal_db = source_signal_db_path(run_dir_path)
    mission = load_json_if_exists(run_dir_path / "mission.json")
    if not isinstance(mission, dict):
        mission = {"run_id": run_id}
    board_brief_text = read_text_if_exists(run_dir_path / "board" / f"board_brief_{round_id}.md")

    rows = load_signal_rows(signal_db, run_id)
    imported_at = utc_now_iso()
    topic = infer_topic(mission, board_brief_text, run_id)
    objective = infer_objective(mission, board_brief_text)
    region_label = infer_region_label(mission, rows)
    round_ids = unique_texts([row["round_id"] for row in rows])
    source_skills = unique_texts([row["source_skill"] for row in rows])
    metric_families = unique_texts([metric_family(row["metric"]) for row in rows if metric_family(row["metric"])])
    public_count = len([row for row in rows if maybe_text(row["plane"]) == "public"])
    environment_count = len([row for row in rows if maybe_text(row["plane"]) == "environment"])

    warnings: list[dict[str, str]] = []
    if not signal_db.exists():
        warnings.append({"code": "missing-signal-plane", "message": f"No normalized signal-plane database was found at {signal_db}."})
    elif not rows:
        warnings.append({"code": "no-signal-rows", "message": f"No normalized_signals rows were available for run_id={run_id}."})

    connection = connect_archive_db(archive_db)
    try:
        existing = connection.execute("SELECT 1 FROM corpus_runs WHERE run_id = ?", (run_id,)).fetchone() is not None
        connection.execute("DELETE FROM corpus_runs WHERE run_id = ?", (run_id,))
        connection.execute(
            """
            INSERT INTO corpus_runs (
                run_id, run_dir, topic, objective, region_label, current_round_id, round_count,
                public_signal_count, environment_signal_count, metric_families_json, source_skills_json,
                mission_json, imported_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                str(run_dir_path),
                topic,
                objective,
                region_label,
                round_id,
                len(round_ids),
                public_count,
                environment_count,
                json_text(metric_families),
                json_text(source_skills),
                json_text(mission),
                imported_at,
            ),
        )
        for row in rows:
            archived_signal_id = "corpus-signal-" + stable_hash(run_id, row["round_id"], row["signal_id"])[:16]
            artifact_path = maybe_text(row["artifact_path"])
            record_locator = maybe_text(row["record_locator"])
            connection.execute(
                """
                INSERT INTO corpus_signals (
                    archived_signal_id, run_id, round_id, signal_id, plane, source_skill, signal_kind,
                    metric, metric_family, title, snippet, query_text, numeric_value, unit,
                    published_at_utc, observed_at_utc, captured_at_utc, latitude, longitude,
                    artifact_path, record_locator, artifact_ref, region_label, topic, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    archived_signal_id,
                    run_id,
                    maybe_text(row["round_id"]),
                    maybe_text(row["signal_id"]),
                    maybe_text(row["plane"]),
                    maybe_text(row["source_skill"]),
                    maybe_text(row["signal_kind"]),
                    canonical_metric(row["metric"]),
                    metric_family(row["metric"]),
                    maybe_text(row["title"]),
                    maybe_text(row["body_text"])[:280],
                    maybe_text(row["query_text"]),
                    maybe_number(row["numeric_value"]),
                    maybe_text(row["unit"]),
                    maybe_text(row["published_at_utc"]),
                    maybe_text(row["observed_at_utc"]),
                    maybe_text(row["captured_at_utc"]),
                    maybe_number(row["latitude"]),
                    maybe_number(row["longitude"]),
                    artifact_path,
                    record_locator,
                    artifact_path if not record_locator else f"{artifact_path}:{record_locator}",
                    region_label,
                    topic,
                    json_text(json.loads(row["metadata_json"] or "{}")),
                ),
            )
        connection.commit()
    finally:
        connection.close()

    import_id = "signal-corpus-import-" + stable_hash(run_id, round_id, archive_db, len(rows))[:12]
    snapshot = {
        "schema_version": "archive-signal-corpus-v1",
        "skill": SKILL_NAME,
        "generated_at_utc": imported_at,
        "run_id": run_id,
        "round_id": round_id,
        "import_id": import_id,
        "db_path": str(archive_db),
        "source_db_path": str(signal_db),
        "topic": topic,
        "objective": objective,
        "region_label": region_label,
        "replaced_existing": bool(rows) and existing,
        "imported_signal_count": len(rows),
        "public_signal_count": public_count,
        "environment_signal_count": environment_count,
        "round_ids": round_ids,
        "metric_families": metric_families,
        "source_skills": source_skills,
    }
    write_json_file(output_file, snapshot)

    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
        archive_artifact_ref(archive_db),
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "import_id": import_id,
            "output_path": str(output_file),
            "db_path": str(archive_db),
            "imported_signal_count": len(rows),
        },
        "receipt_id": "archive-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, import_id)[:20],
        "batch_id": "archivebatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [import_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [run_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if rows else [item["message"] for item in warnings],
            "challenge_hints": ["Signal corpus snapshots with zero rows cannot support cross-run signal reuse."] if not rows else [],
            "suggested_next_skills": ["eco-query-signal-corpus", "eco-materialize-history-context"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive one run's normalized signal plane into a cross-run signal corpus.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = archive_signal_corpus_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        db_path=args.db_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())