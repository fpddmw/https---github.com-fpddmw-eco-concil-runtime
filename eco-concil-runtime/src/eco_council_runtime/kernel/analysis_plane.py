from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ANALYSIS_KIND_EVIDENCE_COVERAGE = "evidence-coverage"
DEFAULT_SOURCE_SKILL = "eco-score-evidence-coverage"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS analysis_result_sets (
    result_set_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    source_skill TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    generated_at_utc TEXT NOT NULL DEFAULT '',
    item_count INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_sets_round_kind
ON analysis_result_sets(run_id, round_id, analysis_kind, generated_at_utc, result_set_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_sets_artifact
ON analysis_result_sets(artifact_path, analysis_kind, generated_at_utc, result_set_id);

CREATE TABLE IF NOT EXISTS analysis_result_items (
    item_id TEXT PRIMARY KEY,
    result_set_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    source_skill TEXT NOT NULL DEFAULT '',
    item_index INTEGER NOT NULL DEFAULT 0,
    subject_id TEXT NOT NULL DEFAULT '',
    readiness TEXT NOT NULL DEFAULT '',
    score REAL,
    related_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    item_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    generated_at_utc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_result_set
ON analysis_result_items(result_set_id, item_index, item_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_round_kind
ON analysis_result_items(run_id, round_id, analysis_kind, item_index, item_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_subject
ON analysis_result_items(subject_id, analysis_kind, readiness);
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


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def decode_json(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "signal_plane.sqlite"


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return default_db_path(run_dir)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    return connection, file_path


def resolve_artifact_path(
    run_dir: Path,
    override: str | Path,
    default_relative: str,
) -> Path:
    if isinstance(override, Path):
        return override.expanduser().resolve()
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def _select_latest_result_set(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    round_id: str,
    analysis_kind: str,
    artifact_path: str = "",
    allow_any_artifact: bool = True,
) -> sqlite3.Row | None:
    artifact_text = maybe_text(artifact_path)
    if artifact_text:
        row = connection.execute(
            """
            SELECT *
            FROM analysis_result_sets
            WHERE run_id = ?
              AND round_id = ?
              AND analysis_kind = ?
              AND artifact_path = ?
            ORDER BY generated_at_utc DESC, result_set_id DESC
            LIMIT 1
            """,
            (run_id, round_id, analysis_kind, artifact_text),
        ).fetchone()
        if row is not None or not allow_any_artifact:
            return row
    return connection.execute(
        """
        SELECT *
        FROM analysis_result_sets
        WHERE run_id = ?
          AND round_id = ?
          AND analysis_kind = ?
        ORDER BY generated_at_utc DESC, result_set_id DESC
        LIMIT 1
        """,
        (run_id, round_id, analysis_kind),
    ).fetchone()


def _load_result_items(
    connection: sqlite3.Connection,
    *,
    result_set_id: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT item_json
        FROM analysis_result_items
        WHERE result_set_id = ?
        ORDER BY item_index, item_id
        """,
        (result_set_id,),
    ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        payload_text = row["item_json"] if isinstance(row["item_json"], str) else ""
        payload = decode_json(payload_text, {})
        if isinstance(payload, dict):
            results.append(payload)
    return results


def _load_result_wrapper(
    connection: sqlite3.Connection,
    *,
    result_set_row: sqlite3.Row,
) -> dict[str, Any]:
    raw_text = result_set_row["raw_json"] if isinstance(result_set_row["raw_json"], str) else ""
    wrapper = decode_json(raw_text, {})
    if not isinstance(wrapper, dict):
        wrapper = {}
    items = _load_result_items(
        connection,
        result_set_id=maybe_text(result_set_row["result_set_id"]),
    )
    wrapper["coverages"] = items
    wrapper["coverage_count"] = len(items)
    return wrapper


def sync_evidence_coverage_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    coverage_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    coverage_file = resolve_artifact_path(
        run_dir_path,
        coverage_path,
        f"analytics/evidence_coverage_{round_id}.json",
    )
    db_file = resolve_db_path(run_dir_path, db_path)
    coverage_payload = load_json_if_exists(coverage_file)
    if not isinstance(coverage_payload, dict):
        return {
            "status": "missing-coverage",
            "analysis_kind": ANALYSIS_KIND_EVIDENCE_COVERAGE,
            "run_id": maybe_text(expected_run_id),
            "round_id": maybe_text(round_id),
            "coverage_path": str(coverage_file),
            "db_path": str(db_file),
            "result_set_id": "",
            "item_count": 0,
        }

    payload_run_id = maybe_text(coverage_payload.get("run_id")) or maybe_text(expected_run_id)
    payload_round_id = maybe_text(coverage_payload.get("round_id")) or maybe_text(round_id)
    source_skill = maybe_text(coverage_payload.get("skill")) or DEFAULT_SOURCE_SKILL
    generated_at_utc = maybe_text(coverage_payload.get("generated_at_utc")) or utc_now_iso()
    coverages = (
        [item for item in coverage_payload.get("coverages", []) if isinstance(item, dict)]
        if isinstance(coverage_payload.get("coverages"), list)
        else []
    )
    result_set_id = "analysis-set-" + stable_hash(
        ANALYSIS_KIND_EVIDENCE_COVERAGE,
        payload_run_id,
        payload_round_id,
        str(coverage_file),
    )[:16]

    connection, resolved_db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                """
                DELETE FROM analysis_result_items
                WHERE run_id = ? AND round_id = ? AND analysis_kind = ?
                """,
                (payload_run_id, payload_round_id, ANALYSIS_KIND_EVIDENCE_COVERAGE),
            )
            connection.execute(
                """
                DELETE FROM analysis_result_sets
                WHERE run_id = ? AND round_id = ? AND analysis_kind = ?
                """,
                (payload_run_id, payload_round_id, ANALYSIS_KIND_EVIDENCE_COVERAGE),
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO analysis_result_sets (
                    result_set_id,
                    run_id,
                    round_id,
                    analysis_kind,
                    source_skill,
                    artifact_path,
                    record_locator,
                    generated_at_utc,
                    item_count,
                    summary_json,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_set_id,
                    payload_run_id,
                    payload_round_id,
                    ANALYSIS_KIND_EVIDENCE_COVERAGE,
                    source_skill,
                    str(coverage_file),
                    "$.coverages",
                    generated_at_utc,
                    len(coverages),
                    json_text(
                        {
                            "coverage_count": len(coverages),
                            "links_path": maybe_text(coverage_payload.get("links_path")),
                            "claim_scope_path": maybe_text(
                                coverage_payload.get("claim_scope_path")
                            ),
                            "observation_scope_path": maybe_text(
                                coverage_payload.get("observation_scope_path")
                            ),
                        }
                    ),
                    json_text(coverage_payload),
                ),
            )
            for index, coverage in enumerate(coverages, start=1):
                coverage_id = maybe_text(coverage.get("coverage_id")) or str(index)
                item_id = "analysis-item-" + stable_hash(result_set_id, coverage_id)[:16]
                connection.execute(
                    """
                    INSERT OR REPLACE INTO analysis_result_items (
                        item_id,
                        result_set_id,
                        run_id,
                        round_id,
                        analysis_kind,
                        source_skill,
                        item_index,
                        subject_id,
                        readiness,
                        score,
                        related_ids_json,
                        evidence_refs_json,
                        item_json,
                        artifact_path,
                        record_locator,
                        generated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item_id,
                        result_set_id,
                        payload_run_id,
                        payload_round_id,
                        ANALYSIS_KIND_EVIDENCE_COVERAGE,
                        source_skill,
                        index,
                        maybe_text(coverage.get("claim_id")) or coverage_id,
                        maybe_text(coverage.get("readiness")),
                        maybe_number(coverage.get("coverage_score")),
                        json_text(
                            unique_texts(
                                [
                                    coverage.get("coverage_id"),
                                    coverage.get("claim_id"),
                                ]
                            )
                        ),
                        json_text(
                            coverage.get("evidence_refs", [])
                            if isinstance(coverage.get("evidence_refs"), list)
                            else []
                        ),
                        json_text(coverage),
                        str(coverage_file),
                        f"$.coverages[{index - 1}]",
                        generated_at_utc,
                    ),
                )
    finally:
        connection.close()

    return {
        "status": "completed",
        "analysis_kind": ANALYSIS_KIND_EVIDENCE_COVERAGE,
        "run_id": payload_run_id,
        "round_id": payload_round_id,
        "coverage_path": str(coverage_file),
        "db_path": str(resolved_db_file),
        "result_set_id": result_set_id,
        "item_count": len(coverages),
        "source_skill": source_skill,
    }


def load_evidence_coverage_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    coverage_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    coverage_file = resolve_artifact_path(
        run_dir_path,
        coverage_path,
        f"analytics/evidence_coverage_{round_id}.json",
    )
    coverage_override_requested = bool(maybe_text(coverage_path))
    artifact_payload = load_json_if_exists(coverage_file)
    artifact_present = isinstance(artifact_payload, dict)

    connection, resolved_db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            existing_row = _select_latest_result_set(
                connection,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=ANALYSIS_KIND_EVIDENCE_COVERAGE,
                artifact_path=str(coverage_file),
                allow_any_artifact=not coverage_override_requested,
            )
    finally:
        connection.close()

    artifact_generated_at = (
        maybe_text(artifact_payload.get("generated_at_utc"))
        if isinstance(artifact_payload, dict)
        else ""
    )
    existing_generated_at = (
        maybe_text(existing_row["generated_at_utc"]) if existing_row is not None else ""
    )
    existing_artifact_path = (
        maybe_text(existing_row["artifact_path"]) if existing_row is not None else ""
    )

    analysis_sync: dict[str, Any] = {}
    should_sync = artifact_present and (
        existing_row is None
        or existing_artifact_path != str(coverage_file)
        or (
            artifact_generated_at
            and existing_generated_at
            and artifact_generated_at != existing_generated_at
        )
    )
    if should_sync:
        analysis_sync = sync_evidence_coverage_result_set(
            run_dir_path,
            expected_run_id=run_id,
            round_id=round_id,
            coverage_path=coverage_file,
            db_path=str(resolved_db_file),
        )

    connection, resolved_db_file = connect_db(run_dir_path, str(resolved_db_file))
    try:
        with connection:
            result_set_row = _select_latest_result_set(
                connection,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=ANALYSIS_KIND_EVIDENCE_COVERAGE,
                artifact_path=str(coverage_file),
                allow_any_artifact=not coverage_override_requested,
            )
            if result_set_row is not None:
                wrapper = _load_result_wrapper(
                    connection,
                    result_set_row=result_set_row,
                )
                coverages = (
                    [
                        item
                        for item in wrapper.get("coverages", [])
                        if isinstance(item, dict)
                    ]
                    if isinstance(wrapper.get("coverages"), list)
                    else []
                )
                if not analysis_sync:
                    analysis_sync = {
                        "status": "existing-result-set",
                        "analysis_kind": ANALYSIS_KIND_EVIDENCE_COVERAGE,
                        "run_id": run_id,
                        "round_id": round_id,
                        "coverage_path": maybe_text(result_set_row["artifact_path"]),
                        "db_path": str(resolved_db_file),
                        "result_set_id": maybe_text(result_set_row["result_set_id"]),
                        "item_count": len(coverages),
                        "source_skill": maybe_text(result_set_row["source_skill"]),
                    }
                return {
                    "coverage_wrapper": wrapper,
                    "coverages": coverages,
                    "coverage_count": len(coverages),
                    "coverage_source": "analysis-plane",
                    "coverage_file": maybe_text(result_set_row["artifact_path"])
                    or str(coverage_file),
                    "db_path": str(resolved_db_file),
                    "analysis_sync": analysis_sync,
                    "coverage_artifact_present": artifact_present,
                    "warnings": [],
                }
    finally:
        connection.close()

    if artifact_present:
        coverages = (
            [item for item in artifact_payload.get("coverages", []) if isinstance(item, dict)]
            if isinstance(artifact_payload.get("coverages"), list)
            else []
        )
        if not analysis_sync:
            analysis_sync = {
                "status": "artifact-only",
                "analysis_kind": ANALYSIS_KIND_EVIDENCE_COVERAGE,
                "run_id": run_id,
                "round_id": round_id,
                "coverage_path": str(coverage_file),
                "db_path": str(resolved_db_file),
                "result_set_id": "",
                "item_count": len(coverages),
                "source_skill": maybe_text(artifact_payload.get("skill"))
                or DEFAULT_SOURCE_SKILL,
            }
        return {
            "coverage_wrapper": artifact_payload,
            "coverages": coverages,
            "coverage_count": len(coverages),
            "coverage_source": "coverage-artifact",
            "coverage_file": str(coverage_file),
            "db_path": str(resolved_db_file),
            "analysis_sync": analysis_sync,
            "coverage_artifact_present": True,
            "warnings": [],
        }

    warnings = [
        {
            "code": "missing-coverage",
            "message": f"No evidence coverage result was found for round {round_id} at {coverage_file}.",
        }
    ]
    if not analysis_sync:
        analysis_sync = {
            "status": "missing-coverage",
            "analysis_kind": ANALYSIS_KIND_EVIDENCE_COVERAGE,
            "run_id": run_id,
            "round_id": round_id,
            "coverage_path": str(coverage_file),
            "db_path": str(resolved_db_file),
            "result_set_id": "",
            "item_count": 0,
            "source_skill": DEFAULT_SOURCE_SKILL,
        }
    return {
        "coverage_wrapper": {"coverages": [], "coverage_count": 0},
        "coverages": [],
        "coverage_count": 0,
        "coverage_source": "missing-coverage",
        "coverage_file": str(coverage_file),
        "db_path": str(resolved_db_file),
        "analysis_sync": analysis_sync,
        "coverage_artifact_present": False,
        "warnings": warnings,
    }
