#!/usr/bin/env python3
"""Archive one run's canonical investigation artifacts into a compact case library."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-archive-case-library"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_scope_context,
    load_evidence_coverage_context,
    load_observation_scope_context,
)
from eco_council_runtime.kernel.investigation_planning import (  # noqa: E402
    load_falsification_probe_wrapper,
    load_next_actions_wrapper,
    load_promotion_basis_wrapper,
    load_round_readiness_wrapper,
)

SIGNAL_TABLE = "normalized_signals"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cases (
    case_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    run_dir TEXT NOT NULL,
    topic TEXT NOT NULL DEFAULT '',
    objective TEXT NOT NULL DEFAULT '',
    region_label TEXT NOT NULL DEFAULT '',
    profile_id TEXT NOT NULL DEFAULT '',
    publication_status TEXT NOT NULL DEFAULT '',
    promotion_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    last_round_id TEXT NOT NULL DEFAULT '',
    round_count INTEGER NOT NULL DEFAULT 0,
    final_decision_summary TEXT NOT NULL DEFAULT '',
    board_brief_excerpt TEXT NOT NULL DEFAULT '',
    history_summary_text TEXT NOT NULL DEFAULT '',
    claim_types_json TEXT NOT NULL DEFAULT '[]',
    metric_families_json TEXT NOT NULL DEFAULT '[]',
    gap_types_json TEXT NOT NULL DEFAULT '[]',
    source_skills_json TEXT NOT NULL DEFAULT '[]',
    alternative_hypotheses_json TEXT NOT NULL DEFAULT '[]',
    open_questions_json TEXT NOT NULL DEFAULT '[]',
    selected_evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    mission_json TEXT NOT NULL DEFAULT '{}',
    archive_payload_json TEXT NOT NULL DEFAULT '{}',
    imported_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS case_rounds (
    case_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    readiness_status TEXT NOT NULL DEFAULT '',
    promotion_status TEXT NOT NULL DEFAULT '',
    open_challenge_count INTEGER NOT NULL DEFAULT 0,
    open_task_count INTEGER NOT NULL DEFAULT 0,
    open_probe_count INTEGER NOT NULL DEFAULT 0,
    active_hypothesis_count INTEGER NOT NULL DEFAULT 0,
    strong_coverage_count INTEGER NOT NULL DEFAULT 0,
    moderate_coverage_count INTEGER NOT NULL DEFAULT 0,
    gate_reasons_json TEXT NOT NULL DEFAULT '[]',
    key_findings_json TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY(case_id, round_id),
    FOREIGN KEY(case_id) REFERENCES cases(case_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS case_excerpts (
    excerpt_id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    artifact_kind TEXT NOT NULL,
    label TEXT NOT NULL,
    text TEXT NOT NULL,
    claim_types_json TEXT NOT NULL DEFAULT '[]',
    metric_families_json TEXT NOT NULL DEFAULT '[]',
    gap_types_json TEXT NOT NULL DEFAULT '[]',
    source_skills_json TEXT NOT NULL DEFAULT '[]',
    excerpt_order INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(case_id) REFERENCES cases(case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cases_profile_region ON cases(profile_id, region_label);
CREATE INDEX IF NOT EXISTS idx_case_excerpts_case_round ON case_excerpts(case_id, round_id, artifact_kind);
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


def parse_json_text(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return payload if isinstance(payload, type(default)) else default


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_archive_db_path(run_dir: Path) -> Path:
    return (run_dir / ".." / "archives" / "eco_case_library.sqlite").resolve()


def default_output_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "archive" / f"case_library_import_{round_id}.json").resolve()


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
    return "other" if normalized else ""


def infer_claim_types(*texts: Any, declared: list[str] | None = None) -> list[str]:
    values: list[str] = []
    if declared:
        values.extend(declared)
    joined = " ".join(maybe_text(text).casefold() for text in texts if maybe_text(text))
    if any(token in joined for token in ("smoke", "wildfire", "haze")):
        values.extend(["smoke", "wildfire"])
    if any(token in joined for token in ("flood", "overflow", "inundation", "river")):
        values.append("flood")
    if any(token in joined for token in ("heat", "temperature", "hot")):
        values.append("heat")
    if "air quality" in joined:
        values.append("air-quality")
    return unique_texts(values)


def infer_profile_id(topic: str, objective: str, claim_types: list[str], metric_families: list[str], gap_types: list[str]) -> str:
    combined = " ".join([maybe_text(topic).casefold(), maybe_text(objective).casefold(), " ".join(claim_types).casefold(), " ".join(metric_families).casefold(), " ".join(gap_types).casefold()])
    if any(token in combined for token in ("smoke", "wildfire", "haze")) and ("air-quality" in metric_families or "air-quality" in combined):
        return "smoke-transport"
    if any(token in combined for token in ("flood", "overflow", "river")) or "hydrology" in metric_families:
        return "flood-propagation"
    if any(token in combined for token in ("heat", "temperature", "heatwave")):
        return "heatwave-impact"
    return "general-investigation"


def infer_gap_types(metric_families: list[str], source_skills: list[str], readiness_status: str, promotion_status: str) -> list[str]:
    gaps: list[str] = []
    if "air-quality" in metric_families:
        gaps.append("station-air-quality")
    if "meteorology" in metric_families:
        gaps.append("meteorology-background")
    if "hydrology" in metric_families:
        gaps.append("precipitation-hydrology")
    if "fire-detection" in metric_families or any("fire" in maybe_text(skill).casefold() for skill in source_skills):
        gaps.append("fire-detection")
    if maybe_text(readiness_status) != "ready":
        gaps.append("gate-blocking")
    if maybe_text(promotion_status) != "promoted":
        gaps.append("promotion-withheld")
    return unique_texts(gaps)


def scope_region_label(scope: dict[str, Any]) -> str:
    if not isinstance(scope, dict):
        return ""
    label = maybe_text(scope.get("scope_label"))
    if label:
        return label
    place_scope = scope.get("place_scope")
    if isinstance(place_scope, dict):
        return maybe_text(place_scope.get("label"))
    return ""


def load_board_round_state(run_dir: Path, round_id: str) -> dict[str, Any]:
    board_summary = load_json_if_exists(run_dir / "board" / f"board_state_summary_{round_id}.json")
    board = load_json_if_exists(run_dir / "board" / "investigation_board.json")
    summary_counts = board_summary.get("counts", {}) if isinstance(board_summary, dict) and isinstance(board_summary.get("counts"), dict) else {}
    round_state: dict[str, Any] = {}
    if isinstance(board, dict):
        rounds = board.get("rounds", {}) if isinstance(board.get("rounds"), dict) else {}
        round_state = rounds.get(round_id, {}) if isinstance(rounds.get(round_id), dict) else {}
    hypotheses = [item for item in round_state.get("hypotheses", []) if isinstance(item, dict)] if isinstance(round_state.get("hypotheses"), list) else []
    challenges = [item for item in round_state.get("challenge_tickets", []) if isinstance(item, dict)] if isinstance(round_state.get("challenge_tickets"), list) else []
    tasks = [item for item in round_state.get("tasks", []) if isinstance(item, dict)] if isinstance(round_state.get("tasks"), list) else []
    notes = [item for item in round_state.get("notes", []) if isinstance(item, dict)] if isinstance(round_state.get("notes"), list) else []
    active_hypotheses = [item for item in hypotheses if maybe_text(item.get("status")) not in {"closed", "rejected"}]
    open_challenges = [item for item in challenges if maybe_text(item.get("status")) != "closed"]
    open_tasks = [item for item in tasks if maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}]
    return {
        "counts": {
            "active_hypotheses": int(summary_counts.get("hypotheses_active") or len(active_hypotheses)),
            "open_challenges": int(summary_counts.get("challenge_open") or len(open_challenges)),
            "open_tasks": int(summary_counts.get("tasks_open") or len(open_tasks)),
            "notes_total": int(summary_counts.get("notes_total") or len(notes)),
        },
        "active_hypotheses": active_hypotheses,
        "open_challenges": open_challenges,
        "open_tasks": open_tasks,
        "notes": notes,
    }


def choose_region_label(mission: dict[str, Any], claim_scopes: list[dict[str, Any]], observation_scopes: list[dict[str, Any]]) -> str:
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    mission_label = maybe_text(region.get("label"))
    if mission_label:
        return mission_label
    for scope in [*claim_scopes, *observation_scopes]:
        label = scope_region_label(scope)
        lowered = label.casefold()
        if label and lowered not in {"unknown observation footprint", "public evidence footprint", "signal-backed point footprint"}:
            return label
    return ""


def open_questions(next_actions: dict[str, Any], probes: dict[str, Any], readiness: dict[str, Any]) -> list[str]:
    questions: list[str] = []
    ranked_actions = next_actions.get("ranked_actions", []) if isinstance(next_actions.get("ranked_actions"), list) else []
    for action in ranked_actions[:4]:
        if not isinstance(action, dict):
            continue
        objective = maybe_text(action.get("objective")) or maybe_text(action.get("reason"))
        if objective:
            questions.append(objective)
    for probe in probes.get("probes", []) if isinstance(probes.get("probes"), list) else []:
        if not isinstance(probe, dict):
            continue
        text = maybe_text(probe.get("falsification_question"))
        if text:
            questions.append(text)
    for reason in readiness.get("gate_reasons", []) if isinstance(readiness.get("gate_reasons"), list) else []:
        if maybe_text(reason):
            questions.append(maybe_text(reason))
    return unique_texts(questions)[:6]


def alternative_hypotheses(board_state: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for hypothesis in board_state.get("active_hypotheses", []):
        if not isinstance(hypothesis, dict):
            continue
        confidence = maybe_number(hypothesis.get("confidence"))
        if confidence is not None and confidence < 0.65:
            text = maybe_text(hypothesis.get("statement") or hypothesis.get("title"))
            if text:
                values.append(text)
    for challenge in board_state.get("open_challenges", []):
        if not isinstance(challenge, dict):
            continue
        text = maybe_text(challenge.get("challenge_statement") or challenge.get("title"))
        if text:
            values.append(text)
    return unique_texts(values)[:5]


def coverage_map(coverage_wrapper: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    rows = coverage_wrapper.get("coverages", []) if isinstance(coverage_wrapper.get("coverages"), list) else []
    for row in rows:
        if isinstance(row, dict) and maybe_text(row.get("coverage_id")):
            mapping[maybe_text(row.get("coverage_id"))] = row
    return mapping


def claim_type_lookup(claim_scopes: list[dict[str, Any]]) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}
    for scope in claim_scopes:
        if not isinstance(scope, dict):
            continue
        claim_id = maybe_text(scope.get("claim_id"))
        if not claim_id:
            continue
        values = [maybe_text(scope.get("claim_type"))] + [maybe_text(item) for item in scope.get("matching_tags", []) if maybe_text(item)]
        lookup[claim_id] = unique_texts(values)
    return lookup


def load_archive_analysis_inputs(
    run_dir: Path,
    run_id: str,
    round_id: str,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    claim_scope_context = load_claim_scope_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    observation_scope_context = load_observation_scope_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=maybe_text(claim_scope_context.get("db_path")),
    )
    coverage_context = load_evidence_coverage_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=maybe_text(observation_scope_context.get("db_path"))
        or maybe_text(claim_scope_context.get("db_path")),
    )
    for context in (
        claim_scope_context,
        observation_scope_context,
        coverage_context,
    ):
        warnings.extend(
            context.get("warnings", [])
            if isinstance(context.get("warnings"), list)
            else []
        )

    claim_scopes = (
        claim_scope_context.get("claim_scopes", [])
        if isinstance(claim_scope_context.get("claim_scopes"), list)
        else []
    )
    observation_scopes = (
        observation_scope_context.get("observation_scopes", [])
        if isinstance(observation_scope_context.get("observation_scopes"), list)
        else []
    )
    coverages = (
        coverage_context.get("coverages", [])
        if isinstance(coverage_context.get("coverages"), list)
        else []
    )
    return (
        {
            "coverage_wrapper": (
                coverage_context.get("coverage_wrapper")
                if isinstance(coverage_context.get("coverage_wrapper"), dict)
                else {}
            ),
            "claim_scopes": claim_scopes,
            "observation_scopes": observation_scopes,
            "coverages": coverages,
            "claim_scope_path": maybe_text(claim_scope_context.get("claim_scope_file")),
            "observation_scope_path": maybe_text(
                observation_scope_context.get("observation_scope_file")
            ),
            "coverage_path": maybe_text(coverage_context.get("coverage_file")),
            "claim_scope_source": maybe_text(claim_scope_context.get("claim_scope_source"))
            or "missing-claim-scope",
            "observation_scope_source": maybe_text(
                observation_scope_context.get("observation_scope_source")
            )
            or "missing-observation-scope",
            "coverage_source": maybe_text(coverage_context.get("coverage_source"))
            or "missing-coverage",
            "analysis_db_path": maybe_text(coverage_context.get("db_path"))
            or maybe_text(observation_scope_context.get("db_path"))
            or maybe_text(claim_scope_context.get("db_path")),
            "observed_inputs": {
                "claim_scope_present": bool(claim_scopes),
                "claim_scope_artifact_present": bool(
                    claim_scope_context.get("claim_scope_artifact_present")
                ),
                "observation_scope_present": bool(observation_scopes),
                "observation_scope_artifact_present": bool(
                    observation_scope_context.get("observation_scope_artifact_present")
                ),
                "coverage_present": bool(coverages),
                "coverage_artifact_present": bool(
                    coverage_context.get("coverage_artifact_present")
                ),
            },
            "input_analysis_sync": {
                "claim_scope": claim_scope_context.get("analysis_sync", {}),
                "observation_scope": observation_scope_context.get(
                    "analysis_sync", {}
                ),
                "coverage": coverage_context.get("analysis_sync", {}),
            },
        },
        warnings,
    )


def build_excerpts(
    *,
    run_id: str,
    round_id: str,
    case_id: str,
    final_decision_summary: str,
    board_brief_excerpt: str,
    handoff: dict[str, Any],
    role_reports: list[dict[str, Any]],
    promotion: dict[str, Any],
    coverage_lookup: dict[str, dict[str, Any]],
    board_state: dict[str, Any],
    metric_families: list[str],
    gap_types: list[str],
    source_skills: list[str],
    claim_types_by_id: dict[str, list[str]],
) -> list[dict[str, Any]]:
    excerpts: list[dict[str, Any]] = []

    def append_excerpt(*, artifact_kind: str, label: str, text: str, claim_types: list[str]) -> None:
        if not maybe_text(text):
            return
        excerpt_order = len(excerpts)
        excerpt_id = "case-excerpt-" + stable_hash(case_id, artifact_kind, label, text, excerpt_order)[:16]
        excerpts.append(
            {
                "excerpt_id": excerpt_id,
                "case_id": case_id,
                "round_id": round_id,
                "artifact_kind": artifact_kind,
                "label": label,
                "text": maybe_text(text)[:320],
                "claim_types_json": json_text(unique_texts(claim_types)),
                "metric_families_json": json_text(metric_families),
                "gap_types_json": json_text(gap_types),
                "source_skills_json": json_text(source_skills),
                "excerpt_order": excerpt_order,
            }
        )

    append_excerpt(artifact_kind="decision-summary", label="final decision summary", text=final_decision_summary, claim_types=[])
    append_excerpt(artifact_kind="round-summary", label="board brief excerpt", text=board_brief_excerpt, claim_types=[])

    for finding in handoff.get("key_findings", []) if isinstance(handoff.get("key_findings"), list) else []:
        if not isinstance(finding, dict):
            continue
        claim_id = maybe_text(finding.get("claim_id"))
        append_excerpt(
            artifact_kind="report-summary",
            label=maybe_text(finding.get("title")) or "reporting finding",
            text=maybe_text(finding.get("summary")),
            claim_types=claim_types_by_id.get(claim_id, []),
        )
    for report in role_reports:
        if not isinstance(report, dict):
            continue
        append_excerpt(
            artifact_kind="report-summary",
            label=f"{maybe_text(report.get('agent_role') or report.get('canonical_role') or 'role')} report",
            text=maybe_text(report.get("summary")),
            claim_types=infer_claim_types(report.get("summary")),
        )
    for coverage in promotion.get("selected_coverages", []) if isinstance(promotion.get("selected_coverages"), list) else []:
        if not isinstance(coverage, dict):
            continue
        coverage_id = maybe_text(coverage.get("coverage_id"))
        detail = coverage_lookup.get(coverage_id, coverage)
        claim_id = maybe_text(detail.get("claim_id") or coverage.get("claim_id"))
        text = (
            f"coverage_id={coverage_id} claim_id={claim_id} readiness={maybe_text(detail.get('readiness') or coverage.get('readiness'))} "
            f"score={maybe_number(detail.get('coverage_score') or coverage.get('coverage_score')) or 0.0:.2f} "
            f"support_links={int(detail.get('support_link_count') or coverage.get('support_link_count') or 0)} "
            f"contradiction_links={int(detail.get('contradiction_link_count') or coverage.get('contradiction_link_count') or 0)}"
        )
        append_excerpt(
            artifact_kind="evidence-card",
            label="promoted evidence coverage",
            text=text,
            claim_types=claim_types_by_id.get(claim_id, []),
        )
    for hypothesis in board_state.get("active_hypotheses", [])[:3]:
        if not isinstance(hypothesis, dict):
            continue
        append_excerpt(
            artifact_kind="curated-summary",
            label=maybe_text(hypothesis.get("title")) or "active hypothesis",
            text=maybe_text(hypothesis.get("statement") or hypothesis.get("title")),
            claim_types=infer_claim_types(hypothesis.get("statement"), hypothesis.get("title")),
        )
    return excerpts[:10]


def archive_ref_for_output(path: Path) -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": "",
        "artifact_ref": str(path),
    }


def archive_case_library_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    archive_db = resolve_path(run_dir_path, db_path, default_archive_db_path(run_dir_path))
    output_file = resolve_path(run_dir_path, output_path, default_output_path(run_dir_path, round_id))

    mission = load_json_if_exists(run_dir_path / "mission.json")
    if not isinstance(mission, dict):
        mission = {"run_id": run_id}
    board_brief_text = read_text_if_exists(run_dir_path / "board" / f"board_brief_{round_id}.md")
    board_state = load_board_round_state(run_dir_path, round_id)
    warnings: list[dict[str, str]] = []
    analysis_inputs, analysis_warnings = load_archive_analysis_inputs(
        run_dir_path,
        run_id,
        round_id,
    )
    warnings.extend(analysis_warnings)
    coverage_wrapper = analysis_inputs.get("coverage_wrapper", {})
    next_actions_wrapper = load_next_actions_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    probes_wrapper = load_falsification_probe_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    next_actions = (
        next_actions_wrapper.get("payload")
        if isinstance(next_actions_wrapper.get("payload"), dict)
        else {}
    )
    probes = (
        probes_wrapper.get("payload")
        if isinstance(probes_wrapper.get("payload"), dict)
        else {}
    )
    readiness_wrapper = load_round_readiness_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    readiness = (
        readiness_wrapper.get("payload")
        if isinstance(readiness_wrapper.get("payload"), dict)
        else {}
    )
    promotion_wrapper = load_promotion_basis_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    promotion = (
        promotion_wrapper.get("payload")
        if isinstance(promotion_wrapper.get("payload"), dict)
        else {}
    )
    handoff = load_json_if_exists(run_dir_path / "reporting" / f"reporting_handoff_{round_id}.json") or {}
    decision = load_json_if_exists(run_dir_path / "reporting" / f"council_decision_{round_id}.json") or load_json_if_exists(run_dir_path / "reporting" / f"council_decision_draft_{round_id}.json") or {}
    final_publication = load_json_if_exists(run_dir_path / "reporting" / f"final_publication_{round_id}.json") or {}
    role_reports = [
        load_json_if_exists(run_dir_path / "reporting" / f"expert_report_sociologist_{round_id}.json") or {},
        load_json_if_exists(run_dir_path / "reporting" / f"expert_report_environmentalist_{round_id}.json") or {},
    ]
    signal_rows = load_signal_rows((run_dir_path / "analytics" / "signal_plane.sqlite").resolve(), run_id)

    claim_scopes = [
        scope
        for scope in analysis_inputs.get("claim_scopes", [])
        if isinstance(scope, dict)
    ]
    observation_scopes = [
        scope
        for scope in analysis_inputs.get("observation_scopes", [])
        if isinstance(scope, dict)
    ]
    coverages = [
        coverage
        for coverage in analysis_inputs.get("coverages", [])
        if isinstance(coverage, dict)
    ]
    declared_claim_types = [maybe_text(scope.get("claim_type")) for scope in claim_scopes if isinstance(scope, dict) and maybe_text(scope.get("claim_type"))]
    topic = maybe_text(mission.get("topic")) or maybe_text(final_publication.get("publication_summary")) or maybe_text(decision.get("decision_summary")) or f"Archived case {run_id}"
    objective = maybe_text(mission.get("objective")) or maybe_text(handoff.get("board_brief_excerpt")) or maybe_text(board_brief_text)[:220]
    claim_types = infer_claim_types(topic, objective, board_brief_text, declared=declared_claim_types)
    metric_families = unique_texts(
        [metric_family(row["metric"]) for row in signal_rows if metric_family(row["metric"])]
        + [maybe_text(tag) for scope in observation_scopes if isinstance(scope, dict) for tag in scope.get("matching_tags", []) if maybe_text(tag) in {"air-quality", "meteorology", "hydrology", "fire-detection"}]
    )
    source_skills = unique_texts([row["source_skill"] for row in signal_rows])
    readiness_status = maybe_text(readiness.get("readiness_status")) or "blocked"
    promotion_status = maybe_text(promotion.get("promotion_status")) or "withheld"
    gap_types = infer_gap_types(metric_families, source_skills, readiness_status, promotion_status)
    region_label = choose_region_label(mission, [scope for scope in claim_scopes if isinstance(scope, dict)], [scope for scope in observation_scopes if isinstance(scope, dict)])
    profile_id = infer_profile_id(topic, objective, claim_types, metric_families, gap_types)
    alternatives = alternative_hypotheses(board_state)
    questions = open_questions(next_actions, probes, readiness)
    final_decision_summary = maybe_text(final_publication.get("publication_summary")) or maybe_text(decision.get("decision_summary")) or maybe_text(promotion.get("promotion_notes"))
    board_brief_excerpt = maybe_text(board_brief_text)[:320]
    history_summary_text = maybe_text(
        " ".join(
            [
                final_decision_summary,
                board_brief_excerpt,
                maybe_text((handoff.get("key_findings") or [{}])[0].get("summary")) if isinstance(handoff.get("key_findings"), list) and handoff.get("key_findings") else "",
            ]
        )
    )[:480]
    selected_evidence_refs = unique_texts(
        (final_publication.get("selected_evidence_refs", []) if isinstance(final_publication.get("selected_evidence_refs"), list) else [])
        + (decision.get("selected_evidence_refs", []) if isinstance(decision.get("selected_evidence_refs"), list) else [])
        + (handoff.get("selected_evidence_refs", []) if isinstance(handoff.get("selected_evidence_refs"), list) else [])
        + (promotion.get("selected_evidence_refs", []) if isinstance(promotion.get("selected_evidence_refs"), list) else [])
    )
    claim_types_by_id = claim_type_lookup([scope for scope in claim_scopes if isinstance(scope, dict)])
    excerpts = build_excerpts(
        run_id=run_id,
        round_id=round_id,
        case_id=maybe_text(mission.get("run_id")) or run_id,
        final_decision_summary=final_decision_summary,
        board_brief_excerpt=board_brief_excerpt,
        handoff=handoff if isinstance(handoff, dict) else {},
        role_reports=[item for item in role_reports if isinstance(item, dict)],
        promotion=promotion if isinstance(promotion, dict) else {},
        coverage_lookup=coverage_map(coverage_wrapper if isinstance(coverage_wrapper, dict) else {}),
        board_state=board_state,
        metric_families=metric_families,
        gap_types=gap_types,
        source_skills=source_skills,
        claim_types_by_id=claim_types_by_id,
    )

    publication_status = maybe_text(final_publication.get("publication_status")) or ("ready-for-release" if maybe_text(decision.get("publication_readiness")) == "ready" else "hold-release")
    case_id = maybe_text(mission.get("run_id")) or run_id
    observed_inputs = (
        dict(analysis_inputs.get("observed_inputs"))
        if isinstance(analysis_inputs.get("observed_inputs"), dict)
        else {}
    )
    observed_inputs.update(
        {
            "next_actions_present": bool(next_actions_wrapper.get("payload_present")),
            "next_actions_artifact_present": bool(
                next_actions_wrapper.get("artifact_present")
            ),
            "probes_present": bool(probes_wrapper.get("payload_present")),
            "probes_artifact_present": bool(probes_wrapper.get("artifact_present")),
        }
    )
    archive_payload = {
        "topic": topic,
        "objective": objective,
        "region_label": region_label,
        "profile_id": profile_id,
        "claim_types": claim_types,
        "metric_families": metric_families,
        "gap_types": gap_types,
        "source_skills": source_skills,
        "alternative_hypotheses": alternatives,
        "open_questions": questions,
        "selected_evidence_refs": selected_evidence_refs,
    }

    if not signal_rows:
        warnings.append({"code": "missing-signal-plane", "message": "Case library import found no normalized signal rows for this run."})
    if not final_decision_summary:
        warnings.append({"code": "missing-decision-summary", "message": "Case library import could not find a final decision-style summary and fell back to board/report text."})

    connection = connect_archive_db(archive_db)
    try:
        existing = connection.execute("SELECT 1 FROM cases WHERE case_id = ?", (case_id,)).fetchone() is not None
        connection.execute("DELETE FROM cases WHERE case_id = ?", (case_id,))
        connection.execute(
            """
            INSERT INTO cases (
                case_id, run_id, run_dir, topic, objective, region_label, profile_id,
                publication_status, promotion_status, readiness_status, last_round_id, round_count,
                final_decision_summary, board_brief_excerpt, history_summary_text,
                claim_types_json, metric_families_json, gap_types_json, source_skills_json,
                alternative_hypotheses_json, open_questions_json, selected_evidence_refs_json,
                mission_json, archive_payload_json, imported_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id,
                run_id,
                str(run_dir_path),
                topic,
                objective,
                region_label,
                profile_id,
                publication_status,
                promotion_status,
                readiness_status,
                round_id,
                1,
                final_decision_summary,
                board_brief_excerpt,
                history_summary_text,
                json_text(claim_types),
                json_text(metric_families),
                json_text(gap_types),
                json_text(source_skills),
                json_text(alternatives),
                json_text(questions),
                json_text(selected_evidence_refs),
                json_text(mission),
                json_text(archive_payload),
                utc_now_iso(),
            ),
        )
        strong_coverages = len(
            [
                row
                for row in coverages
                if maybe_text(row.get("readiness")) == "strong"
            ]
        )
        moderate_coverages = len(
            [
                row
                for row in coverages
                if maybe_text(row.get("readiness")) == "partial"
            ]
        )
        connection.execute(
            """
            INSERT INTO case_rounds (
                case_id, round_id, readiness_status, promotion_status, open_challenge_count,
                open_task_count, open_probe_count, active_hypothesis_count, strong_coverage_count,
                moderate_coverage_count, gate_reasons_json, key_findings_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id,
                round_id,
                readiness_status,
                promotion_status,
                int(board_state["counts"].get("open_challenges") or 0),
                int(board_state["counts"].get("open_tasks") or 0),
                len([item for item in probes.get("probes", []) if isinstance(item, dict)]) if isinstance(probes.get("probes"), list) else 0,
                int(board_state["counts"].get("active_hypotheses") or 0),
                strong_coverages,
                moderate_coverages,
                json_text(readiness.get("gate_reasons", []) if isinstance(readiness.get("gate_reasons"), list) else []),
                json_text(handoff.get("key_findings", []) if isinstance(handoff.get("key_findings"), list) else []),
            ),
        )
        for excerpt in excerpts:
            connection.execute(
                """
                INSERT INTO case_excerpts (
                    excerpt_id, case_id, round_id, artifact_kind, label, text,
                    claim_types_json, metric_families_json, gap_types_json, source_skills_json, excerpt_order
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    excerpt["excerpt_id"],
                    excerpt["case_id"],
                    excerpt["round_id"],
                    excerpt["artifact_kind"],
                    excerpt["label"],
                    excerpt["text"],
                    excerpt["claim_types_json"],
                    excerpt["metric_families_json"],
                    excerpt["gap_types_json"],
                    excerpt["source_skills_json"],
                    int(excerpt["excerpt_order"]),
                ),
            )
        connection.commit()
    finally:
        connection.close()

    import_id = "case-library-import-" + stable_hash(case_id, round_id, archive_db)[:12]
    snapshot = {
        "schema_version": "archive-case-library-v1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "case_id": case_id,
        "import_id": import_id,
        "db_path": str(archive_db),
        "output_path": str(output_file),
        "claim_scope_path": maybe_text(analysis_inputs.get("claim_scope_path")),
        "observation_scope_path": maybe_text(
            analysis_inputs.get("observation_scope_path")
        ),
        "coverage_path": maybe_text(analysis_inputs.get("coverage_path")),
        "next_actions_path": maybe_text(next_actions_wrapper.get("artifact_path")),
        "probes_path": maybe_text(probes_wrapper.get("artifact_path")),
        "claim_scope_source": maybe_text(analysis_inputs.get("claim_scope_source"))
        or "missing-claim-scope",
        "observation_scope_source": maybe_text(
            analysis_inputs.get("observation_scope_source")
        )
        or "missing-observation-scope",
        "coverage_source": maybe_text(analysis_inputs.get("coverage_source"))
        or "missing-coverage",
        "next_actions_source": maybe_text(next_actions_wrapper.get("source"))
        or "missing-next-actions",
        "probes_source": maybe_text(probes_wrapper.get("source"))
        or "missing-probes",
        "analysis_db_path": maybe_text(analysis_inputs.get("analysis_db_path")),
        "observed_inputs": observed_inputs,
        "input_analysis_sync": analysis_inputs.get("input_analysis_sync", {}),
        "profile_id": profile_id,
        "topic": topic,
        "objective": objective,
        "region_label": region_label,
        "replaced_existing": existing,
        "publication_status": publication_status,
        "promotion_status": promotion_status,
        "readiness_status": readiness_status,
        "claim_types": claim_types,
        "metric_families": metric_families,
        "gap_types": gap_types,
        "source_skills": source_skills,
        "excerpt_count": len(excerpts),
    }
    write_json_file(output_file, snapshot)

    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
        archive_ref_for_output(archive_db),
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "case_id": case_id,
            "import_id": import_id,
            "db_path": str(archive_db),
            "output_path": str(output_file),
            "profile_id": profile_id,
            "excerpt_count": len(excerpts),
            "claim_scope_source": maybe_text(analysis_inputs.get("claim_scope_source"))
            or "missing-claim-scope",
            "observation_scope_source": maybe_text(
                analysis_inputs.get("observation_scope_source")
            )
            or "missing-observation-scope",
            "coverage_source": maybe_text(analysis_inputs.get("coverage_source"))
            or "missing-coverage",
            "next_actions_source": maybe_text(next_actions_wrapper.get("source"))
            or "missing-next-actions",
            "probes_source": maybe_text(probes_wrapper.get("source"))
            or "missing-probes",
            "analysis_db_path": maybe_text(analysis_inputs.get("analysis_db_path")),
        },
        "receipt_id": "archive-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, import_id)[:20],
        "batch_id": "archivebatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [case_id],
        "warnings": warnings,
        "input_analysis_sync": analysis_inputs.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [case_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings] if warnings else [],
            "challenge_hints": ["Archived cases remain analog evidence, not direct proof for the current round."] if excerpts else [],
            "suggested_next_skills": ["eco-query-case-library", "eco-materialize-history-context"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive one run's canonical investigation artifacts into a compact case library.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = archive_case_library_skill(
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
