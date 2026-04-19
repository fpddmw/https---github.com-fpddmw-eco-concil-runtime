#!/usr/bin/env python3
"""Materialize retrieval-ready history context from archived cases and signals."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-materialize-history-context"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_scope_context,
    load_observation_scope_context,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_falsification_probe_wrapper,
    load_next_actions_wrapper,
    load_promotion_basis_wrapper,
    load_round_readiness_wrapper,
)

MAX_CASES = 3
MAX_EXCERPTS_PER_CASE = 2
MAX_SIGNALS = 4

CASE_DB_SCHEMA_SQL = """
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
    excerpt_order INTEGER NOT NULL DEFAULT 0
);
"""

EXCERPT_KIND_BONUS = {
    "evidence-card": 2.2,
    "decision-summary": 1.8,
    "round-summary": 1.4,
    "report-summary": 1.1,
    "curated-summary": 0.9,
}


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


def default_case_db_path(run_dir: Path) -> Path:
    return (run_dir / ".." / "archives" / "eco_case_library.sqlite").resolve()


def default_signal_db_path(run_dir: Path) -> Path:
    return (run_dir / ".." / "archives" / "eco_signal_corpus.sqlite").resolve()


def default_case_query_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "archive" / f"case_library_query_{round_id}.json").resolve()


def default_signal_query_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "archive" / f"signal_corpus_query_{round_id}.json").resolve()


def default_retrieval_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "investigation" / f"history_retrieval_{round_id}.json").resolve()


def default_context_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "investigation" / f"history_context_{round_id}.md").resolve()


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


def write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def search_terms(*values: Any) -> list[str]:
    tokens: set[str] = set()
    for value in values:
        text = maybe_text(value).casefold()
        if not text:
            continue
        for raw in text.replace("|", " ").replace(",", " ").split():
            token = raw.strip()
            if len(token) >= 2:
                tokens.add(token)
    return sorted(tokens)


def infer_profile_id(query_text: str, claim_types: list[str], metric_families: list[str], gap_types: list[str]) -> str:
    combined = " ".join([maybe_text(query_text).casefold(), " ".join(claim_types).casefold(), " ".join(metric_families).casefold(), " ".join(gap_types).casefold()])
    if any(token in combined for token in ("smoke", "wildfire", "haze")) and ("air-quality" in metric_families or "station-air-quality" in gap_types):
        return "smoke-transport"
    if any(token in combined for token in ("flood", "river", "overflow")) or "hydrology" in metric_families:
        return "flood-propagation"
    if any(token in combined for token in ("heat", "temperature", "heatwave")):
        return "heatwave-impact"
    return "general-investigation"


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


def infer_metric_family(metric: Any) -> str:
    normalized = maybe_text(metric).casefold().replace(".", "_").replace("-", "_")
    if normalized in {"pm25", "pm2_5", "pm2_5_", "pm2_5m", "pm10", "pm_10", "ozone", "o3", "us_aqi"}:
        return "air-quality"
    if normalized in {"temperature_2m", "wind_speed_10m", "relative_humidity_2m", "precipitation", "precipitation_sum"}:
        return "meteorology"
    if normalized in {"river_discharge", "river_discharge_mean", "river_discharge_max", "river_discharge_min", "gage_height"}:
        return "hydrology"
    if normalized in {"fire_detection", "fire_detection_count"}:
        return "fire-detection"
    return ""


def load_signal_rows(run_dir: Path, run_id: str) -> list[sqlite3.Row]:
    signal_db = (run_dir / "analytics" / "signal_plane.sqlite").resolve()
    if not signal_db.exists():
        return []
    connection = sqlite3.connect(signal_db)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'normalized_signals'").fetchone()
        if row is None:
            return []
        return connection.execute(
            "SELECT * FROM normalized_signals WHERE run_id = ? ORDER BY round_id, signal_id",
            (run_id,),
        ).fetchall()
    finally:
        connection.close()


def build_history_query(
    run_dir: Path,
    run_id: str,
    round_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    mission = load_json_if_exists(run_dir / "mission.json")
    if not isinstance(mission, dict):
        mission = {"run_id": run_id}
    board_summary = load_json_if_exists(run_dir / "board" / f"board_state_summary_{round_id}.json") or {}
    board_brief = read_text_if_exists(run_dir / "board" / f"board_brief_{round_id}.md")
    readiness_wrapper = load_round_readiness_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    readiness = (
        readiness_wrapper.get("payload")
        if isinstance(readiness_wrapper.get("payload"), dict)
        else {}
    )
    next_actions_wrapper = load_next_actions_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    probes_wrapper = load_falsification_probe_wrapper(
        run_dir,
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
    promotion_wrapper = load_promotion_basis_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    promotion = (
        promotion_wrapper.get("payload")
        if isinstance(promotion_wrapper.get("payload"), dict)
        else {}
    )
    analysis_warnings: list[dict[str, str]] = []
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
    analysis_warnings.extend(
        claim_scope_context.get("warnings", [])
        if isinstance(claim_scope_context.get("warnings"), list)
        else []
    )
    analysis_warnings.extend(
        observation_scope_context.get("warnings", [])
        if isinstance(observation_scope_context.get("warnings"), list)
        else []
    )
    signal_rows = load_signal_rows(run_dir, run_id)

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
    active_hypotheses = board_summary.get("active_hypotheses", []) if isinstance(board_summary.get("active_hypotheses"), list) else []
    open_challenges = board_summary.get("open_challenges", []) if isinstance(board_summary.get("open_challenges"), list) else []
    query_fragments = unique_texts(
        [
            maybe_text(mission.get("topic")),
            maybe_text(mission.get("objective")),
            *[
                maybe_text(item.get("title") or item.get("statement"))
                for item in active_hypotheses[:3]
                if isinstance(item, dict)
            ],
        ]
    )
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    region_label = maybe_text(region.get("label"))
    if not region_label:
        for scope in [*claim_scopes, *observation_scopes]:
            if not isinstance(scope, dict):
                continue
            label = scope_region_label(scope)
            if label and "unknown" not in label.casefold():
                region_label = label
                break

    smoke_tokens_present = any(token in " ".join(query_fragments).casefold() for token in ("smoke", "wildfire", "haze"))
    declared_claim_types = [
        maybe_text(scope.get("claim_type"))
        for scope in claim_scopes
        if isinstance(scope, dict) and maybe_text(scope.get("claim_type"))
    ]
    tagged_claim_types = [
        maybe_text(tag)
        for scope in claim_scopes
        if isinstance(scope, dict)
        for tag in scope.get("matching_tags", [])
        if maybe_text(tag) and maybe_text(tag) not in {"air-quality", "meteorology", "hydrology", "fire-detection"}
    ]
    claim_types = unique_texts(
        declared_claim_types
        + tagged_claim_types
        + (["smoke", "wildfire"] if smoke_tokens_present else [])
    )
    metric_families = unique_texts(
        [maybe_text(tag) for scope in observation_scopes if isinstance(scope, dict) for tag in scope.get("matching_tags", []) if maybe_text(tag) in {"air-quality", "meteorology", "hydrology", "fire-detection"}]
        + [infer_metric_family(row["metric"]) for row in signal_rows if infer_metric_family(row["metric"])]
    )
    source_skills = unique_texts([row["source_skill"] for row in signal_rows])
    gap_types = unique_texts(
        (["station-air-quality"] if "air-quality" in metric_families else [])
        + (["meteorology-background"] if "meteorology" in metric_families else [])
        + (["precipitation-hydrology"] if "hydrology" in metric_families else [])
        + ["gate-blocking" for _ in [1] if maybe_text(readiness.get("readiness_status")) != "ready"]
        + ["promotion-withheld" for _ in [1] if maybe_text(promotion.get("promotion_status")) != "promoted"]
    )
    alternatives = unique_texts(
        [maybe_text(item.get("title") or item.get("challenge_statement")) for item in open_challenges[:4] if isinstance(item, dict)]
        + [maybe_text(item.get("statement") or item.get("title")) for item in active_hypotheses[:4] if isinstance(item, dict) and (maybe_number(item.get("confidence")) or 1.0) < 0.65]
    )
    open_questions = unique_texts(
        [maybe_text(item.get("objective")) for item in next_actions.get("ranked_actions", [])[:4] if isinstance(item, dict)]
        + [maybe_text(item.get("falsification_question")) for item in probes.get("probes", [])[:4] if isinstance(item, dict)]
        + [maybe_text(item) for item in readiness.get("gate_reasons", [])[:4] if maybe_text(item)]
    )
    query = {
        "query": " | ".join(query_fragments[:4]),
        "region_label": region_label,
        "profile_id": infer_profile_id(" | ".join(query_fragments), claim_types, metric_families, gap_types),
        "claim_types": claim_types,
        "metric_families": metric_families,
        "gap_types": gap_types,
        "source_skills": source_skills,
        "priority_leg_ids": [],
        "alternative_hypotheses": alternatives,
    }
    current_context = {
        "topic": maybe_text(mission.get("topic")),
        "objective": maybe_text(mission.get("objective")),
        "board_brief_excerpt": maybe_text(board_brief)[:260],
        "open_questions": open_questions,
        "active_hypothesis_count": len(active_hypotheses),
        "open_challenge_count": len(open_challenges),
    }
    analysis_inputs = {
        "claim_scope_path": maybe_text(claim_scope_context.get("claim_scope_file")),
        "observation_scope_path": maybe_text(
            observation_scope_context.get("observation_scope_file")
        ),
        "next_actions_path": maybe_text(next_actions_wrapper.get("artifact_path")),
        "probes_path": maybe_text(probes_wrapper.get("artifact_path")),
        "claim_scope_source": maybe_text(claim_scope_context.get("claim_scope_source"))
        or "missing-claim-scope",
        "observation_scope_source": maybe_text(
            observation_scope_context.get("observation_scope_source")
        )
        or "missing-observation-scope",
        "next_actions_source": maybe_text(next_actions_wrapper.get("source"))
        or "missing-next-actions",
        "probes_source": maybe_text(probes_wrapper.get("source"))
        or "missing-probes",
        "analysis_db_path": maybe_text(observation_scope_context.get("db_path"))
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
            "next_actions_present": bool(next_actions_wrapper.get("payload_present")),
            "next_actions_artifact_present": bool(
                next_actions_wrapper.get("artifact_present")
            ),
            "probes_present": bool(probes_wrapper.get("payload_present")),
            "probes_artifact_present": bool(probes_wrapper.get("artifact_present")),
        },
        "input_analysis_sync": {
            "claim_scope": claim_scope_context.get("analysis_sync", {}),
            "observation_scope": observation_scope_context.get("analysis_sync", {}),
        },
    }
    return query, current_context, analysis_inputs, analysis_warnings


def query_script_path(skill_name: str) -> Path:
    return WORKSPACE_ROOT / "skills" / skill_name / "scripts" / f"{skill_name.replace('-', '_')}.py"


def run_json_script(script_path: Path, *args: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=str(WORKSPACE_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit={completed.returncode}"
        raise RuntimeError(f"Script failed: {script_path.name}: {detail}")
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Script did not emit a JSON object: {script_path.name}")
    return payload


def connect_case_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(CASE_DB_SCHEMA_SQL)
    return connection


def overlap(query_values: list[str], row_values: list[str]) -> list[str]:
    left = {maybe_text(value).casefold() for value in query_values if maybe_text(value)}
    right = {maybe_text(value).casefold() for value in row_values if maybe_text(value)}
    return sorted(left & right)


def lexical_overlap(query_terms: list[str], text: str) -> list[str]:
    haystack = maybe_text(text).casefold()
    return [term for term in query_terms if term and term in haystack]


def score_excerpt(case_row: dict[str, Any], excerpt_row: sqlite3.Row, query: dict[str, Any], query_terms: list[str]) -> tuple[float, list[str]]:
    claim_overlap = overlap(query.get("claim_types", []), parse_json_text(excerpt_row["claim_types_json"], []))
    metric_overlap = overlap(query.get("metric_families", []), parse_json_text(excerpt_row["metric_families_json"], []))
    gap_overlap = overlap(query.get("gap_types", []), parse_json_text(excerpt_row["gap_types_json"], []))
    source_overlap = overlap(query.get("source_skills", []), parse_json_text(excerpt_row["source_skills_json"], []))
    lexical_hits = lexical_overlap(query_terms, excerpt_row["text"])
    score = EXCERPT_KIND_BONUS.get(maybe_text(excerpt_row["artifact_kind"]), 0.6)
    score += 1.0 * len(claim_overlap)
    score += 0.8 * len(metric_overlap)
    score += 1.1 * len(gap_overlap)
    score += 0.4 * len(source_overlap)
    score += min(1.2, 0.2 * len(lexical_hits))
    if maybe_text(case_row.get("profile_id")) and maybe_text(case_row.get("profile_id")) == maybe_text(query.get("profile_id")):
        score += 0.6
    reasons = unique_texts(
        ([f"claim_types:{','.join(claim_overlap[:3])}"] if claim_overlap else [])
        + ([f"metric_families:{','.join(metric_overlap[:3])}"] if metric_overlap else [])
        + ([f"gap_types:{','.join(gap_overlap[:3])}"] if gap_overlap else [])
        + ([f"source_skills:{','.join(source_overlap[:3])}"] if source_overlap else [])
        + ([f"lexical:{','.join(lexical_hits[:4])}"] if lexical_hits else [])
    )
    return score, reasons


def estimated_tokens(*parts: Any) -> int:
    text = " ".join(maybe_text(part) for part in parts if maybe_text(part))
    if not text:
        return 0
    return max(1, int(math.ceil(len(text) / 4.0)))


def render_history_context(snapshot: dict[str, Any]) -> str:
    query = snapshot.get("history_query", {}) if isinstance(snapshot.get("history_query"), dict) else {}
    current_context = snapshot.get("current_context", {}) if isinstance(snapshot.get("current_context"), dict) else {}
    lines = [
        f"# History Context: {maybe_text(snapshot.get('round_id'))}",
        "",
        f"Current investigation profile: {maybe_text(query.get('profile_id')) or 'unknown'}",
        f"Retrieval focus: {maybe_text(query.get('query')) or 'n/a'}",
        f"Region hint: {maybe_text(query.get('region_label')) or 'n/a'}",
        f"Claim types: {', '.join(query.get('claim_types', [])) if isinstance(query.get('claim_types'), list) and query.get('claim_types') else 'n/a'}",
        f"Metric families: {', '.join(query.get('metric_families', [])) if isinstance(query.get('metric_families'), list) and query.get('metric_families') else 'n/a'}",
        f"Gap types: {', '.join(query.get('gap_types', [])) if isinstance(query.get('gap_types'), list) and query.get('gap_types') else 'n/a'}",
        f"alternatives={', '.join(query.get('alternative_hypotheses', [])) if isinstance(query.get('alternative_hypotheses'), list) and query.get('alternative_hypotheses') else 'n/a'}",
        "",
        "## Open Planning Questions",
    ]
    questions = current_context.get("open_questions", []) if isinstance(current_context.get("open_questions"), list) else []
    if questions:
        lines.extend(f"- {maybe_text(item)}" for item in questions)
    else:
        lines.append("- None")
    lines.extend(["", "## Similar Archived Cases"])
    cases = snapshot.get("cases", []) if isinstance(snapshot.get("cases"), list) else []
    if not cases:
        lines.append("- None")
    for index, case in enumerate(cases, start=1):
        if not isinstance(case, dict):
            continue
        lines.append(f"{index}. case_id={maybe_text(case.get('case_id'))} profile={maybe_text(case.get('profile_id'))} score={maybe_number(case.get('score')) or 0.0:.3f} tier={maybe_text(case.get('match_tier'))}")
        lines.append(f"   reasons={', '.join(case.get('match_reasons', [])) if isinstance(case.get('match_reasons'), list) and case.get('match_reasons') else 'n/a'}")
        excerpts = case.get("excerpts", []) if isinstance(case.get("excerpts"), list) else []
        for excerpt_index, excerpt in enumerate(excerpts, start=1):
            if not isinstance(excerpt, dict):
                continue
            lines.append(f"   excerpt_{excerpt_index}={maybe_text(excerpt.get('artifact_kind'))}: {maybe_text(excerpt.get('text'))}")
    lines.extend(["", "## Historical Signal Hints"])
    signals = snapshot.get("signals", []) if isinstance(snapshot.get("signals"), list) else []
    if not signals:
        lines.append("- None")
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        lines.append(
            f"- signal_id={maybe_text(signal.get('signal_id'))} run_id={maybe_text(signal.get('run_id'))} plane={maybe_text(signal.get('plane'))} source_skill={maybe_text(signal.get('source_skill'))} title={maybe_text(signal.get('title') or signal.get('metric'))}"
        )
    return "\n".join(lines) + "\n"


def history_artifact_ref(path: Path, locator: str = "$") -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": locator,
        "artifact_ref": f"{path}:{locator}" if locator else str(path),
    }


def materialize_history_context_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    case_library_db_path: str,
    signal_corpus_db_path: str,
    case_query_path: str,
    signal_query_path: str,
    retrieval_path: str,
    context_path: str,
    max_cases: int,
    max_excerpts_per_case: int,
    max_signals: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    case_db = resolve_path(run_dir_path, case_library_db_path, default_case_db_path(run_dir_path))
    signal_db = resolve_path(run_dir_path, signal_corpus_db_path, default_signal_db_path(run_dir_path))
    case_query_file = resolve_path(run_dir_path, case_query_path, default_case_query_path(run_dir_path, round_id))
    signal_query_file = resolve_path(run_dir_path, signal_query_path, default_signal_query_path(run_dir_path, round_id))
    retrieval_file = resolve_path(run_dir_path, retrieval_path, default_retrieval_path(run_dir_path, round_id))
    context_file = resolve_path(run_dir_path, context_path, default_context_path(run_dir_path, round_id))

    max_cases = max(1, min(MAX_CASES, max_cases))
    max_excerpts_per_case = max(1, min(MAX_EXCERPTS_PER_CASE, max_excerpts_per_case))
    max_signals = max(1, min(MAX_SIGNALS, max_signals))

    warnings: list[dict[str, str]] = []
    history_query, current_context, analysis_inputs, analysis_warnings = (
        build_history_query(run_dir_path, run_id, round_id)
    )
    warnings.extend(analysis_warnings)
    query_text = maybe_text(history_query.get("query"))

    run_json_script(
        query_script_path("eco-query-case-library"),
        "--run-dir",
        str(run_dir_path),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--db-path",
        str(case_db),
        "--output-path",
        str(case_query_file),
        "--query-text",
        query_text,
        "--region-label",
        maybe_text(history_query.get("region_label")),
        "--profile-id",
        maybe_text(history_query.get("profile_id")),
        *[part for value in history_query.get("claim_types", []) if maybe_text(value) for part in ("--claim-type", maybe_text(value))],
        *[part for value in history_query.get("metric_families", []) if maybe_text(value) for part in ("--metric-family", maybe_text(value))],
        *[part for value in history_query.get("gap_types", []) if maybe_text(value) for part in ("--gap-type", maybe_text(value))],
        *[part for value in history_query.get("source_skills", []) if maybe_text(value) for part in ("--source-skill", maybe_text(value))],
        "--exclude-case-id",
        run_id,
        "--limit",
        str(max_cases * 3),
    )
    run_json_script(
        query_script_path("eco-query-signal-corpus"),
        "--run-dir",
        str(run_dir_path),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--db-path",
        str(signal_db),
        "--output-path",
        str(signal_query_file),
        "--query-text",
        query_text,
        "--region-label",
        maybe_text(history_query.get("region_label")),
        *[part for value in history_query.get("metric_families", []) if maybe_text(value) for part in ("--metric-family", maybe_text(value))],
        *[part for value in history_query.get("source_skills", []) if maybe_text(value) for part in ("--source-skill", maybe_text(value))],
        "--exclude-run-id",
        run_id,
        "--limit",
        str(max_signals * 2),
    )

    case_query_artifact = load_json_if_exists(case_query_file)
    signal_query_artifact = load_json_if_exists(signal_query_file)
    case_candidates = case_query_artifact.get("cases", []) if isinstance(case_query_artifact, dict) and isinstance(case_query_artifact.get("cases"), list) else []
    selected_cases = [item for item in case_candidates[:max_cases] if isinstance(item, dict)]
    query_terms = search_terms(query_text, history_query.get("region_label"), *history_query.get("claim_types", []), *history_query.get("metric_families", []), *history_query.get("gap_types", []))

    connection = connect_case_db(case_db)
    try:
        rendered_cases: list[dict[str, Any]] = []
        for case in selected_cases:
            case_id = maybe_text(case.get("case_id"))
            excerpt_rows = connection.execute(
                "SELECT * FROM case_excerpts WHERE case_id = ? ORDER BY excerpt_order, excerpt_id",
                (case_id,),
            ).fetchall()
            scored_excerpts: list[tuple[float, dict[str, Any]]] = []
            for row in excerpt_rows:
                score, reasons = score_excerpt(case, row, history_query, query_terms)
                scored_excerpts.append(
                    (
                        score,
                        {
                            "excerpt_id": maybe_text(row["excerpt_id"]),
                            "artifact_kind": maybe_text(row["artifact_kind"]),
                            "label": maybe_text(row["label"]),
                            "text": maybe_text(row["text"]),
                            "score": round(score, 3),
                            "match_reasons": reasons,
                        },
                    )
                )
            scored_excerpts.sort(key=lambda item: (-item[0], item[1]["excerpt_id"]))
            selected_excerpts = [item[1] for item in scored_excerpts[:max_excerpts_per_case]]
            rendered_cases.append(
                {
                    **case,
                    "match_tier": maybe_text(case.get("score_components", {}).get("match_tier") if isinstance(case.get("score_components"), dict) else ""),
                    "excerpts": selected_excerpts,
                    "excerpt_budget": {
                        "candidate_count": len(scored_excerpts),
                        "selected_count": len(selected_excerpts),
                        "truncated_by_cap": len(scored_excerpts) > len(selected_excerpts),
                    },
                }
            )
    finally:
        connection.close()

    signal_candidates = signal_query_artifact.get("results", []) if isinstance(signal_query_artifact, dict) and isinstance(signal_query_artifact.get("results"), list) else []
    selected_signals = [item for item in signal_candidates[:max_signals] if isinstance(item, dict)]
    estimated_token_cost = estimated_tokens(
        history_query,
        current_context,
        *[excerpt.get("text") for case in rendered_cases for excerpt in case.get("excerpts", []) if isinstance(excerpt, dict)],
        *[signal.get("snippet") for signal in selected_signals if isinstance(signal, dict)],
    )

    snapshot = {
        "schema_version": "archive-history-context-v1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "history_query": history_query,
        "current_context": current_context,
        "claim_scope_path": maybe_text(analysis_inputs.get("claim_scope_path")),
        "observation_scope_path": maybe_text(
            analysis_inputs.get("observation_scope_path")
        ),
        "next_actions_path": maybe_text(analysis_inputs.get("next_actions_path")),
        "probes_path": maybe_text(analysis_inputs.get("probes_path")),
        "claim_scope_source": maybe_text(analysis_inputs.get("claim_scope_source"))
        or "missing-claim-scope",
        "observation_scope_source": maybe_text(
            analysis_inputs.get("observation_scope_source")
        )
        or "missing-observation-scope",
        "next_actions_source": maybe_text(analysis_inputs.get("next_actions_source"))
        or "missing-next-actions",
        "probes_source": maybe_text(analysis_inputs.get("probes_source"))
        or "missing-probes",
        "analysis_db_path": maybe_text(analysis_inputs.get("analysis_db_path")),
        "observed_inputs": analysis_inputs.get("observed_inputs", {}),
        "input_analysis_sync": analysis_inputs.get("input_analysis_sync", {}),
        "case_query_path": str(case_query_file),
        "signal_query_path": str(signal_query_file),
        "budget": {
            "max_cases": max_cases,
            "max_excerpts_per_case": max_excerpts_per_case,
            "max_signals": max_signals,
            "candidate_case_count": len(case_candidates),
            "selected_case_count": len(rendered_cases),
            "candidate_signal_count": len(signal_candidates),
            "selected_signal_count": len(selected_signals),
            "estimated_token_cost": estimated_token_cost,
        },
        "cases": rendered_cases,
        "signals": selected_signals,
    }
    write_json_file(retrieval_file, snapshot)
    write_text_file(context_file, render_history_context(snapshot))

    if not rendered_cases:
        warnings.append({"code": "no-case-matches", "message": "No archived case matches were available for history context."})
    if not selected_signals:
        warnings.append({"code": "no-signal-matches", "message": "No archived signal matches were available for history context."})

    artifact_refs = [
        history_artifact_ref(case_query_file),
        history_artifact_ref(signal_query_file),
        history_artifact_ref(retrieval_file),
        {"signal_id": "", "artifact_path": str(context_file), "record_locator": "", "artifact_ref": str(context_file)},
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "case_query_path": str(case_query_file),
            "signal_query_path": str(signal_query_file),
            "history_retrieval_path": str(retrieval_file),
            "history_context_path": str(context_file),
            "selected_case_count": len(rendered_cases),
            "selected_signal_count": len(selected_signals),
            "claim_scope_source": maybe_text(analysis_inputs.get("claim_scope_source"))
            or "missing-claim-scope",
            "observation_scope_source": maybe_text(
                analysis_inputs.get("observation_scope_source")
            )
            or "missing-observation-scope",
            "next_actions_source": maybe_text(
                analysis_inputs.get("next_actions_source")
            )
            or "missing-next-actions",
            "probes_source": maybe_text(analysis_inputs.get("probes_source"))
            or "missing-probes",
            "analysis_db_path": maybe_text(analysis_inputs.get("analysis_db_path")),
        },
        "receipt_id": "archive-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, retrieval_file)[:20],
        "batch_id": "archivebatch-" + stable_hash(SKILL_NAME, run_id, round_id, context_file)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": unique_texts([item.get("case_id") for item in rendered_cases if isinstance(item, dict)]),
        "warnings": warnings,
        "input_analysis_sync": analysis_inputs.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": unique_texts(
                [item.get("case_id") for item in rendered_cases if isinstance(item, dict)]
                + [item.get("signal_id") for item in selected_signals if isinstance(item, dict)]
            ),
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings],
            "challenge_hints": ["Historical context should inform prioritization and challenge framing, not overwrite current evidence review."] if rendered_cases or selected_signals else [],
            "suggested_next_skills": ["eco-post-board-note", "eco-propose-next-actions", "eco-open-falsification-probe"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize retrieval-ready history context from archived cases and signals.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--case-library-db-path", default="")
    parser.add_argument("--signal-corpus-db-path", default="")
    parser.add_argument("--case-query-path", default="")
    parser.add_argument("--signal-query-path", default="")
    parser.add_argument("--retrieval-path", default="")
    parser.add_argument("--context-path", default="")
    parser.add_argument("--max-cases", type=int, default=3)
    parser.add_argument("--max-excerpts-per-case", type=int, default=2)
    parser.add_argument("--max-signals", type=int, default=4)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_history_context_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        case_library_db_path=args.case_library_db_path,
        signal_corpus_db_path=args.signal_corpus_db_path,
        case_query_path=args.case_query_path,
        signal_query_path=args.signal_query_path,
        retrieval_path=args.retrieval_path,
        context_path=args.context_path,
        max_cases=args.max_cases,
        max_excerpts_per_case=args.max_excerpts_per_case,
        max_signals=args.max_signals,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
