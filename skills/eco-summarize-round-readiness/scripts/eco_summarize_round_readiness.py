#!/usr/bin/env python3
"""Summarize round-level readiness from board, D1, and evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-summarize-round-readiness"


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


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
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


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def readiness_status(*, active_hypotheses: int, strong_coverages: int, moderate_coverages: int, open_challenges: int, open_tasks: int, open_probes: int, high_priority_actions: int) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if active_hypotheses == 0:
        reasons.append("No active hypotheses are available for round-level review.")
        return "blocked", reasons
    if strong_coverages + moderate_coverages == 0:
        reasons.append("No moderate-or-strong evidence coverage objects are available.")
        return "blocked", reasons
    if open_challenges > 0:
        reasons.append(f"{open_challenges} challenge tickets remain open.")
    if open_tasks > 0:
        reasons.append(f"{open_tasks} board tasks remain in flight.")
    if open_probes > 0:
        reasons.append(f"{open_probes} falsification probes remain open.")
    if high_priority_actions > 0:
        reasons.append(f"{high_priority_actions} high-priority investigation actions remain unresolved.")
    if reasons:
        return "needs-more-data", reasons
    if strong_coverages > 0:
        reasons.append("At least one strong evidence coverage object is available and no blocking board objects remain.")
        return "ready", reasons
    reasons.append("Coverage is only moderate and the round should remain open for additional investigation.")
    return "needs-more-data", reasons


def summarize_round_readiness_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_summary_path: str,
    board_brief_path: str,
    next_actions_path: str,
    probes_path: str,
    coverage_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_summary_file = resolve_path(run_dir_path, board_summary_path, f"board/board_state_summary_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    next_actions_file = resolve_path(run_dir_path, next_actions_path, f"investigation/next_actions_{round_id}.json")
    probes_file = resolve_path(run_dir_path, probes_path, f"investigation/falsification_probes_{round_id}.json")
    coverage_file = resolve_path(run_dir_path, coverage_path, f"analytics/evidence_coverage_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/round_readiness_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    board_summary = load_json_if_exists(board_summary_file)
    if not isinstance(board_summary, dict):
        warnings.append({"code": "missing-board-summary", "message": f"No board summary artifact was found at {board_summary_file}."})
        board_summary = {"counts": {}, "active_hypotheses": [], "open_challenges": [], "open_tasks": []}
    next_actions = load_json_if_exists(next_actions_file)
    if not isinstance(next_actions, dict):
        next_actions = {"ranked_actions": [], "action_count": 0}
    probes = load_json_if_exists(probes_file)
    if not isinstance(probes, dict):
        probes = {"probes": [], "probe_count": 0}
    coverage_wrapper = load_json_if_exists(coverage_file)
    if not isinstance(coverage_wrapper, dict):
        warnings.append({"code": "missing-coverage", "message": f"No evidence coverage artifact was found at {coverage_file}."})
        coverage_wrapper = {"coverages": [], "coverage_count": 0}
    brief_excerpt = maybe_text(load_text_if_exists(board_brief_file))[:220]

    coverages = [item for item in coverage_wrapper.get("coverages", []) if isinstance(item, dict)] if isinstance(coverage_wrapper.get("coverages"), list) else []
    strong_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "strong"])
    moderate_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "moderate"])
    weak_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "weak"])

    counts = board_summary.get("counts", {}) if isinstance(board_summary.get("counts"), dict) else {}
    active_hypotheses = int(counts.get("hypotheses_active") or len(board_summary.get("active_hypotheses", [])))
    open_challenges = int(counts.get("challenge_open") or len(board_summary.get("open_challenges", [])))
    open_tasks = int(counts.get("tasks_open") or len(board_summary.get("open_tasks", [])))
    open_probes = len([item for item in probes.get("probes", []) if isinstance(item, dict) and maybe_text(item.get("probe_status")) not in {"closed", "cancelled"}]) if isinstance(probes.get("probes"), list) else 0
    high_priority_actions = len(
        [
            item
            for item in next_actions.get("ranked_actions", [])
            if isinstance(item, dict)
            and maybe_text(item.get("priority")) in {"high", "critical"}
            and maybe_text(item.get("action_kind")) != "prepare-promotion"
        ]
    ) if isinstance(next_actions.get("ranked_actions"), list) else 0

    status_value, reasons = readiness_status(
        active_hypotheses=active_hypotheses,
        strong_coverages=strong_coverages,
        moderate_coverages=moderate_coverages,
        open_challenges=open_challenges,
        open_tasks=open_tasks,
        open_probes=open_probes,
        high_priority_actions=high_priority_actions,
    )
    findings = [
        {"finding_id": "readiness-coverage", "title": "Coverage posture", "summary": f"strong={strong_coverages}, moderate={moderate_coverages}, weak={weak_coverages}", "confidence": "medium"},
        {"finding_id": "readiness-board", "title": "Board posture", "summary": f"active_hypotheses={active_hypotheses}, open_challenges={open_challenges}, open_tasks={open_tasks}", "confidence": "medium"},
        {"finding_id": "readiness-investigation", "title": "Investigation posture", "summary": f"open_probes={open_probes}, high_priority_actions={high_priority_actions}", "confidence": "medium"},
    ]
    if brief_excerpt:
        findings.append({"finding_id": "readiness-brief", "title": "Board brief context", "summary": brief_excerpt, "confidence": "low"})

    recommended_next_skills = ["eco-promote-evidence-basis"] if status_value == "ready" else ["eco-propose-next-actions", "eco-open-falsification-probe", "eco-post-board-note"]
    wrapper = {
        "schema_version": "d2.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "board_summary_path": str(board_summary_file),
        "board_brief_path": str(board_brief_file),
        "next_actions_path": str(next_actions_file),
        "probes_path": str(probes_file),
        "coverage_path": str(coverage_file),
        "readiness_status": status_value,
        "sufficient_for_promotion": status_value == "ready",
        "counts": {
            "active_hypotheses": active_hypotheses,
            "open_challenges": open_challenges,
            "open_tasks": open_tasks,
            "open_probes": open_probes,
            "strong_coverages": strong_coverages,
            "moderate_coverages": moderate_coverages,
            "weak_coverages": weak_coverages,
            "high_priority_actions": high_priority_actions,
        },
        "gate_reasons": reasons,
        "findings": findings[:4],
        "recommended_next_skills": recommended_next_skills,
    }
    write_json_file(output_file, wrapper)
    readiness_id = "round-readiness-" + stable_hash(run_id, round_id, status_value)[:12]
    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "readiness_status": status_value, "readiness_id": readiness_id},
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, readiness_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [readiness_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [readiness_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if status_value == "ready" else reasons[:3],
            "challenge_hints": [reason for reason in reasons if "challenge" in reason.lower() or "probe" in reason.lower()],
            "suggested_next_skills": recommended_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize round-level readiness from board, D1, and evidence artifacts.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-summary-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--probes-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = summarize_round_readiness_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_summary_path=args.board_summary_path,
        board_brief_path=args.board_brief_path,
        next_actions_path=args.next_actions_path,
        probes_path=args.probes_path,
        coverage_path=args.coverage_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())