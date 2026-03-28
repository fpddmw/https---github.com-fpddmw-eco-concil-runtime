#!/usr/bin/env python3
"""Propose a ranked next-action queue from board and evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-propose-next-actions"

PRIORITY_WEIGHT = {"critical": 4.0, "high": 3.0, "medium": 2.0, "low": 1.0}
ACTION_KIND_WEIGHT = {
    "resolve-challenge": 2.6,
    "resolve-contradiction": 2.4,
    "finish-board-task": 2.1,
    "stabilize-hypothesis": 1.8,
    "expand-coverage": 1.7,
    "prepare-promotion": 1.2,
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


def excerpt_text(text: str, limit: int = 180) -> str:
    normalized = maybe_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def priority_score(priority: str) -> float:
    return PRIORITY_WEIGHT.get(maybe_text(priority).lower(), PRIORITY_WEIGHT["medium"])


def score_action(payload: dict[str, Any]) -> float:
    priority_component = priority_score(payload.get("priority"))
    action_kind_component = ACTION_KIND_WEIGHT.get(maybe_text(payload.get("action_kind")), 1.0)
    contradiction_component = min(1.5, float(payload.get("contradiction_link_count") or 0) * 0.4)
    coverage_component = max(0.0, 1.0 - float(payload.get("coverage_score") or 0.0))
    confidence_value = maybe_number(payload.get("confidence"))
    uncertainty_component = 0.0 if confidence_value is None else max(0.0, 0.9 - float(confidence_value))
    probe_component = 0.5 if bool(payload.get("probe_candidate")) else 0.0
    return round(priority_component + action_kind_component + contradiction_component + coverage_component + uncertainty_component + probe_component, 3)


def action_from_open_challenge(challenge: dict[str, Any], brief_context: str) -> dict[str, Any]:
    ticket_id = maybe_text(challenge.get("ticket_id"))
    target_claim_id = maybe_text(challenge.get("target_claim_id"))
    target_hypothesis_id = maybe_text(challenge.get("target_hypothesis_id"))
    return {
        "action_id": "action-" + stable_hash(SKILL_NAME, ticket_id, "challenge")[:12],
        "action_kind": "resolve-challenge",
        "priority": maybe_text(challenge.get("priority")) or "high",
        "assigned_role": maybe_text(challenge.get("owner_role")) or "challenger",
        "objective": maybe_text(challenge.get("title")) or "Resolve an open challenge ticket.",
        "reason": maybe_text(challenge.get("title")) or "An open challenge ticket still needs explicit follow-up.",
        "source_ids": unique_texts([ticket_id, target_claim_id, target_hypothesis_id]),
        "target": {"ticket_id": ticket_id, "claim_id": target_claim_id, "hypothesis_id": target_hypothesis_id},
        "evidence_refs": unique_texts(challenge.get("linked_artifact_refs", []) if isinstance(challenge.get("linked_artifact_refs"), list) else []),
        "probe_candidate": True,
        "contradiction_link_count": 1,
        "coverage_score": 0.45,
        "confidence": None,
        "brief_context": brief_context,
    }


def action_from_open_task(task: dict[str, Any], brief_context: str) -> dict[str, Any]:
    task_id = maybe_text(task.get("task_id"))
    return {
        "action_id": "action-" + stable_hash(SKILL_NAME, task_id, "task")[:12],
        "action_kind": "finish-board-task",
        "priority": maybe_text(task.get("priority")) or "medium",
        "assigned_role": maybe_text(task.get("owner_role")) or "moderator",
        "objective": maybe_text(task.get("title")) or "Finish a claimed board task.",
        "reason": maybe_text(task.get("title")) or maybe_text(task.get("task_text")) or "A claimed task is still in flight.",
        "source_ids": unique_texts([task_id, task.get("source_ticket_id"), task.get("source_hypothesis_id")]),
        "target": {"task_id": task_id, "ticket_id": maybe_text(task.get("source_ticket_id")), "hypothesis_id": maybe_text(task.get("source_hypothesis_id"))},
        "evidence_refs": unique_texts(task.get("linked_artifact_refs", []) if isinstance(task.get("linked_artifact_refs"), list) else []),
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": 0.55,
        "confidence": None,
        "brief_context": brief_context,
    }


def action_from_hypothesis(hypothesis: dict[str, Any], brief_context: str) -> dict[str, Any] | None:
    confidence = maybe_number(hypothesis.get("confidence"))
    if confidence is not None and confidence >= 0.75:
        return None
    hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
    linked_claim_ids = unique_texts(hypothesis.get("linked_claim_ids", []) if isinstance(hypothesis.get("linked_claim_ids"), list) else [])
    return {
        "action_id": "action-" + stable_hash(SKILL_NAME, hypothesis_id, "hypothesis")[:12],
        "action_kind": "stabilize-hypothesis",
        "priority": "high" if (confidence or 0.0) < 0.6 else "medium",
        "assigned_role": maybe_text(hypothesis.get("owner_role")) or "moderator",
        "objective": maybe_text(hypothesis.get("title")) or "Stabilize an active hypothesis.",
        "reason": "The board still carries an active hypothesis with limited confidence.",
        "source_ids": unique_texts([hypothesis_id] + linked_claim_ids),
        "target": {"hypothesis_id": hypothesis_id, "claim_id": linked_claim_ids[0] if linked_claim_ids else ""},
        "evidence_refs": [],
        "probe_candidate": (confidence or 0.0) < 0.6,
        "contradiction_link_count": 0,
        "coverage_score": 0.5,
        "confidence": confidence,
        "brief_context": brief_context,
    }


def role_from_coverage(coverage: dict[str, Any]) -> str:
    if int(coverage.get("contradiction_link_count") or 0) > 0:
        return "challenger"
    if int(coverage.get("linked_observation_count") or 0) > 0:
        return "environmentalist"
    return "sociologist"


def action_from_coverage(coverage: dict[str, Any], brief_context: str) -> dict[str, Any] | None:
    readiness = maybe_text(coverage.get("readiness"))
    contradiction_count = int(coverage.get("contradiction_link_count") or 0)
    if readiness == "strong" and contradiction_count == 0:
        return None
    coverage_id = maybe_text(coverage.get("coverage_id"))
    claim_id = maybe_text(coverage.get("claim_id"))
    action_kind = "resolve-contradiction" if contradiction_count > 0 else "expand-coverage"
    priority = "high" if contradiction_count > 0 or readiness == "weak" else "medium"
    objective = "Resolve contradiction-leaning evidence." if contradiction_count > 0 else "Expand evidence coverage for a still-weak claim."
    reason = (
        f"Claim {claim_id} has {contradiction_count} contradiction links."
        if contradiction_count > 0
        else f"Claim {claim_id} is only {readiness or 'unknown'} on evidence coverage."
    )
    return {
        "action_id": "action-" + stable_hash(SKILL_NAME, coverage_id, action_kind)[:12],
        "action_kind": action_kind,
        "priority": priority,
        "assigned_role": role_from_coverage(coverage),
        "objective": objective,
        "reason": reason,
        "source_ids": unique_texts([coverage_id, claim_id]),
        "target": {"coverage_id": coverage_id, "claim_id": claim_id},
        "evidence_refs": unique_texts(coverage.get("evidence_refs", []) if isinstance(coverage.get("evidence_refs"), list) else []),
        "probe_candidate": contradiction_count > 0 or readiness == "weak",
        "contradiction_link_count": contradiction_count,
        "coverage_score": float(coverage.get("coverage_score") or 0.0),
        "confidence": None,
        "brief_context": brief_context,
    }


def prepare_promotion_action(coverage: dict[str, Any], brief_context: str) -> dict[str, Any]:
    coverage_id = maybe_text(coverage.get("coverage_id"))
    claim_id = maybe_text(coverage.get("claim_id"))
    return {
        "action_id": "action-" + stable_hash(SKILL_NAME, coverage_id, "promotion")[:12],
        "action_kind": "prepare-promotion",
        "priority": "medium",
        "assigned_role": "moderator",
        "objective": "Prepare the round for readiness and promotion review.",
        "reason": f"Claim {claim_id} already has strong evidence coverage and can move toward readiness gating.",
        "source_ids": unique_texts([coverage_id, claim_id]),
        "target": {"coverage_id": coverage_id, "claim_id": claim_id},
        "evidence_refs": unique_texts(coverage.get("evidence_refs", []) if isinstance(coverage.get("evidence_refs"), list) else []),
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": float(coverage.get("coverage_score") or 0.0),
        "confidence": None,
        "brief_context": brief_context,
    }


def build_actions(board_summary: dict[str, Any], coverages: list[dict[str, Any]], brief_text: str) -> list[dict[str, Any]]:
    brief_context = excerpt_text(brief_text)
    actions: list[dict[str, Any]] = []
    for challenge in board_summary.get("open_challenges", []):
        if isinstance(challenge, dict):
            actions.append(action_from_open_challenge(challenge, brief_context))
    for task in board_summary.get("open_tasks", []):
        if isinstance(task, dict):
            actions.append(action_from_open_task(task, brief_context))
    for hypothesis in board_summary.get("active_hypotheses", []):
        if isinstance(hypothesis, dict):
            candidate = action_from_hypothesis(hypothesis, brief_context)
            if candidate is not None:
                actions.append(candidate)
    for coverage in coverages:
        if isinstance(coverage, dict):
            candidate = action_from_coverage(coverage, brief_context)
            if candidate is not None:
                actions.append(candidate)
    if not actions:
        strong_coverages = [coverage for coverage in coverages if isinstance(coverage, dict) and maybe_text(coverage.get("readiness")) == "strong"]
        if strong_coverages:
            actions.append(prepare_promotion_action(strong_coverages[0], brief_context))
    deduped: dict[str, dict[str, Any]] = {}
    for action in actions:
        key = "|".join(unique_texts([action.get("action_kind"), *(action.get("source_ids") or [])]))
        if key in deduped:
            continue
        deduped[key] = action
    ranked = list(deduped.values())
    for action in ranked:
        action["score"] = score_action(action)
    ranked.sort(key=lambda item: (-float(item.get("score") or 0.0), -priority_score(item.get("priority")), maybe_text(item.get("action_id"))))
    for index, action in enumerate(ranked, start=1):
        action["rank"] = index
    return ranked


def propose_next_actions_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_summary_path: str,
    board_brief_path: str,
    coverage_path: str,
    output_path: str,
    max_actions: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_summary_file = resolve_path(run_dir_path, board_summary_path, f"board/board_state_summary_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    coverage_file = resolve_path(run_dir_path, coverage_path, f"analytics/evidence_coverage_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"investigation/next_actions_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    board_summary = load_json_if_exists(board_summary_file)
    if not isinstance(board_summary, dict):
        warnings.append({"code": "missing-board-summary", "message": f"No board summary artifact was found at {board_summary_file}."})
        board_summary = {"active_hypotheses": [], "open_challenges": [], "open_tasks": [], "counts": {}}
    coverage_wrapper = load_json_if_exists(coverage_file)
    if not isinstance(coverage_wrapper, dict):
        warnings.append({"code": "missing-coverage", "message": f"No evidence coverage artifact was found at {coverage_file}."})
        coverage_wrapper = {"coverages": [], "coverage_count": 0}
    brief_text = load_text_if_exists(board_brief_file)
    ranked_actions = build_actions(board_summary, coverage_wrapper.get("coverages", []) if isinstance(coverage_wrapper.get("coverages"), list) else [], brief_text)[: max(1, max_actions)]

    wrapper = {
        "schema_version": "d1.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "board_summary_path": str(board_summary_file),
        "board_brief_path": str(board_brief_file),
        "coverage_path": str(coverage_file),
        "action_count": len(ranked_actions),
        "ranked_actions": ranked_actions,
    }
    write_json_file(output_file, wrapper)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$.ranked_actions", "artifact_ref": f"{output_file}:$.ranked_actions"}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "action_count": len(ranked_actions)},
        "receipt_id": "investigation-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "investigationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [maybe_text(action.get("action_id")) for action in ranked_actions if maybe_text(action.get("action_id"))],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [maybe_text(action.get("action_id")) for action in ranked_actions if maybe_text(action.get("action_id"))],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if ranked_actions else ["No next actions could be proposed from the current board and coverage artifacts."],
            "challenge_hints": ["Open a falsification probe for contradiction-heavy or low-confidence actions."] if any(bool(action.get("probe_candidate")) for action in ranked_actions) else [],
            "suggested_next_skills": ["eco-open-falsification-probe", "eco-summarize-round-readiness", "eco-post-board-note"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Propose a ranked next-action queue from board and evidence artifacts.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-summary-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-actions", type=int, default=6)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = propose_next_actions_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_summary_path=args.board_summary_path,
        board_brief_path=args.board_brief_path,
        coverage_path=args.coverage_path,
        output_path=args.output_path,
        max_actions=args.max_actions,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())