#!/usr/bin/env python3
"""Build one auditable orchestration plan for the phase-2 controller."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-plan-round-orchestration"


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


def round_board_state(board: dict[str, Any], round_id: str) -> dict[str, Any]:
    rounds = board.get("rounds", {}) if isinstance(board.get("rounds"), dict) else {}
    round_state = rounds.get(round_id, {}) if isinstance(rounds.get(round_id), dict) else {}
    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    hypotheses = [item for item in round_state.get("hypotheses", []) if isinstance(item, dict)] if isinstance(round_state.get("hypotheses"), list) else []
    challenges = [item for item in round_state.get("challenge_tickets", []) if isinstance(item, dict)] if isinstance(round_state.get("challenge_tickets"), list) else []
    tasks = [item for item in round_state.get("tasks", []) if isinstance(item, dict)] if isinstance(round_state.get("tasks"), list) else []
    active_hypotheses = [item for item in hypotheses if maybe_text(item.get("status")) not in {"closed", "rejected"}]
    open_challenges = [item for item in challenges if isinstance(item, dict) and maybe_text(item.get("status")) != "closed"]
    open_tasks = [item for item in tasks if maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}]
    low_confidence_hypotheses = [
        item for item in active_hypotheses if (maybe_number(item.get("confidence")) or 0.0) < 0.6
    ]
    return {
        "counts": {
            "notes_total": len(notes),
            "hypotheses_active": len(active_hypotheses),
            "hypotheses_low_confidence": len(low_confidence_hypotheses),
            "challenge_open": len(open_challenges),
            "tasks_open": len(open_tasks),
        },
        "active_hypotheses": active_hypotheses,
        "open_challenges": open_challenges,
        "open_tasks": open_tasks,
    }


def board_snapshot(board: dict[str, Any] | None, board_summary: dict[str, Any] | None, round_id: str) -> dict[str, Any]:
    if isinstance(board_summary, dict):
        counts = board_summary.get("counts", {}) if isinstance(board_summary.get("counts"), dict) else {}
        return {
            "counts": {
                "notes_total": int(counts.get("notes_total") or 0),
                "hypotheses_active": int(counts.get("hypotheses_active") or len(board_summary.get("active_hypotheses", []))),
                "hypotheses_low_confidence": int(counts.get("hypotheses_low_confidence") or 0),
                "challenge_open": int(counts.get("challenge_open") or len(board_summary.get("open_challenges", []))),
                "tasks_open": int(counts.get("tasks_open") or len(board_summary.get("open_tasks", []))),
            },
            "status_rollup": maybe_text(board_summary.get("status_rollup")) or "",
            "active_hypotheses": board_summary.get("active_hypotheses", []) if isinstance(board_summary.get("active_hypotheses"), list) else [],
            "open_challenges": board_summary.get("open_challenges", []) if isinstance(board_summary.get("open_challenges"), list) else [],
            "open_tasks": board_summary.get("open_tasks", []) if isinstance(board_summary.get("open_tasks"), list) else [],
        }
    if isinstance(board, dict):
        derived = round_board_state(board, round_id)
        return {**derived, "status_rollup": "derived-from-board"}
    return {
        "counts": {
            "notes_total": 0,
            "hypotheses_active": 0,
            "hypotheses_low_confidence": 0,
            "challenge_open": 0,
            "tasks_open": 0,
        },
        "status_rollup": "missing-board",
        "active_hypotheses": [],
        "open_challenges": [],
        "open_tasks": [],
    }


def top_actions(next_actions: dict[str, Any]) -> list[dict[str, str]]:
    ranked_actions = next_actions.get("ranked_actions", []) if isinstance(next_actions.get("ranked_actions"), list) else []
    rows: list[dict[str, str]] = []
    for action in ranked_actions[:3]:
        if not isinstance(action, dict):
            continue
        rows.append(
            {
                "action_id": maybe_text(action.get("action_id")),
                "action_kind": maybe_text(action.get("action_kind")),
                "assigned_role": maybe_text(action.get("assigned_role")),
                "priority": maybe_text(action.get("priority")),
                "objective": maybe_text(action.get("objective")),
            }
        )
    return rows


def include_probe_stage(snapshot: dict[str, Any], next_actions: dict[str, Any], readiness: dict[str, Any]) -> bool:
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    if int(counts.get("challenge_open") or 0) > 0:
        return True
    if int(counts.get("hypotheses_low_confidence") or 0) > 0:
        return True
    readiness_status = maybe_text(readiness.get("readiness_status"))
    if readiness_status in {"blocked", "needs-more-data"}:
        return True
    ranked_actions = next_actions.get("ranked_actions", []) if isinstance(next_actions.get("ranked_actions"), list) else []
    for action in ranked_actions[:5]:
        if not isinstance(action, dict):
            continue
        if bool(action.get("probe_candidate")):
            return True
        if maybe_text(action.get("action_kind")) in {"resolve-challenge", "resolve-contradiction", "stabilize-hypothesis"}:
            return True
    return False


def downstream_posture(snapshot: dict[str, Any], readiness: dict[str, Any], include_probe: bool) -> str:
    readiness_status = maybe_text(readiness.get("readiness_status"))
    if readiness_status == "ready":
        return "promote-candidate"
    if readiness_status in {"blocked", "needs-more-data"}:
        return "hold-investigation-open"
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    if include_probe:
        return "hold-investigation-open"
    if int(counts.get("challenge_open") or 0) > 0:
        return "hold-investigation-open"
    if int(counts.get("tasks_open") or 0) > 0:
        return "hold-investigation-open"
    if int(counts.get("hypotheses_active") or 0) == 0:
        return "hold-investigation-open"
    return "promote-candidate"


def step_entry(stage_name: str, skill_name: str, reason: str, assigned_role_hint: str, expected_output_path: Path) -> dict[str, Any]:
    return {
        "stage_name": stage_name,
        "skill_name": skill_name,
        "skill_args": [],
        "assigned_role_hint": assigned_role_hint,
        "reason": reason,
        "expected_output_path": str(expected_output_path),
    }


def stop_conditions(include_probe: bool) -> list[dict[str, str]]:
    rows = [
        {
            "condition_id": "planned-skill-failure",
            "trigger": "Any planned skill returns blocked or failed.",
            "effect": "Abort controller execution and surface the failing stage to runtime callers.",
        },
        {
            "condition_id": "gate-allows-promotion",
            "trigger": "Promotion gate returns allow-promote after round-readiness.",
            "effect": "Run eco-promote-evidence-basis and hand off the round to downstream reporting.",
        },
        {
            "condition_id": "gate-withholds-promotion",
            "trigger": "Promotion gate returns freeze-withheld after round-readiness.",
            "effect": "Run eco-promote-evidence-basis in withheld mode and keep investigation open.",
        },
    ]
    if include_probe:
        rows.insert(
            1,
            {
                "condition_id": "probe-stage-required",
                "trigger": "Board state still contains open challenges or low-confidence hypotheses.",
                "effect": "Keep falsification-probe materialization in the controller queue before readiness review.",
            },
        )
    return rows


def fallback_path(snapshot: dict[str, Any], next_actions: dict[str, Any], posture: str, include_probe: bool) -> list[dict[str, Any]]:
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    fallback_rows: list[dict[str, Any]] = []
    if int(counts.get("challenge_open") or 0) > 0:
        fallback_rows.append(
            {
                "when": "Open challenge tickets remain after controller execution.",
                "reason": "Challenge objects still need explicit closure or a stronger board note.",
                "suggested_next_skills": ["eco-post-board-note", "eco-close-challenge-ticket", "eco-propose-next-actions"],
            }
        )
    if include_probe or int(counts.get("hypotheses_low_confidence") or 0) > 0:
        fallback_rows.append(
            {
                "when": "Low-confidence hypotheses or contradiction-leaning actions persist.",
                "reason": "The round still needs explicit falsification work before a clean promotion path exists.",
                "suggested_next_skills": ["eco-open-falsification-probe", "eco-post-board-note", "eco-update-hypothesis-status"],
            }
        )
    if posture == "promote-candidate":
        fallback_rows.append(
            {
                "when": "Promotion succeeds and the basis is frozen.",
                "reason": "The next system boundary is reporting handoff rather than more investigation work.",
                "suggested_next_skills": ["eco-materialize-reporting-handoff", "eco-draft-council-decision"],
            }
        )
    else:
        top_rows = top_actions(next_actions)
        fallback_rows.append(
            {
                "when": "Gate freezes the round after readiness review.",
                "reason": top_rows[0]["objective"] if top_rows else "The board still carries unresolved investigation work.",
                "suggested_next_skills": ["eco-propose-next-actions", "eco-open-falsification-probe", "eco-post-board-note"],
            }
        )
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in fallback_rows:
        key = "|".join(unique_texts([row.get("when"), *(row.get("suggested_next_skills") or [])]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped[:4]


def plan_round_orchestration_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    board_summary_path: str,
    board_brief_path: str,
    next_actions_path: str,
    probes_path: str,
    readiness_path: str,
    output_path: str,
    planner_mode: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_path(run_dir_path, board_path, "board/investigation_board.json")
    board_summary_file = resolve_path(run_dir_path, board_summary_path, f"board/board_state_summary_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    next_actions_file = resolve_path(run_dir_path, next_actions_path, f"investigation/next_actions_{round_id}.json")
    probes_file = resolve_path(run_dir_path, probes_path, f"investigation/falsification_probes_{round_id}.json")
    readiness_file = resolve_path(run_dir_path, readiness_path, f"reporting/round_readiness_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"runtime/orchestration_plan_{round_id}.json")
    promotion_basis_file = (run_dir_path / "promotion" / f"promoted_evidence_basis_{round_id}.json").resolve()

    warnings: list[dict[str, Any]] = []
    board = load_json_if_exists(board_file)
    if not isinstance(board, dict):
        warnings.append({"code": "missing-board", "message": f"No board artifact was found at {board_file}."})
    board_summary = load_json_if_exists(board_summary_file)
    next_actions = load_json_if_exists(next_actions_file) or {}
    readiness = load_json_if_exists(readiness_file) or {}
    brief_text = load_text_if_exists(board_brief_file)
    probes = load_json_if_exists(probes_file) or {}

    snapshot = board_snapshot(board if isinstance(board, dict) else None, board_summary if isinstance(board_summary, dict) else None, round_id)
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    probe_stage_included = include_probe_stage(snapshot, next_actions, readiness)
    posture = downstream_posture(snapshot, readiness, probe_stage_included)
    top_action_rows = top_actions(next_actions)
    primary_action_role = top_action_rows[0]["assigned_role"] if top_action_rows else "moderator"

    execution_queue = [
        step_entry(
            "board-summary",
            "eco-summarize-board-state",
            "Refresh the board summary from the current investigation board before planning any downstream work.",
            "moderator",
            board_summary_file,
        ),
        step_entry(
            "board-brief",
            "eco-materialize-board-brief",
            "Refresh the board brief so downstream planning uses a compact textual snapshot.",
            "moderator",
            board_brief_file,
        ),
        step_entry(
            "next-actions",
            "eco-propose-next-actions",
            "Re-rank investigation actions from refreshed board and coverage context.",
            primary_action_role or "moderator",
            next_actions_file,
        ),
    ]
    if probe_stage_included:
        execution_queue.append(
            step_entry(
                "falsification-probes",
                "eco-open-falsification-probe",
                "Open or refresh probe objects because the current round still carries contradiction or low-confidence signals.",
                "challenger",
                probes_file,
            )
        )
    execution_queue.append(
        step_entry(
            "round-readiness",
            "eco-summarize-round-readiness",
            "Re-evaluate round readiness from refreshed board, action, and probe artifacts.",
            "moderator",
            readiness_file,
        )
    )

    post_gate_steps = [
        step_entry(
            "promotion-basis",
            "eco-promote-evidence-basis",
            "Freeze a promoted or withheld evidence basis after gate evaluation so controller output always stays auditable.",
            "moderator",
            promotion_basis_file,
        )
    ]

    role_hints = unique_texts(["moderator", primary_action_role, "challenger" if probe_stage_included else ""])
    fallback_rows = fallback_path(snapshot, next_actions, posture, probe_stage_included)
    plan_id = "orchestration-plan-" + stable_hash(run_id, round_id, posture, *(step["skill_name"] for step in execution_queue))[:12]
    planning_notes = [
        "Planner artifact exists to make the phase-2 controller queue explicit and auditable.",
        "Probe materialization is only kept in the queue when the current board state still shows contradiction or low-confidence pressure.",
    ]
    if brief_text:
        planning_notes.append(f"Board brief context: {maybe_text(brief_text)[:180]}")
    plan_payload = {
        "schema_version": "runtime-orchestration-plan-v1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "plan_id": plan_id,
        "planning_status": "advisory-plan-ready" if planner_mode == "agent-advisory" else "ready-for-controller",
        "planning_mode": "agent-advisory" if planner_mode == "agent-advisory" else "planner-backed-phase2",
        "controller_authority": "advisory-only" if planner_mode == "agent-advisory" else "queue-owner",
        "probe_stage_included": probe_stage_included,
        "downstream_posture": posture,
        "assigned_role_hints": role_hints,
        "agent_turn_hints": {
            "primary_role": primary_action_role or "moderator",
            "support_roles": unique_texts(["moderator", primary_action_role, "challenger" if probe_stage_included else ""]),
            "recommended_skill_sequence": [step["skill_name"] for step in execution_queue],
        },
        "observed_state": {
            "board_present": isinstance(board, dict),
            "board_summary_present": isinstance(board_summary, dict),
            "board_brief_present": bool(brief_text),
            "next_actions_present": isinstance(next_actions, dict) and bool(next_actions),
            "probes_present": isinstance(probes, dict) and bool(probes),
            "readiness_present": isinstance(readiness, dict) and bool(readiness),
            "status_rollup": maybe_text(snapshot.get("status_rollup")),
            "readiness_status": maybe_text(readiness.get("readiness_status")),
            "counts": counts,
            "top_actions": top_action_rows,
        },
        "inputs": {
            "board_path": str(board_file),
            "board_summary_path": str(board_summary_file),
            "board_brief_path": str(board_brief_file),
            "next_actions_path": str(next_actions_file),
            "probes_path": str(probes_file),
            "readiness_path": str(readiness_file),
        },
        "execution_queue": execution_queue,
        "post_gate_steps": post_gate_steps,
        "stop_conditions": stop_conditions(probe_stage_included),
        "fallback_path": fallback_rows,
        "planning_notes": planning_notes[:3],
    }
    write_json_file(output_file, plan_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    suggested_next_skills = unique_texts(
        [step["skill_name"] for step in execution_queue]
        + [step["skill_name"] for step in post_gate_steps]
        + [skill_name for row in fallback_rows for skill_name in row.get("suggested_next_skills", [])]
    )
    gap_hints: list[str] = []
    if not isinstance(board, dict):
        gap_hints.append("No board artifact exists yet, so planner output assumes controller will rebuild board-facing artifacts from scratch.")
    if posture != "promote-candidate":
        gap_hints.append("Current board posture still points to investigation hold rather than clean promotion.")
    challenge_hints: list[str] = []
    if int(counts.get("challenge_open") or 0) > 0:
        challenge_hints.append(f"{int(counts.get('challenge_open') or 0)} open challenge tickets are still visible to the planner.")
    if probe_stage_included:
        challenge_hints.append("Planner kept falsification-probe materialization in the queue for this round.")
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "plan_id": plan_id,
            "planned_skill_count": len(execution_queue) + len(post_gate_steps),
            "planning_mode": plan_payload["planning_mode"],
            "probe_stage_included": probe_stage_included,
            "downstream_posture": posture,
        },
        "receipt_id": "runtime-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, plan_id)[:20],
        "batch_id": "runtimebatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [plan_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [plan_id],
            "evidence_refs": artifact_refs,
            "gap_hints": gap_hints,
            "challenge_hints": challenge_hints,
            "suggested_next_skills": suggested_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build one auditable orchestration plan for the phase-2 controller.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--board-summary-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--probes-path", default="")
    parser.add_argument("--readiness-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--planner-mode", choices=["runtime-phase2", "agent-advisory"], default="runtime-phase2")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = plan_round_orchestration_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        board_summary_path=args.board_summary_path,
        board_brief_path=args.board_brief_path,
        next_actions_path=args.next_actions_path,
        probes_path=args.probes_path,
        readiness_path=args.readiness_path,
        output_path=args.output_path,
        planner_mode=args.planner_mode,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())