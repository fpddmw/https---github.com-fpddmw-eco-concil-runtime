#!/usr/bin/env python3
"""Build one auditable orchestration plan for the phase-2 controller."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-plan-round-orchestration"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import load_round_snapshot  # noqa: E402
from eco_council_runtime.kernel.investigation_planning import (  # noqa: E402
    load_d1_shared_context,
    load_falsification_probe_wrapper,
    load_next_actions_wrapper,
    load_round_readiness_wrapper,
)


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


def board_rollup(active_hypothesis_count: int, open_challenge_count: int, open_task_count: int, note_count: int) -> str:
    if active_hypothesis_count == 0 and open_challenge_count == 0 and open_task_count == 0 and note_count == 0:
        return "empty"
    if open_challenge_count > 0 and open_task_count == 0:
        return "needs-triage"
    if open_challenge_count > 0 or open_task_count > 0:
        return "in-flight"
    return "organized"


def round_board_state(round_state: dict[str, Any], *, state_source: str) -> dict[str, Any]:
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
    counts = {
        "notes_total": len(notes),
        "hypotheses_active": len(active_hypotheses),
        "hypotheses_low_confidence": len(low_confidence_hypotheses),
        "challenge_open": len(open_challenges),
        "tasks_open": len(open_tasks),
    }
    return {
        "counts": counts,
        "state_source": state_source,
        "status_rollup": board_rollup(
            counts["hypotheses_active"],
            counts["challenge_open"],
            counts["tasks_open"],
            counts["notes_total"],
        ),
        "active_hypotheses": active_hypotheses,
        "open_challenges": open_challenges,
        "open_tasks": open_tasks,
    }


def board_snapshot(round_state: dict[str, Any] | None, board_summary: dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(round_state, dict):
        return round_board_state(round_state, state_source="deliberation-plane")
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
            "state_source": "board-summary-artifact",
            "status_rollup": maybe_text(board_summary.get("status_rollup")) or "",
            "active_hypotheses": board_summary.get("active_hypotheses", []) if isinstance(board_summary.get("active_hypotheses"), list) else [],
            "open_challenges": board_summary.get("open_challenges", []) if isinstance(board_summary.get("open_challenges"), list) else [],
            "open_tasks": board_summary.get("open_tasks", []) if isinstance(board_summary.get("open_tasks"), list) else [],
        }
    return {
        "counts": {
            "notes_total": 0,
            "hypotheses_active": 0,
            "hypotheses_low_confidence": 0,
            "challenge_open": 0,
            "tasks_open": 0,
        },
        "state_source": "missing-board",
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


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def summarize_counts(items: list[dict[str, Any]], *, field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        value = maybe_text(item.get(field_name))
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def normalized_counts(payload: dict[str, Any], field_name: str) -> dict[str, int]:
    raw_counts = payload.get(field_name)
    if not isinstance(raw_counts, dict):
        return {}
    counts: dict[str, int] = {}
    for key, value in raw_counts.items():
        normalized_key = maybe_text(key)
        if not normalized_key:
            continue
        counts[normalized_key] = safe_int(value)
    return counts


def count_open_probes(probes: dict[str, Any]) -> int:
    if not isinstance(probes.get("probes"), list):
        return 0
    return len(
        [
            item
            for item in probes.get("probes", [])
            if isinstance(item, dict)
            and maybe_text(item.get("probe_status")) not in {"closed", "cancelled"}
        ]
    )


def planning_signal_counts(
    snapshot: dict[str, Any],
    next_actions: dict[str, Any],
    probes: dict[str, Any],
    readiness: dict[str, Any],
    shared_context: dict[str, Any],
) -> dict[str, Any]:
    agenda_counts = normalized_counts(next_actions, "agenda_counts")
    if not agenda_counts:
        agenda_counts = normalized_counts(readiness, "agenda_counts")
    if not agenda_counts:
        agenda_counts = normalized_counts(shared_context, "agenda_counts")

    controversy_gap_counts = summarize_counts(
        next_actions.get("ranked_actions", [])
        if isinstance(next_actions.get("ranked_actions"), list)
        else [],
        field_name="controversy_gap",
    )
    if not controversy_gap_counts:
        controversy_gap_counts = normalized_counts(
            next_actions,
            "controversy_gap_counts",
        )
    if not controversy_gap_counts:
        controversy_gap_counts = normalized_counts(
            readiness,
            "controversy_gap_counts",
        )

    probe_type_counts = summarize_counts(
        probes.get("probes", [])
        if isinstance(probes.get("probes"), list)
        else [],
        field_name="probe_type",
    )
    if not probe_type_counts:
        probe_type_counts = normalized_counts(readiness, "probe_type_counts")

    ranked_actions = (
        next_actions.get("ranked_actions", [])
        if isinstance(next_actions.get("ranked_actions"), list)
        else []
    )
    non_promotion_actions = [
        item
        for item in ranked_actions
        if isinstance(item, dict)
        and maybe_text(item.get("action_kind")) != "prepare-promotion"
    ]
    probe_candidate_actions = len(
        [
            item
            for item in non_promotion_actions
            if bool(item.get("probe_candidate"))
        ]
    )
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    routing_actions = max(
        safe_int(controversy_gap_counts.get("verification-routing-gap")),
        safe_int(agenda_counts.get("routing_issue_count")),
    )
    empirical_gap_actions = max(
        safe_int(controversy_gap_counts.get("verification-gap"))
        + safe_int(controversy_gap_counts.get("formal-public-misalignment")),
        safe_int(agenda_counts.get("empirical_issue_gap_count")),
    )
    representation_gap_actions = max(
        safe_int(controversy_gap_counts.get("representation-gap")),
        safe_int(agenda_counts.get("representation_gap_count")),
    )
    formal_linkage_actions = max(
        safe_int(controversy_gap_counts.get("formal-record-gap"))
        + safe_int(controversy_gap_counts.get("formal-public-linkage-gap"))
        + safe_int(controversy_gap_counts.get("public-discourse-gap"))
        + safe_int(controversy_gap_counts.get("stakeholder-deliberation-gap")),
        safe_int(agenda_counts.get("formal_public_linkage_gap_count")),
    )
    diffusion_focus_count = max(
        safe_int(controversy_gap_counts.get("cross-platform-diffusion")),
        safe_int(agenda_counts.get("diffusion_focus_count")),
    )
    return {
        "agenda_counts": agenda_counts,
        "controversy_gap_counts": controversy_gap_counts,
        "probe_type_counts": probe_type_counts,
        "open_probe_count": count_open_probes(probes),
        "probe_candidate_actions": probe_candidate_actions,
        "pending_non_promotion_actions": len(non_promotion_actions),
        "issue_cluster_count": safe_int(agenda_counts.get("issue_cluster_count")),
        "routing_actions": routing_actions,
        "empirical_gap_actions": empirical_gap_actions,
        "representation_gap_actions": representation_gap_actions,
        "formal_linkage_actions": formal_linkage_actions,
        "diffusion_focus_count": diffusion_focus_count,
        "board_open_challenges": safe_int(counts.get("challenge_open")),
        "board_open_tasks": safe_int(counts.get("tasks_open")),
        "board_low_confidence_hypotheses": safe_int(
            counts.get("hypotheses_low_confidence")
        ),
        "board_active_hypotheses": safe_int(counts.get("hypotheses_active")),
        "readiness_status": maybe_text(readiness.get("readiness_status")),
    }


def probe_stage_decision(
    snapshot: dict[str, Any],
    next_actions: dict[str, Any],
    probes: dict[str, Any],
    readiness: dict[str, Any],
    shared_context: dict[str, Any],
) -> tuple[bool, list[str], dict[str, Any]]:
    signal_counts = planning_signal_counts(
        snapshot,
        next_actions,
        probes,
        readiness,
        shared_context,
    )
    reason_codes: list[str] = []
    if signal_counts["open_probe_count"] > 0:
        reason_codes.append("open-probes")
    if signal_counts["probe_candidate_actions"] > 0:
        reason_codes.append("probe-candidate-actions")
    if signal_counts["routing_actions"] > 0:
        reason_codes.append("agenda-routing-blockers")
    if signal_counts["empirical_gap_actions"] > 0:
        reason_codes.append("agenda-empirical-gaps")
    if signal_counts["representation_gap_actions"] > 0:
        reason_codes.append("agenda-representation-gaps")
    if signal_counts["formal_linkage_actions"] > 0:
        reason_codes.append("agenda-formal-public-linkage-gaps")
    if signal_counts["diffusion_focus_count"] > 0:
        reason_codes.append("agenda-diffusion-focus")

    agenda_signal_available = (
        any(
            safe_int(value) > 0
            for value in signal_counts["agenda_counts"].values()
        )
        or signal_counts["pending_non_promotion_actions"] > 0
        or signal_counts["open_probe_count"] > 0
        or bool(signal_counts["readiness_status"])
    )
    if not reason_codes and not agenda_signal_available:
        if signal_counts["board_open_challenges"] > 0:
            reason_codes.append("board-open-challenges-fallback")
        if signal_counts["board_low_confidence_hypotheses"] > 0:
            reason_codes.append("board-low-confidence-fallback")

    include_probe = (
        signal_counts["readiness_status"] != "ready" and bool(reason_codes)
    )
    return include_probe, reason_codes, signal_counts


def downstream_posture(
    signal_counts: dict[str, Any],
    probe_reason_codes: list[str],
) -> tuple[str, list[str]]:
    readiness_status = maybe_text(signal_counts.get("readiness_status"))
    if readiness_status == "ready":
        return "promote-candidate", ["ready-readiness-artifact"]
    if readiness_status in {"blocked", "needs-more-data"}:
        return "hold-investigation-open", [
            f"readiness-{readiness_status}",
        ]

    reason_codes: list[str] = []
    if signal_counts.get("open_probe_count", 0) > 0:
        reason_codes.append("open-probes")
    if signal_counts.get("pending_non_promotion_actions", 0) > 0:
        reason_codes.append("pending-investigation-actions")
    if probe_reason_codes:
        reason_codes.append("probe-stage-retained")
    if not reason_codes:
        if signal_counts.get("board_open_challenges", 0) > 0:
            reason_codes.append("board-open-challenges-fallback")
        if signal_counts.get("board_open_tasks", 0) > 0:
            reason_codes.append("board-open-tasks-fallback")
        if signal_counts.get("board_active_hypotheses", 0) == 0 and signal_counts.get(
            "issue_cluster_count",
            0,
        ) == 0:
            reason_codes.append("no-active-issue-to-promote")
    if reason_codes:
        return "hold-investigation-open", reason_codes
    return "promote-candidate", ["no-visible-blockers"]


def step_entry(stage_name: str, skill_name: str, reason: str, assigned_role_hint: str, expected_output_path: Path) -> dict[str, Any]:
    return {
        "stage_name": stage_name,
        "skill_name": skill_name,
        "skill_args": [],
        "assigned_role_hint": assigned_role_hint,
        "reason": reason,
        "expected_output_path": str(expected_output_path),
    }


def derived_export_entry(stage_name: str, skill_name: str, reason: str, expected_output_path: Path) -> dict[str, Any]:
    return {
        "stage_name": stage_name,
        "skill_name": skill_name,
        "assigned_role_hint": "moderator",
        "reason": reason,
        "expected_output_path": str(expected_output_path),
        "required_for_controller": False,
        "export_mode": "derived-only",
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
                "trigger": "Agenda artifacts still show unresolved routing, verification, representation, linkage, diffusion, or open probe pressure.",
                "effect": "Keep falsification-probe materialization in the controller queue before readiness review.",
            },
        )
    return rows


def fallback_path(
    snapshot: dict[str, Any],
    next_actions: dict[str, Any],
    posture: str,
    probe_reason_codes: list[str],
    signal_counts: dict[str, Any],
) -> list[dict[str, Any]]:
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
    if probe_reason_codes:
        suggested_next_skills = ["eco-open-falsification-probe", "eco-post-board-note"]
        if signal_counts.get("routing_actions", 0) > 0:
            suggested_next_skills.insert(0, "eco-route-verification-lane")
        if signal_counts.get("representation_gap_actions", 0) > 0 or signal_counts.get(
            "formal_linkage_actions",
            0,
        ) > 0:
            suggested_next_skills.extend(
                [
                    "eco-link-formal-comments-to-public-discourse",
                    "eco-identify-representation-gaps",
                ]
            )
        if signal_counts.get("diffusion_focus_count", 0) > 0:
            suggested_next_skills.append("eco-detect-cross-platform-diffusion")
        fallback_rows.append(
            {
                "when": "Controversy agenda still carries unresolved probe-worthy work.",
                "reason": "Planner kept probe materialization because the agenda still shows unresolved routing, verification, representation, linkage, or diffusion pressure.",
                "suggested_next_skills": unique_texts(suggested_next_skills),
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
    round_snapshot = load_round_snapshot(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        board_path=board_file,
        include_closed=True,
    )
    board = load_json_if_exists(board_file)
    if not isinstance(board, dict):
        warnings.append({"code": "missing-board", "message": f"No board artifact was found at {board_file}."})
    board_summary = load_json_if_exists(board_summary_file)
    shared_context = load_d1_shared_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        board_summary_path=board_summary_path,
        board_brief_path=board_brief_path,
    )
    next_actions_context = load_next_actions_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        next_actions_path=next_actions_path,
    )
    next_actions = (
        next_actions_context.get("payload")
        if isinstance(next_actions_context.get("payload"), dict)
        else {}
    )
    readiness_context = load_round_readiness_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        readiness_path=readiness_path,
    )
    readiness = (
        readiness_context.get("payload")
        if isinstance(readiness_context.get("payload"), dict)
        else {}
    )
    brief_text = load_text_if_exists(board_brief_file)
    probes_context = load_falsification_probe_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        probes_path=probes_path,
    )
    probes = (
        probes_context.get("payload")
        if isinstance(probes_context.get("payload"), dict)
        else {}
    )
    deliberation_sync = (
        round_snapshot.get("deliberation_sync")
        if isinstance(round_snapshot.get("deliberation_sync"), dict)
        else {}
    )
    round_state = (
        round_snapshot.get("round_state")
        if maybe_text(round_snapshot.get("status")) == "completed"
        and isinstance(round_snapshot.get("round_state"), dict)
        else None
    )
    db_path = maybe_text(round_snapshot.get("db_path"))

    snapshot = board_snapshot(round_state, board_summary if isinstance(board_summary, dict) else None)
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    probe_stage_included, probe_reason_codes, signal_counts = probe_stage_decision(
        snapshot,
        next_actions,
        probes,
        readiness,
        shared_context,
    )
    posture, posture_reason_codes = downstream_posture(
        signal_counts,
        probe_reason_codes,
    )
    top_action_rows = top_actions(next_actions)
    primary_action_role = top_action_rows[0]["assigned_role"] if top_action_rows else "moderator"

    execution_queue = [
        step_entry(
            "next-actions",
            "eco-propose-next-actions",
            "Re-rank investigation actions directly from shared board state and controversy agenda context.",
            primary_action_role or "moderator",
            next_actions_file,
        ),
    ]
    if probe_stage_included:
        execution_queue.append(
            step_entry(
                "falsification-probes",
                "eco-open-falsification-probe",
                "Open or refresh probe objects because the current round still carries unresolved agenda-level controversy pressure.",
                "challenger",
                probes_file,
            )
        )
    execution_queue.append(
        step_entry(
            "round-readiness",
            "eco-summarize-round-readiness",
            "Re-evaluate round readiness from shared board state, action, and probe artifacts.",
            "moderator",
            readiness_file,
        )
    )

    derived_exports = [
        derived_export_entry(
            "board-summary",
            "eco-summarize-board-state",
            "Materialize a structured board snapshot only when operators need an explicit export artifact.",
            board_summary_file,
        ),
        derived_export_entry(
            "board-brief",
            "eco-materialize-board-brief",
            "Materialize a compact human-readable board brief only when handoff or archival text is needed.",
            board_brief_file,
        ),
    ]

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
    phase_decision_basis = {
        "probe_stage_reason_codes": probe_reason_codes,
        "posture_reason_codes": posture_reason_codes,
        "agenda_counts": signal_counts.get("agenda_counts", {}),
        "controversy_gap_counts": signal_counts.get("controversy_gap_counts", {}),
        "probe_type_counts": signal_counts.get("probe_type_counts", {}),
        "signal_counts": {
            "open_probe_count": signal_counts.get("open_probe_count", 0),
            "probe_candidate_actions": signal_counts.get("probe_candidate_actions", 0),
            "pending_non_promotion_actions": signal_counts.get("pending_non_promotion_actions", 0),
            "issue_cluster_count": signal_counts.get("issue_cluster_count", 0),
            "routing_actions": signal_counts.get("routing_actions", 0),
            "empirical_gap_actions": signal_counts.get("empirical_gap_actions", 0),
            "representation_gap_actions": signal_counts.get("representation_gap_actions", 0),
            "formal_linkage_actions": signal_counts.get("formal_linkage_actions", 0),
            "diffusion_focus_count": signal_counts.get("diffusion_focus_count", 0),
            "board_open_challenges": signal_counts.get("board_open_challenges", 0),
            "board_open_tasks": signal_counts.get("board_open_tasks", 0),
            "board_low_confidence_hypotheses": signal_counts.get("board_low_confidence_hypotheses", 0),
        },
    }
    fallback_rows = fallback_path(
        snapshot,
        next_actions,
        posture,
        probe_reason_codes,
        signal_counts,
    )
    plan_id = "orchestration-plan-" + stable_hash(run_id, round_id, posture, *(step["skill_name"] for step in execution_queue))[:12]
    planning_notes = [
        "Planner artifact exists to make the phase-2 controller queue explicit and auditable.",
        "Probe materialization is decided from agenda artifacts first; direct board heuristics only remain as a compatibility fallback when those artifacts are absent.",
        "Board summary and board brief are treated as derived exports rather than controller prerequisites.",
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
        "phase_decision_basis": phase_decision_basis,
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
            "board_exports_are_derived": True,
            "next_actions_present": isinstance(next_actions, dict) and bool(next_actions),
            "probes_present": isinstance(probes, dict) and bool(probes),
            "next_actions_source": maybe_text(next_actions_context.get("source")),
            "probes_source": maybe_text(probes_context.get("source")),
            "readiness_present": bool(readiness_context.get("payload_present")),
            "readiness_source": maybe_text(readiness_context.get("source")),
            "board_state_source": maybe_text(snapshot.get("state_source")),
            "board_state_db_path": db_path,
            "status_rollup": maybe_text(snapshot.get("status_rollup")),
            "readiness_status": maybe_text(readiness.get("readiness_status")),
            "counts": counts,
            "top_actions": top_action_rows,
            "agenda_counts": phase_decision_basis["agenda_counts"],
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
        "derived_exports": derived_exports,
        "post_gate_steps": post_gate_steps,
        "stop_conditions": stop_conditions(probe_stage_included),
        "fallback_path": fallback_rows,
        "planning_notes": planning_notes[:4],
        "deliberation_sync": deliberation_sync,
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
            "derived_export_count": len(derived_exports),
            "planning_mode": plan_payload["planning_mode"],
            "probe_stage_included": probe_stage_included,
            "downstream_posture": posture,
            "board_state_source": maybe_text(snapshot.get("state_source")),
            "db_path": db_path,
        },
        "receipt_id": "runtime-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, plan_id)[:20],
        "batch_id": "runtimebatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [plan_id],
        "warnings": warnings,
        "deliberation_sync": deliberation_sync,
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
