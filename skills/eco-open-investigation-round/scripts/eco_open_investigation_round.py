#!/usr/bin/env python3
"""Open a follow-up investigation round while preserving prior round state."""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-open-investigation-round"
SOURCE_SELECTION_ROLES = ("sociologist", "environmentalist")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    sync_board_to_deliberation_plane,
)
from eco_council_runtime.kernel.source_queue_contract import source_role  # noqa: E402


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


def resolve_board_path(run_dir: Path, board_path: str) -> Path:
    return resolve_path(run_dir, board_path, "board/investigation_board.json")


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_or_init_board(path: Path, run_id: str) -> dict[str, Any]:
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    return {"schema_version": "board-v1", "run_id": run_id, "board_revision": 0, "updated_at_utc": utc_now_iso(), "events": [], "rounds": {}}


def board_revision(board: dict[str, Any]) -> int:
    try:
        return max(0, int(board.get("board_revision") or 0))
    except (TypeError, ValueError):
        return 0


def ensure_round(board: dict[str, Any], round_id: str) -> dict[str, Any]:
    rounds = board.setdefault("rounds", {})
    if not isinstance(rounds, dict):
        rounds = {}
        board["rounds"] = rounds
    round_state = rounds.setdefault(round_id, {"notes": [], "challenge_tickets": [], "hypotheses": [], "tasks": []})
    if not isinstance(round_state, dict):
        round_state = {"notes": [], "challenge_tickets": [], "hypotheses": [], "tasks": []}
        rounds[round_id] = round_state
    round_state.setdefault("notes", [])
    round_state.setdefault("challenge_tickets", [])
    round_state.setdefault("hypotheses", [])
    round_state.setdefault("tasks", [])
    return round_state


def append_event(board: dict[str, Any], run_id: str, round_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    events = board.setdefault("events", [])
    if not isinstance(events, list):
        events = []
        board["events"] = events
    timestamp = utc_now_iso()
    event_id = "boardevt-" + stable_hash(run_id, round_id, event_type, len(events), timestamp, payload.get("source_round_id", ""))[:12]
    event = {"event_id": event_id, "run_id": run_id, "round_id": round_id, "event_type": event_type, "created_at_utc": timestamp, "payload": payload}
    events.append(event)
    board["updated_at_utc"] = timestamp
    return event


def write_board(path: Path, board: dict[str, Any], next_revision: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    board["board_revision"] = next_revision
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{next_revision}.tmp")
    temp_path.write_text(json.dumps(board, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp_path, path)


@contextmanager
def locked_board(path: Path, run_id: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield load_or_init_board(path, run_id)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def task_is_open(task: dict[str, Any]) -> bool:
    return maybe_text(task.get("status")) not in {"completed", "closed", "cancelled"}


def challenge_is_open(ticket: dict[str, Any]) -> bool:
    return maybe_text(ticket.get("status")) != "closed"


def hypothesis_is_active(hypothesis: dict[str, Any]) -> bool:
    return maybe_text(hypothesis.get("status")) not in {"closed", "rejected"}


def role_source_skills_from_mission(mission: dict[str, Any], role: str) -> list[str]:
    imports = mission.get("artifact_imports") if isinstance(mission.get("artifact_imports"), list) else []
    requests = mission.get("source_requests") if isinstance(mission.get("source_requests"), list) else []
    values: list[str] = []
    for item in [*imports, *requests]:
        if not isinstance(item, dict):
            continue
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        inferred_role = maybe_text(item.get("role")) or source_role(source_skill)
        if inferred_role == role:
            values.append(source_skill)
    return unique_texts(values)


def clone_hypothesis(source: dict[str, Any], *, run_id: str, round_id: str, source_round_id: str, timestamp: str) -> dict[str, Any]:
    title = maybe_text(source.get("title"))
    statement = maybe_text(source.get("statement"))
    source_hypothesis_id = maybe_text(source.get("hypothesis_id"))
    confidence = maybe_number(source.get("confidence"))
    status = maybe_text(source.get("status")) or "active"
    cloned_id = "hypothesis-" + stable_hash(run_id, round_id, "carry-hypothesis", source_hypothesis_id or title or statement)[:12]
    return {
        "hypothesis_id": cloned_id,
        "run_id": run_id,
        "round_id": round_id,
        "title": title,
        "statement": statement,
        "status": status,
        "owner_role": maybe_text(source.get("owner_role")) or "moderator",
        "linked_claim_ids": [maybe_text(value) for value in source.get("linked_claim_ids", []) if maybe_text(value)] if isinstance(source.get("linked_claim_ids"), list) else [],
        "confidence": confidence,
        "created_at_utc": timestamp,
        "updated_at_utc": timestamp,
        "carryover_from_round_id": source_round_id,
        "carryover_from_hypothesis_id": source_hypothesis_id,
        "history": [
            {
                "status": status,
                "updated_at_utc": timestamp,
                "confidence": confidence,
                "operation": "carried-forward",
                "source_round_id": source_round_id,
                "source_hypothesis_id": source_hypothesis_id,
            }
        ],
    }


def task_payload(
    *,
    run_id: str,
    round_id: str,
    owner_role: str,
    title: str,
    task_text: str,
    task_type: str,
    priority: str,
    status: str,
    source_round_id: str,
    source_task_id: str = "",
    source_ticket_id: str = "",
    source_hypothesis_id: str = "",
    linked_artifact_refs: list[str] | None = None,
    related_ids: list[str] | None = None,
    task_discriminator: str = "",
    timestamp: str = "",
) -> dict[str, Any]:
    resolved_timestamp = timestamp or utc_now_iso()
    payload = {
        "task_id": "boardtask-" + stable_hash(run_id, round_id, task_discriminator or title, source_task_id, source_ticket_id, source_hypothesis_id)[:12],
        "run_id": run_id,
        "round_id": round_id,
        "title": maybe_text(title) or "Follow-up investigation task",
        "task_text": maybe_text(task_text),
        "task_type": maybe_text(task_type) or "board-follow-up",
        "status": maybe_text(status) or "planned",
        "owner_role": maybe_text(owner_role) or "moderator",
        "priority": maybe_text(priority) or "medium",
        "source_ticket_id": maybe_text(source_ticket_id),
        "source_hypothesis_id": maybe_text(source_hypothesis_id),
        "linked_artifact_refs": unique_texts(linked_artifact_refs or []),
        "related_ids": unique_texts([*(related_ids or []), source_task_id, source_ticket_id, source_hypothesis_id]),
        "created_at_utc": resolved_timestamp,
        "updated_at_utc": resolved_timestamp,
        "carryover_from_round_id": source_round_id,
        "carryover_from_task_id": maybe_text(source_task_id),
        "history": [
            {
                "status": maybe_text(status) or "planned",
                "owner_role": maybe_text(owner_role) or "moderator",
                "updated_at_utc": resolved_timestamp,
                "operation": "carried-forward",
                "source_round_id": source_round_id,
            }
        ],
    }
    return payload


def next_action_tasks(
    *,
    next_actions: dict[str, Any],
    run_id: str,
    round_id: str,
    source_round_id: str,
    action_limit: int,
    timestamp: str,
) -> list[dict[str, Any]]:
    ranked_actions = next_actions.get("ranked_actions", []) if isinstance(next_actions.get("ranked_actions"), list) else []
    results: list[dict[str, Any]] = []
    for action in ranked_actions[: max(0, action_limit)]:
        if not isinstance(action, dict):
            continue
        target = action.get("target", {}) if isinstance(action.get("target"), dict) else {}
        task = task_payload(
            run_id=run_id,
            round_id=round_id,
            owner_role=maybe_text(action.get("assigned_role")) or "moderator",
            title=maybe_text(action.get("objective")) or maybe_text(action.get("action_kind")) or "Follow up next action",
            task_text=maybe_text(action.get("reason")) or maybe_text(action.get("brief_context")),
            task_type="round-follow-up",
            priority=maybe_text(action.get("priority")) or "medium",
            status="planned",
            source_round_id=source_round_id,
            source_ticket_id=maybe_text(target.get("ticket_id")),
            source_hypothesis_id=maybe_text(target.get("hypothesis_id")),
            linked_artifact_refs=[maybe_text(ref) for ref in action.get("evidence_refs", []) if maybe_text(ref)] if isinstance(action.get("evidence_refs"), list) else [],
            related_ids=[maybe_text(value) for value in action.get("source_ids", []) if maybe_text(value)] if isinstance(action.get("source_ids"), list) else [],
            task_discriminator=maybe_text(action.get("action_id")),
            timestamp=timestamp,
        )
        task["carryover_from_action_id"] = maybe_text(action.get("action_id"))
        task["action_kind"] = maybe_text(action.get("action_kind"))
        results.append(task)
    return results


def dedupe_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for task in tasks:
        if not isinstance(task, dict):
            continue
        key = "|".join(
            unique_texts(
                [
                    maybe_text(task.get("source_ticket_id")),
                    maybe_text(task.get("carryover_from_task_id")),
                    maybe_text(task.get("carryover_from_action_id")),
                    maybe_text(task.get("title")),
                ]
            )
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(task)
    return deduped


def carryover_requirements(
    *,
    role: str,
    source_round_id: str,
    active_hypothesis_count: int,
    open_challenge_count: int,
    open_task_count: int,
    role_actions: list[dict[str, Any]],
    next_round_id: str,
) -> list[dict[str, Any]]:
    top_objectives = [maybe_text(action.get("objective")) for action in role_actions if maybe_text(action.get("objective"))][:3]
    summary_bits = [
        f"source_round={source_round_id}",
        f"active_hypotheses={active_hypothesis_count}",
        f"open_challenges={open_challenge_count}",
        f"open_tasks={open_task_count}",
    ]
    if top_objectives:
        summary_bits.append("top_actions=" + "; ".join(top_objectives))
    return [
        {
            "requirement_id": f"req-{role}-{next_round_id}-cross-round-carryover",
            "requirement_type": "cross-round-carryover",
            "summary": "Continue evidence collection with prior-round carryover context: " + ", ".join(summary_bits) + ".",
            "priority": "high",
            "source_round_id": source_round_id,
        }
    ]


def build_followup_round_tasks(
    *,
    run_id: str,
    round_id: str,
    source_round_id: str,
    mission: dict[str, Any],
    source_tasks: list[dict[str, Any]],
    next_actions: dict[str, Any],
    active_hypothesis_count: int,
    open_challenge_count: int,
    open_task_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    ranked_actions = next_actions.get("ranked_actions", []) if isinstance(next_actions.get("ranked_actions"), list) else []
    role_actions = {
        role: [item for item in ranked_actions if isinstance(item, dict) and maybe_text(item.get("assigned_role")) == role]
        for role in SOURCE_SELECTION_ROLES
    }

    if source_tasks:
        tasks: list[dict[str, Any]] = []
        for index, source_task in enumerate(source_tasks, start=1):
            if not isinstance(source_task, dict):
                continue
            role = maybe_text(source_task.get("assigned_role"))
            cloned = copy.deepcopy(source_task)
            cloned["task_id"] = maybe_text(source_task.get("task_id")) or f"task-{role}-{round_id}-{index:02d}"
            if role:
                cloned["task_id"] = f"task-{role}-{round_id}-{index:02d}"
            cloned["run_id"] = run_id
            cloned["round_id"] = round_id
            cloned["status"] = "planned"
            cloned["source_round_id"] = source_round_id
            cloned["source_task_id"] = maybe_text(source_task.get("task_id"))
            inputs = cloned.get("inputs") if isinstance(cloned.get("inputs"), dict) else {}
            requirements = [item for item in inputs.get("evidence_requirements", []) if isinstance(item, dict)] if isinstance(inputs.get("evidence_requirements"), list) else []
            requirements.extend(
                carryover_requirements(
                    role=role or "moderator",
                    source_round_id=source_round_id,
                    active_hypothesis_count=active_hypothesis_count,
                    open_challenge_count=open_challenge_count,
                    open_task_count=open_task_count,
                    role_actions=role_actions.get(role or "", []),
                    next_round_id=round_id,
                )
            )
            inputs["evidence_requirements"] = requirements
            inputs["prior_round_ids"] = unique_texts([source_round_id, *inputs.get("prior_round_ids", [])]) if isinstance(inputs.get("prior_round_ids"), list) else [source_round_id]
            cloned["inputs"] = inputs
            tasks.append(cloned)
        return tasks, warnings

    warnings.append(
        {
            "code": "missing-source-round-tasks",
            "message": f"No source round task file was found for {source_round_id}; materializing minimal follow-up tasks.",
        }
    )
    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    geometry = region.get("geometry") if isinstance(region.get("geometry"), dict) else {}
    fallback_tasks: list[dict[str, Any]] = []
    for role in SOURCE_SELECTION_ROLES:
        role_source_skills = role_source_skills_from_mission(mission, role)
        expected_output_kinds = ["normalized-public-signals", "claim-candidates"] if role == "sociologist" else ["normalized-environment-signals", "observation-candidates"]
        fallback_tasks.append(
            {
                "task_id": f"task-{role}-{round_id}-01",
                "run_id": run_id,
                "round_id": round_id,
                "assigned_role": role,
                "status": "planned",
                "source_round_id": source_round_id,
                "objective": (
                    "Continue public-discussion evidence collection for the follow-up round."
                    if role == "sociologist"
                    else "Continue physical-observation evidence collection for the follow-up round."
                ),
                "expected_output_kinds": expected_output_kinds,
                "inputs": {
                    "mission_window": window,
                    "mission_geometry": geometry,
                    "source_skills": role_source_skills,
                    "prior_round_ids": [source_round_id],
                    "evidence_requirements": carryover_requirements(
                        role=role,
                        source_round_id=source_round_id,
                        active_hypothesis_count=active_hypothesis_count,
                        open_challenge_count=open_challenge_count,
                        open_task_count=open_task_count,
                        role_actions=role_actions.get(role, []),
                        next_round_id=round_id,
                    ),
                },
            }
        )
    return fallback_tasks, warnings


def open_investigation_round_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    source_round_id: str,
    board_path: str,
    source_task_path: str,
    source_next_actions_path: str,
    output_path: str,
    author_role: str,
    transition_note: str,
    action_limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    source_task_file = resolve_path(run_dir_path, source_task_path, f"investigation/round_tasks_{source_round_id}.json")
    source_next_actions_file = resolve_path(run_dir_path, source_next_actions_path, f"investigation/next_actions_{source_round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"runtime/round_transition_{round_id}.json")
    target_task_file = (run_dir_path / "investigation" / f"round_tasks_{round_id}.json").resolve()
    mission_file = (run_dir_path / "mission.json").resolve()

    warnings: list[dict[str, str]] = []
    source_tasks = load_json_if_exists(source_task_file)
    source_task_rows = [item for item in source_tasks if isinstance(item, dict)] if isinstance(source_tasks, list) else []
    if not source_task_rows:
        warnings.append({"code": "missing-source-round-tasks", "message": f"No task scaffold was found at {source_task_file}."})
    next_actions = load_json_if_exists(source_next_actions_file)
    if next_actions is None:
        warnings.append({"code": "missing-next-actions", "message": f"No next-actions artifact was found at {source_next_actions_file}."})
        next_actions = {}
    mission = load_json_if_exists(mission_file)
    if not isinstance(mission, dict):
        warnings.append({"code": "missing-mission", "message": f"No mission artifact was found at {mission_file}."})
        mission = {}

    with locked_board(board_file, run_id) as board:
        rounds = board.get("rounds", {}) if isinstance(board.get("rounds"), dict) else {}
        source_round = rounds.get(source_round_id)
        if not isinstance(source_round, dict):
            raise ValueError(f"Source round {source_round_id} does not exist on the board: {board_file}")
        if isinstance(rounds.get(round_id), dict):
            existing_output = load_json_if_exists(output_file)
            return {
                "status": "completed",
                "summary": {
                    "skill": SKILL_NAME,
                    "run_id": run_id,
                    "round_id": round_id,
                    "source_round_id": source_round_id,
                    "operation": "noop",
                    "board_path": str(board_file),
                    "output_path": str(output_file),
                    "task_path": str(target_task_file),
                },
                "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "noop")[:20],
                "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "noop")[:16],
                "artifact_refs": [
                    {"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}", "artifact_ref": f"{board_file}:$.rounds.{round_id}"},
                    {"signal_id": "", "artifact_path": str(target_task_file), "record_locator": "$", "artifact_ref": f"{target_task_file}:$"},
                ],
                "canonical_ids": [maybe_text(existing_output.get("transition_id"))] if isinstance(existing_output, dict) and maybe_text(existing_output.get("transition_id")) else [],
                "warnings": [{"code": "round-already-exists", "message": f"Round {round_id} already exists; no mutation was applied."}],
                "board_handoff": {
                    "candidate_ids": [round_id],
                    "evidence_refs": [{"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}", "artifact_ref": f"{board_file}:$.rounds.{round_id}"}],
                    "gap_hints": [],
                    "challenge_hints": [],
                    "suggested_next_skills": ["eco-read-board-delta", "eco-query-public-signals", "eco-query-environment-signals"],
                },
            }

        next_revision = board_revision(board) + 1
        timestamp = utc_now_iso()
        target_round = ensure_round(board, round_id)
        target_round["notes"] = []
        target_round["challenge_tickets"] = []

        source_hypotheses = source_round.get("hypotheses", []) if isinstance(source_round.get("hypotheses"), list) else []
        source_challenges = source_round.get("challenge_tickets", []) if isinstance(source_round.get("challenge_tickets"), list) else []
        source_board_tasks = source_round.get("tasks", []) if isinstance(source_round.get("tasks"), list) else []
        active_hypotheses = [item for item in source_hypotheses if isinstance(item, dict) and hypothesis_is_active(item)]
        open_challenges = [item for item in source_challenges if isinstance(item, dict) and challenge_is_open(item)]
        open_board_tasks = [item for item in source_board_tasks if isinstance(item, dict) and task_is_open(item)]

        carried_hypotheses = [
            clone_hypothesis(item, run_id=run_id, round_id=round_id, source_round_id=source_round_id, timestamp=timestamp)
            for item in active_hypotheses
        ]
        target_round["hypotheses"] = carried_hypotheses

        carried_tasks: list[dict[str, Any]] = []
        for source_task in open_board_tasks:
            carried_tasks.append(
                task_payload(
                    run_id=run_id,
                    round_id=round_id,
                    owner_role=maybe_text(source_task.get("owner_role")) or "moderator",
                    title=maybe_text(source_task.get("title")) or "Continue carried board task",
                    task_text=maybe_text(source_task.get("task_text")) or maybe_text(source_task.get("title")),
                    task_type=maybe_text(source_task.get("task_type")) or "board-follow-up",
                    priority=maybe_text(source_task.get("priority")) or "medium",
                    status="planned",
                    source_round_id=source_round_id,
                    source_task_id=maybe_text(source_task.get("task_id")),
                    source_ticket_id=maybe_text(source_task.get("source_ticket_id")),
                    source_hypothesis_id=maybe_text(source_task.get("source_hypothesis_id")),
                    linked_artifact_refs=[maybe_text(ref) for ref in source_task.get("linked_artifact_refs", []) if maybe_text(ref)] if isinstance(source_task.get("linked_artifact_refs"), list) else [],
                    related_ids=[maybe_text(value) for value in source_task.get("related_ids", []) if maybe_text(value)] if isinstance(source_task.get("related_ids"), list) else [],
                    task_discriminator=maybe_text(source_task.get("task_id")),
                    timestamp=timestamp,
                )
            )
        existing_ticket_ids = {maybe_text(task.get("source_ticket_id")) for task in carried_tasks if maybe_text(task.get("source_ticket_id"))}
        for ticket in open_challenges:
            ticket_id = maybe_text(ticket.get("ticket_id"))
            if ticket_id and ticket_id in existing_ticket_ids:
                continue
            carried_tasks.append(
                task_payload(
                    run_id=run_id,
                    round_id=round_id,
                    owner_role=maybe_text(ticket.get("owner_role")) or "challenger",
                    title=maybe_text(ticket.get("title")) or "Resolve carried challenge ticket",
                    task_text=maybe_text(ticket.get("challenge_statement")) or maybe_text(ticket.get("title")),
                    task_type="challenge-follow-up",
                    priority=maybe_text(ticket.get("priority")) or "high",
                    status="planned",
                    source_round_id=source_round_id,
                    source_ticket_id=ticket_id,
                    source_hypothesis_id=maybe_text(ticket.get("target_hypothesis_id")),
                    linked_artifact_refs=[maybe_text(ref) for ref in ticket.get("linked_artifact_refs", []) if maybe_text(ref)] if isinstance(ticket.get("linked_artifact_refs"), list) else [],
                    related_ids=[ticket_id, ticket.get("target_claim_id"), ticket.get("target_hypothesis_id")],
                    task_discriminator=ticket_id,
                    timestamp=timestamp,
                )
            )
        carried_tasks.extend(
            next_action_tasks(
                next_actions=next_actions if isinstance(next_actions, dict) else {},
                run_id=run_id,
                round_id=round_id,
                source_round_id=source_round_id,
                action_limit=action_limit,
                timestamp=timestamp,
            )
        )
        carried_tasks = dedupe_tasks(carried_tasks)
        target_round["tasks"] = carried_tasks

        generated_note_text = maybe_text(transition_note) or (
            f"Follow-up round opened from {source_round_id}. "
            f"Carried active_hypotheses={len(active_hypotheses)}, open_challenges={len(open_challenges)}, open_tasks={len(open_board_tasks)}."
        )
        note_id = "boardnote-" + stable_hash(run_id, round_id, "round-open", source_round_id, generated_note_text)[:12]
        note = {
            "note_id": note_id,
            "run_id": run_id,
            "round_id": round_id,
            "created_at_utc": timestamp,
            "author_role": maybe_text(author_role) or "moderator",
            "category": "transition",
            "note_text": generated_note_text,
            "tags": ["round-open", "carryover"],
            "linked_artifact_refs": [],
            "related_ids": unique_texts([source_round_id, *[item.get("task_id") for item in carried_tasks], *[item.get("hypothesis_id") for item in carried_hypotheses]]),
        }
        target_round["notes"] = [note]

        event = append_event(
            board,
            run_id,
            round_id,
            "round-opened",
            {
                "source_round_id": source_round_id,
                "note_id": note_id,
                "carried_hypothesis_count": len(carried_hypotheses),
                "carried_task_count": len(carried_tasks),
                "source_open_challenge_count": len(open_challenges),
            },
        )
        write_board(board_file, board, next_revision)

    followup_tasks, task_warnings = build_followup_round_tasks(
        run_id=run_id,
        round_id=round_id,
        source_round_id=source_round_id,
        mission=mission,
        source_tasks=source_task_rows,
        next_actions=next_actions if isinstance(next_actions, dict) else {},
        active_hypothesis_count=len(carried_hypotheses),
        open_challenge_count=len(open_challenges),
        open_task_count=len(open_board_tasks),
    )
    warnings.extend(task_warnings)
    write_json_file(target_task_file, followup_tasks)

    transition_id = "round-transition-" + stable_hash(run_id, round_id, source_round_id, event["event_id"])[:12]
    transition_payload = {
        "schema_version": "board-round-transition-v1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "transition_id": transition_id,
        "run_id": run_id,
        "round_id": round_id,
        "source_round_id": source_round_id,
        "operation": "created",
        "board_path": str(board_file),
        "task_path": str(target_task_file),
        "source_task_path": str(source_task_file),
        "source_next_actions_path": str(source_next_actions_file),
        "board_revision": next_revision,
        "event_id": event["event_id"],
        "counts": {
            "carried_hypothesis_count": len(carried_hypotheses),
            "carried_board_task_count": len(carried_tasks),
            "source_open_challenge_count": len(open_challenges),
            "source_open_task_count": len(open_board_tasks),
            "followup_round_task_count": len(followup_tasks),
        },
        "prior_round_ids": [source_round_id],
        "cross_round_query_hints": {
            "public_signals": {
                "skill": "eco-query-public-signals",
                "round_scope": "up-to-current",
                "query_round_id": round_id,
            },
            "environment_signals": {
                "skill": "eco-query-environment-signals",
                "round_scope": "up-to-current",
                "query_round_id": round_id,
            },
        },
        "warnings": warnings,
    }
    write_json_file(output_file, transition_payload)
    sync_board_to_deliberation_plane(run_dir_path, expected_run_id=run_id, board_path=board_file)

    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
        {"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}", "artifact_ref": f"{board_file}:$.rounds.{round_id}"},
        {"signal_id": "", "artifact_path": str(target_task_file), "record_locator": "$", "artifact_ref": f"{target_task_file}:$"},
    ]
    canonical_ids = [transition_id, *[maybe_text(item.get("hypothesis_id")) for item in carried_hypotheses], *[maybe_text(item.get("task_id")) for item in carried_tasks]]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "source_round_id": source_round_id,
            "board_path": str(board_file),
            "board_revision": next_revision,
            "event_id": event["event_id"],
            "output_path": str(output_file),
            "task_path": str(target_task_file),
            "operation": "created",
            "carried_hypothesis_count": len(carried_hypotheses),
            "carried_board_task_count": len(carried_tasks),
            "followup_round_task_count": len(followup_tasks),
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, transition_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event["event_id"])[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [item for item in canonical_ids if maybe_text(item)],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [round_id, transition_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings if item.get("code") in {"missing-source-round-tasks", "missing-mission"}],
            "challenge_hints": [f"{len(open_challenges)} source-round challenge tickets were converted into follow-up tasks."] if open_challenges else [],
            "suggested_next_skills": [
                "eco-read-board-delta",
                "eco-query-public-signals",
                "eco-query-environment-signals",
                "eco-prepare-round",
                "eco-materialize-history-context",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open a follow-up investigation round while preserving prior round state.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--source-round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--source-task-path", default="")
    parser.add_argument("--source-next-actions-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--author-role", default="moderator")
    parser.add_argument("--transition-note", default="")
    parser.add_argument("--action-limit", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = open_investigation_round_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        source_round_id=args.source_round_id,
        board_path=args.board_path,
        source_task_path=args.source_task_path,
        source_next_actions_path=args.source_next_actions_path,
        output_path=args.output_path,
        author_role=args.author_role,
        transition_note=args.transition_note,
        action_limit=args.action_limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
