#!/usr/bin/env python3
"""Materialize the current round's board state into a compact markdown brief."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-materialize-board-brief"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import load_round_snapshot  # noqa: E402


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


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_board_path(run_dir: Path, board_path: str) -> Path:
    text = maybe_text(board_path)
    if not text:
        return (run_dir / "board" / "investigation_board.json").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_summary_path(run_dir: Path, summary_path: str, round_id: str) -> Path:
    text = maybe_text(summary_path)
    if not text:
        return (run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_brief_path(run_dir: Path, brief_path: str, round_id: str) -> Path:
    text = maybe_text(brief_path)
    if not text:
        return (run_dir / "board" / f"board_brief_{round_id}.md").resolve()
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


def render_markdown_section(title: str, items: list[str]) -> list[str]:
    lines = [f"## {title}"]
    if not items:
        lines.append("- None")
        return lines
    lines.extend(f"- {item}" for item in items)
    return lines


def summarize_from_round_state(round_state: dict[str, Any], *, state_source: str) -> dict[str, Any]:
    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    hypotheses = [item for item in round_state.get("hypotheses", []) if isinstance(item, dict)] if isinstance(round_state.get("hypotheses"), list) else []
    challenges = [item for item in round_state.get("challenge_tickets", []) if isinstance(item, dict)] if isinstance(round_state.get("challenge_tickets"), list) else []
    tasks = [item for item in round_state.get("tasks", []) if isinstance(item, dict)] if isinstance(round_state.get("tasks"), list) else []
    active_hypotheses = [item for item in hypotheses if maybe_text(item.get("status")) not in {"closed", "rejected"}]
    open_challenges = [item for item in challenges if maybe_text(item.get("status")) != "closed"]
    open_tasks = [item for item in tasks if maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}]
    return {
        "counts": {
            "notes_total": len(notes),
            "hypotheses_active": len(active_hypotheses),
            "challenge_open": len(open_challenges),
            "tasks_open": len(open_tasks),
        },
        "state_source": state_source,
        "active_hypotheses": active_hypotheses,
        "open_challenges": open_challenges,
        "open_tasks": open_tasks,
    }


def next_moves(snapshot: dict[str, Any]) -> list[str]:
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    active_hypotheses = int(counts.get("hypotheses_active") or 0)
    open_challenges = int(counts.get("challenge_open") or 0)
    open_tasks = int(counts.get("tasks_open") or 0)
    note_count = int(counts.get("notes_total") or 0)
    moves: list[str] = []
    if open_challenges > 0 and open_tasks == 0:
        moves.append("Claim at least one follow-up task for each open challenge ticket.")
    if open_challenges > 0:
        moves.append("Close resolved challenge tickets after the task outcome is recorded.")
    if active_hypotheses == 0:
        moves.append("Promote at least one active hypothesis before readiness review.")
    if note_count == 0:
        moves.append("Add a moderator note that explains why the current board state is actionable.")
    if not moves:
        moves.append("Board state is organized enough to move into Phase D action planning.")
    return moves


def materialize_board_brief_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    summary_path: str,
    brief_path: str,
    max_items: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    summary_file = resolve_summary_path(run_dir_path, summary_path, round_id)
    brief_file = resolve_brief_path(run_dir_path, brief_path, round_id)
    round_snapshot = load_round_snapshot(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        board_path=board_file,
        include_closed=True,
    )
    summary_payload = load_json_if_exists(summary_file)
    round_state = (
        round_snapshot.get("round_state")
        if maybe_text(round_snapshot.get("status")) == "completed"
        and isinstance(round_snapshot.get("round_state"), dict)
        else None
    )
    state_source = maybe_text(round_snapshot.get("state_source"))
    db_path = maybe_text(round_snapshot.get("db_path"))
    deliberation_sync = (
        round_snapshot.get("deliberation_sync")
        if isinstance(round_snapshot.get("deliberation_sync"), dict)
        else {}
    )
    warnings: list[dict[str, Any]] = []
    if round_state is None and not isinstance(summary_payload, dict):
        warnings.append({"code": "missing-board", "message": f"No board artifact was found at {board_file}."})

    if isinstance(round_state, dict):
        snapshot = summarize_from_round_state(
            round_state,
            state_source=state_source or "deliberation-plane",
        )
    elif isinstance(summary_payload, dict):
        snapshot = {
            **summary_payload,
            "state_source": maybe_text(summary_payload.get("state_source")) or "board-summary-artifact",
        }
    else:
        snapshot = summarize_from_round_state({}, state_source="missing-board")
    counts = snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {}
    active_hypotheses = snapshot.get("active_hypotheses", []) if isinstance(snapshot.get("active_hypotheses"), list) else []
    open_challenges = snapshot.get("open_challenges", []) if isinstance(snapshot.get("open_challenges"), list) else []
    open_tasks = snapshot.get("open_tasks", []) if isinstance(snapshot.get("open_tasks"), list) else []
    state_source = maybe_text(snapshot.get("state_source")) or "missing-board"
    moves = next_moves(snapshot)

    hypothesis_lines = [
        f"{maybe_text(item.get('hypothesis_id'))}: {maybe_text(item.get('title')) or maybe_text(item.get('statement'))} | status={maybe_text(item.get('status'))} | owner={maybe_text(item.get('owner_role'))} | confidence={maybe_number(item.get('confidence')) if maybe_number(item.get('confidence')) is not None else 'n/a'}"
        for item in active_hypotheses[: max(1, max_items)]
        if isinstance(item, dict)
    ]
    challenge_lines = [
        f"{maybe_text(item.get('ticket_id'))}: {maybe_text(item.get('title'))} | priority={maybe_text(item.get('priority'))} | owner={maybe_text(item.get('owner_role'))}"
        for item in open_challenges[: max(1, max_items)]
        if isinstance(item, dict)
    ]
    task_lines = [
        f"{maybe_text(item.get('task_id'))}: {maybe_text(item.get('title'))} | status={maybe_text(item.get('status'))} | owner={maybe_text(item.get('owner_role'))}"
        for item in open_tasks[: max(1, max_items)]
        if isinstance(item, dict)
    ]

    lines = [
        f"# Investigation Board Brief: {round_id}",
        "",
        f"- Run: {run_id}",
        f"- Generated at: {utc_now_iso()}",
        f"- State source: {state_source}",
        f"- Active hypotheses: {int(counts.get('hypotheses_active') or 0)}",
        f"- Open challenges: {int(counts.get('challenge_open') or 0)}",
        f"- Open tasks: {int(counts.get('tasks_open') or 0)}",
        f"- Notes: {int(counts.get('notes_total') or 0)}",
        "",
    ]
    lines.extend(render_markdown_section("Active Hypotheses", hypothesis_lines))
    lines.append("")
    lines.extend(render_markdown_section("Open Challenges", challenge_lines))
    lines.append("")
    lines.extend(render_markdown_section("Open Tasks", task_lines))
    lines.append("")
    lines.extend(render_markdown_section("Immediate Next Moves", moves))
    lines.append("")
    brief_file.parent.mkdir(parents=True, exist_ok=True)
    brief_file.write_text("\n".join(lines), encoding="utf-8")

    brief_id = "board-brief-" + stable_hash(run_id, round_id, brief_file.name)[:12]
    artifact_refs = [{"signal_id": "", "artifact_path": str(brief_file), "record_locator": "", "artifact_ref": str(brief_file)}]
    suggested_next_skills = ["eco-propose-next-actions", "eco-summarize-round-readiness"]
    if int(counts.get("challenge_open") or 0) > 0 or int(counts.get("tasks_open") or 0) > 0:
        suggested_next_skills = ["eco-summarize-board-state", "eco-post-board-note", "eco-close-challenge-ticket"]
    candidate_ids: list[Any] = []
    for item in active_hypotheses[:4]:
        if isinstance(item, dict):
            candidate_ids.append(item.get("hypothesis_id"))
    for item in open_challenges[:4]:
        if isinstance(item, dict):
            candidate_ids.append(item.get("ticket_id"))
    for item in open_tasks[:4]:
        if isinstance(item, dict):
            candidate_ids.append(item.get("task_id"))
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "brief_path": str(brief_file),
            "brief_id": brief_id,
            "state_source": state_source,
            "db_path": db_path,
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, brief_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, brief_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [brief_id],
        "warnings": warnings,
        "deliberation_sync": deliberation_sync,
        "board_handoff": {
            "candidate_ids": [maybe_text(item) for item in candidate_ids if maybe_text(item)],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if moves == ["Board state is organized enough to move into Phase D action planning."] else moves[:2],
            "challenge_hints": [],
            "suggested_next_skills": suggested_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize the current round's board state into a compact markdown brief.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--summary-path", default="")
    parser.add_argument("--brief-path", default="")
    parser.add_argument("--max-items", type=int, default=4)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_board_brief_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        summary_path=args.summary_path,
        brief_path=args.brief_path,
        max_items=args.max_items,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
