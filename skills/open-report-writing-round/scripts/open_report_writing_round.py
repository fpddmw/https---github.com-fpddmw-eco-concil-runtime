#!/usr/bin/env python3
"""Open a report-editor-only round after council closeout."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "open-report-writing-round"
ROUND_MODE = "report-writing"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.planes.deliberation_plane import (  # noqa: E402
    commit_board_mutation,
    load_round_snapshot,
    load_round_transition_record,
    store_round_task_snapshot,
    store_round_transition_record,
)
from eco_council_runtime.kernel.governance.transition_requests import (  # noqa: E402
    TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
    mark_transition_request_committed,
    request_payload_option,
    resolve_transition_request_for_execution,
)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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


def text_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return unique_texts(value)
    text = maybe_text(value)
    return [text] if text else []


def request_payload_text_list(request: dict[str, Any], key: str) -> list[str]:
    return text_values(request_payload_option(request, key, []))


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


@contextmanager
def locked_board(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def round_snapshot_has_state(snapshot: dict[str, Any]) -> bool:
    if not isinstance(snapshot, dict):
        return False
    if isinstance(snapshot.get("round_events"), list) and snapshot["round_events"]:
        return True
    round_state = snapshot.get("round_state")
    if not isinstance(round_state, dict):
        return False
    return any(
        int(round_state.get(field) or 0) > 0
        for field in ("note_count", "hypothesis_count", "challenge_ticket_count", "task_count")
    )


def build_coordination_context(
    *,
    run_id: str,
    round_id: str,
    source_round_id: str,
    transition_request: dict[str, Any],
    basis_round_id: str,
    reporting_basis_refs: list[str],
    report_language: str,
) -> dict[str, Any]:
    return {
        "schema_version": "round-coordination-context-v1",
        "run_id": run_id,
        "round_id": round_id,
        "source_round_id": source_round_id,
        "basis_round_id": basis_round_id or source_round_id,
        "context_status": "provided",
        "round_mode": ROUND_MODE,
        "report_language": maybe_text(report_language) or "en",
        "primary_focus_refs": unique_texts(reporting_basis_refs),
        "transition_request_id": maybe_text(transition_request.get("request_id")),
        "semantics": (
            "Reporting-only continuation. It schedules report-editor work from "
            "existing council/reporting basis and does not reopen investigation."
        ),
        "agent_autonomy": (
            "Report editor may organize narrative, surface limitations, and ask "
            "moderator for a separate investigation continuation if material gaps remain."
        ),
    }


def report_writing_task(
    *,
    run_id: str,
    round_id: str,
    source_round_id: str,
    basis_round_id: str,
    report_language: str,
    context: dict[str, Any],
    timestamp: str,
) -> dict[str, Any]:
    return {
        "task_id": "task-report-editor-" + stable_hash(run_id, round_id, source_round_id, "narrative-report")[:12],
        "run_id": run_id,
        "round_id": round_id,
        "assigned_role": "report-editor",
        "status": "planned",
        "source_round_id": source_round_id,
        "objective": (
            "Draft, validate, and publish a reader-facing narrative report from existing frozen/canonical "
            "council basis. The report must lead with the bottom line, explain the reasoning path, make "
            "limitations visible, and preserve refs for audit without turning the prose into an object dump."
        ),
        "expected_output_kinds": [
            "narrative-report-draft",
            "narrative-report-validation",
            "narrative-report",
        ],
        "inputs": {
            "prior_round_ids": [source_round_id],
            "basis_round_id": basis_round_id or source_round_id,
            "report_language": maybe_text(report_language) or "en",
            "round_coordination_context": context,
            "allowed_report_skills": [
                "draft-narrative-report",
                "validate-narrative-report",
                "publish-narrative-report",
            ],
            "investigation_policy": (
                "Do not perform source acquisition, fetch, normalize, or new evidence "
                "adoption in this round. Record gaps as limitations or ask moderator "
                "for a separate investigation continuation."
            ),
            "writing_requirements": {
                "primary_audience": "busy human reviewer or decision-maker",
                "required_sections": [
                    "Executive Summary / bottom line",
                    "Key Points",
                    "What Happened",
                    "Evidence And Reasoning",
                    "Limits And Confidence",
                    "Decision Use",
                    "Audit Trail",
                ],
                "language": maybe_text(report_language) or "en",
                "style_rules": [
                    "Use plain language and complete sentences.",
                    "Do not lead paragraphs with runtime object ids or schema labels.",
                    "Separate what the council can say from what it cannot yet say.",
                    "Keep refs visible but secondary to the narrative.",
                ],
            },
        },
        "created_at_utc": timestamp,
        "updated_at_utc": timestamp,
    }


def open_report_writing_round_skill(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    source_round_id: str,
    transition_request_id: str,
    board_path: str = "",
    output_path: str = "",
    basis_round_id: str = "",
    reporting_basis_refs: list[str] | None = None,
    report_language: str = "",
    author_role: str = "moderator",
    transition_note: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_path(run_dir_path, board_path, "board/investigation_board.json")
    output_file = resolve_path(run_dir_path, output_path, f"runtime/round_transition_{round_id}.json")
    task_file = (run_dir_path / "investigation" / f"round_tasks_{round_id}.json").resolve()
    transition_request = resolve_transition_request_for_execution(
        run_dir_path,
        request_id=transition_request_id,
        transition_kind=TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
        run_id=run_id,
        round_id=source_round_id,
        source_round_id=source_round_id,
        target_round_id=round_id,
    )
    resolved_basis_round_id = (
        maybe_text(basis_round_id)
        or maybe_text(request_payload_option(transition_request, "basis_round_id", ""))
        or source_round_id
    )
    resolved_basis_refs = unique_texts(
        [
            *(reporting_basis_refs or []),
            *request_payload_text_list(transition_request, "reporting_basis_refs"),
            *request_payload_text_list(transition_request, "primary_focus_refs"),
        ]
    )
    resolved_report_language = (
        maybe_text(report_language)
        or maybe_text(request_payload_option(transition_request, "report_language", ""))
        or maybe_text(request_payload_option(transition_request, "language", ""))
        or "en"
    )
    warnings: list[dict[str, str]] = []

    with locked_board(board_file):
        source_snapshot = load_round_snapshot(
            run_dir_path,
            expected_run_id=run_id,
            round_id=source_round_id,
            board_path=board_file,
            include_closed=True,
        )
        target_snapshot = load_round_snapshot(
            run_dir_path,
            expected_run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            include_closed=True,
        )
        if not round_snapshot_has_state(source_snapshot):
            raise ValueError(f"Source round {source_round_id} does not exist on the board or deliberation plane.")
        if round_snapshot_has_state(target_snapshot):
            existing_transition = load_json_if_exists(output_file) or {}
            existing_transition_id = maybe_text(existing_transition.get("transition_id"))
            if not existing_transition_id:
                loaded = load_round_transition_record(
                    run_dir_path,
                    run_id=run_id,
                    round_id=round_id,
                    source_round_id=source_round_id,
                    transition_request_id=transition_request_id,
                )
                existing_transition = loaded if isinstance(loaded, dict) else {}
                existing_transition_id = maybe_text(existing_transition.get("transition_id"))
            if existing_transition_id:
                mark_transition_request_committed(
                    run_dir_path,
                    request_id=transition_request_id,
                    committed_by_role=maybe_text(transition_request.get("required_approval_role")) or "runtime-operator",
                    committed_object_kind="round-transition",
                    committed_object_id=existing_transition_id,
                )
            warnings.append({"code": "round-already-exists", "message": f"Round {round_id} already exists; no mutation was applied."})
            return {
                "status": "completed",
                "summary": {
                    "skill": SKILL_NAME,
                    "operation": "noop",
                    "run_id": run_id,
                    "round_id": round_id,
                    "source_round_id": source_round_id,
                    "basis_round_id": resolved_basis_round_id,
                    "report_language": resolved_report_language,
                    "round_mode": ROUND_MODE,
                    "output_path": str(output_file),
                    "task_path": str(task_file),
                    "transition_request_id": transition_request_id,
                },
                "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "noop")[:20],
                "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "noop")[:16],
                "artifact_refs": [
                    {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
                    {"signal_id": "", "artifact_path": str(task_file), "record_locator": "$", "artifact_ref": f"{task_file}:$"},
                ],
                "canonical_ids": [existing_transition_id] if existing_transition_id else [],
                "warnings": warnings,
                "board_handoff": {"candidate_ids": [round_id], "evidence_refs": [], "gap_hints": [], "challenge_hints": [], "suggested_next_skills": []},
            }

        timestamp = utc_now_iso()
        context = build_coordination_context(
            run_id=run_id,
            round_id=round_id,
            source_round_id=source_round_id,
            transition_request=transition_request,
            basis_round_id=resolved_basis_round_id,
            reporting_basis_refs=resolved_basis_refs,
            report_language=resolved_report_language,
        )
        task = report_writing_task(
            run_id=run_id,
            round_id=round_id,
            source_round_id=source_round_id,
            basis_round_id=resolved_basis_round_id,
            report_language=resolved_report_language,
            context=context,
            timestamp=timestamp,
        )
        note_text = maybe_text(transition_note) or (
            f"Report-writing round opened from {source_round_id}; only report-editor is scheduled."
        )
        note_id = "boardnote-" + stable_hash(run_id, round_id, "report-writing-round", note_text)[:12]
        note = {
            "note_id": note_id,
            "run_id": run_id,
            "round_id": round_id,
            "created_at_utc": timestamp,
            "author_role": maybe_text(author_role) or "moderator",
            "category": "transition",
            "note_text": note_text,
            "tags": ["round-open", "report-writing"],
            "linked_artifact_refs": resolved_basis_refs,
            "related_ids": unique_texts([source_round_id, resolved_basis_round_id, task["task_id"]]),
        }
        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            note_records=[note],
            task_records=[task],
            event_type="report-writing-round-opened",
            event_payload={
                "source_round_id": source_round_id,
                "basis_round_id": resolved_basis_round_id,
                "report_language": resolved_report_language,
                "round_mode": ROUND_MODE,
                "task_id": task["task_id"],
                "coordination_context": context,
            },
            event_created_at_utc=timestamp,
            event_discriminator=note_id,
        )
        write_json_file(task_file, [task])
        store_round_task_snapshot(
            run_dir_path,
            task_snapshot={
                "schema_version": "round-task-snapshot-v1",
                "generated_at_utc": utc_now_iso(),
                "run_id": run_id,
                "round_id": round_id,
                "task_source": "report-writing-round-skill",
                "task_count": 1,
                "tasks": [task],
            },
            artifact_path=str(task_file),
        )
        event_id = maybe_text(write_summary.get("event_id"))
        transition_id = "round-transition-" + stable_hash(run_id, round_id, source_round_id, event_id)[:12]
        transition_payload = {
            "schema_version": "board-round-transition-v1",
            "skill": SKILL_NAME,
            "generated_at_utc": utc_now_iso(),
            "transition_id": transition_id,
            "run_id": run_id,
            "round_id": round_id,
            "source_round_id": source_round_id,
            "basis_round_id": resolved_basis_round_id,
            "operation": "created",
            "round_mode": ROUND_MODE,
            "board_path": str(board_file),
            "task_path": str(task_file),
            "output_path": str(output_file),
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
            "board_revision": int(write_summary.get("board_revision") or 0),
            "event_id": event_id,
            "transition_request_id": transition_request_id,
            "transition_request_status": maybe_text(transition_request.get("request_status")),
            "approved_by_role": maybe_text(transition_request.get("latest_decision_by_role")),
            "primary_focus_refs": resolved_basis_refs,
            "coordination_context": context,
            "prior_round_ids": [source_round_id],
            "warnings": warnings,
        }
        write_json_file(output_file, transition_payload)
        store_round_transition_record(
            run_dir_path,
            transition_record={**transition_payload, "artifact_path": str(output_file), "record_locator": "$"},
        )
        mark_transition_request_committed(
            run_dir_path,
            request_id=transition_request_id,
            committed_by_role=maybe_text(transition_request.get("required_approval_role")) or "runtime-operator",
            committed_object_kind="round-transition",
            committed_object_id=transition_id,
        )

    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
        {"signal_id": "", "artifact_path": str(task_file), "record_locator": "$", "artifact_ref": f"{task_file}:$"},
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "operation": "created",
            "run_id": run_id,
            "round_id": round_id,
            "source_round_id": source_round_id,
            "basis_round_id": resolved_basis_round_id,
            "report_language": resolved_report_language,
            "round_mode": ROUND_MODE,
            "transition_request_id": transition_request_id,
            "output_path": str(output_file),
            "task_path": str(task_file),
            "event_id": event_id,
            "board_revision": int(write_summary.get("board_revision") or 0),
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, transition_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [transition_id, task["task_id"]],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": unique_texts([round_id, transition_id, task["task_id"], *resolved_basis_refs]),
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": [],
            "suggested_next_skills": [
                "materialize-agent-entry-gate",
                "materialize-openclaw-agent-registration",
                "draft-narrative-report",
                "validate-narrative-report",
                "publish-narrative-report",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open a report-editor-only reporting round.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--source-round-id", required=True)
    parser.add_argument("--transition-request-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--basis-round-id", default="")
    parser.add_argument("--reporting-basis-ref", action="append", default=[])
    parser.add_argument("--report-language", default="")
    parser.add_argument("--author-role", default="moderator")
    parser.add_argument("--transition-note", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = open_report_writing_round_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        source_round_id=args.source_round_id,
        transition_request_id=args.transition_request_id,
        board_path=args.board_path,
        output_path=args.output_path,
        basis_round_id=args.basis_round_id,
        reporting_basis_refs=args.reporting_basis_ref,
        report_language=args.report_language,
        author_role=args.author_role,
        transition_note=args.transition_note,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
