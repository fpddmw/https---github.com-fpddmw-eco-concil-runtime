#!/usr/bin/env python3
"""Open a follow-up investigation round while preserving prior round state."""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "open-investigation-round"
SOURCE_SELECTION_ROLES = ("social-investigator", "environmental-investigator")
COUNCIL_ROLES = (
    "environmental-investigator",
    "social-investigator",
    "challenger",
    "report-editor",
    "moderator",
)
COORDINATION_HINT_SEMANTICS = (
    "Optional coordination context only; it does not restrict agent write "
    "surfaces, agenda control, evidence acceptance, or investigator autonomy."
)
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.planes.deliberation_plane import (  # noqa: E402
    commit_board_mutation,
    load_round_snapshot,
    load_round_transition_record,
    store_round_transition_record,
    store_round_task_snapshot,
)
from eco_council_runtime.kernel.governance.fallback.common import action_items  # noqa: E402
from eco_council_runtime.kernel.operator.surfaces import load_next_actions_wrapper  # noqa: E402
from eco_council_runtime.kernel.source_queue.source_queue_contract import source_role  # noqa: E402
from eco_council_runtime.kernel.source_queue.source_queue_history import (  # noqa: E402
    load_round_tasks_wrapper,
)
from eco_council_runtime.kernel.governance.transition_requests import (  # noqa: E402
    TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    mark_transition_request_committed,
    request_payload_option,
    resolve_transition_request_for_execution,
)
from eco_council_runtime.objects.council import query_council_objects  # noqa: E402


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


def text_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return unique_texts(value)
    text = maybe_text(value)
    return [text] if text else []


def request_payload_text_list(transition_request: dict[str, Any], key: str) -> list[str]:
    return text_values(request_payload_option(transition_request, key, []))


def latest_round_brief(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    program_id: str,
) -> dict[str, Any]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind="round-brief",
            run_id=run_id,
            round_id=round_id,
            limit=20,
        )
    except Exception:
        return {}
    rows = [item for item in payload.get("objects", []) if isinstance(item, dict)]
    if program_id:
        for item in rows:
            if maybe_text(item.get("program_id")) == program_id:
                return item
    return rows[0] if rows else {}


def build_round_coordination_context(
    *,
    run_id: str,
    round_id: str,
    source_round_id: str,
    transition_request: dict[str, Any],
    round_mode: str = "",
    primary_focus_refs: list[str] | None = None,
    target_challenge_id: str = "",
    context_packet_id: str = "",
    round_brief_id: str = "",
    program_id: str = "",
    round_title: str = "",
    round_subtitle_question: str = "",
    round_category: str = "",
    active_theme_ids: list[str] | None = None,
    round_internal_phases: list[str] | None = None,
    agent_responsibility_boundaries: list[str] | None = None,
    unresolved_responsibility_boundary_refs: list[str] | None = None,
    parent_theme_progress_review_refs: list[str] | None = None,
) -> dict[str, Any]:
    resolved_round_mode = maybe_text(round_mode) or maybe_text(
        request_payload_option(transition_request, "round_mode", "")
    )
    resolved_focus_refs = unique_texts(
        [
            *(primary_focus_refs or []),
            *request_payload_text_list(transition_request, "primary_focus_refs"),
            *request_payload_text_list(transition_request, "primary_focus_ref"),
        ]
    )
    resolved_target_challenge_id = (
        maybe_text(target_challenge_id)
        or maybe_text(request_payload_option(transition_request, "target_challenge_id", ""))
        or maybe_text(request_payload_option(transition_request, "target_ticket_id", ""))
    )
    resolved_context_packet_id = maybe_text(context_packet_id) or maybe_text(
        request_payload_option(transition_request, "context_packet_id", "")
    )
    resolved_round_brief_id = (
        maybe_text(round_brief_id)
        or maybe_text(request_payload_option(transition_request, "round_brief_id", ""))
        or maybe_text(request_payload_option(transition_request, "round_brief_object_id", ""))
    )
    resolved_program_id = maybe_text(program_id) or maybe_text(
        request_payload_option(transition_request, "program_id", "")
    )
    resolved_round_title = maybe_text(round_title) or maybe_text(
        request_payload_option(transition_request, "round_title", "")
    )
    resolved_round_subtitle_question = maybe_text(round_subtitle_question) or maybe_text(
        request_payload_option(transition_request, "round_subtitle_question", "")
    )
    resolved_round_category = maybe_text(round_category) or maybe_text(
        request_payload_option(transition_request, "round_category", "")
    )
    resolved_active_theme_ids = unique_texts(
        [
            *(active_theme_ids or []),
            *request_payload_text_list(transition_request, "active_theme_ids"),
            *request_payload_text_list(transition_request, "active_theme_id"),
        ]
    )
    resolved_internal_phases = unique_texts(
        [
            *(round_internal_phases or []),
            *request_payload_text_list(transition_request, "round_internal_phases"),
            *request_payload_text_list(transition_request, "round_internal_phase"),
        ]
    )
    resolved_boundaries = unique_texts(
        [
            *(agent_responsibility_boundaries or []),
            *request_payload_text_list(transition_request, "agent_responsibility_boundaries"),
            *request_payload_text_list(transition_request, "agent_responsibility_boundary"),
        ]
    )
    resolved_unresolved_boundary_refs = unique_texts(
        [
            *(unresolved_responsibility_boundary_refs or []),
            *request_payload_text_list(transition_request, "unresolved_responsibility_boundary_refs"),
            *request_payload_text_list(transition_request, "unresolved_responsibility_boundary_ref"),
        ]
    )
    resolved_parent_review_refs = unique_texts(
        [
            *(parent_theme_progress_review_refs or []),
            *request_payload_text_list(transition_request, "parent_theme_progress_review_refs"),
            *request_payload_text_list(transition_request, "parent_theme_progress_review_ref"),
            *request_payload_text_list(transition_request, "theme_progress_review_ids"),
        ]
    )
    provided = any(
        [
            resolved_round_mode,
            resolved_focus_refs,
            resolved_target_challenge_id,
            resolved_context_packet_id,
            resolved_round_brief_id,
            resolved_program_id,
            resolved_round_title,
            resolved_round_subtitle_question,
            resolved_round_category,
            resolved_active_theme_ids,
            resolved_internal_phases,
            resolved_boundaries,
            resolved_unresolved_boundary_refs,
            resolved_parent_review_refs,
        ]
    )
    return {
        "schema_version": "round-coordination-context-v1",
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "source_round_id": maybe_text(source_round_id),
        "context_status": "provided" if provided else "minimal",
        "round_mode": resolved_round_mode,
        "program_id": resolved_program_id,
        "round_title": resolved_round_title,
        "round_subtitle_question": resolved_round_subtitle_question,
        "round_category": resolved_round_category,
        "active_theme_ids": resolved_active_theme_ids,
        "round_internal_phases": resolved_internal_phases,
        "agent_responsibility_boundaries": resolved_boundaries,
        "unresolved_responsibility_boundary_refs": resolved_unresolved_boundary_refs,
        "parent_theme_progress_review_refs": resolved_parent_review_refs,
        "primary_focus_refs": resolved_focus_refs,
        "target_challenge_id": resolved_target_challenge_id,
        "context_packet_id": resolved_context_packet_id,
        "round_brief_id": resolved_round_brief_id,
        "transition_request_id": maybe_text(transition_request.get("request_id")),
        "semantics": COORDINATION_HINT_SEMANTICS,
        "agent_autonomy": (
            "Agents may request additional context, pursue alternative evidence, "
            "and decide how to combine or credit evidence."
        ),
    }


def coordination_related_ids(context: dict[str, Any]) -> list[str]:
    if not isinstance(context, dict):
        return []
    return unique_texts(
        [
            *text_values(context.get("primary_focus_refs")),
            *text_values(context.get("active_theme_ids")),
            *text_values(context.get("unresolved_responsibility_boundary_refs")),
            *text_values(context.get("parent_theme_progress_review_refs")),
            context.get("program_id"),
            context.get("target_challenge_id"),
            context.get("context_packet_id"),
            context.get("round_brief_id"),
        ]
    )


def coordination_round_category(context: dict[str, Any]) -> str:
    return maybe_text(context.get("round_category")).casefold()


def coordination_is_acquisition_round(context: dict[str, Any]) -> bool:
    category = coordination_round_category(context)
    mode = maybe_text(context.get("round_mode")).casefold()
    return (
        "acquisition" in category
        or "data-acquisition" in category
        or "acquisition" in mode
    )


def role_boundaries_from_context(context: dict[str, Any]) -> dict[str, list[str]]:
    boundaries_by_role: dict[str, list[str]] = {role: [] for role in COUNCIL_ROLES}
    for boundary in text_values(context.get("agent_responsibility_boundaries")):
        role, sep, detail = boundary.partition(":")
        role = maybe_text(role)
        detail_text = maybe_text(detail if sep else boundary)
        if role in boundaries_by_role and detail_text:
            boundaries_by_role[role].append(detail_text)
    return {
        role: unique_texts(values)
        for role, values in boundaries_by_role.items()
        if unique_texts(values)
    }


def objective_for_context_role(context: dict[str, Any], role: str) -> str:
    question = maybe_text(context.get("round_subtitle_question"))
    title = maybe_text(context.get("round_title"))
    category = coordination_round_category(context)
    prefix = {
        "scope-deliberation": "Deliberate scope and claim boundaries",
        "framing-scope": "Deliberate scope and claim boundaries",
        "semantic-analysis": "Analyze bounded semantic claim basis",
        "data-analysis-semantic-synthesis": "Analyze bounded semantic claim basis",
        "analysis-semantic-synthesis": "Analyze bounded semantic claim basis",
        "interaction-timeline-synthesis": "Synthesize interaction timeline boundaries",
        "interaction-timeline": "Synthesize interaction timeline boundaries",
        "policy-evaluation-basis-synthesis": "Synthesize policy-evaluation basis boundaries",
        "report-readiness-synthesis": "Review report claim readiness and downgrade boundaries",
    }.get(category, "Participate in the issue council round")
    return f"{prefix} as {role}: {question or title}".rstrip(": ")


def requirement_type_for_context(context: dict[str, Any]) -> str:
    category = coordination_round_category(context)
    if "scope" in category or "framing" in category:
        return "scope-boundary-obligation"
    if "semantic" in category or "analysis" in category:
        return "analysis-boundary-obligation"
    if "interaction" in category or "timeline" in category:
        return "interaction-boundary-obligation"
    if "policy" in category or "readiness" in category:
        return "claim-readiness-obligation"
    return "council-round-obligation"


def expected_output_kinds_for_role(role: str, existing: list[Any] | None = None) -> list[str]:
    replacements = {
        "claim-candidates": "public-discourse-evidence",
        "observation-candidates": "environment-evidence",
    }
    values = [replacements.get(maybe_text(value), maybe_text(value)) for value in existing or []]
    defaults = (
        ["normalized-public-signals", "public-discourse-evidence"]
        if maybe_text(role) == "social-investigator"
        else ["normalized-environment-signals", "environment-evidence"]
    )
    return unique_texts(values or defaults)


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
    round_events = snapshot.get("round_events")
    if isinstance(round_events, list) and round_events:
        return True
    round_state = snapshot.get("round_state")
    if not isinstance(round_state, dict):
        return False
    return any(
        int(round_state.get(field) or 0) > 0
        for field in (
            "note_count",
            "hypothesis_count",
            "challenge_ticket_count",
            "task_count",
        )
    )


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
    results: list[dict[str, Any]] = []
    for action in action_items(next_actions)[: max(0, action_limit)]:
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
        for field_name in (
            "relation_id",
            "objection_code",
            "challenged_rule",
            "alternative_explanation",
            "report_risk",
        ):
            field_value = maybe_text(action.get(field_name))
            if field_value:
                task[field_name] = field_value
        if isinstance(action.get("required_followup_evidence"), list):
            task["required_followup_evidence"] = unique_texts(
                action["required_followup_evidence"]
            )
        if isinstance(action.get("target"), dict):
            target = {
                key: maybe_text(value)
                for key, value in action["target"].items()
                if maybe_text(value)
            }
            if target:
                task["target"] = target
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
    coordination_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    context = coordination_context if isinstance(coordination_context, dict) else {}
    top_objectives = [maybe_text(action.get("objective")) for action in role_actions if maybe_text(action.get("objective"))][:3]
    summary_bits = [
        f"source_round={source_round_id}",
        f"active_hypotheses={active_hypothesis_count}",
        f"open_challenges={open_challenge_count}",
        f"open_tasks={open_task_count}",
    ]
    if top_objectives:
        summary_bits.append("top_actions=" + "; ".join(top_objectives))
    if context and not coordination_is_acquisition_round(context):
        question = maybe_text(context.get("round_subtitle_question"))
        return [
            {
                "requirement_id": f"req-{role}-{next_round_id}-council-boundary",
                "requirement_type": requirement_type_for_context(context),
                "summary": (
                    f"Address the round question within the role boundary: {question}. "
                    + "Carry forward prior-round context without treating it as an evidence collection route: "
                    + ", ".join(summary_bits)
                    + "."
                ),
                "priority": "high",
                "source_round_id": source_round_id,
            }
        ]
    return [
        {
            "requirement_id": f"req-{role}-{next_round_id}-cross-round-carryover",
            "requirement_type": "cross-round-carryover",
            "summary": "Continue evidence collection with prior-round carryover context: " + ", ".join(summary_bits) + ".",
            "priority": "high",
            "source_round_id": source_round_id,
        }
    ]


def program_issue_council_tasks(
    *,
    run_id: str,
    round_id: str,
    source_round_id: str,
    mission: dict[str, Any],
    coordination_context: dict[str, Any],
    active_hypothesis_count: int,
    open_challenge_count: int,
    open_task_count: int,
    role_actions: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    geometry = region.get("geometry") if isinstance(region.get("geometry"), dict) else {}
    boundaries_by_role = role_boundaries_from_context(coordination_context)
    roles = unique_texts([*boundaries_by_role.keys(), "moderator"])
    tasks: list[dict[str, Any]] = []
    for index, role in enumerate(roles, start=1):
        tasks.append(
            {
                "task_id": f"task-{role}-{round_id}-{index:02d}",
                "run_id": run_id,
                "round_id": round_id,
                "assigned_role": role,
                "status": "planned",
                "source_round_id": source_round_id,
                "objective": objective_for_context_role(coordination_context, role),
                "expected_output_kinds": [
                    "agent-position",
                    "readiness-opinion",
                    "round-synthesis" if role == "moderator" else "role-boundary-note",
                ],
                "inputs": {
                    "mission_window": window,
                    "mission_geometry": geometry,
                    "source_skills": [],
                    "prior_round_ids": [source_round_id],
                    "round_coordination_context": coordination_context,
                    "role_responsibility_boundaries": boundaries_by_role.get(role, []),
                    "evidence_requirements": carryover_requirements(
                        role=role,
                        source_round_id=source_round_id,
                        active_hypothesis_count=active_hypothesis_count,
                        open_challenge_count=open_challenge_count,
                        open_task_count=open_task_count,
                        role_actions=role_actions.get(role, []),
                        next_round_id=round_id,
                        coordination_context=coordination_context,
                    ),
                },
            }
        )
    return tasks


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
    coordination_context: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    task_coordination_context = (
        copy.deepcopy(coordination_context)
        if isinstance(coordination_context, dict)
        else {}
    )
    role_actions = {
        role: [item for item in action_items(next_actions) if maybe_text(item.get("assigned_role")) == role]
        for role in COUNCIL_ROLES
    }
    if task_coordination_context and not coordination_is_acquisition_round(task_coordination_context):
        return (
            program_issue_council_tasks(
                run_id=run_id,
                round_id=round_id,
                source_round_id=source_round_id,
                mission=mission,
                coordination_context=task_coordination_context,
                active_hypothesis_count=active_hypothesis_count,
                open_challenge_count=open_challenge_count,
                open_task_count=open_task_count,
                role_actions=role_actions,
            ),
            warnings,
        )

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
            cloned["expected_output_kinds"] = expected_output_kinds_for_role(
                role,
                cloned.get("expected_output_kinds")
                if isinstance(cloned.get("expected_output_kinds"), list)
                else [],
            )
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
                    coordination_context=task_coordination_context,
                )
            )
            inputs["evidence_requirements"] = requirements
            inputs["prior_round_ids"] = unique_texts([source_round_id, *inputs.get("prior_round_ids", [])]) if isinstance(inputs.get("prior_round_ids"), list) else [source_round_id]
            if task_coordination_context:
                inputs["round_coordination_context"] = task_coordination_context
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
        expected_output_kinds = expected_output_kinds_for_role(role)
        fallback_tasks.append(
            {
                "task_id": f"task-{role}-{round_id}-01",
                "run_id": run_id,
                "round_id": round_id,
                "assigned_role": role,
                "status": "planned",
                "source_round_id": source_round_id,
                "objective": (
                    "Continue public-discussion evidence collection for investigator query, finding, and evidence-bundle submission."
                    if role == "social-investigator"
                    else "Continue environmental evidence collection for investigator query, quality review, and evidence-bundle submission."
                ),
                "expected_output_kinds": expected_output_kinds,
                "inputs": {
                    "mission_window": window,
                    "mission_geometry": geometry,
                    "source_skills": role_source_skills,
                    "prior_round_ids": [source_round_id],
                    "round_coordination_context": task_coordination_context,
                    "evidence_requirements": carryover_requirements(
                        role=role,
                        source_round_id=source_round_id,
                        active_hypothesis_count=active_hypothesis_count,
                        open_challenge_count=open_challenge_count,
                        open_task_count=open_task_count,
                        role_actions=role_actions.get(role, []),
                        next_round_id=round_id,
                        coordination_context=task_coordination_context,
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
    transition_request_id: str,
    board_path: str,
    source_task_path: str,
    source_next_actions_path: str,
    output_path: str,
    author_role: str,
    transition_note: str,
    action_limit: int,
    round_mode: str = "",
    primary_focus_refs: list[str] | None = None,
    target_challenge_id: str = "",
    context_packet_id: str = "",
    round_brief_id: str = "",
    program_id: str = "",
    round_title: str = "",
    round_subtitle_question: str = "",
    round_category: str = "",
    active_theme_ids: list[str] | None = None,
    round_internal_phases: list[str] | None = None,
    agent_responsibility_boundaries: list[str] | None = None,
    unresolved_responsibility_boundary_refs: list[str] | None = None,
    parent_theme_progress_review_refs: list[str] | None = None,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    source_task_wrapper = load_round_tasks_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=source_round_id,
        task_path=source_task_path,
    )
    source_task_file = Path(
        maybe_text(source_task_wrapper.get("artifact_path"))
        or resolve_path(
            run_dir_path,
            source_task_path,
            f"investigation/round_tasks_{source_round_id}.json",
        )
    ).resolve()
    source_next_actions_wrapper = load_next_actions_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=source_round_id,
        next_actions_path=source_next_actions_path,
    )
    source_next_actions_file = Path(
        maybe_text(source_next_actions_wrapper.get("artifact_path"))
        or resolve_path(
            run_dir_path,
            source_next_actions_path,
            f"investigation/next_actions_{source_round_id}.json",
        )
    ).resolve()
    output_file = resolve_path(run_dir_path, output_path, f"runtime/round_transition_{round_id}.json")
    target_task_file = (run_dir_path / "investigation" / f"round_tasks_{round_id}.json").resolve()
    mission_file = (run_dir_path / "mission.json").resolve()
    transition_request = resolve_transition_request_for_execution(
        run_dir_path,
        request_id=transition_request_id,
        transition_kind=TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
        run_id=run_id,
        round_id=source_round_id,
        source_round_id=source_round_id,
        target_round_id=round_id,
    )
    if not maybe_text(transition_note):
        transition_note = maybe_text(
            request_payload_option(transition_request, "transition_note", "")
        ) or maybe_text(
            request_payload_option(transition_request, "request_note", "")
        )
    requested_action_limit = request_payload_option(
        transition_request,
        "action_limit",
        action_limit,
    )
    try:
        action_limit = max(0, int(requested_action_limit or action_limit))
    except (TypeError, ValueError):
        action_limit = max(0, int(action_limit or 0))
    requested_program_id = maybe_text(program_id) or maybe_text(
        request_payload_option(transition_request, "program_id", "")
    )
    program_round_brief = latest_round_brief(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        program_id=requested_program_id,
    )
    if program_round_brief:
        program_id = requested_program_id or maybe_text(program_round_brief.get("program_id"))
        round_title = maybe_text(round_title) or maybe_text(program_round_brief.get("round_title"))
        round_subtitle_question = maybe_text(round_subtitle_question) or maybe_text(
            program_round_brief.get("round_subtitle_question")
        )
        round_category = maybe_text(round_category) or maybe_text(program_round_brief.get("round_category"))
        round_mode = maybe_text(round_mode) or maybe_text(program_round_brief.get("round_mode"))
        round_brief_id = maybe_text(round_brief_id) or maybe_text(program_round_brief.get("object_id"))
        active_theme_ids = unique_texts(
            [
                *(active_theme_ids or []),
                *text_values(program_round_brief.get("active_theme_ids")),
            ]
        )
        round_internal_phases = unique_texts(
            [
                *(round_internal_phases or []),
                *text_values(program_round_brief.get("round_internal_phases")),
            ]
        )
        agent_responsibility_boundaries = unique_texts(
            [
                *(agent_responsibility_boundaries or []),
                *text_values(program_round_brief.get("agent_responsibility_boundaries")),
            ]
        )
    coordination_context = build_round_coordination_context(
        run_id=run_id,
        round_id=round_id,
        source_round_id=source_round_id,
        transition_request=transition_request,
        round_mode=round_mode,
        primary_focus_refs=primary_focus_refs or [],
        target_challenge_id=target_challenge_id,
        context_packet_id=context_packet_id,
        round_brief_id=round_brief_id,
        program_id=program_id,
        round_title=round_title,
        round_subtitle_question=round_subtitle_question,
        round_category=round_category,
        active_theme_ids=active_theme_ids or [],
        round_internal_phases=round_internal_phases or [],
        agent_responsibility_boundaries=agent_responsibility_boundaries or [],
        unresolved_responsibility_boundary_refs=unresolved_responsibility_boundary_refs or [],
        parent_theme_progress_review_refs=parent_theme_progress_review_refs or [],
    )
    coordination_ids = coordination_related_ids(coordination_context)

    warnings: list[dict[str, str]] = []
    source_task_source = (
        maybe_text(source_task_wrapper.get("source")) or "missing-round-tasks"
    )
    source_task_artifact_present = bool(source_task_wrapper.get("artifact_present"))
    source_task_present = bool(source_task_wrapper.get("payload_present"))
    source_tasks = source_task_wrapper.get("payload")
    source_task_rows = (
        [item for item in source_tasks if isinstance(item, dict)]
        if isinstance(source_tasks, list)
        else []
    )
    if not source_task_rows:
        warnings.append(
            {
                "code": "missing-source-round-tasks",
                "message": (
                    "No source round task scaffold artifact or deliberation-plane "
                    f"snapshot was found for {source_round_id} "
                    f"(expected artifact path: {source_task_file})."
                ),
            }
        )
    source_next_actions_source = (
        maybe_text(source_next_actions_wrapper.get("source")) or "missing-next-actions"
    )
    source_next_actions_artifact_present = bool(
        source_next_actions_wrapper.get("artifact_present")
    )
    source_next_actions_present = bool(
        source_next_actions_wrapper.get("payload_present")
    )
    next_actions = source_next_actions_wrapper.get("payload")
    if not isinstance(next_actions, dict):
        warnings.append(
            {
                "code": "missing-next-actions",
                "message": (
                    "No next-actions artifact or deliberation-plane snapshot was "
                    f"found for source round {source_round_id} "
                    f"(expected artifact path: {source_next_actions_file})."
                ),
            }
        )
        next_actions = {}
    mission = load_json_if_exists(mission_file)
    if not isinstance(mission, dict):
        warnings.append({"code": "missing-mission", "message": f"No mission artifact was found at {mission_file}."})
        mission = {}

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
        source_round = (
            source_snapshot.get("round_state")
            if isinstance(source_snapshot.get("round_state"), dict)
            else {}
        )
        if not round_snapshot_has_state(source_snapshot):
            raise ValueError(
                f"Source round {source_round_id} does not exist on the board or deliberation plane: {board_file}"
            )
        if round_snapshot_has_state(target_snapshot):
            transition_request_id = maybe_text(transition_request.get("request_id"))
            existing_output = load_json_if_exists(output_file)
            existing_transition: dict[str, Any] = {}
            existing_transition_id = ""
            committed_transition_id = (
                maybe_text(transition_request.get("committed_object_id"))
                if maybe_text(transition_request.get("committed_object_kind"))
                == "round-transition"
                else ""
            )
            if committed_transition_id:
                existing_transition_id = committed_transition_id
                loaded_transition = load_round_transition_record(
                    run_dir_path,
                    transition_id=committed_transition_id,
                )
                if isinstance(loaded_transition, dict):
                    existing_transition = loaded_transition
                if not isinstance(existing_output, dict):
                    warnings.append(
                        {
                            "code": "missing-round-transition-artifact",
                            "message": (
                                f"No transition artifact was found at {output_file}; "
                                "recovered the existing transition id from the committed transition request."
                            ),
                        }
                    )
            if not existing_transition_id and isinstance(existing_output, dict):
                existing_transition = existing_output
                existing_transition_id = maybe_text(existing_output.get("transition_id"))
            if not existing_transition_id:
                loaded_transition = load_round_transition_record(
                    run_dir_path,
                    run_id=run_id,
                    round_id=round_id,
                    source_round_id=source_round_id,
                    transition_request_id=transition_request_id,
                )
                if isinstance(loaded_transition, dict):
                    existing_transition = loaded_transition
                    existing_transition_id = maybe_text(
                        loaded_transition.get("transition_id")
                    )
                    if not isinstance(existing_output, dict):
                        warnings.append(
                            {
                                "code": "missing-round-transition-artifact",
                                "message": (
                                    f"No transition artifact was found at {output_file}; "
                                    "recovered the existing transition id from the deliberation plane."
                                ),
                            }
                        )
            existing_transition_request_id = maybe_text(
                existing_transition.get("transition_request_id")
            )
            if existing_transition_id and (
                not existing_transition_request_id
                or existing_transition_request_id == transition_request_id
                or existing_transition_id == committed_transition_id
            ):
                mark_transition_request_committed(
                    run_dir_path,
                    request_id=transition_request_id,
                    committed_by_role=maybe_text(
                        transition_request.get("required_approval_role")
                    )
                    or maybe_text(transition_request.get("latest_decision_by_role"))
                    or "runtime-operator",
                    committed_object_kind="round-transition",
                    committed_object_id=existing_transition_id,
                )
            elif existing_transition_id and existing_transition_request_id:
                warnings.append(
                    {
                        "code": "target-round-owned-by-different-transition-request",
                        "message": (
                            f"Round {round_id} already exists from transition request "
                            f"{existing_transition_request_id}; the current request was not recommitted."
                        ),
                    }
                )
            warnings.append(
                {
                    "code": "round-already-exists",
                    "message": f"Round {round_id} already exists; no mutation was applied.",
                }
            )
            target_sync = (
                target_snapshot.get("deliberation_sync")
                if isinstance(target_snapshot.get("deliberation_sync"), dict)
                else {}
            )
            existing_coordination_context = (
                existing_transition.get("coordination_context")
                if isinstance(existing_transition.get("coordination_context"), dict)
                else coordination_context
            )
            return {
                "status": "completed",
                "summary": {
                    "skill": SKILL_NAME,
                    "run_id": run_id,
                    "round_id": round_id,
                    "source_round_id": source_round_id,
                    "operation": "noop",
                    "board_path": str(board_file),
                    "board_revision": max(0, int(target_sync.get("board_revision") or 0)),
                    "db_path": maybe_text(target_snapshot.get("db_path")) or maybe_text(target_sync.get("db_path")),
                    "write_surface": "deliberation-plane",
                    "output_path": str(output_file),
                    "task_path": str(target_task_file),
                    "transition_request_id": transition_request_id,
                    "round_mode": maybe_text(existing_coordination_context.get("round_mode")),
                    "program_id": maybe_text(existing_coordination_context.get("program_id")),
                    "round_category": maybe_text(existing_coordination_context.get("round_category")),
                    "context_packet_id": maybe_text(existing_coordination_context.get("context_packet_id")),
                    "target_challenge_id": maybe_text(existing_coordination_context.get("target_challenge_id")),
                    "round_brief_id": maybe_text(existing_coordination_context.get("round_brief_id")),
                },
                "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "noop")[:20],
                "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "noop")[:16],
                "artifact_refs": [
                    {"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}", "artifact_ref": f"{board_file}:$.rounds.{round_id}"},
                    {"signal_id": "", "artifact_path": str(target_task_file), "record_locator": "$", "artifact_ref": f"{target_task_file}:$"},
                ],
                "canonical_ids": [existing_transition_id] if existing_transition_id else [],
                "warnings": warnings,
                "board_handoff": {
                    "candidate_ids": unique_texts([round_id, *coordination_related_ids(existing_coordination_context)]),
                    "evidence_refs": [{"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}", "artifact_ref": f"{board_file}:$.rounds.{round_id}"}],
                    "gap_hints": [],
                    "challenge_hints": [],
                    "suggested_next_skills": ["query-board-delta", "query-public-signals", "query-environment-signals"],
                },
            }

        timestamp = utc_now_iso()
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
            "related_ids": unique_texts(
                [
                    source_round_id,
                    *coordination_ids,
                    *[item.get("task_id") for item in carried_tasks],
                    *[item.get("hypothesis_id") for item in carried_hypotheses],
                ]
            ),
        }
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
            coordination_context=coordination_context,
        )
        warnings.extend(task_warnings)
        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            note_records=[note],
            hypothesis_records=carried_hypotheses,
            task_records=carried_tasks,
            event_type="round-opened",
            event_payload={
                "source_round_id": source_round_id,
                "note_id": note_id,
                "carried_hypothesis_count": len(carried_hypotheses),
                "carried_task_count": len(carried_tasks),
                "source_open_challenge_count": len(open_challenges),
                "coordination_context": coordination_context,
            },
            event_created_at_utc=timestamp,
            event_discriminator=note_id,
        )
        write_json_file(target_task_file, followup_tasks)
        store_round_task_snapshot(
            run_dir_path,
            task_snapshot={
                "schema_version": "round-task-snapshot-v1",
                "generated_at_utc": utc_now_iso(),
                "run_id": run_id,
                "round_id": round_id,
                "task_source": "round-tasks-artifact",
                "task_count": len(followup_tasks),
                "tasks": followup_tasks,
            },
            artifact_path=str(target_task_file),
        )

        event_id = maybe_text(write_summary.get("event_id"))
        board_revision = max(0, int(write_summary.get("board_revision") or 0))
        transition_id = "round-transition-" + stable_hash(run_id, round_id, source_round_id, event_id)[:12]
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
            "source_task_source": source_task_source,
            "source_next_actions_path": str(source_next_actions_file),
            "source_next_actions_source": source_next_actions_source,
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
            "board_revision": board_revision,
            "event_id": event_id,
            "transition_request_id": maybe_text(transition_request.get("request_id")),
            "transition_request_status": maybe_text(transition_request.get("request_status")),
            "approved_by_role": maybe_text(transition_request.get("latest_decision_by_role")),
            "round_mode": maybe_text(coordination_context.get("round_mode")),
            "program_id": maybe_text(coordination_context.get("program_id")),
            "round_title": maybe_text(coordination_context.get("round_title")),
            "round_subtitle_question": maybe_text(coordination_context.get("round_subtitle_question")),
            "round_category": maybe_text(coordination_context.get("round_category")),
            "active_theme_ids": text_values(coordination_context.get("active_theme_ids")),
            "round_internal_phases": text_values(coordination_context.get("round_internal_phases")),
            "agent_responsibility_boundaries": text_values(coordination_context.get("agent_responsibility_boundaries")),
            "unresolved_responsibility_boundary_refs": text_values(coordination_context.get("unresolved_responsibility_boundary_refs")),
            "parent_theme_progress_review_refs": text_values(coordination_context.get("parent_theme_progress_review_refs")),
            "primary_focus_refs": text_values(coordination_context.get("primary_focus_refs")),
            "target_challenge_id": maybe_text(coordination_context.get("target_challenge_id")),
            "context_packet_id": maybe_text(coordination_context.get("context_packet_id")),
            "round_brief_id": maybe_text(coordination_context.get("round_brief_id")),
            "coordination_context": coordination_context,
            "observed_inputs": {
                "source_task_present": source_task_present,
                "source_task_artifact_present": source_task_artifact_present,
                "source_next_actions_present": source_next_actions_present,
                "source_next_actions_artifact_present": source_next_actions_artifact_present,
                "coordination_context_status": maybe_text(coordination_context.get("context_status")),
                "program_round_brief_loaded": bool(program_round_brief),
                "round_brief_id_present": bool(maybe_text(coordination_context.get("round_brief_id"))),
                "context_packet_id_present": bool(maybe_text(coordination_context.get("context_packet_id"))),
            },
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
                    "skill": "query-public-signals",
                    "round_scope": "up-to-current",
                    "query_round_id": round_id,
                },
                "environment_signals": {
                    "skill": "query-environment-signals",
                    "round_scope": "up-to-current",
                    "query_round_id": round_id,
                },
            },
            "warnings": warnings,
        }
        write_json_file(output_file, transition_payload)
        store_round_transition_record(
            run_dir_path,
            transition_record={
                **transition_payload,
                "artifact_path": str(output_file),
                "record_locator": "$",
            },
        )
        mark_transition_request_committed(
            run_dir_path,
            request_id=maybe_text(transition_request.get("request_id")),
            committed_by_role=maybe_text(
                transition_request.get("required_approval_role")
            )
            or maybe_text(transition_request.get("latest_decision_by_role"))
            or "runtime-operator",
            committed_object_kind="round-transition",
            committed_object_id=transition_id,
        )

    event_id = maybe_text(write_summary.get("event_id"))
    board_revision = max(0, int(write_summary.get("board_revision") or 0))

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
            "board_revision": board_revision,
            "event_id": event_id,
            "output_path": str(output_file),
            "task_path": str(target_task_file),
            "operation": "created",
            "carried_hypothesis_count": len(carried_hypotheses),
            "carried_board_task_count": len(carried_tasks),
            "followup_round_task_count": len(followup_tasks),
            "source_task_source": source_task_source,
            "source_next_actions_source": source_next_actions_source,
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
            "transition_request_id": maybe_text(transition_request.get("request_id")),
            "round_mode": maybe_text(coordination_context.get("round_mode")),
            "program_id": maybe_text(coordination_context.get("program_id")),
            "round_category": maybe_text(coordination_context.get("round_category")),
            "context_packet_id": maybe_text(coordination_context.get("context_packet_id")),
            "target_challenge_id": maybe_text(coordination_context.get("target_challenge_id")),
            "round_brief_id": maybe_text(coordination_context.get("round_brief_id")),
            "coordination_context_status": maybe_text(coordination_context.get("context_status")),
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, transition_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [item for item in canonical_ids if maybe_text(item)],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": unique_texts([round_id, transition_id, *coordination_ids]),
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings if item.get("code") in {"missing-source-round-tasks", "missing-mission"}],
            "challenge_hints": [f"{len(open_challenges)} source-round challenge tickets were converted into follow-up tasks."] if open_challenges else [],
            "suggested_next_skills": [
                "query-board-delta",
                "query-public-signals",
                "query-environment-signals",
                "prepare-round",
                "materialize-history-context",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open a follow-up investigation round while preserving prior round state.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--source-round-id", required=True)
    parser.add_argument("--transition-request-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--source-task-path", default="")
    parser.add_argument("--source-next-actions-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--author-role", default="moderator")
    parser.add_argument("--transition-note", default="")
    parser.add_argument("--action-limit", type=int, default=3)
    parser.add_argument("--round-mode", default="")
    parser.add_argument("--primary-focus-ref", action="append", default=[])
    parser.add_argument("--target-challenge-id", default="")
    parser.add_argument("--context-packet-id", default="")
    parser.add_argument("--round-brief-id", default="")
    parser.add_argument("--program-id", default="")
    parser.add_argument("--round-title", default="")
    parser.add_argument("--round-subtitle-question", default="")
    parser.add_argument("--round-category", default="")
    parser.add_argument("--active-theme-id", action="append", default=[])
    parser.add_argument("--round-internal-phase", action="append", default=[])
    parser.add_argument("--agent-responsibility-boundary", action="append", default=[])
    parser.add_argument("--unresolved-responsibility-boundary-ref", action="append", default=[])
    parser.add_argument("--parent-theme-progress-review-ref", action="append", default=[])
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = open_investigation_round_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        source_round_id=args.source_round_id,
        transition_request_id=args.transition_request_id,
        board_path=args.board_path,
        source_task_path=args.source_task_path,
        source_next_actions_path=args.source_next_actions_path,
        output_path=args.output_path,
        author_role=args.author_role,
        transition_note=args.transition_note,
        action_limit=args.action_limit,
        round_mode=args.round_mode,
        primary_focus_refs=args.primary_focus_ref,
        target_challenge_id=args.target_challenge_id,
        context_packet_id=args.context_packet_id,
        round_brief_id=args.round_brief_id,
        program_id=args.program_id,
        round_title=args.round_title,
        round_subtitle_question=args.round_subtitle_question,
        round_category=args.round_category,
        active_theme_ids=args.active_theme_id,
        round_internal_phases=args.round_internal_phase,
        agent_responsibility_boundaries=args.agent_responsibility_boundary,
        unresolved_responsibility_boundary_refs=args.unresolved_responsibility_boundary_ref,
        parent_theme_progress_review_refs=args.parent_theme_progress_review_ref,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
