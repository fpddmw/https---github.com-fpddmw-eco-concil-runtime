#!/usr/bin/env python3
"""Build one governed fetch plan from mission, tasks, and source selections."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "prepare-round"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.source_queue.source_queue_contract import (  # noqa: E402
    mission_requires_scoping,
    maybe_text,
    read_json_object,
    resolve_run_dir,
    stable_hash,
    write_json_file,
)
from eco_council_runtime.kernel.source_queue.source_queue_planner import (  # noqa: E402
    build_fetch_plan,
    write_source_selections,
)
from eco_council_runtime.kernel.source_queue.source_queue_history import (  # noqa: E402
    load_round_tasks_wrapper,
)
from eco_council_runtime.kernel.source_queue.source_queue_selection import build_source_selections  # noqa: E402
from eco_council_runtime.objects.council import query_council_objects  # noqa: E402


ROUND_BRIEF_HINT_SEMANTICS = (
    "Optional coordination context only; it does not restrict agent write "
    "surfaces, source selection, evidence acceptance, or investigator autonomy."
)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def suggested_next_skills_for_selections(
    selections: dict[str, dict[str, Any]],
    *,
    mission: dict[str, Any],
    has_fetch_steps: bool,
) -> list[str]:
    if mission_requires_scoping(mission):
        next_skills = [
            "submit-investigation-plan",
            "submit-investigation-scope",
            "submit-round-brief",
            "submit-evidence-request",
        ]
        if has_fetch_steps:
            next_skills.append("normalize-fetch-execution")
        return next_skills
    return ["normalize-fetch-execution"]


def normalize_actor_role_for_source_role(role: str) -> str:
    if role == "environmental-investigator":
        return "environmental-investigator"
    if role == "social-investigator":
        return "social-investigator"
    return role


def text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [maybe_text(item) for item in value if maybe_text(item)]


def latest_round_brief_context(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    query = {
        "object_kind": "round-brief",
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "limit": 1,
    }
    try:
        result = query_council_objects(
            run_dir,
            object_kind="round-brief",
            run_id=run_id,
            round_id=round_id,
            limit=1,
        )
    except ValueError as exc:
        return {
            "present": False,
            "source": "deliberation-plane",
            "query": query,
            "error": str(exc),
            "semantics": ROUND_BRIEF_HINT_SEMANTICS,
        }
    objects = result.get("objects", []) if isinstance(result.get("objects"), list) else []
    brief = objects[0] if objects and isinstance(objects[0], dict) else {}
    if not brief:
        return {
            "present": False,
            "source": "deliberation-plane",
            "query": query,
            "matching_object_count": int(result.get("summary", {}).get("matching_object_count") or 0)
            if isinstance(result.get("summary"), dict)
            else 0,
            "semantics": ROUND_BRIEF_HINT_SEMANTICS,
        }
    return {
        "present": True,
        "source": "deliberation-plane",
        "query": query,
        "object_kind": "round-brief",
        "object_id": maybe_text(brief.get("object_id")),
        "author_role": maybe_text(brief.get("author_role")),
        "status": maybe_text(brief.get("status")),
        "round_mode": maybe_text(brief.get("round_mode")),
        "program_id": maybe_text(brief.get("program_id")),
        "round_title": maybe_text(brief.get("round_title")),
        "round_subtitle_question": maybe_text(brief.get("round_subtitle_question")),
        "round_category": maybe_text(brief.get("round_category")),
        "target_kind": maybe_text(brief.get("target_kind")),
        "target_id": maybe_text(brief.get("target_id")),
        "context_packet_id": maybe_text(brief.get("context_packet_id")),
        "active_theme_ids": text_list(brief.get("active_theme_ids")),
        "agent_responsibility_boundaries": text_list(
            brief.get("agent_responsibility_boundaries")
        ),
        "round_internal_phases": text_list(brief.get("round_internal_phases")),
        "round_exit_criteria": text_list(brief.get("round_exit_criteria")),
        "primary_focus_refs": text_list(brief.get("primary_focus_refs")),
        "open_questions": text_list(brief.get("open_questions")),
        "requested_outputs": text_list(brief.get("requested_outputs")),
        "invited_roles": text_list(brief.get("invited_roles")),
        "boundary_notes": text_list(brief.get("boundary_notes")),
        "source_boundary_notes": text_list(brief.get("source_boundary_notes")),
        "brief_text": maybe_text(brief.get("brief_text")),
        "rationale": maybe_text(brief.get("rationale")),
        "semantics": ROUND_BRIEF_HINT_SEMANTICS,
        "object": brief,
    }


def suggested_next_skill_runs_for_selections(selections: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for role, selection in selections.items():
        if not isinstance(selection, dict):
            continue
        selected_sources = [
            source
            for source in selection.get("selected_sources", [])
            if maybe_text(source)
        ] if isinstance(selection.get("selected_sources"), list) else []
        if not selected_sources:
            continue
        runs.append(
            {
                "skill_name": "normalize-fetch-execution",
                "actor_role": normalize_actor_role_for_source_role(maybe_text(role)),
                "plan_role": maybe_text(role),
                "reason": "Run only this role owner's fetch and normalization steps.",
                "selected_source_skills": selected_sources,
            }
        )
    return runs


def prepare_round_skill(run_dir: str, run_id: str, round_id: str) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    mission_path = (run_dir_path / "mission.json").resolve()
    task_path = (run_dir_path / "investigation" / f"round_tasks_{round_id}.json").resolve()
    output_path = (run_dir_path / "runtime" / f"fetch_plan_{round_id}.json").resolve()

    mission = read_json_object(mission_path)
    if maybe_text(mission.get("run_id")) != run_id:
        raise ValueError(f"run_id mismatch between mission.json and --run-id: {maybe_text(mission.get('run_id'))!r} != {run_id!r}")
    task_context = load_round_tasks_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    task_source = maybe_text(task_context.get("source")) or "missing-round-tasks"
    task_artifact_present = bool(task_context.get("artifact_present"))
    task_present = bool(task_context.get("payload_present"))
    task_payload = task_context.get("payload")
    if not isinstance(task_payload, list) or not all(
        isinstance(item, dict) for item in task_payload
    ):
        raise ValueError(
            "No round task scaffold artifact or deliberation-plane snapshot was "
            f"found for round {round_id} (expected artifact path: {task_path})."
        )
    tasks = list(task_payload)
    if not task_artifact_present:
        write_json_file(task_path, tasks)

    round_brief_context = latest_round_brief_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    selections = build_source_selections(run_dir=run_dir_path, mission=mission, tasks=tasks, run_id=run_id, round_id=round_id)
    write_source_selections(run_dir_path, round_id, selections)
    plan_payload, warnings = build_fetch_plan(
        run_dir=run_dir_path,
        run_id=run_id,
        round_id=round_id,
        mission=mission,
        tasks=tasks,
        selections=selections,
    )
    plan_payload["task_path"] = str(task_path)
    plan_payload["task_source"] = task_source
    plan_payload["round_brief_context"] = round_brief_context
    plan_payload["coordination_context"] = {
        "round_brief": round_brief_context,
        "semantics": ROUND_BRIEF_HINT_SEMANTICS,
    }
    plan_payload["observed_inputs"] = {
        "round_tasks_artifact_present": task_artifact_present,
        "round_tasks_present": task_present,
        "round_brief_present": bool(round_brief_context.get("present")),
        "round_brief_source": maybe_text(round_brief_context.get("source")),
    }
    write_json_file(output_path, plan_payload)

    if not plan_payload["steps"]:
        warnings = [
            *warnings,
            {
                "code": "empty-fetch-plan",
                "message": "prepare-round completed without any runnable fetch steps for the current source selections.",
            },
        ]

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_path), "record_locator": "$", "artifact_ref": f"{output_path}:$"}]
    selected_sources = [
        source_skill
        for role_payload in plan_payload.get("roles", {}).values()
        if isinstance(role_payload, dict)
        for source_skill in role_payload.get("selected_sources", [])
        if maybe_text(source_skill)
    ]
    suggested_next_skills = suggested_next_skills_for_selections(
        selections,
        mission=mission,
        has_fetch_steps=bool(plan_payload.get("steps")),
    )
    suggested_next_skill_runs = suggested_next_skill_runs_for_selections(selections)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_path),
            "plan_id": plan_payload["plan_id"],
            "source_count": len({maybe_text(item) for item in selected_sources if maybe_text(item)}),
            "step_count": len(plan_payload["steps"]),
            "task_source": task_source,
            "round_brief_id": maybe_text(round_brief_context.get("object_id")),
            "round_brief_present": bool(round_brief_context.get("present")),
            "selection_statuses": {
                role: maybe_text(payload.get("selection_status"))
                for role, payload in plan_payload.get("roles", {}).items()
                if isinstance(payload, dict)
            },
        },
        "receipt_id": "ingress-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, plan_payload["plan_id"])[:20],
        "batch_id": "ingressbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_path.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [
            item
            for item in [
                plan_payload["plan_id"],
                maybe_text(round_brief_context.get("object_id")),
            ]
            if maybe_text(item)
        ],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [
                item
                for item in [
                    plan_payload["plan_id"],
                    maybe_text(round_brief_context.get("object_id")),
                ]
                if maybe_text(item)
            ],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings],
            "challenge_hints": [],
            "suggested_next_skills": suggested_next_skills,
            "suggested_next_skill_runs": suggested_next_skill_runs,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build one governed fetch plan from mission, tasks, and source selections.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = prepare_round_skill(run_dir=args.run_dir, run_id=args.run_id, round_id=args.round_id)
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
