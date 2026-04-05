#!/usr/bin/env python3
"""Build one governed fetch plan from mission, tasks, and source selections."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-prepare-round"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.source_queue_contract import (  # noqa: E402
    maybe_text,
    read_json_object,
    resolve_run_dir,
    stable_hash,
    write_json_file,
)
from eco_council_runtime.kernel.source_queue_planner import (  # noqa: E402
    build_fetch_plan,
    write_source_selections,
)
from eco_council_runtime.kernel.source_queue_history import (  # noqa: E402
    load_round_tasks_wrapper,
)
from eco_council_runtime.kernel.source_queue_selection import build_source_selections  # noqa: E402


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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
    plan_payload["observed_inputs"] = {
        "round_tasks_artifact_present": task_artifact_present,
        "round_tasks_present": task_present,
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
            "selection_statuses": {
                role: maybe_text(payload.get("selection_status"))
                for role, payload in plan_payload.get("roles", {}).items()
                if isinstance(payload, dict)
            },
        },
        "receipt_id": "ingress-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, plan_payload["plan_id"])[:20],
        "batch_id": "ingressbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_path.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [plan_payload["plan_id"]],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [plan_payload["plan_id"]],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-import-fetch-execution"],
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
