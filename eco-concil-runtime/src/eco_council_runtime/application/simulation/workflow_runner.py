"""Workflow runners and CLI command handlers for deterministic simulations."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from .common import (
    MODE_VALUES,
    SUPPORTED_SOURCES,
    ensure_fetch_plan_inputs_match,
    ensure_object,
    exclusive_file_lock,
    fetch_execution_path,
    fetch_lock_path,
    fetch_plan_path,
    file_sha256,
    maybe_text,
    mission_path,
    pretty_json,
    read_json,
    resolve_round_id,
    scenario_mode_for_source,
    source_count,
    tasks_path,
    utc_now_iso,
    write_json,
    write_text,
    write_jsonl,
)
from .payload_synthesis import generate_payload_for_source
from .presets import find_preset_path, load_scenario, preset_paths


def write_artifact(path: Path, payload: Any, *, pretty: bool) -> None:
    if path.suffix.lower() == ".jsonl":
        if not isinstance(payload, list):
            raise ValueError(f"Expected list payload for JSONL artifact: {path}")
        write_jsonl(path, [item for item in payload if isinstance(item, dict)])
        return
    write_json(path, payload, pretty=pretty)


def load_plan(run_dir: Path, round_id: str) -> dict[str, Any]:
    return ensure_object(read_json(fetch_plan_path(run_dir, round_id)), "fetch_plan")


def simulate_round(
    *,
    run_dir: Path,
    round_id: str,
    scenario: dict[str, Any],
    scenario_source: str,
    skip_input_check: bool,
    continue_on_error: bool,
    overwrite: bool,
    skip_existing: bool,
) -> dict[str, Any]:
    current_round_id = resolve_round_id(run_dir, round_id)
    plan = load_plan(run_dir, current_round_id)
    if not skip_input_check:
        ensure_fetch_plan_inputs_match(run_dir=run_dir, round_id=current_round_id, plan=plan)
    mission = ensure_object(read_json(mission_path(run_dir)), "mission")
    tasks_payload = read_json(tasks_path(run_dir, current_round_id))
    tasks = [item for item in tasks_payload if isinstance(item, dict)] if isinstance(tasks_payload, list) else []
    steps = [item for item in plan.get("steps", []) if isinstance(item, dict)]

    statuses: list[dict[str, Any]] = []
    succeeded: set[str] = set()
    with exclusive_file_lock(fetch_lock_path(run_dir, current_round_id)):
        for step in steps:
            step_id = maybe_text(step.get("step_id"))
            role = maybe_text(step.get("role"))
            source_skill = maybe_text(step.get("source_skill"))
            if source_skill not in SUPPORTED_SOURCES:
                failure_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": source_skill,
                    "status": "failed",
                    "reason": "unsupported_source_skill",
                }
                statuses.append(failure_status)
                if not continue_on_error:
                    break
                continue

            depends_on = [maybe_text(item) for item in step.get("depends_on", []) if maybe_text(item)]
            if any(item not in succeeded for item in depends_on):
                statuses.append(
                    {
                        "step_id": step_id,
                        "role": role,
                        "source_skill": source_skill,
                        "status": "skipped",
                        "reason": f"Unmet dependencies: {depends_on}",
                    }
                )
                if not continue_on_error:
                    break
                continue

            artifact_path = Path(maybe_text(step.get("artifact_path"))).expanduser().resolve()
            stdout_path = Path(maybe_text(step.get("stdout_path"))).expanduser().resolve()
            stderr_path = Path(maybe_text(step.get("stderr_path"))).expanduser().resolve()
            artifact_capture = maybe_text(step.get("artifact_capture"))

            if artifact_path.exists():
                if skip_existing:
                    statuses.append(
                        {
                            "step_id": step_id,
                            "role": role,
                            "source_skill": source_skill,
                            "status": "skipped",
                            "reason": "artifact_exists",
                            "artifact_path": str(artifact_path),
                            "stdout_path": str(stdout_path),
                            "stderr_path": str(stderr_path),
                        }
                    )
                    succeeded.add(step_id)
                    continue
                if not overwrite:
                    statuses.append(
                        {
                            "step_id": step_id,
                            "role": role,
                            "source_skill": source_skill,
                            "status": "failed",
                            "reason": "artifact_exists",
                            "artifact_path": str(artifact_path),
                        }
                    )
                    if not continue_on_error:
                        break
                    continue

            try:
                mode = scenario_mode_for_source(scenario, source_skill)
                if mode not in MODE_VALUES:
                    raise ValueError(f"Unsupported mode for {source_skill}: {mode}")
                payload = generate_payload_for_source(
                    mission=mission,
                    scenario=scenario,
                    source_skill=source_skill,
                    mode=mode,
                    step=step,
                    dependency_statuses=statuses,
                )
                artifact_path.parent.mkdir(parents=True, exist_ok=True)
                stdout_path.parent.mkdir(parents=True, exist_ok=True)
                stderr_path.parent.mkdir(parents=True, exist_ok=True)
                write_artifact(artifact_path, payload, pretty=True)
                record_count = source_count(payload, source_skill)
                if artifact_capture == "stdout-json":
                    write_artifact(stdout_path, payload, pretty=True)
                else:
                    write_json(
                        stdout_path,
                        {
                            "step_id": step_id,
                            "source_skill": source_skill,
                            "scenario_id": maybe_text(scenario.get("scenario_id")),
                            "scenario_source": scenario_source,
                            "mode": mode,
                            "record_count": record_count,
                            "artifact_path": str(artifact_path),
                            "generated_at_utc": utc_now_iso(),
                        },
                        pretty=True,
                    )
                write_text(
                    stderr_path,
                    f"[simulated] step_id={step_id} source_skill={source_skill} "
                    f"scenario_id={maybe_text(scenario.get('scenario_id'))} mode={mode} records={record_count}\n",
                )
                statuses.append(
                    {
                        "step_id": step_id,
                        "role": role,
                        "source_skill": source_skill,
                        "status": "completed",
                        "artifact_path": str(artifact_path),
                        "stdout_path": str(stdout_path),
                        "stderr_path": str(stderr_path),
                        "simulation_mode": mode,
                    }
                )
                succeeded.add(step_id)
            except Exception as exc:
                stderr_path.parent.mkdir(parents=True, exist_ok=True)
                write_text(stderr_path, f"[simulated-error] {exc}\n")
                failure_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": source_skill,
                    "status": "failed",
                    "reason": str(exc),
                    "artifact_path": str(artifact_path),
                    "stderr_path": str(stderr_path),
                }
                statuses.append(failure_status)
                if not continue_on_error:
                    break

        result = {
            "run_dir": str(run_dir),
            "round_id": current_round_id,
            "plan_path": str(fetch_plan_path(run_dir, current_round_id)),
            "plan_sha256": file_sha256(fetch_plan_path(run_dir, current_round_id)),
            "execution_mode": "simulated",
            "scenario": {
                "scenario_id": maybe_text(scenario.get("scenario_id")),
                "scenario_source": scenario_source,
                "claim_type": maybe_text(scenario.get("claim_type")),
                "mode": maybe_text(scenario.get("mode")),
                "seed": scenario.get("seed"),
            },
            "step_count": len(steps),
            "completed_count": sum(1 for item in statuses if maybe_text(item.get("status")) == "completed"),
            "failed_count": sum(1 for item in statuses if maybe_text(item.get("status")) == "failed"),
            "statuses": statuses,
        }
        execution_path = fetch_execution_path(run_dir, current_round_id)
        write_json(execution_path, result, pretty=True)
        result["execution_path"] = str(execution_path)
        return result


def command_list_presets(args: argparse.Namespace) -> dict[str, Any]:
    presets: list[dict[str, Any]] = []
    for path in preset_paths():
        payload = ensure_object(read_json(path), str(path))
        presets.append(
            {
                "preset": path.stem,
                "path": str(path),
                "scenario_id": maybe_text(payload.get("scenario_id")),
                "description": maybe_text(payload.get("description")),
                "claim_type": maybe_text(payload.get("claim_type")),
                "mode": maybe_text(payload.get("mode")),
            }
        )
    return {"preset_count": len(presets), "presets": presets}


def command_write_preset(args: argparse.Namespace) -> dict[str, Any]:
    preset_path = find_preset_path(args.preset)
    payload = ensure_object(read_json(preset_path), str(preset_path))
    output_path = Path(args.output).expanduser().resolve()
    write_json(output_path, payload, pretty=True)
    return {
        "preset": args.preset,
        "source_path": str(preset_path),
        "output_path": str(output_path),
        "scenario_id": maybe_text(payload.get("scenario_id")),
    }


def command_simulate_round(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    current_round_id = resolve_round_id(run_dir, args.round_id)
    plan = load_plan(run_dir, current_round_id)
    mission = ensure_object(read_json(mission_path(run_dir)), "mission")
    tasks_payload = read_json(tasks_path(run_dir, current_round_id))
    tasks = [item for item in tasks_payload if isinstance(item, dict)] if isinstance(tasks_payload, list) else []
    steps = [item for item in plan.get("steps", []) if isinstance(item, dict)]
    scenario, scenario_source = load_scenario(args=args, mission=mission, tasks=tasks, steps=steps)
    return simulate_round(
        run_dir=run_dir,
        round_id=current_round_id,
        scenario=scenario,
        scenario_source=scenario_source,
        skip_input_check=args.skip_input_check,
        continue_on_error=args.continue_on_error,
        overwrite=args.overwrite,
        skip_existing=args.skip_existing,
    )


__all__ = [
    "command_list_presets",
    "command_simulate_round",
    "command_write_preset",
    "load_plan",
    "pretty_json",
    "simulate_round",
]
