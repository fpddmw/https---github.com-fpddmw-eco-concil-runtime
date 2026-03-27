"""Prepare-stage artifact builders for orchestration workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import atomic_write_text_file, read_json, utc_now_iso, write_json
from eco_council_runtime.adapters.run_paths import discover_round_ids, load_mission, round_dir
from eco_council_runtime.cli_invocation import runtime_module_command
from eco_council_runtime.controller.paths import fetch_plan_path, source_selection_path, task_review_prompt_path
from eco_council_runtime.domain.rounds import normalize_round_id
from eco_council_runtime.domain.text import maybe_text, unique_strings


def write_text(path: Path, content: str) -> None:
    atomic_write_text_file(path, content.rstrip() + "\n")


def ensure_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object.")
    return value


def ensure_object_list(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a JSON list.")
    if not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{label} must contain only JSON objects.")
    return value


def resolve_round_id(run_dir: Path, round_id: str) -> str:
    if round_id:
        return normalize_round_id(round_id)
    round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}.")
    return round_ids[-1]


def load_tasks(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    tasks_path = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    return ensure_object_list(read_json(tasks_path), f"{tasks_path}")


def load_source_selection(run_dir: Path, round_id: str, role: str) -> dict[str, Any] | None:
    path = source_selection_path(run_dir, round_id, role)
    if not path.exists():
        return None
    return ensure_object(read_json(path), f"{path}")


def tasks_for_role(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [task for task in tasks if maybe_text(task.get("assigned_role")) == role]


def mission_window(mission: dict[str, Any]) -> dict[str, str]:
    window = ensure_object(mission.get("window"), "mission.window")
    start_utc = maybe_text(window.get("start_utc"))
    end_utc = maybe_text(window.get("end_utc"))
    if not start_utc or not end_utc:
        raise ValueError("Mission window must include start_utc and end_utc.")
    return {"start_utc": start_utc, "end_utc": end_utc}


def mission_region(mission: dict[str, Any]) -> dict[str, Any]:
    return ensure_object(mission.get("region"), "mission.region")


def fetch_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_fetch_prompt.txt"


def round_manifest_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_round_manifest.json"


def orchestrate_command(*args: object) -> str:
    return runtime_module_command("orchestrate", *args)


def render_moderator_task_review_prompt(*, run_dir: Path, round_id: str) -> Path:
    mission_path = run_dir / "mission.json"
    tasks_path = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    validate_command = runtime_module_command("contract", "validate", "--kind", "round-task", "--input", str(tasks_path))
    lines = [
        "Use the eco-council runtime contract validation command below.",
        f"Open mission at: {mission_path}",
        f"Open current task list at: {tasks_path}",
        "",
        "Review and, if needed, revise the round-task list before expert fetch work begins.",
        "Requirements:",
        "1. Keep the file as a JSON list of valid round-task objects.",
        "2. Keep run_id and round_id unchanged.",
        "3. Use only moderator-owned task assignment; do not write claims, observations, evidence cards, or reports here.",
        "4. Use task.inputs.evidence_requirements to describe evidence gaps, claim focus, and priority instead of prescribing concrete source skills.",
        "5. Leave exact source-family, layer, and source-skill choice to the expert source-selection stage under mission governance.",
        "6. Keep objectives concrete enough that sociologist and environmentalist can choose and fetch raw artifacts deterministically.",
        "",
        "After editing, validate with:",
        validate_command,
        "",
        "Return only the final JSON list.",
    ]
    output_path = task_review_prompt_path(run_dir, round_id)
    write_text(output_path, "\n".join(lines))
    return output_path


def render_role_fetch_prompt(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    plan: dict[str, Any],
) -> Path | None:
    tasks = load_tasks(run_dir, round_id)
    role_tasks = tasks_for_role(tasks, role)
    if not role_tasks:
        return None
    steps = [step for step in plan.get("steps", []) if maybe_text(step.get("role")) == role]
    if not steps:
        return None

    mission = load_mission(run_dir)
    mission_window_value = mission_window(mission)
    objective_lines = [f"- {maybe_text(task.get('task_id'))}: {maybe_text(task.get('objective'))}" for task in role_tasks]
    referenced_skills = unique_strings([skill_ref for step in steps for skill_ref in step.get("skill_refs", []) if maybe_text(skill_ref)])
    lines = [
        f"You are the {role} for {maybe_text(mission.get('run_id'))} {round_id}.",
        "",
        "Mission:",
        f"- topic: {maybe_text(mission.get('topic'))}",
        f"- objective: {maybe_text(mission.get('objective'))}",
        f"- region: {maybe_text(mission_region(mission).get('label'))}",
        f"- window_start_utc: {mission_window_value['start_utc']}",
        f"- window_end_utc: {mission_window_value['end_utc']}",
        "",
        "Assigned tasks:",
        *objective_lines,
        "",
        "Relevant skills:",
        ", ".join(referenced_skills) if referenced_skills else "eco-council-runtime orchestrate",
        "",
        "Execution rules:",
        "1. Execute only the shell commands listed below for your role.",
        "2. Keep raw outputs exactly at the specified artifact paths. Those files are the contract boundary for normalization.",
        "3. Do not create claims, observations, evidence cards, or expert reports in this phase.",
        "4. If you intentionally rerun a step, overwrite only the artifact paths already listed in the plan.",
        "5. After all commands complete, return only JSON summarizing artifact paths and any blockers.",
        "",
    ]
    for step in steps:
        lines.extend(
            [
                f"Step: {maybe_text(step.get('step_id'))}",
                f"Source skill: {maybe_text(step.get('source_skill'))}",
                f"Artifact path: {maybe_text(step.get('artifact_path'))}",
            ]
        )
        if isinstance(step.get("depends_on"), list) and step.get("depends_on"):
            lines.append(f"Depends on: {', '.join(step['depends_on'])}")
        if isinstance(step.get("notes"), list):
            for note in step["notes"]:
                note_text = maybe_text(note)
                if note_text:
                    lines.append(f"Note: {note_text}")
        command_text = step.get("command")
        if not isinstance(command_text, str) or not command_text.strip():
            command_text = "# missing command"
        lines.extend(["Command:", "```bash", command_text, "```", ""])

    lines.extend(
        [
            "Return JSON only with this shape:",
            "```json",
            "{",
            f'  "role": "{role}",',
            f'  "round_id": "{round_id}",',
            '  "status": "raw-data-ready",',
            '  "artifacts": ["..."],',
            '  "notes": []',
            "}",
            "```",
        ]
    )
    output_path = fetch_prompt_path(run_dir, round_id, role)
    write_text(output_path, "\n".join(lines))
    return output_path


def write_round_manifest(
    *,
    run_dir: Path,
    round_id: str,
    stage: str,
    task_prompt: Path | None,
    fetch_plan: Path | None,
    fetch_prompts: dict[str, str],
) -> Path:
    prepare_command = orchestrate_command("prepare-round", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty")
    data_plane_command = orchestrate_command("run-data-plane", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty")
    matching_command = orchestrate_command(
        "run-matching-adjudication",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    execute_fetch_command = orchestrate_command(
        "execute-fetch-plan",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    manifest = {
        "manifest_kind": "eco-council-round-manifest",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "stage": stage,
        "run_dir": str(run_dir),
        "round_id": round_id,
        "task_review_prompt_path": str(task_prompt) if task_prompt is not None else "",
        "fetch_plan_path": str(fetch_plan) if fetch_plan is not None else "",
        "role_fetch_prompt_paths": fetch_prompts,
        "next_commands": {
            "prepare_round": prepare_command,
            "run_data_plane": data_plane_command,
            "run_matching_adjudication": matching_command,
            "execute_fetch_plan": execute_fetch_command,
        },
    }
    output_path = round_manifest_path(run_dir, round_id)
    write_json(output_path, manifest, pretty=True)
    return output_path


def materialize_prepare_round_outputs(
    *,
    run_dir: Path,
    round_id: str,
    plan: dict[str, Any],
) -> dict[str, Any]:
    task_prompt = render_moderator_task_review_prompt(run_dir=run_dir, round_id=round_id)
    plan_output_path = fetch_plan_path(run_dir, round_id)
    write_json(plan_output_path, plan, pretty=True)

    prompt_paths: dict[str, str] = {}
    for role in ("sociologist", "environmentalist"):
        path = render_role_fetch_prompt(run_dir=run_dir, round_id=round_id, role=role, plan=plan)
        if path is not None:
            prompt_paths[role] = str(path)

    manifest_path = write_round_manifest(
        run_dir=run_dir,
        round_id=round_id,
        stage="fetch-ready",
        task_prompt=task_prompt,
        fetch_plan=plan_output_path,
        fetch_prompts=prompt_paths,
    )
    return {
        "run_dir": str(run_dir),
        "round_id": round_id,
        "fetch_plan_path": str(plan_output_path),
        "task_review_prompt_path": str(task_prompt),
        "role_fetch_prompt_paths": prompt_paths,
        "manifest_path": str(manifest_path),
        "step_count": len(plan.get("steps", [])) if isinstance(plan.get("steps"), list) else 0,
    }
