from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .source_queue_contract import (
    SOURCE_SELECTION_ROLES,
    effective_constraints,
    file_sha256,
    file_snapshot,
    maybe_text,
    normalize_artifact_imports,
    normalize_source_requests,
    policy_profile_summary,
    source_config,
    source_selection_path,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json_file,
)
from .source_queue_selection import role_selected_sources

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]


def sanitize_fragment(value: str) -> str:
    cleaned = [char if char.isalnum() else "-" for char in maybe_text(value)]
    normalized = "".join(cleaned).strip("-")
    return normalized or "artifact"


def tasks_for_role(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [task for task in tasks if maybe_text(task.get("assigned_role")) == role]


def task_ids_for_role(tasks: list[dict[str, Any]], role: str) -> list[str]:
    return [maybe_text(task.get("task_id")) for task in tasks_for_role(tasks, role) if maybe_text(task.get("task_id"))]


def role_evidence_requirements(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    role_tasks = tasks_for_role(tasks, role)
    requirements: list[dict[str, Any]] = []
    seen: set[str] = set()
    for task in role_tasks:
        inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
        values = inputs.get("evidence_requirements") if isinstance(inputs.get("evidence_requirements"), list) else []
        for item in values:
            if not isinstance(item, dict):
                continue
            requirement_id = maybe_text(item.get("requirement_id"))
            if not requirement_id or requirement_id.casefold() in seen:
                continue
            seen.add(requirement_id.casefold())
            requirements.append(item)
    return requirements


def normalizer_args_for(source_skill: str, payload: dict[str, Any]) -> list[str]:
    args: list[str] = []
    query_text = maybe_text(payload.get("query_text"))
    source_mode = maybe_text(payload.get("source_mode"))
    if source_skill in {"gdelt-doc-search", "youtube-video-search"} and query_text:
        args.extend(["--query-text-override", query_text])
    if source_skill == "openaq-data-fetch" and source_mode:
        args.extend(["--source-mode", source_mode])
    return args


def planned_artifact_path(run_dir: Path, round_id: str, source_skill: str, step_index: int, override_path: str = "") -> Path:
    if maybe_text(override_path):
        candidate = Path(maybe_text(override_path)).expanduser()
        if not candidate.is_absolute():
            candidate = run_dir / candidate
        return candidate.resolve()
    suffix = maybe_text(source_config(source_skill).get("default_suffix")) or ".json"
    return (run_dir / "raw" / round_id / f"{step_index:02d}-{sanitize_fragment(source_skill)}{suffix}").resolve()


def source_selection_snapshot(run_dir: Path, round_id: str, selections: dict[str, dict[str, Any]]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    for role in SOURCE_SELECTION_ROLES:
        path = source_selection_path(run_dir, round_id, role)
        payload = selections.get(role, {}) if isinstance(selections.get(role), dict) else {}
        snapshot[role] = {
            **file_snapshot(path),
            "status": maybe_text(payload.get("status")),
            "selected_sources": role_selected_sources(payload),
        }
    return snapshot


def write_source_selections(run_dir: Path, round_id: str, selections: dict[str, dict[str, Any]]) -> dict[str, Path]:
    written: dict[str, Path] = {}
    for role in SOURCE_SELECTION_ROLES:
        payload = selections.get(role)
        if not isinstance(payload, dict):
            continue
        path = source_selection_path(run_dir, round_id, role)
        write_json_file(path, payload)
        written[role] = path
    return written


def fetch_plan_input_snapshot(
    *,
    mission_path: Path,
    tasks_path: Path,
    run_dir: Path,
    round_id: str,
    selections: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "mission": file_snapshot(mission_path),
        "tasks": file_snapshot(tasks_path),
        "source_selections": source_selection_snapshot(run_dir, round_id, selections),
    }


def ensure_fetch_plan_inputs_match(*, run_dir: Path, round_id: str, plan: dict[str, Any]) -> None:
    snapshot = plan.get("input_snapshot") if isinstance(plan.get("input_snapshot"), dict) else {}
    issues: list[str] = []
    for key in ("mission", "tasks"):
        entry = snapshot.get(key) if isinstance(snapshot.get(key), dict) else {}
        path_text = maybe_text(entry.get("path"))
        expected_sha = maybe_text(entry.get("sha256"))
        if not path_text or not expected_sha:
            issues.append(f"fetch plan missing input_snapshot.{key}")
            continue
        current_path = Path(path_text).expanduser().resolve()
        if not current_path.exists():
            issues.append(f"{key} missing ({current_path})")
            continue
        if file_sha256(current_path) != expected_sha:
            issues.append(f"{current_path.name} changed ({current_path})")

    source_selections = snapshot.get("source_selections") if isinstance(snapshot.get("source_selections"), dict) else {}
    for role in SOURCE_SELECTION_ROLES:
        entry = source_selections.get(role) if isinstance(source_selections.get(role), dict) else {}
        path_text = maybe_text(entry.get("path"))
        expected_sha = maybe_text(entry.get("sha256"))
        expected_status = maybe_text(entry.get("status"))
        expected_selected = unique_texts(entry.get("selected_sources", []) if isinstance(entry.get("selected_sources"), list) else [])
        current_path = source_selection_path(run_dir, round_id, role) if not path_text else Path(path_text).expanduser().resolve()
        if not current_path.exists():
            issues.append(f"{role} source selection missing ({current_path})")
            continue
        current_payload_text = current_path.read_text(encoding="utf-8")
        current_sha = file_sha256(current_path)
        if expected_sha and current_sha != expected_sha:
            issues.append(f"{role} source selection changed ({current_path})")
        current_payload = json.loads(current_payload_text)
        current_status = maybe_text(current_payload.get("status")) if isinstance(current_payload, dict) else ""
        current_selected = role_selected_sources(current_payload if isinstance(current_payload, dict) else None)
        if expected_status and current_status != expected_status:
            issues.append(f"{role} source selection status changed ({expected_status} -> {current_status or '<empty>'})")
        if expected_selected != current_selected:
            issues.append(f"{role} selected_sources changed ({', '.join(current_selected) or '<empty>'})")
    if issues:
        raise RuntimeError("Fetch plan inputs changed since prepare-round. Rerun prepare-round. " + "; ".join(issues))


def build_fetch_plan(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    selections: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    imports = normalize_artifact_imports(mission)
    requests = normalize_source_requests(mission)
    warnings: list[dict[str, str]] = []
    steps: list[dict[str, Any]] = []
    step_index = 0
    imported_sources = {maybe_text(item.get("source_skill")) for item in imports}

    for item in imports:
        source_skill = maybe_text(item.get("source_skill"))
        role = maybe_text(item.get("role"))
        if source_skill not in role_selected_sources(selections.get(role)):
            continue
        source_artifact_path = Path(maybe_text(item.get("artifact_path"))).expanduser().resolve()
        if not source_artifact_path.exists():
            raise ValueError(f"Mission artifact import does not exist: {source_artifact_path}")
        step_index += 1
        config = source_config(source_skill)
        steps.append(
            {
                "step_id": f"step-{role}-{step_index:02d}-{sanitize_fragment(source_skill)}",
                "step_kind": "import",
                "role": role,
                "source_skill": source_skill,
                "normalizer_skill": maybe_text(config.get("normalizer_skill")),
                "task_ids": task_ids_for_role(tasks, role),
                "depends_on": [],
                "source_artifact_path": str(source_artifact_path),
                "artifact_path": str(planned_artifact_path(run_dir, round_id, source_skill, step_index)),
                "normalizer_args": normalizer_args_for(source_skill, item),
                "notes": [
                    f"Import prepared raw artifact for {source_skill} into the run raw store before normalization.",
                    *item.get("notes", []),
                ],
            }
        )

    for item in requests:
        source_skill = maybe_text(item.get("source_skill"))
        role = maybe_text(item.get("role"))
        if source_skill not in role_selected_sources(selections.get(role)):
            continue
        if source_skill in imported_sources:
            continue
        fetch_argv = item.get("fetch_argv") if isinstance(item.get("fetch_argv"), list) else []
        if not fetch_argv:
            warnings.append({"code": "missing-fetch-argv", "message": f"Selected source_skill={source_skill} has no fetch_argv and will be skipped."})
            continue
        step_index += 1
        config = source_config(source_skill)
        steps.append(
            {
                "step_id": f"step-{role}-{step_index:02d}-{sanitize_fragment(source_skill)}",
                "step_kind": "detached-fetch",
                "role": role,
                "source_skill": source_skill,
                "normalizer_skill": maybe_text(config.get("normalizer_skill")),
                "task_ids": task_ids_for_role(tasks, role),
                "depends_on": [],
                "artifact_path": str(planned_artifact_path(run_dir, round_id, source_skill, step_index, maybe_text(item.get("artifact_path")))),
                "artifact_capture": maybe_text(item.get("artifact_capture")) or "stdout-json",
                "fetch_argv": fetch_argv,
                "fetch_cwd": maybe_text(item.get("fetch_cwd")) or str(WORKSPACE_ROOT),
                "normalizer_args": normalizer_args_for(source_skill, item),
                "notes": [
                    f"Execute detached-fetch request for {source_skill} before normalization.",
                    *item.get("notes", []),
                ],
            }
        )

    for role in SOURCE_SELECTION_ROLES:
        selected = role_selected_sources(selections.get(role))
        if task_ids_for_role(tasks, role) and not selected:
            warnings.append({"code": "missing-role-source-selection", "message": f"No sources were selected for role={role}."})

    mission_path = (run_dir / "mission.json").resolve()
    tasks_path = (run_dir / "investigation" / f"round_tasks_{round_id}.json").resolve()
    plan_id = "fetch-plan-" + stable_hash(run_id, round_id, len(steps), mission_path, tasks_path)[:12]
    plan = {
        "plan_kind": "eco-council-fetch-plan",
        "schema_version": "1.1.0",
        "generated_at_utc": utc_now_iso(),
        "policy_profile": policy_profile_summary(mission),
        "effective_constraints": effective_constraints(mission),
        "run": {
            "run_id": run_id,
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region_label": maybe_text((mission.get("region") or {}).get("label")) if isinstance(mission.get("region"), dict) else "",
            "window": mission.get("window") if isinstance(mission.get("window"), dict) else {},
        },
        "plan_id": plan_id,
        "input_snapshot": fetch_plan_input_snapshot(
            mission_path=mission_path,
            tasks_path=tasks_path,
            run_dir=run_dir,
            round_id=round_id,
            selections=selections,
        ),
        "roles": {
            role: {
                "selected_sources": role_selected_sources(selections.get(role)),
                "task_ids": task_ids_for_role(tasks, role),
                "normalizer_skills": unique_texts([step.get("normalizer_skill") for step in steps if maybe_text(step.get("role")) == role]),
                "step_count": len([step for step in steps if maybe_text(step.get("role")) == role]),
                "selection_status": maybe_text((selections.get(role) or {}).get("status")) if isinstance(selections.get(role), dict) else "",
                "allowed_sources": (selections.get(role) or {}).get("allowed_sources", []) if isinstance(selections.get(role), dict) else [],
                "evidence_requirements": role_evidence_requirements(tasks, role),
                "family_memory": (selections.get(role) or {}).get("family_memory", []) if isinstance(selections.get(role), dict) else [],
                "source_selection_path": str(source_selection_path(run_dir, round_id, role)),
            }
            for role in SOURCE_SELECTION_ROLES
        },
        "steps": steps,
    }
    return plan, warnings


__all__ = [
    "build_fetch_plan",
    "ensure_fetch_plan_inputs_match",
    "fetch_plan_input_snapshot",
    "write_source_selections",
]
