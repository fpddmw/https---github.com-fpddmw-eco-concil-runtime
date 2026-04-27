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
    source_anchor_argument,
    source_artifact_capture,
    source_config,
    source_normalizer_skill,
    source_runtime_default_args,
    source_runtime_output_arg,
    source_runtime_output_mode,
    source_selection_path,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json_file,
)
from .source_queue_history import import_execution_path, prior_round_ids
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
    if source_skill in {"fetch-gdelt-doc-search", "fetch-youtube-video-search"} and query_text:
        args.extend(["--query-text-override", query_text])
    if source_skill == "fetch-openaq" and source_mode:
        args.extend(["--source-mode", source_mode])
    if source_skill in {"fetch-gdelt-events", "fetch-gdelt-mentions", "fetch-gdelt-gkg"}:
        for payload_key, option_name in (
            ("max_rows_per_download", "--max-rows-per-download"),
            ("max_total_rows", "--max-total-rows"),
            ("artifact_ref_limit", "--artifact-ref-limit"),
            ("canonical_id_limit", "--canonical-id-limit"),
        ):
            value = payload.get(payload_key)
            if value in (None, ""):
                continue
            args.extend([option_name, str(value)])
    return args


def planned_artifact_path(run_dir: Path, round_id: str, source_skill: str, step_index: int, override_path: str = "") -> Path:
    if maybe_text(override_path):
        candidate = Path(maybe_text(override_path)).expanduser()
        if not candidate.is_absolute():
            candidate = run_dir / candidate
        return candidate.resolve()
    suffix = maybe_text(source_config(source_skill).get("default_suffix")) or ".json"
    return (run_dir / "raw" / round_id / f"{step_index:02d}-{sanitize_fragment(source_skill)}{suffix}").resolve()


def planned_artifact_dir(artifact_path: Path) -> Path:
    if artifact_path.suffix:
        return artifact_path.with_suffix("").resolve()
    return (artifact_path.parent / f"{artifact_path.name}.files").resolve()


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def argument_present(argv: list[str], option: str) -> bool:
    normalized = maybe_text(option)
    return any(maybe_text(item) == normalized for item in argv)


def append_runtime_fetch_defaults(
    source_skill: str,
    fetch_argv: list[str],
    *,
    artifact_path: Path,
    artifact_dir: Path,
    anchor_artifact_paths: list[str],
) -> list[str]:
    argv = [maybe_text(item) for item in fetch_argv if maybe_text(item)]
    for arg in source_runtime_default_args(source_skill):
        if argument_present(argv, arg):
            continue
        if arg.startswith("--no-") and argument_present(argv, "--" + arg[5:]):
            continue
        if arg.startswith("--") and argument_present(argv, "--no-" + arg[2:]):
            continue
        argv.append(arg)

    output_mode = source_runtime_output_mode(source_skill)
    output_arg = source_runtime_output_arg(source_skill)
    if output_arg and not argument_present(argv, output_arg):
        if output_mode == "file":
            argv.extend([output_arg, str(artifact_path)])
        elif output_mode == "dir":
            argv.extend([output_arg, str(artifact_dir)])

    anchor_arg = source_anchor_argument(source_skill)
    if anchor_arg and anchor_artifact_paths and not argument_present(argv, anchor_arg):
        argv.extend([anchor_arg, anchor_artifact_paths[0]])

    return argv


def selected_source_sequence(selection: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(selection, dict):
        return []
    family_plans = selection.get("family_plans") if isinstance(selection.get("family_plans"), list) else []
    sequence: list[dict[str, Any]] = []
    for family in family_plans:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        layer_plans = family.get("layer_plans") if isinstance(family.get("layer_plans"), list) else []
        ordered_layers = sorted(
            [layer_plan for layer_plan in layer_plans if isinstance(layer_plan, dict)],
            key=lambda layer_plan: (0 if maybe_text(layer_plan.get("tier")) == "l1" else 1, maybe_text(layer_plan.get("layer_id"))),
        )
        for layer_plan in ordered_layers:
            if layer_plan.get("selected") is not True:
                continue
            for source_skill in layer_plan.get("source_skills", []):
                if not maybe_text(source_skill):
                    continue
                sequence.append(
                    {
                        "family_id": family_id,
                        "layer_id": maybe_text(layer_plan.get("layer_id")),
                        "layer_plan": layer_plan,
                        "source_skill": maybe_text(source_skill),
                    }
                )
    return sequence


def latest_completed_status_for_source(
    run_dir: Path,
    *,
    current_round_id: str,
    source_skill: str,
    requested_round_id: str = "",
) -> dict[str, Any] | None:
    round_ids = [requested_round_id] if maybe_text(requested_round_id) else list(reversed(prior_round_ids(run_dir, current_round_id)))
    for observed_round_id in round_ids:
        if not maybe_text(observed_round_id):
            continue
        execution = load_json_if_exists(import_execution_path(run_dir, observed_round_id))
        statuses = execution.get("statuses", []) if isinstance(execution, dict) and isinstance(execution.get("statuses"), list) else []
        for status in reversed(statuses):
            if not isinstance(status, dict):
                continue
            if maybe_text(status.get("status")) != "completed":
                continue
            if maybe_text(status.get("source_skill")) != source_skill:
                continue
            artifact_path = maybe_text(status.get("artifact_path"))
            if not artifact_path:
                continue
            return {
                "round_id": observed_round_id,
                "source_skill": source_skill,
                "artifact_path": artifact_path,
                "step_id": maybe_text(status.get("step_id")),
            }
    return None


def resolved_anchor_context(
    *,
    run_dir: Path,
    current_round_id: str,
    source_skill: str,
    layer_plan: dict[str, Any],
    planned_steps_by_source: dict[str, list[dict[str, Any]]],
) -> tuple[list[str], list[str], list[dict[str, Any]], list[str]]:
    anchor_mode = maybe_text(layer_plan.get("anchor_mode")) or "none"
    refs_payload = layer_plan.get("anchor_refs") if isinstance(layer_plan.get("anchor_refs"), list) else []
    depends_on: list[str] = []
    anchor_paths: list[str] = []
    resolved_refs: list[dict[str, Any]] = []
    notes: list[str] = []

    if anchor_mode == "none" or not refs_payload:
        return depends_on, anchor_paths, resolved_refs, notes

    for raw_ref in refs_payload:
        if isinstance(raw_ref, dict):
            ref_source_skill = maybe_text(raw_ref.get("source_skill"))
            ref_round_id = maybe_text(raw_ref.get("round_id"))
            ref_scope = maybe_text(raw_ref.get("scope"))
            ref_artifact_path = maybe_text(raw_ref.get("artifact_path"))
        else:
            ref_source_skill = maybe_text(raw_ref)
            ref_round_id = ""
            ref_scope = ""
            ref_artifact_path = ""

        if ref_artifact_path:
            anchor_paths.append(ref_artifact_path)
            resolved_refs.append({"artifact_path": ref_artifact_path, "scope": ref_scope or anchor_mode})
            continue

        if anchor_mode in {"same-round-source", "current-round-source"} or ref_scope == "current-round":
            if not ref_source_skill:
                continue
            upstream_steps = planned_steps_by_source.get(ref_source_skill, [])
            if not upstream_steps:
                raise ValueError(f"{source_skill} requires same-round anchor from {ref_source_skill}, but no upstream step was planned.")
            upstream = upstream_steps[-1]
            depends_on.append(maybe_text(upstream.get("step_id")))
            anchor_path = maybe_text(upstream.get("artifact_path"))
            if anchor_path:
                anchor_paths.append(anchor_path)
            resolved_refs.append(
                {
                    "source_skill": ref_source_skill,
                    "scope": "current-round",
                    "step_id": maybe_text(upstream.get("step_id")),
                    "artifact_path": anchor_path,
                }
            )
            continue

        if anchor_mode == "prior_round_l1" or ref_scope == "prior-round":
            if not ref_source_skill:
                continue
            prior_status = latest_completed_status_for_source(
                run_dir,
                current_round_id=current_round_id,
                source_skill=ref_source_skill,
                requested_round_id=ref_round_id,
            )
            if prior_status is None:
                requested = f" round={ref_round_id}" if ref_round_id else ""
                raise ValueError(f"{source_skill} requires prior-round anchor from {ref_source_skill}{requested}, but no completed artifact was found.")
            anchor_path = maybe_text(prior_status.get("artifact_path"))
            if anchor_path:
                anchor_paths.append(anchor_path)
            resolved_refs.append(prior_status)
            notes.append(
                f"Use prior-round anchor from {ref_source_skill} ({maybe_text(prior_status.get('round_id'))})."
            )
            continue

    return unique_texts(depends_on), unique_texts(anchor_paths), resolved_refs, notes


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
    constraints = effective_constraints(mission)
    max_source_steps_per_round = int(constraints.get("max_source_steps_per_round") or 0)
    warnings: list[dict[str, str]] = []
    steps: list[dict[str, Any]] = []
    step_index = 0
    planned_steps_by_source: dict[str, list[dict[str, Any]]] = {}
    import_items_by_source: dict[str, list[dict[str, Any]]] = {}
    request_items_by_source: dict[str, list[dict[str, Any]]] = {}

    def next_step_index(*, role: str, source_skill: str, step_kind: str) -> int:
        candidate = step_index + 1
        if max_source_steps_per_round > 0 and candidate > max_source_steps_per_round:
            raise ValueError(
                "Fetch plan exceeds max_source_steps_per_round="
                f"{max_source_steps_per_round} while planning {step_kind} "
                f"for role={role} source_skill={source_skill}. Rerun prepare-round with fewer selected sources "
                "or raise the per-round source step budget."
            )
        return candidate

    for item in imports:
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        import_items_by_source.setdefault(source_skill, []).append(item)
    for item in requests:
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        request_items_by_source.setdefault(source_skill, []).append(item)

    for role in SOURCE_SELECTION_ROLES:
        selection = selections.get(role) if isinstance(selections.get(role), dict) else {}
        ordered_sources = selected_source_sequence(selection)
        if not ordered_sources:
            ordered_sources = [
                {
                    "family_id": "",
                    "layer_id": "",
                    "layer_plan": {},
                    "source_skill": source_skill,
                }
                for source_skill in role_selected_sources(selection)
            ]
        for source_entry in ordered_sources:
            source_skill = maybe_text(source_entry.get("source_skill"))
            layer_plan = source_entry.get("layer_plan") if isinstance(source_entry.get("layer_plan"), dict) else {}
            import_items = [item for item in import_items_by_source.get(source_skill, []) if maybe_text(item.get("role")) == role]
            request_items = [item for item in request_items_by_source.get(source_skill, []) if maybe_text(item.get("role")) == role]

            if import_items and request_items:
                warnings.append(
                    {
                        "code": "source-input-preferred-import",
                        "message": f"Selected source_skill={source_skill} provided both artifact_imports and source_requests; runtime will use artifact_imports.",
                    }
                )

            if import_items:
                for item in import_items:
                    source_artifact_path = Path(maybe_text(item.get("artifact_path"))).expanduser().resolve()
                    if not source_artifact_path.exists():
                        raise ValueError(f"Mission artifact import does not exist: {source_artifact_path}")
                    step_index = next_step_index(role=role, source_skill=source_skill, step_kind="import")
                    artifact_path = planned_artifact_path(run_dir, round_id, source_skill, step_index)
                    step = {
                        "step_id": f"step-{role}-{step_index:02d}-{sanitize_fragment(source_skill)}",
                        "step_kind": "import",
                        "role": role,
                        "source_skill": source_skill,
                        "family_id": maybe_text(source_entry.get("family_id")),
                        "layer_id": maybe_text(source_entry.get("layer_id")),
                        "normalizer_skill": source_normalizer_skill(source_skill),
                        "task_ids": task_ids_for_role(tasks, role),
                        "depends_on": [],
                        "source_artifact_path": str(source_artifact_path),
                        "artifact_path": str(artifact_path),
                        "normalizer_args": normalizer_args_for(source_skill, item),
                        "notes": [
                            f"Import prepared raw artifact for {source_skill} into the run raw store before normalization.",
                            *item.get("notes", []),
                        ],
                    }
                    steps.append(step)
                    planned_steps_by_source.setdefault(source_skill, []).append(step)
                continue

            if not request_items:
                warnings.append(
                    {
                        "code": "missing-source-input",
                        "message": f"Selected source_skill={source_skill} has no artifact_imports or source_requests entry and will be skipped.",
                    }
                )
                continue

            for item in request_items:
                fetch_argv = item.get("fetch_argv") if isinstance(item.get("fetch_argv"), list) else []
                if not fetch_argv:
                    warnings.append({"code": "missing-fetch-argv", "message": f"Selected source_skill={source_skill} has no fetch_argv and will be skipped."})
                    continue
                step_index = next_step_index(role=role, source_skill=source_skill, step_kind="detached-fetch")
                artifact_path = planned_artifact_path(run_dir, round_id, source_skill, step_index, maybe_text(item.get("artifact_path")))
                artifact_dir = planned_artifact_dir(artifact_path)
                depends_on, anchor_artifact_paths, resolved_anchor_refs, anchor_notes = resolved_anchor_context(
                    run_dir=run_dir,
                    current_round_id=round_id,
                    source_skill=source_skill,
                    layer_plan=layer_plan,
                    planned_steps_by_source=planned_steps_by_source,
                )
                step = {
                    "step_id": f"step-{role}-{step_index:02d}-{sanitize_fragment(source_skill)}",
                    "step_kind": "detached-fetch",
                    "role": role,
                    "source_skill": source_skill,
                    "family_id": maybe_text(source_entry.get("family_id")),
                    "layer_id": maybe_text(source_entry.get("layer_id")),
                    "normalizer_skill": source_normalizer_skill(source_skill),
                    "task_ids": task_ids_for_role(tasks, role),
                    "depends_on": depends_on,
                    "artifact_path": str(artifact_path),
                    "artifact_dir": str(artifact_dir),
                    "artifact_capture": maybe_text(item.get("artifact_capture")) or source_artifact_capture(source_skill),
                    "fetch_argv": append_runtime_fetch_defaults(
                        source_skill,
                        fetch_argv,
                        artifact_path=artifact_path,
                        artifact_dir=artifact_dir,
                        anchor_artifact_paths=anchor_artifact_paths,
                    ),
                    "fetch_cwd": maybe_text(item.get("fetch_cwd")) or str(WORKSPACE_ROOT),
                    "fetch_execution_policy": item.get("fetch_execution_policy", {}) if isinstance(item.get("fetch_execution_policy"), dict) else {},
                    "declared_side_effects": item.get("declared_side_effects", []) if isinstance(item.get("declared_side_effects"), list) else [],
                    "requested_side_effect_approvals": (
                        item.get("requested_side_effect_approvals", [])
                        if isinstance(item.get("requested_side_effect_approvals"), list)
                        else []
                    ),
                    "normalizer_args": normalizer_args_for(source_skill, item),
                    "anchor_mode": maybe_text(layer_plan.get("anchor_mode")) or "none",
                    "anchor_refs": resolved_anchor_refs,
                    "anchor_artifact_paths": anchor_artifact_paths,
                    "notes": [
                        f"Execute detached-fetch request for {source_skill} before normalization.",
                        *anchor_notes,
                        *item.get("notes", []),
                    ],
                }
                steps.append(step)
                planned_steps_by_source.setdefault(source_skill, []).append(step)

    for role in SOURCE_SELECTION_ROLES:
        selected = role_selected_sources(selections.get(role))
        if task_ids_for_role(tasks, role) and not selected:
            warnings.append({"code": "missing-role-source-selection", "message": f"No sources were selected for role={role}."})

    mission_path = (run_dir / "mission.json").resolve()
    tasks_path = (run_dir / "investigation" / f"round_tasks_{round_id}.json").resolve()
    plan_id = "fetch-plan-" + stable_hash(run_id, round_id, len(steps), mission_path, tasks_path)[:12]
    plan = {
        "plan_kind": "eco-council-fetch-plan",
        "schema_version": "1.3.0",
        "generated_at_utc": utc_now_iso(),
        "policy_profile": policy_profile_summary(mission),
        "effective_constraints": constraints,
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
