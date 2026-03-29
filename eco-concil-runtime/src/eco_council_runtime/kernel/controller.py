from __future__ import annotations

from pathlib import Path
from typing import Any

from .executor import maybe_text, new_runtime_event_id, run_skill, utc_now_iso
from .gate import apply_promotion_gate
from .ledger import append_ledger_event
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists, write_json
from .paths import controller_state_path, ensure_runtime_dirs, orchestration_plan_path, promotion_gate_path
from .registry import write_registry

PLANNER_SKILL_NAME = "eco-plan-round-orchestration"
STATIC_PHASE2_STAGES: list[tuple[str, str]] = [
    ("board-summary", "eco-summarize-board-state"),
    ("board-brief", "eco-materialize-board-brief"),
    ("next-actions", "eco-propose-next-actions"),
    ("falsification-probes", "eco-open-falsification-probe"),
    ("round-readiness", "eco-summarize-round-readiness"),
]


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


def phase2_artifact_paths(run_dir: Path, round_id: str) -> dict[str, str]:
    return {
        "board_summary_path": str((run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()),
        "board_brief_path": str((run_dir / "board" / f"board_brief_{round_id}.md").resolve()),
        "next_actions_path": str((run_dir / "investigation" / f"next_actions_{round_id}.json").resolve()),
        "probes_path": str((run_dir / "investigation" / f"falsification_probes_{round_id}.json").resolve()),
        "readiness_path": str((run_dir / "reporting" / f"round_readiness_{round_id}.json").resolve()),
        "orchestration_plan_path": str(orchestration_plan_path(run_dir, round_id)),
        "promotion_gate_path": str(promotion_gate_path(run_dir, round_id)),
        "promotion_basis_path": str((run_dir / "promotion" / f"promoted_evidence_basis_{round_id}.json").resolve()),
        "controller_state_path": str(controller_state_path(run_dir, round_id)),
    }


def summarize_skill_step(stage_name: str, result: dict[str, Any]) -> dict[str, Any]:
    skill_payload = result.get("skill_payload", {}) if isinstance(result.get("skill_payload"), dict) else {}
    event = result.get("event", {}) if isinstance(result.get("event"), dict) else {}
    return {
        "stage": stage_name,
        "kind": "skill",
        "skill_name": maybe_text(result.get("summary", {}).get("skill_name") if isinstance(result.get("summary"), dict) else ""),
        "status": maybe_text(event.get("status")) or "completed",
        "event_id": maybe_text(result.get("summary", {}).get("event_id") if isinstance(result.get("summary"), dict) else ""),
        "receipt_id": maybe_text(result.get("summary", {}).get("receipt_id") if isinstance(result.get("summary"), dict) else ""),
        "artifact_refs": skill_payload.get("artifact_refs", []) if isinstance(skill_payload.get("artifact_refs"), list) else [],
        "canonical_ids": skill_payload.get("canonical_ids", []) if isinstance(skill_payload.get("canonical_ids"), list) else [],
    }


def default_execution_queue() -> list[dict[str, Any]]:
    return [
        {"stage_name": stage_name, "skill_name": skill_name, "skill_args": [], "assigned_role_hint": "moderator", "reason": "Fallback static phase-2 step."}
        for stage_name, skill_name in STATIC_PHASE2_STAGES
    ]


def default_post_gate_steps() -> list[dict[str, Any]]:
    return [
        {
            "stage_name": "promotion-basis",
            "skill_name": "eco-promote-evidence-basis",
            "skill_args": [],
            "assigned_role_hint": "moderator",
            "reason": "Fallback post-gate promotion basis write.",
        }
    ]


def normalized_planned_steps(entries: Any) -> list[dict[str, Any]]:
    if not isinstance(entries, list):
        return []
    results: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        skill_name = maybe_text(item.get("skill_name"))
        if not skill_name:
            continue
        raw_skill_args = item.get("skill_args", [])
        skill_args = [maybe_text(value) for value in raw_skill_args if maybe_text(value)] if isinstance(raw_skill_args, list) else []
        results.append(
            {
                "stage_name": maybe_text(item.get("stage_name") or item.get("stage") or skill_name),
                "skill_name": skill_name,
                "skill_args": skill_args,
                "assigned_role_hint": maybe_text(item.get("assigned_role_hint")),
                "reason": maybe_text(item.get("reason")),
                "expected_output_path": maybe_text(item.get("expected_output_path")),
            }
        )
    return results


def resolve_plan_path(run_dir: Path, round_id: str, plan_payload: dict[str, Any]) -> str:
    summary = plan_payload.get("summary", {}) if isinstance(plan_payload.get("summary"), dict) else {}
    output_path = maybe_text(summary.get("output_path")) or maybe_text(plan_payload.get("output_path"))
    if output_path:
        return output_path
    return str(orchestration_plan_path(run_dir, round_id))


def planning_bundle(run_dir: Path, round_id: str, planner_result: dict[str, Any]) -> dict[str, Any]:
    planner_wrapper = planner_result.get("skill_payload", {}) if isinstance(planner_result.get("skill_payload"), dict) else {}
    plan_payload = load_json_if_exists(Path(resolve_plan_path(run_dir, round_id, planner_wrapper))) or {}
    execution_queue = normalized_planned_steps(plan_payload.get("execution_queue"))
    post_gate_steps = normalized_planned_steps(plan_payload.get("post_gate_steps"))
    if not execution_queue:
        execution_queue = default_execution_queue()
    if not post_gate_steps:
        post_gate_steps = default_post_gate_steps()
    planning_mode = "planner-backed" if normalized_planned_steps(plan_payload.get("execution_queue")) else "fallback-static"
    fallback_path = plan_payload.get("fallback_path", []) if isinstance(plan_payload.get("fallback_path"), list) else []
    return {
        "plan_id": maybe_text(plan_payload.get("plan_id")),
        "plan_path": resolve_plan_path(run_dir, round_id, plan_payload),
        "planning_status": maybe_text(plan_payload.get("planning_status")) or "ready-for-controller",
        "planning_mode": planning_mode,
        "planner_skill_name": PLANNER_SKILL_NAME,
        "probe_stage_included": bool(plan_payload.get("probe_stage_included")),
        "assigned_role_hints": plan_payload.get("assigned_role_hints", []) if isinstance(plan_payload.get("assigned_role_hints"), list) else [],
        "execution_queue": execution_queue,
        "post_gate_steps": post_gate_steps,
        "stop_conditions": plan_payload.get("stop_conditions", []) if isinstance(plan_payload.get("stop_conditions"), list) else [],
        "fallback_path": fallback_path,
        "fallback_suggested_next_skills": unique_texts(
            [skill_name for row in fallback_path if isinstance(row, dict) for skill_name in row.get("suggested_next_skills", [])]
        ),
    }


def apply_planning_metadata(step: dict[str, Any], planned_step: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(step)
    assigned_role_hint = maybe_text(planned_step.get("assigned_role_hint"))
    planner_reason = maybe_text(planned_step.get("reason"))
    expected_output_path = maybe_text(planned_step.get("expected_output_path"))
    if assigned_role_hint:
        enriched["assigned_role_hint"] = assigned_role_hint
    if planner_reason:
        enriched["planner_reason"] = planner_reason
    if expected_output_path:
        enriched["expected_output_path"] = expected_output_path
    return enriched


def run_phase2_round(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    return run_phase2_round_with_contract_mode(run_dir, run_id=run_id, round_id=round_id, contract_mode="warn")


def run_phase2_round_with_contract_mode(run_dir: Path, *, run_id: str, round_id: str, contract_mode: str) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    write_registry(run_dir)
    init_run_manifest(run_dir, run_id)
    init_round_cursor(run_dir, run_id)

    started_at = utc_now_iso()
    steps: list[dict[str, Any]] = []
    planner_result = run_skill(run_dir, run_id=run_id, round_id=round_id, skill_name=PLANNER_SKILL_NAME, skill_args=[], contract_mode=contract_mode)
    steps.append(summarize_skill_step("orchestration-planner", planner_result))
    planning = planning_bundle(run_dir, round_id, planner_result)

    for planned_step in planning["execution_queue"]:
        result = run_skill(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=planned_step["skill_name"],
            skill_args=planned_step["skill_args"],
            contract_mode=contract_mode,
        )
        steps.append(apply_planning_metadata(summarize_skill_step(planned_step["stage_name"], result), planned_step))

    gate_payload = apply_promotion_gate(run_dir, run_id=run_id, round_id=round_id)
    gate_event_id = new_runtime_event_id("runtimeevt", run_id, round_id, "promotion-gate", started_at, gate_payload.get("generated_at_utc"))
    append_ledger_event(
        run_dir,
        {
            "schema_version": "runtime-event-v3",
            "event_id": gate_event_id,
            "event_type": "promotion-gate",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": started_at,
            "completed_at_utc": gate_payload.get("generated_at_utc"),
            "status": "completed",
            "contract_mode": contract_mode,
            "planning_mode": planning["planning_mode"],
            "plan_id": planning["plan_id"],
            "plan_path": planning["plan_path"],
            "gate_status": gate_payload.get("gate_status"),
            "readiness_status": gate_payload.get("readiness_status"),
            "promote_allowed": bool(gate_payload.get("promote_allowed")),
            "gate_path": gate_payload.get("output_path"),
        },
    )
    steps.append(
        {
            "stage": "promotion-gate",
            "kind": "gate",
            "status": "completed",
            "event_id": gate_event_id,
            "gate_status": gate_payload.get("gate_status"),
            "readiness_status": gate_payload.get("readiness_status"),
            "promote_allowed": bool(gate_payload.get("promote_allowed")),
            "artifact_path": gate_payload.get("output_path"),
            "planning_mode": planning["planning_mode"],
        }
    )

    promotion_result: dict[str, Any] | None = None
    for planned_step in planning["post_gate_steps"]:
        result = run_skill(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=planned_step["skill_name"],
            skill_args=planned_step["skill_args"],
            contract_mode=contract_mode,
        )
        steps.append(apply_planning_metadata(summarize_skill_step(planned_step["stage_name"], result), planned_step))
        if maybe_text(planned_step.get("stage_name")) == "promotion-basis" or maybe_text(planned_step.get("skill_name")) == "eco-promote-evidence-basis":
            promotion_result = result

    if not isinstance(promotion_result, dict):
        promotion_result = run_skill(run_dir, run_id=run_id, round_id=round_id, skill_name="eco-promote-evidence-basis", skill_args=[], contract_mode=contract_mode)
        steps.append(summarize_skill_step("promotion-basis", promotion_result))

    promotion_payload = promotion_result.get("skill_payload", {}) if isinstance(promotion_result.get("skill_payload"), dict) else {}
    promotion_summary = promotion_payload.get("summary", {}) if isinstance(promotion_payload.get("summary"), dict) else {}
    finished_at = utc_now_iso()
    artifacts = phase2_artifact_paths(run_dir, round_id)
    promotion_status = maybe_text(promotion_summary.get("promotion_status")) or "withheld"
    if promotion_status == "promoted":
        recommended_next_skills = ["eco-materialize-reporting-handoff", "eco-draft-council-decision"]
    else:
        recommended_next_skills = unique_texts(
            (gate_payload.get("recommended_next_skills", []) if isinstance(gate_payload.get("recommended_next_skills"), list) else [])
            + planning["fallback_suggested_next_skills"]
        )
    controller_payload = {
        "schema_version": "runtime-controller-v2",
        "generated_at_utc": finished_at,
        "run_id": run_id,
        "round_id": round_id,
        "controller_status": "completed",
        "contract_mode": contract_mode,
        "planning_mode": planning["planning_mode"],
        "readiness_status": maybe_text(gate_payload.get("readiness_status")) or "blocked",
        "gate_status": maybe_text(gate_payload.get("gate_status")) or "freeze-withheld",
        "promotion_status": promotion_status,
        "recommended_next_skills": recommended_next_skills,
        "gate_reasons": gate_payload.get("gate_reasons", []),
        "planning": {
            "plan_id": planning["plan_id"],
            "plan_path": planning["plan_path"],
            "planning_status": planning["planning_status"],
            "planner_skill_name": planning["planner_skill_name"],
            "probe_stage_included": planning["probe_stage_included"],
            "assigned_role_hints": planning["assigned_role_hints"],
            "planned_skill_count": len(planning["execution_queue"]) + len(planning["post_gate_steps"]),
            "stop_conditions": planning["stop_conditions"],
            "fallback_path": planning["fallback_path"],
        },
        "steps": steps,
        "artifacts": artifacts,
    }
    write_json(controller_state_path(run_dir, round_id), controller_payload)

    controller_event_id = new_runtime_event_id("runtimeevt", run_id, round_id, "round-controller", started_at, finished_at)
    append_ledger_event(
        run_dir,
        {
            "schema_version": "runtime-event-v3",
            "event_id": controller_event_id,
            "event_type": "round-controller",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "status": "completed",
            "contract_mode": contract_mode,
            "planning_mode": planning["planning_mode"],
            "plan_id": planning["plan_id"],
            "plan_path": planning["plan_path"],
            "readiness_status": controller_payload["readiness_status"],
            "gate_status": controller_payload["gate_status"],
            "promotion_status": controller_payload["promotion_status"],
            "controller_path": artifacts["controller_state_path"],
            "step_count": len(steps),
        },
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "controller_path": artifacts["controller_state_path"],
            "planning_mode": planning["planning_mode"],
            "plan_path": planning["plan_path"],
            "readiness_status": controller_payload["readiness_status"],
            "gate_status": controller_payload["gate_status"],
            "promotion_status": controller_payload["promotion_status"],
        },
        "controller": controller_payload,
        "gate": gate_payload,
    }