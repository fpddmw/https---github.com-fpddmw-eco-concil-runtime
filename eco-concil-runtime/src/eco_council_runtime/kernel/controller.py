from __future__ import annotations

from pathlib import Path
from typing import Any

from .executor import maybe_text, new_runtime_event_id, run_skill, utc_now_iso
from .gate import apply_promotion_gate
from .ledger import append_ledger_event
from .manifest import init_round_cursor, init_run_manifest, write_json
from .paths import controller_state_path, ensure_runtime_dirs, promotion_gate_path
from .registry import write_registry

PHASE2_STAGES: list[tuple[str, str]] = [
    ("board-summary", "eco-summarize-board-state"),
    ("board-brief", "eco-materialize-board-brief"),
    ("next-actions", "eco-propose-next-actions"),
    ("falsification-probes", "eco-open-falsification-probe"),
    ("round-readiness", "eco-summarize-round-readiness"),
]
def phase2_artifact_paths(run_dir: Path, round_id: str) -> dict[str, str]:
    return {
        "board_summary_path": str((run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()),
        "board_brief_path": str((run_dir / "board" / f"board_brief_{round_id}.md").resolve()),
        "next_actions_path": str((run_dir / "investigation" / f"next_actions_{round_id}.json").resolve()),
        "probes_path": str((run_dir / "investigation" / f"falsification_probes_{round_id}.json").resolve()),
        "readiness_path": str((run_dir / "reporting" / f"round_readiness_{round_id}.json").resolve()),
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


def run_phase2_round(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    write_registry(run_dir)
    init_run_manifest(run_dir, run_id)
    init_round_cursor(run_dir, run_id)

    started_at = utc_now_iso()
    steps: list[dict[str, Any]] = []
    for stage_name, skill_name in PHASE2_STAGES:
        result = run_skill(run_dir, run_id=run_id, round_id=round_id, skill_name=skill_name, skill_args=[])
        steps.append(summarize_skill_step(stage_name, result))

    gate_payload = apply_promotion_gate(run_dir, run_id=run_id, round_id=round_id)
    gate_event_id = new_runtime_event_id("runtimeevt", run_id, round_id, "promotion-gate", started_at, gate_payload.get("generated_at_utc"))
    append_ledger_event(
        run_dir,
        {
            "schema_version": "runtime-event-v2",
            "event_id": gate_event_id,
            "event_type": "promotion-gate",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": started_at,
            "completed_at_utc": gate_payload.get("generated_at_utc"),
            "status": "completed",
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
        }
    )

    promotion_result = run_skill(run_dir, run_id=run_id, round_id=round_id, skill_name="eco-promote-evidence-basis", skill_args=[])
    steps.append(summarize_skill_step("promotion-basis", promotion_result))

    promotion_payload = promotion_result.get("skill_payload", {}) if isinstance(promotion_result.get("skill_payload"), dict) else {}
    promotion_summary = promotion_payload.get("summary", {}) if isinstance(promotion_payload.get("summary"), dict) else {}
    finished_at = utc_now_iso()
    artifacts = phase2_artifact_paths(run_dir, round_id)
    controller_payload = {
        "schema_version": "runtime-controller-v1",
        "generated_at_utc": finished_at,
        "run_id": run_id,
        "round_id": round_id,
        "controller_status": "completed",
        "readiness_status": maybe_text(gate_payload.get("readiness_status")) or "blocked",
        "gate_status": maybe_text(gate_payload.get("gate_status")) or "freeze-withheld",
        "promotion_status": maybe_text(promotion_summary.get("promotion_status")) or "withheld",
        "recommended_next_skills": gate_payload.get("recommended_next_skills", []),
        "gate_reasons": gate_payload.get("gate_reasons", []),
        "steps": steps,
        "artifacts": artifacts,
    }
    write_json(controller_state_path(run_dir, round_id), controller_payload)

    controller_event_id = new_runtime_event_id("runtimeevt", run_id, round_id, "round-controller", started_at, finished_at)
    append_ledger_event(
        run_dir,
        {
            "schema_version": "runtime-event-v2",
            "event_id": controller_event_id,
            "event_type": "round-controller",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "status": "completed",
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
            "readiness_status": controller_payload["readiness_status"],
            "gate_status": controller_payload["gate_status"],
            "promotion_status": controller_payload["promotion_status"],
        },
        "controller": controller_payload,
        "gate": gate_payload,
    }