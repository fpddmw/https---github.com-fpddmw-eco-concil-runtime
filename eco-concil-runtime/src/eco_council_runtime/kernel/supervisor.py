from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .controller import run_phase2_round
from .executor import maybe_text, utc_now_iso
from .ledger import append_ledger_event
from .manifest import load_json_if_exists, write_json
from .paths import supervisor_state_path


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def top_actions(next_actions: dict[str, Any]) -> list[dict[str, str]]:
    ranked_actions = next_actions.get("ranked_actions", []) if isinstance(next_actions.get("ranked_actions"), list) else []
    results: list[dict[str, str]] = []
    for action in ranked_actions[:3]:
        if not isinstance(action, dict):
            continue
        results.append(
            {
                "action_id": maybe_text(action.get("action_id")),
                "action_kind": maybe_text(action.get("action_kind")),
                "assigned_role": maybe_text(action.get("assigned_role")),
                "priority": maybe_text(action.get("priority")),
                "objective": maybe_text(action.get("objective")),
            }
        )
    return results


def operator_notes(*, promotion_status: str, gate_status: str, gate_reasons: list[Any], top_action_rows: list[dict[str, str]]) -> list[str]:
    if promotion_status == "promoted":
        return [
            "Round promotion succeeded and the evidence basis is now ready for downstream reporting.",
            "No blocking board or probe objects remain in the current controller snapshot.",
        ]
    notes = [f"Promotion is withheld because gate={gate_status}."]
    notes.extend(maybe_text(reason) for reason in gate_reasons if maybe_text(reason))
    if top_action_rows:
        top_action_kind = top_action_rows[0].get("action_kind") or "unspecified"
        notes.append(f"Highest-priority follow-up remains {top_action_kind}.")
    return notes[:4]


def supervise_round(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    started_at = utc_now_iso()
    controller_result = run_phase2_round(run_dir, run_id=run_id, round_id=round_id)
    controller = controller_result.get("controller", {}) if isinstance(controller_result.get("controller"), dict) else {}
    artifacts = controller.get("artifacts", {}) if isinstance(controller.get("artifacts"), dict) else {}
    next_actions_path = maybe_text(artifacts.get("next_actions_path"))
    next_actions = load_json_if_exists(Path(next_actions_path)) if next_actions_path else {}
    next_actions = next_actions or {}
    top_action_rows = top_actions(next_actions)
    gate_reasons = controller.get("gate_reasons", []) if isinstance(controller.get("gate_reasons"), list) else []
    promotion_status = maybe_text(controller.get("promotion_status")) or "withheld"
    gate_status = maybe_text(controller.get("gate_status")) or "freeze-withheld"
    supervisor_status = "ready-for-reporting" if promotion_status == "promoted" else "hold-investigation-open"
    finished_at = utc_now_iso()

    payload = {
        "schema_version": "runtime-supervisor-v1",
        "generated_at_utc": finished_at,
        "run_id": run_id,
        "round_id": round_id,
        "supervisor_status": supervisor_status,
        "readiness_status": maybe_text(controller.get("readiness_status")) or "blocked",
        "gate_status": gate_status,
        "promotion_status": promotion_status,
        "controller_path": artifacts.get("controller_state_path", ""),
        "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
        "promotion_basis_path": artifacts.get("promotion_basis_path", ""),
        "recommended_next_skills": controller.get("recommended_next_skills", []),
        "top_actions": top_action_rows,
        "operator_notes": operator_notes(
            promotion_status=promotion_status,
            gate_status=gate_status,
            gate_reasons=gate_reasons,
            top_action_rows=top_action_rows,
        ),
    }
    output_file = supervisor_state_path(run_dir, round_id)
    write_json(output_file, payload)

    event_id = "runtimeevt-" + stable_hash(run_id, round_id, "supervisor", started_at, finished_at)[:12]
    append_ledger_event(
        run_dir,
        {
            "schema_version": "runtime-event-v1",
            "event_id": event_id,
            "event_type": "supervisor",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "status": "completed",
            "supervisor_status": supervisor_status,
            "readiness_status": payload["readiness_status"],
            "gate_status": gate_status,
            "promotion_status": promotion_status,
            "supervisor_path": str(output_file),
        },
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "supervisor_status": supervisor_status,
            "supervisor_path": str(output_file),
            "promotion_status": promotion_status,
        },
        "supervisor": payload,
        "controller": controller,
    }