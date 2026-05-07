from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import write_json
from eco_council_runtime.kernel.core.paths import (
    agent_entry_gate_path,
    controller_state_path,
    mission_scaffold_path,
    orchestration_plan_path,
    report_basis_gate_path,
)
from eco_council_runtime.kernel.execution.executor_common import maybe_text
from eco_council_runtime.kernel.execution.governed_execution_controller_state import (
    refresh_controller_payload,
)
from eco_council_runtime.kernel.planes.deliberation_plane import (
    store_runtime_control_freeze_record,
)


def governed_execution_artifact_paths(run_dir: Path, round_id: str) -> dict[str, str]:
    return {
        "agent_entry_gate_path": str(agent_entry_gate_path(run_dir, round_id).resolve()),
        "board_summary_path": str((run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()),
        "board_brief_path": str((run_dir / "board" / f"board_brief_{round_id}.md").resolve()),
        "mission_scaffold_path": str(mission_scaffold_path(run_dir, round_id).resolve()),
        "next_actions_path": str((run_dir / "investigation" / f"next_actions_{round_id}.json").resolve()),
        "probes_path": str((run_dir / "investigation" / f"falsification_probes_{round_id}.json").resolve()),
        "readiness_path": str((run_dir / "reporting" / f"round_readiness_{round_id}.json").resolve()),
        "orchestration_plan_path": str(orchestration_plan_path(run_dir, round_id).resolve()),
        "report_basis_gate_path": str(report_basis_gate_path(run_dir, round_id).resolve()),
        "report_basis_freeze_path": str((run_dir / "report_basis" / f"frozen_report_basis_{round_id}.json").resolve()),
        "controller_state_path": str(controller_state_path(run_dir, round_id).resolve()),
    }


def persist_controller_state(
    run_dir: Path,
    round_id: str,
    controller_payload: dict[str, Any],
    *,
    gate_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    controller_payload["artifacts"] = governed_execution_artifact_paths(run_dir, round_id)
    refreshed_payload = refresh_controller_payload(controller_payload)
    write_json(controller_state_path(run_dir, round_id), refreshed_payload)
    store_runtime_control_freeze_record(
        run_dir,
        run_id=maybe_text(refreshed_payload.get("run_id")),
        round_id=round_id,
        controller_snapshot=refreshed_payload,
        gate_snapshot=gate_payload,
        artifact_paths=refreshed_payload.get("artifacts", {})
        if isinstance(refreshed_payload.get("artifacts"), dict)
        else {},
    )
    return refreshed_payload


__all__ = [
    "governed_execution_artifact_paths",
    "persist_controller_state",
]
