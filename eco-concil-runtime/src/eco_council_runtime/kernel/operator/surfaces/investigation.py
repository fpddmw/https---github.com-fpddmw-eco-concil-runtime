from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane import (
    build_falsification_probe_payload,
    build_moderator_action_payload,
    load_falsification_probe_records,
    load_falsification_probe_snapshot,
    load_moderator_action_records,
    load_moderator_action_snapshot,
    load_report_basis_freeze_record,
    load_round_readiness_assessment,
)
from eco_council_runtime.kernel.operator.surfaces.common import (
    maybe_text,
    orphaned_artifact_wrapper,
    resolve_path,
)


def load_next_actions_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    next_actions_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    next_actions_file = resolve_path(
        run_dir_path,
        next_actions_path,
        f"investigation/next_actions_{round_id}.json",
    )
    record_payload = load_moderator_action_records(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    snapshot_payload = load_moderator_action_snapshot(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if record_payload or isinstance(snapshot_payload, dict):
        payload = build_moderator_action_payload(
            record_payload,
            snapshot_payload=snapshot_payload if isinstance(snapshot_payload, dict) else None,
            run_id=run_id,
            round_id=round_id,
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-actions",
            "artifact_path": str(next_actions_file),
            "artifact_present": next_actions_file.exists(),
            "payload_present": True,
        }
    if next_actions_file.exists():
        return orphaned_artifact_wrapper(
            next_actions_file,
            source="orphaned-next-actions-artifact",
        )
    return {
        "payload": None,
        "source": "missing-next-actions",
        "artifact_path": str(next_actions_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_falsification_probe_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    probes_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    probes_file = resolve_path(
        run_dir_path,
        probes_path,
        f"investigation/falsification_probes_{round_id}.json",
    )
    record_payload = load_falsification_probe_records(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    snapshot_payload = load_falsification_probe_snapshot(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if record_payload or isinstance(snapshot_payload, dict):
        payload = build_falsification_probe_payload(
            record_payload,
            snapshot_payload=snapshot_payload if isinstance(snapshot_payload, dict) else None,
            run_id=run_id,
            round_id=round_id,
        )
        payload["action_source"] = (
            maybe_text(payload.get("action_source"))
            or "deliberation-plane-probes"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-probes",
            "artifact_path": str(probes_file),
            "artifact_present": probes_file.exists(),
            "payload_present": True,
        }
    if probes_file.exists():
        return orphaned_artifact_wrapper(
            probes_file,
            source="orphaned-falsification-probes-artifact",
        )
    return {
        "payload": None,
        "source": "missing-probes",
        "artifact_path": str(probes_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_round_readiness_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    readiness_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    readiness_file = resolve_path(
        run_dir_path,
        readiness_path,
        f"reporting/round_readiness_{round_id}.json",
    )
    readiness_payload = load_round_readiness_assessment(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(readiness_payload, dict):
        payload = dict(readiness_payload)
        payload["readiness_source"] = (
            maybe_text(payload.get("readiness_source"))
            or "deliberation-plane-readiness"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-readiness",
            "artifact_path": str(readiness_file),
            "artifact_present": readiness_file.exists(),
            "payload_present": True,
        }
    if readiness_file.exists():
        return orphaned_artifact_wrapper(
            readiness_file,
            source="orphaned-round-readiness-artifact",
        )
    return {
        "payload": None,
        "source": "missing-readiness",
        "artifact_path": str(readiness_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_report_basis_freeze_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    report_basis_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    report_basis_file = resolve_path(
        run_dir_path,
        report_basis_path,
        f"report_basis/frozen_report_basis_{round_id}.json",
    )
    report_basis_payload = load_report_basis_freeze_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(report_basis_payload, dict):
        payload = dict(report_basis_payload)
        payload["report_basis_source"] = (
            maybe_text(payload.get("report_basis_source"))
            or "deliberation-plane-report-basis-freeze"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-report-basis-freeze",
            "artifact_path": str(report_basis_file),
            "artifact_present": report_basis_file.exists(),
            "payload_present": True,
        }
    if report_basis_file.exists():
        return orphaned_artifact_wrapper(
            report_basis_file,
            source="orphaned-report-basis-freeze-artifact",
        )
    return {
        "payload": None,
        "source": "missing-report_basis",
        "artifact_path": str(report_basis_file),
        "artifact_present": False,
        "payload_present": False,
    }


__all__ = [
    "load_falsification_probe_wrapper",
    "load_next_actions_wrapper",
    "load_report_basis_freeze_wrapper",
    "load_round_readiness_wrapper",
]
