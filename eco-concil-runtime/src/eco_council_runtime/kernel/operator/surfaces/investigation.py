from __future__ import annotations

import json
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


def _load_json_object(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _report_basis_payload_wrapper(
    payload: dict[str, Any],
    *,
    source: str,
    artifact_path: Path,
) -> dict[str, Any]:
    wrapped_payload = dict(payload)
    wrapped_payload["report_basis_source"] = (
        maybe_text(wrapped_payload.get("report_basis_source")) or source
    )
    return {
        "payload": wrapped_payload,
        "source": source,
        "artifact_path": str(artifact_path),
        "artifact_present": artifact_path.exists(),
        "payload_present": True,
    }


def _artifact_matches_report_basis_record(
    *,
    artifact_payload: dict[str, Any],
    record_payload: dict[str, Any],
) -> bool:
    artifact_basis_id = maybe_text(artifact_payload.get("basis_id"))
    record_basis_id = maybe_text(record_payload.get("basis_id"))
    if artifact_basis_id and record_basis_id and artifact_basis_id != record_basis_id:
        return False
    return True


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
        return _report_basis_payload_wrapper(
            report_basis_payload,
            source="deliberation-plane-report-basis-freeze",
            artifact_path=report_basis_file,
        )
    explicit_report_basis_path = bool(maybe_text(report_basis_path))
    if explicit_report_basis_path and report_basis_file.exists():
        artifact_payload = _load_json_object(report_basis_file)
        if isinstance(artifact_payload, dict):
            artifact_run_id = maybe_text(artifact_payload.get("run_id"))
            artifact_round_id = maybe_text(artifact_payload.get("round_id"))
            if artifact_run_id and artifact_run_id != run_id:
                return orphaned_artifact_wrapper(
                    report_basis_file,
                    source="mismatched-report-basis-freeze-artifact",
                )
            if artifact_round_id and artifact_round_id != round_id:
                cross_round_payload = load_report_basis_freeze_record(
                    run_dir_path,
                    run_id=run_id,
                    round_id=artifact_round_id,
                )
                if (
                    isinstance(cross_round_payload, dict)
                    and _artifact_matches_report_basis_record(
                        artifact_payload=artifact_payload,
                        record_payload=cross_round_payload,
                    )
                ):
                    payload = dict(cross_round_payload)
                    payload["requested_round_id"] = round_id
                    return _report_basis_payload_wrapper(
                        payload,
                        source="deliberation-plane-report-basis-freeze-cross-round",
                        artifact_path=report_basis_file,
                    )
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
