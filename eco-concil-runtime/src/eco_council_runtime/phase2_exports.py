from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .kernel.deliberation_plane import maybe_text
from .kernel.phase2_state_surfaces import (
    load_falsification_probe_wrapper,
    load_next_actions_wrapper,
    load_orchestration_plan_wrapper,
    load_report_basis_freeze_wrapper,
    load_round_readiness_wrapper,
    load_supervisor_state_wrapper,
)


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def export_specs(round_id: str) -> list[dict[str, Any]]:
    return [
        {
            "export_kind": "orchestration-plan",
            "output_relative": f"runtime/orchestration_plan_{round_id}.json",
            "loader": load_orchestration_plan_wrapper,
            "loader_kwargs": {},
            "identifier_fields": ("plan_id", "round_id"),
        },
        {
            "export_kind": "next-actions",
            "output_relative": f"investigation/next_actions_{round_id}.json",
            "loader": load_next_actions_wrapper,
            "loader_kwargs": {},
            "identifier_fields": ("snapshot_id", "round_id"),
        },
        {
            "export_kind": "falsification-probes",
            "output_relative": f"investigation/falsification_probes_{round_id}.json",
            "loader": load_falsification_probe_wrapper,
            "loader_kwargs": {},
            "identifier_fields": ("snapshot_id", "round_id"),
        },
        {
            "export_kind": "round-readiness",
            "output_relative": f"reporting/round_readiness_{round_id}.json",
            "loader": load_round_readiness_wrapper,
            "loader_kwargs": {},
            "identifier_fields": ("readiness_id", "round_id"),
        },
        {
            "export_kind": "report-basis-freeze",
            "output_relative": f"report_basis/frozen_report_basis_{round_id}.json",
            "loader": load_report_basis_freeze_wrapper,
            "loader_kwargs": {},
            "identifier_fields": ("basis_id", "round_id"),
        },
        {
            "export_kind": "supervisor-state",
            "output_relative": f"runtime/supervisor_state_{round_id}.json",
            "loader": load_supervisor_state_wrapper,
            "loader_kwargs": {},
            "identifier_fields": ("freeze_id", "round_id"),
        },
    ]


def first_identifier(
    payload: dict[str, Any],
    fields: tuple[str, ...],
) -> str:
    for field in fields:
        value = maybe_text(payload.get(field))
        if value:
            return value
    return ""


def materialize_phase2_exports(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    exports: list[dict[str, Any]] = []
    materialized_count = 0
    missing_db_object_count = 0
    orphaned_artifact_count = 0

    for spec in export_specs(round_id):
        output_path = (run_dir_path / maybe_text(spec.get("output_relative"))).resolve()
        artifact_present_before = output_path.exists()
        loader_kwargs = {
            "run_id": run_id,
            "round_id": round_id,
            **(
                spec.get("loader_kwargs", {})
                if isinstance(spec.get("loader_kwargs"), dict)
                else {}
            ),
        }
        context = spec["loader"](run_dir_path, **loader_kwargs)
        payload = (
            context.get("payload")
            if isinstance(context.get("payload"), dict)
            else None
        )
        payload_present = isinstance(payload, dict)
        export_entry = {
            "export_kind": maybe_text(spec.get("export_kind")),
            "output_path": str(output_path),
            "artifact_present_before": artifact_present_before,
            "artifact_present_after": artifact_present_before,
            "payload_present": payload_present,
            "identifier": "",
            "source": maybe_text(context.get("source")),
            "operation": "",
        }
        if payload_present:
            write_json_file(output_path, payload)
            materialized_count += 1
            export_entry["artifact_present_after"] = True
            export_entry["identifier"] = first_identifier(
                payload,
                tuple(spec.get("identifier_fields", ())),
            )
            export_entry["operation"] = "materialized"
        else:
            export_entry["operation"] = (
                "orphaned-artifact"
                if artifact_present_before
                else "missing-db-object"
            )
            if artifact_present_before:
                orphaned_artifact_count += 1
            else:
                missing_db_object_count += 1
        exports.append(export_entry)

    return {
        "schema_version": "phase2-export-materialization-v1",
        "status": "completed",
        "run_id": run_id,
        "round_id": round_id,
        "summary": {
            "run_dir": str(run_dir_path),
            "materialized_export_count": materialized_count,
            "missing_db_object_count": missing_db_object_count,
            "orphaned_artifact_count": orphaned_artifact_count,
            "target_export_count": len(exports),
        },
        "exports": exports,
    }


__all__ = ["materialize_phase2_exports"]
