from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .kernel.deliberation_plane import (
    load_council_decision_record,
    load_expert_report_record,
    load_final_publication_record,
    load_reporting_handoff_record,
    maybe_text,
)

ROLE_VALUES = ("sociologist", "environmentalist")


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def export_specs(round_id: str) -> list[dict[str, Any]]:
    return [
        {
            "object_kind": "reporting-handoff",
            "stage": "",
            "agent_role": "",
            "id_field": "handoff_id",
            "output_relative": f"reporting/reporting_handoff_{round_id}.json",
            "loader": load_reporting_handoff_record,
            "loader_kwargs": {},
        },
        {
            "object_kind": "council-decision",
            "stage": "draft",
            "agent_role": "",
            "id_field": "decision_id",
            "output_relative": f"reporting/council_decision_draft_{round_id}.json",
            "loader": load_council_decision_record,
            "loader_kwargs": {"decision_stage": "draft"},
        },
        {
            "object_kind": "council-decision",
            "stage": "canonical",
            "agent_role": "",
            "id_field": "decision_id",
            "output_relative": f"reporting/council_decision_{round_id}.json",
            "loader": load_council_decision_record,
            "loader_kwargs": {"decision_stage": "canonical"},
        },
        *[
            {
                "object_kind": "expert-report",
                "stage": "draft",
                "agent_role": role,
                "id_field": "report_id",
                "output_relative": (
                    f"reporting/expert_report_draft_{role}_{round_id}.json"
                ),
                "loader": load_expert_report_record,
                "loader_kwargs": {"report_stage": "draft", "agent_role": role},
            }
            for role in ROLE_VALUES
        ],
        *[
            {
                "object_kind": "expert-report",
                "stage": "canonical",
                "agent_role": role,
                "id_field": "report_id",
                "output_relative": f"reporting/expert_report_{role}_{round_id}.json",
                "loader": load_expert_report_record,
                "loader_kwargs": {
                    "report_stage": "canonical",
                    "agent_role": role,
                },
            }
            for role in ROLE_VALUES
        ],
        {
            "object_kind": "final-publication",
            "stage": "",
            "agent_role": "",
            "id_field": "publication_id",
            "output_relative": f"reporting/final_publication_{round_id}.json",
            "loader": load_final_publication_record,
            "loader_kwargs": {},
        },
    ]


def materialize_reporting_exports(
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
        output_path = (run_dir_path / spec["output_relative"]).resolve()
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
        payload = spec["loader"](run_dir_path, **loader_kwargs)
        payload_present = isinstance(payload, dict)
        export_entry = {
            "object_kind": maybe_text(spec.get("object_kind")),
            "stage": maybe_text(spec.get("stage")),
            "agent_role": maybe_text(spec.get("agent_role")),
            "output_path": str(output_path),
            "artifact_present_before": artifact_present_before,
            "artifact_present_after": artifact_present_before,
            "payload_present": payload_present,
            "identifier": "",
            "operation": "",
        }
        if payload_present:
            write_json_file(output_path, payload)
            materialized_count += 1
            export_entry["artifact_present_after"] = True
            export_entry["identifier"] = maybe_text(
                payload.get(maybe_text(spec.get("id_field")))
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
        "schema_version": "reporting-export-materialization-v1",
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


__all__ = ["ROLE_VALUES", "materialize_reporting_exports"]
