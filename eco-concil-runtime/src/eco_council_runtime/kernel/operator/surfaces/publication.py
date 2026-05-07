from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane import (
    load_council_decision_record,
    load_expert_report_record,
    load_final_publication_record,
    load_reporting_handoff_record,
)
from eco_council_runtime.kernel.operator.surfaces.reporting import (
    enrich_reporting_record_payload,
)
from eco_council_runtime.kernel.operator.surfaces.common import (
    maybe_text,
    orphaned_artifact_wrapper,
    resolve_path,
)


def load_reporting_handoff_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    reporting_handoff_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    handoff_file = resolve_path(
        run_dir_path,
        reporting_handoff_path,
        f"reporting/reporting_handoff_{round_id}.json",
    )
    handoff_payload = load_reporting_handoff_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(handoff_payload, dict):
        payload = enrich_reporting_record_payload(handoff_payload)
        return {
            "payload": payload,
            "source": "deliberation-plane-reporting-handoff",
            "artifact_path": str(handoff_file),
            "artifact_present": handoff_file.exists(),
            "payload_present": True,
        }
    if handoff_file.exists():
        return orphaned_artifact_wrapper(
            handoff_file,
            source="orphaned-reporting-handoff-artifact",
        )
    return {
        "payload": None,
        "source": "missing-reporting-handoff",
        "artifact_path": str(handoff_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_council_decision_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    decision_stage: str = "canonical",
    decision_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    normalized_stage = (
        "draft" if maybe_text(decision_stage) == "draft" else "canonical"
    )
    default_relative = (
        f"reporting/council_decision_draft_{round_id}.json"
        if normalized_stage == "draft"
        else f"reporting/council_decision_{round_id}.json"
    )
    decision_file = resolve_path(
        run_dir_path,
        decision_path,
        default_relative,
    )
    record_payload = load_council_decision_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        decision_stage=normalized_stage,
    )
    if isinstance(record_payload, dict):
        payload = enrich_reporting_record_payload(record_payload)
        return {
            "payload": payload,
            "source": (
                "deliberation-plane-council-decision-draft"
                if normalized_stage == "draft"
                else "deliberation-plane-council-decision"
            ),
            "artifact_path": str(decision_file),
            "artifact_present": decision_file.exists(),
            "payload_present": True,
        }
    if decision_file.exists():
        return orphaned_artifact_wrapper(
            decision_file,
            source=(
                "orphaned-council-decision-draft-artifact"
                if normalized_stage == "draft"
                else "orphaned-council-decision-artifact"
            ),
        )
    return {
        "payload": None,
        "source": (
            "missing-council-decision-draft"
            if normalized_stage == "draft"
            else "missing-council-decision"
        ),
        "artifact_path": str(decision_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_expert_report_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    agent_role: str,
    report_stage: str = "canonical",
    report_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    normalized_role = maybe_text(agent_role)
    normalized_stage = "draft" if maybe_text(report_stage) == "draft" else "canonical"
    default_relative = (
        f"reporting/expert_report_draft_{normalized_role}_{round_id}.json"
        if normalized_stage == "draft"
        else f"reporting/expert_report_{normalized_role}_{round_id}.json"
    )
    report_file = resolve_path(
        run_dir_path,
        report_path,
        default_relative,
    )
    record_payload = load_expert_report_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        report_stage=normalized_stage,
        agent_role=normalized_role,
    )
    if isinstance(record_payload, dict):
        payload = enrich_reporting_record_payload(record_payload)
        return {
            "payload": payload,
            "source": (
                "deliberation-plane-expert-report-draft"
                if normalized_stage == "draft"
                else "deliberation-plane-expert-report"
            ),
            "artifact_path": str(report_file),
            "artifact_present": report_file.exists(),
            "payload_present": True,
        }
    if report_file.exists():
        return orphaned_artifact_wrapper(
            report_file,
            source=(
                "orphaned-expert-report-draft-artifact"
                if normalized_stage == "draft"
                else "orphaned-expert-report-artifact"
            ),
        )
    return {
        "payload": None,
        "source": (
            "missing-expert-report-draft"
            if normalized_stage == "draft"
            else f"missing-{normalized_role}-report"
        ),
        "artifact_path": str(report_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_final_publication_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    output_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    publication_file = resolve_path(
        run_dir_path,
        output_path,
        f"reporting/final_publication_{round_id}.json",
    )
    publication_payload = load_final_publication_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(publication_payload, dict):
        return {
            "payload": publication_payload,
            "source": "deliberation-plane-final-publication",
            "artifact_path": str(publication_file),
            "artifact_present": publication_file.exists(),
            "payload_present": True,
        }
    if publication_file.exists():
        return orphaned_artifact_wrapper(
            publication_file,
            source="orphaned-final-publication-artifact",
        )
    return {
        "payload": None,
        "source": "missing-final-publication",
        "artifact_path": str(publication_file),
        "artifact_present": False,
        "payload_present": False,
    }


__all__ = [
    "load_council_decision_wrapper",
    "load_expert_report_wrapper",
    "load_final_publication_wrapper",
    "load_reporting_handoff_wrapper",
]
