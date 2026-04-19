from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .deliberation_plane import (
    build_falsification_probe_payload,
    build_moderator_action_payload,
    load_council_decision_record,
    load_expert_report_record,
    load_falsification_probe_records,
    load_falsification_probe_snapshot,
    load_final_publication_record,
    load_moderator_action_records,
    load_moderator_action_snapshot,
    load_promotion_basis_record,
    load_reporting_handoff_record,
    load_round_readiness_assessment,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


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
    artifact_payload = load_json_if_exists(next_actions_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("action_source"))
            or "next-actions-artifact",
            "artifact_path": str(next_actions_file),
            "artifact_present": True,
            "payload_present": True,
        }
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
    artifact_payload = load_json_if_exists(probes_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("action_source"))
            or "falsification-probes-artifact",
            "artifact_path": str(probes_file),
            "artifact_present": True,
            "payload_present": True,
        }
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
    artifact_payload = load_json_if_exists(readiness_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("readiness_source"))
            or "round-readiness-artifact",
            "artifact_path": str(readiness_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-readiness",
        "artifact_path": str(readiness_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_promotion_basis_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    promotion_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    promotion_file = resolve_path(
        run_dir_path,
        promotion_path,
        f"promotion/promoted_evidence_basis_{round_id}.json",
    )
    promotion_payload = load_promotion_basis_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(promotion_payload, dict):
        payload = dict(promotion_payload)
        payload["promotion_source"] = (
            maybe_text(payload.get("promotion_source"))
            or "deliberation-plane-promotion-basis"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-promotion-basis",
            "artifact_path": str(promotion_file),
            "artifact_present": promotion_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(promotion_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("promotion_source"))
            or "promotion-artifact",
            "artifact_path": str(promotion_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-promotion",
        "artifact_path": str(promotion_file),
        "artifact_present": False,
        "payload_present": False,
    }


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
        return {
            "payload": handoff_payload,
            "source": "deliberation-plane-reporting-handoff",
            "artifact_path": str(handoff_file),
            "artifact_present": handoff_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(handoff_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": "reporting-handoff-artifact",
            "artifact_path": str(handoff_file),
            "artifact_present": True,
            "payload_present": True,
        }
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
        payload = dict(record_payload)
        payload.pop("record_id", None)
        payload.pop("decision_stage", None)
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
    artifact_payload = load_json_if_exists(decision_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": (
                "council-decision-draft-artifact"
                if normalized_stage == "draft"
                else "council-decision-artifact"
            ),
            "artifact_path": str(decision_file),
            "artifact_present": True,
            "payload_present": True,
        }
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
        payload = dict(record_payload)
        payload.pop("record_id", None)
        payload.pop("report_stage", None)
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
    artifact_payload = load_json_if_exists(report_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": (
                "expert-report-draft-artifact"
                if normalized_stage == "draft"
                else "expert-report-artifact"
            ),
            "artifact_path": str(report_file),
            "artifact_present": True,
            "payload_present": True,
        }
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
    artifact_payload = load_json_if_exists(publication_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": "final-publication-artifact",
            "artifact_path": str(publication_file),
            "artifact_present": True,
            "payload_present": True,
        }
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
    "load_falsification_probe_wrapper",
    "load_next_actions_wrapper",
    "load_promotion_basis_wrapper",
    "load_reporting_handoff_wrapper",
    "load_round_readiness_wrapper",
]
