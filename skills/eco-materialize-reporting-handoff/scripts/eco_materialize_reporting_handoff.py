#!/usr/bin/env python3
"""Materialize a compact reporting handoff from promotion-stage artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-materialize-reporting-handoff"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_reporting_handoff_record,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_promotion_basis_wrapper,
    load_round_readiness_wrapper,
    load_supervisor_state_wrapper,
)
from eco_council_runtime.reporting_status import (  # noqa: E402
    reporting_blocker_summaries,
    reporting_gate_state,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def excerpt_text(text: str, limit: int = 280) -> str:
    normalized = maybe_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def build_key_findings(selected_coverages: list[dict[str, Any]], max_findings: int) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, coverage in enumerate(selected_coverages[: max(1, max_findings)], start=1):
        if not isinstance(coverage, dict):
            continue
        claim_id = maybe_text(coverage.get("claim_id"))
        coverage_id = maybe_text(coverage.get("coverage_id"))
        readiness = maybe_text(coverage.get("readiness")) or "unknown"
        coverage_score = maybe_number(coverage.get("coverage_score")) or 0.0
        support_count = int(coverage.get("support_link_count") or 0)
        contradiction_count = int(coverage.get("contradiction_link_count") or 0)
        summary = (
            f"Claim {claim_id or coverage_id or index} is {readiness} with coverage_score={coverage_score:.2f}, "
            f"support_links={support_count}, contradiction_links={contradiction_count}."
        )
        findings.append(
            {
                "finding_id": f"reporting-finding-{index:03d}",
                "title": f"Evidence basis for {claim_id or coverage_id or 'round target'}",
                "summary": summary,
                "coverage_id": coverage_id,
                "claim_id": claim_id,
                "readiness": readiness,
                "coverage_score": coverage_score,
                "evidence_refs": unique_texts(coverage.get("evidence_refs", []) if isinstance(coverage.get("evidence_refs"), list) else []),
            }
        )
    return findings


def build_open_risks(
    *,
    promotion_basis: dict[str, Any],
    supervisor_state: dict[str, Any],
    readiness: dict[str, Any],
) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    remaining_risks = promotion_basis.get("remaining_risks", []) if isinstance(promotion_basis.get("remaining_risks"), list) else []
    for index, risk in enumerate(remaining_risks, start=1):
        if not isinstance(risk, dict):
            continue
        results.append(
            {
                "risk_id": maybe_text(risk.get("action_id")) or f"promotion-risk-{index:03d}",
                "risk_type": maybe_text(risk.get("action_kind")) or "investigation",
                "priority": maybe_text(risk.get("priority")) or "medium",
                "summary": maybe_text(risk.get("reason")) or "Promotion basis still carries unresolved risk.",
            }
        )
    operator_notes = supervisor_state.get("operator_notes", []) if isinstance(supervisor_state.get("operator_notes"), list) else []
    for index, note in enumerate(operator_notes, start=1):
        text = maybe_text(note)
        if not text:
            continue
        results.append(
            {
                "risk_id": f"operator-note-{index:03d}",
                "risk_type": "operator-note",
                "priority": "medium",
                "summary": text,
            }
        )
    gate_reasons = readiness.get("gate_reasons", []) if isinstance(readiness.get("gate_reasons"), list) else []
    for index, reason in enumerate(gate_reasons, start=1):
        text = maybe_text(reason)
        if not text:
            continue
        results.append(
            {
                "risk_id": f"gate-reason-{index:03d}",
                "risk_type": "gate",
                "priority": "high",
                "summary": text,
            }
        )
    deduped: dict[str, dict[str, str]] = {}
    for item in results:
        key = "|".join([item.get("risk_type", ""), item.get("summary", "")])
        deduped.setdefault(key, item)
    return list(deduped.values())[:6]


def build_recommended_next_actions(supervisor_state: dict[str, Any]) -> list[dict[str, str]]:
    top_actions = supervisor_state.get("top_actions", []) if isinstance(supervisor_state.get("top_actions"), list) else []
    recommendations: list[dict[str, str]] = []
    for action in top_actions[:4]:
        if not isinstance(action, dict):
            continue
        objective = maybe_text(action.get("objective"))
        if not objective:
            continue
        assigned_role = maybe_text(action.get("assigned_role")) or "unspecified"
        action_kind = maybe_text(action.get("action_kind")) or "follow-up"
        priority = maybe_text(action.get("priority")) or "medium"
        recommendations.append(
            {
                "assigned_role": assigned_role,
                "objective": objective,
                "reason": f"Supervisor ranked this as the next {action_kind} follow-up (priority={priority}).",
            }
        )
    return recommendations


def recommended_sections(reporting_ready: bool) -> list[str]:
    if reporting_ready:
        return ["executive-summary", "role-reports", "evidence-basis", "residual-risks", "audit-trace"]
    return ["gating-status", "open-risks", "next-round-plan", "audit-trace"]


def materialize_reporting_handoff_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    promotion_path: str,
    readiness_path: str,
    board_brief_path: str,
    supervisor_state_path: str,
    output_path: str,
    max_findings: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    promotion_file = resolve_path(run_dir_path, promotion_path, f"promotion/promoted_evidence_basis_{round_id}.json")
    readiness_file = resolve_path(run_dir_path, readiness_path, f"reporting/round_readiness_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    supervisor_file = resolve_path(run_dir_path, supervisor_state_path, f"runtime/supervisor_state_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/reporting_handoff_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    promotion_context = load_promotion_basis_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        promotion_path=promotion_path,
    )
    promotion_payload = (
        promotion_context.get("payload")
        if isinstance(promotion_context.get("payload"), dict)
        else None
    )
    if not isinstance(promotion_payload, dict):
        warnings.append(
            {
                "code": "missing-promotion-basis",
                "message": (
                    "No promotion basis DB record was found for "
                    f"{promotion_file}; artifact exists but is orphaned from the deliberation plane."
                    if bool(promotion_context.get("artifact_present"))
                    else (
                        "No promotion basis artifact or DB record was found "
                        f"at {promotion_file}."
                    )
                ),
            }
        )
        promotion_basis = {"promotion_status": "withheld", "selected_coverages": [], "selected_evidence_refs": [], "remaining_risks": []}
    else:
        promotion_basis = promotion_payload
    readiness_context = load_round_readiness_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        readiness_path=readiness_path,
    )
    readiness_payload = (
        readiness_context.get("payload")
        if isinstance(readiness_context.get("payload"), dict)
        else None
    )
    if not isinstance(readiness_payload, dict):
        warnings.append(
            {
                "code": "missing-readiness",
                "message": (
                    "No round readiness DB assessment was found for "
                    f"{readiness_file}; artifact exists but is orphaned from the deliberation plane."
                    if bool(readiness_context.get("artifact_present"))
                    else (
                        "No round readiness artifact or DB assessment was found "
                        f"at {readiness_file}."
                    )
                ),
            }
        )
        readiness = {"readiness_status": "blocked", "gate_reasons": []}
    else:
        readiness = readiness_payload
    supervisor_context = load_supervisor_state_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        supervisor_state_path=supervisor_state_path,
    )
    supervisor_state_payload = (
        supervisor_context.get("payload")
        if isinstance(supervisor_context.get("payload"), dict)
        else None
    )
    if not isinstance(supervisor_state_payload, dict):
        warnings.append(
            {
                "code": "missing-supervisor-state",
                "message": (
                    "No supervisor DB snapshot was found for "
                    f"{supervisor_file}; artifact exists but is orphaned from the phase-2 control plane."
                    if bool(supervisor_context.get("artifact_present"))
                    else (
                        "No supervisor snapshot artifact or DB record was found "
                        f"at {supervisor_file}."
                    )
                ),
            }
        )
        supervisor_state = {"supervisor_status": "unavailable", "top_actions": [], "operator_notes": []}
    else:
        supervisor_state = supervisor_state_payload
    board_brief_text = load_text_if_exists(board_brief_file)
    contract_fields = reporting_contract_fields_from_payload(
        promotion_payload,
        fallback_payload=readiness_payload,
        observed_inputs_overrides={
            "promotion_artifact_present": bool(
                promotion_context.get("artifact_present")
            ),
            "promotion_present": bool(promotion_context.get("payload_present")),
            "readiness_artifact_present": bool(
                readiness_context.get("artifact_present")
            ),
            "readiness_present": bool(readiness_context.get("payload_present")),
            "board_brief_artifact_present": board_brief_file.exists(),
            "board_brief_present": bool(maybe_text(board_brief_text)),
            "supervisor_state_artifact_present": supervisor_file.exists(),
            "supervisor_state_present": bool(supervisor_context.get("payload_present")),
        },
        field_overrides={
            "promotion_source": maybe_text(promotion_context.get("source"))
            or "missing-promotion",
            "readiness_source": (
                maybe_text(readiness_context.get("source"))
                or "missing-readiness"
            ),
            "board_brief_source": (
                "board-brief-artifact"
                if board_brief_file.exists()
                else "missing-board-brief"
            ),
            "supervisor_state_source": (
                maybe_text(supervisor_context.get("source"))
                or "missing-supervisor-state"
            ),
        },
    )

    gate_state = reporting_gate_state(
        promotion_status=maybe_text(promotion_basis.get("promotion_status")) or "withheld",
        readiness_status=maybe_text(readiness.get("readiness_status")) or "blocked",
        supervisor_status=maybe_text(supervisor_state.get("supervisor_status")) or "unavailable",
        require_supervisor=True,
    )
    promotion_status = maybe_text(gate_state.get("promotion_status")) or "withheld"
    readiness_status = maybe_text(gate_state.get("readiness_status")) or "blocked"
    supervisor_status = maybe_text(gate_state.get("supervisor_status")) or "unavailable"
    handoff_status = maybe_text(gate_state.get("handoff_status")) or "investigation-open"
    reporting_ready = bool(gate_state.get("reporting_ready"))
    reporting_blockers = unique_texts(
        gate_state.get("reporting_blockers", [])
        if isinstance(gate_state.get("reporting_blockers"), list)
        else []
    )
    reporting_blocker_hints = reporting_blocker_summaries(reporting_blockers)

    selected_coverages = promotion_basis.get("selected_coverages", []) if isinstance(promotion_basis.get("selected_coverages"), list) else []
    key_findings = build_key_findings(selected_coverages, max_findings)
    open_risks = build_open_risks(promotion_basis=promotion_basis, supervisor_state=supervisor_state, readiness=readiness)
    next_actions = build_recommended_next_actions(supervisor_state)
    board_excerpt = excerpt_text(board_brief_text)
    handoff_id = "reporting-handoff-" + stable_hash(run_id, round_id, handoff_status, promotion_status)[:12]

    wrapper = {
        "schema_version": "e1.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "handoff_id": handoff_id,
        "handoff_status": handoff_status,
        "reporting_ready": reporting_ready,
        "reporting_blockers": reporting_blockers,
        "promotion_status": promotion_status,
        "readiness_status": readiness_status,
        "supervisor_status": supervisor_status,
        "promotion_path": str(promotion_file),
        "readiness_path": str(readiness_file),
        "board_brief_path": str(board_brief_file),
        "supervisor_state_path": str(supervisor_file),
        **contract_fields,
        "promoted_basis_id": maybe_text(promotion_basis.get("basis_id")),
        "basis_selection_mode": maybe_text(promotion_basis.get("basis_selection_mode")),
        "selected_basis_object_ids": unique_texts(
            promotion_basis.get("selected_basis_object_ids", [])
            if isinstance(promotion_basis.get("selected_basis_object_ids"), list)
            else []
        ),
        "supporting_proposal_ids": unique_texts(
            promotion_basis.get("supporting_proposal_ids", [])
            if isinstance(promotion_basis.get("supporting_proposal_ids"), list)
            else []
        ),
        "rejected_proposal_ids": unique_texts(
            promotion_basis.get("rejected_proposal_ids", [])
            if isinstance(promotion_basis.get("rejected_proposal_ids"), list)
            else []
        ),
        "supporting_opinion_ids": unique_texts(
            promotion_basis.get("supporting_opinion_ids", [])
            if isinstance(promotion_basis.get("supporting_opinion_ids"), list)
            else []
        ),
        "rejected_opinion_ids": unique_texts(
            promotion_basis.get("rejected_opinion_ids", [])
            if isinstance(promotion_basis.get("rejected_opinion_ids"), list)
            else []
        ),
        "promotion_resolution_mode": maybe_text(
            promotion_basis.get("promotion_resolution_mode")
        ),
        "promotion_resolution_reasons": (
            promotion_basis.get("promotion_resolution_reasons", [])
            if isinstance(promotion_basis.get("promotion_resolution_reasons"), list)
            else []
        ),
        "council_input_counts": (
            promotion_basis.get("council_input_counts", {})
            if isinstance(promotion_basis.get("council_input_counts"), dict)
            else {}
        ),
        "selected_evidence_refs": unique_texts(promotion_basis.get("selected_evidence_refs", []) if isinstance(promotion_basis.get("selected_evidence_refs"), list) else []),
        "board_brief_excerpt": board_excerpt,
        "key_findings": key_findings,
        "open_risks": open_risks,
        "recommended_next_actions": next_actions,
        "recommended_sections": recommended_sections(reporting_ready),
        "report_targets": ["expert-report-draft", "council-decision-draft"] if reporting_ready else ["expert-report-draft", "another-round-decision"],
    }
    stored_payload = store_reporting_handoff_record(
        run_dir_path,
        handoff_payload=wrapper,
        artifact_path=str(output_file),
    )
    handoff_id = maybe_text(stored_payload.get("handoff_id")) or handoff_id
    write_json_file(output_file, stored_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "handoff_id": handoff_id,
            "handoff_status": handoff_status,
            "reporting_ready": reporting_ready,
            "finding_count": len(key_findings),
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "promotion_source": maybe_text(contract_fields.get("promotion_source")),
            "readiness_source": maybe_text(contract_fields.get("readiness_source")),
            "supervisor_state_source": maybe_text(contract_fields.get("supervisor_state_source")),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, handoff_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [handoff_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [handoff_id],
            "evidence_refs": artifact_refs,
            "gap_hints": unique_texts(
                [item.get("summary", "") for item in open_risks[:3] if maybe_text(item.get("summary"))]
                + (reporting_blocker_hints if not reporting_ready else [])
            )[:3] if not reporting_ready else [],
            "challenge_hints": [item.get("summary", "") for item in open_risks[:2] if maybe_text(item.get("summary"))],
            "suggested_next_skills": ["eco-draft-expert-report", "eco-draft-council-decision"] if reporting_ready else ["eco-draft-expert-report", "eco-draft-council-decision", "eco-submit-council-proposal", "eco-submit-readiness-opinion", "eco-propose-next-actions", "eco-open-falsification-probe"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a compact reporting handoff from promotion-stage artifacts.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--promotion-path", default="")
    parser.add_argument("--readiness-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--supervisor-state-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-findings", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_reporting_handoff_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        promotion_path=args.promotion_path,
        readiness_path=args.readiness_path,
        board_brief_path=args.board_brief_path,
        supervisor_state_path=args.supervisor_state_path,
        output_path=args.output_path,
        max_findings=args.max_findings,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
