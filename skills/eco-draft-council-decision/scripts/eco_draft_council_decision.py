#!/usr/bin/env python3
"""Draft a compact council decision object from the reporting handoff."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-draft-council-decision"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_council_decision_record,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_promotion_basis_wrapper,
    load_reporting_handoff_wrapper,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def decision_summary(
    *,
    handoff_status: str,
    promotion_status: str,
    key_findings: list[dict[str, Any]],
    open_risks: list[dict[str, Any]],
) -> str:
    if handoff_status == "ready-for-reporting" and promotion_status == "promoted":
        if key_findings:
            lead = maybe_text(key_findings[0].get("summary"))
            return f"Round is ready for formal reporting and decision finalization. Lead basis: {lead}"
        return "Round is ready for formal reporting and decision finalization."
    if open_risks:
        reasons = "; ".join(maybe_text(item.get("summary")) for item in open_risks[:3] if maybe_text(item.get("summary")))
        return f"Another round is required before finalization because {reasons}."
    return "Another round is required before finalization because promotion remains withheld."


def reason_codes(
    handoff_status: str,
    promotion_status: str,
    open_risks: list[dict[str, Any]],
    rejected_proposal_ids: list[str],
) -> list[str]:
    codes: list[str] = []
    if handoff_status != "ready-for-reporting":
        codes.append("reporting-handoff-not-ready")
    if promotion_status != "promoted":
        codes.append("promotion-withheld")
    if rejected_proposal_ids:
        codes.append("council-veto")
    for item in open_risks:
        risk_type = maybe_text(item.get("risk_type"))
        if risk_type == "gate":
            codes.append("gate-blocking")
        elif risk_type == "operator-note":
            codes.append("operator-follow-up")
        elif risk_type == "proposal-veto":
            codes.append("council-veto")
        else:
            codes.append("investigation-open")
    return unique_texts(codes)


def draft_council_decision_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    reporting_handoff_path: str,
    promotion_path: str,
    output_path: str,
    max_actions: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    handoff_file = resolve_path(run_dir_path, reporting_handoff_path, f"reporting/reporting_handoff_{round_id}.json")
    promotion_file = resolve_path(run_dir_path, promotion_path, f"promotion/promoted_evidence_basis_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/council_decision_draft_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    handoff_context = load_reporting_handoff_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        reporting_handoff_path=reporting_handoff_path,
    )
    handoff_payload = (
        handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else None
    )
    if not isinstance(handoff_payload, dict):
        warnings.append(
            {
                "code": "missing-reporting-handoff",
                "message": (
                    "No reporting handoff artifact or DB record was found "
                    f"at {handoff_file}."
                ),
            }
        )
        handoff = {
            "handoff_status": "pending-more-investigation",
            "promotion_status": "withheld",
            "key_findings": [],
            "open_risks": [],
            "recommended_next_actions": [],
        }
    else:
        handoff = handoff_payload
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
        promotion_basis = {"selected_evidence_refs": [], "basis_id": ""}
    else:
        promotion_basis = promotion_payload
    contract_fields = reporting_contract_fields_from_payload(
        handoff_payload,
        fallback_payload=promotion_payload,
        observed_inputs_overrides={
            "reporting_handoff_artifact_present": bool(
                handoff_context.get("artifact_present")
            ),
            "reporting_handoff_present": bool(handoff_context.get("payload_present")),
            "promotion_artifact_present": bool(
                promotion_context.get("artifact_present")
            ),
            "promotion_present": bool(promotion_context.get("payload_present")),
        },
        field_overrides={
            "reporting_handoff_source": maybe_text(handoff_context.get("source"))
            or "missing-reporting-handoff",
            "promotion_source": maybe_text(promotion_context.get("source"))
            or "missing-promotion",
        },
    )

    handoff_status = maybe_text(handoff.get("handoff_status")) or "pending-more-investigation"
    promotion_status = maybe_text(handoff.get("promotion_status")) or maybe_text(promotion_basis.get("promotion_status")) or "withheld"
    key_findings = handoff.get("key_findings", []) if isinstance(handoff.get("key_findings"), list) else []
    open_risks = handoff.get("open_risks", []) if isinstance(handoff.get("open_risks"), list) else []
    recommended_next_actions = handoff.get("recommended_next_actions", []) if isinstance(handoff.get("recommended_next_actions"), list) else []
    rejected_proposal_ids = unique_texts(
        handoff.get("rejected_proposal_ids", [])
        if isinstance(handoff.get("rejected_proposal_ids"), list)
        else promotion_basis.get("rejected_proposal_ids", [])
        if isinstance(promotion_basis.get("rejected_proposal_ids"), list)
        else []
    )
    moderator_status = "finalize" if handoff_status == "ready-for-reporting" and promotion_status == "promoted" else "continue"
    publication_readiness = "ready" if moderator_status == "finalize" else "hold"
    decision_id = "council-decision-" + stable_hash(run_id, round_id, moderator_status, publication_readiness)[:12]
    summary_text = decision_summary(
        handoff_status=handoff_status,
        promotion_status=promotion_status,
        key_findings=key_findings,
        open_risks=open_risks,
    )

    wrapper = {
        "schema_version": "e1.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "decision_id": decision_id,
        "moderator_status": moderator_status,
        "publication_readiness": publication_readiness,
        "next_round_required": moderator_status != "finalize",
        "decision_summary": summary_text,
        **contract_fields,
        "promoted_basis_id": maybe_text(handoff.get("promoted_basis_id"))
        or maybe_text(promotion_basis.get("basis_id")),
        "basis_selection_mode": maybe_text(handoff.get("basis_selection_mode"))
        or maybe_text(promotion_basis.get("basis_selection_mode")),
        "selected_basis_object_ids": unique_texts(
            handoff.get("selected_basis_object_ids", [])
            if isinstance(handoff.get("selected_basis_object_ids"), list)
            else promotion_basis.get("selected_basis_object_ids", [])
            if isinstance(promotion_basis.get("selected_basis_object_ids"), list)
            else []
        ),
        "supporting_proposal_ids": unique_texts(
            handoff.get("supporting_proposal_ids", [])
            if isinstance(handoff.get("supporting_proposal_ids"), list)
            else promotion_basis.get("supporting_proposal_ids", [])
            if isinstance(promotion_basis.get("supporting_proposal_ids"), list)
            else []
        ),
        "rejected_proposal_ids": rejected_proposal_ids,
        "supporting_opinion_ids": unique_texts(
            handoff.get("supporting_opinion_ids", [])
            if isinstance(handoff.get("supporting_opinion_ids"), list)
            else promotion_basis.get("supporting_opinion_ids", [])
            if isinstance(promotion_basis.get("supporting_opinion_ids"), list)
            else []
        ),
        "rejected_opinion_ids": unique_texts(
            handoff.get("rejected_opinion_ids", [])
            if isinstance(handoff.get("rejected_opinion_ids"), list)
            else promotion_basis.get("rejected_opinion_ids", [])
            if isinstance(promotion_basis.get("rejected_opinion_ids"), list)
            else []
        ),
        "promotion_resolution_mode": maybe_text(
            handoff.get("promotion_resolution_mode")
        )
        or maybe_text(promotion_basis.get("promotion_resolution_mode")),
        "promotion_resolution_reasons": (
            handoff.get("promotion_resolution_reasons", [])
            if isinstance(handoff.get("promotion_resolution_reasons"), list)
            else promotion_basis.get("promotion_resolution_reasons", [])
            if isinstance(promotion_basis.get("promotion_resolution_reasons"), list)
            else []
        ),
        "council_input_counts": (
            handoff.get("council_input_counts", {})
            if isinstance(handoff.get("council_input_counts"), dict)
            else promotion_basis.get("council_input_counts", {})
            if isinstance(promotion_basis.get("council_input_counts"), dict)
            else {}
        ),
        "decision_gating": {
            "reason_codes": reason_codes(
                handoff_status,
                promotion_status,
                open_risks,
                rejected_proposal_ids,
            ),
            "reasons": [maybe_text(item.get("summary")) for item in open_risks[:4] if maybe_text(item.get("summary"))],
            "open_risk_count": len(open_risks),
        },
        "key_findings": key_findings[:3],
        "recommended_next_actions": [item for item in recommended_next_actions[: max(1, max_actions)] if isinstance(item, dict)],
        "selected_evidence_refs": unique_texts(
            handoff.get("selected_evidence_refs", []) if isinstance(handoff.get("selected_evidence_refs"), list) else promotion_basis.get("selected_evidence_refs", []) if isinstance(promotion_basis.get("selected_evidence_refs"), list) else []
        ),
        "audit_refs": {
            "reporting_handoff_path": str(handoff_file),
            "promotion_path": str(promotion_file),
            "readiness_path": maybe_text(handoff.get("readiness_path")),
            "supervisor_state_path": maybe_text(handoff.get("supervisor_state_path")),
        },
    }
    store_council_decision_record(
        run_dir_path,
        decision_payload=wrapper,
        artifact_path=str(output_file),
    )
    write_json_file(output_file, wrapper)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "decision_id": decision_id,
            "moderator_status": moderator_status,
            "publication_readiness": publication_readiness,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "reporting_handoff_source": maybe_text(contract_fields.get("reporting_handoff_source")),
            "promotion_source": maybe_text(contract_fields.get("promotion_source")),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, decision_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [decision_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [decision_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [maybe_text(item.get("summary")) for item in open_risks[:3] if maybe_text(item.get("summary"))] if moderator_status != "finalize" else [],
            "challenge_hints": [summary_text] if moderator_status != "finalize" else [],
            "suggested_next_skills": ["eco-draft-expert-report", "eco-publish-expert-report", "eco-publish-council-decision"] if moderator_status == "finalize" else ["eco-draft-expert-report", "eco-post-board-note", "eco-propose-next-actions", "eco-open-falsification-probe"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Draft a compact council decision object from the reporting handoff.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--reporting-handoff-path", default="")
    parser.add_argument("--promotion-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-actions", type=int, default=4)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = draft_council_decision_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        reporting_handoff_path=args.reporting_handoff_path,
        promotion_path=args.promotion_path,
        output_path=args.output_path,
        max_actions=args.max_actions,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
