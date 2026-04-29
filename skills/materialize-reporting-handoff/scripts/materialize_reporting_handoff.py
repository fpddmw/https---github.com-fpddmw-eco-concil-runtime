#!/usr/bin/env python3
"""Materialize a compact DB-backed reporting handoff from frozen evidence basis."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "materialize-reporting-handoff"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.council_objects import query_council_objects  # noqa: E402
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


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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


def legacy_reporting_basis_count(promotion_basis: dict[str, Any]) -> int:
    frozen_basis = (
        promotion_basis.get("frozen_basis", {})
        if isinstance(promotion_basis.get("frozen_basis"), dict)
        else {}
    )
    count = 0
    for key in (
        "selected_coverages",
        "coverages",
        "empirical_support_coverages",
        "verification_routes",
        "formal_public_links",
        "representation_gaps",
        "diffusion_edges",
    ):
        rows = promotion_basis.get(key, frozen_basis.get(key, []))
        if isinstance(rows, list):
            count += len(rows)
    return count


def query_round_council_objects(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    object_kind: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception:
        return []
    rows = payload.get("objects", []) if isinstance(payload.get("objects"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def load_reporting_basis_objects(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, list[dict[str, Any]]]:
    object_kinds = (
        "finding-record",
        "evidence-bundle",
        "proposal",
        "readiness-opinion",
        "challenge",
        "review-comment",
    )
    return {
        object_kind: query_round_council_objects(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind=object_kind,
        )
        for object_kind in object_kinds
    }


def object_id_for(object_kind: str, row: dict[str, Any]) -> str:
    for field_name in (
        "finding_id",
        "bundle_id",
        "proposal_id",
        "opinion_id",
        "challenge_id",
        "comment_id",
        "ticket_id",
        "id",
    ):
        value = maybe_text(row.get(field_name))
        if value:
            return value
    return object_kind + "-" + stable_hash(object_kind, row.get("summary"), row.get("title"))[:12]


def object_summary(row: dict[str, Any]) -> str:
    for field_name in ("summary", "title", "rationale", "opinion_text", "challenge_statement", "comment_text"):
        value = maybe_text(row.get(field_name))
        if value:
            return value
    return "DB council object without a compact summary."


def evidence_refs_for(row: dict[str, Any]) -> list[str]:
    return unique_texts(list_items(row.get("evidence_refs")))


def build_evidence_index(
    *,
    selected_evidence_refs: list[str],
    council_basis: dict[str, list[dict[str, Any]]],
    max_items: int = 40,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, evidence_ref in enumerate(selected_evidence_refs, start=1):
        rows.append(
            {
                "evidence_id": f"frozen-evidence-ref-{index:03d}",
                "object_kind": "evidence-ref",
                "object_id": evidence_ref,
                "summary": "Frozen DB report-basis evidence reference.",
                "evidence_refs": [evidence_ref],
                "basis_role": "frozen-report-basis",
                "source_plane": "deliberation",
                "report_use": "citation-index",
            }
        )
    for object_kind in ("finding-record", "evidence-bundle", "proposal", "readiness-opinion", "challenge", "review-comment"):
        for row in council_basis.get(object_kind, []):
            object_id = object_id_for(object_kind, row)
            refs = evidence_refs_for(row)
            if object_kind in {"finding-record", "evidence-bundle"} and not refs:
                continue
            rows.append(
                {
                    "evidence_id": f"{object_kind}:{object_id}",
                    "object_kind": object_kind,
                    "object_id": object_id,
                    "summary": object_summary(row),
                    "evidence_refs": refs,
                    "basis_object_ids": unique_texts(list_items(row.get("basis_object_ids"))),
                    "source_signal_ids": unique_texts(list_items(row.get("source_signal_ids"))),
                    "basis_role": (
                        "investigator-evidence"
                        if object_kind in {"finding-record", "evidence-bundle"}
                        else "council-context"
                    ),
                    "source_plane": "deliberation",
                    "report_use": (
                        "report-basis-candidate"
                        if object_kind in {"finding-record", "evidence-bundle"}
                        else "audit-context"
                    ),
                }
            )
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = "|".join(
            [
                maybe_text(row.get("object_kind")),
                maybe_text(row.get("object_id")),
                ",".join(evidence_refs_for(row)),
            ]
        )
        if key and key not in deduped:
            deduped[key] = row
    return list(deduped.values())[:max_items]


def build_key_findings_from_council_basis(
    council_basis: dict[str, list[dict[str, Any]]],
    max_findings: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, row in enumerate(council_basis.get("finding-record", [])[: max(0, max_findings)], start=1):
        refs = evidence_refs_for(row)
        if not refs:
            continue
        findings.append(
            {
                "finding_id": object_id_for("finding-record", row),
                "title": maybe_text(row.get("title")) or f"Finding {index}",
                "summary": object_summary(row),
                "finding_kind": maybe_text(row.get("finding_kind")) or "finding",
                "agent_role": maybe_text(row.get("agent_role")),
                "evidence_refs": refs,
                "basis_object_ids": unique_texts(list_items(row.get("basis_object_ids"))),
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


def build_recommended_next_actions(
    supervisor_state: dict[str, Any],
    *,
    open_risks: list[dict[str, str]] | None = None,
    reporting_blocker_hints: list[str] | None = None,
) -> list[dict[str, str]]:
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
    if recommendations:
        return recommendations
    for risk in (open_risks or [])[:4]:
        if not isinstance(risk, dict):
            continue
        summary = maybe_text(risk.get("summary"))
        if not summary:
            continue
        priority = maybe_text(risk.get("priority")) or "medium"
        risk_type = maybe_text(risk.get("risk_type")) or "reporting-blocker"
        recommendations.append(
            {
                "assigned_role": maybe_text(risk.get("assigned_role")) or "moderator",
                "objective": f"Resolve or explicitly carry forward: {summary}",
                "reason": (
                    "Reporting is held because this "
                    f"{risk_type} remains open (priority={priority})."
                ),
            }
        )
    if recommendations:
        return recommendations
    for hint in (reporting_blocker_hints or [])[:4]:
        summary = maybe_text(hint)
        if not summary:
            continue
        recommendations.append(
            {
                "assigned_role": "moderator",
                "objective": f"Address reporting blocker: {summary}",
                "reason": "Reporting is held until the blocker is resolved, accepted, or scoped into a follow-up round.",
            }
        )
    return recommendations


def recommended_sections(reporting_ready: bool) -> list[str]:
    if reporting_ready:
        return ["executive-summary", "role-reports", "evidence-basis", "residual-risks", "audit-trace"]
    return ["gating-status", "open-risks", "next-round-plan", "audit-trace"]


def build_uncertainty_register(
    *,
    open_risks: list[dict[str, str]],
    reporting_blocker_hints: list[str],
    evidence_index: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    register: list[dict[str, Any]] = []
    for index, risk in enumerate(open_risks, start=1):
        summary = maybe_text(risk.get("summary")) if isinstance(risk, dict) else ""
        if not summary:
            continue
        register.append(
            {
                "uncertainty_id": f"open-risk-{index:03d}",
                "uncertainty_type": maybe_text(risk.get("risk_type")) or "open-risk",
                "summary": summary,
                "report_treatment": "Carry as unresolved risk or scope a follow-up evidence request.",
            }
        )
    for index, hint in enumerate(reporting_blocker_hints, start=1):
        summary = maybe_text(hint)
        if not summary:
            continue
        register.append(
            {
                "uncertainty_id": f"reporting-blocker-{index:03d}",
                "uncertainty_type": "reporting-blocker",
                "summary": summary,
                "report_treatment": "Do not present as resolved evidence until moderator or report basis explicitly addresses it.",
            }
        )
    if not any(row.get("object_kind") in {"finding-record", "evidence-bundle"} for row in evidence_index):
        register.append(
            {
                "uncertainty_id": "missing-investigator-basis-001",
                "uncertainty_type": "report-basis-gap",
                "summary": "No DB finding-record or evidence-bundle is available for direct report citation.",
                "report_treatment": "Use frozen evidence refs only as citation index until an investigator or report editor cites them through DB basis objects.",
            }
        )
    return register[:8]


def build_residual_disputes(
    *,
    reporting_blockers: list[str],
    rejected_proposal_ids: list[str],
    rejected_opinion_ids: list[str],
    open_risks: list[dict[str, str]],
) -> list[dict[str, Any]]:
    disputes: list[dict[str, Any]] = []
    for index, blocker in enumerate(reporting_blockers, start=1):
        summaries = reporting_blocker_summaries([blocker])
        disputes.append(
            {
                "dispute_id": f"reporting-blocker-{index:03d}",
                "object_kind": "reporting-blocker",
                "object_id": blocker,
                "summary": summaries[0] if summaries else blocker,
                "status": "open",
            }
        )
    for proposal_id in rejected_proposal_ids:
        disputes.append(
            {
                "dispute_id": f"proposal-veto:{proposal_id}",
                "object_kind": "proposal",
                "object_id": proposal_id,
                "summary": "Council proposal is rejected or vetoed for current publication posture.",
                "status": "unresolved",
            }
        )
    for opinion_id in rejected_opinion_ids:
        disputes.append(
            {
                "dispute_id": f"readiness-veto:{opinion_id}",
                "object_kind": "readiness-opinion",
                "object_id": opinion_id,
                "summary": "Readiness opinion blocks or qualifies final publication.",
                "status": "unresolved",
            }
        )
    for risk in open_risks:
        summary = maybe_text(risk.get("summary")) if isinstance(risk, dict) else ""
        if summary:
            disputes.append(
                {
                    "dispute_id": maybe_text(risk.get("risk_id")) or "open-risk",
                    "object_kind": maybe_text(risk.get("risk_type")) or "risk",
                    "object_id": maybe_text(risk.get("risk_id")),
                    "summary": summary,
                    "status": "open",
                }
            )
    return disputes[:8]


def build_policy_recommendations(
    *,
    reporting_ready: bool,
    recommended_next_actions: list[dict[str, str]],
    uncertainty_register: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if reporting_ready:
        return [
            {
                "recommendation_id": "reporting-action-001",
                "recommendation_type": "reporting",
                "summary": "Draft the decision-maker report from frozen DB evidence basis and submitted report section drafts.",
                "basis": "report-basis-freeze",
            },
            {
                "recommendation_id": "reporting-action-002",
                "recommendation_type": "audit",
                "summary": "Carry uncertainty register and residual disputes into the final report rather than turning them into settled conclusions.",
                "basis": "uncertainty-register" if uncertainty_register else "reporting-gate",
            },
        ]
    recommendations: list[dict[str, Any]] = []
    for index, action in enumerate(recommended_next_actions, start=1):
        recommendations.append(
            {
                "recommendation_id": f"next-round-action-{index:03d}",
                "recommendation_type": "follow-up-investigation",
                "summary": maybe_text(action.get("objective")),
                "assigned_role": maybe_text(action.get("assigned_role")),
                "basis": maybe_text(action.get("reason")),
            }
        )
    return [item for item in recommendations if maybe_text(item.get("summary"))][:6]


def build_packets(
    *,
    run_id: str,
    round_id: str,
    reporting_ready: bool,
    handoff_status: str,
    promotion_status: str,
    readiness_status: str,
    supervisor_status: str,
    reporting_blockers: list[str],
    selected_basis_object_ids: list[str],
    selected_evidence_refs: list[str],
    supporting_proposal_ids: list[str],
    rejected_proposal_ids: list[str],
    supporting_opinion_ids: list[str],
    rejected_opinion_ids: list[str],
    council_input_counts: dict[str, Any],
    key_findings: list[dict[str, Any]],
    open_risks: list[dict[str, str]],
    recommended_next_actions: list[dict[str, str]],
    council_basis: dict[str, list[dict[str, Any]]],
    reporting_blocker_hints: list[str],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    evidence_index = build_evidence_index(
        selected_evidence_refs=selected_evidence_refs,
        council_basis=council_basis,
    )
    uncertainty_register = build_uncertainty_register(
        open_risks=open_risks,
        reporting_blocker_hints=reporting_blocker_hints,
        evidence_index=evidence_index,
    )
    residual_disputes = build_residual_disputes(
        reporting_blockers=reporting_blockers,
        rejected_proposal_ids=rejected_proposal_ids,
        rejected_opinion_ids=rejected_opinion_ids,
        open_risks=open_risks,
    )
    policy_recommendations = build_policy_recommendations(
        reporting_ready=reporting_ready,
        recommended_next_actions=recommended_next_actions,
        uncertainty_register=uncertainty_register,
    )
    packet_suffix = stable_hash(run_id, round_id, handoff_status, promotion_status)[:12]
    evidence_packet = {
        "packet_id": f"evidence-packet-{packet_suffix}",
        "packet_kind": "decision-maker-report-evidence-packet",
        "source": "db-canonical-report-basis",
        "selected_basis_object_ids": selected_basis_object_ids,
        "selected_evidence_refs": selected_evidence_refs,
        "evidence_index": evidence_index,
        "key_findings": key_findings,
        "basis_object_counts": {
            object_kind: len(rows)
            for object_kind, rows in sorted(council_basis.items())
        },
        "caveats": [
            "Helper and heuristic outputs are not direct report basis unless cited through DB council/reporting objects.",
            "Frozen evidence refs identify citation candidates; report conclusions require finding, evidence bundle, proposal, or report section basis.",
        ],
    }
    decision_packet = {
        "packet_id": f"decision-packet-{packet_suffix}",
        "packet_kind": "moderator-decision-memo-packet",
        "handoff_status": handoff_status,
        "reporting_ready": reporting_ready,
        "promotion_status": promotion_status,
        "readiness_status": readiness_status,
        "supervisor_status": supervisor_status,
        "reporting_blockers": reporting_blockers,
        "supporting_proposal_ids": supporting_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "supporting_opinion_ids": supporting_opinion_ids,
        "rejected_opinion_ids": rejected_opinion_ids,
        "council_input_counts": council_input_counts,
        "open_risks": open_risks,
        "recommended_next_actions": recommended_next_actions,
        "residual_disputes": residual_disputes,
    }
    report_packet = {
        "packet_id": f"report-packet-{packet_suffix}",
        "packet_kind": "decision-maker-policy-report-packet",
        "report_type": "decision-maker-environmental-policy-report",
        "recommended_sections": [
            {"section_key": "decision-question", "required_basis": "moderator-defined question or decision memo"},
            {"section_key": "regional-and-policy-context", "required_basis": "DB evidence bundle or report section draft"},
            {"section_key": "evidence-sources-and-scope", "required_basis": "evidence_packet.evidence_index"},
            {"section_key": "key-findings", "required_basis": "finding-record or report-section-draft"},
            {"section_key": "options-and-tradeoffs", "required_basis": "proposal or report-section-draft"},
            {"section_key": "risks-and-uncertainties", "required_basis": "uncertainty_register"},
            {"section_key": "recommendations", "required_basis": "policy_recommendations with evidence refs"},
            {"section_key": "remaining-disputes", "required_basis": "residual_disputes"},
            {"section_key": "citation-index", "required_basis": "evidence_packet.evidence_index"},
        ],
        "uncertainty_register": uncertainty_register,
        "residual_disputes": residual_disputes,
        "policy_recommendations": policy_recommendations,
    }
    return evidence_packet, decision_packet, report_packet


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

    council_basis = load_reporting_basis_objects(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    key_findings = build_key_findings_from_council_basis(council_basis, max_findings)
    ignored_legacy_basis_count = legacy_reporting_basis_count(promotion_basis)
    if ignored_legacy_basis_count:
        warnings.append(
            {
                "code": "legacy-wp4-reporting-basis-ignored",
                "message": (
                    "Ignored legacy claim/coverage, routing, linkage, gap, or diffusion "
                    f"basis rows ({ignored_legacy_basis_count}) while materializing reporting handoff."
                ),
            }
        )
    open_risks = build_open_risks(promotion_basis=promotion_basis, supervisor_state=supervisor_state, readiness=readiness)
    next_actions = build_recommended_next_actions(
        supervisor_state,
        open_risks=open_risks,
        reporting_blocker_hints=reporting_blocker_hints,
    )
    selected_basis_object_ids = unique_texts(
        promotion_basis.get("selected_basis_object_ids", [])
        if isinstance(promotion_basis.get("selected_basis_object_ids"), list)
        else []
    )
    supporting_proposal_ids = unique_texts(
        promotion_basis.get("supporting_proposal_ids", [])
        if isinstance(promotion_basis.get("supporting_proposal_ids"), list)
        else []
    )
    rejected_proposal_ids = unique_texts(
        promotion_basis.get("rejected_proposal_ids", [])
        if isinstance(promotion_basis.get("rejected_proposal_ids"), list)
        else []
    )
    supporting_opinion_ids = unique_texts(
        promotion_basis.get("supporting_opinion_ids", [])
        if isinstance(promotion_basis.get("supporting_opinion_ids"), list)
        else []
    )
    rejected_opinion_ids = unique_texts(
        promotion_basis.get("rejected_opinion_ids", [])
        if isinstance(promotion_basis.get("rejected_opinion_ids"), list)
        else []
    )
    council_input_counts = (
        promotion_basis.get("council_input_counts", {})
        if isinstance(promotion_basis.get("council_input_counts"), dict)
        else {}
    )
    selected_evidence_refs = unique_texts(
        promotion_basis.get("selected_evidence_refs", [])
        if isinstance(promotion_basis.get("selected_evidence_refs"), list)
        else []
    )
    evidence_packet, decision_packet, report_packet = build_packets(
        run_id=run_id,
        round_id=round_id,
        reporting_ready=reporting_ready,
        handoff_status=handoff_status,
        promotion_status=promotion_status,
        readiness_status=readiness_status,
        supervisor_status=supervisor_status,
        reporting_blockers=reporting_blockers,
        selected_basis_object_ids=selected_basis_object_ids,
        selected_evidence_refs=selected_evidence_refs,
        supporting_proposal_ids=supporting_proposal_ids,
        rejected_proposal_ids=rejected_proposal_ids,
        supporting_opinion_ids=supporting_opinion_ids,
        rejected_opinion_ids=rejected_opinion_ids,
        council_input_counts=council_input_counts,
        key_findings=key_findings,
        open_risks=open_risks,
        recommended_next_actions=next_actions,
        council_basis=council_basis,
        reporting_blocker_hints=reporting_blocker_hints,
    )
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
        "selected_basis_object_ids": selected_basis_object_ids,
        "supporting_proposal_ids": supporting_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "supporting_opinion_ids": supporting_opinion_ids,
        "rejected_opinion_ids": rejected_opinion_ids,
        "promotion_resolution_mode": maybe_text(
            promotion_basis.get("promotion_resolution_mode")
        ),
        "promotion_resolution_reasons": (
            promotion_basis.get("promotion_resolution_reasons", [])
            if isinstance(promotion_basis.get("promotion_resolution_reasons"), list)
            else []
        ),
        "council_input_counts": council_input_counts,
        "selected_evidence_refs": selected_evidence_refs,
        "evidence_packet": evidence_packet,
        "decision_packet": decision_packet,
        "report_packet": report_packet,
        "evidence_index": list_items(evidence_packet.get("evidence_index")),
        "uncertainty_register": list_items(report_packet.get("uncertainty_register")),
        "residual_disputes": list_items(report_packet.get("residual_disputes")),
        "policy_recommendations": list_items(report_packet.get("policy_recommendations")),
        "board_brief_excerpt": board_excerpt,
        "key_findings": key_findings,
        "open_risks": open_risks,
        "recommended_next_actions": next_actions,
        "recommended_sections": recommended_sections(reporting_ready),
        "report_targets": ["expert-report-draft", "council-decision-draft"] if reporting_ready else ["expert-report-draft", "another-round-decision"],
        "warnings": warnings,
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
            "evidence_packet_id": maybe_text(evidence_packet.get("packet_id")),
            "decision_packet_id": maybe_text(decision_packet.get("packet_id")),
            "report_packet_id": maybe_text(report_packet.get("packet_id")),
            "evidence_index_count": len(list_items(evidence_packet.get("evidence_index"))),
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
            "suggested_next_skills": ["draft-expert-report", "draft-council-decision"] if reporting_ready else ["draft-expert-report", "draft-council-decision", "submit-finding-record", "submit-evidence-bundle", "submit-council-proposal", "submit-readiness-opinion"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a compact DB-backed reporting handoff from frozen evidence basis.")
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
