#!/usr/bin/env python3
"""Materialize a final publication artifact from canonical reporting outputs."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "materialize-final-publication"
ROLE_VALUES = ("sociologist", "environmentalist")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    query_council_objects,
)
from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    normalized_final_publication_payload,
    store_final_publication_record,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_council_decision_wrapper,
    load_expert_report_wrapper,
    load_report_basis_freeze_wrapper,
    load_reporting_handoff_wrapper,
    load_supervisor_state_wrapper,
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


def payload_without_generated_at(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "generated_at_utc"}


def report_summary(role: str, report_payload: dict[str, Any], path: Path) -> dict[str, Any]:
    findings = report_payload.get("findings", []) if isinstance(report_payload.get("findings"), list) else []
    return {
        "role": role,
        "report_id": maybe_text(report_payload.get("report_id")),
        "status": maybe_text(report_payload.get("status")),
        "summary": maybe_text(report_payload.get("summary")),
        "finding_count": len(findings),
        "report_path": str(path),
    }


def release_summary(*, decision: dict[str, Any], handoff: dict[str, Any], publication_posture: str, report_rows: list[dict[str, Any]]) -> str:
    decision_summary = maybe_text(decision.get("decision_summary"))
    if publication_posture == "release":
        return decision_summary or f"Round {maybe_text(decision.get('round_id'))} is ready for final publication with {len(report_rows)} role reports."
    open_risks = handoff.get("open_risks", []) if isinstance(handoff.get("open_risks"), list) else []
    reasons = "; ".join(maybe_text(item.get("summary")) for item in open_risks[:3] if isinstance(item, dict) and maybe_text(item.get("summary")))
    if reasons:
        return f"Release is withheld for this round because {reasons}."
    return decision_summary or f"Release is withheld for round {maybe_text(decision.get('round_id')) or 'current'} pending another investigation pass."


def published_sections(publication_posture: str, report_rows: list[dict[str, Any]]) -> list[str]:
    sections = ["publication-summary", "council-decision", "evidence-index", "citation-index", "uncertainty-register", "remaining-disputes", "recommendations", "audit-trace"]
    if report_rows:
        sections.insert(2, "role-reports")
    if publication_posture == "release":
        sections.insert(2 if report_rows else 1, "evidence-basis")
    else:
        sections.insert(1, "release-hold")
        sections.insert(2 if report_rows else 2, "open-risks")
    return unique_texts(sections)


def operator_review_hints(supervisor_state: dict[str, Any], handoff: dict[str, Any], publication_posture: str) -> list[str]:
    results: list[str] = []
    notes = supervisor_state.get("operator_notes", []) if isinstance(supervisor_state.get("operator_notes"), list) else []
    results.extend(maybe_text(item) for item in notes if maybe_text(item))
    if publication_posture != "release":
        risks = handoff.get("open_risks", []) if isinstance(handoff.get("open_risks"), list) else []
        results.extend(maybe_text(item.get("summary")) for item in risks[:3] if isinstance(item, dict) and maybe_text(item.get("summary")))
    return unique_texts(results)[:5]


def load_decision_traces(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    decision_id: str,
) -> list[dict[str, Any]]:
    payload = query_council_objects(
        run_dir,
        object_kind="decision-trace",
        run_id=run_id,
        round_id=round_id,
        decision_id=decision_id,
        limit=50,
    )
    rows = payload.get("objects", []) if isinstance(payload.get("objects"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def summarized_decision_traces(traces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "trace_id": maybe_text(trace.get("trace_id")),
            "decision_kind": maybe_text(trace.get("decision_kind")),
            "status": maybe_text(trace.get("status")),
            "selected_object_kind": maybe_text(trace.get("selected_object_kind")),
            "selected_object_id": maybe_text(trace.get("selected_object_id")),
            "accepted_object_ids": list_items(trace.get("accepted_object_ids")),
            "rejected_object_ids": list_items(trace.get("rejected_object_ids")),
        }
        for trace in traces
    ]


def collect_report_evidence_index(
    *,
    handoff: dict[str, Any],
    decision: dict[str, Any],
    report_payloads: dict[str, dict[str, Any]],
    selected_evidence_refs: list[str],
) -> list[dict[str, Any]]:
    evidence_packet = dict_items(handoff.get("evidence_packet"))
    rows = list_items(evidence_packet.get("evidence_index"))
    if not rows:
        rows = list_items(handoff.get("evidence_index"))
    for role, report in sorted(report_payloads.items()):
        for ref in list_items(report.get("selected_evidence_refs")):
            rows.append(
                {
                    "evidence_id": f"report:{role}:{maybe_text(ref)}",
                    "object_kind": "expert-report",
                    "object_id": maybe_text(report.get("report_id")),
                    "summary": f"{role} report cites frozen evidence reference.",
                    "evidence_refs": [ref],
                    "basis_role": "role-report-citation",
                    "source_plane": "reporting",
                    "report_use": "citation-index",
                }
            )
    for ref in selected_evidence_refs:
        rows.append(
            {
                "evidence_id": f"final-evidence-ref:{maybe_text(ref)}",
                "object_kind": "evidence-ref",
                "object_id": maybe_text(ref),
                "summary": "Final publication selected evidence reference.",
                "evidence_refs": [ref],
                "basis_role": "final-publication-citation",
                "source_plane": "reporting",
                "report_use": "citation-index",
            }
        )
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        refs = unique_texts(list_items(row.get("evidence_refs")))
        key = "|".join([maybe_text(row.get("object_kind")), maybe_text(row.get("object_id")), ",".join(refs)])
        if key and key not in deduped:
            copied = dict(row)
            copied["evidence_refs"] = refs
            deduped[key] = copied
    return list(deduped.values())[:80]


def decision_maker_report(
    *,
    publication_posture: str,
    publication_summary: str,
    handoff: dict[str, Any],
    decision: dict[str, Any],
    report_rows: list[dict[str, Any]],
    evidence_index: list[dict[str, Any]],
    selected_evidence_refs: list[str],
) -> dict[str, Any]:
    report_packet = dict_items(handoff.get("report_packet"))
    uncertainty_register = list_items(report_packet.get("uncertainty_register")) or list_items(handoff.get("uncertainty_register"))
    residual_disputes = list_items(report_packet.get("residual_disputes")) or list_items(handoff.get("residual_disputes"))
    policy_recommendations = list_items(report_packet.get("policy_recommendations")) or list_items(handoff.get("policy_recommendations"))
    key_findings = list_items(handoff.get("key_findings"))
    sections = [
        {
            "section_key": "executive-summary",
            "title": "Executive Summary",
            "status": "included",
            "summary": publication_summary,
        },
        {
            "section_key": "decision-question-and-boundary",
            "title": "Decision Question And Boundary",
            "status": "needs-explicit-moderator-text" if not maybe_text(decision.get("decision_question")) else "included",
            "summary": maybe_text(decision.get("decision_question")) or "Decision boundary must be taken from moderator-defined mission context or decision memo.",
        },
        {
            "section_key": "evidence-sources-and-scope",
            "title": "Evidence Sources And Scope",
            "status": "included" if evidence_index else "basis-gap",
            "summary": f"{len(evidence_index)} DB evidence index rows and {len(selected_evidence_refs)} selected evidence refs are available.",
        },
        {
            "section_key": "key-findings",
            "title": "Key Findings",
            "status": "included" if key_findings else "basis-gap",
            "summary": f"{len(key_findings)} DB finding records are included." if key_findings else "No DB finding records were included as final report findings.",
        },
        {
            "section_key": "options-and-tradeoffs",
            "title": "Options And Tradeoffs",
            "status": "basis-required",
            "summary": "Policy options require proposal or report-section-draft basis before they can be stated as recommendations.",
        },
        {
            "section_key": "risks-and-uncertainties",
            "title": "Risks And Uncertainties",
            "status": "included" if uncertainty_register else "basis-gap",
            "summary": f"{len(uncertainty_register)} uncertainty rows are carried into the report.",
        },
        {
            "section_key": "remaining-disputes",
            "title": "Remaining Disputes",
            "status": "included" if residual_disputes else "no-open-disputes-recorded",
            "summary": f"{len(residual_disputes)} residual dispute rows are carried into the report.",
        },
        {
            "section_key": "recommendations",
            "title": "Recommendations",
            "status": "included" if policy_recommendations else "basis-gap",
            "summary": f"{len(policy_recommendations)} DB-backed recommendation cues are included.",
        },
        {
            "section_key": "citation-index",
            "title": "Citation Index",
            "status": "included" if evidence_index else "basis-gap",
            "summary": "Citation rows are derived from DB canonical reporting and deliberation objects.",
        },
    ]
    return {
        "report_type": "decision-maker-environmental-policy-report",
        "publication_posture": publication_posture,
        "sections": sections,
        "role_reports": report_rows,
        "evidence_index": evidence_index,
        "key_findings": key_findings,
        "uncertainty_register": uncertainty_register,
        "residual_disputes": residual_disputes,
        "policy_recommendations": policy_recommendations,
        "guardrails": [
            "Report conclusions must cite DB canonical evidence basis, finding, evidence bundle, proposal, or report section draft objects.",
            "Heuristic helper cues remain appendix/audit material unless explicitly cited through report_basis.",
        ],
    }


def materialize_final_publication_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    reporting_handoff_path: str,
    decision_path: str,
    sociologist_report_path: str,
    environmentalist_report_path: str,
    report_basis_path: str,
    supervisor_state_path: str,
    output_path: str,
    allow_overwrite: bool,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    handoff_file = resolve_path(run_dir_path, reporting_handoff_path, f"reporting/reporting_handoff_{round_id}.json")
    decision_file = resolve_path(run_dir_path, decision_path, f"reporting/council_decision_{round_id}.json")
    report_basis_file = resolve_path(run_dir_path, report_basis_path, f"report_basis/frozen_report_basis_{round_id}.json")
    supervisor_file = resolve_path(run_dir_path, supervisor_state_path, f"runtime/supervisor_state_{round_id}.json")
    report_files = {
        "sociologist": resolve_path(run_dir_path, sociologist_report_path, f"reporting/expert_report_sociologist_{round_id}.json"),
        "environmentalist": resolve_path(run_dir_path, environmentalist_report_path, f"reporting/expert_report_environmentalist_{round_id}.json"),
    }
    output_file = resolve_path(run_dir_path, output_path, f"reporting/final_publication_{round_id}.json")

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
        missing_message = (
            "No reporting handoff DB record was found for "
            f"{handoff_file}; artifact exists but is orphaned from the reporting plane."
            if bool(handoff_context.get("artifact_present"))
            else (
                "No reporting handoff artifact or DB record was found "
                f"at {handoff_file}."
            )
        )
        warnings.append(
            {
                "code": "missing-reporting-handoff",
                "message": missing_message,
            }
        )
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-handoff")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-handoff")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["materialize-reporting-handoff"]},
        }
    handoff = handoff_payload

    decision_context = load_council_decision_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        decision_stage="canonical",
        decision_path=decision_path,
    )
    decision_payload = (
        decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else None
    )
    if not isinstance(decision_payload, dict):
        missing_message = (
            "No canonical council decision DB record was found for "
            f"{decision_file}; artifact exists but is orphaned from the reporting plane."
            if bool(decision_context.get("artifact_present"))
            else (
                "No canonical council decision artifact or DB record was "
                f"found at {decision_file}."
            )
        )
        warnings.append(
            {
                "code": "missing-canonical-decision",
                "message": missing_message,
            }
        )
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-decision")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-decision")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["publish-council-decision"]},
        }
    decision = decision_payload

    report_basis_context = load_report_basis_freeze_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        report_basis_path=report_basis_path,
    )
    report_basis_payload = (
        report_basis_context.get("payload")
        if isinstance(report_basis_context.get("payload"), dict)
        else None
    )
    if not isinstance(report_basis_payload, dict):
        missing_message = (
            "No report-basis DB record was found for "
            f"{report_basis_file}; artifact exists but is orphaned from the deliberation plane."
            if bool(report_basis_context.get("artifact_present"))
            else (
                "No report-basis artifact or DB record was found at "
                f"{report_basis_file}."
            )
        )
        warnings.append(
            {
                "code": "missing-report-basis-freeze",
                "message": missing_message,
            }
        )
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-report-basis")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-report-basis")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["freeze-report-basis"]},
        }
    report_basis_freeze = report_basis_payload

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
    supervisor_state = (
        supervisor_state_payload
        if isinstance(supervisor_state_payload, dict)
        else {}
    )
    if not isinstance(supervisor_state_payload, dict):
        missing_message = (
            "No supervisor DB snapshot was found for "
            f"{supervisor_file}; artifact exists but is orphaned from the phase-2 control plane."
            if bool(supervisor_context.get("artifact_present"))
            else (
                "No supervisor snapshot artifact or DB record was found at "
                f"{supervisor_file}."
            )
        )
        warnings.append(
            {
                "code": "missing-supervisor-state",
                "message": missing_message,
            }
        )

    publication_readiness = maybe_text(decision.get("publication_readiness")) or "hold"
    publication_posture = "release" if publication_readiness == "ready" else "withhold"
    report_rows: list[dict[str, Any]] = []
    report_contexts: dict[str, dict[str, Any]] = {}
    missing_ready_reports: list[tuple[str, bool]] = []
    report_payloads: dict[str, dict[str, Any]] = {}
    published_report_refs = decision.get("published_report_refs", []) if isinstance(decision.get("published_report_refs"), list) else []
    for role in ROLE_VALUES:
        report_context = load_expert_report_wrapper(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            agent_role=role,
            report_stage="canonical",
            report_path=(
                sociologist_report_path
                if role == "sociologist"
                else environmentalist_report_path
            ),
        )
        report_contexts[role] = report_context
        report_payload = (
            report_context.get("payload")
            if isinstance(report_context.get("payload"), dict)
            else None
        )
        if isinstance(report_payload, dict):
            report_payloads[role] = report_payload
            report_rows.append(
                report_summary(
                    role,
                    report_payload,
                    Path(str(report_context.get("artifact_path", report_files[role]))),
                )
            )
        elif publication_posture == "release":
            missing_ready_reports.append(
                (
                    str(report_context.get("artifact_path", report_files[role])),
                    bool(report_context.get("artifact_present")),
                )
            )
        else:
            artifact_path = str(report_context.get("artifact_path", report_files[role]))
            if bool(report_context.get("artifact_present")):
                message = (
                    "Canonical expert report has no DB record at "
                    f"{artifact_path}; the artifact is orphaned from the reporting plane, but release posture is still withheld."
                )
            else:
                message = (
                    f"Canonical expert report is missing at {artifact_path} but release posture is still withheld."
                )
            warnings.append(
                {"code": "missing-canonical-report", "message": message}
            )
    if missing_ready_reports:
        warnings.extend(
            {
                "code": "missing-canonical-report",
                "message": (
                    "Required canonical expert report has no DB record at "
                    f"{path}; the artifact is orphaned from the reporting plane."
                    if artifact_present
                    else f"Required canonical expert report is missing at {path}."
                ),
            }
            for path, artifact_present in missing_ready_reports
        )
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-reports")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-reports")[:16],
            "artifact_refs": [],
            "canonical_ids": [maybe_text(decision.get("decision_id"))] if maybe_text(decision.get("decision_id")) else [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [maybe_text(decision.get("decision_id"))] if maybe_text(decision.get("decision_id")) else [], "evidence_refs": [], "gap_hints": [item["message"] for item in warnings], "challenge_hints": [], "suggested_next_skills": ["publish-expert-report"]},
        }
    decision_traces = load_decision_traces(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        decision_id=maybe_text(decision.get("decision_id")),
    )
    if not decision_traces:
        warnings.append(
            {
                "code": "missing-decision-trace",
                "message": "No decision-trace objects were found for the canonical council decision.",
            }
        )

    contract_fields = reporting_contract_fields_from_payload(
        decision_payload,
        fallback_payload=handoff_payload,
        observed_inputs_overrides={
            "reporting_handoff_artifact_present": bool(
                handoff_context.get("artifact_present")
            ),
            "reporting_handoff_present": bool(handoff_context.get("payload_present")),
            "decision_artifact_present": bool(decision_context.get("artifact_present")),
            "decision_present": bool(decision_context.get("payload_present")),
            "report_basis_artifact_present": bool(
                report_basis_context.get("artifact_present")
            ),
            "report_basis_present": bool(report_basis_context.get("payload_present")),
            "report_basis_artifact_present": bool(
                report_basis_context.get("artifact_present")
            ),
            "report_basis_present": bool(report_basis_context.get("payload_present")),
            "supervisor_state_artifact_present": bool(
                supervisor_context.get("artifact_present")
            ),
            "supervisor_state_present": bool(
                supervisor_context.get("payload_present")
            ),
            "sociologist_report_artifact_present": bool(
                report_contexts.get("sociologist", {}).get("artifact_present")
            ),
            "sociologist_report_present": isinstance(
                report_payloads.get("sociologist"), dict
            ),
            "environmentalist_report_artifact_present": bool(
                report_contexts.get("environmentalist", {}).get("artifact_present")
            ),
            "environmentalist_report_present": isinstance(
                report_payloads.get("environmentalist"), dict
            ),
        },
        field_overrides={
            "reporting_handoff_source": maybe_text(handoff_context.get("source"))
            or "missing-reporting-handoff",
            "decision_source": maybe_text(decision_context.get("source"))
            or "missing-canonical-decision",
            "report_basis_source": maybe_text(report_basis_context.get("source"))
            or "missing-report-basis",
            "report_basis_source": maybe_text(report_basis_context.get("source"))
            or "missing-report-basis",
            "supervisor_state_source": maybe_text(supervisor_context.get("source"))
            or "missing-supervisor-state",
            "sociologist_report_source": maybe_text(
                report_contexts.get("sociologist", {}).get("source")
            )
            or "missing-sociologist-report",
            "environmentalist_report_source": maybe_text(
                report_contexts.get("environmentalist", {}).get("source")
            )
            or "missing-environmentalist-report",
        },
    )
    publication_id = "final-publication-" + stable_hash(run_id, round_id, publication_posture, maybe_text(decision.get("decision_id")))[:12]
    selected_evidence_refs = unique_texts(
        handoff.get("selected_evidence_refs", []) if isinstance(handoff.get("selected_evidence_refs"), list) else []
    )
    if not selected_evidence_refs:
        selected_evidence_refs = unique_texts(
            decision.get("selected_evidence_refs", []) if isinstance(decision.get("selected_evidence_refs"), list) else report_basis_freeze.get("selected_evidence_refs", []) if isinstance(report_basis_freeze.get("selected_evidence_refs"), list) else []
        )
    selected_evidence_refs = unique_texts(
        selected_evidence_refs
        + [
            ref
            for trace in decision_traces
            for ref in list_items(trace.get("evidence_refs"))
        ]
    )
    evidence_index = collect_report_evidence_index(
        handoff=handoff,
        decision=decision,
        report_payloads=report_payloads,
        selected_evidence_refs=selected_evidence_refs,
    )
    publication_summary = release_summary(
        decision=decision,
        handoff=handoff,
        publication_posture=publication_posture,
        report_rows=report_rows,
    )
    decision_report = decision_maker_report(
        publication_posture=publication_posture,
        publication_summary=publication_summary,
        handoff=handoff,
        decision=decision,
        report_rows=report_rows,
        evidence_index=evidence_index,
        selected_evidence_refs=selected_evidence_refs,
    )

    publication_payload = normalized_final_publication_payload(
        {
            "schema_version": "e1.2",
            "skill": SKILL_NAME,
            "generated_at_utc": utc_now_iso(),
            "run_id": run_id,
            "round_id": round_id,
            "publication_id": publication_id,
            "publication_status": "ready-for-release"
            if publication_posture == "release"
            else "hold-release",
            "publication_posture": publication_posture,
            **contract_fields,
            "publication_summary": publication_summary,
            "decision_maker_report": decision_report,
            "published_sections": published_sections(
                publication_posture, report_rows
            ),
            "decision": {
                "decision_id": maybe_text(decision.get("decision_id")),
                "moderator_status": maybe_text(decision.get("moderator_status")),
                "publication_readiness": publication_readiness,
                "decision_summary": maybe_text(decision.get("decision_summary")),
            },
            "decision_trace_ids": unique_texts(
                [trace.get("trace_id") for trace in decision_traces]
            ),
            "decision_trace_count": len(decision_traces),
            "decision_traces": summarized_decision_traces(decision_traces),
            "role_reports": report_rows,
            "published_report_refs": unique_texts(
                published_report_refs or [row["report_path"] for row in report_rows]
            ),
            "key_findings": handoff.get("key_findings", [])
            if isinstance(handoff.get("key_findings"), list)
            else [],
            "open_risks": handoff.get("open_risks", [])
            if isinstance(handoff.get("open_risks"), list)
            else [],
            "recommended_next_actions": handoff.get(
                "recommended_next_actions", []
            )
            if isinstance(handoff.get("recommended_next_actions"), list)
            else [],
            "selected_evidence_refs": selected_evidence_refs,
            "evidence_index": evidence_index,
            "uncertainty_register": list_items(decision_report.get("uncertainty_register")),
            "residual_disputes": list_items(decision_report.get("residual_disputes")),
            "policy_recommendations": list_items(decision_report.get("policy_recommendations")),
            "operator_review_hints": operator_review_hints(
                supervisor_state, handoff, publication_posture
            ),
            "audit_refs": {
                "reporting_handoff_path": str(handoff_file),
                "decision_path": str(decision_file),
                "report_basis_path": str(report_basis_file),
                "report_basis_path": str(report_basis_file),
                "supervisor_state_path": str(supervisor_file),
                "decision_trace_ids": unique_texts(
                    [trace.get("trace_id") for trace in decision_traces]
                ),
                "role_report_paths": {
                    role: str(path)
                    for role, path in report_files.items()
                    if isinstance(report_payloads.get(role), dict)
                },
            },
        },
        run_id=run_id,
        round_id=round_id,
    )

    existing = load_json_if_exists(output_file)
    operation = "published"
    overwrote_existing = False
    if isinstance(existing, dict):
        if payload_without_generated_at(existing) == payload_without_generated_at(publication_payload):
            operation = "noop"
            publication_payload["generated_at_utc"] = maybe_text(existing.get("generated_at_utc")) or publication_payload["generated_at_utc"]
        elif allow_overwrite:
            overwrote_existing = True
        else:
            warnings.append({"code": "overwrite-blocked", "message": "Refusing to overwrite non-matching final publication without --allow-overwrite."})
            return {
                "status": "blocked",
                "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
                "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "overwrite-blocked")[:20],
                "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "overwrite-blocked")[:16],
                "artifact_refs": [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}],
                "canonical_ids": [publication_id],
                "warnings": warnings,
                "board_handoff": {"candidate_ids": [publication_id], "evidence_refs": [], "gap_hints": [warnings[-1]["message"]], "challenge_hints": [], "suggested_next_skills": ["materialize-final-publication"]},
            }

    stored_payload = store_final_publication_record(
        run_dir_path,
        publication_payload=publication_payload,
        artifact_path=str(output_file),
    )
    if operation != "noop":
        write_json_file(output_file, stored_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    suggested_next_skills = [] if publication_posture == "release" else ["submit-finding-record", "submit-evidence-bundle", "submit-council-proposal", "submit-readiness-opinion"]
    publication_id = maybe_text(stored_payload.get("publication_id")) or publication_id
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "operation": operation,
            "overwrote_existing": overwrote_existing,
            "output_path": str(output_file),
            "publication_id": publication_id,
            "publication_status": publication_payload["publication_status"],
            "publication_posture": publication_posture,
            "decision_trace_count": len(decision_traces),
            "evidence_index_count": len(evidence_index),
            "report_section_count": len(list_items(decision_report.get("sections"))),
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "reporting_handoff_source": maybe_text(
                contract_fields.get("reporting_handoff_source")
            ),
            "decision_source": maybe_text(contract_fields.get("decision_source")),
            "report_basis_source": maybe_text(contract_fields.get("report_basis_source")),
            "supervisor_state_source": maybe_text(
                contract_fields.get("supervisor_state_source")
            ),
            "sociologist_report_source": maybe_text(
                contract_fields.get("sociologist_report_source")
            ),
            "environmentalist_report_source": maybe_text(
                contract_fields.get("environmentalist_report_source")
            ),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, operation, publication_id)[:20],
        "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [publication_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [publication_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [maybe_text(item.get("summary")) for item in publication_payload.get("open_risks", [])[:3] if isinstance(item, dict) and maybe_text(item.get("summary"))] if publication_posture != "release" else [],
            "challenge_hints": publication_payload.get("operator_review_hints", []),
            "suggested_next_skills": suggested_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a final publication artifact from canonical reporting outputs.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--reporting-handoff-path", default="")
    parser.add_argument("--decision-path", default="")
    parser.add_argument("--sociologist-report-path", default="")
    parser.add_argument("--environmentalist-report-path", default="")
    parser.add_argument("--report-basis-path", default="")
    parser.add_argument("--supervisor-state-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_final_publication_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        reporting_handoff_path=args.reporting_handoff_path,
        decision_path=args.decision_path,
        sociologist_report_path=args.sociologist_report_path,
        environmentalist_report_path=args.environmentalist_report_path,
        report_basis_path=args.report_basis_path,
        supervisor_state_path=args.supervisor_state_path,
        output_path=args.output_path,
        allow_overwrite=args.allow_overwrite,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
