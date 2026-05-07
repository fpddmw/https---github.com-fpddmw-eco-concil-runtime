from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from eco_council_runtime.kernel.execution.governed_execution_action_semantics import maybe_bool
from eco_council_runtime.reporting_status import (
    normalize_reporting_handoff_status,
    reporting_gate_state,
)
from eco_council_runtime.kernel.planes.deliberation_plane_rows import (
    coerce_int,
    dict_items,
    json_text,
    list_items,
    maybe_text,
    merged_lineage,
    normalized_provenance,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_council_decision_record_row,
    write_expert_report_record_row,
    write_final_publication_row,
    write_report_basis_freeze_item_row,
    write_report_basis_freeze_record_row,
    write_reporting_handoff_row,
)
from eco_council_runtime.kernel.planes.deliberation_plane_schema import connect_db, resolve_run_dir
from eco_council_runtime.kernel.planes.deliberation_plane_rows import (
    latest_json_row,
    latest_raw_json_row,
    latest_raw_json_row_where,
)
REPORT_AGENT_ROLES = ("sociologist", "environmentalist")

def nested_evidence_refs(items: Any) -> list[str]:
    values: list[Any] = []
    for item in list_items(items):
        if isinstance(item, dict):
            values.extend(list_items(item.get("evidence_refs")))
    return unique_texts(values)

def nested_text_ids(items: Any, *field_names: str) -> list[str]:
    values: list[str] = []
    for item in list_items(items):
        if not isinstance(item, dict):
            continue
        for field_name in field_names:
            text = maybe_text(item.get(field_name))
            if text:
                values.append(text)
    return unique_texts(values)

def ensure_list_fields(normalized: dict[str, Any], *field_names: str) -> None:
    for field_name in field_names:
        normalized[field_name] = list_items(normalized.get(field_name))

def ensure_dict_fields(normalized: dict[str, Any], *field_names: str) -> None:
    for field_name in field_names:
        normalized[field_name] = dict_items(normalized.get(field_name))

def apply_reporting_contract_defaults(
    normalized: dict[str, Any],
    *,
    object_kind: str,
    decision_source: str,
    lineage_sources: tuple[Any, ...] = (),
    nested_evidence_sources: tuple[Any, ...] = (),
    provenance_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized["schema_version"] = canonical_contract(object_kind).schema_version
    normalized["decision_source"] = decision_source
    evidence_refs = list_items(normalized.get("evidence_refs"))
    evidence_refs.extend(list_items(normalized.get("selected_evidence_refs")))
    for source in nested_evidence_sources:
        evidence_refs.extend(nested_evidence_refs(source))
    normalized["evidence_refs"] = unique_texts(evidence_refs)
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        *lineage_sources,
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=maybe_text(normalized.get("skill")),
        decision_source=decision_source,
        extra=provenance_extra,
    )
    return normalized

PROMOTION_BASIS_ITEM_GROUPS = (
    "issue_clusters",
    "verification_routes",
    "formal_public_links",
    "representation_gaps",
    "diffusion_edges",
    "coverages",
)



def reporting_handoff_id(
    run_id: str,
    round_id: str,
    handoff_status: str,
    report_basis_status: str,
) -> str:
    return "reporting-handoff-" + stable_hash(
        "reporting-handoff",
        run_id,
        round_id,
        handoff_status,
        report_basis_status,
    )[:12]

def council_decision_record_id(
    run_id: str,
    round_id: str,
    decision_stage: str,
    decision_id: str,
) -> str:
    return "decision-record-" + stable_hash(
        "council-decision-record",
        run_id,
        round_id,
        decision_stage,
        decision_id,
    )[:12]

def expert_report_record_id(
    run_id: str,
    round_id: str,
    report_stage: str,
    agent_role: str,
    report_id: str,
) -> str:
    return "expert-report-record-" + stable_hash(
        "expert-report-record",
        run_id,
        round_id,
        report_stage,
        agent_role,
        report_id,
    )[:12]

def decision_stage_from_payload(payload: dict[str, Any]) -> str:
    explicit_stage = maybe_text(payload.get("decision_stage"))
    if explicit_stage in {"draft", "canonical"}:
        return explicit_stage
    if maybe_text(payload.get("canonical_artifact")) == "council-decision":
        return "canonical"
    return "draft"

def expert_report_stage_from_payload(payload: dict[str, Any]) -> str:
    explicit_stage = maybe_text(payload.get("report_stage"))
    if explicit_stage in {"draft", "canonical"}:
        return explicit_stage
    if maybe_text(payload.get("canonical_artifact")) == "expert-report":
        return "canonical"
    return "draft"

def normalized_report_basis_freeze_payload(
    report_basis_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(report_basis_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status"))
        or "withheld"
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "blocked"
    )
    normalized["selected_basis_object_ids"] = list_items(
        normalized.get("selected_basis_object_ids")
    )
    normalized["selected_evidence_refs"] = list_items(
        normalized.get("selected_evidence_refs")
    )
    normalized["basis_id"] = (
        maybe_text(normalized.get("basis_id"))
        or "evidence-basis-"
        + stable_hash(
            "report-basis-freeze",
            normalized["run_id"],
            normalized["round_id"],
            normalized["report_basis_status"],
        )[:12]
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "policy-fallback"
    )
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = list_items(
        normalized.get("evidence_refs")
    ) or list_items(normalized.get("selected_evidence_refs"))
    item_object_ids = [
        report_basis_freeze_item_object_id(item_group, item)
        for item_group, _item_index, item in iter_report_basis_freeze_items(normalized)
    ]
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("selected_basis_object_ids"),
        item_object_ids,
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "basis_selection_mode": maybe_text(
                normalized.get("basis_selection_mode")
            ),
            "board_state_source": maybe_text(normalized.get("board_state_source")),
            "coverage_source": maybe_text(normalized.get("coverage_source")),
            "readiness_source": maybe_text(normalized.get("readiness_source")),
            "next_actions_source": maybe_text(normalized.get("next_actions_source")),
            "board_brief_source": maybe_text(normalized.get("board_brief_source")),
        },
    )
    return validate_canonical_payload("report-basis-freeze", normalized)

def normalized_reporting_handoff_payload(
    handoff_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(handoff_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    gate_state = reporting_gate_state(
        report_basis_status=maybe_text(normalized.get("report_basis_status"))
        or "withheld",
        readiness_status=maybe_text(normalized.get("readiness_status")) or "blocked",
        supervisor_status=maybe_text(normalized.get("supervisor_status")),
        require_supervisor=True,
        reporting_ready=normalized.get("reporting_ready"),
        reporting_blockers_value=normalized.get("reporting_blockers"),
        handoff_status=maybe_text(normalized.get("handoff_status")),
    )
    normalized["handoff_status"] = (
        maybe_text(gate_state.get("handoff_status")) or "investigation-open"
    )
    normalized["report_basis_status"] = maybe_text(gate_state.get("report_basis_status")) or "withheld"
    normalized["readiness_status"] = maybe_text(gate_state.get("readiness_status")) or "blocked"
    normalized["supervisor_status"] = maybe_text(gate_state.get("supervisor_status"))
    normalized["reporting_ready"] = bool(gate_state.get("reporting_ready"))
    normalized["reporting_blockers"] = list_items(
        gate_state.get("reporting_blockers")
    )
    normalized["handoff_id"] = (
        maybe_text(normalized.get("handoff_id"))
        or reporting_handoff_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["handoff_status"],
            normalized["report_basis_status"],
        )
    )
    ensure_list_fields(
        normalized,
        "reporting_blockers",
        "selected_basis_object_ids",
        "selected_evidence_refs",
        "supporting_proposal_ids",
        "rejected_proposal_ids",
        "supporting_opinion_ids",
        "rejected_opinion_ids",
        "recommended_next_actions",
        "key_findings",
        "open_risks",
        "evidence_index",
        "uncertainty_register",
        "residual_disputes",
        "policy_recommendations",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "council_input_counts",
        "evidence_packet",
        "decision_packet",
        "report_packet",
    )
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("readiness_source"))
        or maybe_text(normalized.get("supervisor_state_source"))
        or maybe_text(normalized.get("skill"))
        or "reporting-handoff-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="reporting-handoff",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(normalized.get("report_basis_id")),
            normalized.get("selected_basis_object_ids"),
            normalized.get("supporting_proposal_ids"),
            normalized.get("rejected_proposal_ids"),
            normalized.get("supporting_opinion_ids"),
            normalized.get("rejected_opinion_ids"),
            nested_text_ids(normalized.get("key_findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("evidence_index"), "evidence_id", "object_id"),
        ),
        nested_evidence_sources=(
            normalized.get("key_findings"),
            normalized.get("evidence_index"),
        ),
        provenance_extra={
            "handoff_status": normalized["handoff_status"],
            "report_basis_status": normalized["report_basis_status"],
            "readiness_status": normalized["readiness_status"],
            "supervisor_status": normalized["supervisor_status"],
            "board_state_source": maybe_text(normalized.get("board_state_source")),
            "coverage_source": maybe_text(normalized.get("coverage_source")),
            "report_basis_source": maybe_text(normalized.get("report_basis_source")),
            "readiness_source": maybe_text(normalized.get("readiness_source")),
            "supervisor_state_source": maybe_text(
                normalized.get("supervisor_state_source")
            ),
        },
    )
    return validate_canonical_payload("reporting-handoff", normalized)

def normalized_council_decision_payload(
    decision_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(decision_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["decision_stage"] = decision_stage_from_payload(normalized)
    normalized["decision_id"] = (
        maybe_text(normalized.get("decision_id"))
        or "council-decision-"
        + stable_hash(
            "council-decision",
            normalized["run_id"],
            normalized["round_id"],
            normalized["decision_stage"],
            maybe_text(normalized.get("moderator_status")),
            maybe_text(normalized.get("publication_readiness")),
        )[:12]
    )
    normalized["record_id"] = (
        maybe_text(normalized.get("record_id"))
        or council_decision_record_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["decision_stage"],
            normalized["decision_id"],
        )
    )
    normalized["handoff_status"] = normalize_reporting_handoff_status(
        normalized.get("handoff_status")
    )
    normalized["reporting_ready"] = bool(maybe_bool(normalized.get("reporting_ready")))
    normalized["reporting_blockers"] = list_items(normalized.get("reporting_blockers"))
    normalized["publication_readiness"] = (
        maybe_text(normalized.get("publication_readiness"))
        or ("ready" if normalized["reporting_ready"] else "hold")
    )
    normalized["moderator_status"] = (
        maybe_text(normalized.get("moderator_status"))
        or (
            "finalize"
            if normalized["publication_readiness"] == "ready"
            else "continue"
        )
    )
    normalized["decision_summary"] = (
        maybe_text(normalized.get("decision_summary"))
        or (
            "Round is ready for final reporting."
            if normalized["publication_readiness"] == "ready"
            else "Another round is required before final reporting."
        )
    )
    ensure_list_fields(
        normalized,
        "reporting_blockers",
        "selected_basis_object_ids",
        "selected_evidence_refs",
        "supporting_proposal_ids",
        "rejected_proposal_ids",
        "supporting_opinion_ids",
        "rejected_opinion_ids",
        "decision_trace_ids",
        "published_report_refs",
        "recommended_next_actions",
        "key_findings",
        "open_risks",
        "accepted_object_ids",
        "rejected_object_ids",
        "report_basis_resolution_reasons",
        "memo_sections",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "decision_gating",
        "council_input_counts",
        "audit_refs",
        "decision_packet",
    )
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("reporting_handoff_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("skill"))
        or "council-decision-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="council-decision",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(normalized.get("report_basis_id")),
            normalized.get("selected_basis_object_ids"),
            normalized.get("supporting_proposal_ids"),
            normalized.get("rejected_proposal_ids"),
            normalized.get("supporting_opinion_ids"),
            normalized.get("rejected_opinion_ids"),
            normalized.get("decision_trace_ids"),
            nested_text_ids(normalized.get("key_findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("memo_sections"), "section_id"),
        ),
        nested_evidence_sources=(normalized.get("key_findings"),),
        provenance_extra={
            "decision_stage": normalized["decision_stage"],
            "moderator_status": normalized["moderator_status"],
            "publication_readiness": normalized["publication_readiness"],
            "canonical_artifact": maybe_text(normalized.get("canonical_artifact")),
            "reporting_handoff_source": maybe_text(
                normalized.get("reporting_handoff_source")
            ),
            "report_basis_source": maybe_text(normalized.get("report_basis_source")),
        },
    )
    return validate_canonical_payload("council-decision", normalized)

def normalized_expert_report_payload(
    report_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(report_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["report_stage"] = expert_report_stage_from_payload(normalized)
    normalized["agent_role"] = (
        maybe_text(normalized.get("agent_role"))
        or maybe_text(normalized.get("canonical_role"))
    )
    normalized["report_id"] = (
        maybe_text(normalized.get("report_id"))
        or (
            f"expert-report-{normalized['agent_role']}-{normalized['round_id']}"
            if normalized["agent_role"]
            else "expert-report-"
            + stable_hash(
                "expert-report",
                normalized["run_id"],
                normalized["round_id"],
                normalized["report_stage"],
            )[:12]
        )
    )
    normalized["record_id"] = (
        maybe_text(normalized.get("record_id"))
        or expert_report_record_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["report_stage"],
            normalized["agent_role"],
            normalized["report_id"],
        )
    )
    normalized["handoff_status"] = normalize_reporting_handoff_status(
        normalized.get("handoff_status")
    )
    normalized["reporting_ready"] = bool(maybe_bool(normalized.get("reporting_ready")))
    normalized["reporting_blockers"] = list_items(normalized.get("reporting_blockers"))
    normalized["publication_readiness"] = (
        maybe_text(normalized.get("publication_readiness"))
        or ("ready" if normalized["reporting_ready"] else "hold")
    )
    normalized["status"] = (
        maybe_text(normalized.get("status"))
        or (
            "ready-to-publish"
            if normalized["publication_readiness"] == "ready"
            else "needs-more-evidence"
        )
    )
    normalized["summary"] = (
        maybe_text(normalized.get("summary"))
        or f"Expert report for {normalized['agent_role'] or 'unspecified-role'}."
    )
    if normalized["report_stage"] == "canonical" and not maybe_text(
        normalized.get("canonical_artifact")
    ):
        normalized["canonical_artifact"] = "expert-report"
    ensure_list_fields(
        normalized,
        "reporting_blockers",
        "selected_evidence_refs",
        "findings",
        "open_questions",
        "recommended_next_actions",
        "report_sections",
        "section_draft_refs",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "audit_refs",
        "report_packet",
    )
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("reporting_handoff_source"))
        or maybe_text(normalized.get("expert_report_draft_source"))
        or maybe_text(normalized.get("skill"))
        or "expert-report-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="expert-report",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(normalized.get("decision_id")),
            nested_text_ids(normalized.get("findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("section_draft_refs"), "section_id"),
        ),
        nested_evidence_sources=(normalized.get("findings"),),
        provenance_extra={
            "report_stage": normalized["report_stage"],
            "agent_role": normalized["agent_role"],
            "status": normalized["status"],
            "publication_readiness": normalized["publication_readiness"],
            "canonical_artifact": maybe_text(normalized.get("canonical_artifact")),
            "reporting_handoff_source": maybe_text(
                normalized.get("reporting_handoff_source")
            ),
            "expert_report_draft_source": maybe_text(
                normalized.get("expert_report_draft_source")
            ),
        },
    )
    return validate_canonical_payload("expert-report", normalized)

def normalized_final_publication_payload(
    publication_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    normalized = dict(publication_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["publication_status"] = (
        maybe_text(normalized.get("publication_status")) or "hold-release"
    )
    normalized["publication_posture"] = (
        maybe_text(normalized.get("publication_posture"))
        or (
            "release"
            if normalized["publication_status"] == "ready-for-release"
            else "withhold"
        )
    )
    normalized["publication_id"] = (
        maybe_text(normalized.get("publication_id"))
        or "final-publication-"
        + stable_hash(
            "final-publication",
            normalized["run_id"],
            normalized["round_id"],
            normalized["publication_posture"],
            maybe_text(normalized.get("decision_id")),
        )[:12]
    )
    normalized["publication_summary"] = (
        maybe_text(normalized.get("publication_summary"))
        or (
            "Round is ready for final publication."
            if normalized["publication_posture"] == "release"
            else "Final publication is currently withheld."
        )
    )
    ensure_list_fields(
        normalized,
        "published_sections",
        "decision_trace_ids",
        "decision_traces",
        "role_reports",
        "published_report_refs",
        "key_findings",
        "open_risks",
        "recommended_next_actions",
        "selected_evidence_refs",
        "operator_review_hints",
        "evidence_index",
        "uncertainty_register",
        "residual_disputes",
        "policy_recommendations",
    )
    ensure_dict_fields(
        normalized,
        "observed_inputs",
        "analysis_sync",
        "deliberation_sync",
        "decision",
        "audit_refs",
        "decision_maker_report",
    )
    decision_payload = dict_items(normalized.get("decision"))
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or maybe_text(normalized.get("reporting_handoff_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("report_basis_source"))
        or maybe_text(normalized.get("skill"))
        or "final-publication-generator"
    )
    normalized = apply_reporting_contract_defaults(
        normalized,
        object_kind="final-publication",
        decision_source=decision_source,
        lineage_sources=(
            maybe_text(decision_payload.get("decision_id")),
            normalized.get("decision_trace_ids"),
            nested_text_ids(normalized.get("role_reports"), "report_id"),
            nested_text_ids(normalized.get("key_findings"), "claim_id", "coverage_id"),
            nested_text_ids(normalized.get("evidence_index"), "evidence_id", "object_id"),
        ),
        nested_evidence_sources=(
            normalized.get("key_findings"),
            normalized.get("decision_traces"),
            normalized.get("evidence_index"),
        ),
        provenance_extra={
            "publication_status": normalized["publication_status"],
            "publication_posture": normalized["publication_posture"],
            "reporting_handoff_source": maybe_text(
                normalized.get("reporting_handoff_source")
            ),
            "report_basis_source": maybe_text(normalized.get("report_basis_source")),
            "supervisor_state_source": maybe_text(
                normalized.get("supervisor_state_source")
            ),
        },
    )
    return validate_canonical_payload("final-publication", normalized)

def report_basis_freeze_item_object_type(item_group: str, item: dict[str, Any]) -> str:
    explicit = maybe_text(item.get("object_type"))
    if explicit:
        return explicit
    if item_group == "coverages":
        return "coverage"
    return item_group.rstrip("s").replace("_", "-")

def report_basis_freeze_item_object_id(item_group: str, item: dict[str, Any]) -> str:
    for key in (
        "object_id",
        "coverage_id",
        "claim_id",
        "route_id",
        "linkage_id",
        "gap_id",
        "edge_id",
        "cluster_id",
        "map_issue_id",
    ):
        value = maybe_text(item.get(key))
        if value:
            return value
    return (
        item_group
        + "-"
        + stable_hash(
            "report-basis-freeze-item",
            item_group,
            maybe_text(item.get("summary")),
            maybe_text(item.get("issue_label")),
            json_text(item),
        )[:12]
    )

def iter_report_basis_freeze_items(
    report_basis_payload: dict[str, Any],
) -> list[tuple[str, int, dict[str, Any]]]:
    frozen_basis = (
        report_basis_payload.get("frozen_basis", {})
        if isinstance(report_basis_payload.get("frozen_basis"), dict)
        else {}
    )
    results: list[tuple[str, int, dict[str, Any]]] = []
    for item_group in PROMOTION_BASIS_ITEM_GROUPS:
        rows = (
            frozen_basis.get(item_group, [])
            if isinstance(frozen_basis.get(item_group), list)
            else []
        )
        if item_group == "coverages" and not rows:
            rows = (
                report_basis_payload.get("selected_coverages", [])
                if isinstance(report_basis_payload.get("selected_coverages"), list)
                else []
            )
        for index, item in enumerate(rows):
            if isinstance(item, dict):
                results.append((item_group, index, item))
    return results

def report_basis_freeze_item_row_id(
    basis_id: str,
    item_group: str,
    item_index: int,
    object_id: str,
) -> str:
    return "report_basis-item-" + stable_hash(
        "report-basis-freeze-item-row",
        basis_id,
        item_group,
        item_index,
        object_id,
    )[:12]

def report_basis_freeze_record_row_from_payload(
    report_basis_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "basis_id": maybe_text(report_basis_payload.get("basis_id")),
        "run_id": maybe_text(report_basis_payload.get("run_id")),
        "round_id": maybe_text(report_basis_payload.get("round_id")),
        "generated_at_utc": maybe_text(report_basis_payload.get("generated_at_utc")),
        "report_basis_status": maybe_text(report_basis_payload.get("report_basis_status")),
        "readiness_status": maybe_text(report_basis_payload.get("readiness_status")),
        "board_state_source": maybe_text(report_basis_payload.get("board_state_source")),
        "coverage_source": maybe_text(report_basis_payload.get("coverage_source")),
        "readiness_source": maybe_text(report_basis_payload.get("readiness_source")),
        "next_actions_source": maybe_text(report_basis_payload.get("next_actions_source")),
        "board_brief_source": maybe_text(report_basis_payload.get("board_brief_source")),
        "basis_selection_mode": maybe_text(
            report_basis_payload.get("basis_selection_mode")
        ),
        "basis_counts_json": json_text(report_basis_payload.get("basis_counts", {})),
        "selected_basis_object_ids_json": json_text(
            report_basis_payload.get("selected_basis_object_ids", [])
        ),
        "selected_evidence_refs_json": json_text(
            report_basis_payload.get("selected_evidence_refs", [])
        ),
        "gate_reasons_json": json_text(report_basis_payload.get("gate_reasons", [])),
        "remaining_risks_json": json_text(
            report_basis_payload.get("remaining_risks", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(report_basis_payload),
    }

def report_basis_freeze_item_row_from_payload(
    item: dict[str, Any],
    *,
    basis_id: str,
    run_id: str,
    round_id: str,
    generated_at_utc: str,
    item_group: str,
    item_index: int,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    object_id = report_basis_freeze_item_object_id(item_group, item)
    claim_id = maybe_text(item.get("claim_id"))
    if not claim_id and isinstance(item.get("claim_ids"), list):
        claim_id = maybe_text((item.get("claim_ids") or [""])[0])
    return {
        "item_row_id": report_basis_freeze_item_row_id(
            basis_id,
            item_group,
            item_index,
            object_id,
        ),
        "basis_id": maybe_text(basis_id),
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "generated_at_utc": maybe_text(generated_at_utc),
        "item_group": maybe_text(item_group),
        "item_index": coerce_int(item_index),
        "object_type": report_basis_freeze_item_object_type(item_group, item),
        "object_id": object_id,
        "issue_label": maybe_text(item.get("issue_label")),
        "claim_id": claim_id,
        "recommended_lane": maybe_text(item.get("recommended_lane")),
        "route_status": maybe_text(item.get("route_status")),
        "readiness": maybe_text(item.get("readiness")),
        "evidence_refs_json": json_text(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else []
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(item),
    }

def reporting_handoff_row_from_payload(
    handoff_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "handoff_id": maybe_text(handoff_payload.get("handoff_id")),
        "run_id": maybe_text(handoff_payload.get("run_id")),
        "round_id": maybe_text(handoff_payload.get("round_id")),
        "generated_at_utc": maybe_text(handoff_payload.get("generated_at_utc")),
        "handoff_status": maybe_text(handoff_payload.get("handoff_status")),
        "reporting_ready": 1 if bool(handoff_payload.get("reporting_ready")) else 0,
        "reporting_blockers_json": json_text(
            handoff_payload.get("reporting_blockers", [])
        ),
        "report_basis_status": maybe_text(handoff_payload.get("report_basis_status")),
        "readiness_status": maybe_text(handoff_payload.get("readiness_status")),
        "supervisor_status": maybe_text(handoff_payload.get("supervisor_status")),
        "board_state_source": maybe_text(handoff_payload.get("board_state_source")),
        "coverage_source": maybe_text(handoff_payload.get("coverage_source")),
        "report_basis_source": maybe_text(handoff_payload.get("report_basis_source")),
        "readiness_source": maybe_text(handoff_payload.get("readiness_source")),
        "board_brief_source": maybe_text(handoff_payload.get("board_brief_source")),
        "supervisor_state_source": maybe_text(
            handoff_payload.get("supervisor_state_source")
        ),
        "selected_evidence_refs_json": json_text(
            handoff_payload.get("selected_evidence_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(handoff_payload),
    }

def council_decision_record_row_from_payload(
    decision_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "record_id": maybe_text(decision_payload.get("record_id")),
        "decision_id": maybe_text(decision_payload.get("decision_id")),
        "run_id": maybe_text(decision_payload.get("run_id")),
        "round_id": maybe_text(decision_payload.get("round_id")),
        "generated_at_utc": maybe_text(decision_payload.get("generated_at_utc")),
        "decision_stage": maybe_text(decision_payload.get("decision_stage")),
        "moderator_status": maybe_text(decision_payload.get("moderator_status")),
        "reporting_ready": 1 if bool(decision_payload.get("reporting_ready")) else 0,
        "publication_readiness": maybe_text(
            decision_payload.get("publication_readiness")
        ),
        "decision_gating_json": json_text(
            decision_payload.get("decision_gating", {})
        ),
        "next_round_required": 1
        if bool(decision_payload.get("next_round_required"))
        else 0,
        "canonical_artifact": maybe_text(decision_payload.get("canonical_artifact")),
        "board_state_source": maybe_text(decision_payload.get("board_state_source")),
        "coverage_source": maybe_text(decision_payload.get("coverage_source")),
        "reporting_handoff_source": maybe_text(
            decision_payload.get("reporting_handoff_source")
        ),
        "report_basis_source": maybe_text(decision_payload.get("report_basis_source")),
        "decision_source": maybe_text(decision_payload.get("decision_source")),
        "sociologist_report_source": maybe_text(
            decision_payload.get("sociologist_report_source")
        ),
        "environmentalist_report_source": maybe_text(
            decision_payload.get("environmentalist_report_source")
        ),
        "selected_evidence_refs_json": json_text(
            decision_payload.get("selected_evidence_refs", [])
        ),
        "published_report_refs_json": json_text(
            decision_payload.get("published_report_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(decision_payload),
    }

def expert_report_record_row_from_payload(
    report_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "record_id": maybe_text(report_payload.get("record_id")),
        "report_id": maybe_text(report_payload.get("report_id")),
        "run_id": maybe_text(report_payload.get("run_id")),
        "round_id": maybe_text(report_payload.get("round_id")),
        "generated_at_utc": maybe_text(report_payload.get("generated_at_utc")),
        "report_stage": maybe_text(report_payload.get("report_stage")),
        "agent_role": maybe_text(report_payload.get("agent_role")),
        "status": maybe_text(report_payload.get("status")),
        "handoff_status": maybe_text(report_payload.get("handoff_status")),
        "reporting_ready": 1 if bool(report_payload.get("reporting_ready")) else 0,
        "publication_readiness": maybe_text(
            report_payload.get("publication_readiness")
        ),
        "canonical_artifact": maybe_text(report_payload.get("canonical_artifact")),
        "board_state_source": maybe_text(report_payload.get("board_state_source")),
        "coverage_source": maybe_text(report_payload.get("coverage_source")),
        "reporting_handoff_source": maybe_text(
            report_payload.get("reporting_handoff_source")
        ),
        "decision_source": maybe_text(report_payload.get("decision_source")),
        "expert_report_draft_source": maybe_text(
            report_payload.get("expert_report_draft_source")
        ),
        "board_brief_source": maybe_text(report_payload.get("board_brief_source")),
        "selected_evidence_refs_json": json_text(
            report_payload.get("selected_evidence_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(report_payload),
    }

def final_publication_row_from_payload(
    publication_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "publication_id": maybe_text(publication_payload.get("publication_id")),
        "run_id": maybe_text(publication_payload.get("run_id")),
        "round_id": maybe_text(publication_payload.get("round_id")),
        "generated_at_utc": maybe_text(publication_payload.get("generated_at_utc")),
        "publication_status": maybe_text(
            publication_payload.get("publication_status")
        ),
        "publication_posture": maybe_text(
            publication_payload.get("publication_posture")
        ),
        "board_state_source": maybe_text(
            publication_payload.get("board_state_source")
        ),
        "coverage_source": maybe_text(publication_payload.get("coverage_source")),
        "reporting_handoff_source": maybe_text(
            publication_payload.get("reporting_handoff_source")
        ),
        "decision_source": maybe_text(publication_payload.get("decision_source")),
        "report_basis_source": maybe_text(publication_payload.get("report_basis_source")),
        "supervisor_state_source": maybe_text(
            publication_payload.get("supervisor_state_source")
        ),
        "sociologist_report_source": maybe_text(
            publication_payload.get("sociologist_report_source")
        ),
        "environmentalist_report_source": maybe_text(
            publication_payload.get("environmentalist_report_source")
        ),
        "selected_evidence_refs_json": json_text(
            publication_payload.get("selected_evidence_refs", [])
        ),
        "published_report_refs_json": json_text(
            publication_payload.get("published_report_refs", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(publication_payload),
    }

def fetch_report_basis_freeze_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row(
        connection,
        table_name="report_basis_freeze_records",
        id_column="basis_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
    )

def fetch_report_basis_freeze_items(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> list[dict[str, Any]]:
    return fetch_json_rows(
        connection,
        table_name="report_basis_freeze_items",
        id_column="item_row_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
        extra_order_by="item_group, item_index",
    )

def fetch_reporting_handoff_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="reporting_handoffs",
        id_column="handoff_id",
        timestamp_column="generated_at_utc",
        filters={"run_id": run_id, "round_id": round_id},
    )

def fetch_council_decision_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
    decision_stage: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="council_decision_records",
        id_column="record_id",
        timestamp_column="generated_at_utc",
        filters={
            "run_id": run_id,
            "round_id": round_id,
            "decision_stage": decision_stage,
        },
    )

def fetch_expert_report_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
    report_stage: str = "",
    agent_role: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="expert_report_records",
        id_column="record_id",
        timestamp_column="generated_at_utc",
        filters={
            "run_id": run_id,
            "round_id": round_id,
            "report_stage": report_stage,
            "agent_role": agent_role,
        },
    )

def fetch_final_publication_record(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row_where(
        connection,
        table_name="final_publications",
        id_column="publication_id",
        timestamp_column="generated_at_utc",
        filters={"run_id": run_id, "round_id": round_id},
    )

def store_report_basis_freeze_record(
    run_dir: str | Path,
    *,
    report_basis_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_report_basis_freeze_payload(
        report_basis_payload if isinstance(report_basis_payload, dict) else {},
        run_id=maybe_text(
            report_basis_payload.get("run_id")
            if isinstance(report_basis_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            report_basis_payload.get("round_id")
            if isinstance(report_basis_payload, dict)
            else ""
        ),
        source_skill=maybe_text(
            report_basis_payload.get("skill")
            if isinstance(report_basis_payload, dict)
            else ""
        ),
        artifact_path=artifact_path,
    )
    basis_id = maybe_text(normalized_payload.get("basis_id"))
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    generated_at_utc = maybe_text(normalized_payload.get("generated_at_utc"))
    item_rows = iter_report_basis_freeze_items(normalized_payload)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM report_basis_freeze_records WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            connection.execute(
                "DELETE FROM report_basis_freeze_items WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            write_report_basis_freeze_record_row(
                connection,
                report_basis_freeze_record_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
            for item_group, item_index, item in item_rows:
                write_report_basis_freeze_item_row(
                    connection,
                    report_basis_freeze_item_row_from_payload(
                        item,
                        basis_id=basis_id,
                        run_id=run_id,
                        round_id=round_id,
                        generated_at_utc=generated_at_utc,
                        item_group=item_group,
                        item_index=item_index,
                        artifact_path=artifact_path,
                        record_locator=f"$.frozen_basis.{item_group}[{item_index}]",
                    ),
                )
    finally:
        connection.close()
    return normalized_payload

def load_report_basis_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_report_basis_freeze_record(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_report_basis_freeze_items(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_report_basis_freeze_items(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def store_reporting_handoff_record(
    run_dir: str | Path,
    *,
    handoff_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_reporting_handoff_payload(
        handoff_payload if isinstance(handoff_payload, dict) else {},
        run_id=maybe_text(
            handoff_payload.get("run_id")
            if isinstance(handoff_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            handoff_payload.get("round_id")
            if isinstance(handoff_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM reporting_handoffs WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            write_reporting_handoff_row(
                connection,
                reporting_handoff_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload

def load_reporting_handoff_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_reporting_handoff_record(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def store_council_decision_record(
    run_dir: str | Path,
    *,
    decision_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_council_decision_payload(
        decision_payload if isinstance(decision_payload, dict) else {},
        run_id=maybe_text(
            decision_payload.get("run_id")
            if isinstance(decision_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            decision_payload.get("round_id")
            if isinstance(decision_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    decision_stage = maybe_text(normalized_payload.get("decision_stage"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                """
                DELETE FROM council_decision_records
                WHERE run_id = ? AND round_id = ? AND decision_stage = ?
                """,
                (run_id, round_id, decision_stage),
            )
            write_council_decision_record_row(
                connection,
                council_decision_record_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload

def load_council_decision_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    decision_stage: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_council_decision_record(
            connection,
            run_id=run_id,
            round_id=round_id,
            decision_stage=decision_stage,
        )
    finally:
        connection.close()

def store_expert_report_record(
    run_dir: str | Path,
    *,
    report_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_expert_report_payload(
        report_payload if isinstance(report_payload, dict) else {},
        run_id=maybe_text(
            report_payload.get("run_id")
            if isinstance(report_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            report_payload.get("round_id")
            if isinstance(report_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    report_stage = maybe_text(normalized_payload.get("report_stage"))
    agent_role = maybe_text(normalized_payload.get("agent_role"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                """
                DELETE FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (run_id, round_id, report_stage, agent_role),
            )
            write_expert_report_record_row(
                connection,
                expert_report_record_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload

def load_expert_report_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    report_stage: str = "",
    agent_role: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_expert_report_record(
            connection,
            run_id=run_id,
            round_id=round_id,
            report_stage=report_stage,
            agent_role=agent_role,
        )
    finally:
        connection.close()

def store_final_publication_record(
    run_dir: str | Path,
    *,
    publication_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_final_publication_payload(
        publication_payload if isinstance(publication_payload, dict) else {},
        run_id=maybe_text(
            publication_payload.get("run_id")
            if isinstance(publication_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            publication_payload.get("round_id")
            if isinstance(publication_payload, dict)
            else ""
        ),
    )
    run_id = maybe_text(normalized_payload.get("run_id"))
    round_id = maybe_text(normalized_payload.get("round_id"))
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM final_publications WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            write_final_publication_row(
                connection,
                final_publication_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload

def load_final_publication_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_final_publication_record(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

__all__ = [
    "PROMOTION_BASIS_ITEM_GROUPS",
    "REPORT_AGENT_ROLES",
    "reporting_handoff_id",
    "council_decision_record_id",
    "expert_report_record_id",
    "decision_stage_from_payload",
    "expert_report_stage_from_payload",
    "normalized_report_basis_freeze_payload",
    "nested_evidence_refs",
    "nested_text_ids",
    "ensure_list_fields",
    "ensure_dict_fields",
    "apply_reporting_contract_defaults",
    "normalized_reporting_handoff_payload",
    "normalized_council_decision_payload",
    "normalized_expert_report_payload",
    "normalized_final_publication_payload",
    "report_basis_freeze_item_object_type",
    "report_basis_freeze_item_object_id",
    "iter_report_basis_freeze_items",
    "report_basis_freeze_item_row_id",
    "report_basis_freeze_record_row_from_payload",
    "report_basis_freeze_item_row_from_payload",
    "reporting_handoff_row_from_payload",
    "council_decision_record_row_from_payload",
    "expert_report_record_row_from_payload",
    "final_publication_row_from_payload",
    "fetch_report_basis_freeze_record",
    "fetch_report_basis_freeze_items",
    "fetch_reporting_handoff_record",
    "fetch_council_decision_record",
    "fetch_expert_report_record",
    "fetch_final_publication_record",
    "store_report_basis_freeze_record",
    "load_report_basis_freeze_record",
    "load_report_basis_freeze_items",
    "store_reporting_handoff_record",
    "load_reporting_handoff_record",
    "store_council_decision_record",
    "load_council_decision_record",
    "store_expert_report_record",
    "load_expert_report_record",
    "store_final_publication_record",
    "load_final_publication_record",
]
