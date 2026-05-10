from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.deliberation_target_semantics import (
    deliberation_anchor_fields,
    normalized_deliberation_target,
    source_proposal_id_from_payload,
)
from eco_council_runtime.kernel.execution.governed_execution_action_semantics import action_is_readiness_blocker
from eco_council_runtime.contracts import validate_canonical_payload
from eco_council_runtime.kernel.planes.deliberation_plane_rows import (
    cleaned_wrapper_record,
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
    write_falsification_probe_row,
    write_falsification_probe_snapshot_row,
    write_moderator_action_row,
    write_moderator_action_snapshot_row,
    write_round_readiness_assessment_row,
)
from eco_council_runtime.kernel.planes.deliberation_plane_schema import connect_db, resolve_run_dir
from eco_council_runtime.kernel.planes.deliberation_plane_rows import (
    fetch_json_rows,
    fetch_snapshot_payload,
    latest_json_row,
    latest_raw_json_row,
)

def moderator_action_snapshot_id(run_id: str, round_id: str) -> str:
    return "actions-" + stable_hash("moderator-actions", run_id, round_id)[:12]

def falsification_probe_snapshot_id(run_id: str, round_id: str) -> str:
    return "probes-" + stable_hash("falsification-probes", run_id, round_id)[:12]

def readiness_assessment_id(run_id: str, round_id: str, readiness_status: str) -> str:
    return "round-readiness-" + stable_hash(
        "round-readiness",
        run_id,
        round_id,
        readiness_status,
    )[:12]

def action_target_id(action: dict[str, Any], field_name: str) -> str:
    target = action.get("target", {}) if isinstance(action.get("target"), dict) else {}
    direct_field_name = f"target_{field_name}"
    return (
        maybe_text(action.get(direct_field_name))
        or maybe_text(action.get(field_name))
        or maybe_text(target.get(direct_field_name))
        or maybe_text(target.get(field_name))
    )

def normalized_action_payload(
    action: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    action_rank: int,
    generated_at_utc: str = "",
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(action)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc"))
        or maybe_text(generated_at_utc)
        or utc_now_iso()
    )
    normalized["action_id"] = (
        maybe_text(normalized.get("action_id"))
        or "action-"
        + stable_hash(
            "moderator-action",
            run_id,
            round_id,
            action_rank,
            maybe_text(normalized.get("action_kind")),
            maybe_text(normalized.get("objective")),
            maybe_text(normalized.get("reason")),
            action_target_id(normalized, "hypothesis_id"),
            action_target_id(normalized, "claim_id"),
            action_target_id(normalized, "ticket_id"),
        )[:12]
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    normalized["action_kind"] = maybe_text(normalized.get("action_kind")) or "follow-up"
    normalized["assigned_role"] = (
        maybe_text(normalized.get("assigned_role")) or "moderator"
    )
    normalized["objective"] = (
        maybe_text(normalized.get("objective"))
        or maybe_text(normalized.get("reason"))
        or "Advance the current round posture."
    )
    normalized["reason"] = (
        maybe_text(normalized.get("reason"))
        or maybe_text(normalized.get("objective"))
        or "Advance the current round posture."
    )
    normalized["decision_source"] = decision_source
    normalized["source_ids"] = unique_texts(list_items(normalized.get("source_ids")))
    normalized["evidence_refs"] = list_items(normalized.get("evidence_refs"))
    normalized["probe_candidate"] = bool(normalized.get("probe_candidate"))
    normalized["readiness_blocker"] = action_is_readiness_blocker(normalized)
    source_proposal_id = source_proposal_id_from_payload(normalized)
    target = normalized_deliberation_target(
        normalized.get("target"),
        object_kind=maybe_text(normalized.get("target_object_kind")),
        object_id=maybe_text(normalized.get("target_object_id")),
        issue_label=maybe_text(normalized.get("issue_label")),
        claim_id=action_target_id(normalized, "claim_id"),
        hypothesis_id=action_target_id(normalized, "hypothesis_id"),
        ticket_id=action_target_id(normalized, "ticket_id"),
        route_id=maybe_text(normalized.get("target_route_id"))
        or maybe_text(normalized.get("route_id")),
        actor_id=action_target_id(normalized, "actor_id"),
        assessment_id=maybe_text(normalized.get("target_assessment_id"))
        or maybe_text(normalized.get("assessment_id")),
        linkage_id=maybe_text(normalized.get("target_linkage_id"))
        or maybe_text(normalized.get("linkage_id")),
        gap_id=maybe_text(normalized.get("target_gap_id"))
        or maybe_text(normalized.get("gap_id")),
        map_issue_id=maybe_text(normalized.get("target_map_issue_id"))
        or maybe_text(normalized.get("map_issue_id")),
        proposal_id=action_target_id(normalized, "proposal_id"),
        round_id=normalized["round_id"],
    )
    anchor_fields = deliberation_anchor_fields(
        target,
        source_proposal_id=source_proposal_id,
    )
    normalized["target"] = target
    normalized["target_hypothesis_id"] = maybe_text(target.get("hypothesis_id"))
    normalized["target_claim_id"] = maybe_text(target.get("claim_id"))
    normalized["target_ticket_id"] = maybe_text(target.get("ticket_id"))
    normalized["target_actor_id"] = maybe_text(anchor_fields.get("target_actor_id"))
    normalized["target_proposal_id"] = maybe_text(
        anchor_fields.get("target_proposal_id")
    )
    normalized["target_object_kind"] = maybe_text(anchor_fields.get("target_object_kind"))
    normalized["target_object_id"] = maybe_text(anchor_fields.get("target_object_id"))
    normalized["issue_label"] = (
        maybe_text(anchor_fields.get("issue_label"))
        or maybe_text(normalized.get("issue_label"))
    )
    normalized["target_route_id"] = maybe_text(anchor_fields.get("target_route_id"))
    normalized["target_assessment_id"] = maybe_text(
        anchor_fields.get("target_assessment_id")
    )
    normalized["target_linkage_id"] = maybe_text(
        anchor_fields.get("target_linkage_id")
    )
    normalized["target_gap_id"] = maybe_text(anchor_fields.get("target_gap_id"))
    normalized["source_proposal_id"] = maybe_text(
        anchor_fields.get("source_proposal_id")
    )
    if normalized["source_proposal_id"]:
        normalized["source_ids"] = unique_texts(
            [*normalized["source_ids"], normalized["source_proposal_id"]]
        )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("source_ids"),
        normalized.get("source_proposal_id"),
        normalized.get("target_hypothesis_id"),
        normalized.get("target_claim_id"),
        normalized.get("target_ticket_id"),
        normalized.get("target_actor_id"),
        normalized.get("target_proposal_id"),
        normalized.get("target_object_id"),
        normalized.get("target_route_id"),
        normalized.get("target_assessment_id"),
        normalized.get("target_linkage_id"),
        normalized.get("target_gap_id"),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "agenda_source": maybe_text(normalized.get("agenda_source")),
            "assigned_role": maybe_text(normalized.get("assigned_role")),
            "action_kind": maybe_text(normalized.get("action_kind")),
            "target_actor_id": normalized.get("target_actor_id"),
            "target_proposal_id": normalized.get("target_proposal_id"),
            "source_proposal_id": normalized.get("source_proposal_id"),
        },
    )
    return validate_canonical_payload("next-action", normalized)

def normalized_probe_payload(
    probe: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    probe_index: int,
    generated_at_utc: str = "",
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(probe)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["opened_at_utc"] = (
        maybe_text(normalized.get("opened_at_utc"))
        or maybe_text(generated_at_utc)
        or utc_now_iso()
    )
    normalized["probe_id"] = (
        maybe_text(normalized.get("probe_id"))
        or "probe-"
        + stable_hash(
            "falsification-probe",
            run_id,
            round_id,
            probe_index,
            maybe_text(normalized.get("action_id")),
            maybe_text(normalized.get("probe_type")),
            maybe_text(normalized.get("probe_goal")),
            maybe_text(normalized.get("target_hypothesis_id")),
            maybe_text(normalized.get("target_claim_id")),
            maybe_text(normalized.get("target_ticket_id")),
        )[:12]
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    normalized["probe_status"] = maybe_text(normalized.get("probe_status")) or "open"
    normalized["owner_role"] = maybe_text(normalized.get("owner_role")) or "challenger"
    normalized["probe_type"] = (
        maybe_text(normalized.get("probe_type")) or "uncertainty-probe"
    )
    normalized["probe_goal"] = (
        maybe_text(normalized.get("probe_goal"))
        or maybe_text(normalized.get("falsification_question"))
        or "Probe the current target."
    )
    normalized["falsification_question"] = (
        maybe_text(normalized.get("falsification_question"))
        or f"What evidence would materially weaken: {normalized['probe_goal']}"
    )
    normalized["decision_source"] = decision_source
    normalized["source_ids"] = unique_texts(list_items(normalized.get("source_ids")))
    normalized["requested_skills"] = unique_texts(
        list_items(normalized.get("requested_skills"))
    )
    normalized["success_criteria"] = unique_texts(
        list_items(normalized.get("success_criteria"))
    )
    normalized["disconfirm_signals"] = unique_texts(
        list_items(normalized.get("disconfirm_signals"))
    )
    normalized["evidence_refs"] = list_items(normalized.get("evidence_refs"))
    source_proposal_id = source_proposal_id_from_payload(normalized)
    target = normalized_deliberation_target(
        normalized.get("target"),
        object_kind=maybe_text(normalized.get("target_object_kind")),
        object_id=maybe_text(normalized.get("target_object_id")),
        issue_label=maybe_text(normalized.get("issue_label")),
        claim_id=maybe_text(normalized.get("target_claim_id")),
        hypothesis_id=maybe_text(normalized.get("target_hypothesis_id")),
        ticket_id=maybe_text(normalized.get("target_ticket_id")),
        route_id=maybe_text(normalized.get("target_route_id"))
        or maybe_text(normalized.get("route_id")),
        actor_id=action_target_id(normalized, "actor_id"),
        assessment_id=maybe_text(normalized.get("target_assessment_id"))
        or maybe_text(normalized.get("assessment_id")),
        linkage_id=maybe_text(normalized.get("target_linkage_id"))
        or maybe_text(normalized.get("linkage_id")),
        gap_id=maybe_text(normalized.get("target_gap_id"))
        or maybe_text(normalized.get("gap_id")),
        map_issue_id=maybe_text(normalized.get("target_map_issue_id"))
        or maybe_text(normalized.get("map_issue_id")),
        proposal_id=action_target_id(normalized, "proposal_id"),
        round_id=normalized["round_id"],
        action_id=maybe_text(normalized.get("action_id")),
    )
    anchor_fields = deliberation_anchor_fields(
        target,
        source_proposal_id=source_proposal_id,
    )
    normalized["target"] = target
    normalized["target_hypothesis_id"] = maybe_text(target.get("hypothesis_id"))
    normalized["target_claim_id"] = maybe_text(target.get("claim_id"))
    normalized["target_ticket_id"] = maybe_text(target.get("ticket_id"))
    normalized["target_actor_id"] = maybe_text(anchor_fields.get("target_actor_id"))
    normalized["target_proposal_id"] = maybe_text(
        anchor_fields.get("target_proposal_id")
    )
    normalized["target_object_kind"] = maybe_text(anchor_fields.get("target_object_kind"))
    normalized["target_object_id"] = maybe_text(anchor_fields.get("target_object_id"))
    normalized["issue_label"] = (
        maybe_text(anchor_fields.get("issue_label"))
        or maybe_text(normalized.get("issue_label"))
    )
    normalized["target_route_id"] = maybe_text(anchor_fields.get("target_route_id"))
    normalized["target_assessment_id"] = maybe_text(
        anchor_fields.get("target_assessment_id")
    )
    normalized["target_linkage_id"] = maybe_text(
        anchor_fields.get("target_linkage_id")
    )
    normalized["target_gap_id"] = maybe_text(anchor_fields.get("target_gap_id"))
    normalized["source_proposal_id"] = maybe_text(
        anchor_fields.get("source_proposal_id")
    )
    if normalized["source_proposal_id"]:
        normalized["source_ids"] = unique_texts(
            [*normalized["source_ids"], normalized["source_proposal_id"]]
        )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("source_ids"),
        maybe_text(normalized.get("action_id")),
        normalized.get("source_proposal_id"),
        normalized.get("target_hypothesis_id"),
        normalized.get("target_claim_id"),
        normalized.get("target_ticket_id"),
        normalized.get("target_actor_id"),
        normalized.get("target_proposal_id"),
        normalized.get("target_object_id"),
        normalized.get("target_route_id"),
        normalized.get("target_assessment_id"),
        normalized.get("target_linkage_id"),
        normalized.get("target_gap_id"),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "owner_role": maybe_text(normalized.get("owner_role")),
            "probe_status": maybe_text(normalized.get("probe_status")),
            "probe_type": maybe_text(normalized.get("probe_type")),
            "target_actor_id": normalized.get("target_actor_id"),
            "target_proposal_id": normalized.get("target_proposal_id"),
            "source_proposal_id": normalized.get("source_proposal_id"),
        },
    )
    return validate_canonical_payload("probe", normalized)

def normalized_readiness_payload(
    readiness_payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(readiness_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id")) or run_id
    normalized["round_id"] = maybe_text(normalized.get("round_id")) or round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "blocked"
    )
    normalized["readiness_id"] = (
        maybe_text(normalized.get("readiness_id"))
        or readiness_assessment_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["readiness_status"],
        )
    )
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "policy-fallback"
    )
    normalized["decision_source"] = decision_source
    normalized["selected_basis_object_ids"] = list_items(
        normalized.get("selected_basis_object_ids")
    )
    normalized["basis_object_ids"] = list_items(normalized.get("basis_object_ids"))
    normalized["opinion_ids"] = list_items(normalized.get("opinion_ids"))
    normalized["evidence_refs"] = list_items(
        normalized.get("evidence_refs")
    ) or list_items(normalized.get("selected_evidence_refs"))
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized.get("selected_basis_object_ids"),
        normalized.get("basis_object_ids"),
        normalized.get("opinion_ids"),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "board_state_source": maybe_text(normalized.get("board_state_source")),
            "coverage_source": maybe_text(normalized.get("coverage_source")),
            "next_actions_source": maybe_text(normalized.get("next_actions_source")),
            "probes_source": maybe_text(normalized.get("probes_source")),
        },
    )
    return validate_canonical_payload("readiness-assessment", normalized)

def moderator_action_row_from_payload(
    action: dict[str, Any],
    *,
    generated_at_utc: str,
    action_rank: int,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    target = (
        action.get("target", {})
        if isinstance(action.get("target"), dict)
        else {}
    )
    return {
        "action_id": maybe_text(action.get("action_id")),
        "run_id": maybe_text(action.get("run_id")),
        "round_id": maybe_text(action.get("round_id")),
        "generated_at_utc": maybe_text(generated_at_utc),
        "action_rank": coerce_int(action_rank),
        "action_kind": maybe_text(action.get("action_kind")),
        "priority": maybe_text(action.get("priority")),
        "assigned_role": maybe_text(action.get("assigned_role")),
        "target_hypothesis_id": action_target_id(action, "hypothesis_id"),
        "target_claim_id": action_target_id(action, "claim_id"),
        "target_ticket_id": action_target_id(action, "ticket_id"),
        "target_actor_id": action_target_id(action, "actor_id"),
        "target_proposal_id": action_target_id(action, "proposal_id"),
        "target_object_kind": maybe_text(action.get("target_object_kind"))
        or maybe_text(target.get("object_kind")),
        "target_object_id": maybe_text(action.get("target_object_id"))
        or maybe_text(target.get("object_id")),
        "issue_label": maybe_text(action.get("issue_label"))
        or maybe_text(target.get("issue_label")),
        "target_route_id": maybe_text(action.get("target_route_id"))
        or maybe_text(target.get("route_id")),
        "target_assessment_id": maybe_text(action.get("target_assessment_id"))
        or maybe_text(target.get("assessment_id")),
        "target_linkage_id": maybe_text(action.get("target_linkage_id"))
        or maybe_text(target.get("linkage_id")),
        "target_gap_id": maybe_text(action.get("target_gap_id"))
        or maybe_text(target.get("gap_id")),
        "source_proposal_id": maybe_text(action.get("source_proposal_id")),
        "controversy_gap": maybe_text(action.get("controversy_gap")),
        "recommended_lane": maybe_text(action.get("recommended_lane")),
        "probe_candidate": 1 if bool(action.get("probe_candidate")) else 0,
        "readiness_blocker": 1 if action_is_readiness_blocker(action) else 0,
        "objective": maybe_text(action.get("objective")),
        "reason": maybe_text(action.get("reason")),
        "evidence_refs_json": json_text(
            action.get("evidence_refs", [])
            if isinstance(action.get("evidence_refs"), list)
            else []
        ),
        "source_ids_json": json_text(
            action.get("source_ids", [])
            if isinstance(action.get("source_ids"), list)
            else []
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(action),
    }

def falsification_probe_row_from_payload(
    probe: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    target = probe.get("target", {}) if isinstance(probe.get("target"), dict) else {}
    return {
        "probe_id": maybe_text(probe.get("probe_id")),
        "run_id": maybe_text(probe.get("run_id")),
        "round_id": maybe_text(probe.get("round_id")),
        "opened_at_utc": maybe_text(probe.get("opened_at_utc")),
        "probe_status": maybe_text(probe.get("probe_status")),
        "action_id": maybe_text(probe.get("action_id")),
        "priority": maybe_text(probe.get("priority")),
        "owner_role": maybe_text(probe.get("owner_role")),
        "target_hypothesis_id": maybe_text(probe.get("target_hypothesis_id")),
        "target_claim_id": maybe_text(probe.get("target_claim_id")),
        "target_ticket_id": maybe_text(probe.get("target_ticket_id")),
        "target_actor_id": action_target_id(probe, "actor_id"),
        "target_proposal_id": action_target_id(probe, "proposal_id"),
        "target_object_kind": maybe_text(probe.get("target_object_kind"))
        or maybe_text(target.get("object_kind")),
        "target_object_id": maybe_text(probe.get("target_object_id"))
        or maybe_text(target.get("object_id")),
        "issue_label": maybe_text(probe.get("issue_label"))
        or maybe_text(target.get("issue_label")),
        "target_route_id": maybe_text(probe.get("target_route_id"))
        or maybe_text(target.get("route_id")),
        "target_assessment_id": maybe_text(probe.get("target_assessment_id"))
        or maybe_text(target.get("assessment_id")),
        "target_linkage_id": maybe_text(probe.get("target_linkage_id"))
        or maybe_text(target.get("linkage_id")),
        "target_gap_id": maybe_text(probe.get("target_gap_id"))
        or maybe_text(target.get("gap_id")),
        "source_proposal_id": maybe_text(probe.get("source_proposal_id")),
        "probe_type": maybe_text(probe.get("probe_type")),
        "controversy_gap": maybe_text(probe.get("controversy_gap")),
        "recommended_lane": maybe_text(probe.get("recommended_lane")),
        "probe_goal": maybe_text(probe.get("probe_goal")),
        "falsification_question": maybe_text(probe.get("falsification_question")),
        "requested_skills_json": json_text(
            probe.get("requested_skills", [])
            if isinstance(probe.get("requested_skills"), list)
            else []
        ),
        "evidence_refs_json": json_text(
            probe.get("evidence_refs", [])
            if isinstance(probe.get("evidence_refs"), list)
            else []
        ),
        "source_ids_json": json_text(
            probe.get("source_ids", [])
            if isinstance(probe.get("source_ids"), list)
            else []
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(probe),
    }

def round_readiness_assessment_row_from_payload(
    readiness_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "readiness_id": maybe_text(readiness_payload.get("readiness_id")),
        "run_id": maybe_text(readiness_payload.get("run_id")),
        "round_id": maybe_text(readiness_payload.get("round_id")),
        "generated_at_utc": maybe_text(readiness_payload.get("generated_at_utc")),
        "readiness_status": maybe_text(readiness_payload.get("readiness_status")),
        "sufficient_for_report_basis": 1
        if bool(readiness_payload.get("sufficient_for_report_basis"))
        else 0,
        "board_state_source": maybe_text(readiness_payload.get("board_state_source")),
        "coverage_source": maybe_text(readiness_payload.get("coverage_source")),
        "next_actions_source": maybe_text(readiness_payload.get("next_actions_source")),
        "probes_source": maybe_text(readiness_payload.get("probes_source")),
        "agenda_counts_json": json_text(readiness_payload.get("agenda_counts", {})),
        "counts_json": json_text(readiness_payload.get("counts", {})),
        "controversy_gap_counts_json": json_text(
            readiness_payload.get("controversy_gap_counts", {})
        ),
        "probe_type_counts_json": json_text(
            readiness_payload.get("probe_type_counts", {})
        ),
        "gate_reasons_json": json_text(readiness_payload.get("gate_reasons", [])),
        "recommended_next_skills_json": json_text(
            readiness_payload.get("recommended_next_skills", [])
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(readiness_payload),
    }

def moderator_action_snapshot_row_from_payload(
    snapshot_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "snapshot_id": maybe_text(snapshot_payload.get("snapshot_id")),
        "run_id": maybe_text(snapshot_payload.get("run_id")),
        "round_id": maybe_text(snapshot_payload.get("round_id")),
        "generated_at_utc": maybe_text(snapshot_payload.get("generated_at_utc")),
        "action_source": maybe_text(snapshot_payload.get("action_source")) or "next-actions-artifact",
        "board_state_source": maybe_text(snapshot_payload.get("board_state_source")),
        "coverage_source": maybe_text(snapshot_payload.get("coverage_source")),
        "action_count": coerce_int(
            snapshot_payload.get("action_count")
            or (
                len(snapshot_payload.get("actions", []))
                if isinstance(snapshot_payload.get("actions"), list)
                else 0
            )
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(snapshot_payload),
    }


def action_items_from_snapshot(snapshot_payload: dict[str, Any]) -> list[dict[str, Any]]:
    actions = snapshot_payload.get("actions", [])
    return [item for item in actions if isinstance(item, dict)] if isinstance(actions, list) else []


def falsification_probe_snapshot_row_from_payload(
    snapshot_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "snapshot_id": maybe_text(snapshot_payload.get("snapshot_id")),
        "run_id": maybe_text(snapshot_payload.get("run_id")),
        "round_id": maybe_text(snapshot_payload.get("round_id")),
        "generated_at_utc": maybe_text(snapshot_payload.get("generated_at_utc")),
        "action_source": maybe_text(snapshot_payload.get("action_source")) or "falsification-probes-artifact",
        "board_state_source": maybe_text(snapshot_payload.get("board_state_source")),
        "coverage_source": maybe_text(snapshot_payload.get("coverage_source")),
        "probe_count": coerce_int(
            snapshot_payload.get("probe_count")
            or (
                len(snapshot_payload.get("probes", []))
                if isinstance(snapshot_payload.get("probes"), list)
                else 0
            )
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(snapshot_payload),
    }

def build_moderator_action_payload(
    actions: list[dict[str, Any]],
    *,
    snapshot_payload: dict[str, Any] | None = None,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any]:
    payload = dict(snapshot_payload) if isinstance(snapshot_payload, dict) else {}
    action_run_id = maybe_text(actions[0].get("run_id")) if actions else ""
    action_round_id = maybe_text(actions[0].get("round_id")) if actions else ""
    action_generated_at = maybe_text(actions[-1].get("generated_at_utc")) if actions else ""
    payload["run_id"] = maybe_text(payload.get("run_id")) or maybe_text(run_id) or action_run_id
    payload["round_id"] = (
        maybe_text(payload.get("round_id")) or maybe_text(round_id) or action_round_id
    )
    payload["generated_at_utc"] = (
        maybe_text(payload.get("generated_at_utc")) or action_generated_at or utc_now_iso()
    )
    payload["actions"] = [
        cleaned_wrapper_record(
            dict(action),
            metadata_fields=("action_rank", "artifact_path", "record_locator"),
            optional_empty_fields=(
                "controversy_gap",
                "recommended_lane",
                "issue_label",
                "target_actor_id",
                "target_proposal_id",
                "source_proposal_id",
            ),
        )
        for action in actions
    ]
    payload["action_count"] = len(actions)
    payload["action_source"] = (
        maybe_text(payload.get("action_source")) or "deliberation-plane-actions"
    )
    return payload

def build_falsification_probe_payload(
    probes: list[dict[str, Any]],
    *,
    snapshot_payload: dict[str, Any] | None = None,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any]:
    payload = dict(snapshot_payload) if isinstance(snapshot_payload, dict) else {}
    probe_run_id = maybe_text(probes[0].get("run_id")) if probes else ""
    probe_round_id = maybe_text(probes[0].get("round_id")) if probes else ""
    probe_generated_at = maybe_text(probes[-1].get("opened_at_utc")) if probes else ""
    payload["run_id"] = maybe_text(payload.get("run_id")) or maybe_text(run_id) or probe_run_id
    payload["round_id"] = (
        maybe_text(payload.get("round_id")) or maybe_text(round_id) or probe_round_id
    )
    payload["generated_at_utc"] = (
        maybe_text(payload.get("generated_at_utc")) or probe_generated_at or utc_now_iso()
    )
    payload["probes"] = [
        cleaned_wrapper_record(
            dict(probe),
            metadata_fields=("artifact_path", "record_locator"),
            optional_empty_fields=(
                "action_id",
                "controversy_gap",
                "recommended_lane",
                "target_actor_id",
                "target_proposal_id",
                "source_proposal_id",
            ),
        )
        for probe in probes
    ]
    payload["probe_count"] = len(probes)
    return payload

def fetch_moderator_action_records(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> list[dict[str, Any]]:
    return fetch_json_rows(
        connection,
        table_name="moderator_actions",
        id_column="action_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
        extra_order_by="action_rank",
    )

def fetch_falsification_probe_records(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> list[dict[str, Any]]:
    return fetch_json_rows(
        connection,
        table_name="falsification_probes",
        id_column="probe_id",
        timestamp_column="opened_at_utc",
        run_id=run_id,
        round_id=round_id,
    )

def fetch_round_readiness_assessment(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    return latest_raw_json_row(
        connection,
        table_name="round_readiness_assessments",
        id_column="readiness_id",
        timestamp_column="generated_at_utc",
        run_id=run_id,
        round_id=round_id,
    )

def store_moderator_action_records(
    run_dir: str | Path,
    *,
    action_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(action_snapshot) if isinstance(action_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    generated_at_utc = maybe_text(snapshot_payload.get("generated_at_utc")) or utc_now_iso()
    actions = action_items_from_snapshot(snapshot_payload)
    normalized_actions = [
        normalized_action_payload(
            action,
            run_id=run_id,
            round_id=round_id,
            action_rank=index,
            generated_at_utc=generated_at_utc,
            source_skill=maybe_text(snapshot_payload.get("skill")),
            artifact_path=artifact_path,
        )
        for index, action in enumerate(actions)
        if isinstance(action, dict)
    ]
    snapshot_payload["generated_at_utc"] = generated_at_utc
    snapshot_payload["actions"] = normalized_actions
    snapshot_payload["action_count"] = len(normalized_actions)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM moderator_actions WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, action in enumerate(normalized_actions):
                write_moderator_action_row(
                    connection,
                    moderator_action_row_from_payload(
                        action,
                        generated_at_utc=generated_at_utc,
                        action_rank=index,
                        artifact_path=artifact_path,
                        record_locator=f"$.actions[{index}]",
                    ),
                )
    finally:
        connection.close()
    return snapshot_payload

def store_moderator_action_snapshot(
    run_dir: str | Path,
    *,
    action_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(action_snapshot) if isinstance(action_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    snapshot_payload["snapshot_id"] = (
        maybe_text(snapshot_payload.get("snapshot_id"))
        or moderator_action_snapshot_id(run_id, round_id)
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_moderator_action_snapshot_row(
                connection,
                moderator_action_snapshot_row_from_payload(
                    snapshot_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return snapshot_payload

def load_moderator_action_records(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_moderator_action_records(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_moderator_action_snapshot(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_snapshot_payload(
            connection,
            table_name="moderator_action_snapshots",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def store_falsification_probe_records(
    run_dir: str | Path,
    *,
    probe_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(probe_snapshot) if isinstance(probe_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    probes = (
        snapshot_payload.get("probes", [])
        if isinstance(snapshot_payload.get("probes"), list)
        else []
    )
    normalized_probes = [
        normalized_probe_payload(
            probe,
            run_id=run_id,
            round_id=round_id,
            probe_index=index,
            generated_at_utc=maybe_text(snapshot_payload.get("generated_at_utc")),
            source_skill=maybe_text(snapshot_payload.get("skill")),
            artifact_path=artifact_path,
        )
        for index, probe in enumerate(probes)
        if isinstance(probe, dict)
    ]
    snapshot_payload["generated_at_utc"] = (
        maybe_text(snapshot_payload.get("generated_at_utc"))
        or utc_now_iso()
    )
    snapshot_payload["probes"] = normalized_probes
    snapshot_payload["probe_count"] = len(normalized_probes)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM falsification_probes WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, probe in enumerate(normalized_probes):
                write_falsification_probe_row(
                    connection,
                    falsification_probe_row_from_payload(
                        probe,
                        artifact_path=artifact_path,
                        record_locator=f"$.probes[{index}]",
                    ),
                )
    finally:
        connection.close()
    return snapshot_payload

def store_falsification_probe_snapshot(
    run_dir: str | Path,
    *,
    probe_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(probe_snapshot) if isinstance(probe_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    snapshot_payload["snapshot_id"] = (
        maybe_text(snapshot_payload.get("snapshot_id"))
        or falsification_probe_snapshot_id(run_id, round_id)
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_falsification_probe_snapshot_row(
                connection,
                falsification_probe_snapshot_row_from_payload(
                    snapshot_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return snapshot_payload

def load_falsification_probe_records(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_falsification_probe_records(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_falsification_probe_snapshot(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_snapshot_payload(
            connection,
            table_name="falsification_probe_snapshots",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def store_round_readiness_assessment(
    run_dir: str | Path,
    *,
    readiness_payload: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_payload = normalized_readiness_payload(
        readiness_payload if isinstance(readiness_payload, dict) else {},
        run_id=maybe_text(
            readiness_payload.get("run_id")
            if isinstance(readiness_payload, dict)
            else ""
        ),
        round_id=maybe_text(
            readiness_payload.get("round_id")
            if isinstance(readiness_payload, dict)
            else ""
        ),
        source_skill=maybe_text(
            readiness_payload.get("skill")
            if isinstance(readiness_payload, dict)
            else ""
        ),
        artifact_path=artifact_path,
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_round_readiness_assessment_row(
                connection,
                round_readiness_assessment_row_from_payload(
                    normalized_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return normalized_payload

def load_round_readiness_assessment(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_round_readiness_assessment(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

__all__ = [
    "moderator_action_snapshot_id",
    "falsification_probe_snapshot_id",
    "readiness_assessment_id",
    "action_target_id",
    "normalized_action_payload",
    "normalized_probe_payload",
    "normalized_readiness_payload",
    "moderator_action_row_from_payload",
    "falsification_probe_row_from_payload",
    "round_readiness_assessment_row_from_payload",
    "moderator_action_snapshot_row_from_payload",
    "falsification_probe_snapshot_row_from_payload",
    "build_moderator_action_payload",
    "build_falsification_probe_payload",
    "fetch_moderator_action_records",
    "fetch_falsification_probe_records",
    "fetch_round_readiness_assessment",
    "store_moderator_action_records",
    "store_moderator_action_snapshot",
    "load_moderator_action_records",
    "load_moderator_action_snapshot",
    "store_falsification_probe_records",
    "store_falsification_probe_snapshot",
    "load_falsification_probe_records",
    "load_falsification_probe_snapshot",
    "store_round_readiness_assessment",
    "load_round_readiness_assessment",
]
