from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eco_council_runtime.objects.council import (
    append_evidence_bundle_record,
    append_finding_record,
    query_council_objects,
)
from eco_council_runtime.kernel.planes.analysis_plane import query_spatiotemporal_relation_cues
from .reporting_objects import store_report_section_draft_record

SKILL_NAME = "materialize-spatiotemporal-relation-evidence-packet"
PACKET_OBJECT_KIND = "spatiotemporal-relation-evidence-packet"

ALLOWED_REPORT_PHRASES = [
    "candidate spatiotemporal relation cue",
    "consistent with the specified temporal or spatial window",
    "not consistent with the specified temporal or spatial window",
    "needs further investigation",
    "cannot independently support causality or attribution",
]

PROHIBITED_REPORT_CLAIMS = [
    "proves transport",
    "confirms pollution source",
    "excludes local sources",
    "establishes model attribution",
]


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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


def unique_values(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    results: list[Any] = []
    for value in values:
        try:
            key = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        except TypeError:
            key = maybe_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        results.append(value)
    return results


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_output_path(run_dir: Path, output_path: str, round_id: str) -> Path:
    text = maybe_text(output_path)
    if text:
        candidate = Path(text).expanduser()
        if not candidate.is_absolute():
            candidate = run_dir / candidate
        return candidate.resolve()
    return (run_dir / "reporting" / f"spatiotemporal_relation_evidence_packet_{round_id}.json").resolve()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def artifact_ref(path: Path, locator: str = "$") -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": locator,
        "artifact_ref": f"{path}:{locator}",
    }


def relation_report_posture(relation: dict[str, Any]) -> str:
    status = maybe_text(relation.get("relation_status"))
    if status == "candidate":
        return "candidate-only"
    if status in {"weak-candidate", "needs-human-review"}:
        return "weak-or-needs-review"
    return "not-report-support"


def relation_summary(relation: dict[str, Any]) -> dict[str, Any]:
    return {
        "relation_id": maybe_text(relation.get("relation_id")),
        "relation_type": maybe_text(relation.get("relation_type")),
        "relation_status": maybe_text(relation.get("relation_status")),
        "source_signal_id": maybe_text(relation.get("source_signal_id")),
        "target_signal_id": maybe_text(relation.get("target_signal_id")),
        "context_signal_ids": unique_texts(list_items(relation.get("context_signal_ids"))),
        "source_role": maybe_text(relation.get("source_role")),
        "target_role": maybe_text(relation.get("target_role")),
        "time_delta": dict_items(relation.get("time_delta")),
        "distance": dict_items(relation.get("distance")),
        "rejection_reasons": unique_texts(list_items(relation.get("rejection_reasons"))),
        "caveats": unique_texts(list_items(relation.get("caveats"))),
        "report_posture": relation_report_posture(relation),
        "evidence_refs": unique_values(list_items(relation.get("evidence_refs"))),
    }


def relation_id_from_object(payload: dict[str, Any]) -> str:
    explicit = maybe_text(payload.get("relation_id"))
    if explicit:
        return explicit
    target = dict_items(payload.get("target"))
    if maybe_text(target.get("object_kind")) == "spatiotemporal-relation-cue":
        return maybe_text(target.get("object_id"))
    if maybe_text(payload.get("target_kind")) == "spatiotemporal-relation-cue":
        return maybe_text(payload.get("target_id"))
    if maybe_text(payload.get("target_object_kind")) == "spatiotemporal-relation-cue":
        return maybe_text(payload.get("target_object_id"))
    return ""


def object_id_for_kind(object_kind: str, payload: dict[str, Any]) -> str:
    if object_kind == "challenge":
        return maybe_text(payload.get("ticket_id"))
    if object_kind == "review-comment":
        return maybe_text(payload.get("comment_id"))
    if object_kind == "probe":
        return maybe_text(payload.get("probe_id"))
    return ""


def collect_relation_deliberation_objects(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    relation_ids: list[str],
) -> list[dict[str, Any]]:
    relation_id_set = set(relation_ids)
    if not relation_id_set:
        return []
    results: list[dict[str, Any]] = []
    for object_kind in ("challenge", "review-comment", "probe"):
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=200,
        )
        objects = payload.get("objects", []) if isinstance(payload.get("objects"), list) else []
        for item in objects:
            if not isinstance(item, dict):
                continue
            relation_id = relation_id_from_object(item)
            if relation_id_set and relation_id not in relation_id_set:
                continue
            if not relation_id:
                continue
            results.append(
                {
                    "object_kind": object_kind,
                    "object_id": object_id_for_kind(object_kind, item),
                    "relation_id": relation_id,
                    "status": maybe_text(item.get("status"))
                    or maybe_text(item.get("probe_status")),
                    "author_or_owner_role": maybe_text(item.get("author_role"))
                    or maybe_text(item.get("owner_role")),
                    "objection_code": maybe_text(item.get("objection_code")),
                    "challenged_rule": maybe_text(item.get("challenged_rule")),
                    "alternative_explanation": maybe_text(item.get("alternative_explanation")),
                    "required_followup_evidence": unique_texts(
                        list_items(item.get("required_followup_evidence"))
                    ),
                    "report_risk": maybe_text(item.get("report_risk")),
                    "evidence_refs": unique_values(list_items(item.get("evidence_refs"))),
                    "lineage": unique_texts(list_items(item.get("lineage"))),
                }
            )
    return results


def uncertainty_register(
    relations: list[dict[str, Any]],
    objections: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for relation in relations:
        relation_id = maybe_text(relation.get("relation_id"))
        status = maybe_text(relation.get("relation_status"))
        if status != "candidate":
            rows.append(
                {
                    "uncertainty_id": "uncertainty-" + stable_hash(relation_id, status)[:12],
                    "relation_id": relation_id,
                    "uncertainty_kind": "relation-status",
                    "summary": f"Relation status is {status or 'unknown'}.",
                    "report_constraint": "Do not use this relation as positive report support unless separately reviewed.",
                }
            )
        for reason in unique_texts(list_items(relation.get("rejection_reasons"))):
            rows.append(
                {
                    "uncertainty_id": "uncertainty-" + stable_hash(relation_id, reason)[:12],
                    "relation_id": relation_id,
                    "uncertainty_kind": "rejection-reason",
                    "summary": reason,
                    "report_constraint": "Keep rejected or rule-failed relation rows out of positive report support.",
                }
            )
        for caveat in unique_texts(list_items(relation.get("caveats"))):
            rows.append(
                {
                    "uncertainty_id": "uncertainty-" + stable_hash(relation_id, caveat)[:12],
                    "relation_id": relation_id,
                    "uncertainty_kind": "relation-caveat",
                    "summary": caveat,
                    "report_constraint": "Carry caveat text with any report-facing relation reference.",
                }
            )
    for objection in objections:
        code = maybe_text(objection.get("objection_code")) or "relation-objection"
        object_id = maybe_text(objection.get("object_id"))
        rows.append(
            {
                "uncertainty_id": "uncertainty-" + stable_hash(object_id, code)[:12],
                "relation_id": maybe_text(objection.get("relation_id")),
                "uncertainty_kind": "challenger-objection",
                "summary": code,
                "source_object_kind": maybe_text(objection.get("object_kind")),
                "source_object_id": object_id,
                "report_constraint": maybe_text(objection.get("report_risk"))
                or "Resolve or carry objection before report-facing relation use.",
            }
        )
    return unique_values(rows)


def build_report_section_text(packet_id: str, summary: dict[str, Any]) -> str:
    candidate_count = int(summary.get("candidate_count") or 0)
    weak_count = int(summary.get("weak_or_rejected_count") or 0)
    objection_count = int(summary.get("challenger_objection_count") or 0)
    return (
        f"Spatiotemporal relation packet {packet_id} records {candidate_count} "
        f"candidate relation cue(s), {weak_count} weak or rejected relation cue(s), "
        f"and {objection_count} challenger objection(s). These rows may describe "
        "candidate consistency with the configured time or distance window, but they "
        "do not establish causality, transport, source attribution, or exclusion of "
        "alternative local sources. Any report use should preserve the uncertainty "
        "register and the listed follow-up evidence requirements."
    )


def materialize_spatiotemporal_relation_evidence_packet(
    *,
    run_dir: str | Path,
    run_id: str,
    round_id: str,
    relation_id: str = "",
    relation_status: str = "",
    output_path: str = "",
    write_basis_objects: bool = False,
    report_id: str = "",
    section_key: str = "spatiotemporal-relation-evidence",
    agent_role: str = "environmental-investigator",
    report_agent_role: str = "report-editor",
    confidence: float = 0.55,
    limit: int = 200,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, round_id)
    relation_query = query_spatiotemporal_relation_cues(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        relation_id=relation_id,
        relation_status=relation_status,
        latest_only=True,
        limit=limit,
    )
    relations = [
        item
        for item in list_items(relation_query.get("relations"))
        if isinstance(item, dict)
    ]
    relation_ids = unique_texts([relation.get("relation_id") for relation in relations])
    objections = collect_relation_deliberation_objects(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        relation_ids=relation_ids,
    )
    summaries = [relation_summary(relation) for relation in relations]
    accepted_relation_cue_ids = [
        maybe_text(relation.get("relation_id"))
        for relation in relations
        if maybe_text(relation.get("relation_status")) == "candidate"
    ]
    rejected_or_weak_relation_cue_ids = [
        maybe_text(relation.get("relation_id"))
        for relation in relations
        if maybe_text(relation.get("relation_status"))
        in {"weak-candidate", "insufficient-basis", "rejected-by-rule", "needs-human-review"}
    ]
    relation_evidence_refs = unique_values(
        [
            ref
            for relation in relations
            for ref in list_items(relation.get("evidence_refs"))
        ]
    )
    objection_evidence_refs = unique_values(
        [
            ref
            for objection in objections
            for ref in list_items(objection.get("evidence_refs"))
        ]
    )
    evidence_refs = unique_values([*relation_evidence_refs, *objection_evidence_refs])
    source_signal_ids = unique_texts(
        [
            relation.get("source_signal_id")
            for relation in relations
        ]
        + [
            relation.get("target_signal_id")
            for relation in relations
        ]
    )
    lineage = unique_texts(
        relation_ids
        + source_signal_ids
        + [
            item.get("object_id")
            for item in objections
        ]
    )
    packet_id = "spatiotemporal-relation-evidence-packet-" + stable_hash(
        run_id,
        round_id,
        relation_id,
        relation_status,
        ",".join(relation_ids),
    )[:12]
    packet_ref = artifact_ref(output_file, "$")
    packet_summary = {
        "relation_count": len(relations),
        "candidate_count": len(accepted_relation_cue_ids),
        "weak_or_rejected_count": len(rejected_or_weak_relation_cue_ids),
        "challenger_objection_count": len(objections),
        "evidence_ref_count": len(evidence_refs),
    }
    status = "completed" if relations else "insufficient-relation-basis"
    warnings: list[dict[str, Any]] = []
    if not relations:
        warnings.append(
            {
                "code": "no-relation-cues",
                "message": "No spatiotemporal relation cues matched the requested packet filters.",
            }
        )
    if write_basis_objects and not relations:
        warnings.append(
            {
                "code": "basis-write-skipped",
                "message": "Basis objects were not written because the packet has no relation cues.",
            }
        )
    missing_relation_artifact_paths = unique_texts(
        [
            item.get("artifact_path")
            for item in list_items(relation_query.get("items"))
            if isinstance(item, dict)
            and maybe_text(item.get("artifact_path"))
            and not bool(item.get("artifact_present"))
        ]
    )
    if missing_relation_artifact_paths:
        warnings.append(
            {
                "code": "relation-cue-artifact-missing-db-recovered",
                "message": (
                    "One or more relation cue artifacts were missing; packet materialization "
                    "used DB-backed analysis result rows."
                ),
                "artifact_paths": missing_relation_artifact_paths,
            }
        )

    basis_evidence_refs = unique_values([packet_ref, *evidence_refs])
    basis_lineage = unique_texts([packet_id, *lineage])
    target_payload = {"object_kind": PACKET_OBJECT_KIND, "object_id": packet_id}
    finding_payload = {
        "run_id": run_id,
        "round_id": round_id,
        "finding_kind": PACKET_OBJECT_KIND,
        "agent_role": maybe_text(agent_role) or "environmental-investigator",
        "status": "submitted",
        "title": "Spatiotemporal relation evidence packet",
        "summary": (
            "Relation packet records candidate, weak, or rejected spatiotemporal relation cues "
            "and challenger objections without asserting causality, transport, or source attribution."
        ),
        "rationale": "Packet is a mediated evidence basis for cautious relation reporting and uncertainty tracking.",
        "confidence": float(confidence),
        "target_kind": PACKET_OBJECT_KIND,
        "target_id": packet_id,
        "target": target_payload,
        "basis_object_ids": [packet_id, *relation_ids],
        "source_signal_ids": source_signal_ids,
        "linked_bundle_ids": [],
        "response_to_ids": [],
        "evidence_refs": basis_evidence_refs,
        "lineage": basis_lineage,
        "decision_source": "agent-submitted-finding",
        "provenance": {
            "source_skill": SKILL_NAME,
            "packet_id": packet_id,
            "decision_source": "agent-submitted-finding",
        },
    }
    evidence_bundle_payload = {
        "run_id": run_id,
        "round_id": round_id,
        "bundle_kind": PACKET_OBJECT_KIND,
        "agent_role": maybe_text(agent_role) or "environmental-investigator",
        "status": "submitted",
        "title": "Spatiotemporal relation evidence bundle",
        "summary": "Evidence bundle cites the relation packet artifact and underlying relation evidence refs.",
        "rationale": "Bundle mediates relation cues and objections before report-basis or report-section use.",
        "confidence": float(confidence),
        "target_kind": PACKET_OBJECT_KIND,
        "target_id": packet_id,
        "target": target_payload,
        "basis_object_ids": [packet_id, *relation_ids],
        "source_signal_ids": source_signal_ids,
        "finding_ids": [],
        "evidence_refs": basis_evidence_refs,
        "lineage": basis_lineage,
        "decision_source": "agent-submitted-finding",
        "provenance": {
            "source_skill": SKILL_NAME,
            "packet_id": packet_id,
            "decision_source": "agent-submitted-finding",
        },
    }
    section_payload = {
        "run_id": run_id,
        "round_id": round_id,
        "report_id": maybe_text(report_id) or round_id,
        "agent_role": maybe_text(report_agent_role) or "report-editor",
        "status": "draft",
        "section_key": maybe_text(section_key) or "spatiotemporal-relation-evidence",
        "section_title": "Spatiotemporal Relation Evidence",
        "section_text": build_report_section_text(packet_id, packet_summary),
        "basis_object_ids": [packet_id, *relation_ids],
        "bundle_ids": [],
        "finding_ids": [],
        "evidence_refs": basis_evidence_refs,
        "lineage": basis_lineage,
        "decision_source": "report-editor",
        "provenance": {
            "source_skill": SKILL_NAME,
            "packet_id": packet_id,
            "decision_source": "report-editor",
        },
    }

    written_records: dict[str, Any] = {}
    if write_basis_objects and relations:
        finding_record = append_finding_record(
            run_dir_path,
            finding_payload=finding_payload,
            artifact_path=str(output_file),
            record_locator="$.basis_handoff.payloads.finding",
        )
        finding = dict_items(finding_record.get("finding"))
        finding_id = maybe_text(finding.get("finding_id"))
        evidence_bundle_payload["finding_ids"] = unique_texts([finding_id])
        evidence_bundle_payload["lineage"] = unique_texts(
            [*list_items(evidence_bundle_payload.get("lineage")), finding_id]
        )
        bundle_record = append_evidence_bundle_record(
            run_dir_path,
            bundle_payload=evidence_bundle_payload,
            artifact_path=str(output_file),
            record_locator="$.basis_handoff.payloads.evidence_bundle",
        )
        bundle = dict_items(bundle_record.get("bundle"))
        bundle_id = maybe_text(bundle.get("bundle_id"))
        section_payload["finding_ids"] = unique_texts([finding_id])
        section_payload["bundle_ids"] = unique_texts([bundle_id])
        section_payload["lineage"] = unique_texts(
            [*list_items(section_payload.get("lineage")), finding_id, bundle_id]
        )
        section_record = store_report_section_draft_record(
            run_dir_path,
            section_payload=section_payload,
            artifact_path=str(output_file),
        )
        written_records = {
            "finding": finding_record,
            "evidence_bundle": bundle_record,
            "report_section_draft": section_record,
        }

    artifact_refs = [artifact_ref(output_file, "$")]
    relation_gap_hints = unique_texts(
        [warning.get("message") for warning in warnings]
        + [
            caveat
            for relation in relations
            for caveat in list_items(relation.get("caveats"))
        ]
        + [
            followup
            for objection in objections
            for followup in list_items(objection.get("required_followup_evidence"))
        ]
        + [objection.get("report_risk") for objection in objections]
    )
    challenge_hints = unique_texts(
        [
            "Report-facing relation use must cite the packet through finding, evidence bundle, report section draft, or frozen report basis."
        ]
        + [
            maybe_text(objection.get("objection_code"))
            for objection in objections
        ]
    )
    board_handoff = {
        "candidate_ids": [packet_id],
        "evidence_refs": artifact_refs,
        "gap_hints": relation_gap_hints,
        "challenge_hints": challenge_hints,
        "suggested_next_skills": [
            "submit-finding-record",
            "submit-evidence-bundle",
            "submit-report-section-draft",
            "freeze-report-basis",
        ],
    }

    packet = {
        "schema_version": "spatiotemporal-relation-evidence-packet-v1",
        "object_kind": PACKET_OBJECT_KIND,
        "packet_id": packet_id,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "skill": SKILL_NAME,
        "relation_filters": {
            "relation_id": maybe_text(relation_id),
            "relation_status": maybe_text(relation_status),
        },
        "relation_query_summary": relation_query.get("summary", {}),
        "relation_cues_summary": {
            **packet_summary,
            "accepted_semantics": "accepted_relation_cue_ids are accepted into this packet only as candidate relation evidence, not as causal or attribution findings.",
        },
        "accepted_relation_cue_ids": accepted_relation_cue_ids,
        "rejected_or_weak_relation_cue_ids": rejected_or_weak_relation_cue_ids,
        "relation_cues": summaries,
        "challenger_objections": objections,
        "uncertainty_register": uncertainty_register(relations, objections),
        "report_use_constraints": {
            "allowed_phrases": ALLOWED_REPORT_PHRASES,
            "prohibited_claims": PROHIBITED_REPORT_CLAIMS,
            "required_mediation": [
                "finding",
                "evidence-bundle",
                "report-section-draft",
                "report-basis-freeze",
            ],
        },
        "evidence_refs": evidence_refs,
        "lineage": basis_lineage,
        "provenance": {
            "source_skill": SKILL_NAME,
            "analysis_query_schema": relation_query.get("schema_version", ""),
            "decision_source": "report-basis-mediated-packet",
        },
        "basis_handoff": {
            "write_basis_objects": bool(write_basis_objects),
            "packet_artifact_ref": packet_ref,
            "payloads": {
                "finding": finding_payload,
                "evidence_bundle": evidence_bundle_payload,
                "report_section_draft": section_payload,
            },
            "written_records": written_records,
        },
        "board_handoff": board_handoff,
        "warnings": warnings,
    }
    write_json(output_file, packet)
    return {
        "status": status,
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "packet_id": packet_id,
            "output_path": str(output_file),
            "relation_count": len(relations),
            "candidate_count": len(accepted_relation_cue_ids),
            "weak_or_rejected_count": len(rejected_or_weak_relation_cue_ids),
            "challenger_objection_count": len(objections),
            "basis_object_write_count": len(written_records),
        },
        "receipt_id": "relation-packet-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, packet_id)[:20],
        "batch_id": "relation-packet-batch-" + stable_hash(SKILL_NAME, run_id, round_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [packet_id],
        "warnings": warnings,
        "basis_handoff": packet["basis_handoff"],
        "board_handoff": board_handoff,
    }


__all__ = [
    "PACKET_OBJECT_KIND",
    "SKILL_NAME",
    "materialize_spatiotemporal_relation_evidence_packet",
]
