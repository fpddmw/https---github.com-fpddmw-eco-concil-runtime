from __future__ import annotations

from collections import Counter
from typing import Any

from .research_issues import (
    explicit_approval_ref,
    load_json_file,
    signal_text,
)
from .support import (
    artifact_ref,
    dict_items,
    helper_metadata,
    lineage_from_signals,
    list_items,
    maybe_text,
    query_signals,
    refs_from_signals,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    signal_source_distribution,
    stable_hash,
    text_terms,
    unique_texts,
    utc_now_iso,
    write_json,
)


__all__ = (
    "run_apply_approved_formal_public_taxonomy",
    "run_compare_formal_public_footprints",
    "run_identify_representation_audit_cues",
    "taxonomy_labels",
)


def taxonomy_labels(taxonomy_payload: Any) -> list[dict[str, Any]]:
    if isinstance(taxonomy_payload, list):
        raw_items = taxonomy_payload
    elif isinstance(taxonomy_payload, dict):
        raw_items = list_items(taxonomy_payload.get("labels") or taxonomy_payload.get("taxonomy_labels"))
    else:
        raw_items = []
    labels: list[dict[str, Any]] = []
    for item in raw_items:
        if isinstance(item, str):
            labels.append({"label": maybe_text(item), "terms": [maybe_text(item)]})
        elif isinstance(item, dict):
            label = maybe_text(item.get("label") or item.get("name") or item.get("id"))
            terms = unique_texts(list_items(item.get("terms")) + [label])
            if label:
                labels.append({"label": label, "terms": terms, "description": maybe_text(item.get("description"))})
    return labels


def run_apply_approved_formal_public_taxonomy(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    taxonomy_path: str = "",
    taxonomy_version: str = "",
    approval_ref: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "apply-approved-formal-public-taxonomy"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"formal_public_taxonomy_labels_{round_id}.json")
    taxonomy_payload = load_json_file(taxonomy_path, {})
    labels = taxonomy_labels(taxonomy_payload)
    taxonomy_payload_object = dict_items(taxonomy_payload)
    taxonomy_approval_ref = explicit_approval_ref(approval_ref) or explicit_approval_ref(
        taxonomy_payload_object.get("approval_ref")
        or taxonomy_payload_object.get("approved_taxonomy_ref")
    )
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    metadata = helper_metadata(
        skill_name=skill_name,
        taxonomy_version=maybe_text(taxonomy_version) or maybe_text(taxonomy_payload_object.get("version")),
        approval_ref=taxonomy_approval_ref or "required:approved_taxonomy_record",
        rule_trace=["approved-taxonomy-label-cues"],
        caveats=[
            "No global taxonomy is applied without an explicit approved taxonomy file or record.",
            "Labels are candidate cues and require human audit before report use.",
        ],
    )
    signals = [*public_signals, *formal_signals]
    label_cues: list[dict[str, Any]] = []
    approval_missing = bool(labels and not taxonomy_approval_ref)
    if labels and not approval_missing:
        for signal in signals:
            text = signal_text(signal).casefold()
            matched_labels: list[str] = []
            for label in labels:
                terms = unique_texts(list_items(label.get("terms")))
                if any(term.casefold() in text for term in terms if term):
                    matched_labels.append(maybe_text(label.get("label")))
            if not matched_labels:
                continue
            cue_id = "taxonomy-cue-" + stable_hash(run_id, round_id, signal.get("signal_id"), matched_labels)[:12]
            label_cues.append(
                {
                    "cue_id": cue_id,
                    "signal_id": maybe_text(signal.get("signal_id")),
                    "plane": maybe_text(signal.get("plane")),
                    "candidate_labels": unique_texts(matched_labels),
                    "taxonomy_version": metadata["taxonomy_version"],
                    "taxonomy_approval_ref": metadata["approval_ref"],
                    "audit_status": "candidate-for-human-review",
                    "evidence_refs": list_items(signal.get("evidence_refs")),
                    "lineage": [maybe_text(signal.get("signal_id"))],
                    "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                    "helper_governance": metadata,
                }
            )
    warnings = []
    status = "completed"
    if not labels:
        status = "taxonomy-required"
        warnings.append({"code": "taxonomy-required", "message": "Provide an approved mission-scoped taxonomy before applying labels."})
    elif approval_missing:
        status = "taxonomy-approval-required"
        warnings.append({"code": "taxonomy-approval-required", "message": "Provide a concrete approved taxonomy reference before applying taxonomy labels."})
    elif not label_cues:
        warnings.append({"code": "no-taxonomy-cues", "message": "No DB public/formal signals matched the approved taxonomy terms."})
    payload = {
        "schema_version": "optional-analysis-formal-public-taxonomy-labels-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "helper_governance": metadata,
        "taxonomy_labels": label_cues,
        "observed_inputs": {"db_path": db_path, "taxonomy_path": maybe_text(taxonomy_path), "label_count": len(labels), "signal_count": len(signals)},
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": status,
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "label_cue_count": len(label_cues), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "taxonomy-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "taxonomy-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.taxonomy_labels")],
        "canonical_ids": [maybe_text(item.get("cue_id")) for item in label_cues],
        "warnings": warnings,
        "taxonomy_labels": label_cues,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.taxonomy_labels", candidate_ids=[maybe_text(item.get("cue_id")) for item in label_cues], gap_hints=[item["message"] for item in warnings]),
    }


def run_compare_formal_public_footprints(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    taxonomy_labels_path: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "compare-formal-public-footprints"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"formal_public_footprints_{round_id}.json")
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    label_payload = load_json_file(taxonomy_labels_path, {})
    label_cues = list_items(label_payload.get("taxonomy_labels")) if isinstance(label_payload, dict) else []
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["formal-public-footprint-overlap-cues"],
        caveats=[
            "Footprint comparison describes source-family overlap and absence cues only.",
            "It does not produce paired discourse links or decide representativeness.",
        ],
    )
    public_terms = Counter(term for signal in public_signals for term in text_terms(signal_text(signal), limit=16))
    formal_terms = Counter(term for signal in formal_signals for term in text_terms(signal_text(signal), limit=16))
    shared_terms = sorted(set(public_terms) & set(formal_terms))
    public_only = [term for term, _ in public_terms.most_common(20) if term not in formal_terms]
    formal_only = [term for term, _ in formal_terms.most_common(20) if term not in public_terms]
    footprints = {
        "footprint_id": "formal-public-footprint-" + stable_hash(run_id, round_id, len(public_signals), len(formal_signals))[:12],
        "run_id": run_id,
        "round_id": round_id,
        "formal_record_summary": {
            "signal_count": len(formal_signals),
            "source_distribution": signal_source_distribution(formal_signals),
            "top_terms": [term for term, _ in formal_terms.most_common(20)],
        },
        "public_discourse_summary": {
            "signal_count": len(public_signals),
            "source_distribution": signal_source_distribution(public_signals),
            "top_terms": [term for term, _ in public_terms.most_common(20)],
        },
        "overlap_terms": shared_terms[:20],
        "formal_only_cues": formal_only[:20],
        "public_only_cues": public_only[:20],
        "taxonomy_label_cue_count": len(label_cues),
        "evidence_refs": refs_from_signals([*formal_signals, *public_signals]),
        "lineage": lineage_from_signals([*formal_signals, *public_signals]),
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
        "helper_governance": metadata,
    }
    warnings = [] if public_signals and formal_signals else [{"code": "missing-source-family", "message": "Both public and formal DB signals are needed for a complete footprint comparison."}]
    payload = {
        "schema_version": "optional-analysis-formal-public-footprints-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "formal_public_footprints": footprints,
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "public_signal_count": len(public_signals), "formal_signal_count": len(formal_signals), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "footprint-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "footprint-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.formal_public_footprints")],
        "canonical_ids": [footprints["footprint_id"]],
        "warnings": warnings,
        "formal_public_footprints": footprints,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.formal_public_footprints", candidate_ids=[footprints["footprint_id"]], gap_hints=[item["message"] for item in warnings]),
    }


def run_identify_representation_audit_cues(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "identify-representation-audit-cues"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"representation_audit_cues_{round_id}.json")
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["source-family-representation-audit-cues"],
        caveats=[
            "Representation audit cues are prompts for human review, not findings of exclusion or harm.",
            "No cue may be used in report text without DB council-object review.",
        ],
    )
    cues: list[dict[str, Any]] = []
    source_counts = {
        "public": len(public_signals),
        "formal": len(formal_signals),
    }
    if not public_signals or not formal_signals:
        cues.append(
            {
                "cue_id": "representation-cue-" + stable_hash(run_id, round_id, "source-family-presence")[:12],
                "cue_kind": "source-family-presence-audit",
                "review_prompt": "Review whether available source families are sufficient for the decision context.",
                "source_counts": source_counts,
                "audit_status": "requires-human-review",
                "evidence_refs": refs_from_signals([*public_signals, *formal_signals]),
                "lineage": lineage_from_signals([*public_signals, *formal_signals]),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                "helper_governance": metadata,
            }
        )
    public_authors = unique_texts([signal.get("author_name") or signal.get("channel_name") for signal in public_signals])
    formal_submitters = unique_texts([dict_items(signal.get("metadata")).get("submitter_name") or signal.get("author_name") for signal in formal_signals])
    cues.append(
        {
            "cue_id": "representation-cue-" + stable_hash(run_id, round_id, "participant-name-coverage")[:12],
            "cue_kind": "participant-name-coverage-audit",
            "review_prompt": "Review participant-name coverage across public and formal source families before inferring public participation.",
            "source_counts": {
                "public_named_sources": len(public_authors),
                "formal_named_submitters": len(formal_submitters),
            },
            "sample_public_names": public_authors[:10],
            "sample_formal_names": formal_submitters[:10],
            "audit_status": "requires-human-review",
            "evidence_refs": refs_from_signals([*public_signals, *formal_signals]),
            "lineage": lineage_from_signals([*public_signals, *formal_signals]),
            "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
            "helper_governance": metadata,
        }
    )
    payload = {
        "schema_version": "optional-analysis-representation-audit-cues-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "representation_audit_cues": cues,
        "warnings": [] if public_signals and formal_signals else [{"code": "missing-source-family", "message": "Representation audit cues need human review because at least one source family is absent."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "cue_count": len(cues), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "representation-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "representation-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.representation_audit_cues")],
        "canonical_ids": [maybe_text(item.get("cue_id")) for item in cues],
        "warnings": payload["warnings"],
        "representation_audit_cues": cues,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.representation_audit_cues", candidate_ids=[maybe_text(item.get("cue_id")) for item in cues], gap_hints=[item["message"] for item in payload["warnings"]]),
    }
