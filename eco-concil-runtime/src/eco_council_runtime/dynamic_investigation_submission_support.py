from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

from eco_council_runtime.council_submission_support import (
    maybe_text,
    merged_provenance,
    merged_text_list,
    parse_json_dict,
    parse_json_list,
    resolve_path,
    resolve_run_dir,
    unique_texts,
    write_json_file,
)
from eco_council_runtime.kernel.planes.deliberation_plane import stable_hash
from eco_council_runtime.objects.council import append_dynamic_investigation_object_record


TEXT_FIELDS = (
    "title",
    "question",
    "summary",
    "summary_text",
    "brief_text",
    "synthesis_text",
    "stage_conclusion",
    "moderator_boundary",
    "request_text",
    "scope_text",
    "claim_summary",
    "round_mode",
    "context_packet_id",
    "desired_evidence_type",
    "packet_profile",
    "target_round_id",
    "raw_data_policy",
    "scope_kind",
    "spatial_scope",
    "temporal_scope",
    "object_scope",
    "metric_scope",
    "comparison_frame",
    "mission_ref",
    "planning_round_id",
    "supersedes_plan_id",
    "ordering_semantics",
    "compression_policy",
    "disposition_status",
    "disposition_text",
    "decided_by_role",
    "source_review_comment_id",
    "challenge_id",
)

LIST_FIELDS = (
    "proposed_subissue_refs",
    "scope_hint_refs",
    "open_questions",
    "source_hints",
    "boundary_notes",
    "source_boundary_notes",
    "known_facts",
    "limitations",
    "open_challenge_refs",
    "primary_focus_refs",
    "requested_outputs",
    "invited_roles",
    "included_object_refs",
    "excluded_object_refs",
    "target_refs",
    "delta_refs",
    "source_refs",
    "response_to_ids",
    "covered_object_refs",
    "resolved_object_refs",
    "unresolved_object_refs",
    "evidence_gap_refs",
    "next_round_candidate_refs",
)

LIST_ARG_FLAGS = {
    "proposed_subissue_refs": "proposed-subissue-ref",
    "scope_hint_refs": "scope-hint-ref",
    "open_questions": "open-question",
    "source_hints": "source-hint",
    "boundary_notes": "boundary-note",
    "source_boundary_notes": "source-boundary-note",
    "known_facts": "known-fact",
    "limitations": "limitation",
    "open_challenge_refs": "open-challenge-ref",
    "primary_focus_refs": "primary-focus-ref",
    "requested_outputs": "requested-output",
    "invited_roles": "invited-role",
    "included_object_refs": "included-object-ref",
    "excluded_object_refs": "excluded-object-ref",
    "target_refs": "target-ref",
    "delta_refs": "delta-ref",
    "source_refs": "source-ref",
    "response_to_ids": "response-to-id",
    "covered_object_refs": "covered-object-ref",
    "resolved_object_refs": "resolved-object-ref",
    "unresolved_object_refs": "unresolved-object-ref",
    "evidence_gap_refs": "evidence-gap-ref",
    "next_round_candidate_refs": "next-round-candidate-ref",
}


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def dynamic_object_query_command(
    *,
    run_dir: Path,
    object_kind: str,
    run_id: str,
    round_id: str,
    target_kind: str,
    target_id: str,
) -> str:
    command_parts = [
        "python3",
        "eco-concil-runtime/scripts/eco_runtime_kernel.py",
        "query-council-objects",
        "--run-dir",
        str(run_dir),
        "--object-kind",
        object_kind,
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    ]
    if maybe_text(target_kind):
        command_parts.extend(["--target-kind", maybe_text(target_kind)])
    if maybe_text(target_id):
        command_parts.extend(["--target-id", maybe_text(target_id)])
    return shlex.join(command_parts)


def apply_text_fields(payload: dict[str, Any], values: dict[str, Any]) -> None:
    for field_name in TEXT_FIELDS:
        value = maybe_text(values.get(field_name))
        if value:
            payload[field_name] = value


def apply_list_fields(payload: dict[str, Any], values: dict[str, Any]) -> None:
    for field_name in LIST_FIELDS:
        merged = merged_text_list(
            payload.get(field_name, []),
            values.get(field_name, []),
        )
        if merged or field_name in payload:
            payload[field_name] = merged


def dynamic_investigation_payload_from_inputs(
    *,
    object_kind: str,
    run_id: str,
    round_id: str,
    object_id: str,
    author_role: str,
    status: str,
    target_kind: str,
    target_id: str,
    target_json: str,
    rationale: str,
    evidence_refs: list[str],
    evidence_refs_json: str,
    lineage_ids: list[str],
    lineage_json: str,
    provenance_json: str,
    payload_json: str,
    source_skill: str,
    field_values: dict[str, Any],
) -> dict[str, Any]:
    payload = parse_json_dict(payload_json, option_name="--payload-json")
    payload_kind = maybe_text(payload.get("object_kind"))
    normalized_kind = maybe_text(object_kind)
    if payload_kind and payload_kind != normalized_kind:
        raise ValueError(
            f"--payload-json object_kind `{payload_kind}` does not match "
            f"skill object kind `{normalized_kind}`."
        )
    if maybe_text(target_json):
        target = parse_json_dict(target_json, option_name="--target-json")
        if target:
            payload["target"] = target
    payload["run_id"] = maybe_text(run_id)
    payload["round_id"] = maybe_text(round_id)
    payload["object_kind"] = normalized_kind
    if maybe_text(object_id):
        payload["object_id"] = maybe_text(object_id)
    payload["author_role"] = maybe_text(author_role)
    if maybe_text(status):
        payload["status"] = maybe_text(status)
    if maybe_text(target_kind):
        payload["target_kind"] = maybe_text(target_kind)
    if maybe_text(target_id):
        payload["target_id"] = maybe_text(target_id)
    payload["rationale"] = maybe_text(rationale)
    payload["evidence_refs"] = merged_text_list(
        payload.get("evidence_refs", []),
        parse_json_list(evidence_refs_json, option_name="--evidence-refs-json"),
        evidence_refs,
    )
    payload["lineage"] = merged_text_list(
        payload.get("lineage", []),
        parse_json_list(lineage_json, option_name="--lineage-json"),
        lineage_ids,
    )
    payload["provenance"] = merged_provenance(
        provenance_json=provenance_json,
        payload_provenance=payload.get("provenance"),
        source_skill=source_skill,
    )
    apply_text_fields(payload, field_values)
    apply_list_fields(payload, field_values)
    return payload


def submit_dynamic_investigation_object_skill(
    *,
    skill_name: str,
    object_kind: str,
    run_dir: str,
    run_id: str,
    round_id: str,
    object_id: str,
    author_role: str,
    status: str,
    target_kind: str,
    target_id: str,
    target_json: str,
    rationale: str,
    evidence_refs: list[str],
    evidence_refs_json: str,
    lineage_ids: list[str],
    lineage_json: str,
    provenance_json: str,
    payload_json: str,
    output_path: str,
    field_values: dict[str, Any],
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    object_payload = dynamic_investigation_payload_from_inputs(
        object_kind=object_kind,
        run_id=run_id,
        round_id=round_id,
        object_id=object_id,
        author_role=author_role,
        status=status,
        target_kind=target_kind,
        target_id=target_id,
        target_json=target_json,
        rationale=rationale,
        evidence_refs=evidence_refs,
        evidence_refs_json=evidence_refs_json,
        lineage_ids=lineage_ids,
        lineage_json=lineage_json,
        provenance_json=provenance_json,
        payload_json=payload_json,
        source_skill=skill_name,
        field_values=field_values,
    )
    append_result = append_dynamic_investigation_object_record(
        run_dir_path,
        object_payload=object_payload,
        object_kind=object_kind,
    )
    dynamic_object = (
        append_result.get("object", {})
        if isinstance(append_result.get("object"), dict)
        else {}
    )
    object_identifier = maybe_text(dynamic_object.get("object_id"))
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"runtime/{object_kind}_{object_identifier}.json",
    )
    artifact = {
        "schema_version": "dynamic-investigation-object-submission-v1",
        "skill": skill_name,
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "object_kind": maybe_text(object_kind),
        "db_path": maybe_text(append_result.get("db_path")),
        "object": dynamic_object,
    }
    write_json_file(output_file, artifact)
    artifact_ref = {
        "artifact_path": str(output_file),
        "record_locator": "$.object",
        "artifact_ref": f"{output_file}:$.object",
    }
    target_ref = {
        "object_kind": maybe_text(dynamic_object.get("target_kind")),
        "object_id": maybe_text(dynamic_object.get("target_id")),
    }
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": maybe_text(run_id),
            "round_id": maybe_text(round_id),
            "object_kind": maybe_text(object_kind),
            "object_id": object_identifier,
            "author_role": maybe_text(dynamic_object.get("author_role")),
            "target_kind": maybe_text(dynamic_object.get("target_kind")),
            "target_id": maybe_text(dynamic_object.get("target_id")),
            "output_path": str(output_file),
            "db_path": maybe_text(append_result.get("db_path")),
        },
        "receipt_id": "dynamic-investigation-receipt-"
        + stable_hash(skill_name, run_id, round_id, object_identifier)[:20],
        "batch_id": "dynamic-investigation-batch-"
        + stable_hash(skill_name, run_id, round_id, output_file.name)[:16],
        "artifact_refs": [artifact_ref],
        "canonical_ids": [object_identifier],
        "warnings": [],
        "council_handoff": {
            "object_refs": [
                {
                    "object_kind": maybe_text(object_kind),
                    "object_id": object_identifier,
                }
            ],
            "target_ref": target_ref,
            "evidence_refs": dynamic_object.get("evidence_refs", []),
            "query_command": dynamic_object_query_command(
                run_dir=run_dir_path,
                object_kind=object_kind,
                run_id=maybe_text(run_id),
                round_id=maybe_text(round_id),
                target_kind=maybe_text(dynamic_object.get("target_kind")),
                target_id=maybe_text(dynamic_object.get("target_id")),
            ),
        },
    }


def add_dynamic_submission_args(
    parser: argparse.ArgumentParser,
    *,
    default_author_role: str,
) -> None:
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--object-id", default="")
    parser.add_argument("--author-role", default=default_author_role)
    parser.add_argument("--status", default="")
    parser.add_argument("--target-kind", default="round")
    parser.add_argument("--target-id", default="")
    parser.add_argument("--target-json", default="")
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--evidence-ref", action="append", default=[])
    parser.add_argument("--evidence-refs-json", default="")
    parser.add_argument("--lineage-id", action="append", default=[])
    parser.add_argument("--lineage-json", default="")
    parser.add_argument("--provenance-json", default="")
    parser.add_argument("--payload-json", "--extra-json", default="")
    parser.add_argument("--output-path", default="")
    for field_name in TEXT_FIELDS:
        parser.add_argument(f"--{field_name.replace('_', '-')}", default="")
    for field_name, flag_name in LIST_ARG_FLAGS.items():
        parser.add_argument(f"--{flag_name}", dest=field_name, action="append", default=[])
    parser.add_argument("--pretty", action="store_true")


def field_values_from_args(args: argparse.Namespace) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for field_name in TEXT_FIELDS:
        values[field_name] = getattr(args, field_name, "")
    for field_name in LIST_FIELDS:
        values[field_name] = getattr(args, field_name, [])
    return values


def parse_dynamic_submission_args(
    *,
    skill_name: str,
    object_kind: str,
    default_author_role: str,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            f"Submit one {object_kind} dynamic investigation coordination object."
        )
    )
    parser.set_defaults(skill_name=skill_name, object_kind=object_kind)
    add_dynamic_submission_args(parser, default_author_role=default_author_role)
    return parser.parse_args()


def main_for_dynamic_skill(
    *,
    skill_name: str,
    object_kind: str,
    default_author_role: str,
) -> int:
    try:
        args = parse_dynamic_submission_args(
            skill_name=skill_name,
            object_kind=object_kind,
            default_author_role=default_author_role,
        )
        payload = submit_dynamic_investigation_object_skill(
            skill_name=skill_name,
            object_kind=object_kind,
            run_dir=args.run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            object_id=args.object_id,
            author_role=args.author_role,
            status=args.status,
            target_kind=args.target_kind,
            target_id=args.target_id,
            target_json=args.target_json,
            rationale=args.rationale,
            evidence_refs=args.evidence_ref,
            evidence_refs_json=args.evidence_refs_json,
            lineage_ids=args.lineage_id,
            lineage_json=args.lineage_json,
            provenance_json=args.provenance_json,
            payload_json=args.payload_json,
            output_path=args.output_path,
            field_values=field_values_from_args(args),
        )
    except ValueError as exc:
        sys.stdout.write(
            pretty_json(
                {
                    "status": "failed",
                    "summary": {
                        "skill": skill_name,
                        "object_kind": object_kind,
                    },
                    "message": str(exc),
                },
                bool(getattr(locals().get("args", None), "pretty", False)),
            )
        )
        sys.stdout.write("\n")
        return 1
    sys.stdout.write(pretty_json(payload, args.pretty))
    sys.stdout.write("\n")
    return 0


__all__ = (
    "TEXT_FIELDS",
    "LIST_FIELDS",
    "LIST_ARG_FLAGS",
    "pretty_json",
    "dynamic_object_query_command",
    "dynamic_investigation_payload_from_inputs",
    "submit_dynamic_investigation_object_skill",
    "add_dynamic_submission_args",
    "field_values_from_args",
    "parse_dynamic_submission_args",
    "main_for_dynamic_skill",
)
