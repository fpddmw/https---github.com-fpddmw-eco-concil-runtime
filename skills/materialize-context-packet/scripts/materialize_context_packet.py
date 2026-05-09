#!/usr/bin/env python3
"""Materialize a refs-only context-packet coordination object."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "materialize-context-packet"
OBJECT_KIND = "context-packet"
DEFAULT_AUTHOR_ROLE = "moderator"
VALID_PACKET_PROFILES = ("scoping", "investigation", "supplemental", "synthesis")
DEFAULT_CONTEXT_OBJECT_KINDS = (
    "investigation-plan",
    "subissue",
    "investigation-scope",
    "round-brief",
    "evidence-request",
    "agent-position",
    "challenge-disposition",
    "review-comment",
    "finding",
    "evidence-bundle",
    "proposal",
    "readiness-opinion",
)
RAW_REF_PREFIXES = (
    "raw:",
    "raw-record:",
    "raw-records:",
    "raw-artifact:",
    "raw-artifacts:",
    "artifact://raw",
)

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_submission_support import (  # noqa: E402
    maybe_text,
    parse_json_dict,
    resolve_run_dir,
    unique_texts,
)
from eco_council_runtime.dynamic_investigation_submission_support import (  # noqa: E402
    pretty_json,
    submit_dynamic_investigation_object_skill,
)
from eco_council_runtime.objects.council import query_council_objects  # noqa: E402


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def text_list(value: Any) -> list[str]:
    return unique_texts(list_items(value))


def object_identifier(payload: dict[str, Any]) -> str:
    for field_name in (
        "object_id",
        "proposal_id",
        "finding_id",
        "bundle_id",
        "comment_id",
        "message_id",
        "ticket_id",
        "task_id",
        "opinion_id",
        "assessment_id",
        "trace_id",
    ):
        value = maybe_text(payload.get(field_name))
        if value:
            return value
    return ""


def object_ref(object_kind: str, payload: dict[str, Any]) -> str:
    identifier = object_identifier(payload)
    if not identifier:
        return ""
    return f"{object_kind}:{identifier}"


def ref_is_raw_record(value: Any) -> bool:
    text = maybe_text(value).casefold()
    return bool(text and any(text.startswith(prefix) for prefix in RAW_REF_PREFIXES))


def reject_raw_included_refs(refs: list[str]) -> None:
    raw_refs = [ref for ref in refs if ref_is_raw_record(ref)]
    if raw_refs:
        raise ValueError(
            "context-packet included refs cannot contain raw records: "
            + ", ".join(raw_refs)
        )


def normalize_packet_profile(value: str) -> str:
    profile = maybe_text(value) or "investigation"
    if profile not in VALID_PACKET_PROFILES:
        raise ValueError(
            f"Unsupported --packet-profile {profile!r}. Expected one of "
            f"{', '.join(VALID_PACKET_PROFILES)}."
        )
    return profile


def normalize_raw_data_policy(value: str) -> str:
    policy = maybe_text(value) or "refs-only"
    if policy not in {"refs-only", "no-raw-records", "refs-and-derived-summaries"}:
        raise ValueError(
            "--raw-data-policy must keep raw records out of context packets. "
            "Use refs-only, no-raw-records, or refs-and-derived-summaries."
        )
    return policy


def queried_context_refs(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    object_kinds: list[str],
    limit: int,
) -> tuple[list[str], list[str], list[dict[str, str]]]:
    refs: list[str] = []
    evidence_refs: list[str] = []
    warnings: list[dict[str, str]] = []
    for object_kind in object_kinds:
        normalized_kind = maybe_text(object_kind)
        if not normalized_kind:
            continue
        try:
            payload = query_council_objects(
                run_dir,
                object_kind=normalized_kind,
                run_id=run_id,
                round_id=round_id,
                limit=limit,
            )
        except ValueError as exc:
            warnings.append(
                {
                    "code": "context-object-query-failed",
                    "object_kind": normalized_kind,
                    "message": str(exc),
                }
            )
            continue
        for item in list_items(payload.get("objects")):
            if not isinstance(item, dict):
                continue
            ref = object_ref(normalized_kind, item)
            if ref:
                refs.append(ref)
            evidence_refs.extend(text_list(item.get("evidence_refs")))
    return unique_texts(refs), unique_texts(evidence_refs), warnings


def latest_context_packet(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    payload = query_council_objects(
        run_dir,
        object_kind=OBJECT_KIND,
        run_id=run_id,
        round_id=round_id,
        limit=1,
    )
    objects = list_items(payload.get("objects"))
    first = objects[0] if objects and isinstance(objects[0], dict) else {}
    return first if isinstance(first, dict) else {}


def context_packet_by_id(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    object_id: str,
) -> dict[str, Any]:
    payload = query_council_objects(
        run_dir,
        object_kind=OBJECT_KIND,
        run_id=run_id,
        round_id=round_id,
        limit=50,
    )
    for item in list_items(payload.get("objects")):
        if isinstance(item, dict) and maybe_text(item.get("object_id")) == object_id:
            return item
    return {}


def build_context_packet_payload(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    object_id: str,
    author_role: str,
    status: str,
    target_kind: str,
    target_id: str,
    rationale: str,
    packet_profile: str,
    target_refs: list[str],
    explicit_included_refs: list[str],
    explicit_excluded_refs: list[str],
    explicit_delta_refs: list[str],
    explicit_evidence_refs: list[str],
    source_refs: list[str],
    object_kinds: list[str],
    max_objects_per_kind: int,
    summary_text: str,
    raw_data_policy: str,
    provenance_json: str,
    extra_payload_json: str,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    queried_refs, queried_evidence_refs, warnings = queried_context_refs(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        object_kinds=object_kinds,
        limit=max_objects_per_kind,
    )
    included_refs = unique_texts([*explicit_included_refs, *queried_refs])
    reject_raw_included_refs(included_refs)

    previous_packet = latest_context_packet(run_dir, run_id=run_id, round_id=round_id)
    previous_refs = set(text_list(previous_packet.get("included_object_refs")))
    computed_delta_refs = [
        ref for ref in included_refs if ref and ref not in previous_refs
    ]
    target_id_text = maybe_text(target_id) or round_id
    resolved_target_refs = unique_texts(
        target_refs
        or [
            f"{maybe_text(target_kind) or 'round'}:{target_id_text}",
        ]
    )
    excluded_refs = unique_texts(["raw-records:*", *explicit_excluded_refs])
    evidence_refs = unique_texts([*explicit_evidence_refs, *queried_evidence_refs])
    packet_source_refs = unique_texts(["council-db:deliberation", *source_refs])
    summary = (
        maybe_text(summary_text)
        or f"Refs-only {packet_profile} context packet for round {round_id}."
    )
    payload = parse_json_dict(extra_payload_json, option_name="--payload-json")
    payload.update(
        {
            "run_id": run_id,
            "round_id": round_id,
            "object_kind": OBJECT_KIND,
            "author_role": author_role,
            "status": maybe_text(status) or "materialized",
            "target_kind": maybe_text(target_kind) or "round",
            "target_id": target_id_text,
            "rationale": maybe_text(rationale)
            or f"Materialize refs-only context packet for {packet_profile} round.",
            "packet_profile": packet_profile,
            "target_round_id": round_id,
            "target_refs": resolved_target_refs,
            "included_object_refs": included_refs,
            "excluded_object_refs": excluded_refs,
            "delta_refs": unique_texts([*explicit_delta_refs, *computed_delta_refs]),
            "summary_text": summary,
            "raw_data_policy": raw_data_policy,
            "source_refs": packet_source_refs,
            "evidence_refs": evidence_refs,
            "ordering_semantics": (
                "Deterministic object-kind/query order only; no salience ranking, "
                "evidence scoring, or source weighting is applied."
            ),
            "compression_policy": (
                "Refs-only packet; raw records and full object payloads remain "
                "outside the packet and must be queried explicitly by agents."
            ),
            "provenance": parse_json_dict(
                provenance_json,
                option_name="--provenance-json",
            ),
        }
    )
    if object_id:
        payload["object_id"] = object_id
    return payload, warnings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize one refs-only context-packet coordination object."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--object-id", default="")
    parser.add_argument("--author-role", default=DEFAULT_AUTHOR_ROLE)
    parser.add_argument("--status", default="materialized")
    parser.add_argument("--target-kind", default="round")
    parser.add_argument("--target-id", default="")
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--packet-profile", default="investigation")
    parser.add_argument("--target-ref", action="append", default=[])
    parser.add_argument("--include-object-ref", action="append", default=[])
    parser.add_argument("--excluded-object-ref", action="append", default=[])
    parser.add_argument("--delta-ref", action="append", default=[])
    parser.add_argument("--evidence-ref", action="append", default=[])
    parser.add_argument("--source-ref", action="append", default=[])
    parser.add_argument("--object-kind", action="append", default=[])
    parser.add_argument("--max-objects-per-kind", type=int, default=20)
    parser.add_argument("--summary-text", default="")
    parser.add_argument("--raw-data-policy", default="refs-only")
    parser.add_argument("--provenance-json", default="")
    parser.add_argument("--payload-json", "--extra-json", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        run_dir = resolve_run_dir(args.run_dir)
        packet_profile = normalize_packet_profile(args.packet_profile)
        raw_data_policy = normalize_raw_data_policy(args.raw_data_policy)
        if args.max_objects_per_kind <= 0:
            raise ValueError("--max-objects-per-kind must be > 0.")
        object_kinds = unique_texts(args.object_kind or list(DEFAULT_CONTEXT_OBJECT_KINDS))
        packet_payload, query_warnings = build_context_packet_payload(
            run_dir=run_dir,
            run_id=maybe_text(args.run_id),
            round_id=maybe_text(args.round_id),
            object_id=maybe_text(args.object_id),
            author_role=maybe_text(args.author_role) or DEFAULT_AUTHOR_ROLE,
            status=maybe_text(args.status),
            target_kind=maybe_text(args.target_kind) or "round",
            target_id=maybe_text(args.target_id) or maybe_text(args.round_id),
            rationale=maybe_text(args.rationale),
            packet_profile=packet_profile,
            target_refs=unique_texts(args.target_ref),
            explicit_included_refs=unique_texts(args.include_object_ref),
            explicit_excluded_refs=unique_texts(args.excluded_object_ref),
            explicit_delta_refs=unique_texts(args.delta_ref),
            explicit_evidence_refs=unique_texts(args.evidence_ref),
            source_refs=unique_texts(args.source_ref),
            object_kinds=object_kinds,
            max_objects_per_kind=args.max_objects_per_kind,
            summary_text=maybe_text(args.summary_text),
            raw_data_policy=raw_data_policy,
            provenance_json=args.provenance_json,
            extra_payload_json=args.payload_json,
        )
        result = submit_dynamic_investigation_object_skill(
            skill_name=SKILL_NAME,
            object_kind=OBJECT_KIND,
            run_dir=str(run_dir),
            run_id=maybe_text(args.run_id),
            round_id=maybe_text(args.round_id),
            object_id=maybe_text(args.object_id),
            author_role=maybe_text(args.author_role) or DEFAULT_AUTHOR_ROLE,
            status=maybe_text(args.status),
            target_kind=maybe_text(args.target_kind) or "round",
            target_id=maybe_text(args.target_id) or maybe_text(args.round_id),
            target_json="",
            rationale=maybe_text(args.rationale),
            evidence_refs=[],
            evidence_refs_json="",
            lineage_ids=[],
            lineage_json="",
            provenance_json=args.provenance_json,
            payload_json=json.dumps(packet_payload, ensure_ascii=True, sort_keys=True),
            output_path=maybe_text(args.output_path),
            field_values={},
        )
        packet_id = maybe_text((result.get("summary") or {}).get("object_id"))
        context_packet = context_packet_by_id(
            run_dir,
            run_id=maybe_text(args.run_id),
            round_id=maybe_text(args.round_id),
            object_id=packet_id,
        )
        result["context_packet"] = context_packet
        result["warnings"] = [
            *list_items(result.get("warnings")),
            *query_warnings,
        ]
        if isinstance(result.get("summary"), dict):
            result["summary"]["included_object_ref_count"] = len(
                text_list(context_packet.get("included_object_refs"))
            )
            result["summary"]["delta_ref_count"] = len(
                text_list(context_packet.get("delta_refs"))
            )
            result["summary"]["raw_data_policy"] = maybe_text(
                context_packet.get("raw_data_policy")
            )
    except ValueError as exc:
        print(
            pretty_json(
                {
                    "status": "failed",
                    "summary": {"skill": SKILL_NAME, "object_kind": OBJECT_KIND},
                    "message": str(exc),
                },
                args.pretty,
            )
        )
        return 1
    print(pretty_json(result, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
