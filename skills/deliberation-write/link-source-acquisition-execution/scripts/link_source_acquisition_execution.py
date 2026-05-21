#!/usr/bin/env python3
"""Link source-acquisition proposal execution receipts and normalized refs."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "link-source-acquisition-execution"
OBJECT_KIND = "source-acquisition-proposal"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_submission_support import (  # noqa: E402
    maybe_text,
    parse_json_dict,
    resolve_path,
    resolve_run_dir,
    unique_texts,
    write_json_file,
)
from eco_council_runtime.kernel.planes.deliberation_plane import stable_hash, utc_now_iso  # noqa: E402
from eco_council_runtime.objects.council import (  # noqa: E402
    SOURCE_ACQUISITION_PROPOSAL_STATUSES,
    fetch_dynamic_investigation_object_record,
    update_dynamic_investigation_object_status,
)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def text_list(values: list[Any]) -> list[str]:
    return unique_texts([item for item in values if maybe_text(item)])


def query_command(*, run_dir: Path, run_id: str, round_id: str, object_id: str) -> str:
    return shlex.join(
        [
            "python3",
            "eco-concil-runtime/scripts/eco_runtime_kernel.py",
            "query-council-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            OBJECT_KIND,
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--target-id",
            object_id,
            "--pretty",
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Link execution lineage to one source-acquisition-proposal."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--object-id", "--proposal-id", required=True)
    parser.add_argument(
        "--status",
        choices=SOURCE_ACQUISITION_PROPOSAL_STATUSES,
        default="",
    )
    parser.add_argument("--actor-role", required=True)
    parser.add_argument("--status-rationale", "--rationale", default="")
    parser.add_argument("--fetch-receipt-ref", action="append", default=[])
    parser.add_argument("--normalization-receipt-ref", action="append", default=[])
    parser.add_argument("--normalized-signal-ref", action="append", default=[])
    parser.add_argument("--artifact-ref", action="append", default=[])
    parser.add_argument("--evidence-ref", action="append", default=[])
    parser.add_argument("--lineage-id", action="append", default=[])
    parser.add_argument("--execution-link-json", default="{}")
    parser.add_argument("--provenance-json", default="{}")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser


def inferred_execution_status(args: argparse.Namespace) -> str:
    explicit = maybe_text(args.status)
    if explicit:
        return explicit
    if text_list(args.normalized_signal_ref):
        return "normalized"
    if text_list(args.normalization_receipt_ref):
        return "receipt-only"
    if text_list(args.fetch_receipt_ref) or text_list(args.artifact_ref):
        return "fetched"
    return "executed"


def validate_status_ref_shape(status: str, args: argparse.Namespace) -> None:
    normalized_refs = text_list(args.normalized_signal_ref)
    normalization_receipts = text_list(args.normalization_receipt_ref)
    fetch_receipts = text_list(args.fetch_receipt_ref)
    artifact_refs = text_list(args.artifact_ref)
    if status == "normalized" and not normalized_refs:
        raise ValueError("source-acquisition status `normalized` requires at least one --normalized-signal-ref.")
    if status == "receipt-only" and normalized_refs:
        raise ValueError("source-acquisition status `receipt-only` cannot include --normalized-signal-ref; use `normalized`.")
    if status == "fetched" and (normalized_refs or normalization_receipts):
        raise ValueError("source-acquisition status `fetched` cannot include normalization refs; use `receipt-only` or `normalized`.")
    if status == "fetched" and not (fetch_receipts or artifact_refs):
        raise ValueError("source-acquisition status `fetched` requires a fetch receipt or artifact ref.")


def link_source_acquisition_execution(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    object_id = maybe_text(args.object_id)
    resolved_status = inferred_execution_status(args)
    validate_status_ref_shape(resolved_status, args)
    existing_result = fetch_dynamic_investigation_object_record(
        run_dir,
        object_id=object_id,
        object_kind=OBJECT_KIND,
        run_id=maybe_text(args.run_id),
        round_id=maybe_text(args.round_id),
    )
    proposal = (
        dict(existing_result.get("object", {}))
        if isinstance(existing_result.get("object"), dict)
        else {}
    )
    proposal_id = maybe_text(proposal.get("proposal_id")) or maybe_text(
        proposal.get("object_id")
    )
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"runtime/{OBJECT_KIND}-execution-link_{proposal_id}.json",
    )
    extra_link = parse_json_dict(
        args.execution_link_json,
        option_name="--execution-link-json",
    )
    provenance = parse_json_dict(args.provenance_json, option_name="--provenance-json")
    provenance.setdefault("source", SKILL_NAME)
    link = {
        **extra_link,
        "linked_at_utc": utc_now_iso(),
        "linked_by_role": maybe_text(args.actor_role),
        "status": resolved_status,
        "status_rationale": maybe_text(args.status_rationale),
        "fetch_receipt_refs": text_list(args.fetch_receipt_ref),
        "normalization_receipt_refs": text_list(args.normalization_receipt_ref),
        "normalized_signal_refs": text_list(args.normalized_signal_ref),
        "artifact_refs": text_list(args.artifact_ref),
        "evidence_refs": text_list(args.evidence_ref),
        "lineage": text_list(args.lineage_id),
        "provenance": provenance,
    }
    execution_links = (
        list(proposal.get("execution_links"))
        if isinstance(proposal.get("execution_links"), list)
        else []
    )
    execution_links.append(link)
    evidence_refs = text_list(
        [
            *args.evidence_ref,
            *args.fetch_receipt_ref,
            *args.normalization_receipt_ref,
            *args.normalized_signal_ref,
            *args.artifact_ref,
        ]
    )
    lineage = text_list(
        [
            *args.lineage_id,
            maybe_text(proposal.get("object_id")),
            *args.fetch_receipt_ref,
            *args.normalization_receipt_ref,
            *args.normalized_signal_ref,
            *args.artifact_ref,
        ]
    )
    update_result = update_dynamic_investigation_object_status(
        run_dir,
        object_id=object_id,
        object_kind=OBJECT_KIND,
        run_id=maybe_text(args.run_id),
        round_id=maybe_text(args.round_id),
        status=resolved_status,
        actor_role=maybe_text(args.actor_role),
        status_rationale=maybe_text(args.status_rationale),
        evidence_refs=evidence_refs,
        lineage=lineage,
        provenance=provenance,
        payload_updates={
            "execution_links": execution_links,
            "latest_execution_link": link,
            "fetch_receipt_refs": text_list(
                [
                    *(proposal.get("fetch_receipt_refs") if isinstance(proposal.get("fetch_receipt_refs"), list) else []),
                    *args.fetch_receipt_ref,
                ]
            ),
            "normalization_receipt_refs": text_list(
                [
                    *(proposal.get("normalization_receipt_refs") if isinstance(proposal.get("normalization_receipt_refs"), list) else []),
                    *args.normalization_receipt_ref,
                ]
            ),
            "normalized_signal_refs": text_list(
                [
                    *(proposal.get("normalized_signal_refs") if isinstance(proposal.get("normalized_signal_refs"), list) else []),
                    *args.normalized_signal_ref,
                ]
            ),
            "execution_artifact_refs": text_list(
                [
                    *(proposal.get("execution_artifact_refs") if isinstance(proposal.get("execution_artifact_refs"), list) else []),
                    *args.artifact_ref,
                ]
            ),
        },
        artifact_path=str(output_file),
    )
    updated = (
        update_result.get("object", {})
        if isinstance(update_result.get("object"), dict)
        else {}
    )
    artifact = {
        "schema_version": "source-acquisition-execution-link-v1",
        "skill": SKILL_NAME,
        "run_id": maybe_text(args.run_id),
        "round_id": maybe_text(args.round_id),
        "object_kind": OBJECT_KIND,
        "db_path": maybe_text(update_result.get("db_path")),
        "object": updated,
        "execution_link": link,
        "status_update": update_result.get("status_update", {}),
        "semantics": (
            "Execution links are audit lineage only; they do not rank sources or "
            "decide evidence acceptance."
        ),
    }
    write_json_file(output_file, artifact)
    result_payload = {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": maybe_text(args.run_id),
            "round_id": maybe_text(args.round_id),
            "object_kind": OBJECT_KIND,
            "object_id": maybe_text(updated.get("object_id")),
            "proposal_id": proposal_id,
            "source_skill": maybe_text(updated.get("source_skill")),
            "status": maybe_text(updated.get("status")),
            "fetch_receipt_ref_count": len(link["fetch_receipt_refs"]),
            "normalization_receipt_ref_count": len(link["normalization_receipt_refs"]),
            "normalized_signal_ref_count": len(link["normalized_signal_refs"]),
            "execution_link_count": len(updated.get("execution_links", []))
            if isinstance(updated.get("execution_links"), list)
            else 0,
            "output_path": str(output_file),
            "db_path": maybe_text(update_result.get("db_path")),
            "status_inference": "explicit" if maybe_text(args.status) else "derived-from-linked-refs",
        },
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.execution_link",
                "artifact_ref": f"{output_file}:$.execution_link",
            }
        ],
        "canonical_ids": [maybe_text(updated.get("object_id"))],
        "warnings": [],
        "execution_link": link,
        "council_handoff": {
            "object_refs": [
                {
                    "object_kind": OBJECT_KIND,
                    "object_id": maybe_text(updated.get("object_id")),
                }
            ],
            "query_command": query_command(
                run_dir=run_dir,
                run_id=maybe_text(args.run_id),
                round_id=maybe_text(args.round_id),
                object_id=maybe_text(updated.get("target_id")),
            ),
        },
    }
    result_payload["receipt_id"] = (
        "source-acquisition-execution-link-receipt-"
        + stable_hash(SKILL_NAME, args.run_id, args.round_id, proposal_id, pretty_json(result_payload, pretty=False))[:20]
    )
    return result_payload


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        payload = link_source_acquisition_execution(args)
    except ValueError as exc:
        sys.stdout.write(
            pretty_json(
                {
                    "status": "failed",
                    "summary": {"skill": SKILL_NAME, "object_kind": OBJECT_KIND},
                    "message": str(exc),
                },
                bool(getattr(args, "pretty", False)),
            )
        )
        sys.stdout.write("\n")
        return 1
    sys.stdout.write(pretty_json(payload, args.pretty))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
