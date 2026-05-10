#!/usr/bin/env python3
"""Submit one source-acquisition-proposal coordination object."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "submit-source-acquisition-proposal"
OBJECT_KIND = "source-acquisition-proposal"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_submission_support import (  # noqa: E402
    maybe_text,
    parse_json_dict,
    resolve_path,
    resolve_run_dir,
    write_json_file,
)
from eco_council_runtime.kernel.planes.deliberation_plane import stable_hash  # noqa: E402
from eco_council_runtime.objects.council import (  # noqa: E402
    append_dynamic_investigation_object_record,
)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def query_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    source_skill: str,
) -> str:
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
            "--source-skill",
            source_skill,
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit one source-acquisition-proposal deliberation object."
    )
    parser.add_argument("--run-dir", "--run_dir", required=True)
    parser.add_argument("--run-id", "--run_id", required=True)
    parser.add_argument("--round-id", "--round_id", required=True)
    parser.add_argument("--object-id", default="")
    parser.add_argument("--author-role", required=True)
    parser.add_argument("--status", default="")
    parser.add_argument("--source-skill", "--source_skill", required=True)
    parser.add_argument("--query-parameters-json", "--query_parameters_json", default="{}")
    parser.add_argument("--target-kind", default="round")
    parser.add_argument("--target-id", default="")
    parser.add_argument("--target-evidence-request-id", "--target_evidence_request_id", default="")
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--declared-side-effect", "--declared_side_effect", action="append", default=[])
    parser.add_argument(
        "--requested-side-effect-approval",
        "--requested_side_effect_approval",
        action="append",
        default=[],
    )
    parser.add_argument("--evidence-ref", action="append", default=[])
    parser.add_argument("--lineage-id", action="append", default=[])
    parser.add_argument("--provenance-json", default="{}")
    parser.add_argument("--payload-json", "--extra-json", default="{}")
    parser.add_argument("--output-path", "--output_path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser


def proposal_payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    payload = parse_json_dict(args.payload_json, option_name="--payload-json")
    payload_kind = maybe_text(payload.get("object_kind"))
    if payload_kind and payload_kind != OBJECT_KIND:
        raise ValueError(
            f"--payload-json object_kind `{payload_kind}` does not match `{OBJECT_KIND}`."
        )

    target_kind = maybe_text(args.target_kind) or "round"
    target_id = maybe_text(args.target_id)
    target_evidence_request_id = maybe_text(args.target_evidence_request_id)
    if target_evidence_request_id and (not target_id or target_kind == "round"):
        target_kind = "evidence-request"
        target_id = target_evidence_request_id

    provenance = payload.get("provenance") if isinstance(payload.get("provenance"), dict) else {}
    provenance.update(parse_json_dict(args.provenance_json, option_name="--provenance-json"))

    existing_evidence_refs = (
        payload.get("evidence_refs") if isinstance(payload.get("evidence_refs"), list) else []
    )
    existing_lineage = payload.get("lineage") if isinstance(payload.get("lineage"), list) else []
    payload.update(
        {
            "run_id": maybe_text(args.run_id),
            "round_id": maybe_text(args.round_id),
            "object_kind": OBJECT_KIND,
            "author_role": maybe_text(args.author_role),
            "source_skill": maybe_text(args.source_skill),
            "query_parameters": parse_json_dict(
                args.query_parameters_json,
                option_name="--query-parameters-json",
            ),
            "declared_side_effects": [
                maybe_text(item) for item in args.declared_side_effect if maybe_text(item)
            ],
            "requested_side_effect_approvals": [
                maybe_text(item)
                for item in args.requested_side_effect_approval
                if maybe_text(item)
            ],
            "target_kind": target_kind,
            "target_id": target_id,
            "target_evidence_request_id": target_evidence_request_id,
            "rationale": maybe_text(args.rationale),
            "evidence_refs": [*existing_evidence_refs, *args.evidence_ref],
            "lineage": [*existing_lineage, *args.lineage_id],
            "provenance": provenance or {"source": SKILL_NAME},
        }
    )
    if maybe_text(args.object_id):
        payload["object_id"] = maybe_text(args.object_id)
    if maybe_text(args.status):
        payload["status"] = maybe_text(args.status)
    return payload


def submit_source_acquisition_proposal(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    proposal_payload = proposal_payload_from_args(args)
    append_result = append_dynamic_investigation_object_record(
        run_dir,
        object_payload=proposal_payload,
        object_kind=OBJECT_KIND,
    )
    proposal = (
        append_result.get("object", {})
        if isinstance(append_result.get("object"), dict)
        else {}
    )
    proposal_id = maybe_text(proposal.get("proposal_id")) or maybe_text(
        proposal.get("object_id")
    )
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"runtime/{OBJECT_KIND}_{proposal_id}.json",
    )
    artifact = {
        "schema_version": "source-acquisition-proposal-submission-v1",
        "skill": SKILL_NAME,
        "run_id": maybe_text(args.run_id),
        "round_id": maybe_text(args.round_id),
        "object_kind": OBJECT_KIND,
        "db_path": maybe_text(append_result.get("db_path")),
        "object": proposal,
    }
    write_json_file(output_file, artifact)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": maybe_text(args.run_id),
            "round_id": maybe_text(args.round_id),
            "object_kind": OBJECT_KIND,
            "object_id": maybe_text(proposal.get("object_id")),
            "proposal_id": proposal_id,
            "author_role": maybe_text(proposal.get("author_role")),
            "source_skill": maybe_text(proposal.get("source_skill")),
            "status": maybe_text(proposal.get("status")),
            "output_path": str(output_file),
            "db_path": maybe_text(append_result.get("db_path")),
        },
        "receipt_id": "source-acquisition-proposal-receipt-"
        + stable_hash(SKILL_NAME, args.run_id, args.round_id, proposal_id)[:20],
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.object",
                "artifact_ref": f"{output_file}:$.object",
            }
        ],
        "canonical_ids": [maybe_text(proposal.get("object_id"))],
        "warnings": [],
        "council_handoff": {
            "object_refs": [
                {
                    "object_kind": OBJECT_KIND,
                    "object_id": maybe_text(proposal.get("object_id")),
                }
            ],
            "target_ref": {
                "object_kind": maybe_text(proposal.get("target_kind")),
                "object_id": maybe_text(proposal.get("target_id")),
            },
            "source_skill": maybe_text(proposal.get("source_skill")),
            "query_command": query_command(
                run_dir=run_dir,
                run_id=maybe_text(args.run_id),
                round_id=maybe_text(args.round_id),
                source_skill=maybe_text(proposal.get("source_skill")),
            ),
        },
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        payload = submit_source_acquisition_proposal(args)
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
