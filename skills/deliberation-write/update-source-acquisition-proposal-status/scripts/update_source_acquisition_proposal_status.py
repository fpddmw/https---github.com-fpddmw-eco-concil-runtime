#!/usr/bin/env python3
"""Update one source-acquisition-proposal lifecycle status."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "update-source-acquisition-proposal-status"
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
    write_json_file,
)
from eco_council_runtime.kernel.planes.deliberation_plane import stable_hash  # noqa: E402
from eco_council_runtime.objects.council import (  # noqa: E402
    SOURCE_ACQUISITION_PROPOSAL_STATUSES,
    update_dynamic_investigation_object_status,
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
    status: str,
) -> str:
    return shlex.join(
        [
            "python3",
            "eco-concil-runtime/scripts/eco_runtime_kernel.py",
            "show-source-acquisition-intents",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--source-skill",
            source_skill,
            "--status",
            status,
            "--pretty",
        ]
    )


def source_intents_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    status: str,
) -> str:
    return shlex.join(
        [
            "python3",
            "eco-concil-runtime/scripts/eco_runtime_kernel.py",
            "show-source-acquisition-intents",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--status",
            status,
            "--pretty",
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Update one source-acquisition-proposal lifecycle status."
    )
    parser.add_argument("--run-dir", "--run_dir", required=True)
    parser.add_argument("--run-id", "--run_id", required=True)
    parser.add_argument("--round-id", "--round_id", required=True)
    parser.add_argument("--object-id", "--proposal-id", required=True)
    parser.add_argument(
        "--status",
        required=True,
        choices=SOURCE_ACQUISITION_PROPOSAL_STATUSES,
    )
    parser.add_argument("--actor-role", "--actor_role", required=True)
    parser.add_argument("--status-rationale", "--rationale", default="")
    parser.add_argument("--evidence-ref", action="append", default=[])
    parser.add_argument("--lineage-id", action="append", default=[])
    parser.add_argument("--provenance-json", default="{}")
    parser.add_argument("--output-path", "--output_path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser


def update_source_acquisition_proposal_status(
    args: argparse.Namespace,
) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    object_id = maybe_text(args.object_id)
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"runtime/{OBJECT_KIND}-status_{object_id}_{maybe_text(args.status)}.json",
    )
    provenance = parse_json_dict(args.provenance_json, option_name="--provenance-json")
    provenance.setdefault("source", SKILL_NAME)
    update_result = update_dynamic_investigation_object_status(
        run_dir,
        object_id=object_id,
        object_kind=OBJECT_KIND,
        run_id=maybe_text(args.run_id),
        round_id=maybe_text(args.round_id),
        status=maybe_text(args.status),
        actor_role=maybe_text(args.actor_role),
        status_rationale=maybe_text(args.status_rationale),
        evidence_refs=list(args.evidence_ref or []),
        lineage=list(args.lineage_id or []),
        provenance=provenance,
        artifact_path=str(output_file),
    )
    proposal = (
        update_result.get("object", {})
        if isinstance(update_result.get("object"), dict)
        else {}
    )
    proposal_id = maybe_text(proposal.get("proposal_id")) or maybe_text(
        proposal.get("object_id")
    )
    artifact = {
        "schema_version": "source-acquisition-proposal-status-update-v1",
        "skill": SKILL_NAME,
        "run_id": maybe_text(args.run_id),
        "round_id": maybe_text(args.round_id),
        "object_kind": OBJECT_KIND,
        "db_path": maybe_text(update_result.get("db_path")),
        "object": proposal,
        "status_update": update_result.get("status_update", {}),
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
            "source_skill": maybe_text(proposal.get("source_skill")),
            "previous_status": maybe_text(
                update_result.get("status_update", {}).get("previous_status")
            )
            if isinstance(update_result.get("status_update"), dict)
            else "",
            "status": maybe_text(proposal.get("status")),
            "output_path": str(output_file),
            "db_path": maybe_text(update_result.get("db_path")),
        },
        "receipt_id": "source-acquisition-proposal-status-receipt-"
        + stable_hash(SKILL_NAME, args.run_id, args.round_id, proposal_id, args.status)[:20],
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
                status=maybe_text(proposal.get("status")),
            ),
            "source_intents_command": source_intents_command(
                run_dir=run_dir,
                run_id=maybe_text(args.run_id),
                round_id=maybe_text(args.round_id),
                status=maybe_text(proposal.get("status")),
            ),
        },
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        payload = update_source_acquisition_proposal_status(args)
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
