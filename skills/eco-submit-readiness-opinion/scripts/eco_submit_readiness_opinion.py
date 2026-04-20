#!/usr/bin/env python3
"""Submit one structured readiness opinion directly into the deliberation DB."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-submit-readiness-opinion"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import append_readiness_opinion_record  # noqa: E402
from eco_council_runtime.council_submission_support import (  # noqa: E402
    maybe_text,
    readiness_opinion_payload,
    resolve_path,
    resolve_run_dir,
    unique_texts,
    write_json_file,
)
from eco_council_runtime.kernel.deliberation_plane import stable_hash  # noqa: E402


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def readiness_follow_up_skills(opinion: dict[str, Any]) -> list[str]:
    readiness_status = maybe_text(opinion.get("readiness_status"))
    suggestions = ["eco-read-board-delta"]
    if bool(opinion.get("sufficient_for_promotion")) or readiness_status in {
        "promote",
        "ready",
        "ready-for-promotion",
    }:
        suggestions.extend(["eco-promote-evidence-basis", "eco-materialize-reporting-handoff"])
    else:
        suggestions.extend(
            [
                "eco-submit-council-proposal",
                "eco-open-falsification-probe",
            ]
        )
    return unique_texts(suggestions)


def readiness_gap_hints(opinion: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    if not isinstance(opinion.get("basis_object_ids"), list) or not opinion.get("basis_object_ids"):
        hints.append("Readiness opinion does not identify any basis objects yet.")
    if not isinstance(opinion.get("evidence_refs"), list) or not opinion.get("evidence_refs"):
        hints.append("Readiness opinion does not cite any evidence refs yet.")
    return hints[:2]


def readiness_challenge_hints(opinion: dict[str, Any]) -> list[str]:
    readiness_status = maybe_text(opinion.get("readiness_status"))
    if readiness_status in {"blocked", "reject", "rejected"}:
        return [
            "This opinion blocks promotion, so the council should surface the blocking proposal or contradiction path explicitly."
        ]
    return []


def submit_readiness_opinion_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    opinion_id: str,
    agent_role: str,
    readiness_status: str,
    rationale: str,
    decision_source: str,
    opinion_status: str,
    sufficient_for_promotion: str,
    confidence: str,
    basis_object_ids: list[str],
    basis_object_ids_json: str,
    evidence_refs: list[str],
    evidence_refs_json: str,
    lineage: list[str],
    lineage_json: str,
    provenance_json: str,
    extra_json: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    opinion_payload = readiness_opinion_payload(
        run_id=run_id,
        round_id=round_id,
        opinion_id=opinion_id,
        agent_role=agent_role,
        readiness_status=readiness_status,
        rationale=rationale,
        decision_source=decision_source,
        opinion_status=opinion_status,
        sufficient_for_promotion=sufficient_for_promotion,
        confidence=confidence,
        basis_object_ids=basis_object_ids,
        basis_object_ids_json=basis_object_ids_json,
        evidence_refs=evidence_refs,
        evidence_refs_json=evidence_refs_json,
        lineage=lineage,
        lineage_json=lineage_json,
        provenance_json=provenance_json,
        extra_json=extra_json,
        source_skill=SKILL_NAME,
    )
    append_result = append_readiness_opinion_record(
        run_dir_path,
        opinion_payload=opinion_payload,
    )
    opinion = (
        append_result.get("opinion", {})
        if isinstance(append_result.get("opinion"), dict)
        else {}
    )
    opinion_identifier = maybe_text(opinion.get("opinion_id"))
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"runtime/readiness_opinion_{opinion_identifier}.json",
    )
    artifact = {
        "schema_version": "readiness-opinion-submission-v1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "db_path": maybe_text(append_result.get("db_path")),
        "opinion": opinion,
    }
    write_json_file(output_file, artifact)
    artifact_ref = {
        "signal_id": "",
        "artifact_path": str(output_file),
        "record_locator": "$.opinion",
        "artifact_ref": f"{output_file}:$.opinion",
    }
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "opinion_id": opinion_identifier,
            "agent_role": maybe_text(opinion.get("agent_role")),
            "readiness_status": maybe_text(opinion.get("readiness_status")),
            "output_path": str(output_file),
            "db_path": maybe_text(append_result.get("db_path")),
            "basis_object_count": len(opinion.get("basis_object_ids", []))
            if isinstance(opinion.get("basis_object_ids"), list)
            else 0,
        },
        "receipt_id": "council-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, opinion_identifier)[:20],
        "batch_id": "councilbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": [artifact_ref],
        "canonical_ids": [opinion_identifier],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": unique_texts(
                [opinion_identifier]
                + (
                    opinion.get("basis_object_ids", [])
                    if isinstance(opinion.get("basis_object_ids"), list)
                    else []
                )
            ),
            "evidence_refs": [artifact_ref],
            "gap_hints": readiness_gap_hints(opinion),
            "challenge_hints": readiness_challenge_hints(opinion),
            "suggested_next_skills": readiness_follow_up_skills(opinion),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit one structured readiness opinion into the deliberation DB."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--opinion-id", default="")
    parser.add_argument("--agent-role", required=True)
    parser.add_argument("--readiness-status", required=True)
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--decision-source", default="agent-council")
    parser.add_argument("--opinion-status", default="submitted")
    parser.add_argument("--sufficient-for-promotion", default="")
    parser.add_argument("--confidence", default="")
    parser.add_argument("--basis-object-id", action="append", default=[])
    parser.add_argument("--basis-object-ids-json", default="")
    parser.add_argument("--evidence-ref", action="append", default=[])
    parser.add_argument("--evidence-refs-json", default="")
    parser.add_argument("--lineage-id", action="append", default=[])
    parser.add_argument("--lineage-json", default="")
    parser.add_argument("--provenance-json", default="")
    parser.add_argument("--extra-json", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = submit_readiness_opinion_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        opinion_id=args.opinion_id,
        agent_role=args.agent_role,
        readiness_status=args.readiness_status,
        rationale=args.rationale,
        decision_source=args.decision_source,
        opinion_status=args.opinion_status,
        sufficient_for_promotion=args.sufficient_for_promotion,
        confidence=args.confidence,
        basis_object_ids=args.basis_object_id,
        basis_object_ids_json=args.basis_object_ids_json,
        evidence_refs=args.evidence_ref,
        evidence_refs_json=args.evidence_refs_json,
        lineage=args.lineage_id,
        lineage_json=args.lineage_json,
        provenance_json=args.provenance_json,
        extra_json=args.extra_json,
        output_path=args.output_path,
    )
    sys.stdout.write(pretty_json(payload, args.pretty))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
