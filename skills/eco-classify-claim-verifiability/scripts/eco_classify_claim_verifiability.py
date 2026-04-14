#!/usr/bin/env python3
"""Classify claim-side controversy objects into explicit verifiability lanes."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-classify-claim-verifiability"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_scope_context,
    sync_claim_verifiability_result_set,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, path_text: str, default_name: str) -> Path:
    text = maybe_text(path_text)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def list_field(item: dict[str, Any], key: str) -> list[str]:
    values = item.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def recommended_lane_from_scope(scope: dict[str, Any]) -> str:
    explicit = maybe_text(scope.get("required_evidence_lane"))
    if explicit:
        return explicit
    verifiability_kind = maybe_text(scope.get("verifiability_kind"))
    if verifiability_kind == "empirical-observable":
        return "environmental-observation"
    if verifiability_kind == "procedural-record":
        return "formal-comment-and-policy-record"
    if verifiability_kind == "discourse-representation":
        return "public-discourse-analysis"
    if verifiability_kind == "normative-distribution":
        return "stakeholder-deliberation-analysis"
    if verifiability_kind == "predictive-uncertainty":
        return "mixed-review"
    return "route-before-matching"


def risk_flags_for_scope(scope: dict[str, Any], lane: str) -> list[str]:
    flags: list[str] = []
    citations = list_field(scope, "evidence_citation_types")
    if lane != "environmental-observation":
        flags.append("non-empirical-by-default")
    if "rumor-hearsay" in citations:
        flags.append("weak-citation-base")
    if not bool((scope.get("claim_scope") or {}).get("usable_for_matching")):
        flags.append("not-matching-ready")
    return flags


def assessment_summary(scope: dict[str, Any], lane: str) -> str:
    issue_hint = maybe_text(scope.get("issue_hint")) or maybe_text(scope.get("claim_type")) or "public controversy"
    verifiability_kind = maybe_text(scope.get("verifiability_kind")) or "mixed-public-claim"
    route_text = lane.replace("-", " ")
    return (
        f"Issue {issue_hint} is classified as {verifiability_kind} and should primarily use the "
        f"{route_text} lane."
    )


def classify_claim_verifiability_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_scope_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"claim_verifiability_assessments_{round_id}.json",
    )
    scope_context = load_claim_scope_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_scope_path=claim_scope_path,
    )
    warnings = (
        scope_context.get("warnings", [])
        if isinstance(scope_context.get("warnings"), list)
        else []
    )
    claim_scopes = (
        scope_context.get("claim_scopes", [])
        if isinstance(scope_context.get("claim_scopes"), list)
        else []
    )
    input_file = Path(maybe_text(scope_context.get("claim_scope_file")))
    input_source = maybe_text(scope_context.get("claim_scope_source")) or "missing-claim-scope"

    assessments: list[dict[str, Any]] = []
    for scope in claim_scopes:
        if not isinstance(scope, dict):
            continue
        claim_id = maybe_text(scope.get("claim_id"))
        claim_scope_id = maybe_text(scope.get("claim_scope_id"))
        lane = recommended_lane_from_scope(scope)
        verifiability_kind = maybe_text(scope.get("verifiability_kind")) or "mixed-public-claim"
        route_to_matching = (
            lane == "environmental-observation"
            and bool((scope.get("claim_scope") or {}).get("usable_for_matching"))
        )
        assessment_id = "verif-" + stable_hash(run_id, round_id, claim_scope_id, lane)[:12]
        assessments.append(
            {
                "schema_version": "n3.0",
                "assessment_id": assessment_id,
                "run_id": run_id,
                "round_id": round_id,
                "claim_id": claim_id,
                "claim_scope_id": claim_scope_id,
                "claim_type": maybe_text(scope.get("claim_type")),
                "issue_hint": maybe_text(scope.get("issue_hint")),
                "issue_terms": list_field(scope, "issue_terms"),
                "concern_facets": list_field(scope, "concern_facets"),
                "actor_hints": list_field(scope, "actor_hints"),
                "evidence_citation_types": list_field(scope, "evidence_citation_types"),
                "verifiability_kind": verifiability_kind,
                "dispute_type": maybe_text(scope.get("dispute_type")) or "mixed-controversy",
                "recommended_lane": lane,
                "route_to_observation_matching": route_to_matching,
                "route_blocked": not route_to_matching,
                "risk_flags": risk_flags_for_scope(scope, lane),
                "assessment_summary": assessment_summary(scope, lane),
                "confidence": float(scope.get("confidence") or 0.6),
                "evidence_refs": unique_texts(
                    scope.get("evidence_refs", [])
                    if isinstance(scope.get("evidence_refs"), list)
                    else []
                ),
            }
        )

    lane_counts: dict[str, int] = {}
    for assessment in assessments:
        lane = maybe_text(assessment.get("recommended_lane"))
        if not lane:
            continue
        lane_counts[lane] = lane_counts.get(lane, 0) + 1

    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "input_path": str(input_file),
            "input_source": input_source,
            "selection_mode": "classify-each-claim-scope",
            "method": "controversy-verifiability-routing-v1",
        },
        "input_path": str(input_file),
        "input_source": input_source,
        "observed_inputs": {
            "claim_scope_present": bool(claim_scopes),
            "claim_scope_artifact_present": bool(
                scope_context.get("claim_scope_artifact_present")
            ),
        },
        "input_analysis_sync": {"claim_scope": scope_context.get("analysis_sync", {})},
        "assessment_count": len(assessments),
        "lane_counts": lane_counts,
        "assessments": assessments,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_claim_verifiability_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        claim_verifiability_path=output_file,
        db_path=maybe_text(scope_context.get("db_path")),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.assessments",
            "artifact_ref": f"{output_file}:$.assessments",
        }
    ]
    for assessment in assessments:
        artifact_refs.extend(
            [
                {
                    "signal_id": "",
                    "artifact_path": str(output_file),
                    "record_locator": f"$.assessments[?(@.assessment_id=='{assessment['assessment_id']}')]",
                    "artifact_ref": f"{output_file}:assessment:{assessment['assessment_id']}",
                }
            ]
        )
    if not assessments:
        warnings.append(
            {
                "code": "no-verifiability-assessments",
                "message": "No claim verifiability assessments were produced from the available claim scopes.",
            }
        )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "assessment_count": len(assessments),
            "input_source": input_source,
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "verifiability-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "verifbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs[:40],
        "canonical_ids": [
            maybe_text(assessment.get("assessment_id"))
            for assessment in assessments
            if maybe_text(assessment.get("assessment_id"))
        ],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [
                maybe_text(assessment.get("assessment_id"))
                for assessment in assessments
                if maybe_text(assessment.get("assessment_id"))
            ],
            "evidence_refs": artifact_refs[:20],
            "gap_hints": (
                [
                    "Some claims are explicitly non-empirical and should not be sent to observation matching by default."
                ]
                if any(
                    maybe_text(assessment.get("recommended_lane"))
                    != "environmental-observation"
                    for assessment in assessments
                    if isinstance(assessment, dict)
                )
                else []
            ),
            "challenge_hints": (
                ["Review assessments with weak citation bases before escalating them."]
                if any(
                    "weak-citation-base" in list_field(assessment, "risk_flags")
                    for assessment in assessments
                    if isinstance(assessment, dict)
                )
                else []
            ),
            "suggested_next_skills": [
                "eco-route-verification-lane",
                "eco-materialize-controversy-map",
                "eco-post-board-note",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify claim-side controversy objects into explicit verifiability lanes."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = classify_claim_verifiability_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_scope_path=args.claim_scope_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
