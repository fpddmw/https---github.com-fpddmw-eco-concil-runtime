#!/usr/bin/env python3
"""Route claim-side controversy objects into explicit next lanes."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-route-verification-lane"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_scope_context,
    load_claim_verifiability_context,
    sync_verification_route_result_set,
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


def resolve_path(run_dir: Path, path_text: str, default_relative: str) -> Path:
    text = maybe_text(path_text)
    if not text:
        return (run_dir / default_relative).resolve()
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


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def lane_from_scope(scope: dict[str, Any]) -> str:
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


def fallback_assessment_from_scope(scope: dict[str, Any]) -> dict[str, Any]:
    claim_scope_id = maybe_text(scope.get("claim_scope_id"))
    lane = lane_from_scope(scope)
    return {
        "assessment_id": "verif-fallback-" + stable_hash(claim_scope_id, lane)[:12],
        "claim_id": maybe_text(scope.get("claim_id")),
        "claim_scope_id": claim_scope_id,
        "claim_type": maybe_text(scope.get("claim_type")),
        "issue_hint": maybe_text(scope.get("issue_hint")),
        "concern_facets": list_field(scope, "concern_facets"),
        "evidence_citation_types": list_field(scope, "evidence_citation_types"),
        "verifiability_kind": maybe_text(scope.get("verifiability_kind"))
        or "mixed-public-claim",
        "dispute_type": maybe_text(scope.get("dispute_type")) or "mixed-controversy",
        "recommended_lane": lane,
        "route_to_observation_matching": bool(
            (scope.get("claim_scope") or {}).get("usable_for_matching")
        )
        and lane == "environmental-observation",
        "confidence": float(scope.get("confidence") or 0.6),
        "evidence_refs": unique_texts(
            scope.get("evidence_refs", [])
            if isinstance(scope.get("evidence_refs"), list)
            else []
        ),
    }


def route_status_for_lane(lane: str) -> str:
    if lane == "environmental-observation":
        return "route-to-verification-lane"
    if lane == "formal-comment-and-policy-record":
        return "route-to-formal-record-review"
    if lane == "public-discourse-analysis":
        return "keep-in-public-discourse-analysis"
    if lane == "stakeholder-deliberation-analysis":
        return "keep-in-stakeholder-deliberation"
    return "mixed-routing-review"


def suggested_skills_for_lane(lane: str) -> list[str]:
    if lane == "environmental-observation":
        return [
            "eco-link-claims-to-observations",
            "eco-score-evidence-coverage",
            "eco-post-board-note",
        ]
    if lane == "formal-comment-and-policy-record":
        return [
            "eco-build-normalization-audit",
            "eco-post-board-note",
            "eco-materialize-controversy-map",
        ]
    if lane == "public-discourse-analysis":
        return [
            "eco-post-board-note",
            "eco-materialize-controversy-map",
        ]
    return [
        "eco-post-board-note",
        "eco-materialize-controversy-map",
    ]


def route_reason(assessment: dict[str, Any], lane: str) -> str:
    issue_hint = maybe_text(assessment.get("issue_hint")) or maybe_text(
        assessment.get("claim_type")
    ) or "public controversy"
    verifiability_kind = maybe_text(assessment.get("verifiability_kind")) or "mixed-public-claim"
    return (
        f"Issue {issue_hint} is treated as {verifiability_kind}, so the next lane is "
        f"{lane.replace('-', ' ')}."
    )


def route_verification_lane_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_verifiability_path: str,
    claim_scope_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"investigation/verification_routes_{round_id}.json",
    )
    verifiability_context = load_claim_verifiability_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_verifiability_path=claim_verifiability_path,
    )
    scope_context = load_claim_scope_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_scope_path=claim_scope_path,
        db_path=maybe_text(verifiability_context.get("db_path")),
    )
    warnings = (
        verifiability_context.get("warnings", [])
        if isinstance(verifiability_context.get("warnings"), list)
        else []
    )
    warnings.extend(
        scope_context.get("warnings", [])
        if isinstance(scope_context.get("warnings"), list)
        else []
    )

    assessments = (
        verifiability_context.get("assessments", [])
        if isinstance(verifiability_context.get("assessments"), list)
        else []
    )
    input_path = Path(maybe_text(verifiability_context.get("claim_verifiability_file")))
    input_source = (
        maybe_text(verifiability_context.get("claim_verifiability_source"))
        or "missing-claim-verifiability"
    )
    if not assessments:
        claim_scopes = (
            scope_context.get("claim_scopes", [])
            if isinstance(scope_context.get("claim_scopes"), list)
            else []
        )
        assessments = [
            fallback_assessment_from_scope(scope)
            for scope in claim_scopes
            if isinstance(scope, dict)
        ]
        input_path = Path(maybe_text(scope_context.get("claim_scope_file")))
        input_source = (
            maybe_text(scope_context.get("claim_scope_source"))
            or "missing-claim-scope"
        )

    routes: list[dict[str, Any]] = []
    for assessment in assessments:
        if not isinstance(assessment, dict):
            continue
        lane = maybe_text(assessment.get("recommended_lane")) or "route-before-matching"
        route_status = route_status_for_lane(lane)
        claim_id = maybe_text(assessment.get("claim_id"))
        assessment_id = maybe_text(assessment.get("assessment_id"))
        route_id = "route-" + stable_hash(run_id, round_id, claim_id, lane)[:12]
        should_query_environment = lane == "environmental-observation"
        routes.append(
            {
                "schema_version": "n3.0",
                "route_id": route_id,
                "run_id": run_id,
                "round_id": round_id,
                "claim_id": claim_id,
                "assessment_id": assessment_id,
                "claim_scope_id": maybe_text(assessment.get("claim_scope_id")),
                "issue_hint": maybe_text(assessment.get("issue_hint")),
                "verifiability_kind": maybe_text(assessment.get("verifiability_kind"))
                or "mixed-public-claim",
                "dispute_type": maybe_text(assessment.get("dispute_type"))
                or "mixed-controversy",
                "recommended_lane": lane,
                "route_status": route_status,
                "should_query_environment": should_query_environment,
                "should_link_observations": should_query_environment,
                "route_reason": route_reason(assessment, lane),
                "suggested_next_skills": suggested_skills_for_lane(lane),
                "confidence": float(assessment.get("confidence") or 0.6),
                "evidence_refs": unique_texts(
                    assessment.get("evidence_refs", [])
                    if isinstance(assessment.get("evidence_refs"), list)
                    else []
                ),
            }
        )

    lane_counts: dict[str, int] = {}
    route_status_counts: dict[str, int] = {}
    for route in routes:
        lane = maybe_text(route.get("recommended_lane"))
        status_value = maybe_text(route.get("route_status"))
        if lane:
            lane_counts[lane] = lane_counts.get(lane, 0) + 1
        if status_value:
            route_status_counts[status_value] = route_status_counts.get(status_value, 0) + 1

    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "input_path": str(input_path),
            "input_source": input_source,
            "selection_mode": "route-each-verifiability-assessment",
            "method": "controversy-verification-router-v1",
        },
        "input_path": str(input_path),
        "input_source": input_source,
        "observed_inputs": {
            "claim_verifiability_present": bool(
                source_available(verifiability_context.get("claim_verifiability_source"))
            ),
            "claim_verifiability_artifact_present": bool(
                verifiability_context.get("claim_verifiability_artifact_present")
            ),
            "claim_scope_present": bool(
                source_available(scope_context.get("claim_scope_source"))
            ),
            "claim_scope_artifact_present": bool(
                scope_context.get("claim_scope_artifact_present")
            ),
        },
        "input_analysis_sync": {
            "claim_verifiability": verifiability_context.get("analysis_sync", {}),
            "claim_scope": scope_context.get("analysis_sync", {}),
        },
        "route_count": len(routes),
        "lane_counts": lane_counts,
        "route_status_counts": route_status_counts,
        "routes": routes,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_verification_route_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        verification_route_path=output_file,
        db_path=maybe_text(verifiability_context.get("db_path"))
        or maybe_text(scope_context.get("db_path")),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.routes",
            "artifact_ref": f"{output_file}:$.routes",
        }
    ]
    if not routes:
        warnings.append(
            {
                "code": "no-verification-routes",
                "message": "No verification routes were produced from the available claim-side inputs.",
            }
        )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "route_count": len(routes),
            "input_source": input_source,
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "routing-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "routebatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [
            maybe_text(route.get("route_id"))
            for route in routes
            if maybe_text(route.get("route_id"))
        ],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [
                maybe_text(route.get("route_id"))
                for route in routes
                if maybe_text(route.get("route_id"))
            ],
            "evidence_refs": artifact_refs,
            "gap_hints": (
                [
                    "Some claims remain outside the environmental verification lane and still need discourse or formal-record handling."
                ]
                if any(
                    not bool(route.get("should_query_environment"))
                    for route in routes
                    if isinstance(route, dict)
                )
                else []
            ),
            "challenge_hints": (
                ["Review whether any empirical route is still too weakly scoped for observation matching."]
                if any(
                    maybe_text(route.get("recommended_lane"))
                    == "environmental-observation"
                    and not bool(route.get("should_link_observations"))
                    for route in routes
                    if isinstance(route, dict)
                )
                else []
            ),
            "suggested_next_skills": [
                "eco-materialize-controversy-map",
                "eco-propose-next-actions",
                "eco-post-board-note",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Route claim-side controversy objects into explicit next lanes."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-verifiability-path", default="")
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = route_verification_lane_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_verifiability_path=args.claim_verifiability_path,
        claim_scope_path=args.claim_scope_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
