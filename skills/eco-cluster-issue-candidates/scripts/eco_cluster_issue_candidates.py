#!/usr/bin/env python3
"""Cluster claim-side controversy inputs into canonical issue-cluster objects."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

SKILL_NAME = "eco-cluster-issue-candidates"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.controversy_issue_surfaces import (  # noqa: E402
    actionable_gaps_for_issue_clusters,
    artifact_ref_for_path,
    build_controversy_issue_map_items,
    build_typed_issue_surfaces,
    claim_chain_query_basis,
    fallback_clusters_from_scopes,
    load_claim_chain_contexts,
    maybe_text,
    resolve_path,
    resolve_run_dir,
    stable_hash,
    sync_result_wrapper,
    typed_surface_output_paths,
    utc_now_iso,
)
from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    sync_issue_cluster_result_set,
)


def pretty_json(data: object, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def issue_label_counts(issue_clusters: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in issue_clusters:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("issue_label"))
        if not label:
            continue
        counts[label] = counts.get(label, 0) + 1
    return counts


def cluster_issue_candidates_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_scope_path: str,
    claim_verifiability_path: str,
    verification_route_path: str,
    output_path: str,
) -> dict[str, object]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"analytics/issue_clusters_{round_id}.json",
    )
    contexts = load_claim_chain_contexts(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_cluster_path=claim_cluster_path,
        claim_scope_path=claim_scope_path,
        claim_verifiability_path=claim_verifiability_path,
        verification_route_path=verification_route_path,
    )
    warnings = (
        contexts.get("warnings", [])
        if isinstance(contexts.get("warnings"), list)
        else []
    )
    claim_scopes = (
        contexts["claim_scope"].get("claim_scopes", [])
        if isinstance(contexts["claim_scope"].get("claim_scopes"), list)
        else []
    )
    claim_clusters = (
        contexts["claim_cluster"].get("claim_clusters", [])
        if isinstance(contexts["claim_cluster"].get("claim_clusters"), list)
        else []
    )
    if not claim_clusters:
        claim_clusters = fallback_clusters_from_scopes(claim_scopes)
    issue_map_items = build_controversy_issue_map_items(
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        source_skill=SKILL_NAME,
        claim_clusters=claim_clusters,
        claim_scopes=claim_scopes,
        assessments=(
            contexts["claim_verifiability"].get("assessments", [])
            if isinstance(contexts["claim_verifiability"].get("assessments"), list)
            else []
        ),
        routes=(
            contexts["verification_route"].get("routes", [])
            if isinstance(contexts["verification_route"].get("routes"), list)
            else []
        ),
    )
    surface_paths = typed_surface_output_paths(
        run_dir_path,
        round_id=round_id,
        issue_clusters_path=str(output_file),
    )
    typed_surfaces = build_typed_issue_surfaces(
        issue_map_items,
        run_id=run_id,
        round_id=round_id,
        paths=surface_paths,
        source_skill=SKILL_NAME,
    )
    issue_clusters = typed_surfaces["issue-cluster"]
    generated_at_utc = utc_now_iso()
    actionable_gaps, lane_counts, route_status_counts, controversy_posture_counts, _, _ = (
        actionable_gaps_for_issue_clusters(issue_map_items)
    )
    query_basis = claim_chain_query_basis(
        contexts,
        selection_mode="cluster-issue-candidates-from-claim-chain",
        method="controversy-issue-clustering-v1",
    )
    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": query_basis,
        **query_basis,
        "observed_inputs": {
            "claim_clusters_present": bool(claim_clusters),
            "claim_clusters_artifact_present": bool(
                contexts["claim_cluster"].get("claim_cluster_artifact_present")
            ),
            "claim_scope_present": bool(
                contexts["claim_scope"].get("claim_scopes")
            ),
            "claim_scope_artifact_present": bool(
                contexts["claim_scope"].get("claim_scope_artifact_present")
            ),
            "claim_verifiability_present": bool(
                contexts["claim_verifiability"].get("assessments")
            ),
            "claim_verifiability_artifact_present": bool(
                contexts["claim_verifiability"].get(
                    "claim_verifiability_artifact_present"
                )
            ),
            "verification_route_present": bool(
                contexts["verification_route"].get("routes")
            ),
            "verification_route_artifact_present": bool(
                contexts["verification_route"].get(
                    "verification_route_artifact_present"
                )
            ),
        },
        "input_analysis_sync": {
            "claim_clusters": contexts["claim_cluster"].get("analysis_sync", {}),
            "claim_scope": contexts["claim_scope"].get("analysis_sync", {}),
            "claim_verifiability": contexts["claim_verifiability"].get(
                "analysis_sync", {}
            ),
            "verification_route": contexts["verification_route"].get(
                "analysis_sync", {}
            ),
        },
        "derived_from_controversy_issue_count": len(issue_map_items),
        "issue_cluster_count": len(issue_clusters),
        "issue_label_counts": issue_label_counts(issue_clusters),
        "lane_counts": lane_counts,
        "route_status_counts": route_status_counts,
        "controversy_posture_counts": controversy_posture_counts,
        "issue_clusters": issue_clusters,
    }
    analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        wrapper=wrapper,
        db_path=maybe_text(contexts.get("db_path")),
        sync_callable=sync_issue_cluster_result_set,
        path_kwarg="issue_clusters_path",
    )
    artifact_ref = artifact_ref_for_path(output_file, "$.issue_clusters")
    canonical_ids = [
        maybe_text(item.get("cluster_id"))
        for item in issue_clusters
        if isinstance(item, dict) and maybe_text(item.get("cluster_id"))
    ]
    if not issue_clusters:
        warnings = [
            *warnings,
            {
                "code": "no-issue-clusters",
                "message": "No canonical issue-cluster objects were produced from the available claim-side inputs.",
            },
        ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "issue_cluster_count": len(issue_clusters),
            "cluster_source": maybe_text(wrapper.get("cluster_source"))
            or "missing-claim-cluster",
            "route_source": maybe_text(wrapper.get("route_source"))
            or "missing-verification-route",
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "issue-cluster-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "issueclusterbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": [artifact_ref],
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": canonical_ids,
            "evidence_refs": [artifact_ref],
            "gap_hints": actionable_gaps[:3],
            "challenge_hints": [],
            "suggested_next_skills": [
                "eco-extract-stance-candidates",
                "eco-extract-concern-facets",
                "eco-extract-actor-profiles",
                "eco-extract-evidence-citation-types",
                "eco-materialize-controversy-map",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cluster claim-side controversy inputs into canonical issue-cluster objects."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-cluster-path", default="")
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--claim-verifiability-path", default="")
    parser.add_argument("--verification-route-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = cluster_issue_candidates_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_cluster_path=args.claim_cluster_path,
        claim_scope_path=args.claim_scope_path,
        claim_verifiability_path=args.claim_verifiability_path,
        verification_route_path=args.verification_route_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
