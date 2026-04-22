from __future__ import annotations

from typing import Any

from .controversy_issue_surfaces import (
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
from .kernel.analysis_plane import sync_issue_cluster_result_set


def issue_label_counts(issue_clusters: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in issue_clusters:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("issue_label"))
        if not label:
            continue
        counts[label] = counts.get(label, 0) + 1
    return counts


def materialize_issue_cluster_skill(
    *,
    skill_name: str,
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str = "",
    claim_scope_path: str = "",
    claim_verifiability_path: str = "",
    verification_route_path: str = "",
    output_path: str = "",
    default_output_relative: str = "",
    selection_mode: str,
    method: str,
    use_claim_clusters: bool,
    suggested_next_skills: list[str] | None = None,
) -> dict[str, object]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        default_output_relative or f"analytics/issue_clusters_{round_id}.json",
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
    observed_claim_clusters = (
        contexts["claim_cluster"].get("claim_clusters", [])
        if isinstance(contexts["claim_cluster"].get("claim_clusters"), list)
        else []
    )
    if use_claim_clusters and observed_claim_clusters:
        claim_clusters = observed_claim_clusters
        issue_derivation_mode = "claim-cluster-merged"
    else:
        claim_clusters = fallback_clusters_from_scopes(claim_scopes)
        issue_derivation_mode = "claim-scope-derived-candidates"
    issue_map_items = build_controversy_issue_map_items(
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        source_skill=skill_name,
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
        source_skill=skill_name,
    )
    issue_clusters = typed_surfaces["issue-cluster"]
    generated_at_utc = utc_now_iso()
    actionable_gaps, lane_counts, route_status_counts, controversy_posture_counts, _, _ = (
        actionable_gaps_for_issue_clusters(issue_map_items)
    )
    query_basis = claim_chain_query_basis(
        contexts,
        selection_mode=selection_mode,
        method=method,
    )
    decision_source = "heuristic-fallback"
    provenance = {
        "source_skill": skill_name,
        "decision_source": decision_source,
        "selection_mode": selection_mode,
        "method": method,
        "issue_derivation_mode": issue_derivation_mode,
        "artifact_path": str(output_file),
        "cluster_source": maybe_text(query_basis.get("cluster_source")),
        "claim_scope_source": maybe_text(query_basis.get("claim_scope_source")),
        "verifiability_source": maybe_text(query_basis.get("verifiability_source")),
        "route_source": maybe_text(query_basis.get("route_source")),
    }
    wrapper = {
        "schema_version": "n3.0",
        "skill": skill_name,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "decision_source": decision_source,
        "provenance": provenance,
        "query_basis": query_basis,
        **query_basis,
        "issue_derivation_mode": issue_derivation_mode,
        "observed_inputs": {
            "claim_clusters_present": bool(observed_claim_clusters),
            "claim_clusters_artifact_present": bool(
                contexts["claim_cluster"].get("claim_cluster_artifact_present")
            ),
            "claim_scope_present": bool(contexts["claim_scope"].get("claim_scopes")),
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
            "issue_derivation_mode": issue_derivation_mode,
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
                "message": (
                    "No canonical issue-cluster objects were produced from the "
                    "available claim-side inputs."
                ),
            },
        ]
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "issue_cluster_count": len(issue_clusters),
            "issue_derivation_mode": issue_derivation_mode,
            "cluster_source": maybe_text(wrapper.get("cluster_source"))
            or "missing-claim-cluster",
            "route_source": maybe_text(wrapper.get("route_source"))
            or "missing-verification-route",
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "issue-cluster-receipt-"
        + stable_hash(skill_name, run_id, round_id, str(output_file))[:20],
        "batch_id": "issueclusterbatch-"
        + stable_hash(skill_name, run_id, round_id, output_file.name)[:16],
        "artifact_refs": [artifact_ref],
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "decision_source": decision_source,
        "provenance": provenance,
        "board_handoff": {
            "candidate_ids": canonical_ids,
            "evidence_refs": [artifact_ref],
            "gap_hints": actionable_gaps[:3],
            "challenge_hints": [],
            "suggested_next_skills": list(suggested_next_skills or []),
        },
    }


__all__ = ["materialize_issue_cluster_skill"]
