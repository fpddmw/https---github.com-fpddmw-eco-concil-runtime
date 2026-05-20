from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.analysis_plane_contracts import (
    ANALYSIS_KIND_ACTOR_PROFILE,
    ANALYSIS_KIND_CLAIM_CANDIDATE,
    ANALYSIS_KIND_CLAIM_CLUSTER,
    ANALYSIS_KIND_CLAIM_OBSERVATION_LINK,
    ANALYSIS_KIND_CLAIM_SCOPE,
    ANALYSIS_KIND_CLAIM_VERIFIABILITY,
    ANALYSIS_KIND_CONCERN_FACET,
    ANALYSIS_KIND_CONTROVERSY_MAP,
    ANALYSIS_KIND_DIFFUSION_EDGE,
    ANALYSIS_KIND_EVIDENCE_CITATION_TYPE,
    ANALYSIS_KIND_EVIDENCE_COVERAGE,
    ANALYSIS_KIND_FACT_POLICY_PUBLIC_INTERACTION_NODE,
    ANALYSIS_KIND_FORMAL_PUBLIC_LINK,
    ANALYSIS_KIND_ISSUE_CLUSTER,
    ANALYSIS_KIND_MERGED_OBSERVATION,
    ANALYSIS_KIND_OBSERVATION_CANDIDATE,
    ANALYSIS_KIND_OBSERVATION_SCOPE,
    ANALYSIS_KIND_REPRESENTATION_GAP,
    ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE,
    ANALYSIS_KIND_STANCE_GROUP,
    ANALYSIS_KIND_VERIFICATION_ROUTE,
    maybe_text,
)
from eco_council_runtime.kernel.planes.analysis_plane_support import empty_result_contract
from eco_council_runtime.kernel.planes.analysis_plane_results import load_analysis_result_context, sync_analysis_result_set

def sync_evidence_coverage_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    coverage_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_EVIDENCE_COVERAGE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=coverage_path,
        db_path=db_path,
    )
    return {
        **result,
        "coverage_path": maybe_text(result.get("artifact_path")),
    }

def load_evidence_coverage_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    coverage_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_EVIDENCE_COVERAGE,
        artifact_path=coverage_path,
        db_path=db_path,
    )
    return {
        "coverage_wrapper": context.get("payload_wrapper", {}),
        "coverages": context.get("items", []),
        "coverage_count": int(context.get("item_count") or 0),
        "coverage_source": maybe_text(context.get("source")),
        "coverage_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "coverage_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_diffusion_edge_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    diffusion_edges_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_DIFFUSION_EDGE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=diffusion_edges_path,
        db_path=db_path,
    )
    return {
        **result,
        "diffusion_edges_path": maybe_text(result.get("artifact_path")),
    }

def load_diffusion_edge_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    diffusion_edges_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_DIFFUSION_EDGE,
        artifact_path=diffusion_edges_path,
        db_path=db_path,
    )
    return {
        "diffusion_edges_wrapper": context.get("payload_wrapper", {}),
        "edges": context.get("items", []),
        "edge_count": int(context.get("item_count") or 0),
        "diffusion_edge_source": maybe_text(context.get("source")),
        "diffusion_edges_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "diffusion_edges_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_spatiotemporal_relation_cue_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    relation_cues_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=relation_cues_path,
        db_path=db_path,
        replace_scope="artifact",
    )
    return {
        **result,
        "relation_cues_path": maybe_text(result.get("artifact_path")),
    }

def load_spatiotemporal_relation_cue_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    relation_cues_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE,
        artifact_path=relation_cues_path,
        db_path=db_path,
    )
    return {
        "relation_cues_wrapper": context.get("payload_wrapper", {}),
        "spatiotemporal_relation_cues": context.get("items", []),
        "relation_cue_count": int(context.get("item_count") or 0),
        "relation_cue_source": maybe_text(context.get("source")),
        "relation_cues_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "relation_cues_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_fact_policy_public_interaction_node_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    interaction_timeline_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_FACT_POLICY_PUBLIC_INTERACTION_NODE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=interaction_timeline_path,
        db_path=db_path,
        replace_scope="artifact",
    )
    return {
        **result,
        "interaction_timeline_path": maybe_text(result.get("artifact_path")),
    }

def load_fact_policy_public_interaction_node_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    interaction_timeline_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_FACT_POLICY_PUBLIC_INTERACTION_NODE,
        artifact_path=interaction_timeline_path,
        db_path=db_path,
    )
    return {
        "interaction_timeline_wrapper": context.get("payload_wrapper", {}),
        "interaction_nodes": context.get("items", []),
        "interaction_node_count": int(context.get("item_count") or 0),
        "interaction_timeline_source": maybe_text(context.get("source")),
        "interaction_timeline_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "interaction_timeline_artifact_present": bool(
            context.get("artifact_present")
        ),
        "warnings": context.get("warnings", []),
    }

def sync_formal_public_link_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    formal_public_links_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_FORMAL_PUBLIC_LINK,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=formal_public_links_path,
        db_path=db_path,
    )
    return {
        **result,
        "formal_public_links_path": maybe_text(result.get("artifact_path")),
    }

def load_formal_public_link_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    formal_public_links_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_FORMAL_PUBLIC_LINK,
        artifact_path=formal_public_links_path,
        db_path=db_path,
    )
    return {
        "formal_public_links_wrapper": context.get("payload_wrapper", {}),
        "links": context.get("items", []),
        "link_count": int(context.get("item_count") or 0),
        "formal_public_link_source": maybe_text(context.get("source")),
        "formal_public_links_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "formal_public_links_artifact_present": bool(
            context.get("artifact_present")
        ),
        "warnings": context.get("warnings", []),
    }

def sync_representation_gap_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    representation_gap_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_REPRESENTATION_GAP,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=representation_gap_path,
        db_path=db_path,
    )
    return {
        **result,
        "representation_gap_path": maybe_text(result.get("artifact_path")),
    }

def load_representation_gap_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    representation_gap_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_REPRESENTATION_GAP,
        artifact_path=representation_gap_path,
        db_path=db_path,
    )
    return {
        "representation_gap_wrapper": context.get("payload_wrapper", {}),
        "gaps": context.get("items", []),
        "gap_count": int(context.get("item_count") or 0),
        "representation_gap_source": maybe_text(context.get("source")),
        "representation_gap_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "representation_gap_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_controversy_map_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    controversy_map_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CONTROVERSY_MAP,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=controversy_map_path,
        db_path=db_path,
    )
    return {
        **result,
        "controversy_map_path": maybe_text(result.get("artifact_path")),
    }

def load_controversy_map_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    controversy_map_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CONTROVERSY_MAP,
        artifact_path=controversy_map_path,
        db_path=db_path,
    )
    return {
        "controversy_map_wrapper": context.get("payload_wrapper", {}),
        "issue_clusters": context.get("items", []),
        "issue_cluster_count": int(context.get("item_count") or 0),
        "controversy_map_source": maybe_text(context.get("source")),
        "controversy_map_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "controversy_map_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_issue_cluster_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    issue_clusters_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_ISSUE_CLUSTER,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=issue_clusters_path,
        db_path=db_path,
    )
    return {
        **result,
        "issue_clusters_path": maybe_text(result.get("artifact_path")),
    }

def load_issue_cluster_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    issue_clusters_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_ISSUE_CLUSTER,
        artifact_path=issue_clusters_path,
        db_path=db_path,
    )
    return {
        "issue_clusters_wrapper": context.get("payload_wrapper", {}),
        "issue_clusters": context.get("items", []),
        "issue_cluster_count": int(context.get("item_count") or 0),
        "issue_cluster_source": maybe_text(context.get("source")),
        "issue_clusters_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "issue_clusters_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_stance_group_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    stance_groups_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_STANCE_GROUP,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=stance_groups_path,
        db_path=db_path,
    )
    return {
        **result,
        "stance_groups_path": maybe_text(result.get("artifact_path")),
    }

def load_stance_group_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    stance_groups_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_STANCE_GROUP,
        artifact_path=stance_groups_path,
        db_path=db_path,
    )
    return {
        "stance_groups_wrapper": context.get("payload_wrapper", {}),
        "stance_groups": context.get("items", []),
        "stance_group_count": int(context.get("item_count") or 0),
        "stance_group_source": maybe_text(context.get("source")),
        "stance_groups_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "stance_groups_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_concern_facet_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    concern_facets_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CONCERN_FACET,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=concern_facets_path,
        db_path=db_path,
    )
    return {
        **result,
        "concern_facets_path": maybe_text(result.get("artifact_path")),
    }

def load_concern_facet_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    concern_facets_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CONCERN_FACET,
        artifact_path=concern_facets_path,
        db_path=db_path,
    )
    return {
        "concern_facets_wrapper": context.get("payload_wrapper", {}),
        "concern_facets": context.get("items", []),
        "concern_facet_count": int(context.get("item_count") or 0),
        "concern_facet_source": maybe_text(context.get("source")),
        "concern_facets_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "concern_facets_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_actor_profile_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    actor_profiles_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_ACTOR_PROFILE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=actor_profiles_path,
        db_path=db_path,
    )
    return {
        **result,
        "actor_profiles_path": maybe_text(result.get("artifact_path")),
    }

def load_actor_profile_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    actor_profiles_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_ACTOR_PROFILE,
        artifact_path=actor_profiles_path,
        db_path=db_path,
    )
    return {
        "actor_profiles_wrapper": context.get("payload_wrapper", {}),
        "actor_profiles": context.get("items", []),
        "actor_profile_count": int(context.get("item_count") or 0),
        "actor_profile_source": maybe_text(context.get("source")),
        "actor_profiles_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "actor_profiles_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_evidence_citation_type_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    evidence_citation_types_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_EVIDENCE_CITATION_TYPE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=evidence_citation_types_path,
        db_path=db_path,
    )
    return {
        **result,
        "evidence_citation_types_path": maybe_text(result.get("artifact_path")),
    }

def load_evidence_citation_type_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    evidence_citation_types_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_EVIDENCE_CITATION_TYPE,
        artifact_path=evidence_citation_types_path,
        db_path=db_path,
    )
    return {
        "evidence_citation_types_wrapper": context.get("payload_wrapper", {}),
        "citation_types": context.get("items", []),
        "citation_type_count": int(context.get("item_count") or 0),
        "evidence_citation_type_source": maybe_text(context.get("source")),
        "evidence_citation_types_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "evidence_citation_types_artifact_present": bool(
            context.get("artifact_present")
        ),
        "warnings": context.get("warnings", []),
    }

def sync_verification_route_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    verification_route_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_VERIFICATION_ROUTE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=verification_route_path,
        db_path=db_path,
    )
    return {
        **result,
        "verification_route_path": maybe_text(result.get("artifact_path")),
    }

def load_verification_route_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    verification_route_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_VERIFICATION_ROUTE,
        artifact_path=verification_route_path,
        db_path=db_path,
    )
    return {
        "verification_route_wrapper": context.get("payload_wrapper", {}),
        "routes": context.get("items", []),
        "route_count": int(context.get("item_count") or 0),
        "verification_route_source": maybe_text(context.get("source")),
        "verification_route_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "verification_route_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_claim_verifiability_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    claim_verifiability_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_VERIFIABILITY,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=claim_verifiability_path,
        db_path=db_path,
    )
    return {
        **result,
        "claim_verifiability_path": maybe_text(result.get("artifact_path")),
    }

def load_claim_verifiability_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    claim_verifiability_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_VERIFIABILITY,
        artifact_path=claim_verifiability_path,
        db_path=db_path,
    )
    return {
        "claim_verifiability_wrapper": context.get("payload_wrapper", {}),
        "assessments": context.get("items", []),
        "assessment_count": int(context.get("item_count") or 0),
        "claim_verifiability_source": maybe_text(context.get("source")),
        "claim_verifiability_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "claim_verifiability_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_claim_scope_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    claim_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_SCOPE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=claim_scope_path,
        db_path=db_path,
    )
    return {
        **result,
        "claim_scope_path": maybe_text(result.get("artifact_path")),
    }

def load_claim_scope_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    claim_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_SCOPE,
        artifact_path=claim_scope_path,
        db_path=db_path,
    )
    return {
        "claim_scope_wrapper": context.get("payload_wrapper", {}),
        "claim_scopes": context.get("items", []),
        "claim_scope_count": int(context.get("item_count") or 0),
        "claim_scope_source": maybe_text(context.get("source")),
        "claim_scope_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "claim_scope_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_observation_scope_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    observation_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_SCOPE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=observation_scope_path,
        db_path=db_path,
    )
    return {
        **result,
        "observation_scope_path": maybe_text(result.get("artifact_path")),
    }

def load_observation_scope_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    observation_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_SCOPE,
        artifact_path=observation_scope_path,
        db_path=db_path,
    )
    return {
        "observation_scope_wrapper": context.get("payload_wrapper", {}),
        "observation_scopes": context.get("items", []),
        "observation_scope_count": int(context.get("item_count") or 0),
        "observation_scope_source": maybe_text(context.get("source")),
        "observation_scope_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "observation_scope_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_claim_observation_link_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    links_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_OBSERVATION_LINK,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=links_path,
        db_path=db_path,
    )
    return {
        **result,
        "links_path": maybe_text(result.get("artifact_path")),
    }

def load_claim_observation_link_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    links_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_OBSERVATION_LINK,
        artifact_path=links_path,
        db_path=db_path,
    )
    return {
        "links_wrapper": context.get("payload_wrapper", {}),
        "links": context.get("items", []),
        "link_count": int(context.get("item_count") or 0),
        "links_source": maybe_text(context.get("source")),
        "links_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "links_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_claim_cluster_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    claim_cluster_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_CLUSTER,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=claim_cluster_path,
        db_path=db_path,
    )
    return {
        **result,
        "claim_cluster_path": maybe_text(result.get("artifact_path")),
    }

def load_claim_cluster_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    claim_cluster_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_CLUSTER,
        artifact_path=claim_cluster_path,
        db_path=db_path,
    )
    return {
        "claim_cluster_wrapper": context.get("payload_wrapper", {}),
        "claim_clusters": context.get("items", []),
        "claim_cluster_count": int(context.get("item_count") or 0),
        "claim_cluster_source": maybe_text(context.get("source")),
        "claim_cluster_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "claim_cluster_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_merged_observation_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    merged_observations_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_MERGED_OBSERVATION,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=merged_observations_path,
        db_path=db_path,
    )
    return {
        **result,
        "merged_observations_path": maybe_text(result.get("artifact_path")),
    }

def load_merged_observation_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    merged_observations_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_MERGED_OBSERVATION,
        artifact_path=merged_observations_path,
        db_path=db_path,
    )
    return {
        "merged_observations_wrapper": context.get("payload_wrapper", {}),
        "merged_observations": context.get("items", []),
        "merged_observation_count": int(context.get("item_count") or 0),
        "merged_observation_source": maybe_text(context.get("source")),
        "merged_observations_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "merged_observations_artifact_present": bool(
            context.get("artifact_present")
        ),
        "warnings": context.get("warnings", []),
    }

def sync_claim_candidate_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    claim_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_CANDIDATE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=claim_candidates_path,
        db_path=db_path,
    )
    return {
        **result,
        "claim_candidates_path": maybe_text(result.get("artifact_path")),
    }

def load_claim_candidate_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    claim_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_CANDIDATE,
        artifact_path=claim_candidates_path,
        db_path=db_path,
    )
    return {
        "claim_candidates_wrapper": context.get("payload_wrapper", {}),
        "claim_candidates": context.get("items", []),
        "claim_candidate_count": int(context.get("item_count") or 0),
        "claim_candidate_source": maybe_text(context.get("source")),
        "claim_candidates_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "claim_candidates_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }

def sync_observation_candidate_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    observation_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_CANDIDATE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=observation_candidates_path,
        db_path=db_path,
    )
    return {
        **result,
        "observation_candidates_path": maybe_text(result.get("artifact_path")),
    }

def load_observation_candidate_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    observation_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_CANDIDATE,
        artifact_path=observation_candidates_path,
        db_path=db_path,
    )
    return {
        "observation_candidates_wrapper": context.get("payload_wrapper", {}),
        "observation_candidates": context.get("items", []),
        "observation_candidate_count": int(context.get("item_count") or 0),
        "observation_candidate_source": maybe_text(context.get("source")),
        "observation_candidates_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "observation_candidates_artifact_present": bool(
            context.get("artifact_present")
        ),
        "warnings": context.get("warnings", []),
    }

__all__ = [
    "sync_evidence_coverage_result_set",
    "load_evidence_coverage_context",
    "sync_diffusion_edge_result_set",
    "load_diffusion_edge_context",
    "sync_spatiotemporal_relation_cue_result_set",
    "load_spatiotemporal_relation_cue_context",
    "sync_fact_policy_public_interaction_node_result_set",
    "load_fact_policy_public_interaction_node_context",
    "sync_formal_public_link_result_set",
    "load_formal_public_link_context",
    "sync_representation_gap_result_set",
    "load_representation_gap_context",
    "sync_controversy_map_result_set",
    "load_controversy_map_context",
    "sync_issue_cluster_result_set",
    "load_issue_cluster_context",
    "sync_stance_group_result_set",
    "load_stance_group_context",
    "sync_concern_facet_result_set",
    "load_concern_facet_context",
    "sync_actor_profile_result_set",
    "load_actor_profile_context",
    "sync_evidence_citation_type_result_set",
    "load_evidence_citation_type_context",
    "sync_verification_route_result_set",
    "load_verification_route_context",
    "sync_claim_verifiability_result_set",
    "load_claim_verifiability_context",
    "sync_claim_scope_result_set",
    "load_claim_scope_context",
    "sync_observation_scope_result_set",
    "load_observation_scope_context",
    "sync_claim_observation_link_result_set",
    "load_claim_observation_link_context",
    "sync_claim_cluster_result_set",
    "load_claim_cluster_context",
    "sync_merged_observation_result_set",
    "load_merged_observation_context",
    "sync_claim_candidate_result_set",
    "load_claim_candidate_context",
    "sync_observation_candidate_result_set",
    "load_observation_candidate_context",
]
