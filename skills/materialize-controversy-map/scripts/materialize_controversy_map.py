#!/usr/bin/env python3
"""Materialize one board-facing controversy map from typed issue surfaces."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "materialize-controversy-map"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.controversy_issue_surfaces import (  # noqa: E402
    actionable_gaps_for_issue_clusters,
    artifact_ref_for_path,
    build_controversy_issue_map_items,
    build_controversy_map_items_from_issue_clusters,
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
    load_actor_profile_context,
    load_concern_facet_context,
    load_evidence_citation_type_context,
    load_issue_cluster_context,
    load_stance_group_context,
    sync_controversy_map_result_set,
    sync_issue_cluster_result_set,
)
from eco_council_runtime.typed_issue_skill_runner import (  # noqa: E402
    materialize_typed_issue_surface_skill,
)


def pretty_json(data: object, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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


def _load_typed_contexts(
    run_dir_path: Path,
    *,
    run_id: str,
    round_id: str,
    issue_clusters_path: str,
    stance_groups_path: str,
    concern_facets_path: str,
    actor_profiles_path: str,
    evidence_citation_types_path: str,
    db_path: str,
) -> dict[str, dict[str, Any]]:
    issue_context = load_issue_cluster_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        issue_clusters_path=issue_clusters_path,
        db_path=db_path,
    )
    resolved_db_path = maybe_text(issue_context.get("db_path")) or db_path
    stance_context = load_stance_group_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        stance_groups_path=stance_groups_path,
        db_path=resolved_db_path,
    )
    resolved_db_path = maybe_text(stance_context.get("db_path")) or resolved_db_path
    concern_context = load_concern_facet_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        concern_facets_path=concern_facets_path,
        db_path=resolved_db_path,
    )
    resolved_db_path = maybe_text(concern_context.get("db_path")) or resolved_db_path
    actor_context = load_actor_profile_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        actor_profiles_path=actor_profiles_path,
        db_path=resolved_db_path,
    )
    resolved_db_path = maybe_text(actor_context.get("db_path")) or resolved_db_path
    citation_context = load_evidence_citation_type_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        evidence_citation_types_path=evidence_citation_types_path,
        db_path=resolved_db_path,
    )
    return {
        "issue-cluster": issue_context,
        "stance-group": stance_context,
        "concern-facet": concern_context,
        "actor-profile": actor_context,
        "evidence-citation-type": citation_context,
    }


def _inline_issue_cluster_surface(
    run_dir_path: Path,
    *,
    run_id: str,
    round_id: str,
    output_file: Path,
    claim_cluster_path: str,
    claim_scope_path: str,
    claim_verifiability_path: str,
    verification_route_path: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], str]:
    contexts = load_claim_chain_contexts(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_cluster_path=claim_cluster_path,
        claim_scope_path=claim_scope_path,
        claim_verifiability_path=claim_verifiability_path,
        verification_route_path=verification_route_path,
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
    canonical_issue_clusters = typed_surfaces["issue-cluster"]
    query_basis = claim_chain_query_basis(
        contexts,
        selection_mode="cluster-issue-candidates-from-claim-chain",
        method="controversy-issue-clustering-v1",
    )
    decision_source = "heuristic-fallback"
    provenance = {
        "source_skill": SKILL_NAME,
        "decision_source": decision_source,
        "selection_mode": "cluster-issue-candidates-from-claim-chain",
        "method": "controversy-issue-clustering-v1",
        "artifact_path": str(output_file),
        "cluster_source": maybe_text(query_basis.get("cluster_source")),
        "claim_scope_source": maybe_text(query_basis.get("claim_scope_source")),
        "verifiability_source": maybe_text(query_basis.get("verifiability_source")),
        "route_source": maybe_text(query_basis.get("route_source")),
    }
    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "decision_source": decision_source,
        "provenance": provenance,
        "query_basis": query_basis,
        **query_basis,
        "observed_inputs": {
            "claim_clusters_present": bool(claim_clusters),
            "claim_scope_present": bool(claim_scopes),
            "claim_verifiability_present": bool(
                contexts["claim_verifiability"].get("assessments")
            ),
            "verification_route_present": bool(
                contexts["verification_route"].get("routes")
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
        "issue_cluster_count": len(canonical_issue_clusters),
        "issue_label_counts": issue_label_counts(canonical_issue_clusters),
        "issue_clusters": canonical_issue_clusters,
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
    warnings = [
        *(
            contexts.get("warnings", [])
            if isinstance(contexts.get("warnings"), list)
            else []
        ),
        {
            "code": "issue-clusters-materialized-inline",
            "message": (
                "Canonical issue-cluster rows were missing, so "
                "`materialize-controversy-map` regenerated them inline."
            ),
        },
    ]
    return analysis_sync, canonical_issue_clusters, issue_map_items, warnings


def materialize_controversy_map_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_scope_path: str,
    claim_verifiability_path: str,
    verification_route_path: str,
    issue_clusters_path: str,
    stance_groups_path: str,
    concern_facets_path: str,
    actor_profiles_path: str,
    evidence_citation_types_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"analytics/controversy_map_{round_id}.json",
    )
    surface_paths = typed_surface_output_paths(
        run_dir_path,
        round_id=round_id,
        issue_clusters_path=issue_clusters_path,
        stance_groups_path=stance_groups_path,
        concern_facets_path=concern_facets_path,
        actor_profiles_path=actor_profiles_path,
        evidence_citation_types_path=evidence_citation_types_path,
    )
    typed_contexts = _load_typed_contexts(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        issue_clusters_path=str(surface_paths["issue-cluster"]),
        stance_groups_path=str(surface_paths["stance-group"]),
        concern_facets_path=str(surface_paths["concern-facet"]),
        actor_profiles_path=str(surface_paths["actor-profile"]),
        evidence_citation_types_path=str(surface_paths["evidence-citation-type"]),
        db_path="",
    )
    warnings: list[dict[str, Any]] = []
    for context in typed_contexts.values():
        warnings.extend(
            context.get("warnings", [])
            if isinstance(context.get("warnings"), list)
            else []
        )
    inline_issue_cluster_sync: dict[str, Any] = {}
    if not typed_contexts["issue-cluster"].get("issue_clusters"):
        inline_issue_cluster_sync, _, _, inline_warnings = _inline_issue_cluster_surface(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            output_file=surface_paths["issue-cluster"],
            claim_cluster_path=claim_cluster_path,
            claim_scope_path=claim_scope_path,
            claim_verifiability_path=claim_verifiability_path,
            verification_route_path=verification_route_path,
        )
        warnings.extend(inline_warnings)
        typed_contexts = _load_typed_contexts(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            issue_clusters_path=str(surface_paths["issue-cluster"]),
            stance_groups_path=str(surface_paths["stance-group"]),
            concern_facets_path=str(surface_paths["concern-facet"]),
            actor_profiles_path=str(surface_paths["actor-profile"]),
            evidence_citation_types_path=str(surface_paths["evidence-citation-type"]),
            db_path=maybe_text(inline_issue_cluster_sync.get("db_path")),
        )

    issue_clusters = (
        typed_contexts["issue-cluster"].get("issue_clusters", [])
        if isinstance(typed_contexts["issue-cluster"].get("issue_clusters"), list)
        else []
    )
    inline_typed_syncs: dict[str, dict[str, Any]] = {}
    missing_surface_specs = (
        ("stance-group", "stance_groups"),
        ("concern-facet", "concern_facets"),
        ("actor-profile", "actor_profiles"),
        ("evidence-citation-type", "citation_types"),
    )
    for kind, field_name in missing_surface_specs:
        context = typed_contexts[kind]
        if context.get(field_name):
            continue
        inline_payload = materialize_typed_issue_surface_skill(
            kind=kind,
            skill_name=SKILL_NAME,
            run_dir=str(run_dir_path),
            run_id=run_id,
            round_id=round_id,
            issue_clusters_path=str(surface_paths["issue-cluster"]),
            output_path=str(surface_paths[kind]),
        )
        inline_typed_syncs[kind] = (
            inline_payload.get("analysis_sync", {})
            if isinstance(inline_payload.get("analysis_sync"), dict)
            else {}
        )
        warnings.extend(
            inline_payload.get("warnings", [])
            if isinstance(inline_payload.get("warnings"), list)
            else []
        )
    if inline_typed_syncs:
        typed_contexts = _load_typed_contexts(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            issue_clusters_path=str(surface_paths["issue-cluster"]),
            stance_groups_path=str(surface_paths["stance-group"]),
            concern_facets_path=str(surface_paths["concern-facet"]),
            actor_profiles_path=str(surface_paths["actor-profile"]),
            evidence_citation_types_path=str(surface_paths["evidence-citation-type"]),
            db_path=maybe_text(
                next(
                    (
                        payload.get("db_path")
                        for payload in inline_typed_syncs.values()
                        if isinstance(payload, dict) and maybe_text(payload.get("db_path"))
                    ),
                    maybe_text(typed_contexts["issue-cluster"].get("db_path")),
                )
            ),
        )

    map_items = build_controversy_map_items_from_issue_clusters(
        issue_clusters,
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        source_skill=SKILL_NAME,
    )
    (
        actionable_gaps,
        lane_counts,
        route_status_counts,
        controversy_posture_counts,
        concern_counts,
        actor_counts,
    ) = actionable_gaps_for_issue_clusters(map_items)
    generated_at_utc = utc_now_iso()
    query_basis = {
        "issue_clusters_path": maybe_text(
            typed_contexts["issue-cluster"].get("issue_clusters_file")
        )
        or str(surface_paths["issue-cluster"]),
        "stance_groups_path": maybe_text(
            typed_contexts["stance-group"].get("stance_groups_file")
        )
        or str(surface_paths["stance-group"]),
        "concern_facets_path": maybe_text(
            typed_contexts["concern-facet"].get("concern_facets_file")
        )
        or str(surface_paths["concern-facet"]),
        "actor_profiles_path": maybe_text(
            typed_contexts["actor-profile"].get("actor_profiles_file")
        )
        or str(surface_paths["actor-profile"]),
        "evidence_citation_types_path": maybe_text(
            typed_contexts["evidence-citation-type"].get(
                "evidence_citation_types_file"
            )
        )
        or str(surface_paths["evidence-citation-type"]),
        "issue_clusters_source": maybe_text(
            typed_contexts["issue-cluster"].get("issue_cluster_source")
        )
        or "missing-issue-cluster",
        "stance_groups_source": maybe_text(
            typed_contexts["stance-group"].get("stance_group_source")
        )
        or "missing-stance-group",
        "concern_facets_source": maybe_text(
            typed_contexts["concern-facet"].get("concern_facet_source")
        )
        or "missing-concern-facet",
        "actor_profiles_source": maybe_text(
            typed_contexts["actor-profile"].get("actor_profile_source")
        )
        or "missing-actor-profile",
        "evidence_citation_types_source": maybe_text(
            typed_contexts["evidence-citation-type"].get(
                "evidence_citation_type_source"
            )
        )
        or "missing-evidence-citation-type",
        "selection_mode": "typed-issue-surface-aggregation",
        "method": "controversy-map-aggregation-v3",
    }
    decision_source = "heuristic-fallback"
    provenance = {
        "source_skill": SKILL_NAME,
        "decision_source": decision_source,
        "selection_mode": "typed-issue-surface-aggregation",
        "method": "controversy-map-aggregation-v3",
        "artifact_path": str(output_file),
        "issue_clusters_source": maybe_text(query_basis.get("issue_clusters_source")),
        "stance_groups_source": maybe_text(query_basis.get("stance_groups_source")),
        "concern_facets_source": maybe_text(query_basis.get("concern_facets_source")),
        "actor_profiles_source": maybe_text(query_basis.get("actor_profiles_source")),
        "evidence_citation_types_source": maybe_text(
            query_basis.get("evidence_citation_types_source")
        ),
    }
    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "decision_source": decision_source,
        "provenance": provenance,
        "query_basis": query_basis,
        **query_basis,
        "observed_inputs": {
            "issue_clusters_present": bool(issue_clusters),
            "issue_clusters_artifact_present": bool(
                typed_contexts["issue-cluster"].get("issue_clusters_artifact_present")
            ),
            "stance_groups_present": bool(
                typed_contexts["stance-group"].get("stance_groups")
            ),
            "stance_groups_artifact_present": bool(
                typed_contexts["stance-group"].get("stance_groups_artifact_present")
            ),
            "concern_facets_present": bool(
                typed_contexts["concern-facet"].get("concern_facets")
            ),
            "concern_facets_artifact_present": bool(
                typed_contexts["concern-facet"].get("concern_facets_artifact_present")
            ),
            "actor_profiles_present": bool(
                typed_contexts["actor-profile"].get("actor_profiles")
            ),
            "actor_profiles_artifact_present": bool(
                typed_contexts["actor-profile"].get("actor_profiles_artifact_present")
            ),
            "evidence_citation_types_present": bool(
                typed_contexts["evidence-citation-type"].get("citation_types")
            ),
            "evidence_citation_types_artifact_present": bool(
                typed_contexts["evidence-citation-type"].get(
                    "evidence_citation_types_artifact_present"
                )
            ),
        },
        "input_analysis_sync": {
            "issue_clusters": inline_issue_cluster_sync
            or typed_contexts["issue-cluster"].get("analysis_sync", {}),
            "stance_groups": inline_typed_syncs.get("stance-group", {})
            or typed_contexts["stance-group"].get("analysis_sync", {}),
            "concern_facets": inline_typed_syncs.get("concern-facet", {})
            or typed_contexts["concern-facet"].get("analysis_sync", {}),
            "actor_profiles": inline_typed_syncs.get("actor-profile", {})
            or typed_contexts["actor-profile"].get("analysis_sync", {}),
            "evidence_citation_types": inline_typed_syncs.get(
                "evidence-citation-type", {}
            )
            or typed_contexts["evidence-citation-type"].get("analysis_sync", {}),
        },
        "issue_cluster_count": len(map_items),
        "lane_counts": lane_counts,
        "route_status_counts": route_status_counts,
        "controversy_posture_counts": controversy_posture_counts,
        "concern_counts": concern_counts,
        "actor_counts": actor_counts,
        "actionable_gaps": actionable_gaps,
        "typed_output_paths": {
            "issue_clusters_path": query_basis["issue_clusters_path"],
            "stance_groups_path": query_basis["stance_groups_path"],
            "concern_facets_path": query_basis["concern_facets_path"],
            "actor_profiles_path": query_basis["actor_profiles_path"],
            "evidence_citation_types_path": query_basis[
                "evidence_citation_types_path"
            ],
        },
        "typed_counts": {
            "issue_cluster_object_count": len(issue_clusters),
            "stance_group_count": int(
                typed_contexts["stance-group"].get("stance_group_count") or 0
            ),
            "concern_facet_count": int(
                typed_contexts["concern-facet"].get("concern_facet_count") or 0
            ),
            "actor_profile_count": int(
                typed_contexts["actor-profile"].get("actor_profile_count") or 0
            ),
            "citation_type_count": int(
                typed_contexts["evidence-citation-type"].get("citation_type_count")
                or 0
            ),
        },
        "issue_clusters": map_items,
    }
    analysis_db_path = maybe_text(typed_contexts["issue-cluster"].get("db_path"))
    analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        wrapper=wrapper,
        db_path=analysis_db_path,
        sync_callable=sync_controversy_map_result_set,
        path_kwarg="controversy_map_path",
    )
    typed_analysis_sync = {
        "issue-cluster": inline_issue_cluster_sync
        or typed_contexts["issue-cluster"].get("analysis_sync", {}),
        "stance-group": inline_typed_syncs.get("stance-group", {})
        or typed_contexts["stance-group"].get("analysis_sync", {}),
        "concern-facet": inline_typed_syncs.get("concern-facet", {})
        or typed_contexts["concern-facet"].get("analysis_sync", {}),
        "actor-profile": inline_typed_syncs.get("actor-profile", {})
        or typed_contexts["actor-profile"].get("analysis_sync", {}),
        "evidence-citation-type": inline_typed_syncs.get(
            "evidence-citation-type", {}
        )
        or typed_contexts["evidence-citation-type"].get("analysis_sync", {}),
    }
    artifact_refs = [
        artifact_ref_for_path(output_file, "$.issue_clusters"),
        artifact_ref_for_path(surface_paths["issue-cluster"], "$.issue_clusters"),
        artifact_ref_for_path(surface_paths["stance-group"], "$.stance_groups"),
        artifact_ref_for_path(surface_paths["concern-facet"], "$.concern_facets"),
        artifact_ref_for_path(surface_paths["actor-profile"], "$.actor_profiles"),
        artifact_ref_for_path(
            surface_paths["evidence-citation-type"], "$.citation_types"
        ),
    ]
    canonical_ids = [
        maybe_text(item.get("map_issue_id"))
        for item in map_items
        if isinstance(item, dict) and maybe_text(item.get("map_issue_id"))
    ]
    if not map_items:
        warnings.append(
            {
                "code": "no-controversy-map",
                "message": "No controversy-map issue clusters were produced from the available typed issue surfaces.",
            }
        )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "issue_cluster_count": len(map_items),
            "issue_clusters_source": query_basis["issue_clusters_source"],
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "controversy-map-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "controversymapbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "typed_analysis_sync": typed_analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "decision_source": decision_source,
        "provenance": provenance,
        "board_handoff": {
            "candidate_ids": canonical_ids,
            "evidence_refs": artifact_refs[:2],
            "gap_hints": actionable_gaps[:3],
            "challenge_hints": (
                [
                    "Review whether any issue still routed to observation matching is actually a procedural controversy."
                ]
                if int(controversy_posture_counts.get("empirical-issue", 0)) > 0
                and int(controversy_posture_counts.get("non-empirical-issue", 0))
                > 0
                else []
            ),
            "suggested_next_skills": [
                "propose-next-actions",
                "post-board-note",
                "open-falsification-probe",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize one board-facing controversy map from typed issue surfaces."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-cluster-path", default="")
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--claim-verifiability-path", default="")
    parser.add_argument("--verification-route-path", default="")
    parser.add_argument("--issue-clusters-path", default="")
    parser.add_argument("--stance-groups-path", default="")
    parser.add_argument("--concern-facets-path", default="")
    parser.add_argument("--actor-profiles-path", default="")
    parser.add_argument("--evidence-citation-types-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_controversy_map_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_cluster_path=args.claim_cluster_path,
        claim_scope_path=args.claim_scope_path,
        claim_verifiability_path=args.claim_verifiability_path,
        verification_route_path=args.verification_route_path,
        issue_clusters_path=args.issue_clusters_path,
        stance_groups_path=args.stance_groups_path,
        concern_facets_path=args.concern_facets_path,
        actor_profiles_path=args.actor_profiles_path,
        evidence_citation_types_path=args.evidence_citation_types_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
