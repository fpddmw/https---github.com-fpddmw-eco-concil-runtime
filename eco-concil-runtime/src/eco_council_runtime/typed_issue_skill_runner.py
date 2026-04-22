from __future__ import annotations

from pathlib import Path
from typing import Any

from .controversy_issue_surfaces import (
    artifact_ref_for_path,
    build_typed_issue_surfaces,
    count_labels,
    maybe_text,
    resolve_run_dir,
    sync_result_wrapper,
    typed_surface_output_paths,
    utc_now_iso,
)
from .kernel.analysis_plane import (
    load_issue_cluster_context,
    sync_actor_profile_result_set,
    sync_concern_facet_result_set,
    sync_evidence_citation_type_result_set,
    sync_stance_group_result_set,
)


SURFACE_CONFIGS: dict[str, dict[str, Any]] = {
    "stance-group": {
        "wrapper_items_key": "stance_groups",
        "count_key": "stance_group_count",
        "counts_key": "stance_label_counts",
        "path_kwarg": "stance_groups_path",
        "sync_callable": sync_stance_group_result_set,
        "selection_mode": "derive-stance-groups-from-issue-cluster",
        "summary_field": "stance_label",
        "id_field": "stance_group_id",
    },
    "concern-facet": {
        "wrapper_items_key": "concern_facets",
        "count_key": "concern_facet_count",
        "counts_key": "concern_label_counts",
        "path_kwarg": "concern_facets_path",
        "sync_callable": sync_concern_facet_result_set,
        "selection_mode": "derive-concern-facets-from-issue-cluster",
        "summary_field": "concern_label",
        "id_field": "concern_id",
    },
    "actor-profile": {
        "wrapper_items_key": "actor_profiles",
        "count_key": "actor_profile_count",
        "counts_key": "actor_label_counts",
        "path_kwarg": "actor_profiles_path",
        "sync_callable": sync_actor_profile_result_set,
        "selection_mode": "derive-actor-profiles-from-issue-cluster",
        "summary_field": "actor_label",
        "fallback_summary_field": "display_name",
        "id_field": "actor_id",
    },
    "evidence-citation-type": {
        "wrapper_items_key": "citation_types",
        "count_key": "citation_type_count",
        "counts_key": "citation_type_counts",
        "path_kwarg": "evidence_citation_types_path",
        "sync_callable": sync_evidence_citation_type_result_set,
        "selection_mode": "derive-citation-types-from-issue-cluster",
        "summary_field": "citation_type",
        "id_field": "citation_type_id",
    },
}


def label_counts(kind: str, items: list[dict[str, Any]]) -> dict[str, int]:
    config = SURFACE_CONFIGS[kind]
    summary_field = maybe_text(config.get("summary_field"))
    fallback_summary_field = maybe_text(config.get("fallback_summary_field"))
    return count_labels(
        [
            maybe_text(item.get(summary_field))
            or maybe_text(item.get(fallback_summary_field))
            for item in items
            if isinstance(item, dict)
        ]
    )


def materialize_typed_issue_surface_skill(
    *,
    kind: str,
    skill_name: str,
    run_dir: str,
    run_id: str,
    round_id: str,
    issue_clusters_path: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    if kind not in SURFACE_CONFIGS:
        raise ValueError(f"Unsupported typed issue surface kind: {kind}")
    config = SURFACE_CONFIGS[kind]
    run_dir_path = resolve_run_dir(run_dir)
    issue_context = load_issue_cluster_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        issue_clusters_path=issue_clusters_path,
    )
    warnings = (
        issue_context.get("warnings", [])
        if isinstance(issue_context.get("warnings"), list)
        else []
    )
    surface_paths = typed_surface_output_paths(
        run_dir_path,
        round_id=round_id,
        issue_clusters_path=maybe_text(issue_context.get("issue_clusters_file")),
    )
    if output_path:
        surface_paths[kind] = Path(output_path).expanduser()
        if not surface_paths[kind].is_absolute():
            surface_paths[kind] = (run_dir_path / surface_paths[kind]).resolve()
        else:
            surface_paths[kind] = surface_paths[kind].resolve()
    issue_clusters = (
        issue_context.get("issue_clusters", [])
        if isinstance(issue_context.get("issue_clusters"), list)
        else []
    )
    typed_surfaces = build_typed_issue_surfaces(
        issue_clusters,
        run_id=run_id,
        round_id=round_id,
        paths=surface_paths,
        source_skill=skill_name,
    )
    items = typed_surfaces[kind]
    output_file = surface_paths[kind]
    generated_at_utc = utc_now_iso()
    wrapper = {
        "schema_version": "n3.0",
        "skill": skill_name,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "issue_clusters_path": maybe_text(issue_context.get("issue_clusters_file"))
            or str(surface_paths["issue-cluster"]),
            "issue_clusters_source": maybe_text(
                issue_context.get("issue_cluster_source")
            )
            or "missing-issue-cluster",
            "selection_mode": maybe_text(config.get("selection_mode")),
            "method": "controversy-typed-decomposition-v1",
        },
        "issue_clusters_path": maybe_text(issue_context.get("issue_clusters_file"))
        or str(surface_paths["issue-cluster"]),
        "issue_clusters_source": maybe_text(issue_context.get("issue_cluster_source"))
        or "missing-issue-cluster",
        config["counts_key"]: label_counts(kind, items),
        config["count_key"]: len(items),
        config["wrapper_items_key"]: items,
    }
    analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        wrapper=wrapper,
        db_path=maybe_text(issue_context.get("db_path")),
        sync_callable=config["sync_callable"],
        path_kwarg=maybe_text(config.get("path_kwarg")),
    )
    evidence_ref = artifact_ref_for_path(
        output_file, f"$.{maybe_text(config.get('wrapper_items_key'))}"
    )
    canonical_ids = [
        maybe_text(item.get(maybe_text(config.get("id_field"))))
        for item in items
        if isinstance(item, dict)
        and maybe_text(item.get(maybe_text(config.get("id_field"))))
    ]
    if not items:
        warnings = [
            *warnings,
            {
                "code": "no-typed-issue-items",
                "message": (
                    f"No {kind} objects were produced because canonical issue-cluster "
                    "inputs were missing or empty."
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
            config["count_key"]: len(items),
            "issue_cluster_source": maybe_text(issue_context.get("issue_cluster_source"))
            or "missing-issue-cluster",
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": f"{kind}-receipt-{run_id}-{round_id}",
        "batch_id": f"{kind}-batch-{run_id}-{round_id}",
        "artifact_refs": [evidence_ref],
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": {
            "issue_clusters": issue_context.get("analysis_sync", {})
        },
        "board_handoff": {
            "candidate_ids": canonical_ids,
            "evidence_refs": [evidence_ref],
            "gap_hints": [],
            "challenge_hints": [],
            "suggested_next_skills": [
                "eco-materialize-controversy-map",
                "eco-propose-next-actions",
                "eco-post-board-note",
            ],
        },
    }


__all__ = ["materialize_typed_issue_surface_skill"]
