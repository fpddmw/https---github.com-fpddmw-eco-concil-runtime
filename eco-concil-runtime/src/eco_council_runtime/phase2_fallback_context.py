from __future__ import annotations

from pathlib import Path
from typing import Any

from .kernel.analysis_plane import (
    load_claim_verifiability_context,
    load_controversy_map_context,
    load_diffusion_edge_context,
    load_evidence_coverage_context,
    load_formal_public_link_context,
    load_representation_gap_context,
    load_verification_route_context,
)
from .kernel.deliberation_plane import load_round_snapshot
from .phase2_fallback_agenda import (
    board_snapshot,
    build_actions,
    controversy_context_counts,
)
from .phase2_fallback_common import (
    load_json_if_exists,
    load_text_if_exists,
    maybe_text,
    optional_context_count,
    optional_context_present,
    optional_context_warnings,
    resolve_path,
)
from .phase2_fallback_contracts import d1_contract_fields


def primary_analysis_sync(
    contexts: dict[str, tuple[dict[str, Any], str]],
    *,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_name = ""
    selected_sync = fallback if isinstance(fallback, dict) else {}
    available_analysis_kinds: list[str] = []
    context_item_counts: dict[str, int] = {}
    for name, (context, count_key) in contexts.items():
        count = optional_context_count(context, count_key)
        context_item_counts[name] = count
        if optional_context_present(context, count_key):
            available_analysis_kinds.append(name)
            if not selected_name and isinstance(context.get("analysis_sync"), dict):
                selected_name = name
                selected_sync = context.get("analysis_sync", {})
    if not isinstance(selected_sync, dict):
        selected_sync = {}
    return {
        **selected_sync,
        "selected_analysis_kind": selected_name,
        "available_analysis_kinds": available_analysis_kinds,
        "context_item_counts": context_item_counts,
    }


def load_d1_shared_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    board_summary_path: str = "",
    board_brief_path: str = "",
    coverage_path: str = "",
    include_board_notes: bool = False,
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    board_summary_file = resolve_path(
        run_dir_path,
        board_summary_path,
        f"board/board_state_summary_{round_id}.json",
    )
    board_brief_file = resolve_path(
        run_dir_path,
        board_brief_path,
        f"board/board_brief_{round_id}.md",
    )

    warnings: list[dict[str, Any]] = []
    round_snapshot = load_round_snapshot(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        include_closed=True,
    )
    board_summary = load_json_if_exists(board_summary_file)
    round_state = (
        round_snapshot.get("round_state")
        if maybe_text(round_snapshot.get("status")) == "completed"
        and isinstance(round_snapshot.get("round_state"), dict)
        else None
    )
    if round_state is None and not isinstance(board_summary, dict):
        warnings.append(
            {
                "code": "missing-board-state",
                "message": f"No board state was found for round {round_id}.",
            }
        )
    deliberation_sync = (
        round_snapshot.get("deliberation_sync")
        if isinstance(round_snapshot.get("deliberation_sync"), dict)
        else {}
    )
    db_path = maybe_text(round_snapshot.get("db_path"))
    coverage_context = load_evidence_coverage_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        coverage_path=coverage_path,
        db_path=db_path,
    )
    coverage_warnings = (
        coverage_context.get("warnings", [])
        if isinstance(coverage_context.get("warnings"), list)
        else []
    )
    warnings.extend(coverage_warnings)
    coverages = (
        coverage_context.get("coverages", [])
        if isinstance(coverage_context.get("coverages"), list)
        else []
    )
    coverage_file = maybe_text(coverage_context.get("coverage_file"))
    coverage_source = maybe_text(coverage_context.get("coverage_source"))
    if not db_path:
        db_path = maybe_text(coverage_context.get("db_path"))
    controversy_map_context = load_controversy_map_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    if not db_path:
        db_path = maybe_text(controversy_map_context.get("db_path"))
    verification_route_context = load_verification_route_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    claim_verifiability_context = load_claim_verifiability_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    formal_public_link_context = load_formal_public_link_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    representation_gap_context = load_representation_gap_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    diffusion_edge_context = load_diffusion_edge_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    warnings.extend(optional_context_warnings(controversy_map_context))
    warnings.extend(optional_context_warnings(verification_route_context))
    warnings.extend(optional_context_warnings(claim_verifiability_context))
    warnings.extend(optional_context_warnings(formal_public_link_context))
    warnings.extend(optional_context_warnings(representation_gap_context))
    warnings.extend(optional_context_warnings(diffusion_edge_context))
    analysis_sync = primary_analysis_sync(
        {
            "controversy-map": (controversy_map_context, "issue_cluster_count"),
            "verification-route": (verification_route_context, "route_count"),
            "claim-verifiability": (claim_verifiability_context, "assessment_count"),
            "formal-public-link": (formal_public_link_context, "link_count"),
            "representation-gap": (representation_gap_context, "gap_count"),
            "diffusion-edge": (diffusion_edge_context, "edge_count"),
            "evidence-coverage": (coverage_context, "coverage_count"),
        },
        fallback=coverage_context.get("analysis_sync") if isinstance(coverage_context.get("analysis_sync"), dict) else {},
    )
    brief_text = load_text_if_exists(board_brief_file)
    current_board_state = board_snapshot(
        round_state,
        board_summary if isinstance(board_summary, dict) else None,
        include_notes=include_board_notes,
    )
    issue_clusters = (
        controversy_map_context.get("issue_clusters", [])
        if isinstance(controversy_map_context.get("issue_clusters"), list)
        else []
    )
    routes = (
        verification_route_context.get("routes", [])
        if isinstance(verification_route_context.get("routes"), list)
        else []
    )
    assessments = (
        claim_verifiability_context.get("assessments", [])
        if isinstance(claim_verifiability_context.get("assessments"), list)
        else []
    )
    links = (
        formal_public_link_context.get("links", [])
        if isinstance(formal_public_link_context.get("links"), list)
        else []
    )
    gaps = (
        representation_gap_context.get("gaps", [])
        if isinstance(representation_gap_context.get("gaps"), list)
        else []
    )
    edges = (
        diffusion_edge_context.get("edges", [])
        if isinstance(diffusion_edge_context.get("edges"), list)
        else []
    )
    agenda_counts = controversy_context_counts(
        issue_clusters=issue_clusters,
        routes=routes,
        links=links,
        gaps=gaps,
        edges=edges,
        coverages=coverages,
    )
    contract_fields = d1_contract_fields(
        board_state_source=maybe_text(current_board_state.get("state_source"))
        or "missing-board",
        coverage_source=coverage_source or "missing-coverage",
        db_path=db_path,
        deliberation_sync=deliberation_sync,
        analysis_sync=analysis_sync,
        observed_inputs={
            "board_summary_artifact_present": board_summary_file.exists(),
            "board_summary_present": isinstance(board_summary, dict),
            "board_brief_artifact_present": board_brief_file.exists(),
            "board_brief_present": bool(maybe_text(brief_text)),
            "coverage_present": bool(coverages),
            "coverage_artifact_present": bool(
                coverage_context.get("coverage_artifact_present")
            ),
            "controversy_map_present": optional_context_present(
                controversy_map_context,
                "issue_cluster_count",
            ),
            "controversy_map_artifact_present": bool(
                controversy_map_context.get("controversy_map_artifact_present")
            ),
            "verification_route_present": optional_context_present(
                verification_route_context,
                "route_count",
            ),
            "verification_route_artifact_present": bool(
                verification_route_context.get("verification_route_artifact_present")
            ),
            "claim_verifiability_present": optional_context_present(
                claim_verifiability_context,
                "assessment_count",
            ),
            "claim_verifiability_artifact_present": bool(
                claim_verifiability_context.get("claim_verifiability_artifact_present")
            ),
            "formal_public_links_present": optional_context_present(
                formal_public_link_context,
                "link_count",
            ),
            "formal_public_links_artifact_present": bool(
                formal_public_link_context.get("formal_public_links_artifact_present")
            ),
            "representation_gap_present": optional_context_present(
                representation_gap_context,
                "gap_count",
            ),
            "representation_gap_artifact_present": bool(
                representation_gap_context.get("representation_gap_artifact_present")
            ),
            "diffusion_edges_present": optional_context_present(
                diffusion_edge_context,
                "edge_count",
            ),
            "diffusion_edges_artifact_present": bool(
                diffusion_edge_context.get("diffusion_edges_artifact_present")
            ),
        },
    )
    return {
        "warnings": warnings,
        "board_state": current_board_state,
        "coverages": coverages,
        "issue_clusters": issue_clusters,
        "routes": routes,
        "assessments": assessments,
        "formal_public_links": links,
        "representation_gaps": gaps,
        "diffusion_edges": edges,
        "agenda_counts": agenda_counts,
        "board_brief_text": brief_text,
        "board_summary_file": str(board_summary_file),
        "board_brief_file": str(board_brief_file),
        "coverage_file": str(coverage_file),
        "controversy_map_file": maybe_text(controversy_map_context.get("controversy_map_file")),
        "verification_route_file": maybe_text(verification_route_context.get("verification_route_file")),
        "claim_verifiability_file": maybe_text(claim_verifiability_context.get("claim_verifiability_file")),
        "formal_public_links_file": maybe_text(formal_public_link_context.get("formal_public_links_file")),
        "representation_gap_file": maybe_text(representation_gap_context.get("representation_gap_file")),
        "diffusion_edges_file": maybe_text(diffusion_edge_context.get("diffusion_edges_file")),
        **contract_fields,
    }


def load_ranked_actions_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    board_summary_path: str = "",
    board_brief_path: str = "",
    coverage_path: str = "",
    max_actions: int = 6,
) -> dict[str, Any]:
    shared_context = load_d1_shared_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        board_summary_path=board_summary_path,
        board_brief_path=board_brief_path,
        coverage_path=coverage_path,
    )
    ranked_actions = build_actions(
        shared_context.get("board_state", {}),
        (
            shared_context.get("coverages", [])
            if isinstance(shared_context.get("coverages"), list)
            else []
        ),
        (
            shared_context.get("issue_clusters", [])
            if isinstance(shared_context.get("issue_clusters"), list)
            else []
        ),
        (
            shared_context.get("routes", [])
            if isinstance(shared_context.get("routes"), list)
            else []
        ),
        (
            shared_context.get("assessments", [])
            if isinstance(shared_context.get("assessments"), list)
            else []
        ),
        (
            shared_context.get("formal_public_links", [])
            if isinstance(shared_context.get("formal_public_links"), list)
            else []
        ),
        (
            shared_context.get("representation_gaps", [])
            if isinstance(shared_context.get("representation_gaps"), list)
            else []
        ),
        (
            shared_context.get("diffusion_edges", [])
            if isinstance(shared_context.get("diffusion_edges"), list)
            else []
        ),
        maybe_text(shared_context.get("board_brief_text")),
    )[: max(1, max_actions)]
    for action in ranked_actions:
        action.setdefault("run_id", run_id)
        action.setdefault("round_id", round_id)
    return {
        **shared_context,
        "action_source": (
            "controversy-agenda-materialization"
            if any(
                int(shared_context.get("agenda_counts", {}).get(key) or 0) > 0
                for key in (
                    "issue_cluster_count",
                    "representation_gap_count",
                    "formal_public_linkage_gap_count",
                    "diffusion_focus_count",
                )
            )
            else "coverage-ranking-fallback"
        ),
        "ranked_actions": ranked_actions,
    }
