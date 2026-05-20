from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())

def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)

def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

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

def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)

def decode_json(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default

ANALYSIS_KIND_EVIDENCE_COVERAGE = "evidence-coverage"
ANALYSIS_KIND_CONTROVERSY_MAP = "controversy-map"
ANALYSIS_KIND_ISSUE_CLUSTER = "issue-cluster"
ANALYSIS_KIND_STANCE_GROUP = "stance-group"
ANALYSIS_KIND_CONCERN_FACET = "concern-facet"
ANALYSIS_KIND_ACTOR_PROFILE = "actor-profile"
ANALYSIS_KIND_EVIDENCE_CITATION_TYPE = "evidence-citation-type"
ANALYSIS_KIND_VERIFICATION_ROUTE = "verification-route"
ANALYSIS_KIND_CLAIM_VERIFIABILITY = "claim-verifiability"
ANALYSIS_KIND_FORMAL_PUBLIC_LINK = "formal-public-link"
ANALYSIS_KIND_REPRESENTATION_GAP = "representation-gap"
ANALYSIS_KIND_DIFFUSION_EDGE = "diffusion-edge"
ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE = "spatiotemporal-relation-cue"
ANALYSIS_KIND_CLAIM_GAP_ACTION_CARD = "claim-gap-action-card"
ANALYSIS_KIND_CLAIM_SCOPE = "claim-scope"
ANALYSIS_KIND_OBSERVATION_SCOPE = "observation-scope"
ANALYSIS_KIND_CLAIM_OBSERVATION_LINK = "claim-observation-link"
ANALYSIS_KIND_CLAIM_CLUSTER = "claim-cluster"
ANALYSIS_KIND_MERGED_OBSERVATION = "merged-observation"
ANALYSIS_KIND_CLAIM_CANDIDATE = "claim-candidate"
ANALYSIS_KIND_OBSERVATION_CANDIDATE = "observation-candidate"

ANALYSIS_GOVERNANCE_APPROVAL_GATED_HELPER = "approval-gated-helper-view"
ANALYSIS_GOVERNANCE_LEGACY_FROZEN = "legacy-frozen-compatibility-query-only"
ANALYSIS_GOVERNANCE_LEGACY_NAMED_HELPER = "legacy-named-approval-gated-helper-view"

ANALYSIS_KIND_GOVERNANCE_OVERRIDES: dict[str, dict[str, str]] = {
    ANALYSIS_KIND_EVIDENCE_COVERAGE: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "review-evidence-sufficiency",
        "freeze_reason": "old coverage/readiness scoring is frozen and cannot act as a phase gate or report_basis",
    },
    ANALYSIS_KIND_CLAIM_OBSERVATION_LINK: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "review-fact-check-evidence-scope",
        "freeze_reason": "old claim-observation support/contradiction matching is frozen",
    },
    ANALYSIS_KIND_OBSERVATION_SCOPE: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "aggregate-environment-evidence",
        "freeze_reason": "old observation matching scope is replaced by descriptive environmental evidence coverage",
    },
    ANALYSIS_KIND_OBSERVATION_CANDIDATE: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "aggregate-environment-evidence",
        "freeze_reason": "old observation candidate extraction is replaced by descriptive environmental evidence aggregation",
    },
    ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE: {
        "successor_skill": "detect-temporal-cooccurrence-cues",
        "freeze_reason": "spatiotemporal relation cues are candidate relation objects and cannot prove causality, transport, source attribution, or report readiness",
    },
    ANALYSIS_KIND_CLAIM_GAP_ACTION_CARD: {
        "successor_skill": "materialize-claim-gap-action-cards",
        "freeze_reason": "claim-gap action cards are advisory prompts and cannot rank, schedule, auto-execute, or prove claim sufficiency",
    },
    ANALYSIS_KIND_MERGED_OBSERVATION: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "aggregate-environment-evidence",
        "freeze_reason": "old merged observation candidates are replaced by aggregation records with caveats",
    },
    ANALYSIS_KIND_FORMAL_PUBLIC_LINK: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "compare-formal-public-footprints",
        "freeze_reason": "old formal/public alignment links are replaced by footprint comparison cues",
    },
    ANALYSIS_KIND_REPRESENTATION_GAP: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "identify-representation-audit-cues",
        "freeze_reason": "old representation gap scoring is replaced by audit cues for human review",
    },
    ANALYSIS_KIND_DIFFUSION_EDGE: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
        "successor_skill": "detect-temporal-cooccurrence-cues",
        "freeze_reason": "old diffusion edges are replaced by descriptive temporal co-occurrence cues",
    },
    ANALYSIS_KIND_CLAIM_CANDIDATE: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_NAMED_HELPER,
        "successor_skill": "discover-discourse-issues",
        "freeze_reason": "claim-named compatibility records are discourse hints only, not factual claims",
    },
    ANALYSIS_KIND_CLAIM_CLUSTER: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_NAMED_HELPER,
        "successor_skill": "discover-discourse-issues",
        "freeze_reason": "claim-cluster compatibility records are reversible discourse groupings only",
    },
    ANALYSIS_KIND_CLAIM_SCOPE: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_NAMED_HELPER,
        "successor_skill": "discover-discourse-issues",
        "freeze_reason": "claim-scope compatibility records are mentioned-scope metadata only",
    },
    ANALYSIS_KIND_CLAIM_VERIFIABILITY: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_NAMED_HELPER,
        "successor_skill": "suggest-evidence-lanes",
        "freeze_reason": "verifiability compatibility records are advisory lane cues only",
    },
    ANALYSIS_KIND_VERIFICATION_ROUTE: {
        "governance_status": ANALYSIS_GOVERNANCE_LEGACY_NAMED_HELPER,
        "successor_skill": "suggest-evidence-lanes",
        "freeze_reason": "route compatibility records cannot assign owners or drive the source queue",
    },
}

ANALYSIS_KIND_CONFIGS: dict[str, dict[str, Any]] = {
    ANALYSIS_KIND_ISSUE_CLUSTER: {
        "artifact_label": "issue-cluster",
        "default_relative": "analytics/issue_clusters_{round_id}.json",
        "items_key": "issue_clusters",
        "count_key": "issue_cluster_count",
        "id_field": "cluster_id",
        "subject_field": "issue_label",
        "score_field": "confidence",
        "state_field": "route_status",
        "related_id_fields": [
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "issue_label",
            "recommended_lane",
            "route_status",
        ],
        "canonical_object_kind": "issue-cluster",
        "default_source_skill": "materialize-research-issue-surface",
        "summary_fields": [
            "cluster_input_path",
            "claim_scope_path",
            "verifiability_path",
            "route_path",
        ],
        "query_basis_fields": [
            "cluster_input_path",
            "claim_scope_path",
            "verifiability_path",
            "route_path",
            "cluster_source",
            "claim_scope_source",
            "verifiability_source",
            "route_source",
        ],
        "parent_artifact_fields": [
            "cluster_input_path",
            "claim_scope_path",
            "verifiability_path",
            "route_path",
        ],
        "item_parent_id_fields": [
            "map_issue_id",
            "claim_cluster_id",
            "claim_scope_id",
            "assessment_id",
            "route_id",
        ],
        "item_parent_id_list_fields": ["claim_ids", "source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_STANCE_GROUP: {
        "artifact_label": "stance-group",
        "default_relative": "analytics/stance_groups_{round_id}.json",
        "items_key": "stance_groups",
        "count_key": "stance_group_count",
        "id_field": "stance_group_id",
        "subject_field": "issue_label",
        "score_field": "confidence",
        "state_field": "stance_label",
        "related_id_fields": [
            "stance_group_id",
            "cluster_id",
            "map_issue_id",
            "issue_label",
            "stance_label",
        ],
        "canonical_object_kind": "stance-group",
        "default_source_skill": "project-research-issue-views",
        "summary_fields": ["issue_clusters_path"],
        "query_basis_fields": [
            "issue_clusters_path",
            "issue_clusters_source",
        ],
        "parent_artifact_fields": ["issue_clusters_path"],
        "item_parent_id_fields": [
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "claim_scope_id",
            "assessment_id",
            "route_id",
        ],
        "item_parent_id_list_fields": ["claim_ids", "source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CONCERN_FACET: {
        "artifact_label": "concern-facet",
        "default_relative": "analytics/concern_facets_{round_id}.json",
        "items_key": "concern_facets",
        "count_key": "concern_facet_count",
        "id_field": "concern_id",
        "subject_field": "issue_label",
        "score_field": "confidence",
        "state_field": "priority",
        "related_id_fields": [
            "concern_id",
            "cluster_id",
            "map_issue_id",
            "issue_label",
            "concern_label",
            "priority",
        ],
        "canonical_object_kind": "concern-facet",
        "default_source_skill": "project-research-issue-views",
        "summary_fields": ["issue_clusters_path"],
        "query_basis_fields": [
            "issue_clusters_path",
            "issue_clusters_source",
        ],
        "parent_artifact_fields": ["issue_clusters_path"],
        "item_parent_id_fields": [
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "claim_scope_id",
            "assessment_id",
            "route_id",
        ],
        "item_parent_id_list_fields": ["claim_ids", "source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_ACTOR_PROFILE: {
        "artifact_label": "actor-profile",
        "default_relative": "analytics/actor_profiles_{round_id}.json",
        "items_key": "actor_profiles",
        "count_key": "actor_profile_count",
        "id_field": "actor_id",
        "subject_field": "issue_label",
        "score_field": "confidence",
        "state_field": "dominant_stance",
        "related_id_fields": [
            "actor_id",
            "cluster_id",
            "map_issue_id",
            "issue_label",
            "display_name",
            "actor_label",
        ],
        "canonical_object_kind": "actor-profile",
        "default_source_skill": "project-research-issue-views",
        "summary_fields": ["issue_clusters_path"],
        "query_basis_fields": [
            "issue_clusters_path",
            "issue_clusters_source",
        ],
        "parent_artifact_fields": ["issue_clusters_path"],
        "item_parent_id_fields": [
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "claim_scope_id",
            "assessment_id",
            "route_id",
        ],
        "item_parent_id_list_fields": ["claim_ids", "source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_EVIDENCE_CITATION_TYPE: {
        "artifact_label": "evidence-citation-type",
        "default_relative": "analytics/evidence_citation_types_{round_id}.json",
        "items_key": "citation_types",
        "count_key": "citation_type_count",
        "id_field": "citation_type_id",
        "subject_field": "issue_label",
        "score_field": "confidence",
        "state_field": "citation_type",
        "related_id_fields": [
            "citation_type_id",
            "cluster_id",
            "map_issue_id",
            "issue_label",
            "citation_type",
        ],
        "canonical_object_kind": "evidence-citation-type",
        "default_source_skill": "project-research-issue-views",
        "summary_fields": ["issue_clusters_path"],
        "query_basis_fields": [
            "issue_clusters_path",
            "issue_clusters_source",
        ],
        "parent_artifact_fields": ["issue_clusters_path"],
        "item_parent_id_fields": [
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "claim_scope_id",
            "assessment_id",
            "route_id",
        ],
        "item_parent_id_list_fields": ["claim_ids", "source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_DIFFUSION_EDGE: {
        "artifact_label": "diffusion-edge",
        "default_relative": "analytics/diffusion_edges_{round_id}.json",
        "items_key": "edges",
        "count_key": "edge_count",
        "id_field": "edge_id",
        "subject_field": "issue_label",
        "score_field": "confidence",
        "state_field": "edge_type",
        "related_id_fields": [
            "edge_id",
            "issue_label",
            "source_platform",
            "target_platform",
            "edge_type",
        ],
        "canonical_object_kind": "diffusion-edge",
        "default_source_skill": "detect-temporal-cooccurrence-cues",
        "summary_fields": ["formal_public_links_path"],
        "query_basis_fields": [
            "formal_public_links_path",
            "formal_public_links_source",
        ],
        "parent_artifact_fields": ["formal_public_links_path"],
        "item_parent_id_list_fields": [
            "linkage_ids",
            "cluster_ids",
            "claim_ids",
            "claim_scope_ids",
            "assessment_ids",
            "route_ids",
            "source_signal_ids",
            "target_signal_ids",
            "lineage",
        ],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE: {
        "artifact_label": "spatiotemporal-relation-cue",
        "default_relative": "analytics/spatiotemporal_relation_cues_{round_id}.json",
        "items_key": "spatiotemporal_relation_cues",
        "count_key": "relation_cue_count",
        "id_field": "relation_id",
        "subject_field": "relation_id",
        "state_field": "relation_status",
        "related_id_fields": [
            "relation_id",
            "relation_type",
            "relation_status",
            "source_signal_id",
            "target_signal_id",
            "source_role",
            "target_role",
        ],
        "canonical_object_kind": "spatiotemporal-relation-cue",
        "default_source_skill": "detect-temporal-cooccurrence-cues",
        "summary_fields": ["relation_rule_ref", "taxonomy_version"],
        "query_basis_fields": [
            "verification_scope",
            "relation_rule_ref",
            "taxonomy_version",
        ],
        "item_parent_id_fields": [
            "source_signal_id",
            "target_signal_id",
        ],
        "item_parent_id_list_fields": ["context_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_GAP_ACTION_CARD: {
        "artifact_label": "claim-gap-action-card",
        "default_relative": "analytics/claim_gap_action_cards_{round_id}.json",
        "items_key": "action_cards",
        "count_key": "action_card_count",
        "id_field": "card_id",
        "subject_field": "claim_gap",
        "state_field": "card_kind",
        "related_id_fields": [
            "card_id",
            "card_kind",
            "claim_gap",
        ],
        "default_source_skill": "materialize-claim-gap-action-cards",
        "summary_fields": [
            "mission_focus",
            "advisory_semantics",
        ],
        "query_basis_fields": [
            "mission_focus",
            "observed_input_summary",
        ],
        "item_parent_id_list_fields": [
            "source_attempt_refs",
            "challenge_refs",
            "readiness_refs",
            "lineage",
        ],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_FORMAL_PUBLIC_LINK: {
        "artifact_label": "formal-public-link",
        "default_relative": "analytics/formal_public_links_{round_id}.json",
        "items_key": "links",
        "count_key": "link_count",
        "id_field": "linkage_id",
        "subject_field": "issue_label",
        "score_field": "alignment_score",
        "state_field": "link_status",
        "related_id_fields": [
            "linkage_id",
            "issue_label",
            "link_status",
            "route_status",
            "recommended_lane",
        ],
        "canonical_object_kind": "formal-public-link",
        "default_source_skill": "compare-formal-public-footprints",
        "summary_fields": [
            "claim_cluster_path",
            "claim_candidates_path",
            "verification_route_path",
        ],
        "query_basis_fields": [
            "claim_cluster_path",
            "claim_candidates_path",
            "verification_route_path",
            "claim_cluster_source",
            "claim_candidates_source",
            "verification_route_source",
        ],
        "parent_artifact_fields": [
            "claim_cluster_path",
            "claim_candidates_path",
            "verification_route_path",
        ],
        "item_parent_id_list_fields": [
            "cluster_ids",
            "claim_ids",
            "claim_scope_ids",
            "assessment_ids",
            "route_ids",
            "formal_signal_ids",
            "public_signal_ids",
            "lineage",
        ],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_REPRESENTATION_GAP: {
        "artifact_label": "representation-gap",
        "default_relative": "analytics/representation_gaps_{round_id}.json",
        "items_key": "gaps",
        "count_key": "gap_count",
        "id_field": "gap_id",
        "subject_field": "issue_label",
        "score_field": "severity_score",
        "state_field": "gap_type",
        "related_id_fields": [
            "gap_id",
            "issue_label",
            "linkage_id",
            "gap_type",
            "severity",
        ],
        "canonical_object_kind": "representation-gap",
        "default_source_skill": "identify-representation-audit-cues",
        "summary_fields": ["formal_public_links_path"],
        "query_basis_fields": [
            "formal_public_links_path",
            "formal_public_links_source",
        ],
        "parent_artifact_fields": ["formal_public_links_path"],
        "item_parent_id_fields": ["linkage_id"],
        "item_parent_id_list_fields": [
            "cluster_ids",
            "claim_ids",
            "claim_scope_ids",
            "assessment_ids",
            "route_ids",
            "lineage",
        ],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CONTROVERSY_MAP: {
        "artifact_label": "controversy-map",
        "default_relative": "analytics/controversy_map_{round_id}.json",
        "items_key": "issue_clusters",
        "count_key": "issue_cluster_count",
        "id_field": "map_issue_id",
        "subject_field": "issue_label",
        "score_field": "confidence",
        "state_field": "route_status",
        "related_id_fields": [
            "map_issue_id",
            "cluster_id",
            "claim_scope_id",
            "assessment_id",
            "route_id",
            "issue_label",
            "recommended_lane",
            "route_status",
        ],
        "canonical_object_kind": "controversy-map",
        "default_source_skill": "export-research-issue-map",
        "summary_fields": [
            "issue_clusters_path",
            "stance_groups_path",
            "concern_facets_path",
            "actor_profiles_path",
            "evidence_citation_types_path",
        ],
        "query_basis_fields": [
            "issue_clusters_path",
            "stance_groups_path",
            "concern_facets_path",
            "actor_profiles_path",
            "evidence_citation_types_path",
            "issue_clusters_source",
            "stance_groups_source",
            "concern_facets_source",
            "actor_profiles_source",
            "evidence_citation_types_source",
        ],
        "parent_artifact_fields": [
            "issue_clusters_path",
            "stance_groups_path",
            "concern_facets_path",
            "actor_profiles_path",
            "evidence_citation_types_path",
        ],
        "item_parent_id_fields": [
            "cluster_id",
            "claim_scope_id",
            "assessment_id",
            "route_id",
        ],
        "item_parent_id_list_fields": ["claim_ids", "source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_VERIFICATION_ROUTE: {
        "artifact_label": "verification-route",
        "default_relative": "investigation/verification_routes_{round_id}.json",
        "items_key": "routes",
        "count_key": "route_count",
        "id_field": "route_id",
        "subject_field": "claim_id",
        "score_field": "confidence",
        "state_field": "route_status",
        "related_id_fields": [
            "route_id",
            "claim_id",
            "assessment_id",
            "recommended_lane",
            "route_status",
        ],
        "canonical_object_kind": "verification-route",
        "default_source_skill": "suggest-evidence-lanes",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path", "input_source"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_fields": ["claim_id", "assessment_id", "claim_scope_id"],
        "item_parent_id_list_fields": ["lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_VERIFIABILITY: {
        "artifact_label": "claim-verifiability",
        "default_relative": "analytics/claim_verifiability_assessments_{round_id}.json",
        "items_key": "assessments",
        "count_key": "assessment_count",
        "id_field": "assessment_id",
        "subject_field": "claim_id",
        "score_field": "confidence",
        "state_field": "verifiability_kind",
        "related_id_fields": [
            "assessment_id",
            "claim_id",
            "claim_scope_id",
            "verifiability_kind",
            "recommended_lane",
        ],
        "canonical_object_kind": "verifiability-assessment",
        "default_source_skill": "suggest-evidence-lanes",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path", "input_source"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_fields": ["claim_id", "claim_scope_id"],
        "item_parent_id_list_fields": ["lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_EVIDENCE_COVERAGE: {
        "artifact_label": "coverage",
        "default_relative": "analytics/evidence_coverage_{round_id}.json",
        "items_key": "coverages",
        "count_key": "coverage_count",
        "id_field": "coverage_id",
        "subject_field": "claim_id",
        "score_field": "coverage_score",
        "state_field": "readiness",
        "related_id_fields": ["coverage_id", "claim_id"],
        "default_source_skill": "review-evidence-sufficiency",
        "summary_fields": [
            "links_path",
            "claim_scope_path",
            "observation_scope_path",
        ],
        "query_basis_fields": [
            "links_path",
            "claim_scope_path",
            "observation_scope_path",
            "links_source",
            "claim_scope_source",
            "observation_scope_source",
        ],
        "parent_artifact_fields": [
            "links_path",
            "claim_scope_path",
            "observation_scope_path",
        ],
        "item_parent_id_fields": ["claim_id"],
        "item_parent_id_list_fields": ["linked_observation_ids"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_SCOPE: {
        "artifact_label": "claim-scope",
        "default_relative": "analytics/claim_scope_proposals_{round_id}.json",
        "items_key": "scopes",
        "count_key": "scope_count",
        "id_field": "claim_scope_id",
        "subject_field": "claim_id",
        "score_field": "confidence",
        "state_field": "scope_kind",
        "related_id_fields": ["claim_scope_id", "claim_id", "claim_type"],
        "canonical_object_kind": "claim-scope",
        "default_source_skill": "discover-discourse-issues",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_fields": ["claim_id"],
        "item_parent_id_list_fields": ["lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_OBSERVATION_SCOPE: {
        "artifact_label": "observation-scope",
        "default_relative": "analytics/observation_scope_proposals_{round_id}.json",
        "items_key": "scopes",
        "count_key": "scope_count",
        "id_field": "observation_scope_id",
        "subject_field": "observation_id",
        "score_field": "confidence",
        "state_field": "scope_kind",
        "related_id_fields": ["observation_scope_id", "observation_id", "metric"],
        "default_source_skill": "aggregate-environment-evidence",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_fields": ["observation_id"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_OBSERVATION_LINK: {
        "artifact_label": "claim-observation-link",
        "default_relative": "analytics/claim_observation_links_{round_id}.json",
        "items_key": "links",
        "count_key": "link_count",
        "id_field": "link_id",
        "subject_field": "claim_id",
        "score_field": "confidence",
        "state_field": "relation",
        "related_id_fields": ["link_id", "claim_id", "observation_id"],
        "default_source_skill": "review-fact-check-evidence-scope",
        "summary_fields": ["claim_input_path", "observation_input_path"],
        "query_basis_fields": [
            "claim_input_path",
            "observation_input_path",
        ],
        "parent_artifact_fields": [
            "claim_input_path",
            "observation_input_path",
        ],
        "item_parent_id_fields": ["claim_id", "observation_id"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_CLUSTER: {
        "artifact_label": "claim-cluster",
        "default_relative": "analytics/claim_candidate_clusters_{round_id}.json",
        "items_key": "clusters",
        "count_key": "cluster_count",
        "id_field": "cluster_id",
        "subject_field": "cluster_id",
        "score_field": "confidence",
        "state_field": "status",
        "related_id_fields": ["cluster_id", "claim_type", "semantic_fingerprint"],
        "canonical_object_kind": "claim-cluster",
        "default_source_skill": "discover-discourse-issues",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_list_fields": ["member_claim_ids", "source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_MERGED_OBSERVATION: {
        "artifact_label": "merged-observation",
        "default_relative": "analytics/merged_observation_candidates_{round_id}.json",
        "items_key": "merged_observations",
        "count_key": "merged_count",
        "id_field": "merged_observation_id",
        "subject_field": "merged_observation_id",
        "state_field": "aggregation",
        "related_id_fields": ["merged_observation_id", "metric"],
        "default_source_skill": "aggregate-environment-evidence",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_list_fields": ["member_observation_ids", "source_signal_ids"],
        "item_artifact_ref_fields": ["provenance_refs"],
    },
    ANALYSIS_KIND_CLAIM_CANDIDATE: {
        "artifact_label": "claim-candidate",
        "default_relative": "analytics/claim_candidates_{round_id}.json",
        "items_key": "candidates",
        "count_key": "candidate_count",
        "id_field": "claim_id",
        "subject_field": "claim_id",
        "score_field": "confidence",
        "state_field": "status",
        "related_id_fields": ["claim_id", "claim_type"],
        "canonical_object_kind": "claim-candidate",
        "default_source_skill": "discover-discourse-issues",
        "summary_fields": [],
        "item_parent_id_list_fields": ["source_signal_ids", "lineage"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_OBSERVATION_CANDIDATE: {
        "artifact_label": "observation-candidate",
        "default_relative": "analytics/observation_candidates_{round_id}.json",
        "items_key": "candidates",
        "count_key": "candidate_count",
        "id_field": "observation_id",
        "subject_field": "observation_id",
        "state_field": "aggregation",
        "related_id_fields": ["observation_id", "metric", "source_skill"],
        "default_source_skill": "aggregate-environment-evidence",
        "summary_fields": [],
        "item_parent_id_list_fields": ["source_signal_ids"],
        "item_artifact_ref_fields": ["provenance_refs"],
    },
}

def analysis_config(analysis_kind: str) -> dict[str, Any]:
    config = ANALYSIS_KIND_CONFIGS.get(maybe_text(analysis_kind))
    if config is None:
        raise ValueError(f"Unsupported analysis kind: {analysis_kind}")
    return config

def analysis_kind_governance(analysis_kind: str) -> dict[str, Any]:
    kind = maybe_text(analysis_kind)
    config = analysis_config(kind)
    override = ANALYSIS_KIND_GOVERNANCE_OVERRIDES.get(kind, {})
    governance_status = (
        maybe_text(override.get("governance_status"))
        or ANALYSIS_GOVERNANCE_APPROVAL_GATED_HELPER
    )
    successor_skill = maybe_text(override.get("successor_skill")) or maybe_text(
        config.get("default_source_skill")
    )
    freeze_reason = maybe_text(override.get("freeze_reason"))
    if not freeze_reason:
        freeze_reason = (
            "analysis result is an approval-gated helper surface and cannot act as "
            "a default investigation conclusion, phase gate, or report_basis"
        )
    return {
        "analysis_kind": kind,
        "governance_status": governance_status,
        "default_chain_eligible": False,
        "phase_gate_eligible": False,
        "report_basis_eligible": False,
        "requires_explicit_approval": True,
        "successor_skill": successor_skill,
        "source_skill": maybe_text(config.get("default_source_skill")),
        "report_use_requires": [
            "finding-record",
            "evidence-bundle",
            "proposal",
            "review-comment",
            "report-section-draft",
        ],
        "freeze_reason": freeze_reason,
    }

def _config_list(config: dict[str, Any], field_name: str) -> list[str]:
    values = config.get(field_name)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]

def analysis_kind_names() -> list[str]:
    return sorted(ANALYSIS_KIND_CONFIGS.keys())

__all__ = [
    "normalize_space",
    "maybe_text",
    "maybe_number",
    "unique_texts",
    "stable_hash",
    "utc_now_iso",
    "json_text",
    "decode_json",
    "ANALYSIS_KIND_EVIDENCE_COVERAGE",
    "ANALYSIS_KIND_CONTROVERSY_MAP",
    "ANALYSIS_KIND_ISSUE_CLUSTER",
    "ANALYSIS_KIND_STANCE_GROUP",
    "ANALYSIS_KIND_CONCERN_FACET",
    "ANALYSIS_KIND_ACTOR_PROFILE",
    "ANALYSIS_KIND_EVIDENCE_CITATION_TYPE",
    "ANALYSIS_KIND_VERIFICATION_ROUTE",
    "ANALYSIS_KIND_CLAIM_VERIFIABILITY",
    "ANALYSIS_KIND_FORMAL_PUBLIC_LINK",
    "ANALYSIS_KIND_REPRESENTATION_GAP",
    "ANALYSIS_KIND_DIFFUSION_EDGE",
    "ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE",
    "ANALYSIS_KIND_CLAIM_GAP_ACTION_CARD",
    "ANALYSIS_KIND_CLAIM_SCOPE",
    "ANALYSIS_KIND_OBSERVATION_SCOPE",
    "ANALYSIS_KIND_CLAIM_OBSERVATION_LINK",
    "ANALYSIS_KIND_CLAIM_CLUSTER",
    "ANALYSIS_KIND_MERGED_OBSERVATION",
    "ANALYSIS_KIND_CLAIM_CANDIDATE",
    "ANALYSIS_KIND_OBSERVATION_CANDIDATE",
    "ANALYSIS_GOVERNANCE_APPROVAL_GATED_HELPER",
    "ANALYSIS_GOVERNANCE_LEGACY_FROZEN",
    "ANALYSIS_GOVERNANCE_LEGACY_NAMED_HELPER",
    "analysis_config",
    "analysis_kind_governance",
    "_config_list",
    "analysis_kind_names",
]
