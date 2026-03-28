#!/usr/bin/env python3
"""Build report packets and moderator decision drafts for eco-council rounds."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import (
    atomic_write_text_file,
    load_canonical_list,
    load_json_if_exists,
    pretty_json,
    read_json,
    stable_hash,
    stable_json,
    utc_now_iso,
    write_json,
)
from eco_council_runtime.adapters.run_paths import discover_round_ids, load_mission
from eco_council_runtime.application import reporting_artifacts as application_reporting_artifacts
from eco_council_runtime.application.reporting_state import (
    active_library_list as application_active_library_list,
    augment_context_with_matching_state as application_augment_context_with_matching_state,
    collect_round_state as application_collect_round_state,
    compact_evidence_adjudication_summary as application_compact_evidence_adjudication_summary,
    compact_investigation_review_summary as application_compact_investigation_review_summary,
    compact_matching_adjudication_summary as application_compact_matching_adjudication_summary,
    compact_matching_authorization_summary as application_compact_matching_authorization_summary,
    compact_matching_result_summary as application_compact_matching_result_summary,
    compact_recommendation as application_compact_recommendation,
    effective_shared_claims as application_effective_shared_claims,
    effective_shared_observations as application_effective_shared_observations,
    hydrate_observation_submissions_with_observations as application_hydrate_observation_submissions_with_observations,
    load_dict_if_exists as application_load_dict_if_exists,
    load_override_requests as application_load_override_requests,
    matching_executed_for_state as application_matching_executed_for_state,
    materialize_shared_observation as application_materialize_shared_observation,
    mission_constraints as application_mission_constraints,
    mission_policy_profile as application_mission_policy_profile,
    mission_run_id as application_mission_run_id,
    observation_match_key as application_observation_match_key,
    observation_signature_payload as application_observation_signature_payload,
    observation_submission_id as application_observation_submission_id,
    phase_state_from_round_state as application_phase_state_from_round_state,
    round_ids_through as application_round_ids_through,
    shared_observation_id as application_shared_observation_id,
    state_auditable_submissions as application_state_auditable_submissions,
    state_current_submissions as application_state_current_submissions,
    state_submissions as application_state_submissions,
)
from eco_council_runtime.application.reporting_drafts import (
    build_data_readiness_draft as application_build_data_readiness_draft,
    build_decision_draft_from_state as application_build_decision_draft_from_state,
    build_decision_missing_types as application_build_decision_missing_types,
    build_decision_summary_from_state as application_build_decision_summary_from_state,
    build_environmentalist_findings as application_build_environmentalist_findings,
    build_expert_report_draft_from_state as application_build_expert_report_draft_from_state,
    build_final_brief as application_build_final_brief,
    build_hypothesis_review_from_state as application_build_hypothesis_review_from_state,
    build_investigation_review_draft_from_state as application_build_investigation_review_draft_from_state,
    build_leg_review_from_state as application_build_leg_review_from_state,
    build_open_questions as application_build_open_questions,
    build_pre_match_report_findings as application_build_pre_match_report_findings,
    build_readiness_findings_from_submissions as application_build_readiness_findings_from_submissions,
    build_report_draft as application_build_report_draft,
    build_report_summary_from_state as application_build_report_summary_from_state,
    build_sociologist_findings as application_build_sociologist_findings,
    build_summary_for_role as application_build_summary_for_role,
    claims_by_id_map as application_claims_by_id_map,
    claim_sort_key as application_claim_sort_key,
    completion_score_for_round as application_completion_score_for_round,
    evidence_by_claim_map as application_evidence_by_claim_map,
    evidence_rank as application_evidence_rank,
    evidence_resolution_score as application_evidence_resolution_score,
    evidence_sufficiency_for_round as application_evidence_sufficiency_for_round,
    environment_role_required as application_environment_role_required,
    expert_report_status_from_state as application_expert_report_status_from_state,
    expected_output_kinds_for_role as application_expected_output_kinds_for_role,
    gap_to_question as application_gap_to_question,
    generic_readiness_recommendations as application_generic_readiness_recommendations,
    infer_missing_evidence_types as application_infer_missing_evidence_types,
    investigation_leg_metric_families as application_investigation_leg_metric_families,
    investigation_review_overall_status as application_investigation_review_overall_status,
    metrics_for_evidence as application_metrics_for_evidence,
    missing_types_from_reason_texts as application_missing_types_from_reason_texts,
    observation_metrics_from_submissions as application_observation_metrics_from_submissions,
    observation_supports_investigation_leg as application_observation_supports_investigation_leg,
    observations_by_id_map as application_observations_by_id_map,
    readiness_missing_types as application_readiness_missing_types,
    readiness_score as application_readiness_score,
    record_has_investigation_tags as application_record_has_investigation_tags,
    record_matches_hypothesis_leg as application_record_matches_hypothesis_leg,
    report_completion_score as application_report_completion_score,
    report_has_substance as application_report_has_substance,
    report_is_placeholder as application_report_is_placeholder,
    report_status_for_role as application_report_status_for_role,
    summarize_investigation_leg_status as application_summarize_investigation_leg_status,
)
from eco_council_runtime.application.reporting_views import (
    aggregate_compact_audit as application_aggregate_compact_audit,
    build_claim_candidate_pool_summary as application_build_claim_candidate_pool_summary,
    build_fallback_context as application_build_fallback_context,
    build_fallback_context_from_state as application_build_fallback_context_from_state,
    build_observation_candidate_pool_summary as application_build_observation_candidate_pool_summary,
    candidate_claim_entry_from_candidate as application_candidate_claim_entry_from_candidate,
    candidate_observation_entry_from_candidate as application_candidate_observation_entry_from_candidate,
    claim_day_bucket as application_claim_day_bucket,
    claim_submission_channels as application_claim_submission_channels,
    claim_submission_channels_for_submission as application_claim_submission_channels_for_submission,
    claim_submission_source_skills as application_claim_submission_source_skills,
    compact_claim as application_compact_claim,
    compact_claim_candidate_for_curation as application_compact_claim_candidate_for_curation,
    compact_claim_scope as application_compact_claim_scope,
    compact_claim_submission as application_compact_claim_submission,
    compact_count_items as application_compact_count_items,
    compact_distribution_summary as application_compact_distribution_summary,
    compact_evidence_card as application_compact_evidence_card,
    compact_isolated_entry as application_compact_isolated_entry,
    compact_observation as application_compact_observation,
    compact_observation_candidate_for_curation as application_compact_observation_candidate_for_curation,
    compact_observation_submission as application_compact_observation_submission,
    compact_remand_entry as application_compact_remand_entry,
    compact_statistics as application_compact_statistics,
    counter_dict as application_counter_dict,
    environment_family_priority_order as application_environment_family_priority_order,
    guess_observation_candidate_evidence_role as application_guess_observation_candidate_evidence_role,
    load_context_or_fallback as application_load_context_or_fallback,
    load_context_or_fallback_from_state as application_load_context_or_fallback_from_state,
    merge_count_items as application_merge_count_items,
    maybe_number as application_maybe_number,
    observation_candidate_signal_count as application_observation_candidate_signal_count,
    observation_metric_family as application_observation_metric_family,
    observation_submission_severity as application_observation_submission_severity,
    point_bucket_from_scope as application_point_bucket_from_scope,
    public_source_channel as application_public_source_channel,
    public_source_channels as application_public_source_channels,
    ranked_claim_candidates as application_ranked_claim_candidates,
    representative_observation_order as application_representative_observation_order,
    representative_observations_for_state as application_representative_observations_for_state,
    representative_submissions as application_representative_submissions,
    select_environment_submissions as application_select_environment_submissions,
    select_public_submissions as application_select_public_submissions,
    sorted_counter_items as application_sorted_counter_items,
    to_nonnegative_int as application_to_nonnegative_int,
    top_counter_items as application_top_counter_items,
)
from eco_council_runtime.cli_invocation import runtime_module_command
from eco_council_runtime.controller.audit_chain import record_decision_phase_receipt
from eco_council_runtime.controller.paths import (
    cards_active_path,
    claim_candidates_path,
    claim_curation_draft_path,
    claim_curation_packet_path,
    claim_curation_path,
    claim_curation_prompt_path,
    claim_submissions_path,
    claims_active_path,
    data_readiness_draft_path,
    data_readiness_packet_path,
    data_readiness_prompt_path,
    data_readiness_report_path,
    decision_draft_path,
    decision_packet_path,
    decision_prompt_path,
    decision_target_path,
    evidence_adjudication_path,
    evidence_library_dir,
    fetch_execution_path,
    investigation_plan_path,
    investigation_review_draft_path,
    investigation_review_packet_path,
    investigation_review_path,
    investigation_review_prompt_path,
    isolated_active_path,
    matching_adjudication_draft_path,
    matching_adjudication_packet_path,
    matching_adjudication_path,
    matching_adjudication_prompt_path,
    matching_authorization_draft_path,
    matching_authorization_packet_path,
    matching_authorization_path,
    matching_authorization_prompt_path,
    matching_candidate_set_path,
    matching_result_path,
    mission_path,
    observation_candidates_path,
    observation_curation_draft_path,
    observation_curation_packet_path,
    observation_curation_path,
    observation_curation_prompt_path,
    observation_submissions_path,
    observations_active_path,
    override_requests_path,
    remands_open_path,
    report_draft_path,
    report_packet_path,
    report_prompt_path,
    report_target_path,
    role_context_path,
    round_dir,
    shared_claims_path,
    shared_evidence_path,
    shared_observations_path,
    tasks_path,
)
from eco_council_runtime.drafts import (
    can_replace_existing_exact,
    can_replace_existing_report,
    decision_prompt_text,
    load_draft_payload,
    promote_draft,
    report_prompt_text,
)
from eco_council_runtime.investigation import causal_focus_for_role
from eco_council_runtime.planning import (
    base_recommendations_from_missing_types,
    build_decision_override_requests,
    build_next_round_tasks,
    collect_unresolved_anchor_refs,
    combine_recommendations,
    recommendation_key,
)
from eco_council_runtime.domain.contract_bridge import (
    contract_call,
    effective_matching_authorization,
    resolve_schema_version,
    validate_bundle,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.rounds import current_round_number, next_round_id_for, parse_round_components, round_sort_key
from eco_council_runtime.domain.text import maybe_text, normalize_space, truncate_text, unique_strings

SCHEMA_VERSION = resolve_schema_version("1.0.0")
REPORT_ROLES = ("sociologist", "environmentalist")
READINESS_ROLES = ("sociologist", "environmentalist")
PROMOTABLE_REPORT_ROLES = ("sociologist", "environmentalist", "historian")
VERDICT_SCORES = {"supports": 1.0, "contradicts": 1.0, "mixed": 0.6, "insufficient": 0.25}
METEOROLOGY_METRICS = {"temperature_2m", "wind_speed_10m", "relative_humidity_2m", "precipitation_sum", "precipitation"}
PRECIPITATION_METRICS = {
    "precipitation",
    "precipitation_sum",
    "soil_moisture_0_to_7cm",
}
HYDROLOGY_METRICS = {
    "river_discharge",
    "river_discharge_mean",
    "river_discharge_max",
    "river_discharge_min",
    "river_discharge_p25",
    "river_discharge_p75",
    "gage_height",
}
QUESTION_RULES = (
    ("station-grade corroboration is missing", "Can station-grade air-quality measurements be added for the same mission window?"),
    ("modeled background fields should be cross-checked", "Can modeled air-quality fields be cross-checked with station or local observations?"),
    ("no mission-aligned observations matched", "Should the next round expand physical coverage or narrow claim scope so observations can be matched?"),
)
PUBLIC_SOURCE_FAMILIES = {
    "gdelt-doc-search": "gdelt",
    "gdelt-events-fetch": "gdelt",
    "gdelt-mentions-fetch": "gdelt",
    "gdelt-gkg-fetch": "gdelt",
    "bluesky-cascade-fetch": "bluesky",
    "youtube-video-search": "youtube",
    "youtube-comments-fetch": "youtube",
    "federal-register-doc-fetch": "rulemaking",
    "regulationsgov-comments-fetch": "rulemaking",
    "regulationsgov-comment-detail-fetch": "rulemaking",
}
METRIC_FAMILY_GROUPS = {
    "air-quality": {
        "pm2_5",
        "pm2_5_aqi",
        "pm10",
        "pm10_aqi",
        "us_aqi",
        "nitrogen_dioxide",
        "nitrogen_dioxide_aqi",
        "ozone",
        "ozone_aqi",
        "sulfur_dioxide",
        "sulfur_dioxide_aqi",
        "carbon_monoxide",
        "carbon_monoxide_aqi",
    },
    "fire-detection": {
        "fire_detection",
        "fire_detection_count",
    },
    "meteorology": {
        "temperature_2m",
        "wind_speed_10m",
        "relative_humidity_2m",
        "precipitation",
        "precipitation_sum",
    },
    "hydrology": {
        "river_discharge",
        "river_discharge_mean",
        "river_discharge_max",
        "river_discharge_min",
        "river_discharge_p25",
        "river_discharge_p75",
        "gage_height",
    },
    "soil": {
        "soil_moisture_0_to_7cm",
    },
}
DEFAULT_ENVIRONMENT_FAMILY_ORDER = ("air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other")
INVESTIGATION_LEG_METRIC_FAMILIES: dict[str, dict[str, set[str]]] = {
    "local-event": {
        "impact": {"air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other"},
        "public_interpretation": set(),
    },
    "smoke-transport": {
        "source": {"fire-detection"},
        "mechanism": {"meteorology"},
        "impact": {"air-quality"},
        "public_interpretation": set(),
    },
    "flood-upstream": {
        "source": {"meteorology", "hydrology"},
        "mechanism": {"meteorology", "hydrology"},
        "impact": {"hydrology"},
        "public_interpretation": set(),
    },
}
def write_text(path: Path, text: str) -> None:
    atomic_write_text_file(path, text)


mission_run_id = application_mission_run_id
mission_constraints = application_mission_constraints
mission_policy_profile = application_mission_policy_profile
round_ids_through = application_round_ids_through
observation_signature_payload = application_observation_signature_payload
shared_observation_id = application_shared_observation_id
materialize_shared_observation = application_materialize_shared_observation
observation_submission_id = application_observation_submission_id
effective_shared_observations = application_effective_shared_observations
effective_shared_claims = application_effective_shared_claims
active_library_list = application_active_library_list
counter_dict = application_counter_dict
top_counter_items = application_top_counter_items
sorted_counter_items = application_sorted_counter_items
build_fallback_context = application_build_fallback_context
load_context_or_fallback = application_load_context_or_fallback
public_source_channel = application_public_source_channel
public_source_channels = application_public_source_channels
compact_claim_scope = application_compact_claim_scope
compact_claim = application_compact_claim
compact_observation = application_compact_observation
compact_evidence_card = application_compact_evidence_card
compact_claim_submission = application_compact_claim_submission
compact_observation_submission = application_compact_observation_submission
compact_isolated_entry = application_compact_isolated_entry
compact_remand_entry = application_compact_remand_entry
compact_claim_candidate_for_curation = application_compact_claim_candidate_for_curation
compact_observation_candidate_for_curation = application_compact_observation_candidate_for_curation
merge_count_items = application_merge_count_items
claim_day_bucket = application_claim_day_bucket
point_bucket_from_scope = application_point_bucket_from_scope
observation_candidate_signal_count = application_observation_candidate_signal_count
build_claim_candidate_pool_summary = application_build_claim_candidate_pool_summary
build_observation_candidate_pool_summary = application_build_observation_candidate_pool_summary
ranked_claim_candidates = application_ranked_claim_candidates
candidate_claim_entry_from_candidate = application_candidate_claim_entry_from_candidate
guess_observation_candidate_evidence_role = application_guess_observation_candidate_evidence_role
candidate_observation_entry_from_candidate = application_candidate_observation_entry_from_candidate
to_nonnegative_int = application_to_nonnegative_int
maybe_number = application_maybe_number
compact_count_items = application_compact_count_items
compact_distribution_summary = application_compact_distribution_summary
compact_statistics = application_compact_statistics
observation_metric_family = application_observation_metric_family
environment_family_priority_order = application_environment_family_priority_order
claim_submission_source_skills = application_claim_submission_source_skills
claim_submission_channels_for_submission = application_claim_submission_channels_for_submission
claim_submission_channels = application_claim_submission_channels
select_public_submissions = application_select_public_submissions
observation_submission_severity = application_observation_submission_severity
representative_observation_order = application_representative_observation_order
select_environment_submissions = application_select_environment_submissions
representative_submissions = application_representative_submissions
aggregate_compact_audit = application_aggregate_compact_audit
representative_observations_for_state = application_representative_observations_for_state
build_fallback_context_from_state = application_build_fallback_context_from_state
load_context_or_fallback_from_state = application_load_context_or_fallback_from_state
load_dict_if_exists = application_load_dict_if_exists
load_override_requests = application_load_override_requests
matching_executed_for_state = application_matching_executed_for_state
compact_recommendation = application_compact_recommendation
compact_matching_authorization_summary = application_compact_matching_authorization_summary
compact_matching_result_summary = application_compact_matching_result_summary
compact_matching_adjudication_summary = application_compact_matching_adjudication_summary
compact_evidence_adjudication_summary = application_compact_evidence_adjudication_summary
compact_investigation_review_summary = application_compact_investigation_review_summary
augment_context_with_matching_state = application_augment_context_with_matching_state
phase_state_from_round_state = application_phase_state_from_round_state
collect_round_state = application_collect_round_state
state_current_submissions = application_state_current_submissions
state_auditable_submissions = application_state_auditable_submissions
observation_match_key = application_observation_match_key
hydrate_observation_submissions_with_observations = application_hydrate_observation_submissions_with_observations
state_submissions = application_state_submissions
report_is_placeholder = application_report_is_placeholder
report_has_substance = application_report_has_substance
claim_sort_key = application_claim_sort_key
evidence_rank = application_evidence_rank
gap_to_question = application_gap_to_question
expected_output_kinds_for_role = application_expected_output_kinds_for_role
infer_missing_evidence_types = application_infer_missing_evidence_types
observations_by_id_map = application_observations_by_id_map
evidence_by_claim_map = application_evidence_by_claim_map
claims_by_id_map = application_claims_by_id_map
metrics_for_evidence = application_metrics_for_evidence
report_status_for_role = application_report_status_for_role
build_summary_for_role = application_build_summary_for_role
build_sociologist_findings = application_build_sociologist_findings
build_environmentalist_findings = application_build_environmentalist_findings
build_open_questions = application_build_open_questions
build_report_draft = application_build_report_draft
observation_metrics_from_submissions = application_observation_metrics_from_submissions
environment_role_required = application_environment_role_required
readiness_missing_types = application_readiness_missing_types
generic_readiness_recommendations = application_generic_readiness_recommendations
build_readiness_findings_from_submissions = application_build_readiness_findings_from_submissions
build_data_readiness_draft = application_build_data_readiness_draft
investigation_leg_metric_families = application_investigation_leg_metric_families
summarize_investigation_leg_status = application_summarize_investigation_leg_status
investigation_review_overall_status = application_investigation_review_overall_status
record_matches_hypothesis_leg = application_record_matches_hypothesis_leg
record_has_investigation_tags = application_record_has_investigation_tags
observation_supports_investigation_leg = application_observation_supports_investigation_leg
build_leg_review_from_state = application_build_leg_review_from_state
build_hypothesis_review_from_state = application_build_hypothesis_review_from_state
build_investigation_review_draft_from_state = application_build_investigation_review_draft_from_state
build_pre_match_report_findings = application_build_pre_match_report_findings
expert_report_status_from_state = application_expert_report_status_from_state
build_report_summary_from_state = application_build_report_summary_from_state
build_expert_report_draft_from_state = application_build_expert_report_draft_from_state
readiness_score = application_readiness_score
missing_types_from_reason_texts = application_missing_types_from_reason_texts
build_decision_missing_types = application_build_decision_missing_types
build_decision_summary_from_state = application_build_decision_summary_from_state
evidence_resolution_score = application_evidence_resolution_score
report_completion_score = application_report_completion_score
completion_score_for_round = application_completion_score_for_round
evidence_sufficiency_for_round = application_evidence_sufficiency_for_round
build_final_brief = application_build_final_brief
build_decision_draft_from_state = application_build_decision_draft_from_state
REPORT_ROLES = application_reporting_artifacts.REPORT_ROLES
READINESS_ROLES = application_reporting_artifacts.READINESS_ROLES
PROMOTABLE_REPORT_ROLES = application_reporting_artifacts.PROMOTABLE_REPORT_ROLES
load_report_for_decision = application_reporting_artifacts.load_report_for_decision
build_report_instructions = application_reporting_artifacts.build_report_instructions
build_report_packet = application_reporting_artifacts.build_report_packet
build_claim_curation_draft = application_reporting_artifacts.build_claim_curation_draft
build_observation_curation_draft = application_reporting_artifacts.build_observation_curation_draft
build_claim_curation_instructions = application_reporting_artifacts.build_claim_curation_instructions
build_observation_curation_instructions = application_reporting_artifacts.build_observation_curation_instructions
build_claim_curation_packet = application_reporting_artifacts.build_claim_curation_packet
build_observation_curation_packet = application_reporting_artifacts.build_observation_curation_packet
claim_curation_prompt_text = application_reporting_artifacts.claim_curation_prompt_text
observation_curation_prompt_text = application_reporting_artifacts.observation_curation_prompt_text
build_data_readiness_instructions = application_reporting_artifacts.build_data_readiness_instructions
build_data_readiness_packet = application_reporting_artifacts.build_data_readiness_packet
data_readiness_prompt_text = application_reporting_artifacts.data_readiness_prompt_text
build_matching_authorization_draft = application_reporting_artifacts.build_matching_authorization_draft
build_matching_authorization_instructions = application_reporting_artifacts.build_matching_authorization_instructions
build_matching_authorization_packet = application_reporting_artifacts.build_matching_authorization_packet
matching_authorization_prompt_text = application_reporting_artifacts.matching_authorization_prompt_text
build_matching_adjudication_instructions = application_reporting_artifacts.build_matching_adjudication_instructions
build_matching_adjudication_packet = application_reporting_artifacts.build_matching_adjudication_packet
matching_adjudication_prompt_text = application_reporting_artifacts.matching_adjudication_prompt_text
build_investigation_review_instructions = application_reporting_artifacts.build_investigation_review_instructions
build_investigation_review_packet = application_reporting_artifacts.build_investigation_review_packet
investigation_review_prompt_text = application_reporting_artifacts.investigation_review_prompt_text
build_decision_packet_from_state = application_reporting_artifacts.build_decision_packet_from_state
load_report_draft_payload = application_reporting_artifacts.load_report_draft_payload
load_decision_draft_payload = application_reporting_artifacts.load_decision_draft_payload
load_matching_authorization_draft_payload = application_reporting_artifacts.load_matching_authorization_draft_payload
load_matching_adjudication_draft_payload = application_reporting_artifacts.load_matching_adjudication_draft_payload
load_investigation_review_draft_payload = application_reporting_artifacts.load_investigation_review_draft_payload
promote_report_draft = application_reporting_artifacts.promote_report_draft
promote_decision_draft = application_reporting_artifacts.promote_decision_draft
promote_matching_authorization_draft = application_reporting_artifacts.promote_matching_authorization_draft
promote_matching_adjudication_draft = application_reporting_artifacts.promote_matching_adjudication_draft
promote_investigation_review_draft = application_reporting_artifacts.promote_investigation_review_draft
curation_status_complete = application_reporting_artifacts.curation_status_complete
curations_materialized_for_round = application_reporting_artifacts.curations_materialized_for_round
curation_artifacts = application_reporting_artifacts.curation_artifacts
render_openclaw_prompts = application_reporting_artifacts.render_openclaw_prompts
data_readiness_artifacts = application_reporting_artifacts.data_readiness_artifacts
matching_authorization_artifacts = application_reporting_artifacts.matching_authorization_artifacts
matching_adjudication_artifacts = application_reporting_artifacts.matching_adjudication_artifacts
investigation_review_artifacts = application_reporting_artifacts.investigation_review_artifacts
report_artifacts = application_reporting_artifacts.report_artifacts
decision_artifacts = application_reporting_artifacts.decision_artifacts


def command_build_report_packets(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return report_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_curation_packets(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return curation_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_data_readiness_packets(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return data_readiness_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_matching_authorization_packet(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return matching_authorization_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_matching_adjudication_packet(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return matching_adjudication_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_investigation_review_packet(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return investigation_review_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)


def command_build_decision_packet(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    next_round_id = args.next_round_id or next_round_id_for(args.round_id)
    return decision_artifacts(
        run_dir=run_dir,
        round_id=args.round_id,
        next_round_id=next_round_id,
        pretty=args.pretty,
        prefer_draft_reports=args.prefer_draft_reports,
    )


def command_build_all(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    next_round_id = args.next_round_id or next_round_id_for(args.round_id)
    outputs: dict[str, Any] = {
        "curation": curation_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty),
    }
    state = collect_round_state(run_dir, args.round_id)
    if curations_materialized_for_round(run_dir=run_dir, round_id=args.round_id, state=state):
        outputs["data_readiness"] = data_readiness_artifacts(
            run_dir=run_dir,
            round_id=args.round_id,
            pretty=args.pretty,
        )
        state = collect_round_state(run_dir, args.round_id)
    else:
        outputs["data_readiness"] = {
            "skipped": True,
            "reason": (
                "Curation has not been materialized into claim/observation submissions yet; "
                "readiness packets were intentionally not built."
            ),
        }
    if all(isinstance(state.get("readiness_reports", {}).get(role), dict) and state.get("readiness_reports", {}).get(role) for role in READINESS_ROLES):
        outputs["matching_authorization"] = matching_authorization_artifacts(
            run_dir=run_dir,
            round_id=args.round_id,
            pretty=args.pretty,
        )
        state = collect_round_state(run_dir, args.round_id)
    if maybe_text(state.get("matching_authorization", {}).get("authorization_status")) == "authorized":
        candidate_path = matching_candidate_set_path(run_dir, args.round_id)
        draft_path = matching_adjudication_draft_path(run_dir, args.round_id)
        if candidate_path.exists() and draft_path.exists():
            outputs["matching_adjudication"] = matching_adjudication_artifacts(
                run_dir=run_dir,
                round_id=args.round_id,
                pretty=args.pretty,
            )
    if matching_result_path(run_dir, args.round_id).exists() and evidence_adjudication_path(run_dir, args.round_id).exists():
        outputs["investigation_review"] = investigation_review_artifacts(
            run_dir=run_dir,
            round_id=args.round_id,
            pretty=args.pretty,
        )
    if investigation_review_path(run_dir, args.round_id).exists():
        outputs["reports"] = report_artifacts(run_dir=run_dir, round_id=args.round_id, pretty=args.pretty)
    authorization_status = maybe_text(state.get("matching_authorization", {}).get("authorization_status"))
    if authorization_status in {"deferred", "not-authorized"} or all(
        report_draft_path(run_dir, args.round_id, role).exists() or report_target_path(run_dir, args.round_id, role).exists()
        for role in REPORT_ROLES
    ):
        outputs["decision"] = decision_artifacts(
            run_dir=run_dir,
            round_id=args.round_id,
            next_round_id=next_round_id,
            pretty=args.pretty,
            prefer_draft_reports=args.prefer_draft_reports,
        )
    return outputs


def command_render_openclaw_prompts(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    outputs = render_openclaw_prompts(run_dir=run_dir, round_id=args.round_id)
    return {
        "run_dir": str(run_dir),
        "round_id": args.round_id,
        "outputs": outputs,
    }


def command_promote_report_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_report_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        role=args.role,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_decision_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_decision_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_matching_authorization_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_matching_authorization_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_matching_adjudication_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_matching_adjudication_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_investigation_review_draft(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    return promote_investigation_review_draft(
        run_dir=run_dir,
        round_id=args.round_id,
        draft_path_text=args.draft_path,
        pretty=args.pretty,
        allow_overwrite=args.allow_overwrite,
    )


def command_promote_all(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    authorization_result = None
    adjudication_result = None
    investigation_review_result = None
    if matching_authorization_draft_path(run_dir, args.round_id).exists():
        authorization_result = promote_matching_authorization_draft(
            run_dir=run_dir,
            round_id=args.round_id,
            draft_path_text="",
            pretty=args.pretty,
            allow_overwrite=args.allow_overwrite,
        )
    if matching_adjudication_draft_path(run_dir, args.round_id).exists():
        adjudication_result = promote_matching_adjudication_draft(
            run_dir=run_dir,
            round_id=args.round_id,
            draft_path_text="",
            pretty=args.pretty,
            allow_overwrite=args.allow_overwrite,
        )
    if investigation_review_draft_path(run_dir, args.round_id).exists():
        investigation_review_result = promote_investigation_review_draft(
            run_dir=run_dir,
            round_id=args.round_id,
            draft_path_text="",
            pretty=args.pretty,
            allow_overwrite=args.allow_overwrite,
        )
    report_results = []
    for role in REPORT_ROLES:
        if report_draft_path(run_dir, args.round_id, role).exists():
            report_results.append(
                promote_report_draft(
                    run_dir=run_dir,
                    round_id=args.round_id,
                    role=role,
                    draft_path_text="",
                    pretty=args.pretty,
                    allow_overwrite=args.allow_overwrite,
                )
            )
    decision_result = None
    if decision_draft_path(run_dir, args.round_id).exists():
        decision_result = promote_decision_draft(
            run_dir=run_dir,
            round_id=args.round_id,
            draft_path_text="",
            pretty=args.pretty,
            allow_overwrite=args.allow_overwrite,
        )
    bundle_result = validate_bundle(run_dir)
    return {
        "run_dir": str(run_dir),
        "round_id": args.round_id,
        "matching_authorization_result": authorization_result,
        "matching_adjudication_result": adjudication_result,
        "investigation_review_result": investigation_review_result,
        "report_results": report_results,
        "decision_result": decision_result,
        "bundle_validation": bundle_result,
    }


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build eco-council curation, readiness, investigation-review, report packets, and decision drafts.")
    sub = parser.add_subparsers(dest="command", required=True)

    curation_packets = sub.add_parser("build-curation-packets", help="Build claim/observation curation packets and draft curations.")
    curation_packets.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    curation_packets.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(curation_packets)

    readiness_packets = sub.add_parser("build-data-readiness-packets", help="Build data-readiness packets and draft readiness reports.")
    readiness_packets.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    readiness_packets.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(readiness_packets)

    authorization_packet = sub.add_parser("build-matching-authorization-packet", help="Build moderator matching-authorization packet and draft.")
    authorization_packet.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    authorization_packet.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(authorization_packet)

    adjudication_packet = sub.add_parser("build-matching-adjudication-packet", help="Build moderator matching-adjudication packet from the nominated candidate set and draft.")
    adjudication_packet.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    adjudication_packet.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(adjudication_packet)

    review_packet = sub.add_parser("build-investigation-review-packet", help="Build moderator investigation-review packet and draft after matching materialization.")
    review_packet.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    review_packet.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(review_packet)

    report_packets = sub.add_parser(
        "build-report-packets",
        help="Build expert report packets and draft expert reports after canonical moderator investigation review is available.",
    )
    report_packets.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    report_packets.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(report_packets)

    decision_packet = sub.add_parser("build-decision-packet", help="Build moderator decision packet and decision draft.")
    decision_packet.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    decision_packet.add_argument("--round-id", required=True, help="Round identifier.")
    decision_packet.add_argument("--next-round-id", default="", help="Optional explicit next round identifier.")
    decision_packet.add_argument("--prefer-draft-reports", action="store_true", help="Prefer derived report drafts over canonical expert reports whenever drafts are present.")
    add_pretty_flag(decision_packet)

    build_all = sub.add_parser("build-all", help="Build the next valid reporting artifacts for the round, including investigation review, reports, and decision drafts when applicable.")
    build_all.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    build_all.add_argument("--round-id", required=True, help="Round identifier.")
    build_all.add_argument("--next-round-id", default="", help="Optional explicit next round identifier.")
    build_all.add_argument("--prefer-draft-reports", action="store_true", help="Prefer derived report drafts over canonical expert reports whenever drafts are present.")
    add_pretty_flag(build_all)

    render_prompts = sub.add_parser("render-openclaw-prompts", help="Render OpenClaw text prompts from existing curation, readiness, matching, investigation-review, report, and decision packets.")
    render_prompts.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    render_prompts.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(render_prompts)

    promote_report = sub.add_parser("promote-report-draft", help="Promote one draft expert-report into the canonical report path.")
    promote_report.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_report.add_argument("--round-id", required=True, help="Round identifier.")
    promote_report.add_argument("--role", required=True, choices=PROMOTABLE_REPORT_ROLES, help="Expert role.")
    promote_report.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_report.add_argument("--allow-overwrite", action="store_true", help="Allow overwrite of an existing non-placeholder canonical report.")
    add_pretty_flag(promote_report)

    promote_decision = sub.add_parser("promote-decision-draft", help="Promote one draft council-decision into the canonical moderator path.")
    promote_decision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_decision.add_argument("--round-id", required=True, help="Round identifier.")
    promote_decision.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_decision.add_argument("--allow-overwrite", action="store_true", help="Allow overwrite of an existing canonical decision.")
    add_pretty_flag(promote_decision)

    promote_matching_authorization = sub.add_parser(
        "promote-matching-authorization-draft",
        help="Promote the moderator matching-authorization draft into the canonical moderator path.",
    )
    promote_matching_authorization.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_matching_authorization.add_argument("--round-id", required=True, help="Round identifier.")
    promote_matching_authorization.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_matching_authorization.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Allow overwrite of an existing canonical matching-authorization.",
    )
    add_pretty_flag(promote_matching_authorization)

    promote_matching_adjudication = sub.add_parser(
        "promote-matching-adjudication-draft",
        help="Promote the moderator matching-adjudication draft into the canonical moderator path.",
    )
    promote_matching_adjudication.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_matching_adjudication.add_argument("--round-id", required=True, help="Round identifier.")
    promote_matching_adjudication.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_matching_adjudication.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Allow overwrite of an existing canonical matching-adjudication.",
    )
    add_pretty_flag(promote_matching_adjudication)

    promote_investigation_review = sub.add_parser(
        "promote-investigation-review-draft",
        help="Promote the moderator investigation-review draft into the canonical moderator path.",
    )
    promote_investigation_review.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_investigation_review.add_argument("--round-id", required=True, help="Round identifier.")
    promote_investigation_review.add_argument("--draft-path", default="", help="Optional explicit draft JSON path.")
    promote_investigation_review.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Allow overwrite of an existing canonical investigation-review.",
    )
    add_pretty_flag(promote_investigation_review)

    promote_all = sub.add_parser("promote-all", help="Promote derived matching, investigation-review, expert-report, and decision drafts into canonical paths.")
    promote_all.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    promote_all.add_argument("--round-id", required=True, help="Round identifier.")
    promote_all.add_argument("--allow-overwrite", action="store_true", help="Allow overwrite of existing canonical outputs.")
    add_pretty_flag(promote_all)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "build-curation-packets": command_build_curation_packets,
        "build-data-readiness-packets": command_build_data_readiness_packets,
        "build-matching-authorization-packet": command_build_matching_authorization_packet,
        "build-matching-adjudication-packet": command_build_matching_adjudication_packet,
        "build-investigation-review-packet": command_build_investigation_review_packet,
        "build-report-packets": command_build_report_packets,
        "build-decision-packet": command_build_decision_packet,
        "build-all": command_build_all,
        "render-openclaw-prompts": command_render_openclaw_prompts,
        "promote-report-draft": command_promote_report_draft,
        "promote-decision-draft": command_promote_decision_draft,
        "promote-matching-authorization-draft": command_promote_matching_authorization_draft,
        "promote-matching-adjudication-draft": command_promote_matching_adjudication_draft,
        "promote-investigation-review-draft": command_promote_investigation_review_draft,
        "promote-all": command_promote_all,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        result = {"command": args.command, "ok": False, "error": str(exc)}
        print(pretty_json(result, pretty=getattr(args, "pretty", False)))
        return 1

    result = {"command": args.command, "ok": True, "payload": payload}
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
