#!/usr/bin/env python3
"""Deterministic normalization pipeline for eco-council runs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import (
    atomic_write_text_file,
    load_canonical_list,
    pretty_json,
    read_json,
    read_jsonl,
    utc_now_iso,
    write_json,
    write_jsonl,
)
from eco_council_runtime.adapters.normalize_storage import (
    default_environment_db_path as adapter_default_environment_db_path,
    default_public_db_path as adapter_default_public_db_path,
    load_or_build_manifest as adapter_load_or_build_manifest,
    run_manifest_path as adapter_run_manifest_path,
    save_environment_db as adapter_save_environment_db,
    save_public_db as adapter_save_public_db,
)
from eco_council_runtime.application.normalize_sources import (
    NORMALIZE_CACHE_VERSION,
    normalize_cache_dir,
    normalize_environment_source as application_normalize_environment_source,
    normalize_environment_source_cached as application_normalize_environment_source_cached,
    normalize_public_source as application_normalize_public_source,
    normalize_public_source_cached as application_normalize_public_source_cached,
)
from eco_council_runtime.application.investigation import materialize_investigation_bundle
from eco_council_runtime.application.normalize_matching import (
    build_matching_adjudication_draft as application_build_matching_adjudication_draft,
    build_matching_candidate_set as application_build_matching_candidate_set,
)
from eco_council_runtime.application.normalize_candidates import (
    aggregate_stats as application_aggregate_stats,
    artifact_ref as application_artifact_ref,
    derive_place_scope as application_derive_place_scope,
    distribution_summary_from_environment_group as application_distribution_summary_from_environment_group,
    environment_signals_to_observations as application_environment_signals_to_observations,
    first_datetime_and_last as application_first_datetime_and_last,
    observation_group_compact_audit as application_observation_group_compact_audit,
    observation_group_key as application_observation_group_key,
    percentile as application_percentile,
    percentile95 as application_percentile95,
    public_group_compact_audit as application_public_group_compact_audit,
    public_signals_to_claims as application_public_signals_to_claims,
)
from eco_council_runtime.application.normalize_library import (
    active_library_list as application_active_library_list,
    aggregate_candidate_distribution_summary as application_aggregate_candidate_distribution_summary,
    aggregate_candidate_statistics as application_aggregate_candidate_statistics,
    append_library_events as application_append_library_events,
    auditable_submission_list as application_auditable_submission_list,
    build_compact_audit as application_build_compact_audit,
    canonical_source_skill_for_curated_observation as application_canonical_source_skill_for_curated_observation,
    candidate_claim_source_signal_count as application_candidate_claim_source_signal_count,
    claim_submission_from_claim as application_claim_submission_from_claim,
    compact_audit_from_curated_claim_candidates as application_compact_audit_from_curated_claim_candidates,
    compact_audit_from_curated_observation_candidates as application_compact_audit_from_curated_observation_candidates,
    consensus_nonempty_text as application_consensus_nonempty_text,
    day_bucket_from_time_window as application_day_bucket_from_time_window,
    default_distribution_summary_from_observation as application_default_distribution_summary_from_observation,
    dedupe_artifact_refs as application_dedupe_artifact_refs,
    derive_place_scope_from_candidate_observations as application_derive_place_scope_from_candidate_observations,
    distribution_signal_count as application_distribution_signal_count,
    effective_shared_claims as application_effective_shared_claims,
    effective_shared_observations as application_effective_shared_observations,
    enrich_component_roles_with_candidate_tags as application_enrich_component_roles_with_candidate_tags,
    hydrate_observation_submissions_with_observations as application_hydrate_observation_submissions_with_observations,
    library_state as application_library_state,
    load_object_if_exists as application_load_object_if_exists,
    materialize_claim_submission_from_curated_entry as application_materialize_claim_submission_from_curated_entry,
    materialize_curated_claims as application_materialize_curated_claims,
    materialize_curated_observations as application_materialize_curated_observations,
    materialize_observation_submission_from_curated_entry as application_materialize_observation_submission_from_curated_entry,
    materialize_shared_observation as application_materialize_shared_observation,
    merge_claim_submissions as application_merge_claim_submissions,
    merge_count_items as application_merge_count_items,
    merge_effective_claims as application_merge_effective_claims,
    merge_effective_observations as application_merge_effective_observations,
    merge_evidence_cards as application_merge_evidence_cards,
    merge_isolated_entries as application_merge_isolated_entries,
    merge_observation_submissions as application_merge_observation_submissions,
    merge_remand_entries as application_merge_remand_entries,
    merge_time_windows_from_records as application_merge_time_windows_from_records,
    merge_unique_items as application_merge_unique_items,
    observation_match_key as application_observation_match_key,
    observation_signature_payload as application_observation_signature_payload,
    observation_submission_from_observation as application_observation_submission_from_observation,
    observation_submission_id as application_observation_submission_id,
    point_bucket_from_scope as application_point_bucket_from_scope,
    point_bucket_key as application_point_bucket_key,
    previous_active_list as application_previous_active_list,
    previous_round_id as application_previous_round_id,
    shared_observation_id as application_shared_observation_id,
    sorted_counter_items as application_sorted_counter_items,
    top_counter_items as application_top_counter_items,
    top_counter_text as application_top_counter_text,
)
from eco_council_runtime.application.normalize_evidence import (
    build_evidence_adjudication as application_build_evidence_adjudication,
    build_evidence_cards_from_matches as application_build_evidence_cards_from_matches,
    build_isolated_entries as application_build_isolated_entries,
    build_matching_result as application_build_matching_result,
    build_remand_entries as application_build_remand_entries,
    build_round_snapshot as application_build_round_snapshot,
    link_claims_to_evidence as application_link_claims_to_evidence,
    match_claims_to_observations as application_match_claims_to_observations,
)
from eco_council_runtime.application.normalize_views import (
    build_environment_signal_summary as application_build_environment_signal_summary,
    build_public_signal_summary as application_build_public_signal_summary,
    claim_source_skills as application_claim_source_skills,
    claims_from_submissions as application_claims_from_submissions,
    compact_claim as application_compact_claim,
    compact_claim_submission as application_compact_claim_submission,
    compact_count_items as application_compact_count_items,
    compact_distribution_summary as application_compact_distribution_summary,
    compact_evidence_card as application_compact_evidence_card,
    compact_isolated_entry as application_compact_isolated_entry,
    compact_observation as application_compact_observation,
    compact_observation_submission as application_compact_observation_submission,
    compact_remand_entry as application_compact_remand_entry,
    compact_statistics as application_compact_statistics,
    compact_task as application_compact_task,
    maybe_nonnegative_int as application_maybe_nonnegative_int,
    observation_family_priority_index as application_observation_family_priority_index,
    observation_severity as application_observation_severity,
    observations_from_submissions as application_observations_from_submissions,
    ordered_context_observations as application_ordered_context_observations,
    representative_claim_submissions as application_representative_claim_submissions,
    representative_observation_order as application_representative_observation_order,
    representative_observation_submissions as application_representative_observation_submissions,
)
from eco_council_runtime.adapters.run_paths import load_mission
from eco_council_runtime.controller.audit_chain import ensure_audit_chain_ready, record_match_phase_receipt
from eco_council_runtime.controller.paths import (
    cards_active_path,
    claim_candidates_path,
    claim_curation_path,
    claim_submissions_path,
    claims_active_path,
    data_readiness_report_path,
    default_context_dir,
    evidence_adjudication_path,
    evidence_library_dir,
    evidence_library_ledger_path,
    investigation_plan_path,
    isolated_active_path,
    library_context_path,
    matching_adjudication_draft_path,
    matching_adjudication_path,
    matching_authorization_path,
    matching_candidate_set_path,
    matching_result_path,
    mission_path,
    moderator_derived_dir,
    observation_candidates_path,
    observation_curation_path,
    observation_submissions_path,
    observations_active_path,
    remands_open_path,
    role_normalized_dir,
    round_dir,
    round_dir_name as round_directory_name,
    shared_claims_path,
    shared_evidence_path,
    shared_observations_path,
)
from eco_council_runtime.domain.contract_bridge import (
    contract_call,
    effective_constraints as contract_effective_constraints,
    effective_matching_authorization,
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.normalize_semantics import (
    CLAIM_KEYWORDS,
    CLAIM_METRIC_RULES,
    DEFAULT_OBSERVATION_FAMILY_ORDER,
    ENVIRONMENT_METRIC_ALIASES,
    GENERIC_REGION_TOKENS,
    HYDROLOGY_METRICS,
    METEOROLOGY_METRICS,
    METRIC_FAMILY_GROUPS,
    NON_CLAIM_PUBLIC_SIGNAL_KINDS,
    PHYSICAL_CLAIM_TYPES,
    PHYSICAL_LEG_ORDER,
    POINT_MATCH_EPSILON_DEGREES,
    PRECIPITATION_METRICS,
    STOPWORDS,
    assess_claim_metric_value,
    best_physical_observation_hypothesis_id,
    bbox_scope_from_location_candidates,
    claim_priority_metric_families,
    compact_claim_scope,
    default_evidence_role_for_claim_metric,
    derive_public_claim_place_scope,
    derive_public_claim_time_window,
    effective_component_role_for_claim,
    extract_value_for_metric,
    geometry_to_bbox,
    infer_observation_investigation_tags,
    iter_observation_assessment_components,
    observation_metric_family,
    observation_overlaps_mission_scope,
    physical_investigation_leg_lookup,
    point_matches_geometry,
    public_signal_location_candidates,
    public_signal_mentions_mission_region,
    region_core_tokens,
    row_token_set,
    score_observation_for_investigation_leg,
    text_tokens,
)
from eco_council_runtime.domain.rounds import round_sort_key
from eco_council_runtime.domain.text import maybe_text, unique_strings
from eco_council_runtime.investigation import build_investigation_plan

SCHEMA_VERSION = resolve_schema_version("1.0.0")
MAX_CONTEXT_TASKS = 4
MAX_CONTEXT_CLAIMS = 4
MAX_CONTEXT_OBSERVATIONS = 8
MAX_CONTEXT_EVIDENCE = 4

maybe_nonnegative_int = application_maybe_nonnegative_int
point_bucket_key = application_point_bucket_key
compact_count_items = application_compact_count_items
compact_distribution_summary = application_compact_distribution_summary
compact_statistics = application_compact_statistics
previous_round_id = application_previous_round_id


def mission_run_id(mission: dict[str, Any]) -> str:
    run_id = mission.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("mission.json missing run_id.")
    return run_id


def mission_window(mission: dict[str, Any]) -> dict[str, str]:
    window = mission.get("window")
    if not isinstance(window, dict):
        raise ValueError("mission.json missing window.")
    start_utc = maybe_text(window.get("start_utc"))
    end_utc = maybe_text(window.get("end_utc"))
    if not start_utc or not end_utc:
        raise ValueError("mission.json window is incomplete.")
    return {"start_utc": start_utc, "end_utc": end_utc}


def mission_place_scope(mission: dict[str, Any]) -> dict[str, Any]:
    region = mission.get("region")
    if not isinstance(region, dict):
        raise ValueError("mission.json missing region.")
    label = maybe_text(region.get("label")) or "Mission region"
    geometry = region.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError("mission.json region.geometry must be an object.")
    return {"label": label, "geometry": geometry}


def mission_constraints(mission: dict[str, Any]) -> dict[str, int]:
    values = contract_effective_constraints(mission)
    if isinstance(values, dict):
        return {key: int(value) for key, value in values.items() if isinstance(value, int) and value > 0}
    constraints = mission.get("constraints")
    if not isinstance(constraints, dict):
        return {}
    output: dict[str, int] = {}
    for key in (
        "max_rounds",
        "max_claims_per_round",
        "max_tasks_per_round",
        "claim_target_per_round",
        "claim_hard_cap_per_round",
    ):
        value = constraints.get(key)
        if isinstance(value, int) and value > 0:
            output[key] = value
    return output


def mission_region_tokens(mission: dict[str, Any]) -> list[str]:
    return text_tokens(mission_place_scope(mission).get("label"), minimum_length=3)


def mission_topic_tokens(mission: dict[str, Any]) -> list[str]:
    ignored = set(mission_region_tokens(mission))
    values: list[Any] = [mission.get("topic"), mission.get("objective")]
    hypotheses = mission.get("hypotheses")
    if isinstance(hypotheses, list):
        values.extend(item for item in hypotheses if item is not None)
    tokens: list[str] = []
    seen: set[str] = set()
    for value in values:
        for token in text_tokens(value, minimum_length=4):
            if token in ignored or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
    return tokens


top_counter_items = application_top_counter_items
sorted_counter_items = application_sorted_counter_items
top_counter_text = application_top_counter_text


def default_public_db_path(run_dir: Path) -> Path:
    return adapter_default_public_db_path(run_dir)


def default_environment_db_path(run_dir: Path) -> Path:
    return adapter_default_environment_db_path(run_dir)


def run_manifest_path(run_dir: Path) -> Path:
    return adapter_run_manifest_path(run_dir)


def load_or_build_manifest(run_dir: Path, mission: dict[str, Any]) -> dict[str, Any]:
    return adapter_load_or_build_manifest(
        run_dir,
        run_id=mission_run_id(mission),
    )


def emit_row_id(prefix: str, index: int) -> str:
    return f"{prefix}-{index:03d}"


def parse_input_specs(values: list[str]) -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Invalid --input value {raw!r}. Use source-skill=/path/to/artifact.")
        source_skill, path_text = raw.split("=", 1)
        source_skill = source_skill.strip()
        path_text = path_text.strip()
        if not source_skill or not path_text:
            raise ValueError(f"Invalid --input value {raw!r}.")
        path = Path(path_text).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"Input artifact does not exist: {path}")
        parsed.append((source_skill, path))
    return parsed


consensus_nonempty_text = application_consensus_nonempty_text
enrich_component_roles_with_candidate_tags = application_enrich_component_roles_with_candidate_tags


def normalize_public_source(
    source_skill: str,
    path: Path,
    *,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    return application_normalize_public_source(
        source_skill,
        path,
        run_id=run_id,
        round_id=round_id,
        mission_scope=mission_place_scope(mission),
        mission_region_tokens=mission_region_tokens(mission),
        mission_topic_tokens=mission_topic_tokens(mission),
        mission_topic=maybe_text(mission.get("topic")),
    )


def normalize_public_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    mission: dict[str, Any],
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], str]:
    return application_normalize_public_source_cached(
        run_dir=run_dir,
        source_skill=source_skill,
        path=path,
        run_id=run_id,
        round_id=round_id,
        mission_scope=mission_place_scope(mission),
        mission_region_tokens=mission_region_tokens(mission),
        mission_topic_tokens=mission_topic_tokens(mission),
        mission_topic=maybe_text(mission.get("topic")),
    )


build_compact_audit = application_build_compact_audit
percentile = application_percentile
percentile95 = application_percentile95
artifact_ref = application_artifact_ref


def public_group_compact_audit(items: list[dict[str, Any]]) -> dict[str, Any]:
    return application_public_group_compact_audit(items)


def observation_group_compact_audit(
    group: list[dict[str, Any]],
    *,
    metric_override: str | None = None,
) -> dict[str, Any]:
    return application_observation_group_compact_audit(group, metric_override=metric_override)


def distribution_summary_from_environment_group(
    group: list[dict[str, Any]],
    *,
    metric_override: str | None = None,
) -> dict[str, Any]:
    return application_distribution_summary_from_environment_group(group, metric_override=metric_override)


def public_signals_to_claims(
    *,
    mission: dict[str, Any],
    round_id: str,
    signals: list[dict[str, Any]],
    max_claims: int,
) -> list[dict[str, Any]]:
    return application_public_signals_to_claims(
        run_id=mission_run_id(mission),
        round_id=round_id,
        signals=signals,
        max_claims=max_claims,
        mission_scope=mission_place_scope(mission),
        mission_time_window=mission_window(mission),
        investigation_plan=build_investigation_plan(mission=mission, round_id=round_id),
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def save_public_db(db_path: Path, signals: list[dict[str, Any]], claims: list[dict[str, Any]]) -> None:
    return adapter_save_public_db(db_path, signals, claims)


first_datetime_and_last = application_first_datetime_and_last
aggregate_stats = application_aggregate_stats


def normalize_environment_source(
    source_skill: str,
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return application_normalize_environment_source(
        source_skill,
        path,
        run_id=run_id,
        round_id=round_id,
        schema_version=SCHEMA_VERSION,
    )


def normalize_environment_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    return application_normalize_environment_source_cached(
        run_dir=run_dir,
        source_skill=source_skill,
        path=path,
        run_id=run_id,
        round_id=round_id,
        schema_version=SCHEMA_VERSION,
    )


observation_group_key = application_observation_group_key
derive_place_scope = application_derive_place_scope


def environment_signals_to_observations(
    *,
    mission: dict[str, Any],
    round_id: str,
    signals: list[dict[str, Any]],
    extra_observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return application_environment_signals_to_observations(
        run_id=mission_run_id(mission),
        round_id=round_id,
        signals=signals,
        extra_observations=extra_observations,
        mission_scope=mission_place_scope(mission),
        mission_time_window=mission_window(mission),
        investigation_plan=build_investigation_plan(mission=mission, round_id=round_id),
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def save_environment_db(db_path: Path, signals: list[dict[str, Any]], observations: list[dict[str, Any]]) -> None:
    return adapter_save_environment_db(db_path, signals, observations)


load_object_if_exists = application_load_object_if_exists
append_library_events = application_append_library_events
merge_unique_items = application_merge_unique_items
previous_active_list = application_previous_active_list
merge_claim_submissions = application_merge_claim_submissions
merge_observation_submissions = application_merge_observation_submissions
merge_evidence_cards = application_merge_evidence_cards
merge_isolated_entries = application_merge_isolated_entries
merge_remand_entries = application_merge_remand_entries
observation_signature_payload = application_observation_signature_payload
shared_observation_id = application_shared_observation_id
materialize_shared_observation = application_materialize_shared_observation
observation_submission_id = application_observation_submission_id
merge_effective_observations = application_merge_effective_observations
effective_shared_observations = application_effective_shared_observations
merge_effective_claims = application_merge_effective_claims
effective_shared_claims = application_effective_shared_claims
active_library_list = application_active_library_list
auditable_submission_list = application_auditable_submission_list
observation_match_key = application_observation_match_key
hydrate_observation_submissions_with_observations = application_hydrate_observation_submissions_with_observations
library_state = application_library_state


compact_task = application_compact_task
claim_source_skills = application_claim_source_skills
compact_claim = application_compact_claim
compact_observation = application_compact_observation
compact_evidence_card = application_compact_evidence_card
compact_claim_submission = application_compact_claim_submission
compact_observation_submission = application_compact_observation_submission
compact_isolated_entry = application_compact_isolated_entry
compact_remand_entry = application_compact_remand_entry
observation_severity = application_observation_severity
observation_family_priority_index = application_observation_family_priority_index
representative_observation_order = application_representative_observation_order
ordered_context_observations = application_ordered_context_observations
representative_claim_submissions = application_representative_claim_submissions


def representative_observation_submissions(submissions: list[dict[str, Any]], claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return application_representative_observation_submissions(
        submissions,
        claims,
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def build_public_signal_summary(signals: list[dict[str, Any]], claims: list[dict[str, Any]]) -> dict[str, Any]:
    return application_build_public_signal_summary(
        signals,
        claims,
        max_context_claims=MAX_CONTEXT_CLAIMS,
    )


def build_environment_signal_summary(signals: list[dict[str, Any]], observations: list[dict[str, Any]]) -> dict[str, Any]:
    return application_build_environment_signal_summary(
        signals,
        observations,
        max_context_observations=MAX_CONTEXT_OBSERVATIONS,
    )


def claim_submission_from_claim(claim: dict[str, Any]) -> dict[str, Any]:
    return application_claim_submission_from_claim(
        claim,
        schema_version=SCHEMA_VERSION,
        validate_payload=validate_payload,
    )


def observation_submission_from_observation(observation: dict[str, Any]) -> dict[str, Any]:
    return application_observation_submission_from_observation(
        observation,
        schema_version=SCHEMA_VERSION,
        validate_payload=validate_payload,
    )


def claims_from_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return application_claims_from_submissions(
        submissions,
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def observations_from_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return application_observations_from_submissions(
        submissions,
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


dedupe_artifact_refs = application_dedupe_artifact_refs
merge_time_windows_from_records = application_merge_time_windows_from_records
derive_place_scope_from_candidate_observations = application_derive_place_scope_from_candidate_observations
candidate_claim_source_signal_count = application_candidate_claim_source_signal_count
compact_audit_from_curated_claim_candidates = application_compact_audit_from_curated_claim_candidates
compact_audit_from_curated_observation_candidates = application_compact_audit_from_curated_observation_candidates
aggregate_candidate_statistics = application_aggregate_candidate_statistics
merge_count_items = application_merge_count_items
distribution_signal_count = application_distribution_signal_count
point_bucket_from_scope = application_point_bucket_from_scope
day_bucket_from_time_window = application_day_bucket_from_time_window
default_distribution_summary_from_observation = application_default_distribution_summary_from_observation
aggregate_candidate_distribution_summary = application_aggregate_candidate_distribution_summary
canonical_source_skill_for_curated_observation = application_canonical_source_skill_for_curated_observation


def materialize_claim_submission_from_curated_entry(
    *,
    entry: dict[str, Any],
    candidate_lookup: dict[str, dict[str, Any]],
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
) -> dict[str, Any]:
    return application_materialize_claim_submission_from_curated_entry(
        entry=entry,
        candidate_lookup=candidate_lookup,
        run_id=run_id,
        round_id=round_id,
        mission_scope=mission_scope,
        mission_time_window=mission_time_window,
        schema_version=SCHEMA_VERSION,
        validate_payload=validate_payload,
    )


def materialize_observation_submission_from_curated_entry(
    *,
    entry: dict[str, Any],
    candidate_lookup: dict[str, dict[str, Any]],
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
) -> dict[str, Any]:
    return application_materialize_observation_submission_from_curated_entry(
        entry=entry,
        candidate_lookup=candidate_lookup,
        run_id=run_id,
        round_id=round_id,
        mission_scope=mission_scope,
        mission_time_window=mission_time_window,
        schema_version=SCHEMA_VERSION,
        validate_payload=validate_payload,
    )


def materialize_curated_claims(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    pretty: bool,
) -> dict[str, Any]:
    return application_materialize_curated_claims(
        run_dir=run_dir,
        round_id=round_id,
        run_id=mission_run_id(mission),
        mission_scope=mission_place_scope(mission),
        mission_time_window=mission_window(mission),
        pretty=pretty,
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def materialize_curated_observations(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    pretty: bool,
) -> dict[str, Any]:
    return application_materialize_curated_observations(
        run_dir=run_dir,
        round_id=round_id,
        run_id=mission_run_id(mission),
        mission_scope=mission_place_scope(mission),
        mission_time_window=mission_window(mission),
        pretty=pretty,
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def match_claims_to_observations(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return application_match_claims_to_observations(
        claims=claims,
        observations=observations,
    )


def build_matching_result(
    *,
    authorization: dict[str, Any],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
) -> dict[str, Any]:
    return application_build_matching_result(
        authorization=authorization,
        claims=claims,
        observations=observations,
        matches=matches,
        schema_version=SCHEMA_VERSION,
        validate_payload=validate_payload,
    )


def build_isolated_entries(
    *,
    run_id: str,
    round_id: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    allow_isolated_evidence: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return application_build_isolated_entries(
        run_id=run_id,
        round_id=round_id,
        claims=claims,
        observations=observations,
        matches=matches,
        allow_isolated_evidence=allow_isolated_evidence,
        schema_version=SCHEMA_VERSION,
    )


def build_remand_entries(
    *,
    run_id: str,
    round_id: str,
    matches: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    allow_isolated_evidence: bool,
) -> list[dict[str, Any]]:
    return application_build_remand_entries(
        run_id=run_id,
        round_id=round_id,
        matches=matches,
        observations=observations,
        allow_isolated_evidence=allow_isolated_evidence,
        schema_version=SCHEMA_VERSION,
    )


def build_evidence_adjudication(
    *,
    authorization: dict[str, Any],
    matching_result: dict[str, Any],
    evidence_cards: list[dict[str, Any]],
    isolated_entries: list[dict[str, Any]],
    remands: list[dict[str, Any]],
) -> dict[str, Any]:
    return application_build_evidence_adjudication(
        authorization=authorization,
        matching_result=matching_result,
        evidence_cards=evidence_cards,
        isolated_entries=isolated_entries,
        remands=remands,
        schema_version=SCHEMA_VERSION,
        validate_payload=validate_payload,
    )


def build_evidence_cards_from_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return application_build_evidence_cards_from_matches(
        matches,
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def link_claims_to_evidence(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return application_link_claims_to_evidence(
        claims=claims,
        observations=observations,
        schema_version=SCHEMA_VERSION,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def build_matching_candidate_set(
    *,
    authorization: dict[str, Any],
    matches: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    return application_build_matching_candidate_set(
        authorization=authorization,
        matches=matches,
        observations=observations,
        schema_version=SCHEMA_VERSION,
        compact_claim=compact_claim,
        compact_observation=compact_observation,
    )


def build_matching_adjudication_draft(
    *,
    authorization: dict[str, Any],
    candidate_set: dict[str, Any],
    matching_result: dict[str, Any],
    evidence_cards: list[dict[str, Any]],
    isolated_entries: list[dict[str, Any]],
    remands: list[dict[str, Any]],
    evidence_adjudication: dict[str, Any],
) -> dict[str, Any]:
    return application_build_matching_adjudication_draft(
        authorization=authorization,
        candidate_set=candidate_set,
        matching_result=matching_result,
        evidence_cards=evidence_cards,
        isolated_entries=isolated_entries,
        remands=remands,
        evidence_adjudication=evidence_adjudication,
        schema_version=SCHEMA_VERSION,
        validate_payload=validate_payload,
    )


def build_round_snapshot(
    *,
    run_dir: Path,
    mission: dict[str, Any],
    round_id: str,
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    role: str,
) -> dict[str, Any]:
    state = library_state(run_dir, round_id)
    investigation_plan = load_object_if_exists(investigation_plan_path(run_dir, round_id)) or {}
    matching_authorization = state["matching_authorization"] if isinstance(state.get("matching_authorization"), dict) else {}
    if matching_authorization:
        matching_authorization = effective_matching_authorization(
            mission=mission,
            round_id=round_id,
            authorization=matching_authorization,
        )
    run_payload = {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "topic": maybe_text(mission.get("topic")),
        "objective": maybe_text(mission.get("objective")),
        "region": mission_place_scope(mission),
        "window": mission_window(mission),
        "role": role,
    }
    return application_build_round_snapshot(
        run_dir=run_dir,
        round_id=round_id,
        run=run_payload,
        tasks=tasks,
        claims=claims,
        observations=observations,
        evidence_cards=evidence_cards,
        role=role,
        state=state,
        investigation_plan=investigation_plan if isinstance(investigation_plan, dict) else {},
        matching_authorization=matching_authorization,
        max_context_tasks=MAX_CONTEXT_TASKS,
        max_context_claims=MAX_CONTEXT_CLAIMS,
        max_context_observations=MAX_CONTEXT_OBSERVATIONS,
        max_context_evidence=MAX_CONTEXT_EVIDENCE,
    )


def command_init_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    public_db = Path(args.public_db).expanduser().resolve() if args.public_db else default_public_db_path(run_dir_path)
    environment_db = (
        Path(args.environment_db).expanduser().resolve()
        if args.environment_db
        else default_environment_db_path(run_dir_path)
    )
    save_public_db(public_db, [], [])
    save_environment_db(environment_db, [], [])

    for role in ("moderator", "sociologist", "environmentalist"):
        default_context_dir(run_dir_path, args.round_id, role).mkdir(parents=True, exist_ok=True)
    (round_dir(run_dir_path, args.round_id) / "shared" / "contexts").mkdir(parents=True, exist_ok=True)
    evidence_library_dir(run_dir_path, args.round_id).mkdir(parents=True, exist_ok=True)
    for path, payload in (
        (claims_active_path(run_dir_path, args.round_id), []),
        (observations_active_path(run_dir_path, args.round_id), []),
        (cards_active_path(run_dir_path, args.round_id), []),
        (isolated_active_path(run_dir_path, args.round_id), []),
        (remands_open_path(run_dir_path, args.round_id), []),
    ):
        if not path.exists():
            write_json(path, payload, pretty=args.pretty)
    if not evidence_library_ledger_path(run_dir_path, args.round_id).exists():
        atomic_write_text_file(evidence_library_ledger_path(run_dir_path, args.round_id), "")
    ensure_audit_chain_ready(run_dir_path, args.round_id)

    manifest = load_or_build_manifest(run_dir_path, mission)
    manifest["round_id_initialized"] = args.round_id
    manifest["databases"] = {
        "public_signals": str(public_db),
        "environment_signals": str(environment_db),
    }
    manifest["normalization_cache"] = {
        "version": NORMALIZE_CACHE_VERSION,
        "directory": str(normalize_cache_dir(run_dir_path)),
    }
    manifest["initialized_at_utc"] = utc_now_iso()
    write_json(run_manifest_path(run_dir_path), manifest, pretty=args.pretty)
    investigation_bundle = materialize_investigation_bundle(run_dir_path, args.round_id, pretty=args.pretty)

    return {
        "run_dir": str(run_dir_path),
        "public_db": str(public_db),
        "environment_db": str(environment_db),
        "manifest_path": str(run_manifest_path(run_dir_path)),
        **investigation_bundle,
    }


def command_normalize_public(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    run_id = mission_run_id(mission)
    constraints = mission_constraints(mission)
    public_db = Path(args.public_db).expanduser().resolve() if args.public_db else default_public_db_path(run_dir_path)
    inputs = parse_input_specs(args.input)
    all_signals: list[dict[str, Any]] = []
    cache_hits = 0
    cache_misses = 0
    for source_skill, path in inputs:
        signals, cache_status = normalize_public_source_cached(
            run_dir=run_dir_path,
            source_skill=source_skill,
            path=path,
            mission=mission,
            run_id=run_id,
            round_id=args.round_id,
        )
        all_signals.extend(signals)
        if cache_status == "hit":
            cache_hits += 1
        else:
            cache_misses += 1

    deduped_by_id: dict[str, dict[str, Any]] = {signal["signal_id"]: signal for signal in all_signals}
    signals = sorted(
        deduped_by_id.values(),
        key=lambda item: (
            item.get("published_at_utc") or "",
            item["signal_id"],
        ),
        reverse=False,
    )
    candidate_limit = max(
        1,
        int(args.max_claims or 0),
        int(constraints.get("claim_hard_cap_per_round") or 0),
        int(constraints.get("max_claims_per_round") or 0) * 3,
    )
    claims = public_signals_to_claims(
        mission=mission,
        round_id=args.round_id,
        signals=signals,
        max_claims=candidate_limit,
    )

    save_public_db(public_db, signals, claims)
    normalized_dir = role_normalized_dir(run_dir_path, args.round_id, "sociologist")
    public_signals_file = normalized_dir / "public_signals.jsonl"
    claims_file = claim_candidates_path(run_dir_path, args.round_id)
    summary_file = normalized_dir / "public_signal_summary.json"
    write_jsonl(public_signals_file, signals)
    write_json(claims_file, claims, pretty=args.pretty)
    write_json(summary_file, build_public_signal_summary(signals, claims), pretty=args.pretty)
    write_json(claim_submissions_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(claims_active_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(shared_claims_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    investigation_bundle = materialize_investigation_bundle(run_dir_path, args.round_id, pretty=args.pretty)

    return {
        "public_db": str(public_db),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "signal_count": len(signals),
        "claim_candidate_count": len(claims),
        "signals_path": str(public_signals_file),
        "signal_summary_path": str(summary_file),
        "claim_candidates_path": str(claims_file),
        **investigation_bundle,
    }


def command_normalize_environment(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    run_id = mission_run_id(mission)
    environment_db = (
        Path(args.environment_db).expanduser().resolve()
        if args.environment_db
        else default_environment_db_path(run_dir_path)
    )
    inputs = parse_input_specs(args.input)
    all_signals: list[dict[str, Any]] = []
    extra_observations: list[dict[str, Any]] = []
    cache_hits = 0
    cache_misses = 0
    for source_skill, path in inputs:
        source_signals, source_observations, cache_status = normalize_environment_source_cached(
            run_dir=run_dir_path,
            source_skill=source_skill,
            path=path,
            run_id=run_id,
            round_id=args.round_id,
        )
        all_signals.extend(source_signals)
        extra_observations.extend(source_observations)
        if cache_status == "hit":
            cache_hits += 1
        else:
            cache_misses += 1

    deduped_by_id: dict[str, dict[str, Any]] = {signal["signal_id"]: signal for signal in all_signals}
    signals = sorted(deduped_by_id.values(), key=lambda item: (item.get("metric") or "", item["signal_id"]))
    observations = environment_signals_to_observations(
        mission=mission,
        round_id=args.round_id,
        signals=signals,
        extra_observations=extra_observations,
    )

    save_environment_db(environment_db, signals, observations)
    normalized_dir = role_normalized_dir(run_dir_path, args.round_id, "environmentalist")
    signals_file = normalized_dir / "environment_signals.jsonl"
    observations_file = observation_candidates_path(run_dir_path, args.round_id)
    summary_file = normalized_dir / "environment_signal_summary.json"
    write_jsonl(signals_file, signals)
    write_json(observations_file, observations, pretty=args.pretty)
    write_json(summary_file, build_environment_signal_summary(signals, observations), pretty=args.pretty)
    write_json(observation_submissions_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(observations_active_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    write_json(shared_observations_path(run_dir_path, args.round_id), [], pretty=args.pretty)
    investigation_bundle = materialize_investigation_bundle(run_dir_path, args.round_id, pretty=args.pretty)

    return {
        "environment_db": str(environment_db),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "signal_count": len(signals),
        "observation_candidate_count": len(observations),
        "signals_path": str(signals_file),
        "signal_summary_path": str(summary_file),
        "observation_candidates_path": str(observations_file),
        **investigation_bundle,
    }


def command_materialize_curations(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission = load_mission(run_dir_path)
    claim_result = materialize_curated_claims(
        run_dir=run_dir_path,
        round_id=args.round_id,
        mission=mission,
        pretty=args.pretty,
    )
    observation_result = materialize_curated_observations(
        run_dir=run_dir_path,
        round_id=args.round_id,
        mission=mission,
        pretty=args.pretty,
    )
    investigation_bundle = materialize_investigation_bundle(run_dir_path, args.round_id, pretty=args.pretty)
    return {
        "run_id": mission_run_id(mission),
        "round_id": args.round_id,
        "claim_materialization": claim_result,
        "observation_materialization": observation_result,
        **investigation_bundle,
    }


def authorized_matching_inputs(
    *,
    run_dir_path: Path,
    round_id: str,
    authorization_input: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    mission = load_mission(run_dir_path)
    input_path = (
        Path(authorization_input).expanduser().resolve()
        if maybe_text(authorization_input)
        else matching_authorization_path(run_dir_path, round_id)
    )
    authorization = load_object_if_exists(input_path)
    if authorization is None:
        raise ValueError(f"Matching authorization is missing or invalid: {input_path}")
    validate_payload("matching-authorization", authorization)
    effective_authorization = effective_matching_authorization(
        mission=mission,
        round_id=round_id,
        authorization=authorization,
    )
    validate_payload("matching-authorization", effective_authorization)
    if maybe_text(effective_authorization.get("authorization_status")) != "authorized":
        raise ValueError("Matching authorization exists but does not authorize matching.")
    claims = effective_shared_claims(run_dir_path, round_id)
    observations = effective_shared_observations(run_dir_path, round_id)
    raw_observation_submissions = load_canonical_list(observation_submissions_path(run_dir_path, round_id))
    raw_observations_active = active_library_list(run_dir_path, round_id, observations_active_path)
    hydrated_observation_submissions = hydrate_observation_submissions_with_observations(raw_observation_submissions, observations)
    hydrated_observations_active = hydrate_observation_submissions_with_observations(raw_observations_active, observations)
    observation_id_aliases: dict[str, str] = {}
    for raw_items, hydrated_items in (
        (raw_observation_submissions, hydrated_observation_submissions),
        (raw_observations_active, hydrated_observations_active),
    ):
        for raw_item, hydrated_item in zip(raw_items, hydrated_items):
            if not isinstance(raw_item, dict) or not isinstance(hydrated_item, dict):
                continue
            raw_observation_id = maybe_text(raw_item.get("observation_id"))
            hydrated_observation_id = maybe_text(hydrated_item.get("observation_id"))
            if raw_observation_id and hydrated_observation_id:
                observation_id_aliases[raw_observation_id] = hydrated_observation_id
    authorized_claim_ids = {
        maybe_text(item)
        for item in effective_authorization.get("claim_ids", [])
        if maybe_text(item)
    }
    authorized_observation_ids = {
        observation_id_aliases.get(maybe_text(item), maybe_text(item))
        for item in effective_authorization.get("observation_ids", [])
        if maybe_text(item)
    }
    filtered_claims = [item for item in claims if not authorized_claim_ids or maybe_text(item.get("claim_id")) in authorized_claim_ids]
    filtered_observations = [
        item
        for item in observations
        if not authorized_observation_ids or maybe_text(item.get("observation_id")) in authorized_observation_ids
    ]
    return mission, effective_authorization, filtered_claims, filtered_observations


def command_prepare_matching_adjudication(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    mission, effective_authorization, filtered_claims, filtered_observations = authorized_matching_inputs(
        run_dir_path=run_dir_path,
        round_id=args.round_id,
        authorization_input=maybe_text(args.authorization_input),
    )
    matches = match_claims_to_observations(claims=filtered_claims, observations=filtered_observations)
    evidence_cards = build_evidence_cards_from_matches(matches)
    matching_result = build_matching_result(
        authorization=effective_authorization,
        claims=filtered_claims,
        observations=filtered_observations,
        matches=matches,
    )
    allow_isolated_evidence = bool(effective_authorization.get("allow_isolated_evidence"))
    isolated_entries, _unused = build_isolated_entries(
        run_id=maybe_text(effective_authorization.get("run_id")) or mission_run_id(mission),
        round_id=args.round_id,
        claims=filtered_claims,
        observations=filtered_observations,
        matches=matches,
        allow_isolated_evidence=allow_isolated_evidence,
    )
    remands = build_remand_entries(
        run_id=maybe_text(effective_authorization.get("run_id")) or mission_run_id(mission),
        round_id=args.round_id,
        matches=matches,
        observations=filtered_observations,
        allow_isolated_evidence=allow_isolated_evidence,
    )
    validate_payload("isolated-entry", isolated_entries)
    validate_payload("remand-entry", remands)
    adjudication = build_evidence_adjudication(
        authorization=effective_authorization,
        matching_result=matching_result,
        evidence_cards=evidence_cards,
        isolated_entries=isolated_entries,
        remands=remands,
    )
    candidate_set = build_matching_candidate_set(
        authorization=effective_authorization,
        matches=matches,
        observations=filtered_observations,
    )
    draft = build_matching_adjudication_draft(
        authorization=effective_authorization,
        candidate_set=candidate_set,
        matching_result=matching_result,
        evidence_cards=evidence_cards,
        isolated_entries=isolated_entries,
        remands=remands,
        evidence_adjudication=adjudication,
    )
    candidate_path = matching_candidate_set_path(run_dir_path, args.round_id)
    draft_path = matching_adjudication_draft_path(run_dir_path, args.round_id)
    write_json(candidate_path, candidate_set, pretty=args.pretty)
    write_json(draft_path, draft, pretty=args.pretty)

    return {
        "run_id": mission_run_id(mission),
        "round_id": args.round_id,
        "candidate_set_path": str(candidate_path),
        "matching_adjudication_draft_path": str(draft_path),
        "claim_count": len(filtered_claims),
        "observation_count": len(filtered_observations),
        "evidence_count": len(evidence_cards),
        "isolated_count": len(isolated_entries),
        "remand_count": len(remands),
    }


def command_apply_matching_adjudication(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    _mission, effective_authorization, _filtered_claims, _filtered_observations = authorized_matching_inputs(
        run_dir_path=run_dir_path,
        round_id=args.round_id,
        authorization_input="",
    )
    input_path = (
        Path(args.adjudication_input).expanduser().resolve()
        if maybe_text(args.adjudication_input)
        else matching_adjudication_path(run_dir_path, args.round_id)
    )
    payload = load_object_if_exists(input_path)
    if payload is None:
        raise ValueError(f"Matching adjudication is missing or invalid: {input_path}")
    validate_payload("matching-adjudication", payload)
    if maybe_text(payload.get("run_id")) != maybe_text(effective_authorization.get("run_id")):
        raise ValueError("Matching adjudication run_id does not match the authorized run.")
    if maybe_text(payload.get("round_id")) != maybe_text(effective_authorization.get("round_id")):
        raise ValueError("Matching adjudication round_id does not match the authorized round.")
    if maybe_text(payload.get("authorization_id")) != maybe_text(effective_authorization.get("authorization_id")):
        raise ValueError("Matching adjudication authorization_id does not match matching_authorization.json.")
    write_json(matching_adjudication_path(run_dir_path, args.round_id), payload, pretty=args.pretty)
    matching_result = payload.get("matching_result", {}) if isinstance(payload.get("matching_result"), dict) else {}
    evidence_cards = payload.get("evidence_cards", []) if isinstance(payload.get("evidence_cards"), list) else []
    isolated_entries = payload.get("isolated_entries", []) if isinstance(payload.get("isolated_entries"), list) else []
    remands = payload.get("remand_entries", []) if isinstance(payload.get("remand_entries"), list) else []
    evidence_adjudication = payload.get("evidence_adjudication", {}) if isinstance(payload.get("evidence_adjudication"), dict) else {}

    write_json(shared_evidence_path(run_dir_path, args.round_id), evidence_cards, pretty=args.pretty)
    write_json(matching_result_path(run_dir_path, args.round_id), matching_result, pretty=args.pretty)
    write_json(evidence_adjudication_path(run_dir_path, args.round_id), evidence_adjudication, pretty=args.pretty)
    write_json(
        cards_active_path(run_dir_path, args.round_id),
        merge_evidence_cards(previous_active_list(run_dir_path, args.round_id, cards_active_path), evidence_cards),
        pretty=args.pretty,
    )
    write_json(
        isolated_active_path(run_dir_path, args.round_id),
        merge_isolated_entries(previous_active_list(run_dir_path, args.round_id, isolated_active_path), isolated_entries),
        pretty=args.pretty,
    )
    write_json(
        remands_open_path(run_dir_path, args.round_id),
        merge_remand_entries(previous_active_list(run_dir_path, args.round_id, remands_open_path), remands),
        pretty=args.pretty,
    )
    append_library_events(
        run_dir_path,
        args.round_id,
        [
            {"object_kind": "matching-result", "payload": matching_result},
            {"object_kind": "evidence-adjudication", "payload": evidence_adjudication},
        ],
    )
    investigation_bundle = materialize_investigation_bundle(run_dir_path, args.round_id, pretty=args.pretty)
    record_match_phase_receipt(
        run_dir=run_dir_path,
        round_id=args.round_id,
        evidence_count=len(evidence_cards),
        isolated_count=len(isolated_entries),
        remand_count=len(remands),
    )

    return {
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "evidence_count": len(evidence_cards),
        "isolated_count": len(isolated_entries),
        "remand_count": len(remands),
        "shared_evidence_path": str(shared_evidence_path(run_dir_path, args.round_id)),
        "matching_result_path": str(matching_result_path(run_dir_path, args.round_id)),
        "evidence_adjudication_path": str(evidence_adjudication_path(run_dir_path, args.round_id)),
        **investigation_bundle,
    }


def command_link_evidence(args: argparse.Namespace) -> dict[str, Any]:
    if not hasattr(args, "adjudication_input"):
        setattr(args, "adjudication_input", "")
    return command_apply_matching_adjudication(args)


def command_build_round_context(args: argparse.Namespace) -> dict[str, Any]:
    run_dir_path = Path(args.run_dir).expanduser().resolve()
    investigation_bundle = materialize_investigation_bundle(run_dir_path, args.round_id, pretty=args.pretty)
    mission = load_mission(run_dir_path)
    tasks_path = round_dir(run_dir_path, args.round_id) / "moderator" / "tasks.json"
    tasks = load_canonical_list(tasks_path)
    claims = effective_shared_claims(run_dir_path, args.round_id)
    observations = effective_shared_observations(run_dir_path, args.round_id)
    evidence_cards = load_canonical_list(shared_evidence_path(run_dir_path, args.round_id))

    outputs: dict[str, str] = {}
    for role in ("moderator", "sociologist", "environmentalist"):
        payload = build_round_snapshot(
            run_dir=run_dir_path,
            mission=mission,
            round_id=args.round_id,
            tasks=tasks,
            claims=claims,
            observations=observations,
            evidence_cards=evidence_cards,
            role=role,
        )
        context_path = default_context_dir(run_dir_path, args.round_id, role) / f"context_{role}.json"
        write_json(context_path, payload, pretty=args.pretty)
        write_json(library_context_path(run_dir_path, args.round_id, role), payload, pretty=args.pretty)
        outputs[role] = str(context_path)

    snapshot = build_round_snapshot(
        run_dir=run_dir_path,
        mission=mission,
        round_id=args.round_id,
        tasks=tasks,
        claims=claims,
        observations=observations,
        evidence_cards=evidence_cards,
        role="moderator",
    )
    shared_snapshot_path = round_dir(run_dir_path, args.round_id) / "shared" / "contexts" / "round_snapshot.json"
    write_json(shared_snapshot_path, snapshot, pretty=args.pretty)
    outputs["shared_snapshot"] = str(shared_snapshot_path)

    return {
        "claim_count": len(claims),
        "observation_count": len(observations),
        "evidence_count": len(evidence_cards),
        "cards_active_count": len(library_state(run_dir_path, args.round_id)["cards_active"]),
        "outputs": outputs,
        **investigation_bundle,
    }


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic normalization pipeline for eco-council runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_run = sub.add_parser("init-run", help="Initialize normalization databases and derived directories.")
    init_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    init_run.add_argument("--round-id", default="round-001", help="Round identifier.")
    init_run.add_argument("--public-db", default="", help="Override public-signals SQLite path.")
    init_run.add_argument("--environment-db", default="", help="Override environment-signals SQLite path.")
    add_pretty_flag(init_run)

    normalize_public = sub.add_parser("normalize-public", help="Normalize sociologist-side raw artifacts.")
    normalize_public.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    normalize_public.add_argument("--round-id", required=True, help="Round identifier.")
    normalize_public.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input artifact in source-skill=/path form. Repeat for multiple artifacts.",
    )
    normalize_public.add_argument("--public-db", default="", help="Override public-signals SQLite path.")
    normalize_public.add_argument("--max-claims", type=int, default=12, help="Maximum claim candidates to emit before curation.")
    add_pretty_flag(normalize_public)

    normalize_environment = sub.add_parser("normalize-environment", help="Normalize environment raw artifacts.")
    normalize_environment.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    normalize_environment.add_argument("--round-id", required=True, help="Round identifier.")
    normalize_environment.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input artifact in source-skill=/path form. Repeat for multiple artifacts.",
    )
    normalize_environment.add_argument("--environment-db", default="", help="Override environment-signals SQLite path.")
    add_pretty_flag(normalize_environment)

    materialize_curations = sub.add_parser(
        "materialize-curations",
        help="Materialize curated claims and observations into canonical submission and library files.",
    )
    materialize_curations.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    materialize_curations.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(materialize_curations)

    prepare_matching = sub.add_parser(
        "prepare-matching-adjudication",
        help="Build rule-nominated matching candidates plus a moderator adjudication draft.",
    )
    prepare_matching.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    prepare_matching.add_argument("--round-id", required=True, help="Round identifier.")
    prepare_matching.add_argument(
        "--authorization-input",
        default="",
        help="Optional matching-authorization JSON path. Defaults to the canonical moderator path for the round.",
    )
    add_pretty_flag(prepare_matching)

    apply_matching = sub.add_parser(
        "apply-matching-adjudication",
        help="Materialize a moderator-approved matching-adjudication payload into shared evidence artifacts.",
    )
    apply_matching.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    apply_matching.add_argument("--round-id", required=True, help="Round identifier.")
    apply_matching.add_argument(
        "--adjudication-input",
        default="",
        help="Optional matching-adjudication JSON path. Defaults to the canonical moderator path for the round.",
    )
    add_pretty_flag(apply_matching)

    link_evidence = sub.add_parser("link-evidence", help="Deprecated alias for apply-matching-adjudication.")
    link_evidence.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    link_evidence.add_argument("--round-id", required=True, help="Round identifier.")
    link_evidence.add_argument(
        "--adjudication-input",
        default="",
        help="Optional matching-adjudication JSON path. Defaults to the canonical moderator path for the round.",
    )
    add_pretty_flag(link_evidence)

    build_context = sub.add_parser("build-round-context", help="Build role-specific round context payloads.")
    build_context.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    build_context.add_argument("--round-id", required=True, help="Round identifier.")
    add_pretty_flag(build_context)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-run": command_init_run,
        "normalize-public": command_normalize_public,
        "normalize-environment": command_normalize_environment,
        "materialize-curations": command_materialize_curations,
        "prepare-matching-adjudication": command_prepare_matching_adjudication,
        "apply-matching-adjudication": command_apply_matching_adjudication,
        "link-evidence": command_link_evidence,
        "build-round-context": command_build_round_context,
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
