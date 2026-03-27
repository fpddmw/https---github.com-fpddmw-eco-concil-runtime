"""Application services for normalize library state and curated materialization."""

from __future__ import annotations

import copy
import math
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.adapters.filesystem import (
    load_canonical_list,
    load_json_if_exists,
    read_jsonl,
    stable_hash,
    stable_json,
    utc_now_iso,
    write_json,
    write_jsonl,
)
from eco_council_runtime.adapters.run_paths import discover_round_ids
from eco_council_runtime.application.normalize_views import (
    claims_from_submissions,
    compact_statistics,
    maybe_nonnegative_int,
    observations_from_submissions,
)
from eco_council_runtime.controller.paths import (
    cards_active_path,
    claim_candidates_path,
    claim_curation_path,
    claim_submissions_path,
    claims_active_path,
    data_readiness_report_path,
    evidence_adjudication_path,
    evidence_library_ledger_path,
    isolated_active_path,
    matching_authorization_path,
    matching_result_path,
    observation_candidates_path,
    observation_curation_path,
    observation_submissions_path,
    observations_active_path,
    remands_open_path,
    round_dir_name,
    shared_claims_path,
    shared_observations_path,
)
from eco_council_runtime.domain.normalize_semantics import (
    canonical_environment_metric,
    maybe_number,
    parse_loose_datetime,
    to_rfc3339_z,
)
from eco_council_runtime.domain.rounds import parse_round_components
from eco_council_runtime.domain.text import maybe_text, unique_strings

IdEmitter = Callable[[str, int], str]
PathBuilder = Callable[[Path, str], Path]
PayloadValidator = Callable[[str, dict[str, Any]], Any]


def _noop_validate_payload(_kind: str, _payload: dict[str, Any]) -> None:
    return None


def point_bucket_key(latitude: Any, longitude: Any) -> str:
    lat = maybe_number(latitude)
    lon = maybe_number(longitude)
    if lat is None or lon is None:
        return ""
    return f"{lat:.3f},{lon:.3f}"


def top_counter_items(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
    return items


def sorted_counter_items(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key in sorted(counter):
        count = counter.get(key, 0)
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
        if len(items) >= limit:
            break
    return items


def top_counter_text(counter: Counter[str], limit: int = 3) -> str:
    parts = [f"{item['value']} ({item['count']})" for item in top_counter_items(counter, limit=limit)]
    return ", ".join(parts)


def previous_round_id(run_dir: Path, round_id: str) -> str | None:
    components = parse_round_components(round_id)
    if components is None:
        candidates = [item for item in discover_round_ids(run_dir) if item < round_id]
        return candidates[-1] if candidates else None
    prefix, number, _width = components
    candidates: list[str] = []
    for item in discover_round_ids(run_dir):
        item_components = parse_round_components(item)
        if item_components is None:
            continue
        item_prefix, item_number, _item_width = item_components
        if item_prefix == prefix and item_number < number:
            candidates.append(item)
    return candidates[-1] if candidates else None


def load_object_if_exists(path: Path) -> dict[str, Any] | None:
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return payload
    return None


def append_library_events(run_dir: Path, round_id: str, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    ledger_path = evidence_library_ledger_path(run_dir, round_id)
    existing = read_jsonl(ledger_path) if ledger_path.exists() else []
    entries = [item for item in existing if isinstance(item, dict)]
    for event in events:
        object_kind = maybe_text(event.get("object_kind"))
        payload = event.get("payload")
        if not object_kind or not isinstance(payload, dict):
            continue
        entries.append(
            {
                "recorded_at_utc": utc_now_iso(),
                "object_kind": object_kind,
                "payload": payload,
            }
        )
    write_jsonl(ledger_path, entries)


def consensus_nonempty_text(values: list[Any]) -> str:
    unique = unique_strings([maybe_text(value) for value in values if maybe_text(value)])
    if len(unique) == 1:
        return unique[0]
    return ""


def enrich_component_roles_with_candidate_tags(
    component_roles: list[dict[str, Any]],
    *,
    candidate_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for component in component_roles:
        if not isinstance(component, dict):
            continue
        item = copy.deepcopy(component)
        candidate = candidate_lookup.get(maybe_text(component.get("candidate_observation_id")), {})
        if isinstance(candidate, dict):
            if not maybe_text(item.get("hypothesis_id")) and maybe_text(candidate.get("hypothesis_id")):
                item["hypothesis_id"] = maybe_text(candidate.get("hypothesis_id"))
            if not maybe_text(item.get("leg_id")) and maybe_text(candidate.get("leg_id")):
                item["leg_id"] = maybe_text(candidate.get("leg_id"))
        enriched.append(item)
    return enriched


def build_compact_audit(
    *,
    total_candidate_count: int,
    retained_count: int,
    coverage_summary: str,
    coverage_dimensions: list[str] | None = None,
    missing_dimensions: list[str] | None = None,
    concentration_flags: list[str] | None = None,
    sampling_notes: list[str] | None = None,
) -> dict[str, Any]:
    normalized_coverage_dimensions = unique_strings(
        [maybe_text(item) for item in (coverage_dimensions or []) if maybe_text(item)]
    )
    normalized_missing_dimensions = unique_strings(
        [maybe_text(item) for item in (missing_dimensions or []) if maybe_text(item)]
    )
    normalized_concentration_flags = unique_strings(
        [maybe_text(item) for item in (concentration_flags or []) if maybe_text(item)]
    )
    return {
        "representative": not normalized_concentration_flags and not normalized_missing_dimensions,
        "retained_count": max(0, int(retained_count)),
        "total_candidate_count": max(0, int(total_candidate_count)),
        "coverage_summary": coverage_summary,
        "concentration_flags": normalized_concentration_flags,
        "coverage_dimensions": normalized_coverage_dimensions,
        "missing_dimensions": normalized_missing_dimensions,
        "sampling_notes": unique_strings([maybe_text(item) for item in (sampling_notes or []) if maybe_text(item)]),
    }


def merge_unique_items(*groups: list[dict[str, Any]], key_fn: Any) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for group in groups:
        for item in group:
            if not isinstance(item, dict):
                continue
            key = maybe_text(key_fn(item))
            if not key:
                key = stable_hash(stable_json(item))
            if key not in merged:
                ordered_keys.append(key)
            merged[key] = item
    return [merged[key] for key in ordered_keys]


def previous_active_list(run_dir: Path, round_id: str, path_fn: PathBuilder) -> list[dict[str, Any]]:
    prior_round = previous_round_id(run_dir, round_id)
    if prior_round is None:
        return []
    return load_canonical_list(path_fn(run_dir, prior_round))


def merge_claim_submissions(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("submission_id") or item.get("claim_id"))


def observation_signature_payload(observation: dict[str, Any]) -> dict[str, Any]:
    provenance = observation.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    return {
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "observation_mode": maybe_text(observation.get("observation_mode")),
        "evidence_role": maybe_text(observation.get("evidence_role")),
        "hypothesis_id": maybe_text(observation.get("hypothesis_id")),
        "leg_id": maybe_text(observation.get("leg_id")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": observation.get("statistics"),
        "distribution_summary": observation.get("distribution_summary"),
        "time_window": observation.get("time_window"),
        "place_scope": observation.get("place_scope"),
        "source_skills": sorted(maybe_text(item) for item in observation.get("source_skills", []) if maybe_text(item)),
        "metric_bundle": sorted(maybe_text(item) for item in observation.get("metric_bundle", []) if maybe_text(item)),
        "candidate_observation_ids": sorted(
            maybe_text(item) for item in observation.get("candidate_observation_ids", []) if maybe_text(item)
        ),
        "provenance_refs": sorted(
            stable_hash(stable_json(item))
            for item in observation.get("provenance_refs", [])
            if isinstance(item, dict)
        ),
        "component_roles": observation.get("component_roles"),
        "quality_flags": sorted(
            maybe_text(item) for item in observation.get("quality_flags", []) if maybe_text(item)
        ),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }


def merge_observation_submissions(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(
        *groups,
        key_fn=lambda item: stable_hash(stable_json(observation_signature_payload(item))),
    )


def merge_evidence_cards(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("evidence_id"))


def merge_isolated_entries(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("isolated_id"))


def merge_remand_entries(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*groups, key_fn=lambda item: item.get("remand_id"))


def shared_observation_id(observation: dict[str, Any]) -> str:
    signature = stable_hash(stable_json(observation_signature_payload(observation)))
    return f"obs-{signature[:12]}"


def materialize_shared_observation(observation: dict[str, Any]) -> dict[str, Any]:
    item = dict(observation)
    item["observation_id"] = shared_observation_id(observation)
    return item


def observation_submission_id(observation_id: str) -> str:
    return f"obssub-{maybe_text(observation_id)}"


def merge_effective_observations(*observation_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged_by_signature: dict[str, dict[str, Any]] = {}
    ordered_signatures: list[str] = []
    for group in observation_groups:
        for observation in group:
            signature_payload = observation_signature_payload(observation)
            signature = stable_hash(stable_json(signature_payload))
            if signature not in merged_by_signature:
                ordered_signatures.append(signature)
            merged_by_signature[signature] = materialize_shared_observation(observation)
    return [merged_by_signature[signature] for signature in ordered_signatures]


def effective_shared_observations(
    run_dir: Path,
    round_id: str,
    *,
    current_round_observations: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    inherited: list[dict[str, Any]] = []
    prior_round = previous_round_id(run_dir, round_id)
    if prior_round is not None:
        inherited = effective_shared_observations(run_dir, prior_round)
    current = (
        current_round_observations
        if current_round_observations is not None
        else load_canonical_list(shared_observations_path(run_dir, round_id))
    )
    return merge_effective_observations(inherited, current)


def merge_effective_claims(*claim_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_unique_items(*claim_groups, key_fn=lambda item: item.get("claim_id"))


def effective_shared_claims(
    run_dir: Path,
    round_id: str,
    *,
    current_round_claims: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    inherited: list[dict[str, Any]] = []
    prior_round = previous_round_id(run_dir, round_id)
    if prior_round is not None:
        inherited = effective_shared_claims(run_dir, prior_round)
    current = (
        current_round_claims
        if current_round_claims is not None
        else load_canonical_list(shared_claims_path(run_dir, round_id))
    )
    return merge_effective_claims(inherited, current)


def active_library_list(run_dir: Path, round_id: str, path_fn: PathBuilder) -> list[dict[str, Any]]:
    current_path = path_fn(run_dir, round_id)
    if current_path.exists():
        current = load_canonical_list(current_path)
        if current:
            return current
    return previous_active_list(run_dir, round_id, path_fn)


def auditable_submission_list(current: Any, active: Any) -> list[dict[str, Any]]:
    if isinstance(active, list) and active:
        return [item for item in active if isinstance(item, dict)]
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    return []


def observation_match_key(item: dict[str, Any], *, include_investigation_tags: bool = True) -> str:
    provenance = item.get("provenance") if isinstance(item.get("provenance"), dict) else {}
    payload = {
        "source_skill": maybe_text(item.get("source_skill")),
        "metric": maybe_text(item.get("metric")),
        "aggregation": maybe_text(item.get("aggregation")),
        "observation_mode": maybe_text(item.get("observation_mode")),
        "evidence_role": maybe_text(item.get("evidence_role")),
        "value": item.get("value"),
        "unit": maybe_text(item.get("unit")),
        "time_window": item.get("time_window"),
        "place_scope": item.get("place_scope"),
        "source_skills": sorted(maybe_text(value) for value in item.get("source_skills", []) if maybe_text(value)),
        "metric_bundle": sorted(maybe_text(value) for value in item.get("metric_bundle", []) if maybe_text(value)),
        "candidate_observation_ids": sorted(
            maybe_text(value) for value in item.get("candidate_observation_ids", []) if maybe_text(value)
        ),
        "quality_flags": sorted(maybe_text(flag) for flag in item.get("quality_flags", []) if maybe_text(flag)),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }
    if include_investigation_tags:
        payload["hypothesis_id"] = maybe_text(item.get("hypothesis_id"))
        payload["leg_id"] = maybe_text(item.get("leg_id"))
        payload["component_roles"] = item.get("component_roles")
    return stable_hash(stable_json(payload))


def hydrate_observation_submissions_with_observations(
    submissions: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    observations_by_id = {
        maybe_text(item.get("observation_id")): item
        for item in observations
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    observations_by_key = {
        observation_match_key(item): item
        for item in observations
        if isinstance(item, dict)
    }
    observations_by_legacy_key = {
        observation_match_key(item, include_investigation_tags=False): item
        for item in observations
        if isinstance(item, dict)
    }
    hydrated: list[dict[str, Any]] = []
    for submission in submissions:
        if not isinstance(submission, dict):
            continue
        item = dict(submission)
        observation = observations_by_id.get(maybe_text(item.get("observation_id")))
        if observation is None:
            observation = observations_by_key.get(observation_match_key(item))
        if observation is None:
            observation = observations_by_legacy_key.get(
                observation_match_key(item, include_investigation_tags=False)
            )
        if observation is not None:
            canonical_observation_id = maybe_text(observation.get("observation_id"))
            if canonical_observation_id and maybe_text(item.get("observation_id")) != canonical_observation_id:
                item["observation_id"] = canonical_observation_id
                item["submission_id"] = observation_submission_id(canonical_observation_id)
            if not isinstance(item.get("statistics"), dict) and isinstance(observation.get("statistics"), dict):
                item["statistics"] = observation.get("statistics")
            if not isinstance(item.get("distribution_summary"), dict) and isinstance(observation.get("distribution_summary"), dict):
                item["distribution_summary"] = observation.get("distribution_summary")
            if not isinstance(item.get("time_window"), dict) and isinstance(observation.get("time_window"), dict):
                item["time_window"] = observation.get("time_window")
            if not maybe_text(item.get("unit")) and maybe_text(observation.get("unit")):
                item["unit"] = maybe_text(observation.get("unit"))
            if not maybe_text(item.get("hypothesis_id")) and maybe_text(observation.get("hypothesis_id")):
                item["hypothesis_id"] = maybe_text(observation.get("hypothesis_id"))
            if not maybe_text(item.get("leg_id")) and maybe_text(observation.get("leg_id")):
                item["leg_id"] = maybe_text(observation.get("leg_id"))
            if not isinstance(item.get("component_roles"), list) and isinstance(observation.get("component_roles"), list):
                item["component_roles"] = copy.deepcopy(observation.get("component_roles"))
        hydrated.append(item)
    return hydrated


def library_state(run_dir: Path, round_id: str) -> dict[str, Any]:
    shared_observations = effective_shared_observations(run_dir, round_id)
    shared_claims = effective_shared_claims(run_dir, round_id)
    claim_submissions_current = load_canonical_list(claim_submissions_path(run_dir, round_id))
    observation_submissions_current = hydrate_observation_submissions_with_observations(
        load_canonical_list(observation_submissions_path(run_dir, round_id)),
        shared_observations,
    )
    claims_active = active_library_list(run_dir, round_id, claims_active_path)
    observations_active = hydrate_observation_submissions_with_observations(
        active_library_list(run_dir, round_id, observations_active_path),
        shared_observations,
    )
    return {
        "claim_candidates_current": load_canonical_list(claim_candidates_path(run_dir, round_id)),
        "observation_candidates_current": load_canonical_list(observation_candidates_path(run_dir, round_id)),
        "claim_curation": load_object_if_exists(claim_curation_path(run_dir, round_id)) or {},
        "observation_curation": load_object_if_exists(observation_curation_path(run_dir, round_id)) or {},
        "shared_claims": shared_claims,
        "shared_observations": shared_observations,
        "claim_submissions_current": claim_submissions_current,
        "observation_submissions_current": observation_submissions_current,
        "claim_submissions_auditable": auditable_submission_list(claim_submissions_current, claims_active),
        "observation_submissions_auditable": auditable_submission_list(observation_submissions_current, observations_active),
        "claims_active": claims_active,
        "observations_active": observations_active,
        "cards_active": active_library_list(run_dir, round_id, cards_active_path),
        "isolated_active": active_library_list(run_dir, round_id, isolated_active_path),
        "remands_open": active_library_list(run_dir, round_id, remands_open_path),
        "matching_result": load_object_if_exists(matching_result_path(run_dir, round_id)) or {},
        "evidence_adjudication": load_object_if_exists(evidence_adjudication_path(run_dir, round_id)) or {},
        "matching_authorization": load_object_if_exists(matching_authorization_path(run_dir, round_id)) or {},
        "readiness_reports": {
            "sociologist": load_object_if_exists(data_readiness_report_path(run_dir, round_id, "sociologist")) or {},
            "environmentalist": load_object_if_exists(data_readiness_report_path(run_dir, round_id, "environmentalist")) or {},
        },
    }


def claim_submission_from_claim(
    claim: dict[str, Any],
    *,
    schema_version: str = "1.0.0",
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    submission = {
        "schema_version": schema_version,
        "submission_id": f"claimsub-{maybe_text(claim.get('claim_id'))}",
        "run_id": maybe_text(claim.get("run_id")),
        "round_id": maybe_text(claim.get("round_id")),
        "agent_role": "sociologist",
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": maybe_text(claim.get("summary")),
        "statement": maybe_text(claim.get("statement")),
        "meaning": (
            f"This public-side claim captures the mission-relevant narrative for {maybe_text(claim.get('claim_type')) or 'the current event'}."
        ),
        "priority": int(claim.get("priority") or 1),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "worth_storing": True,
        "source_signal_count": int(claim.get("source_signal_count") or max(1, len(claim.get("public_refs", [])))),
        "time_window": copy.deepcopy(claim.get("time_window")) if isinstance(claim.get("time_window"), dict) else claim.get("time_window"),
        "place_scope": copy.deepcopy(claim.get("place_scope")) if isinstance(claim.get("place_scope"), dict) else claim.get("place_scope"),
        "public_refs": claim.get("public_refs", []),
        "compact_audit": claim.get("compact_audit")
        if isinstance(claim.get("compact_audit"), dict)
        else build_compact_audit(
            total_candidate_count=max(1, len(claim.get("public_refs", []))),
            retained_count=min(max(1, len(claim.get("public_refs", []))), 8),
            coverage_summary="Derived claim was materialized into a library submission without an explicit compact audit.",
            concentration_flags=[],
            sampling_notes=[],
        ),
    }
    if isinstance(claim.get("claim_scope"), dict):
        submission["claim_scope"] = copy.deepcopy(claim.get("claim_scope"))
    hypothesis_id = maybe_text(claim.get("hypothesis_id"))
    if hypothesis_id:
        submission["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(claim.get("leg_id"))
    if leg_id:
        submission["leg_id"] = leg_id
    validate("claim-submission", submission)
    return submission


def observation_submission_from_observation(
    observation: dict[str, Any],
    *,
    schema_version: str = "1.0.0",
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    canonical_observation = materialize_shared_observation(observation)
    observation_id = maybe_text(canonical_observation.get("observation_id"))
    submission = {
        "schema_version": schema_version,
        "submission_id": observation_submission_id(observation_id),
        "run_id": maybe_text(canonical_observation.get("run_id")),
        "round_id": maybe_text(canonical_observation.get("round_id")),
        "agent_role": "environmentalist",
        "observation_id": observation_id,
        "source_skill": maybe_text(canonical_observation.get("source_skill")),
        "metric": maybe_text(canonical_observation.get("metric")),
        "aggregation": maybe_text(canonical_observation.get("aggregation")),
        "value": canonical_observation.get("value"),
        "unit": maybe_text(canonical_observation.get("unit")),
        "meaning": (
            f"This observation records mission-window physical evidence for metric {maybe_text(canonical_observation.get('metric'))}."
        ),
        "worth_storing": True,
        "time_window": canonical_observation.get("time_window"),
        "place_scope": canonical_observation.get("place_scope"),
        "quality_flags": canonical_observation.get("quality_flags", []),
        "provenance": canonical_observation.get("provenance"),
        "statistics": canonical_observation.get("statistics"),
        "compact_audit": canonical_observation.get("compact_audit")
        if isinstance(canonical_observation.get("compact_audit"), dict)
        else build_compact_audit(
            total_candidate_count=1,
            retained_count=1,
            coverage_summary="Derived observation was materialized into a library submission without an explicit compact audit.",
            concentration_flags=[],
            sampling_notes=[],
        ),
    }
    for field_name in (
        "observation_mode",
        "evidence_role",
        "hypothesis_id",
        "leg_id",
        "source_skills",
        "metric_bundle",
        "candidate_observation_ids",
        "provenance_refs",
        "component_roles",
        "distribution_summary",
    ):
        value = canonical_observation.get(field_name)
        if value is None:
            continue
        submission[field_name] = copy.deepcopy(value) if isinstance(value, (dict, list)) else value
    validate("observation-submission", submission)
    return submission


def dedupe_artifact_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        key = stable_hash(stable_json(ref))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(copy.deepcopy(ref))
    return deduped


def merge_time_windows_from_records(records: list[dict[str, Any]], fallback: dict[str, Any]) -> dict[str, Any]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for record in records:
        window = record.get("time_window") if isinstance(record.get("time_window"), dict) else {}
        start_dt = parse_loose_datetime(window.get("start_utc"))
        end_dt = parse_loose_datetime(window.get("end_utc"))
        if start_dt is not None:
            starts.append(start_dt)
        if end_dt is not None:
            ends.append(end_dt)
    if not starts or not ends:
        return copy.deepcopy(fallback)
    return {
        "start_utc": to_rfc3339_z(min(starts)) or fallback.get("start_utc"),
        "end_utc": to_rfc3339_z(max(ends)) or fallback.get("end_utc"),
    }


def derive_place_scope_from_candidate_observations(records: list[dict[str, Any]], fallback: dict[str, Any]) -> dict[str, Any]:
    if not records:
        return copy.deepcopy(fallback)
    points: list[tuple[float, float]] = []
    for record in records:
        place_scope = record.get("place_scope") if isinstance(record.get("place_scope"), dict) else {}
        geometry = place_scope.get("geometry") if isinstance(place_scope.get("geometry"), dict) else {}
        if maybe_text(geometry.get("type")) != "Point":
            return copy.deepcopy(fallback)
        latitude = maybe_number(geometry.get("latitude"))
        longitude = maybe_number(geometry.get("longitude"))
        if latitude is None or longitude is None:
            return copy.deepcopy(fallback)
        points.append((float(latitude), float(longitude)))
    if not points:
        return copy.deepcopy(fallback)
    unique_points = {(round(lat, 3), round(lon, 3)) for lat, lon in points}
    if len(unique_points) != 1:
        return copy.deepcopy(fallback)
    lat = round(statistics.fmean(point[0] for point in points), 6)
    lon = round(statistics.fmean(point[1] for point in points), 6)
    label = maybe_text(((records[0].get("place_scope") or {}).get("label"))) or maybe_text(fallback.get("label")) or "Mission region"
    return {
        "label": label,
        "geometry": {
            "type": "Point",
            "latitude": lat,
            "longitude": lon,
        },
    }


def candidate_claim_source_signal_count(candidate: dict[str, Any]) -> int:
    value = candidate.get("source_signal_count")
    if isinstance(value, int) and value > 0:
        return value
    refs = candidate.get("public_refs")
    if isinstance(refs, list) and refs:
        return len([item for item in refs if isinstance(item, dict)])
    return 1


def compact_audit_from_curated_claim_candidates(
    candidates: list[dict[str, Any]],
    public_refs: list[dict[str, Any]],
) -> dict[str, Any]:
    audits = [item.get("compact_audit") for item in candidates if isinstance(item.get("compact_audit"), dict)]
    concentration_flags = unique_strings(
        [
            maybe_text(flag)
            for audit in audits
            for flag in audit.get("concentration_flags", [])
            if maybe_text(flag)
        ]
    )
    coverage_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("coverage_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    missing_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("missing_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    sampling_notes = unique_strings(
        [
            maybe_text(note)
            for audit in audits
            for note in audit.get("sampling_notes", [])
            if maybe_text(note)
        ]
    )
    total_candidate_count = 0
    for candidate in candidates:
        audit = candidate.get("compact_audit") if isinstance(candidate.get("compact_audit"), dict) else {}
        count = audit.get("total_candidate_count")
        if isinstance(count, int) and count > 0:
            total_candidate_count += count
        else:
            total_candidate_count += candidate_claim_source_signal_count(candidate)
    return build_compact_audit(
        total_candidate_count=max(1, total_candidate_count),
        retained_count=max(1, len(public_refs)),
        coverage_summary=(
            f"Curated this claim from {len(candidates)} candidate claims and retained {len(public_refs)} supporting public references."
        ),
        coverage_dimensions=coverage_dimensions or ["supporting-artifacts", "source-skill", "channel", "time-window"],
        missing_dimensions=missing_dimensions,
        concentration_flags=concentration_flags,
        sampling_notes=sampling_notes,
    )


def compact_audit_from_curated_observation_candidates(
    candidates: list[dict[str, Any]],
    source_skills: list[str],
    metric_bundle: list[str],
) -> dict[str, Any]:
    audits = [item.get("compact_audit") for item in candidates if isinstance(item.get("compact_audit"), dict)]
    concentration_flags = unique_strings(
        [
            maybe_text(flag)
            for audit in audits
            for flag in audit.get("concentration_flags", [])
            if maybe_text(flag)
        ]
    )
    coverage_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("coverage_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    missing_dimensions = unique_strings(
        [
            maybe_text(dimension)
            for audit in audits
            for dimension in audit.get("missing_dimensions", [])
            if maybe_text(dimension)
        ]
    )
    sampling_notes = unique_strings(
        [
            maybe_text(note)
            for audit in audits
            for note in audit.get("sampling_notes", [])
            if maybe_text(note)
        ]
    )
    total_candidate_count = 0
    for candidate in candidates:
        audit = candidate.get("compact_audit") if isinstance(candidate.get("compact_audit"), dict) else {}
        count = audit.get("total_candidate_count")
        if isinstance(count, int) and count > 0:
            total_candidate_count += count
        else:
            total_candidate_count += distribution_signal_count(candidate)
    return build_compact_audit(
        total_candidate_count=max(1, total_candidate_count),
        retained_count=max(1, len(candidates)),
        coverage_summary=(
            f"Curated this observation from {len(candidates)} candidate observations representing "
            f"{max(1, total_candidate_count)} normalized signals across {len(source_skills) or 1} source skills "
            f"and {len(metric_bundle) or 1} metrics."
        ),
        coverage_dimensions=coverage_dimensions or ["metric-family", "source-skill", "time-window", "distribution-summary", "point-distribution"],
        missing_dimensions=missing_dimensions,
        concentration_flags=concentration_flags,
        sampling_notes=sampling_notes,
    )


def aggregate_candidate_statistics(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    metrics = {
        canonical_environment_metric(candidate.get("metric"))
        for candidate in candidates
        if maybe_text(candidate.get("metric"))
    }
    units = {
        maybe_text(candidate.get("unit"))
        for candidate in candidates
        if maybe_text(candidate.get("unit"))
    }
    if len(metrics) > 1 or len(units) > 1:
        return None
    weighted_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        stats_obj = candidate.get("statistics") if isinstance(candidate.get("statistics"), dict) else {}
        sample_count = maybe_nonnegative_int(stats_obj.get("sample_count")) or 1
        value = maybe_number(candidate.get("value"))
        row = {
            "sample_count": sample_count,
            "min": maybe_number(stats_obj.get("min")),
            "p05": maybe_number(stats_obj.get("p05")),
            "p25": maybe_number(stats_obj.get("p25")),
            "mean": maybe_number(stats_obj.get("mean")),
            "median": maybe_number(stats_obj.get("median")),
            "p75": maybe_number(stats_obj.get("p75")),
            "p95": maybe_number(stats_obj.get("p95")),
            "max": maybe_number(stats_obj.get("max")),
            "stddev": maybe_number(stats_obj.get("stddev")),
        }
        if row["mean"] is None and value is not None:
            row["mean"] = float(value)
        if row["median"] is None:
            row["median"] = row["mean"]
        if row["min"] is None:
            row["min"] = value if value is not None else row["mean"]
        if row["max"] is None:
            row["max"] = value if value is not None else row["mean"]
        if row["p05"] is None:
            row["p05"] = row["min"]
        if row["p25"] is None:
            row["p25"] = row["median"] if row["median"] is not None else row["mean"]
        if row["p75"] is None:
            row["p75"] = row["median"] if row["median"] is not None else row["mean"]
        if row["p95"] is None:
            row["p95"] = row["max"]
        if all(row[field_name] is None for field_name in ("min", "p05", "p25", "mean", "median", "p75", "p95", "max")):
            continue
        weighted_rows.append(row)
    if not weighted_rows:
        return None
    total_sample_count = sum(int(row["sample_count"]) for row in weighted_rows)

    def weighted_average(field_name: str) -> float | None:
        numerator = 0.0
        denominator = 0
        for row in weighted_rows:
            row_value = maybe_number(row.get(field_name))
            if row_value is None:
                continue
            weight = int(row["sample_count"])
            numerator += row_value * weight
            denominator += weight
        if denominator <= 0:
            return None
        return numerator / denominator

    mean_value = weighted_average("mean")
    stddev_value = None
    if mean_value is not None:
        variance_numerator = 0.0
        variance_denominator = 0
        for row in weighted_rows:
            row_mean = maybe_number(row.get("mean"))
            if row_mean is None:
                continue
            weight = int(row["sample_count"])
            row_stddev = maybe_number(row.get("stddev")) or 0.0
            variance_numerator += weight * ((row_stddev ** 2) + ((row_mean - mean_value) ** 2))
            variance_denominator += weight
        if variance_denominator > 0:
            stddev_value = math.sqrt(variance_numerator / variance_denominator)

    min_value = min(
        maybe_number(row.get("min"))
        for row in weighted_rows
        if maybe_number(row.get("min")) is not None
    )
    max_value = max(
        maybe_number(row.get("max"))
        for row in weighted_rows
        if maybe_number(row.get("max")) is not None
    )
    return compact_statistics(
        {
            "sample_count": total_sample_count,
            "min": min_value,
            "p05": weighted_average("p05"),
            "p25": weighted_average("p25"),
            "mean": mean_value,
            "median": weighted_average("median"),
            "p75": weighted_average("p75"),
            "p95": weighted_average("p95"),
            "max": max_value,
            "stddev": stddev_value,
        }
    )


def merge_count_items(counter: Counter[str], items: Any) -> None:
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("value"))
        count = maybe_nonnegative_int(item.get("count"))
        if not label or count is None or count <= 0:
            continue
        counter[label] += count


def distribution_signal_count(candidate: dict[str, Any]) -> int:
    distribution_summary = candidate.get("distribution_summary") if isinstance(candidate.get("distribution_summary"), dict) else {}
    signal_count = maybe_nonnegative_int(distribution_summary.get("signal_count"))
    if signal_count is not None and signal_count > 0:
        return signal_count
    stats_obj = candidate.get("statistics") if isinstance(candidate.get("statistics"), dict) else {}
    signal_count = maybe_nonnegative_int(stats_obj.get("sample_count"))
    if signal_count is not None and signal_count > 0:
        return signal_count
    compact_audit = candidate.get("compact_audit") if isinstance(candidate.get("compact_audit"), dict) else {}
    signal_count = maybe_nonnegative_int(compact_audit.get("total_candidate_count"))
    if signal_count is not None and signal_count > 0:
        return signal_count
    return 1


def point_bucket_from_scope(scope: Any) -> str:
    if not isinstance(scope, dict):
        return ""
    geometry = scope.get("geometry") if isinstance(scope.get("geometry"), dict) else {}
    if maybe_text(geometry.get("type")) == "Point":
        return point_bucket_key(geometry.get("latitude"), geometry.get("longitude"))
    return maybe_text(scope.get("label"))


def day_bucket_from_time_window(window: Any) -> str:
    if not isinstance(window, dict):
        return ""
    start_utc = maybe_text(window.get("start_utc"))
    if len(start_utc) >= 10:
        return start_utc[:10]
    end_utc = maybe_text(window.get("end_utc"))
    if len(end_utc) >= 10:
        return end_utc[:10]
    return ""


def default_distribution_summary_from_observation(observation: dict[str, Any]) -> dict[str, Any]:
    signal_count = distribution_signal_count(observation)
    day_counter: Counter[str] = Counter()
    day_bucket = day_bucket_from_time_window(observation.get("time_window"))
    if day_bucket:
        day_counter[day_bucket] += signal_count
    source_counter: Counter[str] = Counter()
    source_skill = maybe_text(observation.get("source_skill"))
    if source_skill:
        source_counter[source_skill] += signal_count
    metric_counter: Counter[str] = Counter()
    metric = maybe_text(observation.get("metric"))
    if metric:
        metric_counter[metric] += signal_count
    point_counter: Counter[str] = Counter()
    point_bucket = point_bucket_from_scope(observation.get("place_scope"))
    if point_bucket:
        point_counter[point_bucket] += signal_count
    return {
        "signal_count": signal_count,
        "distinct_day_count": len(day_counter),
        "distinct_source_skill_count": len(source_counter),
        "distinct_point_count": len(point_counter),
        "time_bucket_counts": sorted_counter_items(day_counter, limit=10),
        "source_skill_counts": top_counter_items(source_counter, limit=10),
        "metric_counts": top_counter_items(metric_counter, limit=10),
        "point_bucket_counts": sorted_counter_items(point_counter, limit=10),
    }


def aggregate_candidate_distribution_summary(
    candidates: list[dict[str, Any]],
    source_skills: list[str],
    metric_bundle: list[str],
) -> dict[str, Any] | None:
    if not candidates:
        return None
    total_signal_count = 0
    day_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    metric_counter: Counter[str] = Counter()
    point_counter: Counter[str] = Counter()
    for candidate in candidates:
        signal_count = distribution_signal_count(candidate)
        total_signal_count += signal_count
        distribution_summary = candidate.get("distribution_summary") if isinstance(candidate.get("distribution_summary"), dict) else {}
        if distribution_summary:
            merge_count_items(day_counter, distribution_summary.get("time_bucket_counts"))
            merge_count_items(source_counter, distribution_summary.get("source_skill_counts"))
            merge_count_items(metric_counter, distribution_summary.get("metric_counts"))
            merge_count_items(point_counter, distribution_summary.get("point_bucket_counts"))
            continue
        day_bucket = day_bucket_from_time_window(candidate.get("time_window"))
        if day_bucket:
            day_counter[day_bucket] += signal_count
        source_skill = maybe_text(candidate.get("source_skill"))
        if source_skill:
            source_counter[source_skill] += signal_count
        metric = maybe_text(candidate.get("metric"))
        if metric:
            metric_counter[metric] += signal_count
        point_bucket = point_bucket_from_scope(candidate.get("place_scope"))
        if point_bucket:
            point_counter[point_bucket] += signal_count
    if not source_counter:
        for source_skill in source_skills:
            if source_skill:
                source_counter[source_skill] += 1
    if not metric_counter:
        for metric in metric_bundle:
            if metric:
                metric_counter[metric] += 1
    return {
        "signal_count": max(1, total_signal_count),
        "distinct_day_count": len(day_counter),
        "distinct_source_skill_count": len(source_counter),
        "distinct_point_count": len(point_counter),
        "time_bucket_counts": sorted_counter_items(day_counter, limit=10),
        "source_skill_counts": top_counter_items(source_counter, limit=10),
        "metric_counts": top_counter_items(metric_counter, limit=10),
        "point_bucket_counts": sorted_counter_items(point_counter, limit=10),
    }


def canonical_source_skill_for_curated_observation(entry: dict[str, Any], candidates: list[dict[str, Any]]) -> tuple[str, list[str]]:
    source_skills = unique_strings([maybe_text(item) for item in entry.get("source_skills", []) if maybe_text(item)])
    if not source_skills:
        source_skills = unique_strings([maybe_text(item.get("source_skill")) for item in candidates if maybe_text(item.get("source_skill"))])
    if len(source_skills) == 1:
        return source_skills[0], source_skills
    if source_skills:
        return "composite-curation", source_skills
    fallback = unique_strings([maybe_text(item.get("source_skill")) for item in candidates if maybe_text(item.get("source_skill"))])
    if len(fallback) == 1:
        return fallback[0], fallback
    if fallback:
        return "composite-curation", fallback
    return "curation-derived-observation", []


def materialize_claim_submission_from_curated_entry(
    *,
    entry: dict[str, Any],
    candidate_lookup: dict[str, dict[str, Any]],
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
    schema_version: str = "1.0.0",
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    candidate_claim_ids = [maybe_text(item) for item in entry.get("candidate_claim_ids", []) if maybe_text(item)]
    candidates = [candidate_lookup[item] for item in candidate_claim_ids if item in candidate_lookup]
    public_refs = dedupe_artifact_refs(
        [
            ref
            for candidate in candidates
            for ref in candidate.get("public_refs", [])
            if isinstance(ref, dict)
        ]
    )
    compact_audit = compact_audit_from_curated_claim_candidates(candidates, public_refs)
    time_window = (
        copy.deepcopy(entry.get("time_window"))
        if isinstance(entry.get("time_window"), dict)
        else merge_time_windows_from_records(candidates, mission_time_window)
    )
    place_scope = copy.deepcopy(mission_scope)
    if isinstance(entry.get("place_scope"), dict):
        place_scope = copy.deepcopy(entry.get("place_scope"))
    elif candidates:
        candidate_scope = candidates[0].get("place_scope")
        if isinstance(candidate_scope, dict):
            place_scope = copy.deepcopy(candidate_scope)
    claim_scope = (
        copy.deepcopy(entry.get("claim_scope"))
        if isinstance(entry.get("claim_scope"), dict)
        else copy.deepcopy(candidates[0].get("claim_scope"))
        if candidates and isinstance(candidates[0].get("claim_scope"), dict)
        else {
            "time_window": copy.deepcopy(time_window),
            "place_scope": copy.deepcopy(place_scope),
            "time_source": "curation-override" if isinstance(entry.get("time_window"), dict) else "candidate-merged",
            "place_source": "curation-override" if isinstance(entry.get("place_scope"), dict) else "candidate-merged",
            "usable_for_matching": True,
            "notes": [],
        }
    )
    if isinstance(entry.get("time_window"), dict):
        claim_scope["time_window"] = copy.deepcopy(entry.get("time_window"))
        claim_scope["time_source"] = "curation-override"
    elif not isinstance(claim_scope.get("time_window"), dict):
        claim_scope["time_window"] = copy.deepcopy(time_window)
    if isinstance(entry.get("place_scope"), dict):
        claim_scope["place_scope"] = copy.deepcopy(entry.get("place_scope"))
        claim_scope["place_source"] = "curation-override"
    elif not isinstance(claim_scope.get("place_scope"), dict):
        claim_scope["place_scope"] = copy.deepcopy(place_scope)
    if not maybe_text(claim_scope.get("time_source")):
        claim_scope["time_source"] = "candidate-merged"
    if not maybe_text(claim_scope.get("place_source")):
        claim_scope["place_source"] = "candidate-merged"
    claim_scope["usable_for_matching"] = (
        maybe_text(claim_scope.get("time_source")) != "mission-fallback"
        and maybe_text(claim_scope.get("place_source")) != "mission-fallback"
    )
    submission = {
        "schema_version": schema_version,
        "submission_id": f"claimsub-{maybe_text(entry.get('claim_id'))}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": "sociologist",
        "claim_id": maybe_text(entry.get("claim_id")),
        "claim_type": maybe_text(entry.get("claim_type")),
        "summary": maybe_text(entry.get("summary")),
        "statement": maybe_text(entry.get("statement")),
        "meaning": maybe_text(entry.get("meaning")),
        "priority": int(entry.get("priority") or 1),
        "needs_physical_validation": bool(entry.get("needs_physical_validation")),
        "worth_storing": bool(entry.get("worth_storing")),
        "source_signal_count": max(1, sum(candidate_claim_source_signal_count(item) for item in candidates)),
        "time_window": time_window,
        "place_scope": place_scope,
        "claim_scope": claim_scope,
        "public_refs": public_refs,
        "compact_audit": compact_audit,
        "candidate_claim_ids": candidate_claim_ids,
        "selection_reason": maybe_text(entry.get("selection_reason")),
    }
    hypothesis_id = maybe_text(entry.get("hypothesis_id")) or (
        maybe_text(candidates[0].get("hypothesis_id")) if candidates else ""
    )
    if hypothesis_id:
        submission["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(entry.get("leg_id")) or (
        maybe_text(candidates[0].get("leg_id")) if candidates else ""
    )
    if leg_id:
        submission["leg_id"] = leg_id
    validate("claim-submission", submission)
    return submission


def materialize_observation_submission_from_curated_entry(
    *,
    entry: dict[str, Any],
    candidate_lookup: dict[str, dict[str, Any]],
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
    schema_version: str = "1.0.0",
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    candidate_observation_ids = [maybe_text(item) for item in entry.get("candidate_observation_ids", []) if maybe_text(item)]
    candidates = [candidate_lookup[item] for item in candidate_observation_ids if item in candidate_lookup]
    source_skill, source_skills = canonical_source_skill_for_curated_observation(entry, candidates)
    metric_bundle = unique_strings(
        [maybe_text(item) for item in entry.get("metric_bundle", []) if maybe_text(item)]
        or [maybe_text(item.get("metric")) for item in candidates if maybe_text(item.get("metric"))]
    )
    provenance_refs = dedupe_artifact_refs(
        [
            ref
            for ref in entry.get("provenance_refs", [])
            if isinstance(ref, dict)
        ]
        or [
            item.get("provenance")
            for item in candidates
            if isinstance(item.get("provenance"), dict)
        ]
    )
    metric = maybe_text(entry.get("metric")) or maybe_text(candidates[0].get("metric")) if candidates else ""
    aggregation = maybe_text(entry.get("aggregation")) or maybe_text(candidates[0].get("aggregation")) if candidates else ""
    unit = maybe_text(entry.get("unit")) or maybe_text(candidates[0].get("unit")) if candidates else ""
    candidate_metric_set = unique_strings(
        canonical_environment_metric(item.get("metric"))
        for item in candidates
        if maybe_text(item.get("metric"))
    )
    candidate_unit_set = unique_strings(
        maybe_text(item.get("unit"))
        for item in candidates
        if maybe_text(item.get("unit"))
    )
    candidate_statistics_comparable = len(candidate_metric_set) <= 1 and len(candidate_unit_set) <= 1
    if len(candidates) > 1 and not maybe_text(entry.get("unit")) and not candidate_statistics_comparable:
        unit = "mixed"
    value = entry.get("value")
    if value is None and candidates:
        if len(candidates) == 1:
            value = candidates[0].get("value")
        elif candidate_statistics_comparable:
            stats_candidate = aggregate_candidate_statistics(candidates)
            if isinstance(stats_candidate, dict):
                value = stats_candidate.get("mean")
    provenance = copy.deepcopy(provenance_refs[0]) if provenance_refs else {
        "source_skill": source_skill,
        "artifact_path": f"{round_dir_name(round_id)}/environmentalist/observation_curation.json",
    }
    time_window = (
        copy.deepcopy(entry.get("time_window"))
        if isinstance(entry.get("time_window"), dict)
        else merge_time_windows_from_records(candidates, mission_time_window)
    )
    place_scope = (
        copy.deepcopy(entry.get("place_scope"))
        if isinstance(entry.get("place_scope"), dict)
        else derive_place_scope_from_candidate_observations(candidates, mission_scope)
    )
    quality_flags = unique_strings(
        [maybe_text(item) for item in entry.get("quality_flags", []) if maybe_text(item)]
        + [
            maybe_text(flag)
            for candidate in candidates
            for flag in candidate.get("quality_flags", [])
            if maybe_text(flag)
        ]
    )
    if len(candidates) > 1 and len(candidate_metric_set) > 1:
        quality_flags = unique_strings(quality_flags + ["mixed-metric-composite"])
    if len(candidates) > 1 and len(candidate_unit_set) > 1:
        quality_flags = unique_strings(quality_flags + ["mixed-unit-composite"])
    statistics_obj = (
        copy.deepcopy(entry.get("statistics"))
        if isinstance(entry.get("statistics"), dict)
        else aggregate_candidate_statistics(candidates) if candidate_statistics_comparable else None
    )
    distribution_summary = (
        copy.deepcopy(entry.get("distribution_summary"))
        if isinstance(entry.get("distribution_summary"), dict)
        else aggregate_candidate_distribution_summary(candidates, source_skills, metric_bundle)
    )
    component_roles = enrich_component_roles_with_candidate_tags(
        copy.deepcopy(entry.get("component_roles")) if isinstance(entry.get("component_roles"), list) else [],
        candidate_lookup=candidate_lookup,
    )
    compact_audit = compact_audit_from_curated_observation_candidates(candidates, source_skills, metric_bundle)
    if len(candidates) > 1 and not isinstance(entry.get("statistics"), dict):
        if candidate_statistics_comparable and isinstance(statistics_obj, dict):
            quality_flags = unique_strings(quality_flags + ["statistics-derived-from-candidate-summaries"])
        elif not candidate_statistics_comparable:
            quality_flags = unique_strings(quality_flags + ["statistics-omitted-noncomparable-components"])
    submission = {
        "schema_version": schema_version,
        "submission_id": observation_submission_id(maybe_text(entry.get("observation_id"))),
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": "environmentalist",
        "observation_id": maybe_text(entry.get("observation_id")),
        "source_skill": source_skill,
        "metric": metric,
        "aggregation": aggregation or ("composite" if len(candidate_observation_ids) > 1 else "point"),
        "value": value,
        "unit": unit or "index",
        "meaning": maybe_text(entry.get("meaning")),
        "worth_storing": bool(entry.get("worth_storing")),
        "time_window": time_window,
        "place_scope": place_scope,
        "quality_flags": quality_flags,
        "provenance": provenance,
        "statistics": statistics_obj,
        "distribution_summary": distribution_summary,
        "compact_audit": compact_audit,
        "observation_mode": maybe_text(entry.get("observation_mode")) or ("composite" if len(candidate_observation_ids) > 1 else "atomic"),
        "evidence_role": maybe_text(entry.get("evidence_role")) or "primary",
        "source_skills": source_skills,
        "metric_bundle": metric_bundle,
        "candidate_observation_ids": candidate_observation_ids,
        "provenance_refs": provenance_refs,
        "selection_reason": maybe_text(entry.get("selection_reason")),
        "component_roles": component_roles,
    }
    hypothesis_id = maybe_text(entry.get("hypothesis_id")) or consensus_nonempty_text(
        [candidate.get("hypothesis_id") for candidate in candidates]
    )
    if hypothesis_id:
        submission["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(entry.get("leg_id")) or consensus_nonempty_text(
        [candidate.get("leg_id") for candidate in candidates]
    )
    if leg_id:
        submission["leg_id"] = leg_id
    validate("observation-submission", submission)
    return submission


def materialize_curated_claims(
    *,
    run_dir: Path,
    round_id: str,
    run_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
    pretty: bool,
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    curation = load_object_if_exists(claim_curation_path(run_dir, round_id)) or {}
    candidates = load_canonical_list(claim_candidates_path(run_dir, round_id))
    candidate_lookup = {
        maybe_text(item.get("claim_id")): item
        for item in candidates
        if maybe_text(item.get("claim_id"))
    }
    curated_entries = curation.get("curated_claims") if isinstance(curation.get("curated_claims"), list) else []
    current_submissions = [
        materialize_claim_submission_from_curated_entry(
            entry=item,
            candidate_lookup=candidate_lookup,
            run_id=run_id,
            round_id=round_id,
            mission_scope=mission_scope,
            mission_time_window=mission_time_window,
            schema_version=schema_version,
            validate_payload=validate,
        )
        for item in curated_entries
        if isinstance(item, dict)
    ]
    active_submissions = merge_claim_submissions(
        previous_active_list(run_dir, round_id, claims_active_path),
        [item for item in current_submissions if bool(item.get("worth_storing"))],
    )
    shared_claims = claims_from_submissions(
        active_submissions,
        schema_version=schema_version,
        emit_row_id=emit_row_id,
        validate_payload=validate,
    )
    write_json(claim_submissions_path(run_dir, round_id), current_submissions, pretty=pretty)
    write_json(claims_active_path(run_dir, round_id), active_submissions, pretty=pretty)
    write_json(shared_claims_path(run_dir, round_id), shared_claims, pretty=pretty)
    append_library_events(
        run_dir,
        round_id,
        [{"object_kind": "claim-submission", "payload": item} for item in current_submissions],
    )
    return {
        "candidate_count": len(candidates),
        "curated_count": len(curated_entries),
        "claim_submission_count": len(current_submissions),
        "claims_active_count": len(active_submissions),
        "shared_claims_count": len(shared_claims),
        "claim_submissions_path": str(claim_submissions_path(run_dir, round_id)),
        "claims_active_path": str(claims_active_path(run_dir, round_id)),
        "shared_claims_path": str(shared_claims_path(run_dir, round_id)),
    }


def materialize_curated_observations(
    *,
    run_dir: Path,
    round_id: str,
    run_id: str,
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
    pretty: bool,
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    curation = load_object_if_exists(observation_curation_path(run_dir, round_id)) or {}
    candidates = load_canonical_list(observation_candidates_path(run_dir, round_id))
    candidate_lookup = {
        maybe_text(item.get("observation_id")): item
        for item in candidates
        if maybe_text(item.get("observation_id"))
    }
    curated_entries = curation.get("curated_observations") if isinstance(curation.get("curated_observations"), list) else []
    current_submissions = [
        materialize_observation_submission_from_curated_entry(
            entry=item,
            candidate_lookup=candidate_lookup,
            run_id=run_id,
            round_id=round_id,
            mission_scope=mission_scope,
            mission_time_window=mission_time_window,
            schema_version=schema_version,
            validate_payload=validate,
        )
        for item in curated_entries
        if isinstance(item, dict)
    ]
    active_submissions = merge_observation_submissions(
        previous_active_list(run_dir, round_id, observations_active_path),
        [item for item in current_submissions if bool(item.get("worth_storing"))],
    )
    shared_observations = observations_from_submissions(
        active_submissions,
        schema_version=schema_version,
        emit_row_id=emit_row_id,
        validate_payload=validate,
    )
    write_json(observation_submissions_path(run_dir, round_id), current_submissions, pretty=pretty)
    write_json(observations_active_path(run_dir, round_id), active_submissions, pretty=pretty)
    write_json(shared_observations_path(run_dir, round_id), shared_observations, pretty=pretty)
    append_library_events(
        run_dir,
        round_id,
        [{"object_kind": "observation-submission", "payload": item} for item in current_submissions],
    )
    return {
        "candidate_count": len(candidates),
        "curated_count": len(curated_entries),
        "observation_submission_count": len(current_submissions),
        "observations_active_count": len(active_submissions),
        "shared_observation_count": len(shared_observations),
        "observation_submissions_path": str(observation_submissions_path(run_dir, round_id)),
        "observations_active_path": str(observations_active_path(run_dir, round_id)),
        "shared_observations_path": str(shared_observations_path(run_dir, round_id)),
    }


__all__ = [
    "active_library_list",
    "aggregate_candidate_distribution_summary",
    "aggregate_candidate_statistics",
    "append_library_events",
    "auditable_submission_list",
    "build_compact_audit",
    "canonical_source_skill_for_curated_observation",
    "candidate_claim_source_signal_count",
    "claim_submission_from_claim",
    "compact_audit_from_curated_claim_candidates",
    "compact_audit_from_curated_observation_candidates",
    "consensus_nonempty_text",
    "day_bucket_from_time_window",
    "default_distribution_summary_from_observation",
    "dedupe_artifact_refs",
    "distribution_signal_count",
    "effective_shared_claims",
    "effective_shared_observations",
    "enrich_component_roles_with_candidate_tags",
    "hydrate_observation_submissions_with_observations",
    "library_state",
    "load_object_if_exists",
    "materialize_claim_submission_from_curated_entry",
    "materialize_curated_claims",
    "materialize_curated_observations",
    "materialize_observation_submission_from_curated_entry",
    "materialize_shared_observation",
    "merge_claim_submissions",
    "merge_count_items",
    "merge_effective_claims",
    "merge_effective_observations",
    "merge_evidence_cards",
    "merge_isolated_entries",
    "merge_observation_submissions",
    "merge_remand_entries",
    "merge_time_windows_from_records",
    "merge_unique_items",
    "observation_match_key",
    "observation_signature_payload",
    "observation_submission_from_observation",
    "observation_submission_id",
    "point_bucket_from_scope",
    "point_bucket_key",
    "previous_active_list",
    "previous_round_id",
    "shared_observation_id",
    "sorted_counter_items",
    "top_counter_items",
    "top_counter_text",
]
