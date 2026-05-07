from __future__ import annotations

import json
from typing import Any

from eco_council_runtime.contracts import (
    ENVIRONMENT_SIGNAL_CLASS_VALUES,
    ENVIRONMENT_SIGNAL_TAXONOMY_AUDIT_STATUS,
    ENVIRONMENT_SIGNAL_TAXONOMY_APPROVAL_REF,
    ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
    SIGNAL_ROLE_VALUES,
)
from eco_council_runtime.kernel.planes.signal.common import (
    json_list,
    json_text,
    maybe_text,
    unique_texts,
)

FORMAL_SOURCE_SKILLS = {
    "fetch-regulationsgov-comments",
    "fetch-regulationsgov-comment-detail",
}


INDEXED_METADATA_TEXT_FIELDS = (
    "agency_id",
    "comment_on_id",
    "decision_source",
    "docket_id",
    "environment_signal_class",
    "route_hint",
    "route_status_hint",
    "relation_candidate_role",
    "signal_role",
    "stance_hint",
    "submitter_name",
    "submitter_type",
    "typing_method",
)

INDEXED_METADATA_LIST_FIELDS = (
    "concern_facets",
    "evidence_citation_types",
    "issue_labels",
    "issue_terms",
)


def default_canonical_object_kind(*, plane: str) -> str:
    resolved_plane = maybe_text(plane)
    if resolved_plane == "formal":
        return "formal-comment-signal"
    if resolved_plane == "environment":
        return "environment-observation-signal"
    if resolved_plane == "public":
        return "public-discourse-signal"
    return ""


def resolved_canonical_object_kind(
    *,
    plane: str,
    source_skill: str = "",
    signal_kind: str = "",
    canonical_object_kind: str = "",
) -> str:
    explicit_kind = maybe_text(canonical_object_kind)
    if explicit_kind:
        return explicit_kind
    if maybe_text(source_skill) in FORMAL_SOURCE_SKILLS:
        return "formal-comment-signal"
    return default_canonical_object_kind(plane=plane)


def metadata_payload(signal: dict[str, Any]) -> dict[str, Any]:
    raw_payload = signal.get("metadata_json")
    if isinstance(raw_payload, str):
        metadata_json = raw_payload
    else:
        metadata_json = "{}" if raw_payload in (None, "") else str(raw_payload)
    try:
        payload = json.loads(metadata_json or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def default_coverage_limitations(*, plane: str, source_skill: str) -> list[str]:
    resolved_plane = maybe_text(plane)
    source = maybe_text(source_skill)
    if resolved_plane == "formal":
        return [
            "Formal records represent submitted or agency-published material available through the provider export; absence of a row is not evidence that no formal record exists.",
        ]
    if resolved_plane == "public":
        return [
            "Public discourse rows reflect the queried platform or media source only and are not a representative sample of affected communities.",
        ]
    if resolved_plane == "environment":
        if source.startswith("open-meteo"):
            return [
                "Modeled environmental data is useful background context and must not be treated as a ground-station measurement without corroboration.",
            ]
        return [
            "Environmental coverage depends on provider station, sensor, model, or archive availability in the requested place and time window.",
        ]
    return ["Coverage is limited to the raw artifact and provider fields normalized into this signal row."]


def environment_signal_taxonomy_defaults(
    *,
    source_skill: str,
    metric: str,
) -> dict[str, str]:
    source = maybe_text(source_skill)
    metric_text = maybe_text(metric).casefold().replace("-", "_")
    if source == "fetch-nasa-firms-fire":
        return {
            "signal_role": "source-event",
            "environment_signal_class": "fire-detection",
        }
    if source in {
        "fetch-airnow-hourly-observations",
        "fetch-openaq",
        "fetch-open-meteo-air-quality",
    }:
        return {
            "signal_role": "receptor-observation",
            "environment_signal_class": "air-quality",
        }
    if source == "fetch-open-meteo-historical" and any(
        token in metric_text
        for token in ("wind", "precip", "rain", "temperature", "humidity")
    ):
        return {
            "signal_role": "context-observation",
            "environment_signal_class": "meteorology",
        }
    if source == "fetch-usgs-water-iv":
        if usgs_water_context_metric(metric_text):
            return {
                "signal_role": "context-observation",
                "environment_signal_class": "hydrology",
            }
        if usgs_water_receptor_metric(metric_text):
            return {
                "signal_role": "receptor-observation",
                "environment_signal_class": "water-quality",
            }
        return {
            "signal_role": "unknown-environment-signal-role",
            "environment_signal_class": "hydrology",
        }
    return {
        "signal_role": "unknown-environment-signal-role",
        "environment_signal_class": "unknown-environment-class",
    }


def usgs_water_context_metric(metric_text: str) -> bool:
    metric_tokens = metric_text.casefold().replace("-", "_").replace(" ", "_")
    if metric_tokens in {
        "00060",
        "00065",
        "river_discharge",
        "river_discharge_mean",
        "river_discharge_max",
        "river_discharge_min",
        "gage_height",
        "gauge_height",
        "streamflow",
        "discharge",
        "flow",
        "stage",
        "water_level",
    }:
        return True
    return any(
        token in metric_tokens
        for token in (
            "discharge",
            "streamflow",
            "gage_height",
            "gauge_height",
            "water_level",
            "stage",
        )
    )


def usgs_water_receptor_metric(metric_text: str) -> bool:
    metric_tokens = metric_text.casefold().replace("-", "_").replace(" ", "_")
    if metric_tokens in {
        "00010",
        "00095",
        "00300",
        "00400",
        "00530",
        "00618",
        "00631",
        "63680",
        "99133",
        "water_temperature",
        "specific_conductance",
        "dissolved_oxygen",
        "ph",
        "turbidity",
        "nitrate",
        "suspended_solids",
        "salinity",
    }:
        return True
    return any(
        token in metric_tokens
        for token in (
            "temperature",
            "conductance",
            "oxygen",
            "turbidity",
            "nitrate",
            "ph",
            "salinity",
            "solids",
            "quality",
        )
    )


def enrich_environment_taxonomy_metadata(
    metadata: dict[str, Any],
    *,
    plane: str,
    source_skill: str,
    metric: str,
) -> None:
    if maybe_text(plane) != "environment":
        return
    defaults = environment_signal_taxonomy_defaults(
        source_skill=source_skill,
        metric=metric,
    )
    signal_role = maybe_text(metadata.get("signal_role")) or defaults["signal_role"]
    environment_class = (
        maybe_text(metadata.get("environment_signal_class"))
        or defaults["environment_signal_class"]
    )
    if signal_role not in SIGNAL_ROLE_VALUES:
        signal_role = "unknown-environment-signal-role"
    if environment_class not in ENVIRONMENT_SIGNAL_CLASS_VALUES:
        environment_class = "unknown-environment-class"
    metadata["signal_role"] = signal_role
    metadata["environment_signal_class"] = environment_class
    metadata.setdefault(
        "environment_signal_taxonomy",
        {
            "taxonomy_version": ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
            "approval_ref": ENVIRONMENT_SIGNAL_TAXONOMY_APPROVAL_REF,
            "audit_status": ENVIRONMENT_SIGNAL_TAXONOMY_AUDIT_STATUS,
        },
    )


def safe_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def enrich_signal_metadata_fields(signal: dict[str, Any]) -> dict[str, Any]:
    """Attach minimal provenance/quality metadata without adding research judgement."""

    metadata = metadata_payload(signal)
    quality_flags = unique_texts(json_list(signal.get("quality_flags_json")))
    plane = maybe_text(signal.get("plane"))
    source_skill = maybe_text(signal.get("source_skill"))
    bbox = safe_json_object(signal.get("bbox_json"))
    enrich_environment_taxonomy_metadata(
        metadata,
        plane=plane,
        source_skill=source_skill,
        metric=maybe_text(signal.get("metric")),
    )

    metadata.setdefault(
        "source_provenance",
        {
            "source_skill": source_skill,
            "signal_kind": maybe_text(signal.get("signal_kind")),
            "canonical_object_kind": maybe_text(signal.get("canonical_object_kind")),
            "artifact_path": maybe_text(signal.get("artifact_path")),
            "record_locator": maybe_text(signal.get("record_locator")),
            "artifact_sha256": maybe_text(signal.get("artifact_sha256")),
        },
    )
    metadata.setdefault(
        "data_quality",
        {
            "quality_flags": quality_flags,
            "normalization_scope": maybe_text(metadata.get("normalization_scope"))
            or "provider-field-normalization",
            "research_judgement": "none",
        },
    )
    metadata.setdefault(
        "temporal_scope",
        {
            "published_at_utc": maybe_text(signal.get("published_at_utc")),
            "observed_at_utc": maybe_text(signal.get("observed_at_utc")),
            "window_start_utc": maybe_text(signal.get("window_start_utc")),
            "window_end_utc": maybe_text(signal.get("window_end_utc")),
            "captured_at_utc": maybe_text(signal.get("captured_at_utc")),
        },
    )
    metadata.setdefault(
        "spatial_scope",
        {
            "latitude": signal.get("latitude"),
            "longitude": signal.get("longitude"),
            "bbox": bbox,
        },
    )
    metadata.setdefault(
        "coverage_limitations",
        default_coverage_limitations(plane=plane, source_skill=source_skill),
    )
    signal["metadata_json"] = json_text(metadata)
    return signal


def indexed_signal_rows(signal: dict[str, Any]) -> list[dict[str, str]]:
    metadata = metadata_payload(signal)
    signal_id = maybe_text(signal.get("signal_id"))
    if not signal_id or not metadata:
        return []

    rows: list[dict[str, str]] = []

    def append_row(field_name: str, value: Any) -> None:
        field_value = maybe_text(value)
        if not field_value:
            return
        rows.append(
            {
                "signal_id": signal_id,
                "run_id": maybe_text(signal.get("run_id")),
                "round_id": maybe_text(signal.get("round_id")),
                "plane": maybe_text(signal.get("plane")),
                "source_skill": maybe_text(signal.get("source_skill")),
                "canonical_object_kind": maybe_text(signal.get("canonical_object_kind")),
                "field_name": field_name,
                "field_value": field_value,
            }
        )

    for field_name in INDEXED_METADATA_TEXT_FIELDS:
        append_row(field_name, metadata.get(field_name))
    for field_name in INDEXED_METADATA_LIST_FIELDS:
        values = metadata.get(field_name)
        if not isinstance(values, list):
            continue
        for value in values:
            append_row(field_name, value)
    return rows


__all__ = [
    "FORMAL_SOURCE_SKILLS",
    "INDEXED_METADATA_LIST_FIELDS",
    "INDEXED_METADATA_TEXT_FIELDS",
    "default_canonical_object_kind",
    "default_coverage_limitations",
    "enrich_environment_taxonomy_metadata",
    "enrich_signal_metadata_fields",
    "environment_signal_taxonomy_defaults",
    "indexed_signal_rows",
    "metadata_payload",
    "resolved_canonical_object_kind",
    "safe_json_object",
    "usgs_water_context_metric",
    "usgs_water_receptor_metric",
]
