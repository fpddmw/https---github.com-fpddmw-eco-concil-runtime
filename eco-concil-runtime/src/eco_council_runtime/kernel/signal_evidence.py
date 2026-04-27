from __future__ import annotations

import json
from typing import Any

from .signal_plane_normalizer import resolved_canonical_object_kind


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def decode_json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value) if value is not None else "")
    except (json.JSONDecodeError, TypeError, ValueError):
        return default


def row_value(row: Any, key: str, default: Any = "") -> Any:
    try:
        if hasattr(row, "keys") and key not in row.keys():
            return default
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def signal_metadata(row: Any) -> dict[str, Any]:
    metadata = decode_json(row_value(row, "metadata_json", "{}"), {})
    return metadata if isinstance(metadata, dict) else {}


def signal_quality_flags(row: Any) -> list[Any]:
    flags = decode_json(row_value(row, "quality_flags_json", "[]"), [])
    return flags if isinstance(flags, list) else []


def canonical_signal_kind(row: Any, *, plane: str = "") -> str:
    resolved_plane = maybe_text(plane) or maybe_text(row_value(row, "plane"))
    return resolved_canonical_object_kind(
        plane=resolved_plane,
        source_skill=maybe_text(row_value(row, "source_skill")),
        signal_kind=maybe_text(row_value(row, "signal_kind")),
        canonical_object_kind=maybe_text(row_value(row, "canonical_object_kind")),
    )


def signal_artifact_ref(row: Any, *, plane: str = "") -> dict[str, Any]:
    artifact_path = maybe_text(row_value(row, "artifact_path"))
    record_locator = maybe_text(row_value(row, "record_locator"))
    return {
        "ref_kind": "normalized-signal",
        "object_kind": canonical_signal_kind(row, plane=plane),
        "signal_id": maybe_text(row_value(row, "signal_id")),
        "round_id": maybe_text(row_value(row, "round_id")),
        "plane": maybe_text(plane) or maybe_text(row_value(row, "plane")),
        "source_skill": maybe_text(row_value(row, "source_skill")),
        "signal_kind": maybe_text(row_value(row, "signal_kind")),
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_sha256": maybe_text(row_value(row, "artifact_sha256")),
        "artifact_ref": f"{artifact_path}:{record_locator}",
    }


def signal_evidence_basis(row: Any, *, plane: str = "") -> dict[str, Any]:
    metadata = signal_metadata(row)
    ref = signal_artifact_ref(row, plane=plane)
    temporal_scope = metadata.get("temporal_scope")
    if not isinstance(temporal_scope, dict):
        temporal_scope = {
            "published_at_utc": maybe_text(row_value(row, "published_at_utc")),
            "observed_at_utc": maybe_text(row_value(row, "observed_at_utc")),
            "window_start_utc": maybe_text(row_value(row, "window_start_utc")),
            "window_end_utc": maybe_text(row_value(row, "window_end_utc")),
            "captured_at_utc": maybe_text(row_value(row, "captured_at_utc")),
        }
    spatial_scope = metadata.get("spatial_scope")
    if not isinstance(spatial_scope, dict):
        spatial_scope = {
            "latitude": row_value(row, "latitude", None),
            "longitude": row_value(row, "longitude", None),
            "bbox": decode_json(row_value(row, "bbox_json", "{}"), {}),
        }
    data_quality = metadata.get("data_quality")
    if not isinstance(data_quality, dict):
        data_quality = {
            "quality_flags": signal_quality_flags(row),
            "research_judgement": maybe_text(metadata.get("research_judgement"))
            or "none",
        }
    coverage_limitations = metadata.get("coverage_limitations")
    if not isinstance(coverage_limitations, list):
        coverage_limitations = []
    source_provenance = metadata.get("source_provenance")
    if not isinstance(source_provenance, dict):
        source_provenance = {
            "source_skill": ref["source_skill"],
            "signal_kind": ref["signal_kind"],
            "canonical_object_kind": ref["object_kind"],
            "artifact_path": ref["artifact_path"],
            "record_locator": ref["record_locator"],
            "artifact_sha256": ref["artifact_sha256"],
        }
    return {
        "basis_object_id": ref["signal_id"],
        "basis_object_kind": ref["object_kind"],
        "source_signal_id": ref["signal_id"],
        "evidence_ref": ref,
        "source_provenance": source_provenance,
        "data_quality": data_quality,
        "temporal_scope": temporal_scope,
        "spatial_scope": spatial_scope,
        "coverage_limitations": coverage_limitations,
    }


def with_signal_evidence_fields(
    result: dict[str, Any],
    row: Any,
    *,
    plane: str = "",
) -> dict[str, Any]:
    enriched = dict(result)
    ref = signal_artifact_ref(row, plane=plane)
    enriched["artifact_ref"] = ref["artifact_ref"]
    enriched["evidence_refs"] = [ref]
    enriched["evidence_basis"] = signal_evidence_basis(row, plane=plane)
    return enriched

