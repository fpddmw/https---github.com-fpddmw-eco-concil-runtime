from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.signal_normalizer_common import (
    json_text,
    maybe_text,
    resolve_run_dir,
    stable_hash,
    utc_now_iso,
)
from eco_council_runtime.kernel.planes.signal_normalizer_metadata import (
    resolved_canonical_object_kind,
)
from eco_council_runtime.kernel.planes.signal_normalizer_store import (
    delete_existing_rows_for_artifacts,
    insert_signals,
)
from eco_council_runtime.kernel.planes.signal_plane_schema import connect_db


def base_signal(
    *,
    signal_id: str,
    run_id: str,
    round_id: str,
    plane: str,
    source_skill: str,
    signal_kind: str,
    canonical_object_kind: str = "",
    external_id: str,
    dedupe_key: str,
    title: str,
    body_text: str,
    url: str,
    author_name: str,
    channel_name: str,
    language: str,
    query_text: str,
    metric: str,
    numeric_value: float | None,
    unit: str,
    published_at_utc: str,
    observed_at_utc: str,
    window_start_utc: str,
    window_end_utc: str,
    captured_at_utc: str,
    latitude: float | None,
    longitude: float | None,
    quality_flags: list[Any],
    engagement: dict[str, Any],
    metadata: dict[str, Any],
    raw_record: Any,
    artifact_path: Path,
    record_locator: str,
    artifact_sha256: str,
) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "run_id": run_id,
        "round_id": round_id,
        "plane": plane,
        "batch_id": "",
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "canonical_object_kind": resolved_canonical_object_kind(
            plane=plane,
            source_skill=source_skill,
            signal_kind=signal_kind,
            canonical_object_kind=canonical_object_kind,
        ),
        "external_id": external_id,
        "dedupe_key": dedupe_key,
        "title": title,
        "body_text": body_text,
        "url": url,
        "author_name": author_name,
        "channel_name": channel_name,
        "language": language,
        "query_text": query_text,
        "metric": metric,
        "numeric_value": numeric_value,
        "unit": unit,
        "published_at_utc": published_at_utc,
        "observed_at_utc": observed_at_utc,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "captured_at_utc": captured_at_utc,
        "latitude": latitude,
        "longitude": longitude,
        "bbox_json": json_text({}),
        "quality_flags_json": json_text(quality_flags),
        "engagement_json": json_text(engagement),
        "metadata_json": json_text(metadata),
        "raw_json": json.dumps(raw_record, ensure_ascii=True, sort_keys=True),
        "artifact_path": str(artifact_path),
        "record_locator": record_locator,
        "artifact_sha256": artifact_sha256,
    }


def artifact_ref(signal: dict[str, Any]) -> dict[str, str]:
    return {
        "signal_id": maybe_text(signal.get("signal_id")),
        "artifact_path": maybe_text(signal.get("artifact_path")),
        "record_locator": maybe_text(signal.get("record_locator")),
        "artifact_ref": f"{maybe_text(signal.get('artifact_path'))}:{maybe_text(signal.get('record_locator'))}",
    }


def plane_gap_hints(plane: str, signals: list[dict[str, Any]]) -> list[str]:
    if signals:
        return []
    if plane == "formal":
        return ["No formal-comment signals were normalized from the provided artifact."]
    if plane == "public":
        return ["No public signals were normalized from the provided artifact."]
    return ["No environment signals were normalized from the provided artifact."]


def plane_challenge_hints(plane: str) -> list[str]:
    if plane == "formal":
        return ["Check whether formal comments retained docket, agency, and record-level provenance before investigator review."]
    if plane == "public":
        return ["Check whether normalization kept enough text context and source provenance for investigator review."]
    return ["Check whether provider-specific observation rows retained spatial, metric, and quality metadata before investigator review."]


def suggested_next_skills_for_plane(plane: str) -> list[str]:
    if plane == "formal":
        return ["query-formal-signals"]
    if plane == "public":
        return ["query-public-signals"]
    return ["query-environment-signals"]


def normalize_limit(value: int | None) -> int | None:
    if value is None:
        return None
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return None
    if resolved <= 0:
        return None
    return resolved


def finalize_normalization_streaming(
    *,
    skill_name: str,
    source_skill: str,
    plane: str,
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_file: Path,
    db_path: str,
    signals: Iterable[dict[str, Any]],
    warnings: list[dict[str, str]],
    cleanup_artifact_paths: list[str] | None = None,
    artifact_ref_limit: int | None = None,
    canonical_id_limit: int | None = None,
    chunk_size: int = 1000,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    batch_id = "sigbatch-" + stable_hash(skill_name, run_id, round_id, artifact_file.name, utc_now_iso())[:16]
    connection, db_file = connect_db(run_dir_path, db_path)
    returned_artifact_refs: list[dict[str, str]] = []
    returned_canonical_ids: list[str] = []
    canonical_object_kind_counts: dict[str, int] = {}
    signal_count = 0
    artifact_limit = normalize_limit(artifact_ref_limit)
    canonical_limit = normalize_limit(canonical_id_limit)
    resolved_chunk_size = max(1, int(chunk_size))
    buffer: list[dict[str, Any]] = []

    try:
        delete_existing_rows_for_artifacts(
            connection,
            run_id,
            round_id,
            source_skill,
            cleanup_artifact_paths or [str(artifact_file)],
        )
        for signal in signals:
            canonical_object_kind = resolved_canonical_object_kind(
                plane=maybe_text(signal.get("plane")),
                source_skill=maybe_text(signal.get("source_skill")),
                signal_kind=maybe_text(signal.get("signal_kind")),
                canonical_object_kind=maybe_text(signal.get("canonical_object_kind")),
            )
            signal["canonical_object_kind"] = canonical_object_kind
            if canonical_object_kind:
                canonical_object_kind_counts[canonical_object_kind] = (
                    canonical_object_kind_counts.get(canonical_object_kind, 0) + 1
                )
            signal["batch_id"] = batch_id
            buffer.append(signal)
            signal_count += 1

            if artifact_limit is None or len(returned_artifact_refs) < artifact_limit:
                returned_artifact_refs.append(artifact_ref(signal))
            if canonical_limit is None or len(returned_canonical_ids) < canonical_limit:
                signal_id = maybe_text(signal.get("signal_id"))
                if signal_id:
                    returned_canonical_ids.append(signal_id)

            if len(buffer) >= resolved_chunk_size:
                insert_signals(connection, buffer)
                buffer.clear()

        if buffer:
            insert_signals(connection, buffer)
        connection.commit()
    finally:
        connection.close()

    if artifact_limit is not None and signal_count > artifact_limit:
        warnings.append(
            {
                "code": "artifact-refs-truncated",
                "message": f"Returned artifact_refs were truncated to {artifact_limit} while {signal_count} signals were normalized.",
            }
        )
    if canonical_limit is not None and signal_count > canonical_limit:
        warnings.append(
            {
                "code": "canonical-ids-truncated",
                "message": f"Returned canonical_ids were truncated to {canonical_limit} while {signal_count} signals were normalized.",
            }
        )

    suggested_next_skills = suggested_next_skills_for_plane(plane)
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "plane": plane,
            "source_skill": source_skill,
            "signal_count": signal_count,
            "canonical_object_kind_counts": dict(
                sorted(canonical_object_kind_counts.items())
            ),
            "warning_count": len(warnings),
            "returned_artifact_ref_count": len(returned_artifact_refs),
            "returned_canonical_id_count": len(returned_canonical_ids),
            "db_path": str(db_file),
        },
        "receipt_id": "normalize-receipt-" + stable_hash(skill_name, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": returned_artifact_refs,
        "canonical_ids": returned_canonical_ids,
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": returned_canonical_ids,
            "evidence_refs": returned_artifact_refs[:20],
            "signal_object_kind_counts": dict(
                sorted(canonical_object_kind_counts.items())
            ),
            "gap_hints": [] if signal_count else plane_gap_hints(plane, []),
            "challenge_hints": plane_challenge_hints(plane),
            "suggested_next_skills": suggested_next_skills,
        },
    }


def finalize_normalization(
    *,
    skill_name: str,
    source_skill: str,
    plane: str,
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_file: Path,
    db_path: str,
    signals: list[dict[str, Any]],
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    return finalize_normalization_streaming(
        skill_name=skill_name,
        source_skill=source_skill,
        plane=plane,
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_file=artifact_file,
        db_path=db_path,
        signals=signals,
        warnings=warnings,
    )


__all__ = [
    "artifact_ref",
    "base_signal",
    "finalize_normalization",
    "finalize_normalization_streaming",
    "normalize_limit",
    "plane_challenge_hints",
    "plane_gap_hints",
    "suggested_next_skills_for_plane",
]
