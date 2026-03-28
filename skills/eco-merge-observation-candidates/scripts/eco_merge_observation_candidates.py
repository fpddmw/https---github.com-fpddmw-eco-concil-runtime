#!/usr/bin/env python3
"""Merge observation candidates into board-ready observation groups."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean, median
from typing import Any

SKILL_NAME = "eco-merge-observation-candidates"


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


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, path_text: str, default_name: str) -> Path:
    text = maybe_text(path_text)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def unique_refs(refs: list[dict[str, Any]], limit: int) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        artifact_ref = maybe_text(ref.get("artifact_ref"))
        if not artifact_ref or artifact_ref in seen:
            continue
        seen.add(artifact_ref)
        deduped.append(
            {
                "signal_id": maybe_text(ref.get("signal_id")),
                "artifact_path": maybe_text(ref.get("artifact_path")),
                "record_locator": maybe_text(ref.get("record_locator")),
                "artifact_ref": artifact_ref,
            }
        )
        if len(deduped) >= limit:
            break
    return deduped


def point_bucket(observation: dict[str, Any], precision: int) -> str:
    geometry = (observation.get("place_scope") or {}).get("geometry")
    if not isinstance(geometry, dict):
        return "no-point"
    latitude = maybe_number(geometry.get("latitude"))
    longitude = maybe_number(geometry.get("longitude"))
    if latitude is None or longitude is None:
        return "no-point"
    return f"{latitude:.{precision}f},{longitude:.{precision}f}"


def point_geometry_from_bucket(bucket: str) -> dict[str, Any]:
    if bucket == "no-point":
        return {}
    latitude_text, longitude_text = bucket.split(",", 1)
    return {"type": "Point", "latitude": float(latitude_text), "longitude": float(longitude_text)}


def time_bucket(observation: dict[str, Any]) -> str:
    time_window = observation.get("time_window") if isinstance(observation.get("time_window"), dict) else {}
    start_utc = maybe_text(time_window.get("start_utc"))
    end_utc = maybe_text(time_window.get("end_utc"))
    reference = start_utc or end_utc
    return reference[:10] if len(reference) >= 10 else "no-time"


def merge_observation_candidates_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str,
    output_path: str,
    metric: str,
    source_skill: str,
    point_precision: int,
    max_groups: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    input_file = resolve_path(run_dir_path, input_path, f"observation_candidates_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"merged_observation_candidates_{round_id}.json")
    payload = load_json_if_exists(input_file)
    warnings: list[dict[str, str]] = []
    if payload is None:
        warnings.append({"code": "missing-input", "message": f"Observation candidate input was not found at {input_file}."})
    observations = payload.get("candidates", []) if isinstance(payload, dict) and isinstance(payload.get("candidates"), list) else []
    groups: dict[str, list[dict[str, Any]]] = {}
    wanted_metric = maybe_text(metric)
    wanted_source_skill = maybe_text(source_skill)
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        if maybe_text(observation.get("run_id")) and maybe_text(observation.get("run_id")) != run_id:
            continue
        if maybe_text(observation.get("round_id")) and maybe_text(observation.get("round_id")) != round_id:
            continue
        observation_metric = maybe_text(observation.get("metric"))
        if wanted_metric and observation_metric != wanted_metric:
            continue
        source_skills = observation.get("source_skills") if isinstance(observation.get("source_skills"), list) else []
        if wanted_source_skill and wanted_source_skill not in [maybe_text(item) for item in source_skills]:
            lead_source_skill = maybe_text(observation.get("source_skill"))
            if lead_source_skill != wanted_source_skill:
                continue
        key = f"{observation_metric}|{point_bucket(observation, max(0, point_precision))}|{time_bucket(observation)}"
        groups.setdefault(key, []).append(observation)
    merged_items: list[dict[str, Any]] = []
    singleton_count = 0
    for group_key in sorted(groups.keys())[: max(1, max_groups)]:
        group = groups[group_key]
        lead = group[0]
        values = [float(value) for value in [maybe_number(item.get("value")) for item in group] if value is not None]
        if not values:
            continue
        if len(group) == 1:
            singleton_count += 1
        refs: list[dict[str, Any]] = []
        source_signal_ids: set[str] = set()
        source_skills: set[str] = set()
        quality_flags: set[str] = set()
        member_observation_ids: list[str] = []
        start_times = sorted(maybe_text(item.get("time_window", {}).get("start_utc")) for item in group if isinstance(item.get("time_window"), dict) and maybe_text(item.get("time_window", {}).get("start_utc")))
        end_times = sorted(maybe_text(item.get("time_window", {}).get("end_utc")) for item in group if isinstance(item.get("time_window"), dict) and maybe_text(item.get("time_window", {}).get("end_utc")))
        for item in group:
            member_observation_ids.append(maybe_text(item.get("observation_id")))
            provenance_refs = item.get("provenance_refs")
            if isinstance(provenance_refs, list):
                refs.extend(provenance_refs)
            item_signal_ids = item.get("source_signal_ids")
            if isinstance(item_signal_ids, list):
                source_signal_ids.update(maybe_text(signal_id) for signal_id in item_signal_ids if maybe_text(signal_id))
            item_source_skills = item.get("source_skills")
            if isinstance(item_source_skills, list):
                source_skills.update(maybe_text(skill_name) for skill_name in item_source_skills if maybe_text(skill_name))
            elif maybe_text(item.get("source_skill")):
                source_skills.add(maybe_text(item.get("source_skill")))
            item_quality_flags = item.get("quality_flags")
            if isinstance(item_quality_flags, list):
                quality_flags.update(maybe_text(flag) for flag in item_quality_flags if maybe_text(flag))
        metric_name = maybe_text(lead.get("metric"))
        location_bucket = point_bucket(lead, max(0, point_precision))
        day_bucket = time_bucket(lead)
        merged_observation_id = "obsmerge-" + stable_hash(run_id, round_id, metric_name, location_bucket, day_bucket)[:12]
        merged_items.append(
            {
                "schema_version": "n2.1",
                "merged_observation_id": merged_observation_id,
                "run_id": run_id,
                "round_id": round_id,
                "metric": metric_name,
                "unit": maybe_text(lead.get("unit")),
                "aggregation": "merged-candidate-set" if len(group) > 1 else "single-candidate",
                "member_observation_ids": member_observation_ids,
                "member_count": len(group),
                "source_skills": sorted(source_skills),
                "source_signal_ids": sorted(source_signal_ids),
                "value": fmean(values),
                "value_summary": {
                    "sample_count": len(values),
                    "min": min(values),
                    "median": median(values),
                    "mean": fmean(values),
                    "max": max(values),
                },
                "time_window": {
                    "start_utc": start_times[0] if start_times else "",
                    "end_utc": end_times[-1] if end_times else "",
                },
                "place_scope": {
                    "label": "Merged observation footprint" if location_bucket != "no-point" else "Unknown merged observation footprint",
                    "geometry": point_geometry_from_bucket(location_bucket),
                },
                "quality_flags": sorted(quality_flags),
                "provenance_refs": unique_refs(refs, 12),
                "merge_basis": {
                    "metric": metric_name,
                    "point_bucket": location_bucket,
                    "time_bucket": day_bucket,
                },
                "compact_audit": {
                    "representative": len(group) > 1,
                    "retained_count": min(len(group), 8),
                    "total_candidate_count": len(group),
                    "coverage_summary": f"Merged {len(group)} observation candidates into one observation group.",
                    "concentration_flags": ["singleton-merge"] if len(group) == 1 else [],
                    "coverage_dimensions": ["metric", "time-bucket", "point-bucket", "source-skill"],
                    "missing_dimensions": ["coordinate-coverage"] if location_bucket == "no-point" else [],
                    "sampling_notes": [],
                },
            }
        )
    wrapper = {
        "schema_version": "n2.1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "merged_count": len(merged_items),
        "merged_observations": merged_items,
    }
    write_json(output_file, wrapper)
    artifact_refs: list[dict[str, str]] = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.merged_observations",
            "artifact_ref": f"{output_file}:$.merged_observations",
        }
    ]
    for merged_item in merged_items:
        artifact_refs.extend(merged_item["provenance_refs"])
    if not merged_items:
        warnings.append({"code": "no-merged-observations", "message": "No merged observations were produced from the available observation candidates."})
    gap_hints: list[str] = []
    if not merged_items:
        gap_hints.append("No merged observations are available for downstream evidence linking.")
    elif singleton_count == len(merged_items):
        gap_hints.append("Most merged observations remain singletons; cross-source overlap is still shallow.")
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "input_path": str(input_file),
            "output_path": str(output_file),
            "merged_count": len(merged_items),
            "input_candidate_count": len(observations),
        },
        "receipt_id": "evidence-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "evbatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:16],
        "artifact_refs": unique_refs(artifact_refs, 40),
        "canonical_ids": [item["merged_observation_id"] for item in merged_items],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [item["merged_observation_id"] for item in merged_items],
            "evidence_refs": unique_refs(artifact_refs, 20),
            "gap_hints": gap_hints,
            "challenge_hints": ["Check whether nearby observations from different providers should still remain separate groups."] if merged_items else [],
            "suggested_next_skills": ["eco-link-claims-to-observations", "eco-build-normalization-audit"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge observation candidates into board-ready observation groups.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--input-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--metric", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--point-precision", type=int, default=2)
    parser.add_argument("--max-groups", type=int, default=100)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = merge_observation_candidates_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        input_path=args.input_path,
        output_path=args.output_path,
        metric=args.metric,
        source_skill=args.source_skill,
        point_precision=args.point_precision,
        max_groups=args.max_groups,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())