#!/usr/bin/env python3
"""Normalize fetch-open-meteo-flood artifacts into environment signal-plane rows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    base_signal,
    file_sha256,
    finalize_normalization,
    maybe_number,
    maybe_text,
    pretty_json,
    read_json,
    stable_hash,
    utc_now_iso,
)

SKILL_NAME = "normalize-open-meteo-flood-signals"
SOURCE_SKILL = "fetch-open-meteo-flood"
PLANE = "environment"


def canonical_metric(metric: str) -> str:
    return maybe_text(metric).casefold().replace(".", "_").replace("-", "_")


def build_signals(payload: Any, run_id: str, round_id: str, artifact_file: Path, artifact_sha256: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        warnings.append({"code": "missing-records", "message": "Expected payload.records to be a list."})
        return [], warnings
    captured_at = maybe_text(payload.get("generated_at_utc")) or utc_now_iso()
    signals: list[dict[str, Any]] = []
    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        daily = record.get("daily") if isinstance(record.get("daily"), dict) else {}
        units = record.get("daily_units") if isinstance(record.get("daily_units"), dict) else {}
        time_values = daily.get("time") if isinstance(daily.get("time"), list) else []
        latitude = maybe_number(record.get("latitude"))
        longitude = maybe_number(record.get("longitude"))
        for key, values in daily.items():
            if key == "time" or not isinstance(values, list):
                continue
            if len(values) != len(time_values):
                warnings.append({"code": "length-mismatch", "message": f"Skipped daily.{key} because value count did not match time count."})
                continue
            metric = canonical_metric(key)
            for index, raw_value in enumerate(values):
                value = maybe_number(raw_value)
                if value is None:
                    continue
                observed_at = maybe_text(time_values[index])
                signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, record_index, key, index, value)[:16]
                signals.append(
                    base_signal(
                        signal_id=signal_id,
                        run_id=run_id,
                        round_id=round_id,
                        plane=PLANE,
                        source_skill=SOURCE_SKILL,
                        signal_kind="daily-observation",
                        external_id=f"{record_index}:{key}:{index}",
                        dedupe_key=f"{metric}:{latitude}:{longitude}:{observed_at}",
                        title=f"{metric} at {latitude},{longitude}" if latitude is not None and longitude is not None else metric,
                        body_text="",
                        url="",
                        author_name="",
                        channel_name=maybe_text(record.get("timezone")),
                        language="",
                        query_text="",
                        metric=metric,
                        numeric_value=value,
                        unit=maybe_text(units.get(key)),
                        published_at_utc="",
                        observed_at_utc=observed_at,
                        window_start_utc=observed_at,
                        window_end_utc=observed_at,
                        captured_at_utc=captured_at,
                        latitude=latitude,
                        longitude=longitude,
                        quality_flags=["hydrology-model"],
                        engagement={},
                        metadata={"section": "daily", "timezone": maybe_text(record.get("timezone"))},
                        raw_record={"time": observed_at, "metric": key, "value": raw_value},
                        artifact_path=artifact_file,
                        record_locator=f"$.records[{record_index}].daily.{key}[{index}]",
                        artifact_sha256=artifact_sha256,
                    )
                )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No Open-Meteo flood values produced normalized signals."})
    return signals, warnings


def normalize_open_meteo_flood(run_dir: str, run_id: str, round_id: str, artifact_path: str, db_path: str) -> dict[str, Any]:
    artifact_file = Path(artifact_path).expanduser().resolve()
    artifact_payload = read_json(artifact_file)
    artifact_sha256 = file_sha256(artifact_file)
    signals, warnings = build_signals(artifact_payload, run_id, round_id, artifact_file, artifact_sha256)
    return finalize_normalization(
        skill_name=SKILL_NAME,
        source_skill=SOURCE_SKILL,
        plane=PLANE,
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_file=artifact_file,
        db_path=db_path,
        signals=signals,
        warnings=warnings,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize fetch-open-meteo-flood artifacts into environment signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_open_meteo_flood(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        artifact_path=args.artifact_path,
        db_path=args.db_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
