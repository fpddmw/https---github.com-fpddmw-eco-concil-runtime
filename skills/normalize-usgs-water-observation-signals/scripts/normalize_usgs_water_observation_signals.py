#!/usr/bin/env python3
"""Normalize fetch-usgs-water-iv artifacts into environment signal-plane rows."""

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

SKILL_NAME = "normalize-usgs-water-observation-signals"
SOURCE_SKILL = "fetch-usgs-water-iv"
PLANE = "environment"


def canonical_metric(record: dict[str, Any]) -> str:
    parameter_code = maybe_text(record.get("parameter_code"))
    if parameter_code:
        return parameter_code.casefold().replace("-", "_")
    return maybe_text(record.get("variable_name")).casefold().replace(" ", "_")


def build_signals(payload: Any, run_id: str, round_id: str, artifact_file: Path, artifact_sha256: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    wrapper = payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload
    records = wrapper.get("records") if isinstance(wrapper, dict) else None
    if not isinstance(records, list):
        warnings.append({"code": "missing-records", "message": "Expected payload.records to be a list."})
        return [], warnings
    captured_at = maybe_text(wrapper.get("generated_at_utc")) or utc_now_iso()
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        external_id = f"{maybe_text(record.get('site_number'))}:{maybe_text(record.get('parameter_code'))}:{maybe_text(record.get('observed_at_utc'))}:{index}"
        metric = canonical_metric(record)
        value = maybe_number(record.get("value"))
        observed_at = maybe_text(record.get("observed_at_utc"))
        latitude = maybe_number(record.get("latitude"))
        longitude = maybe_number(record.get("longitude"))
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, external_id)[:16]
        quality_flags: list[str] = []
        if record.get("provisional") is True:
            quality_flags.append("provisional")
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=SOURCE_SKILL,
                signal_kind="instantaneous-value",
                external_id=external_id,
                dedupe_key=f"{metric}:{maybe_text(record.get('site_number'))}:{observed_at}",
                title=f"{metric} at site {maybe_text(record.get('site_number'))}",
                body_text=maybe_text(record.get("variable_description")),
                url=maybe_text(record.get("source_query_url")),
                author_name="USGS",
                channel_name=maybe_text(record.get("site_name")),
                language="",
                query_text="",
                metric=metric,
                numeric_value=value,
                unit=maybe_text(record.get("unit")),
                published_at_utc="",
                observed_at_utc=observed_at,
                window_start_utc="",
                window_end_utc="",
                captured_at_utc=captured_at,
                latitude=latitude,
                longitude=longitude,
                quality_flags=quality_flags,
                engagement={},
                metadata={
                    "site_number": maybe_text(record.get("site_number")),
                    "agency_code": maybe_text(record.get("agency_code")),
                    "statistic_code": maybe_text(record.get("statistic_code")),
                    "qualifiers": record.get("qualifiers") if isinstance(record.get("qualifiers"), list) else [],
                },
                raw_record=record,
                artifact_path=artifact_file,
                record_locator=f"$.payload.records[{index}]",
                artifact_sha256=artifact_sha256,
            )
        )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No USGS rows produced normalized signals."})
    return signals, warnings


def normalize_usgs_water(run_dir: str, run_id: str, round_id: str, artifact_path: str, db_path: str) -> dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Normalize fetch-usgs-water-iv artifacts into environment signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_usgs_water(
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
