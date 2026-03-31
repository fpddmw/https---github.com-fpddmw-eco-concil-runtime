#!/usr/bin/env python3
"""Normalize nasa-firms-fire-fetch artifacts into environment signal-plane rows."""

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

SKILL_NAME = "eco-normalize-nasa-firms-fire-observation-signals"
SOURCE_SKILL = "nasa-firms-fire-fetch"
PLANE = "environment"


def build_signals(payload: Any, run_id: str, round_id: str, artifact_file: Path, artifact_sha256: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        warnings.append({"code": "missing-records", "message": "Expected payload.records to be a list."})
        return [], warnings
    captured_at = maybe_text(payload.get("generated_at_utc")) or utc_now_iso()
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        acquired_at = maybe_text(record.get("_acquired_at_utc"))
        latitude = maybe_number(record.get("_latitude") or record.get("latitude"))
        longitude = maybe_number(record.get("_longitude") or record.get("longitude"))
        external_id = f"{maybe_text(record.get('latitude'))}:{maybe_text(record.get('longitude'))}:{acquired_at}:{index}"
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, external_id)[:16]
        quality_flags = [maybe_text(record.get("daynight"))] if maybe_text(record.get("daynight")) else []
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=SOURCE_SKILL,
                signal_kind="fire-detection",
                external_id=external_id,
                dedupe_key=external_id,
                title=f"Fire detection at {latitude},{longitude}" if latitude is not None and longitude is not None else "Fire detection",
                body_text=maybe_text(record.get("instrument")) or maybe_text(record.get("satellite")),
                url="",
                author_name="NASA FIRMS",
                channel_name=maybe_text(record.get("_source")) or maybe_text(record.get("satellite")),
                language="",
                query_text="",
                metric="fire_detection_count",
                numeric_value=1.0,
                unit="count",
                published_at_utc="",
                observed_at_utc=acquired_at,
                window_start_utc="",
                window_end_utc="",
                captured_at_utc=captured_at,
                latitude=latitude,
                longitude=longitude,
                quality_flags=quality_flags,
                engagement={},
                metadata={
                    "confidence": maybe_text(record.get("confidence")),
                    "bright_ti4": maybe_text(record.get("bright_ti4")),
                    "bright_ti5": maybe_text(record.get("bright_ti5")),
                    "frp": maybe_text(record.get("frp")),
                    "satellite": maybe_text(record.get("satellite")),
                    "instrument": maybe_text(record.get("instrument")),
                },
                raw_record=record,
                artifact_path=artifact_file,
                record_locator=f"$.records[{index}]",
                artifact_sha256=artifact_sha256,
            )
        )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No NASA FIRMS rows produced normalized signals."})
    return signals, warnings


def normalize_nasa_firms_fire(run_dir: str, run_id: str, round_id: str, artifact_path: str, db_path: str) -> dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Normalize nasa-firms-fire-fetch artifacts into environment signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_nasa_firms_fire(
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
