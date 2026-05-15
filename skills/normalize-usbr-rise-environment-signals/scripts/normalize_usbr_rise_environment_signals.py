#!/usr/bin/env python3
"""Normalize fetch-usbr-rise artifacts into environment signal-plane rows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.planes.signal import (  # noqa: E402
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

SKILL_NAME = "normalize-usbr-rise-environment-signals"
SOURCE_SKILL = "fetch-usbr-rise"
PLANE = "environment"
CANONICAL_OBJECT_KIND = "environment-observation-signal"


def metric_token(record: dict[str, Any]) -> str:
    name = maybe_text(record.get("parameter_name")).casefold()
    if name:
        cleaned = "".join(char if char.isalnum() else "_" for char in name)
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")
        return cleaned.strip("_")
    parameter_id = maybe_text(record.get("parameter_id"))
    return f"rise_parameter_{parameter_id}" if parameter_id else "rise_result"


def title_for_record(record: dict[str, Any], metric: str) -> str:
    item_title = maybe_text(record.get("item_title"))
    location_name = maybe_text(record.get("location_name"))
    if item_title:
        return item_title
    if location_name:
        return f"{metric} at {location_name}"
    return f"{metric} RISE result"


def quality_flags(record: dict[str, Any]) -> list[str]:
    flags = ["usbr-rise-result", "provider-field-normalized"]
    if maybe_text(record.get("provider_disclaimer")):
        flags.append("provider-provisional-disclaimer")
    if maybe_number(record.get("value")) is None:
        flags.append("missing-numeric-result")
    if not maybe_text(record.get("observed_at_utc")):
        flags.append("missing-observed-at")
    if not maybe_text(record.get("item_id")):
        flags.append("missing-item-id")
    return flags


def provider_metadata(record: dict[str, Any], artifact_sha256: str) -> dict[str, Any]:
    raw = record.get("raw") if isinstance(record.get("raw"), dict) else {}
    return {
        "decision_source": "provider-field-normalization",
        "normalization_scope": "usbr-rise-result-fields-only",
        "typed_metadata_status": "not-derived-by-normalizer",
        "provider": "Bureau of Reclamation RISE",
        "item_id": maybe_text(record.get("item_id")),
        "location_id": maybe_text(record.get("location_id")),
        "location_name": maybe_text(record.get("location_name")),
        "parameter_id": maybe_text(record.get("parameter_id")),
        "parameter_name": maybe_text(record.get("parameter_name")),
        "parameter_unit": maybe_text(record.get("parameter_unit")),
        "parameter_group": maybe_text(record.get("parameter_group")),
        "parameter_timestep": maybe_text(record.get("parameter_timestep")),
        "parameter_transformation": maybe_text(record.get("parameter_transformation")),
        "source_code": maybe_text(record.get("source_code")),
        "status": maybe_text(record.get("status")),
        "last_update": maybe_text(record.get("last_update")),
        "create_date": maybe_text(record.get("create_date")),
        "update_date": maybe_text(record.get("update_date")),
        "item_title": maybe_text(record.get("item_title")),
        "landing_page": maybe_text(record.get("landing_page")),
        "provider_disclaimer": maybe_text(record.get("provider_disclaimer")),
        "provider_record_id": maybe_text(raw.get("id")),
        "source_provenance": {
            "source_skill": SOURCE_SKILL,
            "provider": "Bureau of Reclamation RISE",
            "artifact_sha256": artifact_sha256,
        },
    }


def build_signals(
    payload: Any,
    run_id: str,
    round_id: str,
    artifact_file: Path,
    artifact_sha256: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
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
        metric = metric_token(record)
        observed_at = maybe_text(record.get("observed_at_utc"))
        item_id = maybe_text(record.get("item_id"))
        location_id = maybe_text(record.get("location_id"))
        parameter_id = maybe_text(record.get("parameter_id"))
        record_id = maybe_text(record.get("record_id")) or f"{item_id}:{location_id}:{parameter_id}:{observed_at}:{index}"
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, record_id)[:16]
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=SOURCE_SKILL,
                signal_kind="usbr-rise-result",
                canonical_object_kind=CANONICAL_OBJECT_KIND,
                external_id=record_id,
                dedupe_key=f"{item_id}:{location_id}:{parameter_id}:{observed_at}",
                title=title_for_record(record, metric),
                body_text=maybe_text(record.get("item_description")),
                url=maybe_text(record.get("landing_page")),
                author_name="Bureau of Reclamation",
                channel_name=maybe_text(record.get("location_name")) or f"RISE location {location_id}",
                language="",
                query_text="",
                metric=metric,
                numeric_value=maybe_number(record.get("value")),
                unit=maybe_text(record.get("parameter_unit")),
                published_at_utc="",
                observed_at_utc=observed_at,
                window_start_utc="",
                window_end_utc="",
                captured_at_utc=captured_at,
                latitude=maybe_number(record.get("latitude")),
                longitude=maybe_number(record.get("longitude")),
                quality_flags=quality_flags(record),
                engagement={},
                metadata=provider_metadata(record, artifact_sha256),
                raw_record=record,
                artifact_path=artifact_file,
                record_locator=f"$.records[{index}]",
                artifact_sha256=artifact_sha256,
            )
        )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No USBR RISE records produced normalized signals."})
    return signals, warnings


def normalize_usbr_rise(
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_path: str,
    db_path: str,
) -> dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Normalize fetch-usbr-rise artifacts into environment signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_usbr_rise(
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
