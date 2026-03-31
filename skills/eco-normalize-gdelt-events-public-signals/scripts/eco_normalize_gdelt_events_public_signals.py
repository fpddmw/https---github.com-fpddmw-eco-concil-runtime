#!/usr/bin/env python3
"""Normalize gdelt-events-fetch manifests into public signal-plane rows."""

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

SKILL_NAME = "eco-normalize-gdelt-events-public-signals"
SOURCE_SKILL = "gdelt-events-fetch"
PLANE = "public"


def build_signals(payload: Any, run_id: str, round_id: str, artifact_file: Path, artifact_sha256: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    downloads = payload.get("downloads") if isinstance(payload, dict) else None
    if not isinstance(downloads, list):
        warnings.append({"code": "missing-downloads", "message": "Expected payload.downloads to be a list."})
        return [], warnings
    captured_at = utc_now_iso()
    signals: list[dict[str, Any]] = []
    for index, download in enumerate(downloads):
        if not isinstance(download, dict):
            continue
        entry = download.get("entry") if isinstance(download.get("entry"), dict) else {}
        timestamp_utc = maybe_text(entry.get("timestamp_utc"))
        url = maybe_text(entry.get("url"))
        title = f"GDELT events export {timestamp_utc}" if timestamp_utc else "GDELT events export"
        body_text = " | ".join(maybe_text(item) for item in download.get("preview_lines", [])[:3] if maybe_text(item))
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, timestamp_utc, url, index)[:16]
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=SOURCE_SKILL,
                signal_kind="export-download",
                external_id=timestamp_utc or f"download-{index}",
                dedupe_key=url or f"{timestamp_utc}:{index}",
                title=title,
                body_text=body_text,
                url=url,
                author_name="",
                channel_name="GDELT Events",
                language="",
                query_text=maybe_text(payload.get("mode")),
                metric="file_size_bytes",
                numeric_value=maybe_number(entry.get("size_bytes")),
                unit="bytes",
                published_at_utc=timestamp_utc,
                observed_at_utc="",
                window_start_utc="",
                window_end_utc="",
                captured_at_utc=captured_at,
                latitude=None,
                longitude=None,
                quality_flags=["raw-export-manifest"],
                engagement={},
                metadata={
                    "download_output_path": maybe_text(download.get("output_path")),
                    "md5": maybe_text(entry.get("md5")),
                    "preview_member": maybe_text(download.get("preview_member")),
                    "validation": download.get("validation") if isinstance(download.get("validation"), dict) else {},
                },
                raw_record=download,
                artifact_path=artifact_file,
                record_locator=f"$.downloads[{index}]",
                artifact_sha256=artifact_sha256,
            )
        )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No GDELT events downloads produced normalized rows."})
    return signals, warnings


def normalize_gdelt_events(run_dir: str, run_id: str, round_id: str, artifact_path: str, db_path: str) -> dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Normalize gdelt-events-fetch manifests into public signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_gdelt_events(
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
