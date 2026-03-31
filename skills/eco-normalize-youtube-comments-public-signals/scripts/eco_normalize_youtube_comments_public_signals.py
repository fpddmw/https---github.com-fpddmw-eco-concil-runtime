#!/usr/bin/env python3
"""Normalize youtube-comments-fetch artifacts into public signal-plane rows."""

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

SKILL_NAME = "eco-normalize-youtube-comments-public-signals"
SOURCE_SKILL = "youtube-comments-fetch"
PLANE = "public"


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
        comment_id = maybe_text(record.get("comment_id")) or f"comment-{index}"
        video_id = maybe_text(record.get("video_id"))
        body_text = maybe_text(record.get("text_original")) or maybe_text(record.get("text_display"))
        published_at = maybe_text(record.get("published_at"))
        updated_at = maybe_text(record.get("updated_at"))
        author_name = maybe_text(record.get("author_display_name"))
        comment_type = maybe_text(record.get("comment_type"))
        source_meta = record.get("source") if isinstance(record.get("source"), dict) else {}
        query_text = maybe_text(source_meta.get("search_terms"))
        url = f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}" if video_id and comment_id else ""
        title = f"YouTube comment on {video_id}" if video_id else f"YouTube comment {comment_id}"
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, comment_id)[:16]
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=SOURCE_SKILL,
                signal_kind="comment",
                external_id=comment_id,
                dedupe_key=comment_id,
                title=title,
                body_text=body_text,
                url=url,
                author_name=author_name,
                channel_name=maybe_text(record.get("channel_id")) or video_id,
                language="",
                query_text=query_text,
                metric="",
                numeric_value=None,
                unit="",
                published_at_utc=published_at,
                observed_at_utc="",
                window_start_utc="",
                window_end_utc="",
                captured_at_utc=captured_at,
                latitude=None,
                longitude=None,
                quality_flags=[comment_type] if comment_type else [],
                engagement={"like_count": maybe_number(record.get("like_count"))},
                metadata={
                    "video_id": video_id,
                    "thread_id": maybe_text(record.get("thread_id")),
                    "parent_comment_id": maybe_text(record.get("parent_comment_id")),
                    "comment_type": comment_type,
                    "updated_at": updated_at,
                    "source": source_meta,
                },
                raw_record=record,
                artifact_path=artifact_file,
                record_locator=f"$.records[{index}]",
                artifact_sha256=artifact_sha256,
            )
        )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No YouTube comment rows produced normalized signals."})
    return signals, warnings


def normalize_youtube_comments(run_dir: str, run_id: str, round_id: str, artifact_path: str, db_path: str) -> dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Normalize youtube-comments-fetch artifacts into public signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_youtube_comments(
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
