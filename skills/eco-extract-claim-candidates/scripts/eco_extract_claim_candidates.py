#!/usr/bin/env python3
"""Extract board-ready public claim candidates from local signal-plane storage."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-extract-claim-candidates"
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    external_id TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    author_name TEXT NOT NULL DEFAULT '',
    channel_name TEXT NOT NULL DEFAULT '',
    language TEXT NOT NULL DEFAULT '',
    query_text TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL DEFAULT '',
    numeric_value REAL,
    unit TEXT NOT NULL DEFAULT '',
    published_at_utc TEXT NOT NULL DEFAULT '',
    observed_at_utc TEXT NOT NULL DEFAULT '',
    window_start_utc TEXT NOT NULL DEFAULT '',
    window_end_utc TEXT NOT NULL DEFAULT '',
    captured_at_utc TEXT NOT NULL DEFAULT '',
    latitude REAL,
    longitude REAL,
    bbox_json TEXT NOT NULL DEFAULT '{}',
    quality_flags_json TEXT NOT NULL DEFAULT '[]',
    engagement_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT 'null',
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL DEFAULT ''
);
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def truncate_text(value: Any, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


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


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return run_dir / "analytics" / "signal_plane.sqlite"
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_output_path(run_dir: Path, output_path: str, default_name: str) -> Path:
    text = maybe_text(output_path)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str) -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    return connection, file_path


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def claim_type_from_text(text: str) -> str:
    folded = text.casefold()
    if any(token in folded for token in ("smoke", "wildfire", "fire", "flood", "pollution", "contamination")):
        return "hazard-impact"
    if any(token in folded for token in ("fear", "concern", "anger", "protest", "panic")):
        return "social-response"
    if any(token in folded for token in ("confirmed", "report", "official", "evidence")):
        return "verification"
    return "public-claim"


def semantic_fingerprint(text: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", text.casefold())
    if not tokens:
        return "empty"
    return "-".join(tokens[:12])


def signal_ref(row: sqlite3.Row) -> dict[str, str]:
    return {
        "signal_id": row["signal_id"],
        "artifact_path": maybe_text(row["artifact_path"]),
        "record_locator": maybe_text(row["record_locator"]),
        "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}",
    }


def extract_claim_candidates_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    source_skill: str,
    claim_type: str,
    keyword_any: list[str],
    max_candidates: int,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    candidate_path = resolve_output_path(run_dir_path, output_path, f"claim_candidates_{round_id}.json")
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        rows = connection.execute(
            "SELECT * FROM normalized_signals WHERE plane = 'public' AND run_id = ? AND round_id = ? ORDER BY COALESCE(published_at_utc, captured_at_utc) DESC, signal_id",
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()
    groups: dict[str, list[sqlite3.Row]] = {}
    wanted_claim_type = maybe_text(claim_type)
    keywords = [maybe_text(keyword).casefold() for keyword in keyword_any if maybe_text(keyword)]
    for row in rows:
        if maybe_text(source_skill) and maybe_text(row["source_skill"]) != maybe_text(source_skill):
            continue
        text = " ".join(part for part in (maybe_text(row["title"]), maybe_text(row["body_text"])) if part)
        if not text:
            continue
        if keywords and not any(keyword in text.casefold() for keyword in keywords):
            continue
        derived_claim_type = claim_type_from_text(text)
        if wanted_claim_type and derived_claim_type != wanted_claim_type:
            continue
        group_key = f"{derived_claim_type}|{semantic_fingerprint(text)}"
        groups.setdefault(group_key, []).append(row)
    ordered_groups = sorted(groups.values(), key=lambda items: (-len(items), maybe_text(items[0]["signal_id"])))
    candidates: list[dict[str, Any]] = []
    for group in ordered_groups[: max(1, max_candidates)]:
        lead = group[0]
        source_text = " ".join(part for part in (maybe_text(lead["title"]), maybe_text(lead["body_text"])) if part)
        derived_claim_type = claim_type_from_text(source_text)
        candidate_id = "claimcand-" + stable_hash(run_id, round_id, derived_claim_type, semantic_fingerprint(source_text))[:12]
        time_values = sorted(maybe_text(row["published_at_utc"]) for row in group if maybe_text(row["published_at_utc"]))
        candidates.append(
            {
                "schema_version": "n2",
                "claim_id": candidate_id,
                "run_id": run_id,
                "round_id": round_id,
                "agent_role": "sociologist",
                "claim_type": derived_claim_type,
                "status": "candidate",
                "summary": truncate_text(lead["title"] or lead["body_text"], 180),
                "statement": truncate_text(source_text, 320),
                "time_window": {
                    "start_utc": time_values[0] if time_values else "",
                    "end_utc": time_values[-1] if time_values else "",
                },
                "place_scope": {"label": "Public evidence footprint", "geometry": {}},
                "claim_scope": {"label": "Public evidence footprint", "geometry": {}, "usable_for_matching": False},
                "source_signal_count": len(group),
                "source_signal_ids": [row["signal_id"] for row in group],
                "public_refs": [signal_ref(row) for row in group[:8]],
                "compact_audit": {
                    "representative": len(group) > 1,
                    "retained_count": min(len(group), 8),
                    "total_candidate_count": len(group),
                    "coverage_summary": f"Grouped {len(group)} public signals into one claim candidate.",
                    "concentration_flags": [],
                    "coverage_dimensions": ["source-skill", "publication-time"],
                    "missing_dimensions": ["place-scope"],
                    "sampling_notes": [],
                },
            }
        )
    wrapper = {
        "schema_version": "n2",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    write_json(candidate_path, wrapper)
    artifact_refs = [{"signal_id": "", "artifact_path": str(candidate_path), "record_locator": "$.candidates", "artifact_ref": f"{candidate_path}:$.candidates"}]
    for candidate in candidates:
        artifact_refs.extend(candidate["public_refs"])
    batch_id = "candbatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(candidate_path))[:16]
    warnings = [] if candidates else [{"code": "no-candidates", "message": "No claim candidates were extracted from the current public signal plane."}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "candidate_count": len(candidates), "output_path": str(candidate_path), "db_path": str(db_file)},
        "receipt_id": "candidate-receipt-" + stable_hash(SKILL_NAME, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": artifact_refs[:40],
        "canonical_ids": [candidate["claim_id"] for candidate in candidates],
        "warnings": warnings,
        "board_handoff": {"candidate_ids": [candidate["claim_id"] for candidate in candidates], "evidence_refs": artifact_refs[:20], "gap_hints": ["Claim candidates still need scope derivation before direct matching."] if candidates else [], "challenge_hints": ["Check whether repeated public narratives are still collapsing distinct sub-claims."] if candidates else [], "suggested_next_skills": ["eco-build-normalization-audit"]},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract board-ready public claim candidates from local signal-plane storage.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--claim-type", default="")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--max-candidates", type=int, default=50)
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = extract_claim_candidates_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        db_path=args.db_path,
        source_skill=args.source_skill,
        claim_type=args.claim_type,
        keyword_any=args.keyword,
        max_candidates=args.max_candidates,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())