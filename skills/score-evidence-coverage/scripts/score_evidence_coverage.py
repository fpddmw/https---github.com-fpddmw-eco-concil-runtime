#!/usr/bin/env python3
"""Deprecated WP4 alias for the legacy evidence coverage formula."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "score-evidence-coverage"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.analysis_objects import (  # noqa: E402
    WP4_DECISION_SOURCE_DEPRECATED_LEGACY_HELPER,
    wp4_helper_metadata,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def artifact_ref(path: Path) -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": "$",
        "artifact_ref": f"{path}:$",
    }


def deprecated_helper_payload(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    output_file: Path,
) -> dict[str, Any]:
    metadata = wp4_helper_metadata(
        skill_name=SKILL_NAME,
        rule_id="HEUR-COVERAGE-001",
        destination="review-evidence-sufficiency",
        decision_source=WP4_DECISION_SOURCE_DEPRECATED_LEGACY_HELPER,
        approval_ref="required:skill_approval_request",
        audit_ref="docs/openclaw-wp4-skills-refactor-workplan.md#8",
        rule_trace=["wp4-deprecated-formula-block"],
        caveats=[
            "Legacy empirical coverage formula is isolated and no longer emits promotion posture fields.",
            "Use the successor sufficiency-review helper only against DB-backed findings, evidence bundles, or report basis.",
            "Any helper cue must be carried into DB council objects before report-basis use.",
        ],
        audit_status="legacy-isolated; default-frozen; approval-required; audit-pending",
        helper_status="deprecated-alias-blocked",
    )
    wrapper = {
        "schema_version": "wp4-deprecated-helper-v1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "deprecated-blocked",
        "wp4_helper_metadata": metadata,
        "query_basis": {
            "run_dir": str(run_dir),
            "method": "wp4-deprecated-formula-block",
        },
        "replacement": {
            "skill": "review-evidence-sufficiency",
            "required_input_surfaces": [
                "finding_records",
                "evidence_bundles",
                "report_section_basis",
                "challenger_review_comments",
            ],
            "default_output_mode": "notes-and-caveats",
        },
        "observed_inputs": {
            "legacy_inputs_loaded": False,
            "analysis_sync_attempted": False,
        },
        "warnings": [
            {
                "code": "deprecated-legacy-helper",
                "message": "WP4 isolates this legacy helper; no empirical sufficiency formula output was produced.",
            },
            {
                "code": "successor-helper-required",
                "message": "Use a DB-backed sufficiency review against explicit findings, evidence bundles, or report basis.",
            },
        ],
    }
    write_json(output_file, wrapper)
    ref = artifact_ref(output_file)
    return {
        "status": "deprecated-blocked",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "replacement_skill": "review-evidence-sufficiency",
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
            "audit_status": metadata["audit_status"],
        },
        "receipt_id": "deprecated-coverage-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "deprecated-coverage-batch-"
        + stable_hash(SKILL_NAME, run_id, round_id, "wp4")[:16],
        "artifact_refs": [ref],
        "canonical_ids": [],
        "warnings": wrapper["warnings"],
        "analysis_sync": {
            "status": "skipped",
            "reason": "deprecated-legacy-helper",
        },
        "input_analysis_sync": {},
        "board_handoff": {
            "candidate_ids": [],
            "evidence_refs": [ref],
            "gap_hints": [
                "No helper result is available for board use; the legacy helper was blocked by WP4 guardrails."
            ],
            "challenge_hints": [
                "Review the sufficiency rubric, source coverage, aggregation, and report usage before approving any successor helper."
            ],
            "suggested_next_skills": [],
        },
    }


def score_evidence_coverage_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"evidence_coverage_{round_id}.json",
    )
    return deprecated_helper_payload(
        run_dir=run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deprecated WP4 alias for the legacy evidence coverage formula."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--links-path", default="")
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--observation-scope-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = score_evidence_coverage_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
