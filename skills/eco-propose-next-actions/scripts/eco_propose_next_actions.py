#!/usr/bin/env python3
"""Propose a ranked next-action queue from board and evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-propose-next-actions"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.investigation_planning import (  # noqa: E402
    d1_contract_fields_from_payload,
    load_ranked_actions_context,
    maybe_text,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_moderator_action_snapshot,
)


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


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def propose_next_actions_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_summary_path: str,
    board_brief_path: str,
    coverage_path: str,
    output_path: str,
    max_actions: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"investigation/next_actions_{round_id}.json",
    )

    action_context = load_ranked_actions_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        board_summary_path=board_summary_path,
        board_brief_path=board_brief_path,
        coverage_path=coverage_path,
        max_actions=max_actions,
    )
    ranked_actions = action_context.get("ranked_actions", [])
    warnings = (
        action_context.get("warnings", [])
        if isinstance(action_context.get("warnings"), list)
        else []
    )
    contract_fields = d1_contract_fields_from_payload(action_context)

    wrapper = {
        "schema_version": "d1.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "board_summary_path": maybe_text(action_context.get("board_summary_file")),
        "board_brief_path": maybe_text(action_context.get("board_brief_file")),
        "coverage_path": maybe_text(action_context.get("coverage_file")),
        **contract_fields,
        "action_count": len(ranked_actions),
        "ranked_actions": ranked_actions,
    }
    write_json_file(output_file, wrapper)
    store_moderator_action_snapshot(
        run_dir_path,
        action_snapshot=wrapper,
        artifact_path=str(output_file),
    )

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.ranked_actions",
            "artifact_ref": f"{output_file}:$.ranked_actions",
        }
    ]
    canonical_ids = [
        maybe_text(action.get("action_id"))
        for action in ranked_actions
        if isinstance(action, dict) and maybe_text(action.get("action_id"))
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "action_count": len(ranked_actions),
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "investigation-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "investigationbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": canonical_ids,
            "evidence_refs": artifact_refs,
            "gap_hints": []
            if ranked_actions
            else [
                "No next actions could be proposed from the current board and coverage context."
            ],
            "challenge_hints": [
                "Open a falsification probe for contradiction-heavy or low-confidence actions."
            ]
            if any(
                isinstance(action, dict) and bool(action.get("probe_candidate"))
                for action in ranked_actions
            )
            else [],
            "suggested_next_skills": [
                "eco-open-falsification-probe",
                "eco-summarize-round-readiness",
                "eco-post-board-note",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Propose a ranked next-action queue from board and evidence artifacts."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-summary-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-actions", type=int, default=6)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = propose_next_actions_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_summary_path=args.board_summary_path,
        board_brief_path=args.board_brief_path,
        coverage_path=args.coverage_path,
        output_path=args.output_path,
        max_actions=args.max_actions,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
