#!/usr/bin/env python3
"""Query DB-backed spatiotemporal relation cue rows."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "query-spatiotemporal-relations"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.planes.analysis_plane import query_spatiotemporal_relation_cues  # noqa: E402
from eco_council_runtime.optional_analysis import pretty_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Query spatiotemporal relation cue rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--result-set-id", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--round-id", default="")
    parser.add_argument("--relation-id", default="")
    parser.add_argument("--relation-type", default="")
    parser.add_argument("--relation-status", default="")
    parser.add_argument("--source-signal-id", default="")
    parser.add_argument("--target-signal-id", default="")
    parser.add_argument("--source-role", default="")
    parser.add_argument("--target-role", default="")
    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--include-result-sets", action="store_true")
    parser.add_argument("--include-contract", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = query_spatiotemporal_relation_cues(
        args.run_dir,
        result_set_id=args.result_set_id,
        run_id=args.run_id,
        round_id=args.round_id,
        relation_id=args.relation_id,
        relation_type=args.relation_type,
        relation_status=args.relation_status,
        source_signal_id=args.source_signal_id,
        target_signal_id=args.target_signal_id,
        source_role=args.source_role,
        target_role=args.target_role,
        latest_only=args.latest_only,
        include_result_sets=args.include_result_sets,
        include_contract=args.include_contract,
        limit=args.limit,
        offset=args.offset,
        db_path=args.db_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
