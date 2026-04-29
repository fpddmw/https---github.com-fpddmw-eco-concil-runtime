#!/usr/bin/env python3
"""Run the optional-analysis environment evidence aggregation helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "aggregate-environment-evidence"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis_helpers import pretty_json, run_aggregate_environment_evidence  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate DB-backed environment evidence.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--aggregation-method", default="source-metric-day-summary")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_aggregate_environment_evidence(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        output_path=args.output_path,
        aggregation_method=args.aggregation_method,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
