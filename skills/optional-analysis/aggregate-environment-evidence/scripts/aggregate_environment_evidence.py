#!/usr/bin/env python3
"""Run the optional-analysis environment evidence aggregation helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "aggregate-environment-evidence"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import pretty_json, run_aggregate_environment_evidence  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate DB-backed environment evidence.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument(
        "--aggregation-method",
        choices=[
            "coverage-summary",
            "time-series-summary",
            "point-event-summary",
            "auto-summary",
            "source-metric-day-summary",
        ],
        default="auto-summary",
    )
    parser.add_argument("--round-scope", choices=["current", "up-to-current", "all"], default="current")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--metric", default="")
    parser.add_argument("--observed-after-utc", default="")
    parser.add_argument("--observed-before-utc", default="")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--group-limit", type=int, default=50)
    parser.add_argument("--sample-ref-limit", type=int, default=25)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    bbox = ",".join(str(value) for value in args.bbox) if args.bbox is not None else ""
    payload = run_aggregate_environment_evidence(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        output_path=args.output_path,
        aggregation_method=args.aggregation_method,
        limit=args.limit,
        round_scope=args.round_scope,
        source_skill=args.source_skill,
        metric=args.metric,
        observed_after_utc=args.observed_after_utc,
        observed_before_utc=args.observed_before_utc,
        bbox=bbox,
        group_limit=args.group_limit,
        sample_ref_limit=args.sample_ref_limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
