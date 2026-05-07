#!/usr/bin/env python3
"""Run the Optional-analysis temporal co-occurrence cue helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "detect-temporal-cooccurrence-cues"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import pretty_json, run_detect_temporal_cooccurrence_cues  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect descriptive temporal co-occurrence cues.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--output-path", default="")
    parser.add_argument("--relation-output-path", default="")
    parser.add_argument("--source-role", default="")
    parser.add_argument("--target-role", default="")
    parser.add_argument("--source-class", default="")
    parser.add_argument("--target-class", default="")
    parser.add_argument("--observed-after-utc", default="")
    parser.add_argument("--observed-before-utc", default="")
    parser.add_argument("--lag-min-hours", type=float, default=None)
    parser.add_argument("--lag-max-hours", type=float, default=None)
    parser.add_argument("--bbox", default="")
    parser.add_argument("--max-distance-km", type=float, default=None)
    parser.add_argument("--spatial-rule", default="")
    parser.add_argument("--taxonomy-version", default="")
    parser.add_argument("--limit", type=int, default=700)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_detect_temporal_cooccurrence_cues(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        output_path=args.output_path,
        relation_output_path=args.relation_output_path,
        source_role=args.source_role,
        target_role=args.target_role,
        source_class=args.source_class,
        target_class=args.target_class,
        observed_after_utc=args.observed_after_utc,
        observed_before_utc=args.observed_before_utc,
        lag_min_hours=args.lag_min_hours,
        lag_max_hours=args.lag_max_hours,
        bbox=args.bbox,
        max_distance_km=args.max_distance_km,
        spatial_rule=args.spatial_rule,
        taxonomy_version=args.taxonomy_version,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
