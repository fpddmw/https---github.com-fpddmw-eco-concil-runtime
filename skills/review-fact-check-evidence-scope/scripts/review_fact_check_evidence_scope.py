#!/usr/bin/env python3
"""Run the Optional-analysis explicit fact-check evidence scope review helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "review-fact-check-evidence-scope"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis_helpers import pretty_json, run_review_fact_check_evidence_scope  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Review explicit evidence scope for a fact-check question.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--verification-question", default="")
    parser.add_argument("--geographic-scope", default="")
    parser.add_argument("--study-period", default="")
    parser.add_argument("--evidence-window", default="")
    parser.add_argument("--lag-assumptions", default="")
    parser.add_argument("--metric-requirements", default="")
    parser.add_argument("--source-requirements", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_review_fact_check_evidence_scope(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        verification_question=args.verification_question,
        geographic_scope=args.geographic_scope,
        study_period=args.study_period,
        evidence_window=args.evidence_window,
        lag_assumptions=args.lag_assumptions,
        metric_requirements=args.metric_requirements,
        source_requirements=args.source_requirements,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
