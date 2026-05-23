#!/usr/bin/env python3
"""Run the optional-analysis public discourse sample summary helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "summarize-public-discourse-sample"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_summarize_public_discourse_sample,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize a DB-backed public discourse sample.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--round-scope", choices=["current", "run"], default="current")
    parser.add_argument("--source-round-id", default="")
    parser.add_argument("--corpus-path", default="")
    parser.add_argument("--coverage-audit-path", default="")
    parser.add_argument("--aggregation-path", default="")
    parser.add_argument("--comparison-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--example-limit", type=int, default=8)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_summarize_public_discourse_sample(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        round_scope=args.round_scope,
        source_round_id=args.source_round_id,
        corpus_path=args.corpus_path,
        coverage_audit_path=args.coverage_audit_path,
        aggregation_path=args.aggregation_path,
        comparison_path=args.comparison_path,
        output_path=args.output_path,
        limit=args.limit,
        example_limit=args.example_limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
