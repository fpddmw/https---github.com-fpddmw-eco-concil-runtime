#!/usr/bin/env python3
"""Run the Optional-analysis research issue surface helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "materialize-research-issue-surface"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis_helpers import pretty_json, run_materialize_research_issue_surface  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize candidate research issue surfaces.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--input-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=400)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_materialize_research_issue_surface(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        input_path=args.input_path,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
