#!/usr/bin/env python3
"""Run the fact/policy/public interaction timeline helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_build_fact_policy_public_interaction_timeline,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a descriptive fact/policy/public interaction timeline."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-nodes", type=int, default=200)
    parser.add_argument("--limit", type=int, default=100000)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_build_fact_policy_public_interaction_timeline(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        output_path=args.output_path,
        max_nodes=args.max_nodes,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
