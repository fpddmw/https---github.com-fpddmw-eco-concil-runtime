#!/usr/bin/env python3
"""Generate challenger objection candidates for spatiotemporal relation cues."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "review-spatiotemporal-relation-alternatives"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_review_spatiotemporal_relation_alternatives,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Review spatiotemporal relation alternatives.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--relation-id", default="")
    parser.add_argument("--relation-status", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_review_spatiotemporal_relation_alternatives(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        relation_id=args.relation_id,
        relation_status=args.relation_status,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
