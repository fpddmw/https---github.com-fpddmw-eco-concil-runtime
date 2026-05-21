#!/usr/bin/env python3
"""Run the claim-gap action card advisory helper."""

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
    run_materialize_claim_gap_action_cards,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Materialize advisory claim-gap action cards."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--output-path", default="")
    parser.add_argument("--low-volume-threshold", type=int, default=3)
    parser.add_argument("--max-cards", type=int, default=50)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_materialize_claim_gap_action_cards(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        output_path=args.output_path,
        low_volume_threshold=args.low_volume_threshold,
        max_cards=args.max_cards,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
