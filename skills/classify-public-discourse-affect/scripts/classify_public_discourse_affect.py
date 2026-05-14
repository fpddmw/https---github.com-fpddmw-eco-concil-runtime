#!/usr/bin/env python3
"""Run the public discourse annotation-worker helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "classify-public-discourse-affect"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_classify_public_discourse_affect,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify public discourse corpus items into sample-local annotation labels.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--corpus-path", required=True)
    parser.add_argument("--annotation-basis-ref", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-items", type=int, default=500)
    parser.add_argument("--max-labels-per-family", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_classify_public_discourse_affect(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        corpus_path=args.corpus_path,
        annotation_basis_ref=args.annotation_basis_ref,
        output_path=args.output_path,
        max_items=args.max_items,
        max_labels_per_family=args.max_labels_per_family,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
