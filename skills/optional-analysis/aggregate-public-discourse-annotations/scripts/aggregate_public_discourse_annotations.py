#!/usr/bin/env python3
"""Run the optional-analysis public discourse annotation aggregation helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "aggregate-public-discourse-annotations"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_aggregate_public_discourse_annotations,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate public discourse sample annotations.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--round-scope", choices=["current", "run"], default="current")
    parser.add_argument("--corpus-path", default="")
    parser.add_argument("--annotations-path", default="")
    parser.add_argument("--taxonomy-labels-path", default="")
    parser.add_argument("--annotation-basis-ref", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_aggregate_public_discourse_annotations(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        round_scope=args.round_scope,
        corpus_path=args.corpus_path,
        annotations_path=args.annotations_path,
        taxonomy_labels_path=args.taxonomy_labels_path,
        annotation_basis_ref=args.annotation_basis_ref,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
