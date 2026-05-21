#!/usr/bin/env python3
"""Run the formal comment issue annotation-worker helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "classify-formal-comment-issues"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_classify_formal_comment_issues,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify formal comment text into sample-local issue, stance, and concern labels.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--round-scope", choices=["current", "run"], default="current")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--signal-kind", default="")
    parser.add_argument("--annotation-basis-ref", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-items", type=int, default=500)
    parser.add_argument("--max-labels-per-family", type=int, default=4)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_classify_formal_comment_issues(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        round_scope=args.round_scope,
        source_skill=args.source_skill,
        signal_kind=args.signal_kind,
        annotation_basis_ref=args.annotation_basis_ref,
        output_path=args.output_path,
        max_items=args.max_items,
        max_labels_per_family=args.max_labels_per_family,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
