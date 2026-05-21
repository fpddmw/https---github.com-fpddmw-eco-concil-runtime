#!/usr/bin/env python3
"""Run the optional-analysis public discourse corpus materialization helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "materialize-public-discourse-corpus"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_materialize_public_discourse_corpus,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize a DB-backed public discourse corpus.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--round-scope", choices=["current", "run"], default="current")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--source-family", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--observed-after-utc", default="")
    parser.add_argument("--observed-before-utc", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_materialize_public_discourse_corpus(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        round_scope=args.round_scope,
        output_path=args.output_path,
        source_family=args.source_family,
        source_skill=args.source_skill,
        keyword_any=args.keyword,
        observed_after_utc=args.observed_after_utc,
        observed_before_utc=args.observed_before_utc,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
