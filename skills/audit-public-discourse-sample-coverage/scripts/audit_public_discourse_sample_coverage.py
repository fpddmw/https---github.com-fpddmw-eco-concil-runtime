#!/usr/bin/env python3
"""Run the optional-analysis public discourse sample coverage audit helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "audit-public-discourse-sample-coverage"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import (  # noqa: E402
    pretty_json,
    run_audit_public_discourse_sample_coverage,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit DB-backed public discourse sample coverage.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--round-scope", choices=["current", "run"], default="current")
    parser.add_argument("--corpus-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_audit_public_discourse_sample_coverage(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        round_scope=args.round_scope,
        corpus_path=args.corpus_path,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
