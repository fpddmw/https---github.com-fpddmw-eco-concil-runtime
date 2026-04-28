#!/usr/bin/env python3
"""Run the WP4 approved formal/public taxonomy helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "apply-approved-formal-public-taxonomy"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.wp4_helpers import pretty_json, run_apply_approved_formal_public_taxonomy  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply an approved formal/public taxonomy.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--taxonomy-path", default="")
    parser.add_argument("--taxonomy-version", default="")
    parser.add_argument("--approval-ref", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_apply_approved_formal_public_taxonomy(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        taxonomy_path=args.taxonomy_path,
        taxonomy_version=args.taxonomy_version,
        approval_ref=args.approval_ref,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
