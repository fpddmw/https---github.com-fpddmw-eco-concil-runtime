#!/usr/bin/env python3
"""Derive scope-level canonical issue-cluster candidates from the claim-side chain."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

SKILL_NAME = "extract-issue-candidates"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.issue_cluster_skill_runner import (  # noqa: E402
    materialize_issue_cluster_skill,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive scope-level canonical issue-cluster candidates from the claim-side chain."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-cluster-path", default="")
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--claim-verifiability-path", default="")
    parser.add_argument("--verification-route-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def pretty_json(data: object, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def main() -> int:
    args = parse_args()
    payload = materialize_issue_cluster_skill(
        skill_name=SKILL_NAME,
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_cluster_path=args.claim_cluster_path,
        claim_scope_path=args.claim_scope_path,
        claim_verifiability_path=args.claim_verifiability_path,
        verification_route_path=args.verification_route_path,
        output_path=args.output_path,
        default_output_relative=f"analytics/issue_candidates_{args.round_id}.json",
        selection_mode="extract-issue-candidates-from-claim-scopes",
        method="controversy-issue-extraction-v1",
        use_claim_clusters=False,
        suggested_next_skills=[
            "cluster-issue-candidates",
            "materialize-controversy-map",
        ],
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
