#!/usr/bin/env python3
"""Cluster claim-side controversy inputs into canonical issue-cluster objects."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

SKILL_NAME = "eco-cluster-issue-candidates"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.issue_cluster_skill_runner import (  # noqa: E402
    materialize_issue_cluster_skill,
)


def pretty_json(data: object, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def cluster_issue_candidates_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_scope_path: str,
    claim_verifiability_path: str,
    verification_route_path: str,
    output_path: str,
) -> dict[str, object]:
    return materialize_issue_cluster_skill(
        skill_name=SKILL_NAME,
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        claim_cluster_path=claim_cluster_path,
        claim_scope_path=claim_scope_path,
        claim_verifiability_path=claim_verifiability_path,
        verification_route_path=verification_route_path,
        output_path=output_path,
        default_output_relative=f"analytics/issue_clusters_{round_id}.json",
        selection_mode="cluster-issue-candidates-from-claim-chain",
        method="controversy-issue-clustering-v1",
        use_claim_clusters=True,
        suggested_next_skills=[
            "eco-extract-stance-candidates",
            "eco-extract-concern-facets",
            "eco-extract-actor-profiles",
            "eco-extract-evidence-citation-types",
            "eco-materialize-controversy-map",
        ],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cluster claim-side controversy inputs into canonical issue-cluster objects."
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


def main() -> int:
    args = parse_args()
    payload = cluster_issue_candidates_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_cluster_path=args.claim_cluster_path,
        claim_scope_path=args.claim_scope_path,
        claim_verifiability_path=args.claim_verifiability_path,
        verification_route_path=args.verification_route_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
