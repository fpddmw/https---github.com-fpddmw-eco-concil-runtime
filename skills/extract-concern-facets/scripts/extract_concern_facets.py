#!/usr/bin/env python3
"""Derive canonical concern-facet objects from canonical issue-cluster rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

SKILL_NAME = "extract-concern-facets"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.typed_issue_skill_runner import (  # noqa: E402
    materialize_typed_issue_surface_skill,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive canonical concern-facet objects from canonical issue-cluster rows."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--issue-clusters-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def pretty_json(data: object, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def main() -> int:
    args = parse_args()
    payload = materialize_typed_issue_surface_skill(
        kind="concern-facet",
        skill_name=SKILL_NAME,
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        issue_clusters_path=args.issue_clusters_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
