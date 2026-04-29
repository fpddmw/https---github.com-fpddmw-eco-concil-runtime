#!/usr/bin/env python3
"""Run the Optional-analysis research issue map export helper."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SKILL_NAME = "export-research-issue-map"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis_helpers import pretty_json, run_export_research_issue_map  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a research issue navigation map.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--issue-surface-path", default="")
    parser.add_argument("--issue-views-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_export_research_issue_map(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        issue_surface_path=args.issue_surface_path,
        issue_views_path=args.issue_views_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
