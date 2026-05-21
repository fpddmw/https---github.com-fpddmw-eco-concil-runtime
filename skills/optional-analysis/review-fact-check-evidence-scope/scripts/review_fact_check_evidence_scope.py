#!/usr/bin/env python3
"""Run the Optional-analysis explicit fact-check evidence scope review helper."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

SKILL_NAME = "review-fact-check-evidence-scope"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import pretty_json, run_review_fact_check_evidence_scope  # noqa: E402


def load_json_object_arg(value: str) -> dict:
    text = " ".join(str(value or "").split())
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        candidate = Path(text).expanduser()
        if not candidate.exists():
            return {}
        parsed = json.loads(candidate.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Review explicit evidence scope for a fact-check question.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--verification-scope-json", default="")
    parser.add_argument("--verification-question", default="")
    parser.add_argument("--receptor-scope", default="")
    parser.add_argument("--candidate-source-scope", default="")
    parser.add_argument("--geographic-scope", default="")
    parser.add_argument("--study-period", default="")
    parser.add_argument("--evidence-window", default="")
    parser.add_argument("--lag-window", default="")
    parser.add_argument("--spatial-rule", default="")
    parser.add_argument("--required-source-role", action="append", default=[])
    parser.add_argument("--required-target-role", action="append", default=[])
    parser.add_argument("--required-context-class", action="append", default=[])
    parser.add_argument("--excluded-inference", action="append", default=[])
    parser.add_argument("--lag-assumptions", default="")
    parser.add_argument("--metric-requirements", default="")
    parser.add_argument("--source-requirements", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = run_review_fact_check_evidence_scope(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        verification_scope=load_json_object_arg(args.verification_scope_json),
        verification_question=args.verification_question,
        receptor_scope=args.receptor_scope,
        candidate_source_scope=args.candidate_source_scope,
        lag_window=args.lag_window,
        spatial_rule=args.spatial_rule,
        required_source_roles=args.required_source_role,
        required_target_roles=args.required_target_role,
        required_context_classes=args.required_context_class,
        excluded_inferences=args.excluded_inference,
        geographic_scope=args.geographic_scope,
        study_period=args.study_period,
        evidence_window=args.evidence_window,
        lag_assumptions=args.lag_assumptions,
        metric_requirements=args.metric_requirements,
        source_requirements=args.source_requirements,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
