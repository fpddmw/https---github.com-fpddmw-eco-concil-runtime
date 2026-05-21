#!/usr/bin/env python3
"""Build a cautious evidence packet for spatiotemporal relation cues."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "materialize-spatiotemporal-relation-evidence-packet"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.spatiotemporal_relation_evidence_packet import (  # noqa: E402
    materialize_spatiotemporal_relation_evidence_packet,
)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Materialize a spatiotemporal relation evidence packet."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--relation-id", default="")
    parser.add_argument("--relation-status", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--write-basis-objects", action="store_true")
    parser.add_argument("--report-id", default="")
    parser.add_argument("--section-key", default="spatiotemporal-relation-evidence")
    parser.add_argument("--agent-role", default="environmental-investigator")
    parser.add_argument("--report-agent-role", default="report-editor")
    parser.add_argument("--confidence", type=float, default=0.55)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = materialize_spatiotemporal_relation_evidence_packet(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        relation_id=args.relation_id,
        relation_status=args.relation_status,
        output_path=args.output_path,
        write_basis_objects=args.write_basis_objects,
        report_id=args.report_id,
        section_key=args.section_key,
        agent_role=args.agent_role,
        report_agent_role=args.report_agent_role,
        confidence=args.confidence,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
