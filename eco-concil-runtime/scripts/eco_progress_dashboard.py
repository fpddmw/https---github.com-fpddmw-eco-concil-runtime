#!/usr/bin/env python3
"""Render the DB-first progress dashboard from the master plan and progress log."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.progress_dashboard import render_dashboard_from_paths  # noqa: E402


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def pretty_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the DB-first progress dashboard from the master plan and progress log."
    )
    parser.add_argument(
        "--master-plan-path",
        default=str(WORKSPACE_ROOT / "docs" / "archive" / "openclaw-db-first-master-plan.md"),
    )
    parser.add_argument(
        "--progress-log-path",
        default=str(WORKSPACE_ROOT / "docs" / "archive" / "openclaw-db-first-progress-log.md"),
    )
    parser.add_argument(
        "--output-path",
        default=str(WORKSPACE_ROOT / "docs" / "archive" / "openclaw-db-first-dashboard.md"),
    )
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    master_plan_path = Path(args.master_plan_path).expanduser().resolve()
    progress_log_path = Path(args.progress_log_path).expanduser().resolve()
    output_path = Path(args.output_path).expanduser().resolve()
    model, markdown_text = render_dashboard_from_paths(
        master_plan_path=master_plan_path,
        progress_log_path=progress_log_path,
    )
    if args.stdout:
        print(markdown_text, end="")
        return 0
    output_path.write_text(markdown_text, encoding="utf-8")
    next_stage = "none"
    for stage in model.queue:
        stage_status = next(
            (
                item.status
                for item in model.stage_definitions
                if item.stage_id == stage.stage_id
            ),
            "",
        )
        if stage_status not in {"completed", "deferred"}:
            next_stage = maybe_text(stage.stage_id)
            break
    payload = {
        "status": "completed",
        "summary": {
            "master_plan_path": str(master_plan_path),
            "progress_log_path": str(progress_log_path),
            "output_path": str(output_path),
            "stage_count": len(model.stage_definitions),
            "delivery_count": len(model.deliveries),
            "active_stage_count": len(
                [stage for stage in model.stage_definitions if stage.status == "in_progress"]
            ),
            "blocked_stage_count": len(
                [stage for stage in model.stage_definitions if stage.status == "blocked"]
            ),
            "next_stage": next_stage,
        },
    }
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
