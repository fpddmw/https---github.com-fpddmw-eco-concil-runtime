#!/usr/bin/env python3
"""Materialize the DB-first milestone package from the master plan, progress log, and dashboard."""

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

from eco_council_runtime.kernel.milestone_package import (  # noqa: E402
    current_date_iso,
    materialize_milestone_package_from_paths,
)


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
        description="Materialize the DB-first milestone package from the master plan, progress log, and dashboard."
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
        "--dashboard-path",
        default=str(WORKSPACE_ROOT / "docs" / "archive" / "openclaw-db-first-dashboard.md"),
    )
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--package-date", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    package_date = maybe_text(args.package_date) or current_date_iso()
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if maybe_text(args.output_dir)
        else (WORKSPACE_ROOT / "docs" / "archive" / f"{package_date}-milestone-package").resolve()
    )
    payload = materialize_milestone_package_from_paths(
        master_plan_path=Path(args.master_plan_path).expanduser().resolve(),
        progress_log_path=Path(args.progress_log_path).expanduser().resolve(),
        dashboard_path=Path(args.dashboard_path).expanduser().resolve(),
        output_dir=output_dir,
        package_date=package_date,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
