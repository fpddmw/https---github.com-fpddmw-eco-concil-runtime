#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
SKIP_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "venv",
}

DECOMPOSITION_TARGETS: dict[str, str] = {
    "eco-concil-runtime/src/eco_council_runtime/kernel/planes/deliberation_plane.py": "P1/P2 deliberation plane facade split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/cli.py": "P5 CLI command split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/planes/analysis_plane.py": "P3 analysis plane split",
    "eco-concil-runtime/src/eco_council_runtime/optional_analysis/__init__.py": "P4 optional-analysis package facade",
    "eco-concil-runtime/src/eco_council_runtime/objects/council/__init__.py": "P6 council object package facade",
    "eco-concil-runtime/src/eco_council_runtime/objects/analysis/__init__.py": "P6 analysis object package facade",
    "eco-concil-runtime/src/eco_council_runtime/contracts/__init__.py": "P6 canonical contract package facade",
    "eco-concil-runtime/src/eco_council_runtime/kernel/archive/benchmark.py": "P8 benchmark split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/governance/skill_approvals.py": "P7 skill approval split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/execution/controller/__init__.py": "P7 controller split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/governance/transition_requests.py": "P7 transition request split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/operator/surfaces/__init__.py": "P7 operator state surface rename/consolidation",
    "eco-concil-runtime/src/eco_council_runtime/kernel/planes/signal/__init__.py": "P7 signal normalizer split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/operator/operations.py": "P7 runtime operations split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/execution/executor.py": "P7 runtime executor split",
    "eco-concil-runtime/src/eco_council_runtime/kernel/archive/post_round.py": "P8 post-round split",
}


def line_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def decomposition_target_rows(root: Path = ROOT) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for relative_path, planned_stage in DECOMPOSITION_TARGETS.items():
        path = root / relative_path
        rows.append(
            {
                "path": relative_path,
                "line_count": line_count(path),
                "planned_stage": planned_stage,
                "exists": path.exists(),
            }
        )
    return sorted(rows, key=lambda item: (-int(item["line_count"]), item["path"]))


def oversized_python_files(
    root: Path = ROOT,
    *,
    threshold: int = 1500,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.py")):
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        relative_path = path.relative_to(root).as_posix()
        if relative_path.startswith("skills/"):
            continue
        count = line_count(path)
        if count < threshold:
            continue
        rows.append(
            {
                "path": relative_path,
                "line_count": count,
                "planned_stage": DECOMPOSITION_TARGETS.get(relative_path, ""),
                "exists": True,
            }
        )
    return sorted(rows, key=lambda item: (-int(item["line_count"]), item["path"]))


def build_report(*, threshold: int = 1500) -> dict[str, Any]:
    targets = decomposition_target_rows()
    oversized = oversized_python_files(threshold=threshold)
    return {
        "schema_version": "module-size-report-v1",
        "status": "completed",
        "threshold": threshold,
        "summary": {
            "decomposition_target_count": len(targets),
            "oversized_file_count": len(oversized),
            "largest_target_line_count": max(
                [int(item["line_count"]) for item in targets] or [0]
            ),
        },
        "decomposition_targets": targets,
        "oversized_files": oversized,
    }


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Module Size Report",
        "",
        f"- Threshold: `{report.get('threshold')}`",
        f"- Decomposition targets: `{report.get('summary', {}).get('decomposition_target_count')}`",
        f"- Oversized files: `{report.get('summary', {}).get('oversized_file_count')}`",
        "",
        "## Decomposition Targets",
        "",
        "| Lines | Path | Planned stage |",
        "| ---: | --- | --- |",
    ]
    for item in report.get("decomposition_targets", []):
        if not isinstance(item, dict):
            continue
        lines.append(
            f"| {int(item.get('line_count') or 0)} | `{item.get('path')}` | {item.get('planned_stage')} |"
        )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Report large OpenClaw modules targeted for decomposition.")
    parser.add_argument("--threshold", type=int, default=1500)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = build_report(threshold=max(1, int(args.threshold)))
    if args.format == "markdown":
        print(markdown_report(report), end="")
    else:
        print(json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
